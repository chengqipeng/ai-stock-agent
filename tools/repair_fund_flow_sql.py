"""
资金流向历史数据 SQL 修复工具

背景：
  数据库中约 61 万条旧数据使用了错误的东方财富字段映射：
    旧映射: big_net=f[4](大单), big_net_pct=f[9](大单占比), net_flow=f[1](主力)
    新映射: big_net=f[1](主力), big_net_pct=f[6](主力占比), net_flow=big+mid+small

修复方法（纯 SQL，无需网络请求）：
  1. big_net     ← old net_flow（旧 net_flow 存的就是 f[1]=主力净流入）
  2. big_net_pct ← old net_flow * 10000 / kline.trading_amount * 100
     （占比 = 净额(万元)*10000 / 成交额(元) * 100，通过 K线表成交额精确计算）
  3. net_flow    ← new big_net + mid_net + small_net（总净流入）

识别旧数据：
  东方财富旧映射的特征是 f[1]+f[3]+f[2] ≈ 0（全市场资金守恒），
  即 old_net_flow + mid_net + small_net ≈ 0，
  而 net_flow ≠ big_net + mid_net + small_net（不满足新映射的守恒关系）。

Usage:
    # 仅诊断（不修改数据）
    .venv/bin/python -m tools.repair_fund_flow_sql --dry-run

    # 执行修复
    .venv/bin/python -m tools.repair_fund_flow_sql

    # 只修复指定股票
    .venv/bin/python -m tools.repair_fund_flow_sql --stock 600519.SH
"""
import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dao import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("repair_fund_flow_sql")


def diagnose(stock_filter: str | None = None) -> dict:
    """诊断旧映射数据，返回统计信息。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    where_extra = ""
    params: list = []
    if stock_filter:
        codes = [c.strip() for c in stock_filter.split(",")]
        ph = ",".join(["%s"] * len(codes))
        where_extra = f"AND f.stock_code IN ({ph})"
        params = codes

    # 旧数据特征：net_flow + mid_net + small_net ≈ 0 且 net_flow ≠ big+mid+small
    cur.execute(f"""
        SELECT COUNT(*) as old_cnt
        FROM stock_fund_flow f
        WHERE ABS(IFNULL(f.net_flow,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0)) < 1.0
          AND ABS(IFNULL(f.net_flow,0) - (IFNULL(f.big_net,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0))) >= 0.1
          {where_extra}
    """, params)
    old_cnt = cur.fetchone()["old_cnt"]

    cur.execute(f"""
        SELECT COUNT(DISTINCT f.stock_code) as old_stocks
        FROM stock_fund_flow f
        WHERE ABS(IFNULL(f.net_flow,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0)) < 1.0
          AND ABS(IFNULL(f.net_flow,0) - (IFNULL(f.big_net,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0))) >= 0.1
          {where_extra}
    """, params)
    old_stocks = cur.fetchone()["old_stocks"]

    # 有 K线成交额可用于计算 big_net_pct 的记录数
    cur.execute(f"""
        SELECT COUNT(*) as with_kline
        FROM stock_fund_flow f
        JOIN stock_kline k ON f.stock_code = k.stock_code AND f.date = k.date
        WHERE ABS(IFNULL(f.net_flow,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0)) < 1.0
          AND ABS(IFNULL(f.net_flow,0) - (IFNULL(f.big_net,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0))) >= 0.1
          AND k.trading_amount > 0
          {where_extra}
    """, params)
    with_kline = cur.fetchone()["with_kline"]

    # 无 K线匹配的旧数据
    no_kline = old_cnt - with_kline

    cur.execute(f"SELECT COUNT(*) as total FROM stock_fund_flow f WHERE 1=1 {where_extra}", params)
    total = cur.fetchone()["total"]

    cur.close()
    conn.close()

    result = {
        "total_records": total,
        "old_mapping_records": old_cnt,
        "old_mapping_stocks": old_stocks,
        "with_kline_amount": with_kline,
        "without_kline_amount": no_kline,
    }

    logger.info("═" * 60)
    logger.info("诊断结果")
    logger.info("  总记录数:              %d", total)
    logger.info("  旧映射记录数:          %d (%.1f%%)", old_cnt, old_cnt / total * 100 if total else 0)
    logger.info("  旧映射涉及股票数:      %d", old_stocks)
    logger.info("  有K线成交额可修复:     %d", with_kline)
    logger.info("  无K线成交额(仅部分修复): %d", no_kline)
    logger.info("═" * 60)

    return result


def repair(stock_filter: str | None = None, dry_run: bool = False) -> dict:
    """
    执行 SQL 修复。

    修复逻辑：
    1. 有 K线成交额的记录：完整修复 big_net, big_net_pct, net_flow
    2. 无 K线成交额的记录：修复 big_net 和 net_flow，big_net_pct 用比例估算
    """
    conn = get_connection()
    cur = conn.cursor()

    where_extra = ""
    params: list = []
    if stock_filter:
        codes = [c.strip() for c in stock_filter.split(",")]
        ph = ",".join(["%s"] * len(codes))
        where_extra = f"AND f.stock_code IN ({ph})"
        params = codes

    # 阶段1：有 K线成交额的记录 — 完整修复
    sql_with_kline = f"""
        UPDATE stock_fund_flow f
        JOIN stock_kline k ON f.stock_code = k.stock_code AND f.date = k.date
        SET
            f.big_net     = f.net_flow,
            f.big_net_pct = ROUND(f.net_flow * 10000 / k.trading_amount * 100, 2),
            f.net_flow    = ROUND(f.net_flow + IFNULL(f.mid_net, 0) + IFNULL(f.small_net, 0), 2)
        WHERE ABS(IFNULL(f.net_flow,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0)) < 1.0
          AND ABS(IFNULL(f.net_flow,0) - (IFNULL(f.big_net,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0))) >= 0.1
          AND k.trading_amount > 0
          {where_extra}
    """

    # 阶段2：无 K线成交额的记录 — 修复 big_net 和 net_flow，big_net_pct 按比例估算
    # 旧数据中 big_net_pct = f[9](大单占比), net_flow = f[1](主力)
    # f[6] = f[9] * (f[1]/f[4]) 当符号一致时成立
    # 但符号不一致时不可靠，此时设为 NULL（后续由同花顺增量覆盖）
    sql_without_kline = f"""
        UPDATE stock_fund_flow f
        SET
            f.big_net     = f.net_flow,
            f.big_net_pct = CASE
                WHEN f.big_net != 0 AND SIGN(f.net_flow) = SIGN(f.big_net)
                THEN ROUND(f.big_net_pct * f.net_flow / f.big_net, 2)
                ELSE NULL
            END,
            f.net_flow    = ROUND(f.net_flow + IFNULL(f.mid_net, 0) + IFNULL(f.small_net, 0), 2)
        WHERE ABS(IFNULL(f.net_flow,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0)) < 1.0
          AND ABS(IFNULL(f.net_flow,0) - (IFNULL(f.big_net,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0))) >= 0.1
          AND f.stock_code NOT IN (
              SELECT DISTINCT k.stock_code FROM stock_kline k
              WHERE k.stock_code = f.stock_code AND k.date = f.date AND k.trading_amount > 0
          )
          {where_extra}
    """

    if dry_run:
        logger.info("[DRY RUN] 以下 SQL 将被执行（不实际修改数据）：")
        logger.info("阶段1 (有K线成交额): %s", sql_with_kline[:200] + "...")
        logger.info("阶段2 (无K线成交额): %s", sql_without_kline[:200] + "...")
        cur.close()
        conn.close()
        return {"phase1": 0, "phase2": 0, "dry_run": True}

    logger.info("阶段1：修复有K线成交额的记录...")
    t0 = time.time()
    cur.execute(sql_with_kline, params)
    phase1_rows = cur.rowcount
    logger.info("  阶段1完成：%d 行受影响 (%.1fs)", phase1_rows, time.time() - t0)

    logger.info("阶段2：修复无K线成交额的记录...")
    t0 = time.time()
    cur.execute(sql_without_kline, params)
    phase2_rows = cur.rowcount
    logger.info("  阶段2完成：%d 行受影响 (%.1fs)", phase2_rows, time.time() - t0)

    conn.commit()
    cur.close()
    conn.close()

    logger.info("═" * 60)
    logger.info("修复完成：阶段1=%d行, 阶段2=%d行, 总计=%d行",
                phase1_rows, phase2_rows, phase1_rows + phase2_rows)
    logger.info("═" * 60)

    return {"phase1": phase1_rows, "phase2": phase2_rows}


def verify(stock_filter: str | None = None):
    """修复后验证。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    where_extra = ""
    params: list = []
    if stock_filter:
        codes = [c.strip() for c in stock_filter.split(",")]
        ph = ",".join(["%s"] * len(codes))
        where_extra = f"AND f.stock_code IN ({ph})"
        params = codes

    # 检查是否还有旧映射数据
    cur.execute(f"""
        SELECT COUNT(*) as remaining
        FROM stock_fund_flow f
        WHERE ABS(IFNULL(f.net_flow,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0)) < 1.0
          AND ABS(IFNULL(f.net_flow,0) - (IFNULL(f.big_net,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0))) >= 0.1
          {where_extra}
    """, params)
    remaining = cur.fetchone()["remaining"]

    # 检查 big_net_pct 为 NULL 的记录
    cur.execute(f"""
        SELECT COUNT(*) as null_pct FROM stock_fund_flow f
        WHERE f.big_net_pct IS NULL {where_extra}
    """, params)
    null_pct = cur.fetchone()["null_pct"]

    # 抽样验证：big_net_pct ≈ big_net * 10000 / trading_amount * 100
    cur.execute(f"""
        SELECT f.stock_code, f.date, f.big_net, f.big_net_pct,
               ROUND(f.big_net * 10000 / k.trading_amount * 100, 2) as expected_pct,
               ABS(f.big_net_pct - ROUND(f.big_net * 10000 / k.trading_amount * 100, 2)) as diff
        FROM stock_fund_flow f
        JOIN stock_kline k ON f.stock_code = k.stock_code AND f.date = k.date
        WHERE k.trading_amount > 0 AND f.big_net_pct IS NOT NULL
          {where_extra}
        ORDER BY RAND() LIMIT 10
    """, params)
    samples = cur.fetchall()

    # 守恒验证：net_flow ≈ big_net + mid_net + small_net
    cur.execute(f"""
        SELECT COUNT(*) as balanced
        FROM stock_fund_flow f
        WHERE ABS(IFNULL(f.net_flow,0) - (IFNULL(f.big_net,0) + IFNULL(f.mid_net,0) + IFNULL(f.small_net,0))) < 0.1
          {where_extra}
    """, params)
    balanced = cur.fetchone()["balanced"]

    cur.execute(f"SELECT COUNT(*) as total FROM stock_fund_flow f WHERE 1=1 {where_extra}", params)
    total = cur.fetchone()["total"]

    cur.close()
    conn.close()

    logger.info("═" * 60)
    logger.info("验证结果")
    logger.info("  总记录数:          %d", total)
    logger.info("  旧映射残留:        %d", remaining)
    logger.info("  守恒记录数:        %d (%.1f%%)", balanced, balanced / total * 100 if total else 0)
    logger.info("  big_net_pct=NULL:  %d", null_pct)
    logger.info("")
    logger.info("  抽样验证 (big_net_pct vs K线计算值):")
    for s in samples:
        logger.info("    %s %s  pct=%.2f  expected=%.2f  diff=%.4f",
                     s["stock_code"], str(s["date"]), s["big_net_pct"],
                     s["expected_pct"], s["diff"])
    logger.info("═" * 60)

    if remaining > 0:
        logger.warning("仍有 %d 条旧映射数据未修复", remaining)
    if null_pct > 0:
        logger.warning("%d 条记录 big_net_pct 为 NULL（无K线成交额，待同花顺增量覆盖）", null_pct)

    return {
        "total": total,
        "remaining_old": remaining,
        "balanced": balanced,
        "null_pct": null_pct,
    }


def main():
    parser = argparse.ArgumentParser(description="资金流向历史数据 SQL 修复工具")
    parser.add_argument("--dry-run", action="store_true", help="仅显示将执行的SQL，不修改数据")
    parser.add_argument("--stock", type=str, default=None, help="只修复指定股票（逗号分隔）")
    parser.add_argument("--diagnose-only", action="store_true", help="仅诊断")
    parser.add_argument("--verify-only", action="store_true", help="仅验证")
    args = parser.parse_args()

    start = time.time()

    if args.verify_only:
        verify(args.stock)
        logger.info("耗时 %.1fs", time.time() - start)
        return

    # 诊断
    diag = diagnose(args.stock)
    if args.diagnose_only:
        logger.info("耗时 %.1fs", time.time() - start)
        return

    if diag["old_mapping_records"] == 0:
        logger.info("无旧映射数据需要修复。")
        return

    # 修复
    result = repair(args.stock, dry_run=args.dry_run)
    if args.dry_run:
        logger.info("耗时 %.1fs", time.time() - start)
        return

    # 验证
    logger.info("")
    verify(args.stock)
    logger.info("总耗时 %.1fs", time.time() - start)


if __name__ == "__main__":
    main()
