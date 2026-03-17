"""
K线衍生字段全零修复工具

扫描 stock_kline 表中所有股票，找出 amplitude/change_percent/change_amount 全为 0
但价格有变动的异常记录（即"智洋创新"类问题），重新从同花顺拉取数据并修复。

根因：dao/stock_kline_dao.py 的 kline_to_dao_record 曾将三个衍生字段硬编码为 0.0，
导致所有经过 save_kline_to_db 路径写入的数据丢失衍生字段。

修复策略：
  阶段1 - 诊断：扫描全部股票，统计受影响个股和异常记录数
  阶段2 - 修复：对每只问题股票重新拉取K线，利用已修复的 kline_to_dao_record 写入DB
  阶段3 - 验证：修复后重新检测，输出最终报告

Usage:
    # 仅诊断，不修复
    python -m tools.repair_zero_derived_kline --diagnose-only

    # 诊断 + 修复全部
    python -m tools.repair_zero_derived_kline

    # 只修复指定股票
    python -m tools.repair_zero_derived_kline --stock 301370.SZ

    # 限制并发数（默认3）
    python -m tools.repair_zero_derived_kline --concurrency 5
"""
import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dao import get_connection
from dao.stock_kline_dao import (
    TABLE_NAME, get_all_stock_codes, save_kline_to_db, check_db,
)

_CST = ZoneInfo("Asia/Shanghai")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("repair_zero_derived")


# ─────────────────── 阶段1：诊断 ───────────────────

def diagnose_all(stock_codes: list[str] | None = None) -> dict:
    """
    用单条聚合SQL快速扫描数据库，找出衍生字段全零的异常股票。
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 单条SQL批量聚合：找出 amplitude=0 AND change_percent=0 AND change_amount=0
    # 但价格有变动（排除停牌和一字板）的记录
    where_extra = ""
    params = []
    if stock_codes:
        placeholders = ",".join(["%s"] * len(stock_codes))
        where_extra = f"AND stock_code IN ({placeholders})"
        params = list(stock_codes)

    sql = f"""
        SELECT stock_code,
               COUNT(*) AS zero_cnt,
               MIN(`date`) AS first_date,
               MAX(`date`) AS last_date
        FROM {TABLE_NAME}
        WHERE amplitude = 0 AND change_percent = 0 AND change_amount = 0
          AND NOT (open_price = 0 AND close_price = 0 AND high_price = 0 AND low_price = 0)
          AND NOT (open_price = close_price AND close_price = high_price AND high_price = low_price)
          {where_extra}
        GROUP BY stock_code
        ORDER BY zero_cnt DESC
    """
    logger.info("开始诊断（聚合SQL）...")
    cursor.execute(sql, params)
    rows = cursor.fetchall()

    # 获取总股票数
    if stock_codes:
        total_stocks = len(stock_codes)
    else:
        cursor.execute(f"SELECT COUNT(DISTINCT stock_code) FROM {TABLE_NAME}")
        total_stocks = cursor.fetchone()[0]

    affected = []
    total_zero = 0
    for r in rows:
        code, zero_cnt, first_d, last_d = r
        total_zero += zero_cnt
        affected.append({
            "stock_code": code,
            "zero_derived_count": zero_cnt,
            "first_zero_date": str(first_d),
            "last_zero_date": str(last_d),
        })

    cursor.close()
    conn.close()

    result = {
        "total_stocks": total_stocks,
        "affected_stock_count": len(affected),
        "total_zero_derived_records": total_zero,
        "affected_stocks": affected,
    }

    logger.info("═" * 60)
    logger.info("诊断完成")
    logger.info("  总股票数:        %d", result["total_stocks"])
    logger.info("  受影响股票数:    %d", result["affected_stock_count"])
    logger.info("  异常记录总数:    %d", result["total_zero_derived_records"])
    logger.info("═" * 60)

    if affected:
        logger.info("受影响个股明细（按异常记录数降序）:")
        for item in affected[:50]:
            logger.info("  %-12s  异常%4d条  范围: %s ~ %s",
                        item["stock_code"],
                        item["zero_derived_count"],
                        item["first_zero_date"],
                        item["last_zero_date"])
        if len(affected) > 50:
            logger.info("  ... 还有 %d 只股票未展示", len(affected) - 50)

    return result


# ─────────────────── 阶段2：修复 ───────────────────

async def repair_stock(stock_code: str) -> dict:
    """
    修复单只股票：重新拉取K线数据并写入DB（用于少量股票的精确修复）。
    返回 {"stock_code": str, "success": bool, "detail": str}
    """
    from common.utils.stock_info_utils import get_stock_info_by_code
    from service.jqka10.stock_day_kline_data_10jqka import get_stock_day_kline_10jqka

    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        return {"stock_code": stock_code, "success": False,
                "detail": f"无法获取 StockInfo（可能不在 stocks_data 中）"}

    try:
        klines = await get_stock_day_kline_10jqka(stock_info, limit=800)
    except Exception as e:
        return {"stock_code": stock_code, "success": False,
                "detail": f"拉取K线失败: {e}"}

    if not klines:
        return {"stock_code": stock_code, "success": False,
                "detail": "拉取到空数据"}

    _ALL_FIELDS = ("date", "open_price", "close_price", "high_price", "low_price", "trading_volume")
    clean = [k for k in klines if all(k.get(f) is not None and k.get(f) != "" for f in _ALL_FIELDS)]
    if not clean:
        return {"stock_code": stock_code, "success": False,
                "detail": "过滤后无有效数据"}

    try:
        save_kline_to_db(stock_code, clean)
    except Exception as e:
        return {"stock_code": stock_code, "success": False,
                "detail": f"写入DB失败: {e}"}

    issues = check_db(stock_code)
    zero_issues = [i for i in issues if i["type"] == "zero_derived"]
    if zero_issues:
        return {"stock_code": stock_code, "success": False,
                "detail": f"修复后仍有 {len(zero_issues)} 条 zero_derived 异常"}

    return {"stock_code": stock_code, "success": True,
            "detail": f"已修复，写入 {len(clean)} 条记录"}


def repair_all_by_sql() -> dict:
    """
    纯SQL批量修复：利用数据库中已有的 OHLC 价格，通过窗口函数
    用前一交易日的 close_price 重新计算 amplitude/change_percent/change_amount。
    无需重新拉取数据，速度极快。
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 先统计受影响行数
    cursor.execute(f"""
        SELECT COUNT(*) FROM {TABLE_NAME}
        WHERE amplitude = 0 AND change_percent = 0 AND change_amount = 0
          AND NOT (open_price = 0 AND close_price = 0 AND high_price = 0 AND low_price = 0)
          AND NOT (open_price = close_price AND close_price = high_price AND high_price = low_price)
    """)
    affected_rows = cursor.fetchone()[0]
    logger.info("待修复记录数: %d", affected_rows)

    if affected_rows == 0:
        cursor.close()
        conn.close()
        return {"affected_rows": 0, "updated_rows": 0}

    # 用窗口函数计算 prev_close，然后 UPDATE
    # MySQL 8.0+ 支持 LAG() 窗口函数
    update_sql = f"""
        UPDATE {TABLE_NAME} AS t
        INNER JOIN (
            SELECT id,
                   LAG(close_price) OVER (PARTITION BY stock_code ORDER BY `date`) AS prev_close,
                   high_price, low_price, close_price
            FROM {TABLE_NAME}
        ) AS calc ON t.id = calc.id
        SET
            t.amplitude      = IF(calc.prev_close > 0,
                                  ROUND((calc.high_price - calc.low_price) / calc.prev_close * 100, 2), 0),
            t.change_percent = IF(calc.prev_close > 0,
                                  ROUND((calc.close_price - calc.prev_close) / calc.prev_close * 100, 2), 0),
            t.change_amount  = IF(calc.prev_close > 0,
                                  ROUND(calc.close_price - calc.prev_close, 2), 0)
        WHERE t.amplitude = 0 AND t.change_percent = 0 AND t.change_amount = 0
          AND NOT (t.open_price = 0 AND t.close_price = 0 AND t.high_price = 0 AND t.low_price = 0)
          AND NOT (t.open_price = t.close_price AND t.close_price = t.high_price AND t.high_price = t.low_price)
          AND calc.prev_close IS NOT NULL AND calc.prev_close > 0
    """

    logger.info("执行SQL批量修复...")
    start = time.time()
    cursor.execute(update_sql)
    updated = cursor.rowcount
    conn.commit()
    elapsed = time.time() - start

    logger.info("SQL修复完成: 更新 %d 行，耗时 %.1fs", updated, elapsed)

    cursor.close()
    conn.close()

    return {"affected_rows": affected_rows, "updated_rows": updated}


async def repair_all_by_fetch(affected_stocks: list[dict], concurrency: int = 3) -> list[dict]:
    """批量修复：逐只股票重新拉取（用于SQL修复后仍有残留的情况）"""
    codes = [s["stock_code"] for s in affected_stocks]
    logger.info("开始逐只拉取修复 %d 只股票（并发=%d）...", len(codes), concurrency)

    semaphore = asyncio.Semaphore(concurrency)
    results = []
    success_count = 0
    fail_count = 0

    async def _do(code: str, idx: int):
        nonlocal success_count, fail_count
        async with semaphore:
            r = await repair_stock(code)
            results.append(r)
            if r["success"]:
                success_count += 1
                if idx % 20 == 0 or idx == len(codes):
                    logger.info("  [%d/%d] 进度更新 - 成功:%d 失败:%d",
                                idx, len(codes), success_count, fail_count)
            else:
                fail_count += 1
                logger.warning("  [%d/%d] %-12s ✗ %s", idx, len(codes), code, r["detail"])
            await asyncio.sleep(0.5)

    tasks = [_do(code, i + 1) for i, code in enumerate(codes)]
    await asyncio.gather(*tasks)

    logger.info("═" * 60)
    logger.info("拉取修复完成: 成功 %d, 失败 %d", success_count, fail_count)
    logger.info("═" * 60)

    return results


# ─────────────────── 阶段3：验证 ───────────────────

def verify_repair(stock_codes: list[str]) -> dict:
    """修复后重新诊断，确认问题已解决"""
    logger.info("开始修复后验证...")
    result = diagnose_all(stock_codes)
    if result["affected_stock_count"] == 0:
        logger.info("✓ 所有股票衍生字段已修复，无异常")
    else:
        logger.warning("✗ 仍有 %d 只股票存在 zero_derived 异常",
                       result["affected_stock_count"])
    return result


# ─────────────────── 主入口 ───────────────────

async def main():
    parser = argparse.ArgumentParser(description="K线衍生字段全零修复工具")
    parser.add_argument("--diagnose-only", action="store_true",
                        help="仅诊断，不执行修复")
    parser.add_argument("--stock", type=str, default=None,
                        help="只修复指定股票代码（如 301370.SZ）")
    parser.add_argument("--concurrency", type=int, default=3,
                        help="拉取修复并发数（默认3）")
    parser.add_argument("--output", type=str, default=None,
                        help="诊断结果输出到JSON文件")
    parser.add_argument("--fetch-only", action="store_true",
                        help="跳过SQL修复，直接逐只拉取修复")
    args = parser.parse_args()

    start_time = time.time()

    # 阶段1：诊断
    target_codes = [args.stock] if args.stock else None
    diag = diagnose_all(target_codes)

    # 过滤北交所个股
    diag["affected_stocks"] = [s for s in diag["affected_stocks"]
                                if not s["stock_code"].endswith('.BJ')]

    if args.output:
        Path(args.output).write_text(
            json.dumps(diag, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("诊断结果已写入: %s", args.output)

    if args.diagnose_only:
        logger.info("仅诊断模式，跳过修复。耗时 %.1fs", time.time() - start_time)
        return

    if not diag["affected_stocks"]:
        logger.info("无需修复。耗时 %.1fs", time.time() - start_time)
        return

    # 阶段2a：SQL批量修复（快速，利用已有OHLC数据计算衍生字段）
    if not args.fetch_only and not args.stock:
        logger.info("=" * 60)
        logger.info("阶段2a: SQL批量修复")
        logger.info("=" * 60)
        sql_result = repair_all_by_sql()
        logger.info("SQL修复结果: 待修复 %d 行, 实际更新 %d 行",
                    sql_result["affected_rows"], sql_result["updated_rows"])

        # 重新诊断看残留
        logger.info("SQL修复后重新诊断...")
        diag2 = diagnose_all(target_codes)

        if not diag2["affected_stocks"]:
            logger.info("✓ SQL修复已解决全部问题。总耗时 %.1fs", time.time() - start_time)
            return

        logger.info("SQL修复后仍有 %d 只股票存在异常，将逐只拉取修复...",
                    diag2["affected_stock_count"])
        remaining = diag2["affected_stocks"]
    else:
        remaining = diag["affected_stocks"]

    # 阶段2b：逐只拉取修复（处理SQL无法修复的情况，如第一条记录无prev_close）
    if args.stock:
        # 单只股票直接拉取修复
        r = await repair_stock(args.stock)
        if r["success"]:
            logger.info("✓ %s 修复成功: %s", args.stock, r["detail"])
        else:
            logger.warning("✗ %s 修复失败: %s", args.stock, r["detail"])
    elif remaining:
        logger.info("=" * 60)
        logger.info("阶段2b: 逐只拉取修复残留 %d 只股票", len(remaining))
        logger.info("=" * 60)
        await repair_all_by_fetch(remaining, args.concurrency)

    # 阶段3：最终验证
    logger.info("=" * 60)
    logger.info("阶段3: 最终验证")
    logger.info("=" * 60)
    final_codes = [args.stock] if args.stock else None
    verify_repair(final_codes)

    logger.info("总耗时: %.1fs", time.time() - start_time)


if __name__ == "__main__":
    asyncio.run(main())
