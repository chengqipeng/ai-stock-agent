"""
资金流向数据归一化修复工具

背景：_convert_em_klines_to_dicts 的字段映射已更新（归一化到同花顺语义），
但数据库中存量数据仍使用旧映射，不满足 net_flow = big_net + mid_net + small_net。

修复策略：
  阶段1 - 诊断：用SQL快速扫描 ff_flow_imbalance（net_flow ≠ big+mid+small）
  阶段2 - 修复：对问题股票用东方财富全量重拉（新映射），再用同花顺覆盖最近30条
  阶段3 - 验证：修复后重新检测

Usage:
    # 仅诊断
    python -m tools.repair_fund_flow --diagnose-only

    # 诊断 + 修复全部
    python -m tools.repair_fund_flow

    # 只修复指定股票
    python -m tools.repair_fund_flow --stock 300502.SZ

    # 调整并发（默认2，东方财富反爬较严）
    python -m tools.repair_fund_flow --concurrency 2
"""
import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dao import get_connection
from dao.stock_fund_flow_dao import (
    TABLE_NAME, check_fund_flow_db, batch_upsert_fund_flow,
)

_CST = ZoneInfo("Asia/Shanghai")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("repair_fund_flow")


# ─────────────────── 阶段1：诊断 ───────────────────

def diagnose_all(stock_codes: list[str] | None = None) -> list[str]:
    """
    扫描数据库，找出 net_flow ≠ big_net + mid_net + small_net 的股票。
    返回需要修复的股票代码列表。
    """
    conn = get_connection()
    cursor = conn.cursor()

    where_extra = ""
    params = []
    if stock_codes:
        placeholders = ",".join(["%s"] * len(stock_codes))
        where_extra = f"AND stock_code IN ({placeholders})"
        params = list(stock_codes)

    # 找出 net_flow 与 big+mid+small 差值超过 0.1 万元的记录
    sql = f"""
        SELECT stock_code,
               COUNT(*) AS imbalance_cnt
        FROM {TABLE_NAME}
        WHERE ABS(IFNULL(net_flow, 0) - (IFNULL(big_net, 0) + IFNULL(mid_net, 0) + IFNULL(small_net, 0))) > 0.1
          {where_extra}
        GROUP BY stock_code
        ORDER BY imbalance_cnt DESC
    """
    logger.info("开始诊断资金守恒异常...")
    cursor.execute(sql, params)
    rows = cursor.fetchall()

    # 总股票数
    if stock_codes:
        total = len(stock_codes)
    else:
        cursor.execute(f"SELECT COUNT(DISTINCT stock_code) FROM {TABLE_NAME}")
        total = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    affected_codes = []
    total_imbalance = 0
    for code, cnt in rows:
        affected_codes.append(code)
        total_imbalance += cnt

    logger.info("═" * 60)
    logger.info("诊断完成")
    logger.info("  总股票数:          %d", total)
    logger.info("  资金守恒异常股票:  %d", len(affected_codes))
    logger.info("  异常记录总数:      %d", total_imbalance)
    logger.info("═" * 60)

    if affected_codes and len(affected_codes) <= 30:
        for code, cnt in rows:
            logger.info("  %-12s  异常 %d 条", code, cnt)

    return affected_codes


# ─────────────────── 阶段2：修复 ───────────────────

def _clear_em_cache(stock_code: str):
    """清除东方财富资金流向的当日缓存，确保重新拉取"""
    from common.utils.cache_utils import get_cache_path
    cache_path = get_cache_path("fund_flow", stock_code)
    if os.path.exists(cache_path):
        os.remove(cache_path)


async def repair_stock(stock_code: str) -> dict:
    """
    修复单只股票：
    1. 清除东方财富缓存
    2. 东方财富全量拉取（~120条，新映射）
    3. 同花顺增量覆盖最近30条（数据更准确）
    4. 重新检测

    返回 {"stock_code": str, "success": bool, "detail": str}
    """
    from common.utils.stock_info_utils import get_stock_info_by_code
    from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history as get_em
    from service.jqka10.stock_history_fund_flow_10jqka import get_fund_flow_history as get_jqka
    from service.auto_job.fund_flow_scheduler import _convert_em_klines_to_dicts

    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        return {"stock_code": stock_code, "success": False,
                "detail": "无法获取 StockInfo"}

    # 1. 清除缓存
    _clear_em_cache(stock_info.stock_code)

    # 2. 东方财富全量拉取
    try:
        em_klines = await get_em(stock_info)
    except Exception as e:
        return {"stock_code": stock_code, "success": False,
                "detail": f"东方财富拉取失败: {e}"}

    if not em_klines:
        return {"stock_code": stock_code, "success": False,
                "detail": "东方财富返回空数据"}

    em_data = _convert_em_klines_to_dicts(em_klines)
    if not em_data:
        return {"stock_code": stock_code, "success": False,
                "detail": "东方财富数据转换后为空"}

    # 写入东方财富数据
    conn = get_connection()
    cursor = conn.cursor()
    try:
        batch_upsert_fund_flow(stock_code, em_data, cursor=cursor)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    # 3. 同花顺增量覆盖
    try:
        jqka_data = await get_jqka(stock_info)
        if jqka_data:
            conn = get_connection()
            cursor = conn.cursor()
            try:
                batch_upsert_fund_flow(stock_code, jqka_data, cursor=cursor)
                conn.commit()
            finally:
                cursor.close()
                conn.close()
    except Exception as e:
        logger.warning("[%s] 同花顺覆盖失败（不影响主修复）: %s", stock_code, e)

    # 4. 重新检测
    issues = check_fund_flow_db(stock_code)
    if issues:
        return {"stock_code": stock_code, "success": False,
                "detail": f"修复后仍有 {len(issues)} 条异常"}

    return {"stock_code": stock_code, "success": True,
            "detail": f"已修复，东方财富 {len(em_data)} 条"}


async def repair_all(codes: list[str], concurrency: int = 2) -> tuple[int, int]:
    """批量修复，返回 (成功数, 失败数)"""
    logger.info("开始批量修复 %d 只股票（并发=%d）...", len(codes), concurrency)
    sem = asyncio.Semaphore(concurrency)
    success = 0
    failed = 0

    async def _task(code: str, idx: int):
        nonlocal success, failed
        async with sem:
            r = await repair_stock(code)
            if r["success"]:
                success += 1
            else:
                failed += 1
                logger.warning("  [%d/%d] %-12s ✗ %s", idx, len(codes), code, r["detail"])
            if idx % 50 == 0:
                logger.info("  [%d/%d] 进度: 成功=%d 失败=%d", idx, len(codes), success, failed)
            await asyncio.sleep(1.0)  # 东方财富反爬，间隔稍长

    await asyncio.gather(*[_task(c, i + 1) for i, c in enumerate(codes)])

    logger.info("═" * 60)
    logger.info("批量修复完成: 成功 %d, 失败 %d", success, failed)
    logger.info("═" * 60)
    return success, failed


# ─────────────────── 主入口 ───────────────────

async def main():
    parser = argparse.ArgumentParser(description="资金流向数据归一化修复工具")
    parser.add_argument("--diagnose-only", action="store_true",
                        help="仅诊断，不执行修复")
    parser.add_argument("--stock", type=str, default=None,
                        help="只修复指定股票代码（逗号分隔）")
    parser.add_argument("--concurrency", type=int, default=2,
                        help="并发数（默认2）")
    parser.add_argument("--top", type=int, default=None,
                        help="只修复前N只异常股票")
    args = parser.parse_args()

    start = time.time()

    target = [c.strip() for c in args.stock.split(",")] if args.stock else None
    affected = diagnose_all(target)

    if args.diagnose_only:
        logger.info("仅诊断模式。耗时 %.1fs", time.time() - start)
        return

    if not affected:
        logger.info("无需修复。耗时 %.1fs", time.time() - start)
        return

    to_repair = affected[:args.top] if args.top else affected
    logger.info("将修复 %d 只股票", len(to_repair))

    ok, fail = await repair_all(to_repair, args.concurrency)

    # 最终验证
    logger.info("最终验证...")
    remaining = diagnose_all(to_repair)
    if not remaining:
        logger.info("✓ 全部修复完成")
    else:
        logger.warning("✗ 仍有 %d 只股票存在异常", len(remaining))

    logger.info("总耗时: %.1fs", time.time() - start)


if __name__ == "__main__":
    asyncio.run(main())
