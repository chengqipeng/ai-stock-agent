"""
批量检测 stock_*.db 数据异常脚本

检测场景：
1. 价格逻辑错误：close_price <= 0 / open_price <= 0 / high_price <= 0 / low_price <= 0
2. 价格关系异常：high < low / close > high / close < low / open > high / open < low
3. 交易量/金额异常：trading_volume < 0 / trading_amount < 0
4. 涨跌幅异常：|change_percent| > 21%（A股单日涨跌幅上限，ST股10%，新股除外）
5. 日期格式异常：date 不符合 YYYY-MM-DD 格式
6. 日期重复：同一股票存在重复日期
7. 缺失交易日：相邻两条记录间存在未记录的交易日
8. 停牌占位记录异常：全零记录中存在非零字段
9. 数据库无表或表为空
10. 非交易日数据：date 对应非交易日（周末/节假日），立即删除并打印日志

发现异常时：调用 get_stock_day_kline_10jqka 重新拉取数据，重新检测，
若通过则覆盖写入数据库，否则输出日志。
"""

import sys
import asyncio
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dao.stock_kline_dao import (
    check_db, save_kline_to_db, get_all_stock_codes, logger
)
from service.jqka10.stock_day_kline_data_10jqka import get_stock_day_kline_10jqka
from common.utils.stock_info_utils import get_stock_info_by_code

log = logging.getLogger(__name__)

DB_DIR = Path(__file__).parent.parent.parent / "data_results/sql_lite"


def _get_all_kline_stock_codes() -> list[str]:
    """从 stock_kline 单表中查询所有不同的股票代码"""
    return get_all_stock_codes()


async def _repair_stock(stock_code_normalize: str, original_issues: list[dict]) -> None:
    """拉取最新数据，重新检测，通过则保存，否则记录日志"""
    log.info("[%s] 发现 %d 条异常，开始重新拉取数据...", stock_code_normalize, len(original_issues))
    try:
        stock_info = get_stock_info_by_code(stock_code_normalize)
    except Exception as e:
        log.error("[%s] 获取 StockInfo 失败: %s", stock_code_normalize, e)
        return

    try:
        klines = await get_stock_day_kline_10jqka(stock_info, limit=800)
    except Exception as e:
        log.error("[%s] 拉取 K 线数据失败: %s", stock_code_normalize, e)
        return

    if not klines:
        log.error("[%s] 拉取到空数据，跳过", stock_code_normalize)
        return

    # 校验每条K线所有字段不能为空，过滤掉有空值的记录
    _ALL_FIELDS = ("date", "open_price", "close_price", "high_price", "low_price",
                   "trading_volume", "trading_amount", "amplitude", "change_percent",
                   "change_amount", "change_hand")
    clean_klines = []
    for k in klines:
        empty_fields = [f for f in _ALL_FIELDS if k.get(f) is None or k.get(f) == ""]
        if empty_fields:
            log.error("[%s] K线数据存在空字段，date=%s, 空字段=%s，该条数据不存入数据库",
                      stock_code_normalize, k.get("date"), empty_fields)
        else:
            clean_klines.append(k)

    if not clean_klines:
        log.error("[%s] 过滤空字段后无有效数据，跳过写入", stock_code_normalize)
        return

    save_kline_to_db(stock_code_normalize, clean_klines)

    re_issues = check_db(stock_code_normalize)
    if not re_issues:
        log.info("[%s] 重新拉取后检测通过，数据已更新 ✓", stock_code_normalize)
    else:
        log.warning(
            "[%s] 重新拉取后仍有 %d 条异常，请人工核查：",
            stock_code_normalize, len(re_issues)
        )
        for iss in re_issues:
            log.warning("  [%s] 日期=%s  %s", iss["type"], iss["date"], iss["detail"])


async def main():
    stock_codes = _get_all_kline_stock_codes()
    if not stock_codes:
        log.warning("未找到任何 kline 表")
        return

    log.info("共发现 %d 只股票的K线表，开始检测...", len(stock_codes))

    total_issues = 0
    stocks_with_issues = 0

    for stock_code in stock_codes:
        issues = check_db(stock_code)
        if not issues:
            continue

        stocks_with_issues += 1
        total_issues += len(issues)

        legacy_issues = [i for i in issues if i.get("legacy")]
        active_issues = [i for i in issues if not i.get("legacy")]

        if active_issues:
            log.info("[%s] 发现 %d 条异常：", stock_code, len(active_issues))
            for iss in active_issues:
                log.info("  [%s] 日期=%s  %s", iss["type"], iss["date"], iss["detail"])
            await _repair_stock(stock_code, active_issues)

    log.info(
        "检测完成：共 %d 只股票，%d 只有异常，共 %d 条异常记录",
        len(stock_codes), stocks_with_issues, total_issues
    )


if __name__ == "__main__":
    asyncio.run(main())
