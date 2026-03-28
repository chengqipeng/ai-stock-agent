"""
五档盘口数据模块（用于盘后调度入库）

数据来源：腾讯财经实时行情接口 qt.gtimg.cn
返回英文key格式，与 stock_order_book DAO 字段对齐。
包含五档买卖盘、外盘、内盘数据。
"""

import asyncio
import json
import logging
import re

import aiohttp

from common.utils.stock_info_utils import StockInfo
from common.utils.amount_utils import convert_amount_unit

logger = logging.getLogger(__name__)

_QT_URL = "https://qt.gtimg.cn/q="
_HEADERS = {
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/145.0.0.0 Safari/537.36",
}


def _build_qt_symbol(stock_info: StockInfo) -> str:
    """600519.SH → sh600519, 002371.SZ → sz002371"""
    code, market = stock_info.stock_code_normalize.split('.')
    prefix = 'sh' if market == 'SH' else 'sz'
    return f"{prefix}{code}"


def _parse_qt_fields(fields: list[str]) -> dict:
    """
    解析腾讯行情接口字段列表。

    腾讯字段索引（0-based，以 ~ 分隔）：
    0: 未知(51), 1: 名称, 2: 代码, 3: 当前价, 4: 昨收, 5: 今开
    6: 成交量(手), 7: 外盘(手), 8: 内盘(手)
    9,10: 买一价,买一量  11,12: 买二  13,14: 买三  15,16: 买四  17,18: 买五
    19,20: 卖一价,卖一量  21,22: 卖二  23,24: 卖三  25,26: 卖四  27,28: 卖五
    30: 时间(YYYYMMDDHHmmss), 31: 涨跌额, 32: 涨跌幅
    33: 最高, 34: 最低, 36: 成交量(手), 37: 成交额(万)
    38: 换手率
    """
    def _f(idx):
        try:
            return float(fields[idx])
        except (ValueError, TypeError, IndexError):
            return 0.0

    def _i(idx):
        try:
            return int(float(fields[idx]))
        except (ValueError, TypeError, IndexError):
            return 0

    # 解析交易日期: "20260327161412" → "2026-03-27"
    trade_date = ""
    if len(fields) > 30 and fields[30]:
        dt_str = fields[30].strip()
        if len(dt_str) >= 8:
            trade_date = f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]}"

    # 成交额：腾讯返回单位是万元，转换为元后再用 convert_amount_unit
    amount_wan = _f(37)
    amount_yuan = amount_wan * 10000

    result = {
        "current_price": _f(3),
        "open_price": _f(5),
        "prev_close": _f(4),
        "high_price": _f(33),
        "low_price": _f(34),
        "volume": _i(6),
        "amount": convert_amount_unit(amount_yuan),
        "outer_vol": _i(7),
        "inner_vol": _i(8),
    }

    if trade_date:
        result["_trade_date"] = trade_date

    # 五档买盘
    for i in range(5):
        n = i + 1
        result[f"buy{n}_price"] = _f(9 + i * 2)
        result[f"buy{n}_vol"] = _i(10 + i * 2)

    # 五档卖盘
    for i in range(5):
        n = i + 1
        result[f"sell{n}_price"] = _f(19 + i * 2)
        result[f"sell{n}_vol"] = _i(20 + i * 2)

    return result


async def get_order_book_10jqka(stock_info: StockInfo) -> dict | None:
    """
    获取五档盘口数据（含外盘、内盘），返回与 DAO upsert_order_book 对齐的英文key字典。

    Returns:
        dict: {
            "current_price": 452.98,
            "open_price": 439.50,
            "prev_close": 446.11,
            "high_price": 457.97,
            "low_price": 434.50,
            "volume": 58054,           # 成交量（手）
            "amount": "25.99亿",       # 成交额
            "outer_vol": 31020,        # 外盘（手）
            "inner_vol": 27034,        # 内盘（手）
            "buy1_price": ..., "buy1_vol": ...,
            ...
            "sell5_price": ..., "sell5_vol": ...,
            "_trade_date": "2026-03-27",
        }
    """
    symbol = _build_qt_symbol(stock_info)
    url = f"{_QT_URL}{symbol}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                text = await resp.text(encoding='gbk')

        # 响应格式: v_sz002371="51~北方华创~002371~452.98~...";
        if '="' not in text:
            logger.warning("[%s] 腾讯盘口接口返回异常: %s", stock_info.stock_code, text[:200])
            return None

        content = text.split('="')[1].rstrip('";').rstrip('"')
        fields = content.split('~')
        if len(fields) < 35:
            logger.warning("[%s] 腾讯盘口字段不足: %d", stock_info.stock_code, len(fields))
            return None

        return _parse_qt_fields(fields)

    except Exception as e:
        logger.error("[%s] 获取盘口数据异常: %s", stock_info.stock_code, e)
        return None


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        info = get_stock_info_by_name("生益电子")
        data = await get_order_book_10jqka(info)
        print(json.dumps(data, ensure_ascii=False, indent=2))

    asyncio.run(main())
