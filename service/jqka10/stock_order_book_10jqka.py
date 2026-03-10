"""
同花顺五档盘口数据模块

数据来源：https://d.10jqka.com.cn/v6/line/hs_{code}/01/defer/today.js
从同花顺实时交易数据接口中提取五档买卖盘口信息。
使用 aiohttp 请求 JSONP 接口，解析后返回标准化的盘口数据。
"""

import asyncio
import json
import logging
import re

import aiohttp

from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


async def get_order_book_10jqka(stock_info: StockInfo) -> dict | None:
    """
    从同花顺获取五档盘口数据。

    Returns:
        dict: {
            "stock_code": "002371",
            "stock_name": "...",
            "current_price": 18.50,
            "open_price": 18.30,
            "prev_close": 18.20,
            "high_price": 18.80,
            "low_price": 18.10,
            "volume": 123456,          # 成交量（手）
            "amount": "1.23亿",        # 成交额
            "buy1_price": ..., "buy1_vol": ...,
            ...
            "sell5_price": ..., "sell5_vol": ...,
        }
    """
    code = stock_info.stock_code
    url = f"https://d.10jqka.com.cn/v6/line/hs_{code}/01/defer/today.js"

    try:
        async with aiohttp.ClientSession(headers=_HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                text = await resp.text()

        match = re.search(r"\((\{.*\})\)", text, re.DOTALL)
        if not match:
            logger.warning("[%s] 盘口数据响应格式异常", code)
            return None

        data = json.loads(match.group(1))
        item = data.get(f"hs_{code}", {})
        if not item:
            logger.warning("[%s] 盘口数据响应中缺少 hs_%s", code, code)
            return None

        # 字段映射（同花顺 today.js 字段编号）
        # 7=开盘价, 8=最高, 9=最低, 11=当前价, 13=成交量(股), 19=成交额
        # 264648=买一价, 264652=买二价, 264656=买三价, 264660=买四价, 264664=买五价
        # 264649=买一量, 264653=买二量, 264657=买三量, 264661=买四量, 264665=买五量
        # 264650=卖一价, 264654=卖二价, 264658=卖三价, 264662=卖四价, 264666=卖五价
        # 264651=卖一量, 264655=卖二量, 264659=卖三量, 264663=卖四量, 264667=卖五量

        def _f(key, default=0.0):
            v = item.get(key, "")
            if not v or v == "--":
                return default
            try:
                return float(v)
            except (ValueError, TypeError):
                return default

        def _i(key, default=0):
            v = item.get(key, "")
            if not v or v == "--":
                return default
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return default

        result = {
            "stock_code": code,
            "stock_name": stock_info.stock_name,
            "current_price": _f("11"),
            "open_price": _f("7"),
            "prev_close": _f("6"),
            "high_price": _f("8"),
            "low_price": _f("9"),
            "volume": _i("13") // 100,  # 股→手
            "amount": item.get("19", ""),
            "buy1_price": _f("264648"), "buy1_vol": _i("264649"),
            "buy2_price": _f("264652"), "buy2_vol": _i("264653"),
            "buy3_price": _f("264656"), "buy3_vol": _i("264657"),
            "buy4_price": _f("264660"), "buy4_vol": _i("264661"),
            "buy5_price": _f("264664"), "buy5_vol": _i("264665"),
            "sell1_price": _f("264650"), "sell1_vol": _i("264651"),
            "sell2_price": _f("264654"), "sell2_vol": _i("264655"),
            "sell3_price": _f("264658"), "sell3_vol": _i("264659"),
            "sell4_price": _f("264662"), "sell4_vol": _i("264663"),
            "sell5_price": _f("264666"), "sell5_vol": _i("264667"),
        }
        return result

    except Exception as e:
        logger.error("[%s] 获取盘口数据异常: %s", code, e)
        return None


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        info = get_stock_info_by_name("生益电子")
        data = await get_order_book_10jqka(info)
        print(json.dumps(data, ensure_ascii=False, indent=2))

    asyncio.run(main())
