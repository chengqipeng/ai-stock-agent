"""
五档盘口数据模块（用于盘后调度入库）

数据来源：新浪财经实时行情接口 hq.sinajs.cn
返回英文key格式，与 stock_order_book DAO 字段对齐。

注意：同花顺 today.js 接口不包含五档盘口数据，因此改用新浪接口。
"""

import asyncio
import json
import logging
import re

import aiohttp

from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

_SINA_HQ_URL = "https://hq.sinajs.cn"


def _build_sina_symbol(stock_info: StockInfo) -> str:
    code, market = stock_info.stock_code_normalize.split('.')
    prefix = 'sh' if market == 'SH' else 'sz'
    return f"{prefix}{code}"


async def get_order_book_10jqka(stock_info: StockInfo) -> dict | None:
    """
    从新浪财经获取五档盘口数据，返回与 DAO upsert_order_book 对齐的英文key字典。

    Returns:
        dict: {
            "current_price": 18.50,
            "open_price": 18.30,
            "prev_close": 18.20,
            "high_price": 18.80,
            "low_price": 18.10,
            "volume": 123456,          # 成交量（手）
            "amount": "2.28亿",        # 成交额
            "buy1_price": ..., "buy1_vol": ...,
            ...
            "sell5_price": ..., "sell5_vol": ...,
        }
    """
    symbol = _build_sina_symbol(stock_info)
    url = f"{_SINA_HQ_URL}/list={symbol}"
    headers = {
        "Referer": "https://finance.sina.com.cn/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                text = await resp.text(encoding='gbk')

        match = re.search(r'"(.+)"', text)
        if not match:
            logger.warning("[%s] 新浪盘口接口返回异常: %s", stock_info.stock_code, text[:200])
            return None

        fields = match.group(1).split(',')
        if len(fields) < 32:
            logger.warning("[%s] 新浪盘口字段不足: %d", stock_info.stock_code, len(fields))
            return None

        return _parse_fields(fields)

    except Exception as e:
        logger.error("[%s] 获取盘口数据异常: %s", stock_info.stock_code, e)
        return None


def _parse_fields(fields: list[str]) -> dict:
    """
    解析新浪行情接口字段列表。

    新浪字段索引（0-based）：
    0: 股票名称, 1: 今开, 2: 昨收, 3: 当前价, 4: 最高, 5: 最低
    8: 成交量(股), 9: 成交额(元)
    10,11: 买一量,买一价  12,13: 买二  14,15: 买三  16,17: 买四  18,19: 买五
    20,21: 卖一量,卖一价  22,23: 卖二  24,25: 卖三  26,27: 卖四  28,29: 卖五
    """
    def _f(idx):
        try:
            return float(fields[idx])
        except (ValueError, TypeError, IndexError):
            return 0.0

    def _vol(idx):
        """成交量：股 → 手"""
        try:
            return int(float(fields[idx])) // 100
        except (ValueError, TypeError, IndexError):
            return 0

    from common.utils.amount_utils import convert_amount_unit
    amount_yuan = _f(9)

    result = {
        "current_price": _f(3),
        "open_price": _f(1),
        "prev_close": _f(2),
        "high_price": _f(4),
        "low_price": _f(5),
        "volume": _vol(8),
        "amount": convert_amount_unit(amount_yuan),
    }

    # 五档买盘：(量idx, 价idx)
    for i in range(5):
        n = i + 1
        result[f"buy{n}_price"] = _f(11 + i * 2)
        result[f"buy{n}_vol"] = _vol(10 + i * 2)

    # 五档卖盘
    for i in range(5):
        n = i + 1
        result[f"sell{n}_price"] = _f(21 + i * 2)
        result[f"sell{n}_vol"] = _vol(20 + i * 2)

    return result


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        info = get_stock_info_by_name("生益电子")
        data = await get_order_book_10jqka(info)
        print(json.dumps(data, ensure_ascii=False, indent=2))

    asyncio.run(main())
