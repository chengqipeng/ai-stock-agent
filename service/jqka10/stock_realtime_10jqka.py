import asyncio
import re
import json
import logging
import aiohttp

from common.constants.stocks_data import INDEX_CODES_FULL
from service.jqka10.stock_day_kline_data_10jqka import _jqka_symbol

logger = logging.getLogger(__name__)

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://stockpage.10jqka.com.cn/",
    "Sec-Fetch-Dest": "script",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}


async def get_today_trade_data(stock_code: str, market: str = "hs") -> dict:
    """
    获取同花顺今日交易数据
    :param stock_code: 股票代码，如 '002371'
    :param market: 市场前缀，'hs' 普通股票，'zs' 指数
    :return: 解析后的今日交易数据字典
    """
    url = f"https://d.10jqka.com.cn/v6/line/{market}_{stock_code}/01/defer/today.js"
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            text = await resp.text()

    # 响应格式: quotebridge_v6_line_{market}_{code}_01_defer_today({"data":...})
    match = re.search(r"\((\{.*\})\)", text, re.DOTALL)
    if not match:
        raise ValueError(f"Unexpected response format: {text[:200]}")

    data = json.loads(match.group(1))
    # 校验响应数据
    key = f"{market}_{stock_code}"
    if not data.get(key):
        logger.error("[%s] 实时交易数据响应中缺少 key=%s，data keys=%s", stock_code, key, list(data.keys()))
    else:
        item = data[key]
        missing = [f for f in ("1", "7", "8", "9", "11", "13") if not item.get(f)]
        if missing:
            logger.error("[%s] 实时交易数据存在空值字段 %s，item=%s", stock_code, missing, item)
    return data


async def _get_prev_close(stock_code: str, market: str = "hs") -> float | None:
    """从同花顺分时接口获取昨收价"""
    url = f"https://d.10jqka.com.cn/v6/time/{market}_{stock_code}/last.js"
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url) as resp:
            text = await resp.text()
    match = re.search(r"\((.+)\)", text, re.DOTALL)
    if not match:
        return None
    data = json.loads(match.group(1))
    pre = data.get(f"{market}_{stock_code}", {}).get("pre")
    return float(pre) if pre else None


def _resolve_symbol(stock_code_normalize: str) -> str:
    """
    根据标准化代码（如 '000300.SH'）返回同花顺实时接口使用的 symbol。
    复用 _jqka_symbol 的映射规则，保证与日K接口一致。
    """
    return _jqka_symbol(stock_code_normalize)


async def get_today_kline_as_str(stock_code: str, stock_code_normalize: str = None) -> str | None:
    """
    获取今日实时K线，返回与 get_stock_day_range_kline 格式一致的逗号分隔字符串：
    date,open_price,close_price,high_price,low_price,trading_volume,trading_amount,amplitude,change_percent,change_amount,change_hand

    :param stock_code: 纯数字代码，如 '002371'（向后兼容）
    :param stock_code_normalize: 标准化代码，如 '000300.SH'（指数需要此参数以正确映射 symbol）
    """
    # 确定 symbol 和 response key
    if stock_code_normalize and stock_code_normalize in INDEX_CODES_FULL:
        symbol = _resolve_symbol(stock_code_normalize)
    else:
        symbol = f"hs_{stock_code}"

    # 实时接口使用 symbol 构造 URL
    url_today = f"https://d.10jqka.com.cn/v6/line/{symbol}/01/defer/today.js"
    url_prev = f"https://d.10jqka.com.cn/v6/time/{symbol}/last.js"

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        resp_today, resp_prev = await asyncio.gather(
            session.get(url_today),
            session.get(url_prev),
        )
        resp_today.raise_for_status()
        text_today = await resp_today.text()
        text_prev = await resp_prev.text()

    # 解析 today
    match = re.search(r"\((\{.*\})\)", text_today, re.DOTALL)
    if not match:
        raise ValueError(f"Unexpected response format: {text_today[:200]}")
    today_data = json.loads(match.group(1))
    item = today_data.get(symbol, {})

    # 解析 prev_close
    prev_close = None
    match_prev = re.search(r"\((.+)\)", text_prev, re.DOTALL)
    if match_prev:
        prev_data = json.loads(match_prev.group(1))
        pre = prev_data.get(symbol, {}).get("pre")
        prev_close = float(pre) if pre else None

    if not item:
        logger.error("[%s] 实时K线数据为空：%s 不存在于响应中", stock_code, symbol)
    trade_date = item.get("1", "")
    if len(trade_date) == 8:
        trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    close_p = item.get("11")
    if not trade_date or not close_p:
        return None
    close_p = float(close_p)
    open_p = float(item.get("7", close_p))
    high_p = float(item.get("8", close_p))
    low_p = float(item.get("9", close_p))
    volume = int(item.get("13", 0)) // 100
    amount = item.get("19", "")
    change_hand = item.get("1968584", "")
    if prev_close:
        amplitude  = round((high_p - low_p) / prev_close * 100, 2)
        change_pct = round((close_p - prev_close) / prev_close * 100, 2)
        change_amt = round(close_p - prev_close, 2)
    else:
        amplitude = change_pct = change_amt = ""
    return ','.join(str(v) for v in (
        trade_date, open_p, close_p, high_p, low_p,
        volume, amount, amplitude, change_pct, change_amt, change_hand,
    ))


if __name__ == "__main__":
    import asyncio

    async def main():
        data = await get_today_kline_as_str("002371")
        logger.info(data)

    asyncio.run(main())
