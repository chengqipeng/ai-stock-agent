import asyncio
import re
import json
import aiohttp

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


async def get_today_trade_data(stock_code: str) -> dict:
    """
    获取同花顺今日交易数据
    :param stock_code: 股票代码，如 '002371'
    :return: 解析后的今日交易数据字典
    """
    url = f"https://d.10jqka.com.cn/v6/line/hs_{stock_code}/01/defer/today.js"
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            text = await resp.text()

    # 响应格式: quotebridge_v6_line_hs_002371_01_defer_today({"data":...})
    match = re.search(r"\((\{.*\})\)", text, re.DOTALL)
    if not match:
        raise ValueError(f"Unexpected response format: {text[:200]}")

    return json.loads(match.group(1))


async def _get_prev_close(stock_code: str) -> float | None:
    """从同花顺分时接口获取昨收价"""
    url = f"https://d.10jqka.com.cn/v6/time/hs_{stock_code}/last.js"
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url) as resp:
            text = await resp.text()
    match = re.search(r"\((.+)\)", text, re.DOTALL)
    if not match:
        return None
    data = json.loads(match.group(1))
    pre = data.get(f"hs_{stock_code}", {}).get("pre")
    return float(pre) if pre else None


async def get_today_kline_as_str(stock_code: str) -> str | None:
    """
    获取今日实时K线，返回与 get_stock_day_range_kline 格式一致的逗号分隔字符串：
    date,open_price,close_price,high_price,low_price,trading_volume,trading_amount,amplitude,change_percent,change_amount,change_hand
    """
    raw, prev_close = await asyncio.gather(
        get_today_trade_data(stock_code),
        _get_prev_close(stock_code),
    )
    item = raw.get(f"hs_{stock_code}", {})
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
        print(data)

    asyncio.run(main())
