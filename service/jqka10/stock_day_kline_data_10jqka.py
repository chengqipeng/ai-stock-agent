import re
import json
import aiohttp
from datetime import date
from common.utils.stock_info_utils import StockInfo
from service.jqka10.stock_realtime_10jqka import get_today_trade_data

_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


def _build_dates(start: str, sort_year: list, dates_str: str) -> list[str]:
    """将 sortYear + dates 还原为完整日期列表 YYYYMMDD"""
    mmdd_list = dates_str.split(",")
    result = []
    idx = 0
    for year, count in sort_year:
        for _ in range(count):
            if idx >= len(mmdd_list):
                break
            result.append(f"{year}{mmdd_list[idx]}")
            idx += 1
    return result


def _decode_prices(price_str: str, price_factor: int) -> list[tuple]:
    """
    解码同花顺价格数据，每4个数字一组：
      [open*pf, (close-open)*pf, (high-close)*pf, (close-low)*pf]
    返回 (open, close, high, low) 列表，单位：元
    """
    nums = list(map(int, price_str.split(",")))
    records = []
    for i in range(0, len(nums), 4):
        chunk = nums[i:i + 4]
        if len(chunk) < 4:
            break
        open_p  = chunk[0] / price_factor
        close_p = open_p  + chunk[1] / price_factor
        high_p  = close_p + chunk[2] / price_factor
        low_p   = close_p - chunk[3] / price_factor
        records.append((
            round(open_p,  2),
            round(close_p, 2),
            round(high_p,  2),
            round(low_p,   2),
        ))
    return records


async def _get_today_kline(stock_code: str) -> dict | None:
    """从实时数据获取今日K线，若非交易日或数据不完整则返回 None"""
    try:
        raw = await get_today_trade_data(stock_code)
        item = raw.get(f"hs_{stock_code}", {})
        trade_date = item.get("1", "")
        close_p = item.get("11")
        if not trade_date or not close_p:
            return None
        volume = int(item.get("13", 0)) // 100  # 股 -> 手
        return {
            "date":           trade_date,
            "open_price":     float(item.get("7", close_p)),
            "close_price":    float(close_p),
            "high_price":     float(item.get("8", close_p)),
            "low_price":      float(item.get("9", close_p)),
            "trading_volume": volume,
            "change_hand":    float(item["1968584"]) if item.get("1968584") else None,
        }
    except Exception:
        return None


async def get_stock_day_kline_10jqka(stock_info: StockInfo, limit: int = 400) -> list[dict]:
    """
    从同花顺获取日K线数据，返回最近 limit 条记录（由旧到新排列）。

    每条记录包含：date, open_price, close_price, high_price, low_price, trading_volume（手）, change_hand（换手率）
    """
    market = "hs"
    code = stock_info.stock_code
    url = f"https://d.10jqka.com.cn/v6/line/{market}_{code}/01/all.js"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_HEADERS) as resp:
            text = await resp.text()

    json_text = re.sub(r"^\w+\(", "", text)
    json_text = re.sub(r"\);?\s*$", "", json_text)
    data = json.loads(json_text)

    price_factor = data.get("priceFactor", 100)
    sort_year = data.get("sortYear", [])
    dates_str = data.get("dates", "")
    price_str = data.get("price", "")
    volume_str = data.get("volumn", "")

    dates = _build_dates(data.get("start", ""), sort_year, dates_str)
    prices = _decode_prices(price_str, price_factor)
    volumes = [int(v) // 100 for v in volume_str.split(",") if v]  # 股 -> 手

    n = min(len(dates), len(prices), len(volumes))
    start = max(0, n - limit)

    result = []
    for i in range(start, n):
        open_p, close_p, high_p, low_p = prices[i]
        result.append({
            "date":           dates[i],
            "open_price":     open_p,
            "close_price":    close_p,
            "high_price":     high_p,
            "low_price":      low_p,
            "trading_volume": volumes[i],
            "change_hand":    None,
        })

    last_date = result[-1]["date"] if result else ""
    today_kline = await _get_today_kline(stock_info.stock_code)
    if today_kline and today_kline["date"] > last_date:
        result.append(today_kline)
        if len(result) > limit:
            result = result[-limit:]

    return result


if __name__ == "__main__":
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        while True:
            klines = await get_stock_day_kline_10jqka(stock_info, limit=300)
            print(json.dumps(klines, ensure_ascii=False, indent=2))
            await asyncio.sleep(1)

    asyncio.run(main())
