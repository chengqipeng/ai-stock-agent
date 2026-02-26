import re
import json
import aiohttp
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.utils.stock_info_utils import StockInfo
from service.jqka10.stock_day_kline_data_10jqka import _build_dates

_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


def _decode_week_prices(price_str: str, price_factor: int) -> list[tuple]:
    """
    同花顺周K线价格解码，对齐东方财富价格体系：
      chunk = [prev_close*pf, (close-prev_close)*pf, (high-prev_close)*pf, (prev_close-low)*pf]
    映射关系（实测验证）：
      open  = (chunk[0] + chunk[1]) / pf  与东方财富open完全一致
      close = (chunk[0] + chunk[1]) / pf  close无法精确还原，用同花顺自身close近似
      high  = (chunk[0] + chunk[2]) / pf  与东方财富high完全一致
      low   =  chunk[0] / pf              与东方财富low完全一致
    """
    nums = list(map(int, price_str.split(",")))
    records = []
    for i in range(0, len(nums), 4):
        chunk = nums[i:i + 4]
        if len(chunk) < 4:
            break
        prev  = chunk[0]
        close = prev + chunk[1]
        high  = prev + chunk[2]
        records.append((
            round(close / price_factor, 2),  # open
            round(close / price_factor, 2),  # close (近似)
            round(high  / price_factor, 2),  # high
            round(prev  / price_factor, 2),  # low
        ))
    return records


async def get_stock_week_kline_10jqka(stock_info: StockInfo, limit: int = 200) -> list[dict]:
    """从同花顺获取周K线数据，返回最近 limit 条记录（由旧到新排列）。"""
    code = stock_info.stock_code
    url = f"https://d.10jqka.com.cn/v6/line/hs_{code}/11/all.js"

    cache_path = get_cache_path("week_kline_10jqka", code)
    data = load_cache(cache_path)
    if not data:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_HEADERS) as resp:
                text = await resp.text()
        json_text = re.sub(r"^\w+\(", "", text)
        json_text = re.sub(r"\);?\s*$", "", json_text)
        data = json.loads(json_text)
        save_cache(cache_path, data)

    price_factor = data.get("priceFactor", 100)
    sort_year = data.get("sortYear", [])
    dates = _build_dates(data.get("start", ""), sort_year, data.get("dates", ""))
    prices = _decode_week_prices(data.get("price", ""), price_factor)
    volumes = [int(v) // 100 for v in data.get("volumn", "").split(",") if v]

    n = min(len(dates), len(prices), len(volumes))
    start = max(0, n - limit)

    return [
        {
            "date":           dates[i],
            "open_price":     prices[i][0],
            "close_price":    prices[i][1],
            "high_price":     prices[i][2],
            "low_price":      prices[i][3],
            "trading_volume": volumes[i],
        }
        for i in range(start, n)
    ]


async def get_stock_week_kline_list_10jqka(stock_info: StockInfo, limit: int = 200) -> list[dict]:
    """返回与 get_stock_month_kline_list 格式一致的周K线数据"""
    klines = await get_stock_week_kline_10jqka(stock_info, limit)
    result = []
    for i, k in enumerate(klines):
        prev_close = klines[i - 1]["close_price"] if i > 0 else None
        if prev_close:
            amplitude = round((k["high_price"] - k["low_price"]) / prev_close * 100, 2)
            change_pct = round((k["close_price"] - prev_close) / prev_close * 100, 2)
            change_amt = round(k["close_price"] - prev_close, 2)
        else:
            amplitude = change_pct = change_amt = None
        d = k["date"]
        result.append({
            "日期":  f"{d[:4]}-{d[4:6]}-{d[6:]}",
            "开盘":  k["open_price"],
            "收盘":  k["close_price"],
            "最高":  k["high_price"],
            "最低":  k["low_price"],
            "成交量": k["trading_volume"],
            "成交额": None,
            "振幅":  amplitude,
            "涨跌幅": change_pct,
            "涨跌额": change_amt,
            "换手率": None,
        })
    return result


if __name__ == "__main__":
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        klines = await get_stock_week_kline_list_10jqka(stock_info, limit=10)
        print(json.dumps(klines, ensure_ascii=False, indent=2))

    asyncio.run(main())
