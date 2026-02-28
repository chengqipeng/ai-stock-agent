import re
import json
import asyncio
import aiohttp
from common.utils.stock_info_utils import StockInfo

_HEADERS = {
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


async def get_stock_time_kline_10jqka(stock_info: StockInfo, limit: int = None) -> list[dict]:
    """
    从同花顺获取当日分时数据，返回列表（由旧到新）。
    每条记录包含：time, price, avg_price, volume, change_percent
    """
    code = stock_info.stock_code
    url = f"https://d.10jqka.com.cn/v6/time/hs_{code}/defer/last.js"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_HEADERS) as resp:
            text = await resp.text()

    json_text = re.sub(r"^\w+\(", "", text)
    json_text = re.sub(r"\);?\s*$", "", json_text)
    data = json.loads(json_text)

    # 响应结构：{"hs_{code}": {"pre": "488.88", "data": "0930,481.12,38975531,481.120,81010;...", ...}}
    inner = data.get(f"hs_{code}", {})
    pre_close = float(inner.get("pre", 0) or 0)
    data_str = inner.get("data", "")

    result = []
    for row in data_str.split(";"):
        parts = row.split(",")
        if len(parts) < 5:
            continue
        price = float(parts[1])
        change_pct = round((price - pre_close) / pre_close * 100, 2) if pre_close else None
        result.append({
            "time":           f"{parts[0][:2]}:{parts[0][2:]}",
            "price":          price,
            "amount":         float(parts[2]),
            "avg_price":      float(parts[3]),
            "volume":         int(parts[4]),
            "change_percent": change_pct,
        })

    return result if limit is None else result[-limit:]


async def get_stock_time_kline_cn_10jqka(stock_info: StockInfo, limit: int = None) -> list[dict]:
    """获取当日分时数据，返回中文key列表"""
    rows = await get_stock_time_kline_10jqka(stock_info, limit)
    return [{
        "时间":   r["time"],
        "价格":   r["price"],
        "成交额":  r["amount"],
        "均价":   r["avg_price"],
        "成交量":  r["volume"],
        "涨跌幅":  r["change_percent"],
    } for r in rows]


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        klines = await get_stock_time_kline_cn_10jqka(stock_info)
        print(json.dumps(klines[:50], ensure_ascii=False))

    asyncio.run(main())
