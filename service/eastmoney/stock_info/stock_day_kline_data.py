from datetime import datetime, timedelta
from common.http.http_utils import EASTMONEY_PUSH2HIS_API_URL, fetch_eastmoney_api
from common.utils.amount_utils import convert_amount_unit
from common.utils.stock_info_utils import StockInfo
from common.utils.cache_utils import get_cache_path, load_cache, save_cache


def _get_cache_date() -> str:
    today = datetime.now()
    offset = today.weekday() - 4
    if offset > 0:
        today -= timedelta(days=offset)
    return today.strftime("%Y%m%d")


async def get_stock_day_range_kline(stock_info: StockInfo, limit=400):
    """获取股票日K线数据"""
    cache_path = get_cache_path(f"kline_{_get_cache_date()}_{limit}", stock_info.stock_code)

    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data

    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/kline/get"
    params = {
        "secid": stock_info.secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "end": datetime.now().strftime("%Y%m%d"),
        "lmt": limit,
        "cb": "quote_jp1"
    }
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://quote.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    }
    result = await fetch_eastmoney_api(url, params, headers)
    klines = result.get('data', {}).get('klines', [])

    save_cache(cache_path, klines)
    return klines


def _parse_kline_fields(kline):
    """解析单条K线数据字段"""
    fields = kline.split(',')
    return {
        'date': fields[0],
        'close_price': float(fields[2]),
        'high_price': float(fields[3]),
        'low_price': float(fields[4]),
        'trading_volume': round(float(fields[5]) / 10000, 2),
        'trading_amount': convert_amount_unit(float(fields[6])),
        'change_hand': float(fields[10])
    }


async def get_stock_history_kline_max_min(stock_info: StockInfo, limit=400):
    klines = await get_stock_day_range_kline(stock_info, limit)
    result = {}
    for kline in klines:
        data = _parse_kline_fields(kline)
        result[data['date']] = {
            "close_price": data['close_price'],
            "high_price": data['high_price'],
            "low_price": data['low_price'],
            "change_hand": data['change_hand'],
            "trading_volume": data['trading_volume'],
            "trading_amount": data['trading_amount']
        }
    return result


async def get_stock_52week_high_low(stock_info: StockInfo):
    """获取52周内历史最高价和最低价及对应日期"""
    klines = await get_stock_day_range_kline(stock_info, limit=250)
    high_price, high_date, low_price, low_date = None, None, None, None
    for kline in klines:
        data = _parse_kline_fields(kline)
        if high_price is None or data['high_price'] > high_price:
            high_price, high_date = data['high_price'], data['date']
        if low_price is None or data['low_price'] < low_price:
            low_price, low_date = data['low_price'], data['date']
    return {
        "highest_price": high_price,
        "highest_date": high_date,
        "lowest_price": low_price,
        "lowest_date": low_date,
        "latest_date": _parse_kline_fields(klines[-1])['date'] if klines else None
    }

if __name__ == "__main__":
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name
    
    async def main():
        stock_name = "北方华创"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)
        result = await get_stock_52week_high_low(stock_info)
        print(f"{result['latest_date']}")

    asyncio.run(main())
