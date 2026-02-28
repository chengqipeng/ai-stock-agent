import json
import time
from datetime import datetime
from common.http.http_utils import EASTMONEY_PUSH2HIS_API_URL, fetch_eastmoney_api
from common.utils.amount_utils import convert_amount_unit
from common.utils.stock_info_utils import StockInfo
from common.utils.cache_utils import get_cache_path, load_cache, save_cache, get_market_cache_key
from service.auto_job.stock_history_klines_data import get_db_cache_kline_data
from service.eastmoney.stock_info.headers.stock_day_kline_headers import (
    get_kline_header_builders, kline_headers_index
)
from service.jqka10.stock_day_kline_data_10jqka import get_stock_day_kline_10jqka, get_stock_day_kline_as_str_10jqka


async def get_stock_day_range_kline(stock_info: StockInfo, limit=400, headers=None):
    """获取股票日K线数据"""
    cache_path = get_cache_path(f"kline_{get_market_cache_key()}_{limit}", stock_info.stock_code)

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
        "smplmt": 460,
        "cb": "quote_jp1",
        "-": int(time.time() * 1000)
    }

    builders = get_kline_header_builders()
    if headers is not None:
        result = await fetch_eastmoney_api(url, params, headers)
        klines = result.get('data', {}).get('klines', [])
    else:
        start = kline_headers_index[0] % len(builders)
        kline_headers_index[0] += 1
        last_exc = None
        klines = []
        for i in range(len(builders)):
            try:
                result = await fetch_eastmoney_api(url, params, builders[(start + i) % len(builders)]())
                klines = result.get('data', {}).get('klines', [])
                last_exc = None
                break
            except Exception as e:
                last_exc = e
        if last_exc:
            raise last_exc

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


def _row_to_kline_str(row: dict) -> str:
    return ','.join(str(row[f]) for f in (
        'date', 'open_price', 'close_price', 'high_price', 'low_price',
        'trading_volume', 'trading_amount', 'amplitude', 'change_percent',
        'change_amount', 'change_hand'
    ))


async def get_stock_day_range_kline_by_db_cache(stock_info: StockInfo, limit=400) -> list[str]:
    """优先从DB缓存获取K线数据，无数据则回退到网络请求"""
    rows = get_db_cache_kline_data(stock_info.stock_code_normalize, limit=limit)
    if rows:
        return [_row_to_kline_str(r) for r in rows]
    try:
        return await get_stock_day_kline_as_str_10jqka(stock_info, limit)
    except Exception:
        return await get_stock_day_kline_as_str_10jqka(stock_info, limit)


async def get_stock_day_kline_cn(stock_info: StockInfo, limit=20) -> list[dict]:
    """获取K线数据，返回中文key，可指定条数"""
    klines = await get_stock_day_range_kline_by_db_cache(stock_info, limit=limit)
    result = []
    for kline in reversed(klines):
        fields = kline.split(',')
        result.append({
            '日期':       fields[0],
            '开盘价':     float(fields[1]),
            '收盘价':     float(fields[2]),
            '最高价':     float(fields[3]),
            '最低价':     float(fields[4]),
            '成交量（手）': float(fields[5]),
            '成交额':     fields[6],
            '振幅(%)':       float(fields[7]),
            '涨跌幅(%)':     float(fields[8]),
            '涨跌额':     float(fields[9]),
            '换手率(%)':     float(fields[10]),
        })
    return result


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

async def get_120day_high_to_latest_change(stock_info: StockInfo) -> dict:
    """计算120天内最高点到最近一次交易日的涨跌幅"""
    klines = await get_stock_day_range_kline_by_db_cache(stock_info, limit=120)
    if not klines:
        return {}
    high_price, high_date = None, None
    for kline in klines:
        fields = kline.split(',')
        high = float(fields[3])
        if high_price is None or high > high_price:
            high_price, high_date = high, fields[0]
    latest = klines[-1].split(',')
    latest_close = float(latest[2])
    latest_date = latest[0]
    change_amount = round(latest_close - high_price, 2)
    change_pct = round(change_amount / high_price * 100, 2)
    return {
        "120天最高价": high_price,
        "120天最高价日期": high_date,
        "最新收盘价": latest_close,
        "最新交易日": latest_date,
        "涨跌幅度": change_amount,
        "涨跌幅(%)": change_pct
    }


if __name__ == "__main__":
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name
    
    async def main():
        stock_name = "沪电股份"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)
        result = await get_120day_high_to_latest_change(stock_info)
        print(json.dumps(result, ensure_ascii=False))

    asyncio.run(main())
