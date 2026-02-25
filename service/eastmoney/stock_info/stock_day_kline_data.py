import json
import time
import random
from datetime import datetime
from common.http.http_utils import EASTMONEY_PUSH2HIS_API_URL, fetch_eastmoney_api
from common.utils.amount_utils import convert_amount_unit
from common.utils.stock_info_utils import StockInfo
from common.utils.cache_utils import get_cache_path, load_cache, save_cache, get_market_cache_key
from service.auto_job.stock_history_klines_data import get_db_cache_kline_data

_session_sn = random.randint(30, 50)

# 已验证可用的真实设备指纹（固定不变，服务端已记录）
_DEVICE_COOKIE_BASE = (
    "qgqp_b_id=f4748f77325434072983eb6c8d3b1787;"
    " websitepoptg_api_time=1771929823568;"
    " st_nvi=mGKfIoG14uDZGoXVC5f25e1e4;"
    " nid18=0f512d6ee90e691d53d979bde12a1561;"
    " nid18_create_time=1771929823775;"
    " gviem=HLIMP8z85-dn3-VQzTHLLcfbb;"
    " gviem_create_time=1771929823775;"
    " fullscreengg=1; fullscreengg2=1;"
    " st_pvi=37471974443836;"
    " st_sp=2026-02-24%2018%3A43%3A43"
)


async def get_stock_day_range_kline(stock_info: StockInfo, limit=400):
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

    global _session_sn
    _session_sn += 1
    chrome_minor = random.randint(0, 5)
    user_agent = f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.{chrome_minor}.0 Safari/537.36"
    psi_base = f"{time.strftime('%Y%m%d%H%M%S', time.localtime())}{int(time.time()*1000)%1000:03d}-113200301201-{random.randint(10**9, 10**10 - 1)}"
    page_tags = ["hqzx.hsjAghqdy.dtt.lcKx", "hqzx.hsjBghqdy.dtt.lcKx", "datacenter.eastmoney"]
    st_asi = f"{psi_base}-{random.choice(page_tags)}-{random.randint(1, 5)}"
    cookie = (
        f"{_DEVICE_COOKIE_BASE};"
        f" st_si={random.randint(10**13, 10**14 - 1)};"
        f" st_sn={_session_sn};"
        f" st_psi={psi_base};"
        f" st_asi={st_asi}"
    )
    headers = {
        "User-Agent": user_agent,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/",
        "Sec-Fetch-Dest": "script",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-site",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "Cookie": cookie
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
    return await get_stock_day_range_kline(stock_info, limit)


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
        result = await get_stock_day_range_kline(stock_info)
        print(json.dumps(result[:2], ensure_ascii=False))

    asyncio.run(main())
