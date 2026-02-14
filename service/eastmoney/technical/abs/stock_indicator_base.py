from datetime import datetime
import pandas as pd
from common.http.http_utils import EASTMONEY_PUSH2HIS_API_URL, fetch_eastmoney_api
from common.utils.amount_utils import convert_amount_unit
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name

# 指标数据配置
INDICATOR_CONFIG = {
    # 'boll': {'kline_limit': 300, 'tail_limit': 300, 'markdown_limit': 250},
    # 'macd': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 365},
    # 'rsi': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 365},
    # 'ma': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 200},
    # 'vwma': {'kline_limit': 200, 'tail_limit': 200, 'markdown_limit': 60}
    'boll': {'kline_limit': 300, 'tail_limit': 300, 'markdown_limit': 200},
    'macd': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 200},
    'rsi': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 200},
    'ma': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 200},
    'atr': {'kline_limit': 200, 'tail_limit': 200, 'markdown_limit': 100},
    'vwma': {'kline_limit': 200, 'tail_limit': 200, 'markdown_limit': 60}
}

async def get_stock_day_range_kline(stock_info: StockInfo, limit=400):
    """获取股票K线数据"""
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
    return result.get('data', {}).get('klines', [])

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

async def get_stock_history_kline_max_min(stock_info: StockInfo):
    klines = await get_stock_day_range_kline(stock_info)
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


def parse_klines_to_df(klines):
    """解析K线数据为DataFrame"""
    data = [_parse_kline_fields(kline) for kline in klines]
    return pd.DataFrame(data)

def process_indicator_data(df, indicator_type):
    """处理指标数据，应用tail限制并返回倒序记录"""
    config = INDICATOR_CONFIG.get(indicator_type, {'tail_limit': 400})
    return df.tail(config['tail_limit']).to_dict('records')[::-1]


if __name__ == "__main__":
    import asyncio
    import json


    async def main():
        stock_name = "北方华创"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)
        # 测试 JSON 格式
        result = await get_stock_day_range_kline(stock_info)
        print("股东增减持数据 (JSON格式):")
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())