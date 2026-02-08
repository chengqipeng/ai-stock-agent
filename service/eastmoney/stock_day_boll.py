import asyncio
from datetime import datetime
import pandas as pd

from common.http.http_utils import EASTMONEY_PUSH2HIS_API_URL, fetch_eastmoney_api

def calculate_bollinger_bands(klines, window=20, num_std=2):
    """计算布林线指标"""
    close_prices = []
    dates = []
    for kline in klines:
        fields = kline.split(',')
        dates.append(fields[0])
        close_prices.append(float(fields[2]))

    df = pd.DataFrame({'date': dates, 'close': close_prices})
    
    boll = df['close'].rolling(window=window).mean()
    rolling_std = df['close'].rolling(window=window).std()
    
    df['boll'] = boll.round(4)
    df['boll_ub'] = (boll + rolling_std * num_std).round(4)
    df['boll_lb'] = (boll - rolling_std * num_std).round(4)
    
    df = df.tail(200)
    return df.to_dict('records')[::-1]

async def get_boll_markdown(secid, stock_code, stock_name):
    """将布林线数据转换为markdown格式"""
    klines = await get_stock_day_range_kline(secid, 300)
    boll_data = calculate_bollinger_bands(klines)

    markdown = f"## <{stock_code} {stock_name}> - 布林线数据\n\n"
    markdown += "| 日期 | 收盘价 | BOLL | BOLL_UB | BOLL_LB |\n"
    markdown += "|------|--------|------|---------|---------|\\n"
    for item in boll_data[:250]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item['boll']} | {item['boll_ub']} | {item['boll_lb']} |\n"
    markdown += "\n"
    return markdown

async def get_stock_day_range_kline(secid="0.002371", limit=300):
    """获取股票K线数据"""
    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/kline/get"
    params = {
        "secid": secid,
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
    return klines

async def main():
    boll_data = await get_boll_markdown("0.002371", "002371", "北方华创")
    print(boll_data)

if __name__ == "__main__":
    asyncio.run(main())
