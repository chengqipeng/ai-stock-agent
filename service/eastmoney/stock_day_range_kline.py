import asyncio
from datetime import datetime
import pandas as pd

from common.http.http_utils import EASTMONEY_PUSH2HIS_API_URL, fetch_eastmoney_api

def calculate_moving_averages(klines):
    """计算移动平均线指标"""
    close_prices = []
    dates = []
    for kline in klines:
        fields = kline.split(',')
        dates.append(fields[0])
        close_prices.append(float(fields[2]))

    df = pd.DataFrame({'date': dates, 'close': close_prices})
    
    # 10日EMA - 使用adjust=True
    df['close_10_ema'] = df['close'].rolling(window=10).mean().round(2)
    
    # 50日SMA
    df['close_50_sma'] = df['close'].rolling(window=50).mean().round(2)
    
    # 200日SMA
    df['close_200_sma'] = df['close'].rolling(window=200).mean().round(2)
    
    # 多头排列判断
    df['is_bullish_alignment'] = (df['close_10_ema'] > df['close_50_sma']) & (df['close_50_sma'] > df['close_200_sma'])
    
    # 取时间倒序的前100条数据
    df = df.tail(200)
    return df.to_dict('records')[::-1]

async def get_moving_averages_markdown(secid, stock_code, stock_name):
    """将移动平均线数据转换为markdown格式"""
    klines = await get_stock_day_range_kline(secid)
    ma_data = calculate_moving_averages(klines)

    markdown = f"## <{stock_code} {stock_name}> - 移动平均线数据\n\n"
    markdown += "| 日期 | 收盘价 | 10日EMA | 50日SMA | 200日SMA | 多头排列 |\n"
    markdown += "|------|--------|---------|---------|----------|----------|\n"
    for item in ma_data[:100]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item.get('close_10_ema', 'N/A')} | {item.get('close_50_sma', 'N/A')} | {item.get('close_200_sma', 'N/A')} | {'是' if item.get('is_bullish_alignment') else '否'} |\n"
    markdown += "\n"
    return markdown

async def get_stock_day_range_kline(secid="0.002371", limit=400):
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
    klines = await get_stock_day_range_kline("0.002371", 400)
    print(klines)
    calculate_moving_averages(klines)

if __name__ == "__main__":
    asyncio.run(main())
