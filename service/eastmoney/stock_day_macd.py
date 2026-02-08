import asyncio
from datetime import datetime
import pandas as pd

from common.http.http_utils import EASTMONEY_PUSH2HIS_API_URL, fetch_eastmoney_api

def calculate_macd(klines, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    close_prices = []
    dates = []
    for kline in klines:
        fields = kline.split(',')
        dates.append(fields[0])
        close_prices.append(float(fields[2]))

    df = pd.DataFrame({'date': dates, 'close': close_prices})
    
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    
    df['macd'] = (ema_fast - ema_slow).round(4)
    df['macds'] = df['macd'].ewm(span=signal, adjust=False).mean().round(4)
    df['macdh'] = (2 * (df['macd'] - df['macds'])).round(4)
    
    df = df.tail(150)
    return df.to_dict('records')[::-1]

async def get_macd_markdown(secid, stock_code, stock_name):
    """将MACD数据转换为markdown格式"""
    klines = await get_stock_day_range_kline(secid, 200)
    macd_data = calculate_macd(klines)

    markdown = f"## <{stock_code} {stock_name}> - MACD数据\n\n"
    markdown += "| 日期 | 收盘价 | MACD | MACDS | MACDH |\n"
    markdown += "|------|--------|------|-------|-------|\n"
    for item in macd_data[:150]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item['macd']} | {item['macds']} | {item['macdh']} |\n"
    markdown += "\n"
    return markdown

async def get_stock_day_range_kline(secid="0.002371", limit=200):
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
    macd_data = await get_macd_markdown("0.002371", "002371", "北方华创")
    print(macd_data)

if __name__ == "__main__":
    asyncio.run(main())
