from datetime import datetime
import pandas as pd
from common.http.http_utils import EASTMONEY_PUSH2HIS_API_URL, fetch_eastmoney_api

async def get_stock_day_range_kline(secid, limit=400):
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
    return result.get('data', {}).get('klines', [])

def parse_klines_to_df(klines):
    """解析K线数据为DataFrame"""
    dates, close_prices = [], []
    for kline in klines:
        fields = kline.split(',')
        dates.append(fields[0])
        close_prices.append(float(fields[2]))
    return pd.DataFrame({'date': dates, 'close': close_prices})

def generate_markdown_table(stock_code, stock_name, title, headers, data, limit=None):
    """生成markdown表格"""
    markdown = f"## <{stock_code} {stock_name}> - {title}\n\n"
    markdown += "| " + " | ".join(headers) + " |\n"
    markdown += "|" + "------|" * len(headers) + "\n"
    for item in (data[:limit] if limit else data):
        markdown += "| " + " | ".join(str(v) for v in item) + " |\n"
    markdown += "\n"
    return markdown
