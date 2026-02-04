from common.utils.amount_utils import convert_amount_unit
from .common_utils import EASTMONEY_PUSH_API_URL, clean_jsonp_response
import aiohttp
import json
import re


async def get_stock_realtime(secid="1.601698"):
    """
    获取股票实时数据
    secid格式: 市场代码.股票代码
    1 = 上海, 0 = 深圳
    """
    url = f"{EASTMONEY_PUSH_API_URL}/stock/get"

    params = {
        "fltt": "2",
        "invt": "2",
        "secid": secid,
        "fields": "f57,f58,f43,f47,f48,f168,f169,f170,f152",
        "ut": "b2884a393a59ad64002292a3e90d46a5"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()
            json_text = clean_jsonp_response(text)
            data = json.loads(json_text)

            if data.get("data"):
                stock_data = data["data"]
                return stock_data
            else:
                raise Exception(f"未获取到股票 {secid} 的实时数据")


def format_realtime_markdown(realtime_data):
    """格式化实时交易信息为markdown"""
    return f"""## 当日交易信息
- **股票代码**: {realtime_data.get('f57', '--')}
- **最新价**: {realtime_data.get('f43', '--')}
- **涨跌幅**: {realtime_data.get('f170', '--')}%
- **涨跌额**: {realtime_data.get('f169', '--')}
- **成交量**: {convert_amount_unit(realtime_data.get('f47'))}
- **成交额**: {convert_amount_unit(realtime_data.get('f48'))}
- **换手率**: {realtime_data.get('f168', '--')}%"""
