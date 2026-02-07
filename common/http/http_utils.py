import aiohttp
import json
import re


# 东方财富API基础URL
EASTMONEY_API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_PUSH_API_URL = "https://push2delay.eastmoney.com/api/qt"
EASTMONEY_PUSH2_API_URL = "https://push2.eastmoney.com/api/qt"
EASTMONEY_PUSH2HIS_API_URL = "https://push2his.eastmoney.com/api/qt"
EASTMONEY_DATA_API_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
EASTMONEY_ZLSJ_API_URL = "https://data.eastmoney.com/dataapi/zlsj"


def get_default_headers():
    """获取默认请求头"""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://datacenter.eastmoney.com/"
    }


def clean_jsonp_response(text):
    """清理JSONP响应包装"""
    json_text = re.sub(r'^\w+\(', '', text)
    json_text = re.sub(r'\);?$', '', json_text)
    return json_text


async def fetch_eastmoney_api(url, params, headers=None, referer=None):
    """通用的东方财富API请求方法"""
    if headers is None:
        headers = get_default_headers()
    if referer:
        headers = headers.copy()
        headers["Referer"] = referer
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()
            json_text = clean_jsonp_response(text)
            return json.loads(json_text)
