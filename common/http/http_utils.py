import aiohttp
import json
import re
import random
import time
import uuid


# 东方财富API基础URL
EASTMONEY_API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_PUSH_API_URL = "https://push2delay.eastmoney.com/api/qt"
EASTMONEY_PUSH2_API_URL = "https://push2.eastmoney.com/api/qt"
EASTMONEY_PUSH2HIS_API_URL = "https://push2his.eastmoney.com/api/qt"
EASTMONEY_DATA_API_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
EASTMONEY_ZLSJ_API_URL = "https://data.eastmoney.com/dataapi/zlsj"


def get_dynamic_headers():
    """获取动态随机化的请求头"""
    # 动态生成User-Agent
    chrome_version = f"{random.randint(115, 122)}.0.{random.randint(0, 9999)}.{random.randint(0, 999)}"
    webkit_version = f"{random.randint(535, 540)}.{random.randint(1, 99)}"
    safari_version = f"{random.randint(535, 540)}.{random.randint(1, 99)}"
    firefox_version = f"{random.randint(115, 125)}.0"
    
    user_agents = [
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/{webkit_version} (KHTML, like Gecko) Chrome/{chrome_version} Safari/{safari_version}",
        f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{random.randint(14, 16)}_{random.randint(0, 7)}) AppleWebKit/{webkit_version} (KHTML, like Gecko) Chrome/{chrome_version} Safari/{safari_version}",
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{firefox_version}) Gecko/20100101 Firefox/{firefox_version}",
        f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/{webkit_version} (KHTML, like Gecko) Chrome/{chrome_version} Safari/{safari_version}"
    ]
    
    # 动态生成请求ID和时间戳
    request_id = str(uuid.uuid4())[:8]
    timestamp = str(int(time.time() * 1000))
    
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://datacenter.eastmoney.com/",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "X-Request-ID": request_id,
        "X-Timestamp": timestamp
    }
    
    # 随机添加可选头
    if random.choice([True, False]):
        headers["Accept-Encoding"] = "gzip, deflate"
    if random.choice([True, False]):
        headers["DNT"] = "1"
    
    return headers

def get_default_headers():
    """兼容性方法，调用动态头生成"""
    return get_dynamic_headers()


def clean_jsonp_response(text):
    """清理JSONP响应包装"""
    json_text = re.sub(r'^\w+\(', '', text)
    json_text = re.sub(r'\);?$', '', json_text)
    return json_text


async def fetch_eastmoney_api(url, params, headers=None, referer=None):
    """通用的东方财富API请求方法"""
    if headers is None:
        headers = get_dynamic_headers()
    if referer:
        headers = headers.copy()
        headers["Referer"] = referer
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()
            json_text = clean_jsonp_response(text)
            return json.loads(json_text)
