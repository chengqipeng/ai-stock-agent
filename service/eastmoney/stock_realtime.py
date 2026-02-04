import aiohttp
import json
import re


async def get_stock_realtime(secid="1.601698"):
    """
    获取股票实时数据
    secid格式: 市场代码.股票代码
    1 = 上海, 0 = 深圳
    """
    url = "https://push2delay.eastmoney.com/api/qt/stock/get"

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

            # 移除 JSONP 回调函数包装
            json_text = re.sub(r'^jQuery\d+_\d+\(', '', text)
            json_text = re.sub(r'\)$', '', json_text)

            data = json.loads(json_text)

            if data.get("data"):
                stock_data = data["data"]

                return stock_data
            else:
                raise Exception(f"未获取到股票 {secid} 的实时数据")
