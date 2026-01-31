import aiohttp
import asyncio
import json
import re

async def fetch_star_market_stocks():
    """拉取科创板股票列表"""
    url = "https://yunhq.sse.com.cn:32042/v1/sh1/list/exchange/kshare"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.sse.com.cn/'
    }
    
    params = {
        'select': 'code,name',
        'begin': 0,
        'end': 1000
    }
    
    stocks = []
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                text = await response.text()
                
                data = json.loads(text)
                items = data.get('list', [])

                for item in items:
                    code = item[0]
                    name = item[1]
                    if code and name:
                        stocks.append({"code": f"{code}.SH", "name": name})
        
        print(f"成功获取 {len(stocks)} 只科创板股票")
        
        with open('star_market_stocks.json', 'w', encoding='utf-8') as f:
            json.dump(stocks, f, ensure_ascii=False, indent=2)
        
        return stocks
        
    except Exception as e:
        print(f"获取科创板数据失败: {e}")
        return []

if __name__ == '__main__':
    asyncio.run(fetch_star_market_stocks())
