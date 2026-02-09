import aiohttp
import asyncio
import base64
from typing import Optional
from datetime import datetime, timedelta

ACCESS_TOKEN = "YmNlLXYzL0FMVEFLLVdvR08wUlllWEFGWXRnU09jc1JRbS8wOGMxYTcyNzUzYWY1NWIzZTg3MTljOGJmMjA5YTc5MDdmOGIzZTNi"

def _decode_token(encoded: str) -> str:
    return base64.b64decode(encoded).decode('utf-8')

async def baidu_search(
    query: str
) -> dict:
    """使用百度千帆 AI Search 进行搜索"""
    url = "https://qianfan.baidubce.com/v2/ai_search/web_search"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_decode_token(ACCESS_TOKEN)}"
    }
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    payload = {
        "messages": [{"role": "user", "content": query}],
        "edition": "standard",
        "search_source": "baidu_search_v2",
        "resource_type_filter": [{"type": "web", "top_k": 5}],
        "search_recency_filter": "year",
        "search_filter": {
            "range": {
                "page_time": {
                    "gte": start_date.strftime("%Y-%m-%d"),
                    "lte": end_date.strftime("%Y-%m-%d")
                }
            }
        }
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            response.raise_for_status()
            result = await response.json()
            references = result.get('references', [])
            return [{
                'id': ref.get('id'),
                'url': ref.get('url'),
                'title': ref.get('title'),
                'date': ref.get('date'),
                'content': ref.get('content')
            } for ref in references]


if __name__ == "__main__":
    async def main():
        result = await baidu_search(
            query="三花智控 实际控制人 董事 减持计划 股权激励 2026-02"
        )
        print(result)
    
    asyncio.run(main())
