import aiohttp
import asyncio
from typing import Optional

ACCESS_TOKEN = "bce-v3/ALTAK-WoGO0RYeXAFYtgSOcsRQm/08c1a72753af55b3e8719c8bf209a7907f8b3e3b"

async def baidu_search(
    query: str,
    edition: str = "standard",
    search_source: str = "baidu_search_v2",
    search_recency_filter: Optional[str] = None
) -> dict:
    """使用百度千帆 AI Search 进行搜索"""
    url = "https://qianfan.baidubce.com/v2/ai_search/web_search"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    
    payload = {
        "messages": [{"role": "user", "content": query}],
        "edition": edition,
        "search_source": search_source
    }
    
    if search_recency_filter:
        payload["search_recency_filter"] = search_recency_filter
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            response.raise_for_status()
            return await response.json()


if __name__ == "__main__":
    async def main():
        result = await baidu_search(
            query="今天热点新闻",
            search_recency_filter="month"
        )
        print(result)
    
    asyncio.run(main())
