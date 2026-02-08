import aiohttp
import asyncio
from typing import List, Dict

SERPAPI_KEY = "61ac5e704d09b61d45174c0d7ed881fa0658aaadc803f91912a8949634a132c3"

async def google_search(
    query: str,
    num_results: int = 5
) -> List[Dict[str, any]]:
    """使用SerpAPI进行Google搜索，只返回organic_results中的position、title、link、snippet"""
    url = "https://serpapi.com/search.json"
    
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_KEY,
        "num": num_results,
        "hl": "en",
        "gl": "us"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            result = await response.json()
            
            organic_results = result.get('organic_results', [])
            return [{
                'id': item.get('position'),
                'title': item.get('title'),
                'url': item.get('link'),
                'content': item.get('snippet')
            } for item in organic_results]


if __name__ == "__main__":
    async def main():
        result = await google_search(
            query="销售易创始人"
        )
        for item in result:
            print(f"Position: {item['position']}")
            print(f"Title: {item['title']}")
            print(f"Link: {item['link']}")
            print(f"Snippet: {item['snippet']}")
            print("-" * 80)
    
    asyncio.run(main())
