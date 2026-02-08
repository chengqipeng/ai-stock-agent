import aiohttp
import asyncio
from typing import List, Dict

SERPAPI_KEY_1 = "61ac5e704d09b61d45174c0d7ed881fa0658aaadc803f91912a8949634a132c3"
SERPAPI_KEY_2 = "4e90bb9bd6193fc6f42fe6b8cd9bd436f783e395c6cfe6853828788e3a70f99d"
SERPAPI_KEY_3 = "15c965c9fda857980ab742f2306bdd44349392055844143a20b5cc6f9b6625df"

async def google_search(
    query: str,
    num_results: int = 5
) -> List[Dict[str, any]]:
    """使用SerpAPI进行Google搜索，只返回organic_results中的position、title、link、snippet"""
    url = "https://serpapi.com/search.json"
    keys = [SERPAPI_KEY_1, SERPAPI_KEY_2, SERPAPI_KEY_3]
    
    for i, api_key in enumerate(keys[:2]):  # 只尝试前2个key
        params = {
            "engine": "google",
            "q": query,
            "api_key": api_key,
            "num": num_results,
            "hl": "en",
            "gl": "us"
        }

        try:
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
        except Exception as e:
            if i == 0:  # 第一次失败，切换key
                continue
            raise  # 第二次失败，抛出异常


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
