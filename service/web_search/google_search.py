import aiohttp
import asyncio
import base64
from typing import List, Dict

SERPAPI_KEY_1 = "NjFhYzVlNzA0ZDA5YjYxZDQ1MTc0YzBkN2VkODgxZmEwNjU4YWFhZGM4MDNmOTE5MTJhODk0OTYzNGExMzJjMw=="
SERPAPI_KEY_2 = "NGU5MGJiOWJkNjE5M2ZjNmY0MmZlNmI4Y2Q5YmQ0MzZmNzgzZTM5NWM2Y2ZlNjg1MzgyODc4OGUzYTcwZjk5ZA=="
SERPAPI_KEY_3 = "MTVjOTY1YzlmZGE4NTc5ODBhYjc0MmYyMzA2YmRkNDQzNDkzOTIwNTU4NDQxNDNhMjBiNWNjNmY5YjY2MjVkZg=="
SERPAPI_KEY_4 = "MDRhOGU4OTNlODI1MDNiYThmMTY0ZWE4NWQ2MGVjZTBhYWRkMDVlZmUxY2Y4NGQzYmY2MmQ0NDdmZDg5MTBiYg=="
SERPAPI_KEY_5 = "MzVjYTY3MjJlYzAxZWJmNzRhN2ZkZDIwODZkNThiYzE2ZTRhN2ZkYzE2OGRmZjE1NWY1NWIxZTA2NmI0NzdkYQ=="

def _decode_key(encoded_key: str) -> str:
    return base64.b64decode(encoded_key).decode('utf-8')

async def google_search(
    query: str,
    num_results: int = 5,
    days = 90
) -> List[Dict[str, any]]:
    """使用SerpAPI进行Google搜索，只返回organic_results中的position、title、link、snippet"""
    url = "https://serpapi.com/search.json"
    keys = [_decode_key(SERPAPI_KEY_1), _decode_key(SERPAPI_KEY_2), _decode_key(SERPAPI_KEY_3), _decode_key(SERPAPI_KEY_4), _decode_key(SERPAPI_KEY_5)]
    
    for i, api_key in enumerate(keys[:5]):  # 只尝试前2个key
        params = {
            "engine": "google",
            "q": query,
            "api_key": api_key,
            "num": num_results,
            "tbm": "nws",
            "hl": "en",
            "gl": "us",
            "tbs": f"qdr:d{days}"  # 限制搜索时间范围
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
