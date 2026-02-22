import aiohttp
import asyncio
import base64
import json
import logging
from typing import List, Dict
from datetime import datetime
from pathlib import Path

from service.web_search.baidu_search import baidu_search
from service.web_search.web_scraper import extract_main_content

logger = logging.getLogger(__name__)

SERPAPI_KEY_1 = "NjFhYzVlNzA0ZDA5YjYxZDQ1MTc0YzBkN2VkODgxZmEwNjU4YWFhZGM4MDNmOTE5MTJhODk0OTYzNGExMzJjMw=="
SERPAPI_KEY_2 = "NGU5MGJiOWJkNjE5M2ZjNmY0MmZlNmI4Y2Q5YmQ0MzZmNzgzZTM5NWM2Y2ZlNjg1MzgyODc4OGUzYTcwZjk5ZA=="
SERPAPI_KEY_3 = "MTVjOTY1YzlmZGE4NTc5ODBhYjc0MmYyMzA2YmRkNDQzNDkzOTIwNTU4NDQxNDNhMjBiNWNjNmY5YjY2MjVkZg=="
SERPAPI_KEY_4 = "MDRhOGU4OTNlODI1MDNiYThmMTY0ZWE4NWQ2MGVjZTBhYWRkMDVlZmUxY2Y4NGQzYmY2MmQ0NDdmZDg5MTBiYg=="
SERPAPI_KEY_5 = "MzVjYTY3MjJlYzAxZWJmNzRhN2ZkZDIwODZkNThiYzE2ZTRhN2ZkYzE2OGRmZjE1NWY1NWIxZTA2NmI0NzdkYQ=="
SERPAPI_KEY_6 = "MmMwY2IyYmNjYzI2ZWQzMTFhNmI2Yjg0MjM2NDdmNTg4NzM1YjA2NDc4YTYyMzM1ODM4OWViNGE1ODBhMjM1ZA=="
SERPAPI_KEY_7 = "MjNjMTFlNDVkNTk0ZjM5MzEzYjE0OGQ4MzAyZjRjM2JlYWVkOTdiZTlhZTM2ZmZkYzkwY2E3ZWIzM2JjNjlhOQ=="
SERPAPI_KEY_8 = "MWQ3ODdjODdjMjkzYjNhZWU2OGE0ZjNiZmI5Nzg4NmIyNGZlOTQ5NmRmYmM2NWFhNWM1NjE4NGZhOTQ1OWM4NQ=="
SERPAPI_KEY_9 = "NjFiOGZhMWRjNDZlMzk0ZWM3OGEwMGEyYjE2MDIwZmRlM2NmN2QyYjI1MThlNGM3NWZiMmNjZjQ2MzkyZDI1MQ=="
SERPAPI_KEY_10 = "YzM0ODMzMTdlY2M0MzEyMThhMDk2N2Y2YWRmZDFhMGVjOTZkY2E1NWFhN2QxNmFjN2RjNmEzZWMwMTI5NTc4Yw=="
SERPAPI_KEY_11 = "NTkyOWYyNGIyYTViNDFhYjNhZjc2NDdjNTgxZmQ5MjIxMjQzNmNkYjliMTg2NzIzZTZiMDkyZDY3ZGU3MjY4Ng=="
SERPAPI_KEY_12 = "NTdkY2FhMGE0YTA4MTIyNzUwNDlhMzk3YzVjMTM4ZWExZmQ3ZGViNTlhNDM4N2U1YzU5N2FkMDg3ODE3MDliMg=="
SERPAPI_KEY_13 = "NDFjOThlMzhkMDRhYzc0OWYwY2NlODlhYzA0NTdjOTI4NjRmZTU4YTJhOWE1ZDlmMzIzMjk3ZTRlMzgyMjVjMw=="
SERPAPI_KEY_14 = "N2UyZTAyODBmZTQ3NDVjMmE1NTc3MjMyODExYTdmZmU2ZTM5YzFiNjZmNTlhMDkxZjg3YWFlMTcxNmUxMDI4MQ=="
SERPAPI_KEY_15 = "OTYxNjRjODhkYmUwMGUwZWU5NjE3MzQwYjExM2NlMDg0NGYwMjIyY2JhMTc5ZTBhMmQ3NDY5NmM5MGY2YTFhYw=="
SERPAPI_KEY_16 = "Yjc4YjBhMzljZDcwYmI3YTY3ZDJjYWY5Zjg4ZGNlNWNmYjllMGU3NmQ1Njk5MTJkYjAxYzBiZWY5MjQ1ZmNmYg=="
SERPAPI_KEY_17 = "MTNiY2E4NDI1N2YyYWY2NmQzZTY4ZjJiY2VlNjM3YzBlNDM2MjEwNjc4M2I1ZjIzYzBiNTBlMjQ0MzdhNzRjMw=="
SERPAPI_KEY_18 = "YTIzYTlhMTAzY2E5YTM3NTk3MGRmM2U1NDlhYjcwNzNiMzdlYWZkMzkxZGRmZDgyY2Q2YzhlMjE5OTkxOGE0OA=="
SERPAPI_KEY_19 = "YjE1NzU0ZWZhZjFlNjQ1N2U1ZGIzZDI5YWZjMTg5YTU0YzVlODI1MjI4NDM2YzZhYzIzYTQyNmFjNzE1ODNjYQ=="
SERPAPI_KEY_20 = "M2E2N2NmODEyOTRkMTcxN2JhYzNhMDRkNzYyZDJlMjJjOTBmZDMzZDQzOThhNGI4YjljZWZlYjYyZjM5ODQyZQ=="
SERPAPI_KEY_21 = "NzBlNDUxNzM2YTdkM2NkZGRhNjljODkwYTcwOWFkYmJhOGI1OTEyMWU1YThhZjViMjE0YzkzNmUxMmU2ZWIyYw=="

def _decode_key(encoded_key: str) -> str:
    return base64.b64decode(encoded_key).decode('utf-8')

def _get_cache_file() -> Path:
    cache_dir = Path(__file__).parent.parent.parent / '.cache'
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / 'serpapi_failed_keys.json'

def _load_failed_keys() -> Dict[str, List[int]]:
    cache_file = _get_cache_file()
    if not cache_file.exists():
        return {}
    try:
        with open(cache_file, 'r') as f:
            data = json.load(f)
            current_month = datetime.now().strftime('%Y-%m')
            return {k: v for k, v in data.items() if k == current_month}
    except:
        return {}

def _save_failed_keys(failed_keys: List[int]):
    cache_file = _get_cache_file()
    current_month = datetime.now().strftime('%Y-%m')
    data = _load_failed_keys()
    data[current_month] = failed_keys
    with open(cache_file, 'w') as f:
        json.dump(data, f)

def _get_current_month_failed_keys() -> List[int]:
    data = _load_failed_keys()
    current_month = datetime.now().strftime('%Y-%m')
    return data.get(current_month, [])

def _is_mobile_url(url: str) -> bool:
    from urllib.parse import urlparse
    host = urlparse(url).hostname or ''
    return host.startswith('wap.') or host.startswith('m.')

async def google_search(
    query: str,
    num_results: int = 20,
    days = 90
) -> List[Dict[str, any]]:
    """使用SerpAPI进行Google搜索，失败时自动切换到百度搜索"""
    url = "https://serpapi.com/search.json"
    keys = [_decode_key(SERPAPI_KEY_1), _decode_key(SERPAPI_KEY_2), _decode_key(SERPAPI_KEY_3), _decode_key(SERPAPI_KEY_4), _decode_key(SERPAPI_KEY_5), _decode_key(SERPAPI_KEY_6), _decode_key(SERPAPI_KEY_7), _decode_key(SERPAPI_KEY_8), _decode_key(SERPAPI_KEY_9), _decode_key(SERPAPI_KEY_10), _decode_key(SERPAPI_KEY_11), _decode_key(SERPAPI_KEY_12), _decode_key(SERPAPI_KEY_13), _decode_key(SERPAPI_KEY_14), _decode_key(SERPAPI_KEY_15), _decode_key(SERPAPI_KEY_16), _decode_key(SERPAPI_KEY_17), _decode_key(SERPAPI_KEY_18), _decode_key(SERPAPI_KEY_19), _decode_key(SERPAPI_KEY_20), _decode_key(SERPAPI_KEY_21)]
    
    failed_keys = _get_current_month_failed_keys()
    
    for i, key in enumerate(keys):
        if i in failed_keys:
            continue
            
        params = {
            "engine": "google",
            "q": query,
            "device": "desktop",
            "location": "United States",
            "api_key": key,
            "num": num_results,
            "hl": "en",
            "gl": "us",
            "tbm": "nws",
            "tbs": f"qdr:d{days}"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    response.raise_for_status()
                    result = await response.json()

                    items = result.get('news_results') or result.get('organic_results', [])
                    results =  [{
                        'id': item.get('position'),
                        'title': item.get('title'),
                        'url': item.get('link'),
                        'content': item.get('snippet')
                    } for item in items if not _is_mobile_url(item.get('link', ''))]

                    semaphore = asyncio.Semaphore(len(results))

                    async def fetch_content(item):
                        async with semaphore:
                            try:
                                text = await asyncio.to_thread(extract_main_content, item['url'])
                                if text:
                                    item['content'] = text[:800]
                            except Exception:
                                pass
                        return item

                    results = await asyncio.gather(*[fetch_content(r) for r in results])
                    return results
        except Exception as e:
            if "Too Many Requests" in str(e):
                failed_keys.append(i)
                _save_failed_keys(failed_keys)
                logger.warning(f"SerpAPI key {i} exhausted, trying next key: {e}")
                continue
            else:
                logger.error(f"SerpAPI error: {e}")
                break

    return await baidu_search(query, days)

if __name__ == "__main__":
    async def main():
        result = await google_search(
            query="北方华创"
        )
        for item in result:
            print(f"ID: {item['id']}")
            print(f"Title: {item['title']}")
            print(f"URL: {item['url']}")
            print(f"Content: {item['content']}")
            print("-" * 80)
    
    asyncio.run(main())
