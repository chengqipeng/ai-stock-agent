import logging

import aiohttp
import asyncio
import base64
from datetime import datetime, timedelta

from service.web_search.web_scraper import extract_main_content, BUSINESS_TIMEOUT

logger = logging.getLogger(__name__)

ACCESS_TOKEN = "YmNlLXYzL0FMVEFLLVdvR08wUlllWEFGWXRnU09jc1JRbS8wOGMxYTcyNzUzYWY1NWIzZTg3MTljOGJmMjA5YTc5MDdmOGIzZTNi"

_semaphore = asyncio.Semaphore(18)

def _decode_token(encoded: str) -> str:
    return base64.b64decode(encoded).decode('utf-8')


async def baidu_search(
    query: str,
    days: int = 30,
    top_k: int = 5,
    preferred_domains: list[str] | None = None,
) -> dict:
    """使用百度千帆 AI Search 进行搜索

    Args:
        query: 搜索关键词
        days: 搜索时间范围（天）
        top_k: 返回结果数量
        preferred_domains: 优先域名列表，结果会按域名匹配度排序（匹配的排前面）
    """
    url = "https://qianfan.baidubce.com/v2/ai_search/web_search"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_decode_token(ACCESS_TOKEN)}"
    }

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    payload = {
        "messages": [{"role": "user", "content": query}],
        "edition": "standard",
        "search_source": "baidu_search_v2",
        "resource_type_filter": [{"type": "web", "top_k": top_k}],
        "search_recency_filter": "year",
        "search_filter": {
            "range": {
                "page_time": {
                    "gte": start_date.strftime("%Y-%m-%d"),
                    "lte": end_date.strftime("%Y-%m-%d")
                }
            },
            **({
                "match": {"site": preferred_domains}
            } if preferred_domains else {})
        }
    }

    async with _semaphore:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                response.raise_for_status()
                result = await response.json()
                references = result.get('references', [])
                results = [{
                    'id': ref.get('id'),
                    'url': ref.get('url'),
                    'title': ref.get('title'),
                    'date': ref.get('date'),
                    'content': ref.get('content')
                } for ref in references]

                semaphore = asyncio.Semaphore(len(results))

                async def fetch_content(item):
                    async with semaphore:
                        try:
                            text = await extract_main_content(item['url'], timeout=BUSINESS_TIMEOUT)
                            if text and len(text[:800]) > len(item.get('content') or ''):
                                item['content'] = text[:800]
                        except Exception as e:
                            logging.warning(f"fetch_content error for {item['url']}: {e}")
                    return item

                return list(await asyncio.gather(*[fetch_content(r) for r in results]))


if __name__ == "__main__":
    async def main():
        result = await baidu_search(
            query="三花智控"
        )
        logger.info(result)
    
    asyncio.run(main())
