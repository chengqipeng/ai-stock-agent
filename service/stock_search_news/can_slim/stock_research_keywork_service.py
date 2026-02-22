import asyncio
import json
import logging
from datetime import datetime

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.stock_search_news.can_slim.stock_search_key_word_service import get_search_key_result
from service.web_search.baidu_search import baidu_search

logger = logging.getLogger(__name__)

_semaphore = asyncio.Semaphore(20)

async def research_stock_news(stock_info: StockInfo):
    """获取搜索关键词并执行搜索"""
    search_key_result = await get_search_key_result(stock_info)

    keyword_cache = {}

    async def search_for_category(category_data):
        async def search_one(keyword):
            if keyword in keyword_cache:
                return keyword_cache[keyword]
            async with _semaphore:
                if category_data['type'] == 'domestic':
                    result = await baidu_search(keyword, days=category_data['search_key_time_range'])
                else:
                    result = await baidu_search(keyword, days=category_data['search_key_time_range'])
                keyword_cache[keyword] = result
                return result

        nested = await asyncio.gather(*[search_one(kw) for kw in category_data['search_keys']], return_exceptions=True)
        nested = [r if not isinstance(r, Exception) else (logger.error("search_one error: %s", r) or []) for r in nested]
        all_results = [item for sublist in nested if sublist for item in sublist]

        # URL去重
        seen_urls = set()
        deduped = []
        for result in all_results:
            url = result.get("url")
            if url and url not in seen_urls:
                seen_urls.add(url)
                deduped.append(result)

        # 过滤时间范围内的消息
        current_date = datetime.now()
        filtered = []
        for result in deduped:
            date_str = result.get("date", "")
            if not date_str:
                continue
            try:
                result_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                if (current_date - result_date).days <= category_data['search_key_time_range']:
                    filtered.append(result)
            except (ValueError, TypeError) as e:
                logging.error("Failed to parse date '%s': %s", date_str, e)
                continue

        return {
            'category': category_data['category'],
            'intent': category_data['intent'],
            'type': category_data['type'],
            'search_results': filtered,
            'search_key_time_range': category_data['search_key_time_range']
        }
    
    tasks = [search_for_category(item) for item in search_key_result]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    results = [r if not isinstance(r, Exception) else (logger.error("search_for_category error: %s", r) or {'category': '', 'intent': '', 'type': '', 'search_results': [], 'search_key_time_range': 0}) for r in results]
    
    # 重新分配ID
    id_counter = 1
    for item in results:
        for result in item['search_results']:
            result['id'] = id_counter
            id_counter += 1
    
    return results


if __name__ == "__main__":
    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        result = await research_stock_news(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())
