import asyncio
import json
from datetime import datetime
from service.stock_news.can_slim.stock_search_key_word_service import get_search_key_result
from service.web_search.baidu_search import baidu_search
from service.web_search.google_search import google_search


async def research_stock_news(secucode="002371.SZ", stock_name=None):
    """获取搜索关键词并执行搜索"""
    search_key_result = await get_search_key_result(secucode, stock_name)
    
    semaphore = asyncio.Semaphore(6)
    
    async def search_for_category(category_data):
        async with semaphore:
            all_results = []
            for keyword in category_data['search_keys']:
                if category_data['type'] == 'domestic':
                    news = await baidu_search(keyword, days=category_data['search_key_time_range'])
                else:
                    news = await baidu_search(keyword, category_data['search_key_time_range'])
                all_results.extend(news)
            
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
                try:
                    date_str = result.get("date", "")
                    result_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    if (current_date - result_date).days <= category_data['search_key_time_range']:
                        filtered.append(result)
                except (ValueError, TypeError):
                    continue
            
            return {
                'category': category_data['category'],
                'intent': category_data['intent'],
                'type': category_data['type'],
                'search_results': filtered,
                'search_key_time_range': category_data['search_key_time_range']
            }
    
    tasks = [search_for_category(item) for item in search_key_result]
    results = await asyncio.gather(*tasks)
    
    # 重新分配ID
    id_counter = 1
    for item in results:
        for result in item['search_results']:
            result['id'] = id_counter
            id_counter += 1
    
    return results


if __name__ == "__main__":
    async def main():
        result = await research_stock_news("002371.SZ", "北方华创")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())
