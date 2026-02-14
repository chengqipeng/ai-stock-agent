import asyncio
import json
from datetime import datetime, timedelta
from service.stock_news.can_slim.stock_search_key_word_service import get_search_key_result
from service.web_search.baidu_search import baidu_search
from service.web_search.google_search import google_search


async def research_stock_news(secucode="002371.SZ", stock_name=None):
    """获取搜索关键词并执行搜索"""
    # 获取搜索关键词
    search_key_result = await get_search_key_result(secucode, stock_name)
    search_data = json.loads(search_key_result)
    
    results = {
        "domestic_news": [],
        "global_news": []
    }
    
    semaphore = asyncio.Semaphore(6)
    tasks = []
    
    async def search_domestic(intent, keyword, time_range_days):
        async with semaphore:
            news = await baidu_search(keyword, days=time_range_days)
            for result in news:
                result["source"] = "domestic"
            return {"type": "domestic", "intent": intent, "keyword": keyword, "results": news}
    
    async def search_global(intent, keyword, time_range_days):
        async with semaphore:
            news = await baidu_search(keyword, time_range_days)
            for result in news:
                result["source"] = "global"
            return {"type": "global", "intent": intent, "keyword": keyword, "results": news}
    
    for item in search_data.get("search_news", []):
        intent = item.get("intent")
        time_range_days = item.get("search_key_time_range")
        for keyword in item.get("search_key", []):
            tasks.append(search_domestic(intent, keyword, time_range_days))
    
    for item in search_data.get("search_global_news", []):
        intent = item.get("intent")
        time_range_days = item.get("search_key_time_range")
        for keyword in item.get("search_key", []):
            tasks.append(search_global(intent, keyword, time_range_days))
    
    search_results = await asyncio.gather(*tasks)
    
    for result in search_results:
        if result["type"] == "domestic":
            results["domestic_news"].append({
                "intent": result["intent"],
                "keyword": result["keyword"],
                "results": result["results"]
            })
        else:
            results["global_news"].append({
                "intent": result["intent"],
                "keyword": result["keyword"],
                "results": result["results"]
            })
    
    # URL去重
    def deduplicate_by_url(news_list):
        seen_urls = set()
        for item in news_list:
            deduped = []
            for result in item["results"]:
                url = result.get("url")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    deduped.append(result)
            item["results"] = deduped
    
    deduplicate_by_url(results["domestic_news"])
    deduplicate_by_url(results["global_news"])
    
    # 过滤30天内的消息
    def filter_by_date(news_list):
        current_date = datetime.now()
        for item in news_list:
            filtered = []
            for result in item["results"]:
                try:
                    date_str = result.get("date", "")
                    result_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    if (current_date - result_date).days <= 30:
                        filtered.append(result)
                except (ValueError, TypeError):
                    continue
            item["results"] = filtered
    
    filter_by_date(results["domestic_news"])
    filter_by_date(results["global_news"])
    
    # 重新分配ID
    id_counter = 1
    for item in results["domestic_news"]:
        for result in item["results"]:
            result["id"] = id_counter
            id_counter += 1
    for item in results["global_news"]:
        for result in item["results"]:
            result["id"] = id_counter
            id_counter += 1
    
    return results


if __name__ == "__main__":
    async def main():
        result = await research_stock_news("002371.SZ", "北方华创")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())
