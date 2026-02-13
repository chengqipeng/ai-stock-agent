import asyncio
import json
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
    
    # 遍历国内搜索关键词，使用百度搜索
    for item in search_data.get("search_news", []):
        intent = item.get("intent")
        for keyword in item.get("search_key", []) + item.get("advanced_search_key", []):
            news = await baidu_search(keyword)
            results["domestic_news"].append({
                "intent": intent,
                "keyword": keyword,
                "results": news
            })
    
    # 遍历海外搜索关键词，使用谷歌搜索
    for item in search_data.get("search_global_news", []):
        intent = item.get("intent")
        for keyword in item.get("search_key", []) + item.get("advanced_search_key", []):
            news = await google_search(keyword)
            results["global_news"].append({
                "intent": intent,
                "keyword": keyword,
                "results": news
            })
    
    return results


if __name__ == "__main__":
    async def main():
        result = await research_stock_news("002371.SZ", "北方华创")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())
