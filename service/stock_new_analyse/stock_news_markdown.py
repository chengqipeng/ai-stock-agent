import asyncio
import json
from common.prompt.stock_news_keyword_prompt import get_news_keyword_prompt
from common.prompt.stock_news_prompt import get_news_prompt
from service.web_search.baidu_search import baidu_search
from service.web_search.google_search import google_search
from service.llm.deepseek_client import DeepSeekClient


async def process_stock_news(company_name: str):
    client = DeepSeekClient()
    
    keyword_prompt = get_news_keyword_prompt(company_name)
    keyword_response = await client.chat(messages=[{"role": "user", "content": keyword_prompt}])
    keyword_result = keyword_response["choices"][0]["message"]["content"]

    print(keyword_result)
    
    # 清理可能的markdown代码块标记
    keyword_result = keyword_result.strip()
    if keyword_result.startswith("```json"):
        keyword_result = keyword_result[7:]
    if keyword_result.startswith("```"):
        keyword_result = keyword_result[3:]
    if keyword_result.endswith("```"):
        keyword_result = keyword_result[:-3]
    keyword_result = keyword_result.strip()
    
    data = json.loads(keyword_result)
    
    tasks = []
    for query in data.get("get_news", []):
        tasks.append(baidu_search(query))
    
    for query in data.get("get_global_news", []):
        tasks.append(google_search(query))
    
    search_results = await asyncio.gather(*tasks)
    
    # 将二维数组扁平化为一维数组
    flattened_results = [item for sublist in search_results for item in sublist]
    
    news_prompt = get_news_prompt(company_name, json.dumps(flattened_results, ensure_ascii=False))
    final_response = await client.chat(messages=[{"role": "user", "content": news_prompt}])
    final_result = final_response["choices"][0]["message"]["content"]

    return final_result


if __name__ == "__main__":
    result = asyncio.run(process_stock_news("三花智控"))
    print(result)
