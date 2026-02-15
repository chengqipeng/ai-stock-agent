import asyncio
import json
import os
from datetime import datetime

from common.utils.stock_info_utils import StockInfo
from common.prompt.stock_news_keyword_prompt import get_news_keyword_prompt
from common.prompt.stock_news_prompt import get_news_prompt
from service.web_search.baidu_search import baidu_search
from service.web_search.google_search import google_search
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient


async def process_stock_news(stock_info: StockInfo, llm_type: str = "deepseek"):
    try:
        if llm_type == "gemini":
            client = GeminiClient()
            model = "gemini-3-pro-all"
        else:
            client = DeepSeekClient()
            model = "deepseek-chat"
        
        keyword_prompt = get_news_keyword_prompt(stock_info)
        keyword_response = await client.chat(
            messages=[{"role": "user", "content": keyword_prompt}],
            model=model
        )
        keyword_result = keyword_response["choices"][0]["message"]["content"]

        print(keyword_result)
        
        # 保存搜索关键字到文件
        result_dir = "stock_full_result"
        os.makedirs(result_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        keyword_file = f"{result_dir}/news_keywords_{stock_info.stock_name}_{stock_info.stock_code}_{timestamp}.md"
        with open(keyword_file, "w", encoding="utf-8") as f:
            f.write(f"# {stock_info.stock_name} 搜索关键字\n\n")
            f.write(keyword_result)
        
        # 清理并提取JSON
        keyword_result = keyword_result.strip()
        # 解码HTML实体
        keyword_result = keyword_result.replace("&quot;", '"').replace("&amp;", "&")
        # 查找JSON代码块
        start = keyword_result.find("```json")
        if start != -1:
            keyword_result = keyword_result[start + 7:]
        elif keyword_result.startswith("```"):
            keyword_result = keyword_result[3:]
        end = keyword_result.rfind("```")
        if end != -1:
            keyword_result = keyword_result[:end]
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
        
        news_prompt = get_news_prompt(stock_info, json.dumps(flattened_results, ensure_ascii=False))
        final_response = await client.chat(
            messages=[{"role": "user", "content": news_prompt}],
            model=model
        )
        final_result = final_response["choices"][0]["message"]["content"]

        return final_result
    except Exception as e:
        print(f"process_stock_news error: {e}")
        raise Exception(f"资讯数据分析失败: {str(e)}")


if __name__ == "__main__":
    result = asyncio.run(process_stock_news("三花智控"))
    print(result)
