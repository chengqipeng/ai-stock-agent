import asyncio
import os
from datetime import datetime
from service.eastmoney.stock_structure_markdown import get_stock_markdown_for_llm_analyse
from service.eastmoney.stock_technical_markdown import get_technical_indicators_for_llm_analysis_prompt
from service.stock_new_analyse.stock_news_markdown import process_stock_news
from common.prompt.stock_final_prompt import get_final_prompt
from service.llm.deepseek_client import DeepSeekClient


async def stock_full_analysis(secid: str, stock_name: str):
    """完整股票分析流程"""
    stock_code = secid.split('.')[-1]
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    
    # 并行调用三个分析方法
    can_slim_result, technical_result, news_result = await asyncio.gather(
        get_stock_markdown_for_llm_analyse(secid, stock_name),
        get_technical_indicators_for_llm_analysis_prompt(secid, stock_code, stock_name),
        process_stock_news(stock_name)
    )
    
    # 保存结果到文件
    result_dir = "stock_full_result"
    os.makedirs(result_dir, exist_ok=True)
    
    with open(f"{result_dir}/can_slim_result_{stock_name}_{stock_code}_{timestamp}.md", "w", encoding="utf-8") as f:
        f.write(can_slim_result)
    
    with open(f"{result_dir}/technical_result_{stock_name}_{stock_code}_{timestamp}.md", "w", encoding="utf-8") as f:
        f.write(technical_result)
    
    with open(f"{result_dir}/news_result_{stock_name}_{stock_code}_{timestamp}.md", "w", encoding="utf-8") as f:
        f.write(news_result)
    
    # 生成最终提示词
    final_prompt = get_final_prompt(
        stock_code=stock_code,
        stock_name=stock_name,
        can_slim_conclusion=can_slim_result,
        technical_conclusion=technical_result,
        news_conclusion=news_result
    )
    
    with open(f"{result_dir}/final_prompt_{stock_name}_{stock_code}_{timestamp}.md", "w", encoding="utf-8") as f:
        f.write(final_prompt)
    
    # 调用DeepSeek获取最终结果
    client = DeepSeekClient()
    response = await client.chat(
        messages=[{"role": "user", "content": final_prompt}],
        model="deepseek-chat",
        temperature=0.7
    )
    
    final_analysis_result = response.get("choices", [{}])[0].get("message", {}).get("content", "")
    
    with open(f"{result_dir}/final_analysis_result_{stock_name}_{stock_code}_{timestamp}.md", "w", encoding="utf-8") as f:
        f.write(final_analysis_result)
    
    return final_analysis_result


if __name__ == "__main__":
    result = asyncio.run(stock_full_analysis("0.002371", "北方华创"))
    print(result)
