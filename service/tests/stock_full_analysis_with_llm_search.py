import asyncio
import os
from datetime import datetime
from service.eastmoney.stock_structure_markdown import get_stock_markdown_with_llm_result
from service.eastmoney.stock_technical_markdown import get_technical_indicators_for_llm_analysis_prompt
from service.stock_news_result.stock_news_markdown import process_stock_news
from common.prompt.stock_final_prompt import get_final_prompt
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient


async def stock_full_analysis(secid: str, stock_name: str, progress_callback=None, llm_type: str = "deepseek"):
    """完整股票分析流程"""
    stock_code = secid.split('.')[-1]
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    
    # 并行调用三个分析方法，每个阶段发送开始和结束进度
    async def analyze_can_slim():
        if progress_callback:
            await progress_callback('progress', '基本面数据分析', 'start')
        result = await get_stock_markdown_with_llm_result(secid, stock_name, llm_type=llm_type)
        if progress_callback:
            await progress_callback('progress', '基本面数据分析', 'done')
        return result
    
    async def analyze_technical():
        if progress_callback:
            await progress_callback('progress', '技术维度数据分析', 'start')
        result = await get_technical_indicators_for_llm_analysis_prompt(secid, stock_code, stock_name, llm_type=llm_type)
        if progress_callback:
            await progress_callback('progress', '技术维度数据分析', 'done')
        return result
    
    async def analyze_news():
        if progress_callback:
            await progress_callback('progress', '咨询数据分析', 'start')
        result = await process_stock_news(stock_name, stock_code, llm_type=llm_type)
        if progress_callback:
            await progress_callback('progress', '咨询数据分析', 'done')
        return result
    
    can_slim_result, technical_result, news_result = await asyncio.gather(
        analyze_can_slim(),
        analyze_technical(),
        analyze_news()
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
    if progress_callback:
        await progress_callback('processing', '正在生成分析提示词')
    final_prompt = get_final_prompt(
        stock_code=stock_code,
        stock_name=stock_name,
        can_slim_conclusion=can_slim_result,
        technical_conclusion=technical_result,
        news_conclusion=news_result
    )
    
    with open(f"{result_dir}/final_prompt_{stock_name}_{stock_code}_{timestamp}.md", "w", encoding="utf-8") as f:
        f.write(final_prompt)
    
    # 调用LLM获取最终结果
    if llm_type == "gemini":
        if progress_callback:
            await progress_callback('analyzing', '正在调用大模型Gemini进行综合分析')
        client = GeminiClient()
        response = await client.chat(
            messages=[{"role": "user", "content": final_prompt}],
            model="gemini-3-pro-all",
            temperature=0.7
        )
    else:
        if progress_callback:
            await progress_callback('analyzing', '正在调用大模型DeepSeek进行综合分析')
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
