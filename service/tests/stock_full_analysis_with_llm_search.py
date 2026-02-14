import asyncio
from datetime import datetime

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.stock_structure_markdown import get_stock_markdown_with_llm_result
from service.eastmoney.stock_technical_markdown import get_technical_indicators_for_llm_analysis_prompt
from service.prompt.stock_final_prompt import get_final_prompt
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient
from service.stock_news.stock_news_markdown import process_stock_news


async def stock_full_analysis(stock_info: StockInfo, progress_callback=None, llm_type: str = "deepseek"):
    """完整股票分析流程"""

    # 并行调用三个分析方法，每个阶段发送开始和结束进度
    async def analyze_can_slim():
        if progress_callback:
            await progress_callback('progress', '基本面数据分析', 'start')
        result = await get_stock_markdown_with_llm_result(stock_info, llm_type=llm_type)
        if progress_callback:
            await progress_callback('progress', '基本面数据分析', 'done')
        return result
    
    async def analyze_technical():
        if progress_callback:
            await progress_callback('progress', '技术维度数据分析', 'start')
        result = await get_technical_indicators_for_llm_analysis_prompt(stock_info, llm_type=llm_type)
        if progress_callback:
            await progress_callback('progress', '技术维度数据分析', 'done')
        return result
    
    async def analyze_news():
        if progress_callback:
            await progress_callback('progress', '咨询数据分析', 'start')
        result = await process_stock_news(stock_info, llm_type=llm_type)
        if progress_callback:
            await progress_callback('progress', '咨询数据分析', 'done')
        return result
    
    can_slim_result, technical_result, news_result = await asyncio.gather(
        analyze_can_slim(),
        analyze_technical(),
        analyze_news()
    )
    
    # 生成最终提示词
    if progress_callback:
        await progress_callback('processing', '正在生成分析提示词')
    final_prompt = get_final_prompt(
        stock_info=stock_info,
        can_slim_conclusion=can_slim_result,
        technical_conclusion=technical_result,
        news_conclusion=news_result
    )
    
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
    
    return final_analysis_result


if __name__ == "__main__":
    stock_name = "北方华创"
    stock_info = get_stock_info_by_name(stock_name)
    result = asyncio.run(stock_full_analysis(stock_info))
    print(result)
