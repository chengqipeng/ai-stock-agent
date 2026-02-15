import asyncio
from datetime import datetime

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history_markdown
from service.eastmoney.technical.stock_day_atr import get_atr_markdown
from service.eastmoney.technical.stock_day_boll import get_boll_markdown
from service.eastmoney.technical.stock_day_macd import get_macd_markdown
from service.eastmoney.technical.stock_day_rsi import get_rsi_markdown
from service.eastmoney.technical.stock_day_vwma import get_vwma_markdown
from service.eastmoney.technical.stock_day_range_kline import generate_can_slim_50_200_summary
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient
from common.prompt.stock_technical_indicator_prompt import get_technical_prompt
from common.prompt.stock_technical_indicator_simple_prompt import get_technical_prompt_score


async def get_technical_indicators_markdown(stock_info: StockInfo):
    """汇总所有技术指标数据为markdown格式"""
    markdown = await get_fund_flow_history_markdown(stock_info)
    markdown += await get_boll_markdown(stock_info)
    markdown += await get_macd_markdown(stock_info)
    markdown += await get_rsi_markdown(stock_info)
    markdown += await get_vwma_markdown(stock_info)
    markdown += await get_atr_markdown(stock_info)
    markdown += await generate_can_slim_50_200_summary(stock_info)

    return markdown

async def get_technical_indicators_prompt(stock_info: StockInfo):
    """生成完整的技术分析prompt"""
    technical_data = await get_technical_indicators_markdown(stock_info)
    current_date = datetime.now().strftime("%Y年%m月%d日")
    return get_technical_prompt(current_date, stock_info, technical_data)

async def get_technical_indicators_prompt_score(stock_info: StockInfo):
    """生成完整的技术分析prompt"""
    technical_data = await get_technical_indicators_markdown(stock_info)
    current_date = datetime.now().strftime("%Y年%m月%d日")
    return get_technical_prompt_score(current_date, stock_info, technical_data)

async def get_technical_indicators_for_llm_analysis_prompt(stock_info: StockInfo, llm_type="deepseek"):
    """生成完整的技术分析prompt并调用LLM大模型"""
    try:
        technical_data = await get_technical_indicators_markdown(stock_info)
        current_date = datetime.now().strftime("%Y年%m月%d日")
        prompt = get_technical_prompt(current_date, stock_info, technical_data)
        
        if llm_type == "gemini":
            client = GeminiClient()
            response = await client.chat(
                messages=[{"role": "user", "content": prompt}],
                model="gemini-3-pro-all",
                temperature=0.7
            )
        else:
            client = DeepSeekClient()
            response = await client.chat(
                messages=[{"role": "user", "content": prompt}],
                model="deepseek-chat",
                temperature=0.7
            )
        
        return response.get("choices", [{}])[0].get("message", {}).get("content", "")
    except Exception as e:
        return f"# 错误\n\n技术分析失败: {str(e)}"

async def main():
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    result = await get_technical_indicators_prompt(stock_info)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
