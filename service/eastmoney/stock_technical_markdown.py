import asyncio
from datetime import datetime

from common.prompt.stock_technical_indicator_simple_prompt import get_technical_prompt, get_technical_prompt_score
from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history_markdown
from service.eastmoney.technical.stock_day_atr import get_atr_markdown
from service.eastmoney.technical.stock_day_boll import get_boll_markdown
from service.eastmoney.technical.stock_day_macd import get_macd_markdown
from service.eastmoney.technical.stock_day_rsi import get_rsi_markdown
from service.eastmoney.technical.stock_day_vwma import get_vwma_markdown
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_markdown, \
    generate_can_slim_50_200_summary
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient

async def get_technical_indicators_markdown(secid, stock_code, stock_name):
    """汇总所有技术指标数据为markdown格式"""
    markdown = await get_fund_flow_history_markdown(secid)
    markdown += await get_boll_markdown(secid, stock_code, stock_name)
    markdown += await get_macd_markdown(secid, stock_code, stock_name)
    markdown += await get_rsi_markdown(secid, stock_code, stock_name)
    markdown += await get_vwma_markdown(secid, stock_code, stock_name)
    markdown += await get_atr_markdown(secid, stock_code, stock_name)
    markdown += await generate_can_slim_50_200_summary(secid, stock_code, stock_name)

    return markdown

async def get_technical_indicators_prompt(secid, stock_code, stock_name):
    """生成完整的技术分析prompt"""
    technical_data = await get_technical_indicators_markdown(secid, stock_code, stock_name)
    current_date = datetime.now().strftime("%Y年%m月%d日")
    return get_technical_prompt(current_date, stock_name, technical_data)

async def get_technical_indicators_prompt_score(secid, stock_code, stock_name):
    """生成完整的技术分析prompt"""
    technical_data = await get_technical_indicators_markdown(secid, stock_code, stock_name)
    current_date = datetime.now().strftime("%Y年%m月%d日")
    return get_technical_prompt_score(current_date, stock_name, technical_data)

async def get_technical_indicators_for_llm_analysis_prompt(secid, stock_code, stock_name, llm_type="deepseek"):
    """生成完整的技术分析prompt并调用LLM大模型"""
    try:
        technical_data = await get_technical_indicators_markdown(secid, stock_code, stock_name)
        current_date = datetime.now().strftime("%Y年%m月%d日")
        prompt = get_technical_prompt(current_date, stock_name, technical_data)
        
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
    result = await get_technical_indicators_prompt("0.002371", "002371", "北方华创")
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
