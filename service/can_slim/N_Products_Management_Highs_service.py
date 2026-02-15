import json
from datetime import datetime

from common.prompt.can_slim.N_Products_Management_Highs_prompt import N_PRODUCTS_MANAGEMENT_HIGHS_PROMPT_TEMPLATE
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.indices.stock_market_data import get_stock_relative_strength
from service.eastmoney.stock_info.stock_holder_data import get_shareholder_increase_json
from service.eastmoney.stock_info.stock_revenue_analysis import get_revenue_analysis_three_years
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json, get_stock_history_volume_amount_yearly
from service.llm.deepseek_client import DeepSeekClient
from service.stock_search_news.can_slim.stock_search_result_filter_service import get_search_filter_result_dict


async def execute_N_Products_Management_Highs(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """
    执行N新产品/新管理层/新高点分析
    
    Args:
        stock_info: 股票信息对象
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    shareholder_increase_result = await get_shareholder_increase_json(stock_info)
    revenue_analysis_three_years = await get_revenue_analysis_three_years(stock_info)
    search_filter_result_dict = await get_search_filter_result_dict(stock_info)
    moving_averages_json = await get_moving_averages_json(stock_info)
    stock_history_volume_amount_yearly = await get_stock_history_volume_amount_yearly(stock_info)
    stock_relative_strength = await get_stock_relative_strength(stock_info)
    
    prompt = N_PRODUCTS_MANAGEMENT_HIGHS_PROMPT_TEMPLATE.format(
        current_date=datetime.now().strftime('%Y-%m-%d'),
        stock_name=stock_info.stock_name,
        stock_code=stock_info.stock_code_normalize,
        announcements_json=json.dumps(search_filter_result_dict['announcements'], ensure_ascii=False, indent=2),
        finance_and_expectations_json=json.dumps(search_filter_result_dict['finance_and_expectations'], ensure_ascii=False, indent=2),
        revenue_analysis_json=json.dumps(revenue_analysis_three_years, ensure_ascii=False, indent=2),
        corporate_governance_json=json.dumps(search_filter_result_dict['corporate_governance'], ensure_ascii=False, indent=2),
        stock_incentive_plan_json=json.dumps(search_filter_result_dict['stock_incentive_plan'], ensure_ascii=False, indent=2),
        shareholder_increase_json=json.dumps(shareholder_increase_result, ensure_ascii=False, indent=2),
        moving_averages_json=json.dumps(moving_averages_json, ensure_ascii=False, indent=2),
        stock_relative_strength_json=json.dumps(stock_relative_strength, ensure_ascii=False, indent=2),
        stock_history_volume_json=json.dumps(stock_history_volume_amount_yearly, ensure_ascii=False, indent=2)
    )

    print(prompt)
    print("\n =============================== \n")
    
    model = "deepseek-reasoner" if deep_thinking else "deepseek-chat"
    client = DeepSeekClient()
    
    result = ""
    async for content in client.chat_stream(
        messages=[{"role": "user", "content": prompt}],
        model=model
    ):
        result += content

    return result
