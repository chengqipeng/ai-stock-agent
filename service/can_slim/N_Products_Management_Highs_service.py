from common.utils.stock_info_utils import StockInfo
from service.llm.deepseek_client import DeepSeekClient
from service.eastmoney.indices.stock_market_data import get_stock_relative_strength
from service.eastmoney.stock_info.stock_holder_data import get_shareholder_increase_json
from service.eastmoney.stock_info.stock_revenue_analysis import get_revenue_analysis_three_years
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json, get_stock_history_volume_amount_yearly
from service.stock_search_news.can_slim.stock_search_result_filter_service import get_search_filter_result_dict


async def get_N_data(stock_info: StockInfo) -> dict:
    """
    获取N维度分析所需的所有数据
    
    Args:
        stock_info: 股票信息对象
    
    Returns:
        包含所有分析数据的字典
    """
    shareholder_increase_result = await get_shareholder_increase_json(stock_info)
    revenue_analysis_three_years = await get_revenue_analysis_three_years(stock_info)
    search_filter_result_dict = await get_search_filter_result_dict(stock_info)
    moving_averages_json = await get_moving_averages_json(stock_info)
    stock_history_volume_amount_yearly = await get_stock_history_volume_amount_yearly(stock_info)
    stock_relative_strength = await get_stock_relative_strength(stock_info)
    
    return {
        'shareholder_increase_result': shareholder_increase_result,
        'revenue_analysis_three_years': revenue_analysis_three_years,
        'announcements': search_filter_result_dict['announcements'],
        'finance_and_expectations': search_filter_result_dict['finance_and_expectations'],
        'corporate_governance': search_filter_result_dict['corporate_governance'],
        'stock_incentive_plan': search_filter_result_dict['stock_incentive_plan'],
        'moving_averages_json': moving_averages_json,
        'stock_history_volume_amount_yearly': stock_history_volume_amount_yearly,
        'stock_relative_strength': stock_relative_strength
    }

async def execute_N_Products_Management_Highs(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """
    执行N新产品/新管理层/新高点分析
    
    Args:
        stock_info: 股票信息对象
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    from common.prompt.can_slim.N_Products_Management_Highs_prompt import get_N_Products_Management_Highs_prompt
    
    data = await get_N_data(stock_info)
    prompt = get_N_Products_Management_Highs_prompt(data, stock_info)

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
