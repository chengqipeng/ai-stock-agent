from common.prompt.can_slim.C_Quarterly_Earnings_prompt import get_C_Quarterly_Earnings_prompt
from common.utils.stock_info_utils import StockInfo
from service.llm.deepseek_client import DeepSeekClient
from service.eastmoney.forecast.stock_institution_forecast_list import get_institution_forecast_future_to_json, \
    get_institution_forecast_historical_to_json
from service.eastmoney.forecast.stock_institution_forecast_summary import get_institution_forecast_summary_future_json, \
    get_institution_forecast_summary_historical_json
from service.eastmoney.stock_info.stock_financial_main import get_financial_data_to_json


async def execute_C_Quarterly_Earnings(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """
    执行C季度盈利分析
    
    Args:
        stock_info: 股票信息对象
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    # 获取所有需要的数据
    financial_revenue = await get_financial_data_to_json(
        stock_info=stock_info, 
        indicator_keys=['REPORT_DATE', 'TOTALOPERATEREVETZ', 'SINGLE_QUARTER_REVENUE', 'TOTALOPERATEREVE', 'SINGLE_QUARTER_REVENUETZ']
    )
    financial_profit = await get_financial_data_to_json(
        stock_info=stock_info, 
        indicator_keys=['REPORT_DATE', 'SINGLE_QUARTER_PARENTNETPROFITTZ', 'SINGLE_QUARTER_KCFJCXSYJLRTZ']
    )
    financial_eps = await get_financial_data_to_json(
        stock_info=stock_info, 
        indicator_keys=['REPORT_DATE', 'EPSJB']
    )
    historical_forecast_json = await get_institution_forecast_historical_to_json(stock_info=stock_info)
    future_forecast_json = await get_institution_forecast_future_to_json(stock_info=stock_info)
    historical_forecast_summary = await get_institution_forecast_summary_historical_json(stock_info=stock_info)
    future_forecast_summary = await get_institution_forecast_summary_future_json(stock_info=stock_info)
    
    # 组装数据字典
    data = {
        'financial_revenue': financial_revenue,
        'financial_profit': financial_profit,
        'financial_eps': financial_eps,
        'historical_forecast_json': historical_forecast_json,
        'future_forecast_json': future_forecast_json,
        'historical_forecast_summary': historical_forecast_summary,
        'future_forecast_summary': future_forecast_summary
    }
    
    prompt = await get_C_Quarterly_Earnings_prompt(data, stock_info)

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
