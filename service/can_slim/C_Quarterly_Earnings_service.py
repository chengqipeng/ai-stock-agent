import json
from datetime import datetime

from common.prompt.can_slim.C_Quarterly_Earnings_prompt import C_QUARTERLY_EARNINGS_PROMPT_TEMPLATE
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
    
    prompt = C_QUARTERLY_EARNINGS_PROMPT_TEMPLATE.format(
        current_date=datetime.now().strftime('%Y-%m-%d'),
        stock_name=stock_info.stock_name,
        stock_code=stock_info.stock_code_normalize,
        financial_revenue_json=json.dumps(financial_revenue, ensure_ascii=False, indent=2),
        financial_profit_json=json.dumps(financial_profit, ensure_ascii=False, indent=2),
        financial_eps_json=json.dumps(financial_eps, ensure_ascii=False, indent=2),
        historical_forecast_json=json.dumps(historical_forecast_json, ensure_ascii=False, indent=2),
        future_forecast_json=json.dumps(future_forecast_json, ensure_ascii=False, indent=2),
        historical_forecast_summary=historical_forecast_summary,
        future_forecast_summary=future_forecast_summary
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
