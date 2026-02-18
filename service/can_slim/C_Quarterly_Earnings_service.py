from typing import Dict, Any

from common.prompt.can_slim.C_Quarterly_Earnings_prompt import C_QUARTERLY_EARNINGS_PROMPT_TEMPLATE
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.forecast.stock_institution_forecast_list import get_institution_forecast_future_to_json, get_institution_forecast_historical_to_json
from service.eastmoney.forecast.stock_institution_forecast_summary import get_institution_forecast_summary_future_json, get_institution_forecast_summary_historical_json
from service.eastmoney.stock_info.stock_financial_main import get_financial_data_to_json
from service.can_slim.base_can_slim_service import BaseCanSlimService

class CQuarterlyEarningsService(BaseCanSlimService):
    """C季度盈利分析服务"""
    
    async def collect_data(self) -> Dict[str, Any]:
        return {
            'financial_revenue': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'TOTALOPERATEREVETZ', 'SINGLE_QUARTER_REVENUE', 'TOTALOPERATEREVE', 'SINGLE_QUARTER_REVENUETZ']),
            'financial_profit': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'SINGLE_QUARTER_PARENTNETPROFITTZ', 'SINGLE_QUARTER_KCFJCXSYJLRTZ']),
            'financial_eps': await get_financial_data_to_json(self.stock_info, indicator_keys=['REPORT_DATE', 'EPSJB']),
            'historical_forecast': await get_institution_forecast_historical_to_json(stock_info=self.stock_info),
            'future_forecast': await get_institution_forecast_future_to_json(stock_info=self.stock_info),
            'historical_forecast_summary': await get_institution_forecast_summary_historical_json(stock_info=self.stock_info),
            'future_forecast_summary': await get_institution_forecast_summary_future_json(stock_info=self.stock_info)
        }
    
    def get_prompt_template(self) -> str:
        return C_QUARTERLY_EARNINGS_PROMPT_TEMPLATE
    
    def get_prompt_params(self) -> Dict[str, Any]:
        return {
            'financial_revenue_json': self.to_json(self.data_cache['financial_revenue']),
            'financial_profit_json': self.to_json(self.data_cache['financial_profit']),
            'financial_eps_json': self.to_json(self.data_cache['financial_eps']),
            'historical_forecast_json': self.to_json(self.data_cache['historical_forecast']),
            'future_forecast_json': self.to_json(self.data_cache['future_forecast']),
            'historical_forecast_summary': self.data_cache['historical_forecast_summary'],
            'future_forecast_summary': self.data_cache['future_forecast_summary']
        }


async def execute_C_Quarterly_Earnings(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """执行C季度盈利分析"""
    service = CQuarterlyEarningsService(stock_info)
    return await service.execute(deep_thinking)
