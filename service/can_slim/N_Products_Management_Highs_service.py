from typing import Dict, Any

from common.prompt.can_slim.N_Products_Management_Highs_prompt import N_PRODUCTS_MANAGEMENT_HIGHS_PROMPT_TEMPLATE
from common.constants.can_slim_final_outputs import N_FINAL_OUTPUT
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.indices.stock_market_data import get_stock_relative_strength_cn
from service.eastmoney.stock_info.stock_holder_data import get_shareholder_increase_json
from service.eastmoney.stock_info.stock_revenue_analysis import get_revenue_analysis_three_years
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json_cn, get_stock_history_volume_amount_yearly
from service.stock_search_news.can_slim.stock_search_result_filter_service import get_search_filter_result_dict
from service.can_slim.base_can_slim_service import BaseCanSlimService


class NProductsManagementHighsService(BaseCanSlimService):
    """N新产品/新管理层/新高点分析服务"""
    
    async def collect_data(self) -> Dict[str, Any]:
        search_filter_result = await get_search_filter_result_dict(self.stock_info)
        return {
            'shareholder_increase': await get_shareholder_increase_json(self.stock_info),
            'revenue_analysis_three_years': await get_revenue_analysis_three_years(self.stock_info),
            'search_filter_result': search_filter_result,
            'moving_averages': await get_moving_averages_json_cn(self.stock_info),
            'stock_history_volume': await get_stock_history_volume_amount_yearly(self.stock_info),
            'stock_relative_strength': await get_stock_relative_strength_cn(self.stock_info)
        }
    
    def get_prompt_template(self) -> str:
        return N_PRODUCTS_MANAGEMENT_HIGHS_PROMPT_TEMPLATE
    
    def get_prompt_params(self) -> Dict[str, Any]:
        return {
            'announcements_json': self.to_json(self.data_cache['search_filter_result']['announcements']),
            'finance_and_expectations_json': self.to_json(self.data_cache['search_filter_result']['finance_and_expectations']),
            'revenue_analysis_json': self.to_json(self.data_cache['revenue_analysis_three_years']),
            'corporate_governance_json': self.to_json(self.data_cache['search_filter_result']['corporate_governance']),
            'stock_incentive_plan_json': self.to_json(self.data_cache['search_filter_result']['stock_incentive_plan']),
            'shareholder_increase_json': self.to_json(self.data_cache['shareholder_increase']),
            'moving_averages_json': self.to_json(self.data_cache['moving_averages']),
            'stock_relative_strength_json': self.to_json(self.data_cache['stock_relative_strength']),
            'stock_history_volume_json': self.to_json(self.data_cache['stock_history_volume'])
        }
    
    def get_final_output_instruction(self) -> str:
        return N_FINAL_OUTPUT

async def execute_N_Products_Management_Highs(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """执行N新产品/新管理层/新高点分析"""
    service = NProductsManagementHighsService(stock_info)
    return await service.execute(deep_thinking)
