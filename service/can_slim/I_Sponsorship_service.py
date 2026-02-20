from typing import Dict, Any

from common.prompt.can_slim.I_Sponsorship_prompt import I_SPONSORSHIP_PROMPT_TEMPLATE
from common.constants.can_slim_final_outputs import I_FINAL_OUTPUT
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_holder_data import get_org_holder_count, get_org_holder_by_type, get_org_holder_json
from service.eastmoney.stock_info.stock_new_major_shareholders_detector import get_detect_new_major_shareholders
from service.eastmoney.stock_info.stock_northbound_funds import get_northbound_funds_cn
from service.eastmoney.stock_info.stock_top_ten_shareholders_circulation import get_top_ten_shareholders_circulation_by_dates
from service.can_slim.base_can_slim_service import BaseCanSlimService

class ISponsorshipService(BaseCanSlimService):
    """I机构认同度分析服务"""
    
    async def collect_data(self) -> Dict[str, Any]:
        return {
            #'org_holder_count': await get_org_holder_count(self.stock_info),
            'org_holder_json': await get_org_holder_json(self.stock_info),
            'top_ten_name': await get_top_ten_shareholders_circulation_by_dates(self.stock_info, page_size=3, limit=3, fields=['rank', 'holder_name', 'report_date']),
            'top_ten_hold_change': await get_top_ten_shareholders_circulation_by_dates(self.stock_info, page_size=3, limit=3, fields=['rank', 'holder_name', 'hold_change', 'report_date']),
            'northbound_funds': await get_northbound_funds_cn(self.stock_info, ['TRADE_DATE', 'ADD_MARKET_CAP', 'ADD_SHARES_AMP', 'ADD_SHARES_AMP']),
            'org_holder_she_bao': await get_org_holder_by_type(self.stock_info, '社保'),
            'detect_new_major_shareholders': await get_detect_new_major_shareholders(self.stock_info)
        }
    
    def get_prompt_template(self) -> str:
        return I_SPONSORSHIP_PROMPT_TEMPLATE
    
    def get_prompt_params(self) -> Dict[str, Any]:
        return {
            #'org_holder_count_json': self.to_json(self.data_cache['org_holder_count']),
            'org_holder_json': self.to_json(self.data_cache['org_holder_json']),
            'top_ten_name_shareholders_json': self.to_json(self.data_cache['top_ten_name']),
            'northbound_funds_json': self.to_json(self.data_cache['northbound_funds']),
            'org_holder_she_bao_json': self.to_json(self.data_cache['org_holder_she_bao']),
            'top_ten_hold_change_json': self.to_json(self.data_cache['top_ten_hold_change']),
            'detect_new_major_shareholders_json': self.to_json(self.data_cache['detect_new_major_shareholders'])
        }
    
    def get_final_output_instruction(self) -> str:
        return I_FINAL_OUTPUT


async def build_I_Sponsorship_prompt(stock_info: StockInfo) -> str:
    """构建I机构认同度分析提示词"""
    service = ISponsorshipService(stock_info)
    await service.collect_data()
    return service.build_prompt()


async def execute_I_Sponsorship(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """执行I机构认同度分析"""
    service = ISponsorshipService(stock_info)
    return await service.execute(deep_thinking)
