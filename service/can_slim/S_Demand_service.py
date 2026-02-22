from typing import Dict, Any

from common.prompt.can_slim.S_Demand_prompt import S_DEMAND_PROMPT_TEMPLATE
from common.constants.can_slim_final_outputs import S_FINAL_OUTPUT
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_financial_main_with_total_share import get_equity_data_to_json
from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history_json, get_fund_flow_history_json_cn
from service.eastmoney.technical.stock_day_volume_avg import get_20day_volume_avg_cn, get_5day_volume_avg
from service.eastmoney.stock_info.stock_holder_data import get_org_holder_json
from service.eastmoney.stock_info.stock_lock_up_period import get_stock_lock_up_period_year_range
from service.eastmoney.stock_info.stock_realtime import get_stock_realtime_json
from service.eastmoney.stock_info.stock_repurchase import get_stock_repurchase_json
from service.eastmoney.stock_info.stock_top_ten_shareholders_circulation import get_top_ten_shareholders_circulation_by_dates
from service.can_slim.base_can_slim_service import BaseCanSlimService

async def day_5_volume_ratio_cn(stock_info: StockInfo):
    """计算5日量比"""
    day_5_volume_avg_result = await get_5day_volume_avg(stock_info, 200)
    fund_flow_history_json = await get_fund_flow_history_json(stock_info, ['date', 'close_price', 'trading_volume', 'change_pct'])

    ma_dict = {item['date']: item['volume_avg'] for item in day_5_volume_avg_result}
    
    result = []
    for item in fund_flow_history_json['data'][:50]:
        date = item['date']
        trading_volume = item['trading_volume']
        change_pct = item['change_pct']
        close_price = item['close_price']
        
        if date in ma_dict and ma_dict[date]:
            volume_20_avg = ma_dict[date]
            quantity_relative_ratio = trading_volume / volume_20_avg
            result.append({
                '日期': date,
                '交易量': trading_volume,
                '收盘价': close_price,
                '涨跌幅': change_pct,
                '量比（今日成交量/5日均量）': round(quantity_relative_ratio, 4)
            })
    
    return result

class SDemandService(BaseCanSlimService):
    """S供需分析服务"""
    
    async def collect_data(self) -> Dict[str, Any]:
        return {
            'equity_total_shares': await get_equity_data_to_json(self.stock_info, ['END_DATE', 'TOTAL_SHARES']),
            'equity_unlimited_shares': await get_equity_data_to_json(self.stock_info, ['END_DATE', 'UNLIMITED_SHARES']),
            'top_ten_shareholders': await get_top_ten_shareholders_circulation_by_dates(self.stock_info, page_size=3, limit=3),
            'org_holder': await get_org_holder_json(self.stock_info),
            'stock_realtime': await get_stock_realtime_json(self.stock_info, ['stock_name', 'stock_code', 'volume']),
            'fund_flow_history': await get_fund_flow_history_json_cn(self.stock_info, ['date', 'change_hand', 'trading_volume', 'trading_amount']),
            'stock_lock_up_period': await get_stock_lock_up_period_year_range(self.stock_info),
            'stock_repurchase': await get_stock_repurchase_json(self.stock_info),
            'day_5_volume_ratio_cn': await day_5_volume_ratio_cn(self.stock_info),
            'day_20_volume_avg_cn': await get_20day_volume_avg_cn(self.stock_info, 200)
        }
    
    def get_prompt_template(self) -> str:
        return S_DEMAND_PROMPT_TEMPLATE
    
    def get_prompt_params(self) -> Dict[str, Any]:
        return {
            'total_shares_json': self.to_json(self.data_cache['equity_total_shares']),
            'unlimited_shares_json': self.to_json(self.data_cache['equity_unlimited_shares']),
            'top_ten_holders_json': self.to_json(self.data_cache['top_ten_shareholders'][:10]),
            'org_holder_json': self.to_json(self.data_cache['org_holder'][:2]),
            'day_20_volume_avg_cn': self.to_json(self.data_cache['day_20_volume_avg_cn'][:20]),
            'day_5_volume_ratio_cn': self.to_json(self.data_cache['day_5_volume_ratio_cn']),
            'stock_realtime_json': self.to_json(self.data_cache['stock_realtime']),
            'fund_flow_history_json_cn': self.to_json(self.data_cache['fund_flow_history']),
            'stock_lock_up_period_json': self.to_json(self.data_cache['stock_lock_up_period']),
            'stock_repurchase_json': self.to_json(self.data_cache['stock_repurchase'])
        }
    
    def get_final_output_instruction(self) -> str:
        return S_FINAL_OUTPUT


async def build_S_Demand_prompt(stock_info: StockInfo) -> str:
    """构建S供需分析提示词"""
    service = SDemandService(stock_info)
    await service.collect_data()
    return service.build_prompt()


async def execute_S_Demand(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """执行S供需分析"""
    service = SDemandService(stock_info)
    return await service.execute(deep_thinking)
