from typing import Dict, Any
import pandas as pd

from common.prompt.can_slim.M_Direction_prompt import M_DIRECTION_PROMPT_TEMPLATE
from common.constants.can_slim_final_outputs import M_FINAL_OUTPUT
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.auto_job.stock_history_highest_lowest_price_data import get_new_high_low_count, get_top_strongest_stocks
from service.eastmoney.technical.stock_day_range_kline import calculate_moving_averages
from service.eastmoney.technical.abs.stock_indicator_base import get_stock_history_kline_max_min
from service.can_slim.base_can_slim_service import BaseCanSlimService

async def distribution_Days_Count(stock_info: StockInfo, window_days: int = 25) -> dict:
    """计算出货日计数
    
    Args:
        stock_info: 股票信息
        window_days: 回溯天数，默认25天（5周）
        
    Returns:
        包含出货日分析结果的字典
    """
    kline_data = await get_stock_history_kline_max_min(stock_info)
    
    df = pd.DataFrame.from_dict(kline_data, orient='index')
    df.index.name = 'date'
    df = df.sort_index()
    
    df['pct_chg'] = df['close_price'].pct_change()
    df['prev_vol'] = df['trading_volume'].shift(1)
    df['is_distribution'] = (df['pct_chg'] < -0.002) & (df['trading_volume'] > df['prev_vol'])
    df['distribution_count'] = df['is_distribution'].rolling(window=window_days).sum()
    
    latest = df.iloc[-1]
    recent_df = df.tail(window_days)
    distribution_days_list = recent_df[recent_df['is_distribution']].index.tolist()
    
    return {
        "分析日期": df.index[-1],
        "今日是否出货日": bool(latest['is_distribution']),
        "过去{}个交易日出货日总数".format(window_days): int(latest['distribution_count']),
        "出货日列表": distribution_days_list,
        "当前收盘价": latest['close_price'],
        "涨跌幅": round(latest['pct_chg'] * 100, 2) if pd.notna(latest['pct_chg']) else None
    }

class MDirectionService(BaseCanSlimService):
    """M市场方向分析服务"""
    
    async def collect_data(self) -> Dict[str, Any]:
        indices_stock_info = get_stock_info_by_name(self.stock_info.indices_stock_name)
        return {
            'indices_stock_name': self.stock_info.indices_stock_name,
            'indices_moving_averages': await calculate_moving_averages(indices_stock_info),
            'distribution_days': await distribution_Days_Count(indices_stock_info),
            'new_high_low_count': get_new_high_low_count(),
            'top_strongest_stocks': await get_top_strongest_stocks()
        }
    
    def get_prompt_template(self) -> str:
        return M_DIRECTION_PROMPT_TEMPLATE
    
    def get_prompt_params(self) -> Dict[str, Any]:
        return {
            'indices_stock_name': self.data_cache['indices_stock_name'],
            'indices_moving_averages_json': self.to_json(self.data_cache['indices_moving_averages']),
            'distribution_days_json': self.to_json(self.data_cache['distribution_days']),
            'new_high_low_count_json': self.to_json(self.data_cache['new_high_low_count']),
            'top_strongest_stocks_json': self.to_json(self.data_cache['top_strongest_stocks'])
        }
    
    def get_final_output_instruction(self) -> str:
        return M_FINAL_OUTPUT


async def build_M_Direction_prompt(stock_info: StockInfo) -> str:
    """构建M市场方向分析提示词"""
    service = MDirectionService(stock_info)
    await service.collect_data()
    return service.build_prompt()


async def execute_M_Direction(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """执行M市场方向分析"""
    service = MDirectionService(stock_info)
    return await service.execute(deep_thinking)

