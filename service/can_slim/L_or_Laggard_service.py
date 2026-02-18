from typing import Dict, Any

import pandas as pd

from common.prompt.can_slim.L_or_Laggard_prompt import L_OR_LAGGARD_PROMPT_TEMPLATE
from common.constants.can_slim_final_outputs import L_FINAL_OUTPUT
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.indices.stock_market_data import get_stock_relative_strength_cn
from service.eastmoney.stock_info.stock_industry_ranking import get_stock_industry_ranking_json
from service.eastmoney.technical.abs.stock_indicator_base import get_stock_history_kline_max_min
from service.can_slim.base_can_slim_service import BaseCanSlimService


def _find_corrections(index_df, num_corrections=1, min_drop_pct=0.03):
    """找出指数的多次回调期间"""
    corrections = []
    df = index_df.copy().sort_values('date')

    while len(corrections) < num_corrections and len(df) > 10:
        idx_max_date = df.loc[df['close'].idxmax(), 'date']
        after_max = df[df['date'] >= idx_max_date]
        if after_max.empty:
            break

        idx_min_date = after_max.loc[after_max['close'].idxmin(), 'date']
        max_price = df[df['date'] == idx_max_date]['close'].values[0]
        min_price = df[df['date'] == idx_min_date]['close'].values[0]
        drop = (min_price - max_price) / max_price

        if drop < -min_drop_pct:
            corrections.append((idx_max_date, idx_min_date))

        df = df[df['date'] < idx_max_date]

    return corrections


async def calculate_resilience(stock_info: StockInfo, days=250, num_corrections=1):
    """
    计算个股在指数最近多次回调期间的抗跌性表现
    :param stock_info: 股票信息对象
    :param days: 获取天数，默认250天
    :param num_corrections: 分析最近几次回调，默认1次
    """
    # 获取个股K线数据
    stock_kline = await get_stock_history_kline_max_min(stock_info, days)
    stock_df = pd.DataFrame([
        {'date': date, 'close': data['close_price']}
        for date, data in stock_kline.items()
    ])

    # 获取指数K线数据
    if not stock_info.indices_stock_name:
        raise ValueError(f"股票 {stock_info.stock_code_normalize} 没有关联的指数信息")

    index_info = get_stock_info_by_name(stock_info.indices_stock_name)
    index_kline = await get_stock_history_kline_max_min(index_info, days)
    index_df = pd.DataFrame([
        {'date': date, 'close': data['close_price']}
        for date, data in index_kline.items()
    ])

    # 确保日期格式正确并排序
    stock_df['date'] = pd.to_datetime(stock_df['date'])
    index_df['date'] = pd.to_datetime(index_df['date'])
    stock_df = stock_df.sort_values('date')
    index_df = index_df.sort_values('date')

    # 找出多次回调期间
    corrections = _find_corrections(index_df, num_corrections)
    if not corrections:
        return "无法获取回调数据"

    results = []
    for idx_max_date, idx_min_date in corrections:
        idx_start_price = index_df[index_df['date'] == idx_max_date]['close'].values[0]
        idx_end_price = index_df[index_df['date'] == idx_min_date]['close'].values[0]
        idx_drop = (idx_end_price - idx_start_price) / idx_start_price

        stk_start_price = stock_df[stock_df['date'] == idx_max_date]['close'].values[0]
        stk_end_price = stock_df[stock_df['date'] == idx_min_date]['close'].values[0]
        stk_drop = (stk_end_price - stk_start_price) / stk_start_price

        relative_performance = stk_drop - idx_drop

        results.append({
            "correction_period": (idx_max_date.strftime('%Y-%m-%d'), idx_min_date.strftime('%Y-%m-%d')),
            "index_drop": f"{idx_drop:.2%}",
            "stock_drop": f"{stk_drop:.2%}",
            "relative_resilience": f"{relative_performance:.2%}",
            "is_resilient": stk_drop > idx_drop
        })

    return results if num_corrections > 1 else results[0]


def format_resilience_to_chinese(data):
    """将calculate_resilience返回的数据转换为中文key"""
    if isinstance(data, str):
        return data

    def convert_item(item):
        return {
            "回调期间": f"{item['correction_period'][0]} 至 {item['correction_period'][1]}",
            "指数涨跌幅": item['index_drop'],
            "个股涨跌幅": item['stock_drop'],
            "相对抗跌性": item['relative_resilience'],
            "是否抗跌": "是" if item['is_resilient'] else "否"
        }

    return [convert_item(item) for item in data] if isinstance(data, list) else convert_item(data)


class LOrLaggardService(BaseCanSlimService):
    """L领军股或落后股分析服务"""
    
    async def collect_data(self) -> Dict[str, Any]:
        return {
            'stock_relative_strength': await get_stock_relative_strength_cn(self.stock_info),
            'stock_industry_ranking': await get_stock_industry_ranking_json(self.stock_info),
            'resilience_data': await calculate_resilience(self.stock_info, days=250, num_corrections=3)
        }
    
    async def process_data(self) -> None:
        self.data_cache['resilience_data_cn'] = format_resilience_to_chinese(self.data_cache['resilience_data'])
    
    def get_prompt_template(self) -> str:
        return L_OR_LAGGARD_PROMPT_TEMPLATE
    
    def get_prompt_params(self) -> Dict[str, Any]:
        return {
            'stock_relative_strength_json': self.to_json(self.data_cache['stock_relative_strength']),
            'stock_industry_ranking_json': self.to_json(self.data_cache['stock_industry_ranking']),
            'resilience_data_json': self.to_json(self.data_cache['resilience_data_cn'])
        }
    
    def get_final_output_instruction(self) -> str:
        return L_FINAL_OUTPUT


async def build_L_or_Laggard_prompt(stock_info: StockInfo) -> str:
    """构建L领军股或落后股分析提示词"""
    service = LOrLaggardService(stock_info)
    await service.collect_data()
    await service.process_data()
    return service.build_prompt()


async def execute_L_or_Laggard(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """执行L领军股或落后股分析"""
    service = LOrLaggardService(stock_info)
    return await service.execute(deep_thinking)

