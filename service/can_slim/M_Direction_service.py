import json
from datetime import datetime
import pandas as pd

from common.prompt.can_slim.M_Direction_prompt import M_DIRECTION_PROMPT_TEMPLATE
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.auto_job.stock_history_highest_lowest_price_data import get_new_high_low_count, get_top_strongest_stocks
from service.eastmoney.technical.stock_day_range_kline import calculate_moving_averages
from service.eastmoney.technical.abs.stock_indicator_base import get_stock_history_kline_max_min
from service.llm.deepseek_client import DeepSeekClient

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

async def build_M_Direction_prompt(stock_info: StockInfo) -> str:
    """构建M市场方向分析提示词"""
    indices_stock_info = get_stock_info_by_name(stock_info.indices_stock_name)
    indices_moving_averages = await calculate_moving_averages(indices_stock_info)
    distribution_days = await distribution_Days_Count(indices_stock_info)
    new_high_low_count = get_new_high_low_count()
    top_strongest_stocks = await get_top_strongest_stocks()

    return M_DIRECTION_PROMPT_TEMPLATE.format(
        current_date=datetime.now().strftime('%Y-%m-%d'),
        stock_name=stock_info.stock_name,
        stock_code=stock_info.stock_code_normalize,
        indices_stock_name=stock_info.indices_stock_name,
        indices_moving_averages_json=json.dumps(indices_moving_averages, ensure_ascii=False, indent=2),
        distribution_days_json=json.dumps(distribution_days, ensure_ascii=False, indent=2),
        new_high_low_count_json=json.dumps(new_high_low_count, ensure_ascii=False, indent=2),
        top_strongest_stocks_json=json.dumps(top_strongest_stocks, ensure_ascii=False, indent=2)
    )

async def execute_M_Direction(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """
    执行M市场方向分析
    
    Args:
        stock_info: 股票信息对象
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    prompt = await build_M_Direction_prompt(stock_info)

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

if __name__ == "__main__":
    import asyncio

    async def main():
        stock_info = get_stock_info_by_name("上证指数")
        result = await distribution_Days_Count(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
