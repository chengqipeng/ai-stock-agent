import asyncio
import pandas as pd
from common.utils.stock_info_utils import get_stock_info_by_name, StockInfo
from service.can_slim.L_or_Laggard_service import execute_L_or_Laggard
from service.eastmoney.technical.abs.stock_indicator_base import get_stock_history_kline_max_min


async def calculate_resilience(stock_info: StockInfo, days=250):
    """
    计算个股在指数最近一次回调期间的抗跌性表现
    :param stock_info: 股票信息对象
    :param days: 获取天数，默认250天
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

    # 寻找指数的最近一次回调
    idx_max_date = index_df.loc[index_df['close'].idxmax(), 'date']
    after_max_df = index_df[index_df['date'] >= idx_max_date]
    if after_max_df.empty:
        return "无法获取回调数据"

    idx_min_date = after_max_df.loc[after_max_df['close'].idxmin(), 'date']

    # 提取回调窗口内的价格
    idx_start_price = index_df[index_df['date'] == idx_max_date]['close'].values[0]
    idx_end_price = index_df[index_df['date'] == idx_min_date]['close'].values[0]
    idx_drop = (idx_end_price - idx_start_price) / idx_start_price

    stk_start_price = stock_df[stock_df['date'] == idx_max_date]['close'].values[0]
    stk_end_price = stock_df[stock_df['date'] == idx_min_date]['close'].values[0]
    stk_drop = (stk_end_price - stk_start_price) / stk_start_price

    # 计算相对表现
    relative_performance = stk_drop - idx_drop

    return {
        "correction_period": (idx_max_date.strftime('%Y-%m-%d'), idx_min_date.strftime('%Y-%m-%d')),
        "index_drop": f"{idx_drop:.2%}",
        "stock_drop": f"{stk_drop:.2%}",
        "relative_resilience": f"{relative_performance:.2%}",
        "is_resilient": stk_drop > idx_drop
    }


if __name__ == "__main__":
    stock_name = "北方华创"
    stock_info = get_stock_info_by_name(stock_name)
    
    # 测试抗跌性分析
    result = asyncio.run(calculate_resilience(stock_info))
    print("抗跌性分析结果:")
    print(result)
