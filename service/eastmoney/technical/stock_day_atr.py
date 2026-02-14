import asyncio

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.technical.abs.stock_indicator_base import parse_klines_to_df, process_indicator_data, \
    INDICATOR_CONFIG, get_stock_day_range_kline

"""
ATR: Averages true range to measure volatility.
Usage: Set stop-loss levels and adjust position sizes based on current market volatility.
Tips: It's a reactive measure, so use it as part of a broader risk management strategy.

场景建议数据量 (以 14 日 ATR 为例)目的快速测试15 - 20 天验证代码逻辑是否跑通。
实盘交易100 天以上确保平滑算法已经稳定，消除初始值波动的影响。
回测研究覆盖整个回测时段观察波动率在不同市场周期下的表现。
"""

def calculate_atr(klines, period=14):
    """计算 ATR 指标"""
    df = parse_klines_to_df(klines)
    
    df['h_l'] = df['high'] - df['low']
    df['h_pc'] = abs(df['high'] - df['close'].shift(1))
    df['l_pc'] = abs(df['low'] - df['close'].shift(1))
    
    df['TR'] = df[['h_l', 'h_pc', 'l_pc']].max(axis=1)
    df['atr'] = df['TR'].ewm(alpha=1/period, adjust=False).mean().round(2)
    
    df.drop(['h_l', 'h_pc', 'l_pc', 'TR'], axis=1, inplace=True)
    
    return process_indicator_data(df, 'atr')

async def get_atr_markdown(stock_info: StockInfo, klines):
    """将ATR数据转换为markdown格式"""
    config = INDICATOR_CONFIG['atr']
    atr_data = calculate_atr(klines)

    markdown = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - ATR数据\n\n"
    markdown += "| 日期 | 收盘价 | ATR(14) | 波动率 |\n"
    markdown += "|------|------|---------|--------|\n"
    for item in atr_data[:config['markdown_limit']]:
        atr = item.get('atr', 'N/A')
        volatility = '高' if atr != 'N/A' and atr > item['close'] * 0.03 else ('低' if atr != 'N/A' and atr < item['close'] * 0.01 else '中')
        markdown += f"| {item['date']} | {item['close']:.2f} | {atr} | {volatility} |\n"
    markdown += "\n"
    return markdown

async def main():
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    atr_data = await get_atr_markdown(stock_info, [])
    print(atr_data)

if __name__ == "__main__":
    asyncio.run(main())
