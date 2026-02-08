import asyncio

from service.eastmoney.technical.abs.stock_indicator_base import parse_klines_to_df, process_indicator_data, \
    INDICATOR_CONFIG, get_stock_day_range_kline

"""
ATR: Averages true range to measure volatility.
Usage: Set stop-loss levels and adjust position sizes based on current market volatility.
Tips: It's a reactive measure, so use it as part of a broader risk management strategy.
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

async def get_atr_markdown(secid, stock_code, stock_name):
    """将ATR数据转换为markdown格式"""
    config = INDICATOR_CONFIG['atr']
    klines = await get_stock_day_range_kline(secid, config['kline_limit'])
    atr_data = calculate_atr(klines)

    markdown = f"## <{stock_code} {stock_name}> - ATR数据\n\n"
    markdown += "| 日期 | 收盘价 | ATR(14) | 波动率 |\n"
    markdown += "|------|------|---------|--------|\n"
    for item in atr_data[:config['markdown_limit']]:
        atr = item.get('atr', 'N/A')
        volatility = '高' if atr != 'N/A' and atr > item['close'] * 0.03 else ('低' if atr != 'N/A' and atr < item['close'] * 0.01 else '中')
        markdown += f"| {item['date']} | {item['close']:.2f} | {atr} | {volatility} |\n"
    markdown += "\n"
    return markdown

async def main():
    atr_data = await get_atr_markdown("0.002371", "002371", "北方华创")
    print(atr_data)

if __name__ == "__main__":
    asyncio.run(main())
