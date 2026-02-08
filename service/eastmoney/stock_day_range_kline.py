import asyncio
import pandas as pd
from service.eastmoney.stock_indicator_base import get_stock_day_range_kline, parse_klines_to_df

def calculate_moving_averages(klines):
    """计算移动平均线指标"""
    df = parse_klines_to_df(klines)
    
    # 10日EMA - 使用adjust=True
    df['close_10_ema'] = df['close'].rolling(window=10).mean().round(2)
    
    # 50日SMA
    df['close_50_sma'] = df['close'].rolling(window=50).mean().round(2)
    
    # 200日SMA
    df['close_200_sma'] = df['close'].rolling(window=200).mean().round(2)
    
    # 多头排列判断
    df['is_bullish_alignment'] = (df['close_10_ema'] > df['close_50_sma']) & (df['close_50_sma'] > df['close_200_sma'])
    
    # 取时间倒序的前100条数据
    df = df.tail(400)
    return df.to_dict('records')[::-1]

async def get_moving_averages_markdown(secid, stock_code, stock_name):
    """将移动平均线数据转换为markdown格式"""
    klines = await get_stock_day_range_kline(secid, 400)
    ma_data = calculate_moving_averages(klines)

    markdown = f"## <{stock_code} {stock_name}> - 移动平均线数据\n\n"
    markdown += "| 日期 | 收盘价 | 10日EMA | 50日SMA | 200日SMA | 多头排列 |\n"
    markdown += "|------|--------|---------|---------|----------|----------|\n"
    for item in ma_data[:200]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item.get('close_10_ema', 'N/A')} | {item.get('close_50_sma', 'N/A')} | {item.get('close_200_sma', 'N/A')} | {'是' if item.get('is_bullish_alignment') else '否'} |\n"
    markdown += "\n"
    return markdown



async def main():
    klines = await get_moving_averages_markdown("0.002371", "002371", "北方华创")
    print(klines)

if __name__ == "__main__":
    asyncio.run(main())
