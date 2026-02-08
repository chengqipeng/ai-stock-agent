import asyncio
import pandas as pd
from service.eastmoney.stock_indicator_base import get_stock_day_range_kline, parse_klines_to_df

"""
技术计算： 至少 20 条。
图形观察： 建议 60 条（看清近期趋势）。
完整分析： 建议 250 条（对齐 RS 评级和年度业绩周期）。
"""
def calculate_bollinger_bands(klines, window=20, num_std=2):
    """计算布林线指标"""
    df = parse_klines_to_df(klines)
    
    boll = df['close'].rolling(window=window).mean()
    rolling_std = df['close'].rolling(window=window).std()
    
    df['boll'] = boll.round(4)
    df['boll_ub'] = (boll + rolling_std * num_std).round(4)
    df['boll_lb'] = (boll - rolling_std * num_std).round(4)
    
    df = df.tail(300)
    return df.to_dict('records')[::-1]

async def get_boll_markdown(secid, stock_code, stock_name):
    """将布林线数据转换为markdown格式"""
    klines = await get_stock_day_range_kline(secid, 300)
    boll_data = calculate_bollinger_bands(klines)

    markdown = f"## <{stock_code} {stock_name}> - 布林线数据\n\n"
    markdown += "| 日期 | 收盘价 | BOLL | BOLL_UB | BOLL_LB |\n"
    markdown += "|------|--------|------|---------|---------|\\n"
    for item in boll_data[:250]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item['boll']} | {item['boll_ub']} | {item['boll_lb']} |\n"
    markdown += "\n"
    return markdown



async def main():
    boll_data = await get_boll_markdown("0.002371", "002371", "北方华创")
    print(boll_data)

if __name__ == "__main__":
    asyncio.run(main())
