import asyncio
import pandas as pd

from service.eastmoney.technical.abs.stock_indicator_base import parse_klines_to_df, process_indicator_data, \
    INDICATOR_CONFIG, get_stock_day_range_kline

"""
核心原则：必须拥有 250 条（约 1 年）以上的历史数据
理由如下：
趋势过滤： CAN SLIM 明确规定，不买入股价处于 200 日均线之下的股票。要确认 200-SMA 的**斜率（Slope）**是否向上，你至少需要观察过去几个月的均线走势，这需要总量约 250-300 条 数据。
第二阶段（Stage 2）确认： 马克·米勒维尼（Minervini）在完善 CAN SLIM 时提出，领涨股必须满足“150日线高于200日线”且“200日线向上至少 1 个月”。这需要至少 1.2 年（约 300 条） 的数据回溯。
杯柄形（Cup with a Handle）深度： 大型的底部分析通常跨越 6-12 个月，数据量不足会导致你无法看到完整的“地量”区域。

指标	最小计算条数	CAN SLIM 推荐加载条数	在系统中的作用
10-EMA	10	30+	观察强力领涨股的短期爆发力（N）。
50-SMA	50	100+	确认主力机构（I）是否在回撤时护盘。
200-SMA	200	250 - 300	核心红线。 过滤掉所有处于下降通道的弱势股。

"""
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
    
    return process_indicator_data(df, 'ma')

async def get_moving_averages_markdown(secid, stock_code, stock_name):
    """将移动平均线数据转换为markdown格式"""
    config = INDICATOR_CONFIG['ma']
    klines = await get_stock_day_range_kline(secid, config['kline_limit'])
    ma_data = calculate_moving_averages(klines)

    markdown = f"## <{stock_code} {stock_name}> - 移动平均线数据\n\n"
    markdown += "| 日期 | 收盘价 | 10日EMA | 50日SMA | 200日SMA | 多头排列 |\n"
    markdown += "|------|--------|---------|---------|----------|----------|\n"
    for item in ma_data[:config['markdown_limit']]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item.get('close_10_ema', 'N/A')} | {item.get('close_50_sma', 'N/A')} | {item.get('close_200_sma', 'N/A')} | {'是' if item.get('is_bullish_alignment') else '否'} |\n"
    markdown += "\n"
    return markdown



async def main():
    klines = await get_moving_averages_markdown("0.002371", "002371", "北方华创")
    print(klines)

if __name__ == "__main__":
    asyncio.run(main())
