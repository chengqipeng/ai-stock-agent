import asyncio
import pandas as pd

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.technical.abs.stock_indicator_base import parse_klines_to_df, process_indicator_data, \
    INDICATOR_CONFIG, get_stock_day_range_kline, get_stock_history_kline_max_min

"""
核心原则：必须拥有 250 条（约 1 年）以上的历史数据
理由如下：
趋势过滤： CAN SLIM 明确规定，不买入股价处于 200 日均线之下的股票。要确认 200-SMA 的**斜率（Slope）**是否向上，你至少需要观察过去几个月的均线走势，这需要总量约 250-300 条 数据。
第二阶段（Stage 2）确认： 马克·米勒维尼（Minervini）在完善 CAN SLIM 时提出，领涨股必须满足"150日线高于200日线"且"200日线向上至少 1 个月"。这需要至少 1.2 年（约 300 条） 的数据回溯。
杯柄形（Cup with a Handle）深度： 大型的底部分析通常跨越 6-12 个月，数据量不足会导致你无法看到完整的"地量"区域。

指标	最小计算条数	CAN SLIM 推荐加载条数	在系统中的作用
10-EMA	10	30+	观察强力领涨股的短期爆发力（N）。
50-SMA	50	100+	确认主力机构（I）是否在回撤时护盘。
200-SMA	200	250 - 300	核心红线。 过滤掉所有处于下降通道的弱势股。

"""
def calculate_moving_averages(klines, stock_info: StockInfo):
    """计算移动平均线指标"""
    df = parse_klines_to_df(klines)
    
    # 10日EMA - 使用adjust=True
    df['close_10_ema'] = df['close_price'].rolling(window=10).mean().round(2)
    
    # 50日SMA
    df['close_50_sma'] = df['close_price'].rolling(window=50).mean().round(2)
    
    # 200日SMA
    df['close_200_sma'] = df['close_price'].rolling(window=200).mean().round(2)
    
    # 多头排列判断
    df['is_bullish_alignment'] = (df['close_10_ema'] > df['close_50_sma']) & (df['close_50_sma'] > df['close_200_sma'])
    
    return process_indicator_data(df, 'ma')


async def generate_can_slim_50_200_summary(stock_info: StockInfo, klines):
    """生成CAN SLIM 10/50/200日均线分析摘要"""
    df = parse_klines_to_df(klines)
    
    # 计算均线和EMA10
    df['close_10_ema'] = df['close_price'].rolling(window=10).mean()
    df['SMA50'] = df['close_price'].rolling(window=50).mean()
    df['SMA200'] = df['close_price'].rolling(window=200).mean()
    df['sma200_diff'] = df['SMA200'].diff()

    # 提取最新数据点
    latest = df.iloc[-1]
    curr_price = latest['close']
    ema10 = latest['close_10_ema']
    sma50 = latest['SMA50']
    sma200 = latest['SMA200']

    # 200日线连续上涨天数
    rising_days = 0
    for val in reversed(df['sma200_diff'].tolist()):
        if val > 0:
            rising_days += 1
        else:
            break

    # 相对位置计算
    bias_200 = ((curr_price - sma200) / sma200) * 100
    high_52w = df['high_price'].rolling(window=250).max().iloc[-1]
    drop_from_high = ((curr_price - high_52w) / high_52w) * 100

    # 异常波动捕捉
    recent_10 = df.iloc[-10:]
    avg_vol_50 = df['trading_volume'].rolling(window=50).mean().iloc[-1]
    anomalies = sum(1 for _, day in recent_10.iterrows() 
                    if day['low'] < day['close_10_ema'] and day['close'] > day['close_10_ema'] and day['volume'] > avg_vol_50)

    # 格式化输出数据包
    summary = f"""## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - 均线状态总结 (截至{latest['date']})：
* **核心价格关系**：当前价({curr_price:.2f}) {' > ' if curr_price > ema10 else ' < '} 10日线({ema10:.2f}) {' > ' if ema10 > sma50 else ' < '} 50日线({sma50:.2f}) {' > ' if sma50 > sma200 else ' < '} 200日线({sma200:.2f})。
* **趋势得分**：200日均线已连续上涨 {rising_days} 个交易日，斜率为{'正' if latest['sma200_diff'] > 0 else '负'}。
* **相对位置**：当前价较200日线乖离率为 {bias_200:+.1f}%，较52周最高价({high_52w:.2f})跌幅为 {drop_from_high:.1f}%。
* **异常波动**：过去 10 个交易日中，有 {anomalies} 次放量跌破 10 日 EMA 后迅速收回。
    """
    return summary


async def get_moving_averages_markdown(stock_info: StockInfo, klines):
    """将移动平均线数据转换为markdown格式"""
    config = INDICATOR_CONFIG['ma']
    ma_data = calculate_moving_averages(klines, stock_info)

    markdown = f"## <{stock_info.stock_name}（ {stock_info.stock_code_normalize}）> - 移动平均线数据\n\n"
    markdown += "| 日期 | 收盘价 | 10日EMA | 50日SMA | 200日SMA | 多头排列 |\n"
    markdown += "|------|------|---------|---------|----------|--------|\n"
    for item in ma_data[:config['markdown_limit']]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item.get('close_10_ema', 'N/A')} | {item.get('close_50_sma', 'N/A')} | {item.get('close_200_sma', 'N/A')} | {'是' if item.get('is_bullish_alignment') else '否'} |\n"
    markdown += "\n"
    return markdown


async def get_moving_averages_json(stock_info: StockInfo, klines):
    """返回移动平均线数据的JSON格式"""
    config = INDICATOR_CONFIG['ma']
    ma_data = calculate_moving_averages(klines, stock_info)
    
    return {
        "stock_code": stock_info.stock_code_normalize,
        "stock_name": stock_info.stock_name,
        "data": ma_data[:config['markdown_limit']]
    }

async def get_stock_history_volume_amount_yearly(stock_info: StockInfo):
    """获取一年的成交量和成交额数据，返回JSON格式"""
    kline_data = await get_stock_history_kline_max_min(stock_info)
    result = []
    for date, data in sorted(kline_data.items(), reverse=True)[:250]:
        result.append({
            "成交日期": date,
            "成交量": data["trading_volume"],
            "成交额": data["trading_amount"]
        })
    print(result)
    return result

async def main():
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    klines = await generate_can_slim_50_200_summary(stock_info, [])
    print(klines)

if __name__ == "__main__":
    asyncio.run(main())
