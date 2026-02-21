import asyncio

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.technical.abs.stock_indicator_base import parse_klines_to_df, process_indicator_data, INDICATOR_CONFIG
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_history_kline_max_min, get_stock_day_range_kline

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
async def calculate_moving_averages(stock_info: StockInfo):
    """计算移动平均线指标"""
    klines = await get_stock_day_range_kline(stock_info)
    df = parse_klines_to_df(klines)

    df['close_5_ema'] = df['close_price'].rolling(window=10).mean().round(2)
    
    # 5日SMA
    df['close_5_sma'] = df['close_price'].rolling(window=5).mean().round(2)
    
    # 10日EMA - 使用adjust=True
    df['close_10_ema'] = df['close_price'].rolling(window=10).mean().round(2)

    # 21日SMA
    df['close_21_sma'] = df['close_price'].rolling(window=21).mean().round(2)

    # 50日SMA
    df['close_50_sma'] = df['close_price'].rolling(window=50).mean().round(2)
    
    # 200日SMA
    df['close_200_sma'] = df['close_price'].rolling(window=200).mean().round(2)
    
    # 多头排列判断
    df['is_bullish_alignment'] = (df['close_5_sma'] > df['close_10_ema']) & (df['close_10_ema'] > df['close_21_sma']) & (df['close_21_sma'] > df['close_50_sma']) & (df['close_50_sma'] > df['close_200_sma'])
    
    config = INDICATOR_CONFIG.get('ma', {'tail_limit': 400})
    return df[['date', 'close_5_sma', 'close_10_ema', 'close_21_sma', 'close_50_sma', 'close_200_sma', 'is_bullish_alignment']].tail(config['tail_limit']).to_dict('records')[::-1]


async def generate_can_slim_50_200_summary(stock_info: StockInfo):
    """生成CAN SLIM 10/50/200日均线分析摘要"""
    klines = await get_stock_day_range_kline(stock_info)
    df = parse_klines_to_df(klines)
    
    # 计算均线和EMA10
    df['close_10_ema'] = df['close_price'].rolling(window=10).mean()
    df['SMA50'] = df['close_price'].rolling(window=50).mean()
    df['SMA200'] = df['close_price'].rolling(window=200).mean()
    df['sma200_diff'] = df['SMA200'].diff()

    # 提取最新数据点
    latest = df.iloc[-1]
    curr_price = latest['close_price']
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
                    if day['low_price'] < day['close_10_ema'] and day['close_price'] > day['close_10_ema'] and day['trading_volume'] > avg_vol_50)

    # 格式化输出数据包
    summary = f"""## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - 均线状态总结 (截至{latest['date']})：
* **核心价格关系**：当前价({curr_price:.2f}) {' > ' if curr_price > ema10 else ' < '} 10日线({ema10:.2f}) {' > ' if ema10 > sma50 else ' < '} 50日线({sma50:.2f}) {' > ' if sma50 > sma200 else ' < '} 200日线({sma200:.2f})。
* **趋势得分**：200日均线已连续上涨 {rising_days} 个交易日，斜率为{'正' if latest['sma200_diff'] > 0 else '负'}。
* **相对位置**：当前价较200日线乖离率为 {bias_200:+.1f}%，较52周最高价({high_52w:.2f})跌幅为 {drop_from_high:.1f}%。
* **异常波动**：过去 10 个交易日中，有 {anomalies} 次放量跌破 10 日 EMA 后迅速收回。
    """
    return summary


async def get_moving_averages_markdown(stock_info: StockInfo):
    """将移动平均线数据转换为markdown格式"""
    config = INDICATOR_CONFIG['ma']
    ma_data = await calculate_moving_averages(stock_info)

    markdown = f"## <{stock_info.stock_name}（ {stock_info.stock_code_normalize}）> - 移动平均线数据\n\n"
    markdown += "| 日期 | 5日SMA | 10日EMA | 21日SMA | 50日SMA | 200日SMA | 多头排列 |\n"
    markdown += "|------|--------|---------|---------|---------|----------|--------|\n"
    for item in ma_data[:config['markdown_limit']]:
        markdown += f"| {item['date']} | {item.get('close_5_sma', 'N/A')} | {item.get('close_10_ema', 'N/A')} | {item.get('close_21_sma', 'N/A')} | {item.get('close_50_sma', 'N/A')} | {item.get('close_200_sma', 'N/A')} | {'是' if item.get('is_bullish_alignment') else '否'} |\n"
    markdown += "\n"
    return markdown


async def get_moving_averages_json(stock_info: StockInfo, include_fields: list[str] = None, limit: int = None):
    """返回移动平均线数据的JSON格式
    
    Args:
        stock_info: 股票信息
        include_fields: 可选字段列表，可选值: ['close_5_sma', 'close_10_ema', 'close_21_sma', 'close_50_sma', 'close_200_sma', 'is_bullish_alignment']
                       如果为None，则返回所有字段
        limit: 返回数据条数限制，如果为None则使用配置中的默认值
    """
    config = INDICATOR_CONFIG['ma']
    ma_data = await calculate_moving_averages(stock_info)
    
    # 确定返回数量
    data_limit = limit if limit is not None else config['markdown_limit']
    
    # 如果指定了字段，则过滤数据
    if include_fields:
        filtered_data = []
        for item in ma_data[:data_limit]:
            filtered_item = {'date': item['date']}
            for field in include_fields:
                if field in item:
                    filtered_item[field] = item[field]
            filtered_data.append(filtered_item)
        data = filtered_data
    else:
        data = ma_data[:data_limit]
    
    return {
        "stock_code": stock_info.stock_code_normalize,
        "stock_name": stock_info.stock_name,
        "data": data
    }

async def get_moving_averages_json_cn(stock_info: StockInfo, include_fields: list[str] = None, limit: int = None):
    """返回移动平均线数据的JSON格式（中文键名）
    
    Args:
        stock_info: 股票信息
        include_fields: 可选字段列表，可选值: ['close_5_sma', 'close_10_ema', 'close_21_sma', 'close_50_sma', 'close_200_sma', 'is_bullish_alignment']
                       如果为None，则返回所有字段
        limit: 返回数据条数限制，如果为None则使用配置中的默认值
    """
    result = await get_moving_averages_json(stock_info, include_fields, limit)
    
    # 字段映射表
    field_mapping = {
        'date': '日期',
        'close_5_sma': '5日均线',
        'close_10_ema': '10日均线',
        'close_21_sma': '21日均线',
        'close_50_sma': '50日均线',
        'close_200_sma': '200日均线',
        'is_bullish_alignment': '多头排列'
    }
    
    # 转换数据字段
    cn_data = []
    for item in result['data']:
        cn_item = {}
        for en_key, value in item.items():
            cn_key = field_mapping.get(en_key, en_key)
            cn_item[cn_key] = '是' if value is True else ('否' if value is False else value)
        cn_data.append(cn_item)
    
    return {
        "股票代码": result['stock_code'],
        "股票名称": result['stock_name'],
        "数据": cn_data
    }

async def get_stock_history_volume_amount_yearly(stock_info: StockInfo):
    """获取一年的成交量和成交额数据，返回JSON格式"""
    kline_data = await get_stock_history_kline_max_min(stock_info)
    result = []
    for date, data in sorted(kline_data.items(), reverse=True)[:250]:
        result.append({
            "成交日期": date,
            "成交量（万）": data["trading_volume"],
            "成交额": data["trading_amount"]
        })
    return result

async def main():
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    klines = await get_stock_history_volume_amount_yearly(stock_info)
    print(klines)

if __name__ == "__main__":
    asyncio.run(main())
