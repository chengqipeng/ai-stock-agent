import asyncio
import pandas as pd

from service.eastmoney.technical.abs.stock_indicator_base import process_indicator_data, INDICATOR_CONFIG, \
    get_stock_day_range_kline, parse_klines_to_df

"""
CAN SLIM 选股逻辑下的数据需求虽然 20 条数据就能算出指标，但在实际 A 股选股筛选中，为了配合 CAN SLIM 的核心逻辑，建议加载的数据量如下：
数据量 (条)对应的 CAN SLIM 维度实际用途20 条极简择时仅计算当前的布林通道宽度（Bandwidth）。
60 条 (1个季度)C (Current Earnings)观察季度业绩发布前后的股价波动，确认是否在布林中轨上方稳住。
120 条 (半年)L (Leader)寻找处于“上升通道”的领头羊，确认其回撤不破布林下轨。
250 条 (1年)A (Annual Earnings)推荐量。 用于识别长达数月的“大底”和“杯身”形态。

布林线与 CAN SLIM 的组合拳
寻找“挤压” (Squeeze)： 在 CAN SLIM 的 N (New) 维度中，当布林带宽度缩至近一年的极值时，配合成交量（Volume）爆发突破上轨，这通常是“新产品/新高”启动的信号。
假突破识别： 如果股价突破布林上轨，但 RSI 没创新高，这在 A 股通常是假突破，需警惕。

总结
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
    
    return process_indicator_data(df, 'boll')

async def get_boll_markdown(stock_code, stock_name, klines):
    """将布林线数据转换为markdown格式"""
    config = INDICATOR_CONFIG['boll']
    boll_data = calculate_bollinger_bands(klines)

    markdown = f"## <{stock_code} {stock_name}> - 布林线数据\n\n"
    markdown += "| 日期 | 收盘价 | BOLL | BOLL_UB | BOLL_LB |\n"
    markdown += "|------|--------|------|---------|---------|\n"
    for item in boll_data[:config['markdown_limit']]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item['boll']} | {item['boll_ub']} | {item['boll_lb']} |\n"
    markdown += "\n"
    return markdown



async def main():
    boll_data = await get_boll_markdown("0.002371", "002371", "北方华创")
    print(boll_data)

if __name__ == "__main__":
    asyncio.run(main())
