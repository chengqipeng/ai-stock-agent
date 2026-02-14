import asyncio

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.technical.abs.stock_indicator_base import parse_klines_to_df, process_indicator_data, \
    INDICATOR_CONFIG, get_stock_day_range_kline

"""
CAN SLIM 系统下的“实战”阈值（战略数据量）
在 CAN SLIM 逻辑中，MACD 不仅仅是金叉死叉，更多是用来确认趋势和背离：
确认杯柄形 (Cup with a Handle): 一个完整的杯柄形态通常持续 7 到 65 周。如果你在日线级别分析，需要至少 150 - 250 条 数据才能完整观察到：
股价构筑杯身时 MACD 在 0 轴下方的运行。
构筑柄部（Handle）时 MACD 的回抽和死叉不破。
确认底背离: 欧奈尔提到的“最后洗盘”，通常伴随着股价创新低而 MACD 不创新低。这需要涵盖至少 2 个月（约 40 条数据） 的波动对比。

基础计算35+算出第一组数据的数学门槛。
精准对齐软件130+消除 EMA 递归初期的数值漂移。
CAN SLIM 实战250 (1年)配合分析 C、A 增长及中长期杯柄形态。
"""

async def calculate_macd(stock_info: StockInfo, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    klines = await get_stock_day_range_kline(stock_info)
    df = parse_klines_to_df(klines)
    
    ema_fast = df['close_price'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close_price'].ewm(span=slow, adjust=False).mean()
    
    df['macd'] = (ema_fast - ema_slow).round(4)
    df['macds'] = df['macd'].ewm(span=signal, adjust=False).mean().round(4)
    df['macdh'] = (2 * (df['macd'] - df['macds'])).round(4)
    
    return process_indicator_data(df, 'macd')

async def get_macd_markdown(stock_info: StockInfo):
    """将MACD数据转换为markdown格式"""
    config = INDICATOR_CONFIG['macd']
    macd_data = await calculate_macd(stock_info)

    markdown = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - MACD数据\n\n"
    markdown += "| 日期 | 收盘价 | MACD | MACDS | MACDH |\n"
    markdown += "|------|--------|------|-------|-------|\n"
    for item in macd_data[:config['markdown_limit']]:
        markdown += f"| {item['date']} | {item['close_price']:.2f} | {item['macd']} | {item['macds']} | {item['macdh']} |\n"
    markdown += "\n"
    return markdown



async def main():
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    macd_data = await get_macd_markdown(stock_info)
    print(macd_data)

if __name__ == "__main__":
    asyncio.run(main())
