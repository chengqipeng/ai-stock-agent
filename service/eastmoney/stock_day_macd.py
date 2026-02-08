import asyncio
from service.eastmoney.stock_indicator_base import get_stock_day_range_kline, parse_klines_to_df

"""
基础计算	35+	算出第一组数据的数学门槛。
精准对齐软件	130+	消除 EMA 递归初期的数值漂移。
CAN SLIM 实战	250 (1年)	配合分析 C、A 增长及中长期杯柄形态。
"""
def calculate_macd(klines, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    df = parse_klines_to_df(klines)
    
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    
    df['macd'] = (ema_fast - ema_slow).round(4)
    df['macds'] = df['macd'].ewm(span=signal, adjust=False).mean().round(4)
    df['macdh'] = (2 * (df['macd'] - df['macds'])).round(4)
    
    df = df.tail(400)
    return df.to_dict('records')[::-1]

async def get_macd_markdown(secid, stock_code, stock_name):
    """将MACD数据转换为markdown格式"""
    klines = await get_stock_day_range_kline(secid, 400)
    macd_data = calculate_macd(klines)

    markdown = f"## <{stock_code} {stock_name}> - MACD数据\n\n"
    markdown += "| 日期 | 收盘价 | MACD | MACDS | MACDH |\n"
    markdown += "|------|--------|------|-------|-------|\n"
    for item in macd_data[:365]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item['macd']} | {item['macds']} | {item['macdh']} |\n"
    markdown += "\n"
    return markdown



async def main():
    macd_data = await get_macd_markdown("0.002371", "002371", "北方华创")
    print(macd_data)

if __name__ == "__main__":
    asyncio.run(main())
