import asyncio
import pandas as pd
from service.eastmoney.stock_indicator_base import get_stock_day_range_kline, parse_klines_to_df

"""
短期择时 (Pivot Point)	30 - 50 条	确认突破瞬间的 RSI 动能爆发。
形态识别 (Cup with Handle)	150 条	覆盖杯身构造期（通常 7-65 周）的 RSI 波动。
全系统扫描 (CAN SLIM Filter)	250 条 (约1年)	保证 RSI 计算完全平滑，并能计算年度 EPS 增长和相对强度排名。
"""
#东方财富是6 12 24，行业标准取14
def calculate_rsi(klines, window=14):
    """计算 RSI 指标"""
    df = parse_klines_to_df(klines)
    
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta.where(delta < 0, 0))
    
    avg_gain = gain.ewm(alpha=1/window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/window, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    df['rsi'] = (100 - (100 / (1 + rs))).round(2)
    
    df = df.tail(400)
    return df.to_dict('records')[::-1]

async def get_rsi_markdown(secid, stock_code, stock_name):
    """将RSI数据转换为markdown格式"""
    klines = await get_stock_day_range_kline(secid, 400)
    rsi_data = calculate_rsi(klines)

    markdown = f"## <{stock_code} {stock_name}> - RSI数据\n\n"
    markdown += "| 日期 | 收盘价 | RSI(14) | 状态 |\n"
    markdown += "|------|--------|---------|------|\n"
    for item in rsi_data[:365]:
        rsi = item.get('rsi', 'N/A')
        status = '超买' if rsi != 'N/A' and rsi > 70 else ('超卖' if rsi != 'N/A' and rsi < 30 else '正常')
        markdown += f"| {item['date']} | {item['close']:.2f} | {rsi} | {status} |\n"
    markdown += "\n"
    return markdown

async def main():
    rsi_data = await get_rsi_markdown("0.002371", "002371", "北方华创")
    print(rsi_data)

if __name__ == "__main__":
    asyncio.run(main())
