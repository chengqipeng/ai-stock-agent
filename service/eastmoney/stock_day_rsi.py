import asyncio
import pandas as pd
from service.eastmoney.stock_day_range_kline import get_stock_day_range_kline

#东方财富是6 12 24，行业标准取14
def calculate_rsi(klines, window=14):
    """计算 RSI 指标"""
    close_prices = []
    dates = []
    for kline in klines:
        fields = kline.split(',')
        dates.append(fields[0])
        close_prices.append(float(fields[2]))

    df = pd.DataFrame({'date': dates, 'close': close_prices})
    
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta.where(delta < 0, 0))
    
    avg_gain = gain.ewm(alpha=1/window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/window, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    df['rsi'] = (100 - (100 / (1 + rs))).round(2)
    
    df = df.tail(200)
    return df.to_dict('records')[::-1]

async def get_rsi_markdown(secid, stock_code, stock_name):
    """将RSI数据转换为markdown格式"""
    klines = await get_stock_day_range_kline(secid, 200)
    rsi_data = calculate_rsi(klines)

    markdown = f"## <{stock_code} {stock_name}> - RSI数据\n\n"
    markdown += "| 日期 | 收盘价 | RSI(14) | 状态 |\n"
    markdown += "|------|--------|---------|------|\n"
    for item in rsi_data[:150]:
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
