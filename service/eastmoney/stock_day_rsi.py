import asyncio
from service.eastmoney.base_info.stock_indicator_base import (
    get_stock_day_range_kline, parse_klines_to_df, 
    process_indicator_data, INDICATOR_CONFIG
)

"""
CAN SLIM 选股逻辑下的数据需求在威廉·欧奈尔的框架下，RSI 主要是为了辅助判断“杯柄形”（Cup with a Handle）的形成过程，这需要更宏观的数据视野：
A. 识别“底部的底部” (3-6个月数据)CAN SLIM 强调观察个股在过去 6 个月到 1 年的表现。
数据量： 约 120 - 250 条 日线数据。
逻辑： 只有拥有半年的数据，你才能判断 RSI 是否从长期的低位超卖区（<30）逐渐抬升至强力支撑区（40-50），这标志着机构（Institutional Sponsorship）开始建仓。
B. 确认“相对强度” (RS Rating)虽然你问的是 RSI，但在 CAN SLIM 中，RS Rating（相对强度评级）才是核心。
数据量： 欧奈尔计算 RS 评级通常需要 过去 52 周（约 250 个交易日） 的数据。
逻辑： RSI 只能告诉你股票“自己跟自己比”快不快，而 250 条数据能让你算出该股在全 A 股市场中的百分比排名。

实战建议：针对不同周期的配置如果你正在编写代码实现量化筛选，建议根据分析目的加载不同长度的数据：分析维度推荐加载数据条数 (日线)原因短期择时 (Pivot Point)30 - 50 条确认突破瞬间的 RSI 动能爆发。
形态识别 (Cup with Handle)150 条覆盖杯身构造期（通常 7-65 周）的 RSI 波动。
全系统扫描 (CAN SLIM Filter)250 条 (约1年)保证 RSI 计算完全平滑，并能计算年度 EPS 增长和相对强度排名。
"""

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
    
    return process_indicator_data(df, 'rsi')

async def get_rsi_markdown(secid, stock_code, stock_name):
    """将RSI数据转换为markdown格式"""
    config = INDICATOR_CONFIG['rsi']
    klines = await get_stock_day_range_kline(secid, config['kline_limit'])
    rsi_data = calculate_rsi(klines)

    markdown = f"## <{stock_code} {stock_name}> - RSI数据\n\n"
    markdown += "| 日期 | 收盘价 | RSI(14) | 状态 |\n"
    markdown += "|------|--------|---------|------|\n"
    for item in rsi_data[:config['markdown_limit']]:
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
