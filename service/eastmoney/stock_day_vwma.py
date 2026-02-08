import asyncio
from service.eastmoney.base_info.stock_indicator_base import (
    get_stock_day_range_kline, parse_klines_to_df, 
    process_indicator_data, INDICATOR_CONFIG
)

def calculate_vwma(klines, window=20):
    """计算成交量加权移动平均线 (VWMA)"""
    df = parse_klines_to_df(klines)
    
    pv = df['close'] * df['volume']
    pv_sum = pv.rolling(window=window).sum()
    vol_sum = df['volume'].rolling(window=window).sum()
    df['vwma'] = (pv_sum / vol_sum).round(2)
    
    return process_indicator_data(df, 'vwma')

async def get_vwma_markdown(secid, stock_code, stock_name):
    """将VWMA数据转换为markdown格式"""
    config = INDICATOR_CONFIG.get('vwma', {'kline_limit': 200, 'markdown_limit': 60})
    klines = await get_stock_day_range_kline(secid, config['kline_limit'])
    vwma_data = calculate_vwma(klines)
    
    markdown = f"## <{stock_code} {stock_name}> - VWMA数据\n\n"
    markdown += "| 日期 | 收盘价 | VWMA(20) |\n"
    markdown += "|------|--------|----------|\n"
    for item in vwma_data[:config['markdown_limit']]:
        markdown += f"| {item['date']} | {item['close']:.2f} | {item.get('vwma', 'N/A')} |\n"
    markdown += "\n"
    return markdown

async def main():
    result = await get_vwma_markdown("0.002371", "002371", "北方华创")
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
