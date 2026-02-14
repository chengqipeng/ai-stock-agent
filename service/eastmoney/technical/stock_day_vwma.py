import asyncio

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.technical.abs.stock_indicator_base import parse_klines_to_df, process_indicator_data, \
    INDICATOR_CONFIG, get_stock_day_range_kline

"""
如果你只是做单点计算： 只需要 20 条。
如果你要判断趋势方向： 建议加载 30 条。
如果你要配合 CAN SLIM 选股： 建议预留 60 条（一个季度），这样可以清晰看到机构在最近一个业绩周期内的吸筹成本。
"""
async def calculate_vwma(stock_info: StockInfo, window=20):
    """计算成交量加权移动平均线 (VWMA)"""
    klines = await get_stock_day_range_kline(stock_info)

    df = parse_klines_to_df(klines)
    
    pv = df['close_price'] * df['trading_volume']
    pv_sum = pv.rolling(window=window).sum()
    vol_sum = df['trading_volume'].rolling(window=window).sum()
    df['vwma'] = (pv_sum / vol_sum).round(2)
    
    return process_indicator_data(df, 'vwma')

async def get_vwma_markdown(stock_info: StockInfo):
    """将VWMA数据转换为markdown格式"""
    config = INDICATOR_CONFIG.get('vwma', {'kline_limit': 200, 'markdown_limit': 60})
    vwma_data = await calculate_vwma(stock_info)
    
    markdown = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - VWMA数据\n\n"
    markdown += "| 日期 | 收盘价 | VWMA(20) |\n"
    markdown += "|------|--------|----------|\n"
    for item in vwma_data[:config['markdown_limit']]:
        markdown += f"| {item['date']} | {item['close_price']:.2f} | {item.get('vwma', 'N/A')} |\n"
    markdown += "\n"
    return markdown

async def main():
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    result = await get_vwma_markdown(stock_info)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
