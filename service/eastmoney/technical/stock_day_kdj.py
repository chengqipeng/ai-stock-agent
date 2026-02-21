import asyncio

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.technical.abs.stock_indicator_base import parse_klines_to_df, process_indicator_data, INDICATOR_CONFIG
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline

INDICATOR_CONFIG['kdj'] = {'kline_limit': 300, 'tail_limit': 300, 'markdown_limit': 200}


async def calculate_kdj(stock_info: StockInfo, n=9, m1=3, m2=3):
    """计算KDJ指标"""
    klines = await get_stock_day_range_kline(stock_info)
    df = parse_klines_to_df(klines)

    low_list = df['low_price'].rolling(window=n).min()
    high_list = df['high_price'].rolling(window=n).max()

    rsv = ((df['close_price'] - low_list) / (high_list - low_list) * 100).fillna(0)

    df['k'] = rsv.ewm(com=m1 - 1, adjust=False).mean().round(4)
    df['d'] = df['k'].ewm(com=m2 - 1, adjust=False).mean().round(4)
    df['j'] = (3 * df['k'] - 2 * df['d']).round(4)

    return process_indicator_data(df, 'kdj')


async def get_kdj_markdown(stock_info: StockInfo):
    """将KDJ数据转换为markdown格式"""
    config = INDICATOR_CONFIG['kdj']
    kdj_data = await calculate_kdj(stock_info)

    markdown = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - KDJ数据\n\n"
    markdown += "| 日期 | 收盘价 | K | D | J |\n"
    markdown += "|------|--------|---|---|---|\n"
    for item in kdj_data[:config['markdown_limit']]:
        markdown += f"| {item['date']} | {item['close_price']:.2f} | {item['k']} | {item['d']} | {item['j']} |\n"
    markdown += "\n"
    return markdown


async def main():
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    print(await get_kdj_markdown(stock_info))

if __name__ == "__main__":
    asyncio.run(main())
