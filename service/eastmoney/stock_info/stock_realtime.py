from common.utils.amount_utils import convert_amount_unit
from common.http.http_utils import EASTMONEY_PUSH_API_URL, fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo

async def get_stock_realtime(stock_info: StockInfo):
    """获取股票实时数据"""
    url = f"{EASTMONEY_PUSH_API_URL}/stock/get"
    params = {
        "fltt": "2",
        "invt": "2",
        "secid": stock_info.secid,
        "fields": "f57,f58,f43,f47,f48,f168,f169,f170,f152",
        "ut": "b2884a393a59ad64002292a3e90d46a5"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data"):
        return data["data"]
    else:
        raise Exception(f"未获取到股票 {stock_info.secid} 的实时数据")


# async def get_stock_realtime_markdown(stock_info: StockInfo):
#     """获取实时交易信息并转换为markdown"""
#     realtime_data = await get_stock_realtime(stock_info)
#     header = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - 当日交易信息"
#     return f"""{header}
# - **股票代码**: {realtime_data.get('f57', '--')}
# - **最新价**: {realtime_data.get('f43', '--')}
# - **涨跌幅**: {realtime_data.get('f170', '--')}%
# - **涨跌额**: {realtime_data.get('f169', '--')}
# - **成交量**: {convert_amount_unit(realtime_data.get('f47', "-"))}
# - **成交额**: {convert_amount_unit(realtime_data.get('f48', "-"))}
# - **换手率**: {realtime_data.get('f168', '--')}% \n"""


async def get_stock_realtime_json(stock_info: StockInfo, fields: list[str] = None):
    """获取实时交易信息并转换为JSON格式
    
    Args:
        stock_info: 股票信息对象
        fields: 需要返回的字段列表（英文），为None时返回所有字段
                可选字段: stock_name, stock_code, latest_price, change_percent, 
                         change_amount, volume, amount, turnover_rate
    """
    realtime_data = await get_stock_realtime(stock_info)
    
    field_mapping = {
        "stock_name": ("股票名称", stock_info.stock_name),
        "stock_code": ("股票代码", stock_info.stock_code_normalize),
        "latest_price": ("最新价", realtime_data.get('f43', '--')),
        "change_percent": ("涨跌幅", realtime_data.get('f170', '--')),
        "change_amount": ("涨跌额", realtime_data.get('f169', '--')),
        "volume": ("成交量", convert_amount_unit(realtime_data.get('f47', "-"))),
        "amount": ("成交额", convert_amount_unit(realtime_data.get('f48', "-"))),
        "turnover_rate": ("换手率", realtime_data.get('f168', '--'))
    }
    
    if fields is None:
        return {cn_name: value for _, (cn_name, value) in field_mapping.items()}
    
    return {field_mapping[f][0]: field_mapping[f][1] for f in fields if f in field_mapping}


if __name__ == "__main__":
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name
    
    async def main():
        stock_name = "北方华创"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)
        result = await get_stock_realtime_json(stock_info, ['stock_name', 'stock_code', 'volume'])
        print(result)
    
    asyncio.run(main())
