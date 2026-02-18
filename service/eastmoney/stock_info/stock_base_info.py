from common.utils.amount_utils import convert_amount_unit
from common.http.http_utils import EASTMONEY_PUSH2_API_URL, fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name


async def get_stock_detail(stock_info: StockInfo):
    """获取股票详细数据"""
    url = f"{EASTMONEY_PUSH2_API_URL}/stock/get"
    params = {
        "invt": "2",
        "fltt": "1",
        "fields": "f58,f734,f107,f57,f43,f59,f169,f301,f60,f170,f152,f177,f111,f46,f44,f45,f47,f260,f48,f261,f279,f277,f278,f288,f19,f17,f531,f15,f13,f11,f20,f18,f16,f14,f12,f39,f37,f35,f33,f31,f40,f38,f36,f34,f32,f211,f212,f213,f214,f215,f210,f209,f208,f207,f206,f161,f49,f171,f50,f86,f84,f85,f168,f108,f116,f167,f164,f162,f163,f92,f71,f117,f292,f51,f52,f191,f192,f262,f294,f181,f295,f269,f270,f256,f257,f285,f286,f748,f747",
        "secid": stock_info.secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "wbp2u": "|0|0|0|web",
        "dect": "1"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data"):
        return data["data"]
    else:
        raise Exception(f"未获取到股票 {stock_info.secid} 的详细数据")

async def get_stock_base_info_json(stock_info: StockInfo, fields: list[str] = None):
    """获取股票基本信息并转换为JSON格式
    
    Args:
        stock_info: 股票信息对象
        fields: 需要返回的字段列表（英文），为None时返回所有字段
                可选字段: stock_code, stock_name, latest_price, change_percent, change_amount,
                         volume, amount, pe_ratio, total_market_value, circulation_market_value,
                         turnover_rate, volume_ratio, high, low, open, prev_close, limit_up, limit_down
    """
    detail_data = await get_stock_detail(stock_info)
    
    field_mapping = {
        "stock_code": ("股票代码", detail_data.get('f57', '--')),
        "stock_name": ("股票名称", detail_data.get('f58', '--')),
        "latest_price": ("最新价", round(detail_data.get('f43', 0) / 100, 2) if detail_data.get('f43') else '--'),
        "change_percent": ("涨跌幅", round(detail_data.get('f170', 0) / 100, 2) if detail_data.get('f170') else '--'),
        "change_amount": ("涨跌额", round(detail_data.get('f169', 0) / 100, 2) if detail_data.get('f169') else '--'),
        "volume": ("成交量", convert_amount_unit(detail_data.get('f47'))),
        "amount": ("成交额", convert_amount_unit(detail_data.get('f48'))),
        "pe_ratio": ("市盈率", round(detail_data.get('f162', 0) / 100, 2) if detail_data.get('f162') else '--'),
        "total_market_value": ("总市值", convert_amount_unit(detail_data.get('f116'))),
        "circulation_market_value": ("流通市值", convert_amount_unit(detail_data.get('f117'))),
        "turnover_rate": ("换手率", round(detail_data.get('f168', 0) / 100, 2) if detail_data.get('f168') else '--'),
        "volume_ratio": ("量比", round(detail_data.get('f50', 0) / 100, 2) if detail_data.get('f50') else '--'),
        "high": ("最高", round(detail_data.get('f44', 0) / 100, 2) if detail_data.get('f44') else '--'),
        "low": ("最低", round(detail_data.get('f45', 0) / 100, 2) if detail_data.get('f45') else '--'),
        "open": ("今开", round(detail_data.get('f46', 0) / 100, 2) if detail_data.get('f46') else '--'),
        "prev_close": ("昨收", round(detail_data.get('f60', 0) / 100, 2) if detail_data.get('f60') else '--'),
        "limit_up": ("涨停", round(detail_data.get('f51', 0) / 100, 2) if detail_data.get('f51') else '--'),
        "limit_down": ("跌停", round(detail_data.get('f52', 0) / 100, 2) if detail_data.get('f52') else '--')
    }
    
    if fields is None:
        return {cn_name: value for _, (cn_name, value) in field_mapping.items()}
    
    return {field_mapping[f][0]: field_mapping[f][1] for f in fields if f in field_mapping}


if __name__ == "__main__":
    import asyncio
    
    async def main():
        stock_info: StockInfo = get_stock_info_by_name("北方华创")
        result = await get_stock_base_info_json(stock_info)
        print(result)
    
    asyncio.run(main())
