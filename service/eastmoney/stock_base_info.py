from common.utils.amount_utils import convert_amount_unit
from .common_utils import EASTMONEY_PUSH2_API_URL, fetch_eastmoney_api


async def get_stock_detail(secid="0.002371"):
    """获取股票详细数据"""
    url = f"{EASTMONEY_PUSH2_API_URL}/stock/get"
    params = {
        "invt": "2",
        "fltt": "1",
        "fields": "f58,f734,f107,f57,f43,f59,f169,f301,f60,f170,f152,f177,f111,f46,f44,f45,f47,f260,f48,f261,f279,f277,f278,f288,f19,f17,f531,f15,f13,f11,f20,f18,f16,f14,f12,f39,f37,f35,f33,f31,f40,f38,f36,f34,f32,f211,f212,f213,f214,f215,f210,f209,f208,f207,f206,f161,f49,f171,f50,f86,f84,f85,f168,f108,f116,f167,f164,f162,f163,f92,f71,f117,f292,f51,f52,f191,f192,f262,f294,f181,f295,f269,f270,f256,f257,f285,f286,f748,f747",
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "wbp2u": "|0|0|0|web",
        "dect": "1"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data"):
        return data["data"]
    else:
        raise Exception(f"未获取到股票 {secid} 的详细数据")


async def get_stock_base_info_markdown(secid="0.002371"):
    """获取股票基本信息并转换为markdown"""
    detail_data = await get_stock_detail(secid)
    markdown = """## 股票基本信息\n"""
    markdown += f"- **股票代码**: {detail_data.get('f57', '--')}\n"
    markdown += f"- **股票名称**: {detail_data.get('f58', '--')}\n"
    markdown += f"- **最新价**: {round(detail_data.get('f43', 0) / 100, 2) if detail_data.get('f43') else '--'}\n"
    markdown += f"- **涨跌幅**: {round(detail_data.get('f170', 0) / 100, 2) if detail_data.get('f170') else '--'}%\n"
    markdown += f"- **涨跌额**: {round(detail_data.get('f169', 0) / 100, 2) if detail_data.get('f169') else '--'}\n"
    markdown += f"- **成交量**: {convert_amount_unit(detail_data.get('f47'))}\n"
    markdown += f"- **成交额**: {convert_amount_unit(detail_data.get('f48'))}\n"
    markdown += f"- **市盈率**: {round(detail_data.get('f162', 0) / 100, 2) if detail_data.get('f162') else '--'}\n"
    markdown += f"- **总市值**: {convert_amount_unit(detail_data.get('f116'))}\n"
    markdown += f"- **流通市值**: {convert_amount_unit(detail_data.get('f117'))}\n"
    markdown += f"- **换手率**: {round(detail_data.get('f168', 0) / 100, 2) if detail_data.get('f168') else '--'}%\n"
    markdown += f"- **量比**: {round(detail_data.get('f50', 0) / 100, 2) if detail_data.get('f50') else '--'}\n"
    markdown += f"- **最高**: {round(detail_data.get('f44', 0) / 100, 2) if detail_data.get('f44') else '--'}\n"
    markdown += f"- **最低**: {round(detail_data.get('f45', 0) / 100, 2) if detail_data.get('f45') else '--'}\n"
    markdown += f"- **今开**: {round(detail_data.get('f46', 0) / 100, 2) if detail_data.get('f46') else '--'}\n"
    markdown += f"- **昨收**: {round(detail_data.get('f60', 0) / 100, 2) if detail_data.get('f60') else '--'}\n"
    markdown += f"- **涨停**: {round(detail_data.get('f51', 0) / 100, 2) if detail_data.get('f51') else '--'}\n"
    markdown += f"- **跌停**: {round(detail_data.get('f52', 0) / 100, 2) if detail_data.get('f52') else '--'}\n"
    return markdown
