from common.utils.amount_utils import convert_amount_unit
from .common_utils import EASTMONEY_PUSH_API_URL, fetch_eastmoney_api


async def get_stock_realtime(secid="1.601698"):
    """获取股票实时数据"""
    url = f"{EASTMONEY_PUSH_API_URL}/stock/get"
    params = {
        "fltt": "2",
        "invt": "2",
        "secid": secid,
        "fields": "f57,f58,f43,f47,f48,f168,f169,f170,f152",
        "ut": "b2884a393a59ad64002292a3e90d46a5"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data"):
        return data["data"]
    else:
        raise Exception(f"未获取到股票 {secid} 的实时数据")


async def get_stock_realtime_markdown(secid="1.601698"):
    """获取实时交易信息并转换为markdown"""
    realtime_data = await get_stock_realtime(secid)
    return f"""## 当日交易信息
- **股票代码**: {realtime_data.get('f57', '--')}
- **最新价**: {realtime_data.get('f43', '--')}
- **涨跌幅**: {realtime_data.get('f170', '--')}%
- **涨跌额**: {realtime_data.get('f169', '--')}
- **成交量**: {convert_amount_unit(realtime_data.get('f47', "-"))}
- **成交额**: {convert_amount_unit(realtime_data.get('f48', "-"))}
- **换手率**: {realtime_data.get('f168', '--')}%"""
