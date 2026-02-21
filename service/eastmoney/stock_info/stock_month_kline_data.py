from datetime import datetime, timedelta
from common.http.http_utils import fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo
from common.utils.cache_utils import get_cache_path, load_cache, save_cache


def _get_cache_date() -> str:
    today = datetime.now()
    offset = today.weekday() - 4  # 4 = Friday
    if offset > 0:
        today -= timedelta(days=offset)
    return today.strftime("%Y%m%d")


async def get_stock_month_kline(stock_info: StockInfo, beg: str = "0", end: str = "20500101", limit: int = 1000000):
    """获取股票月K线数据
    
    Args:
        stock_info: 股票信息对象
        beg: 开始日期，格式YYYYMMDD，默认"0"表示从最早开始
        end: 结束日期，格式YYYYMMDD，默认"20500101"
        limit: 数据条数限制，默认1000000
    
    Returns:
        dict: 包含K线数据的字典
    """
    cache_path = get_cache_path("month_kline_" + beg + "_" + end + "_" + _get_cache_date(), stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    # 获取数据
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": stock_info.secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "102",  # 102代表周线、103表示月K线
        "fqt": "1",    # 前复权
        "beg": beg,
        "end": end,
        "smplmt": "460",
        "lmt": str(limit)
    }
    
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    
    if data.get("data"):
        # 保存缓存
        save_cache(cache_path, data["data"])
        return data["data"]
    else:
        raise Exception(f"未获取到股票 {stock_info.secid} 的月K线数据")


async def get_stock_month_kline_list(stock_info: StockInfo, beg: str = "0", end: str = "20500101", limit: int = 1000000):
    """获取股票月K线数据列表
    
    Returns:
        list: K线数据列表，每条数据格式为 [日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率]
    """
    kline_data = await get_stock_month_kline(stock_info, beg, end, limit)
    klines = kline_data.get("klines", [])
    
    result = []
    for kline in klines:
        parts = kline.split(",")
        result.append({
            "日期": parts[0],
            "开盘": float(parts[1]),
            "收盘": float(parts[2]),
            "最高": float(parts[3]),
            "最低": float(parts[4]),
            "成交量": int(parts[5]),
            "成交额": float(parts[6]),
            "振幅": float(parts[7]),
            "涨跌幅": float(parts[8]),
            "涨跌额": float(parts[9]),
            "换手率": float(parts[10])
        })
    
    return result


if __name__ == "__main__":
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name
    
    async def main():
        stock_name = "北方华创"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)
        result = await get_stock_month_kline_list(stock_info)
        print(f"获取到 {len(result)} 条月K线数据")
        if result:
            print("最近3条数据:")
            for item in result:
                print(item)
    
    asyncio.run(main())
