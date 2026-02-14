from common.utils.amount_utils import convert_amount_unit
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.http.http_utils import fetch_eastmoney_api, EASTMONEY_PUSH2HIS_API_URL
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.technical.abs.stock_indicator_base import get_stock_history_kline_max_min


async def get_fund_flow_history(stock_info: StockInfo):
    """获取资金流向历史数据"""
    cache_path = get_cache_path("fund_flow", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    # 获取数据
    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/fflow/daykline/get"
    params = {
        "lmt": "150",
        "klt": "101",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "secid": stock_info.secid,
        "_": 1715330901
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data") and data["data"].get("klines"):
        klines = data["data"]["klines"]
        klines.reverse()
        
        # 保存缓存
        save_cache(cache_path, klines)
        
        return klines
    else:
        raise Exception(f"未获取到股票 {stock_info.secid} 的资金流向历史数据")

async def get_fund_flow_history_markdown(stock_info: StockInfo, page_size = 120):
    """获取资金流向历史数据并转换为markdown"""
    klines = await get_fund_flow_history(stock_info)
    kline_max_min_map = await get_stock_history_kline_max_min(stock_info)
    header = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - 历史资金流向"
    markdown = f"""{header}
| 日期 | 收盘价 | 涨跌幅 | 主力净流入净额 | 主力净流入净占比 | 超大单净流入净额 | 超大单净流入净占比 | 大单净流入净额 | 大单净流入净占比 | 中单净流入净额 | 中单净流入占比 | 小单净流入净额 | 小单净流入净占比 | 当日最高价 | 当日最低价 | 换手率 | 成交量(万手) | 成交额 |
|-----|-------|-------|--------------|---------------|----------------|-----------------|-------------|----------------|-------------|--------------|--------------|---------------|----------|-----------|-------|------------|-------|
"""
    for kline in klines[:page_size]:
        fields = kline.split(',')
        if len(fields) >= 15:
            date = fields[0]
            kline_max_min_item = kline_max_min_map[date]
            close_price = round(float(fields[11]), 2) if fields[11] != '-' else '--'
            change_pct = f"{round(float(fields[12]), 2)}%" if fields[12] != '-' else "--"
            super_net = float(fields[5]) if fields[5] != '-' else 0
            super_pct = f"{round(float(fields[10]), 2)}%" if fields[10] != '-' else "--"
            super_net_str = convert_amount_unit(super_net)
            big_net = float(fields[4]) if fields[4] != '-' else 0
            big_net_str = convert_amount_unit(big_net)
            big_pct = f"{round(float(fields[9]), 2)}%" if fields[9] != '-' else "--"
            mid_net = float(fields[3]) if fields[3] != '-' else 0
            mid_net_str = convert_amount_unit(mid_net)
            mid_pct = f"{round(float(fields[8]), 2)}%" if fields[8] != '-' else "--"
            small_net = float(fields[2]) if fields[2] != '-' else 0
            small_net_str = convert_amount_unit(small_net)
            small_pct = f"{round(float(fields[7]), 2)}%" if fields[7] != '-' else "--"
            main_net = super_net + big_net
            main_net_str = convert_amount_unit(main_net)
            main_pct = f"{round(float(fields[6]), 2)}%" if fields[6] != '-' else "--"
            markdown += f"| {date} | {close_price} | {change_pct} | {main_net_str} | {main_pct} | {super_net_str} | {super_pct} | {big_net_str} | {big_pct} | {mid_net_str} | {mid_pct} | {small_net_str} | {small_pct} | {kline_max_min_item['high_price']} | {kline_max_min_item['low_price']} | {kline_max_min_item['change_hand']}% | {kline_max_min_item['trading_volume']} | {kline_max_min_item['trading_amount']} |\n"
    return markdown + "\n"


# async def get_stock_history_kline_max_min(stock_info: StockInfo):
#     """获取股票K线数据"""
#     cache_path = get_cache_path("kline", stock_info.stock_code)
#
#     # 检查缓存
#     cached_data = load_cache(cache_path)
#     if cached_data:
#         return cached_data
#
#     # 获取数据
#     url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/kline/get"
#     params = {
#         "secid": stock_info.secid,
#         "ut": "fa5fd1943c7b386f172d6893dbfba10b",
#         "fields1": "f1,f2,f3,f4,f5,f6",
#         "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
#         "klt": "101",
#         "fqt": "1",
#         "end": datetime.now().strftime("%Y%m%d"),
#         "smplmt": "460",
#         "lmt": "400"
#     }
#     data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
#     if data.get("data") and data["data"].get("klines"):
#         klines = data["data"]["klines"]
#         result = {}
#         for kline in klines:
#             fields = kline.split(',')
#             date = fields[0]
#             high_price = float(fields[2])
#             low_price = float(fields[3])
#             trading_volume = f"{round(float(fields[5])/10000, 2)}"
#             trading_amount = convert_amount_unit(float(fields[6]))
#             change_hands = float(fields[10])
#
#             result[date] = {"high_price": high_price, "low_price": low_price, "change_hands": change_hands, "trading_volume": trading_volume, "trading_amount": trading_amount}
#
#         # 保存缓存
#         save_cache(cache_path, result)
#
#         return result
#     else:
#         raise Exception(f"未获取到股票 {stock_info.secid} 的K线数据")


if __name__ == "__main__":
    import asyncio
    import json


    async def main():
        stock_name = "北方华创"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)
        # 测试 JSON 格式
        result = await get_fund_flow_history_markdown(stock_info)
        print("股东增减持数据 (JSON格式):")
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())

