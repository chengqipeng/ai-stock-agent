from common.utils.amount_utils import convert_amount_unit
from common.http.http_utils import EASTMONEY_PUSH_API_URL, fetch_eastmoney_api, EASTMONEY_PUSH2HIS_API_URL
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name


# async def get_main_fund_flow(stock_info: StockInfo):
#     """获取资金流向数据"""
#     url = f"{EASTMONEY_PUSH_API_URL}/ulist.np/get"
#     params = {
#         "fltt": "2",
#         "secids": stock_info.secid,
#         "fields": "f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f64,f65,f70,f71,f76,f77,f82,f83,f164,f166,f168,f170,f172,f252,f253,f254,f255,f256,f124,f6,f278,f279,f280,f281,f282",
#         "ut": "b2884a393a59ad64002292a3e90d46a5"
#     }
#     data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
#     if data.get("data") and data["data"].get("diff"):
#         result = []
#         for stock in data["data"]["diff"]:
#             amount = stock.get('f6', 1)
#             super_ratio = round(stock.get('f66', 0) / amount * 100, 2) if amount else 0
#             big_ratio = round(stock.get('f72', 0) / amount * 100, 2) if amount else 0
#             mid_ratio = round(stock.get('f78', 0) / amount * 100, 2) if amount else 0
#             small_ratio = round(stock.get('f84', 0) / amount * 100, 2) if amount else 0
#             stock_info = {
#                 "成交额": convert_amount_unit(amount),
#                 "主力净流入": convert_amount_unit(stock.get('f62')),
#                 "主力净流入占比": f"{round(stock.get('f184', 0), 2)}%",
#                 "超大单净流入": convert_amount_unit(stock.get('f66')),
#                 "超大单净比": f"{round(super_ratio, 2)}%",
#                 "大单净流入": convert_amount_unit(stock.get('f72')),
#                 "大单净比": f"{round(big_ratio, 2)}%",
#                 "中单净流入": convert_amount_unit(stock.get('f78')),
#                 "中单净比": f"{round(mid_ratio, 2)}%",
#                 "小单净流入": convert_amount_unit(stock.get('f84')),
#                 "小单净比": f"{round(small_ratio, 2)}%",
#                 "超大单流入": f"{convert_amount_unit(stock.get('f64'))}",
#                 "超大单流出": f"{convert_amount_unit(stock.get('f65'))}",
#                 "大单流入": f"{convert_amount_unit(stock.get('f70'))}",
#                 "大单流出": f"{convert_amount_unit(stock.get('f71'))}",
#                 "中单流入": f"{convert_amount_unit(stock.get('f76'))}",
#                 "中单流出": f"{convert_amount_unit(stock.get('f77'))}",
#                 "小单流入": f"{convert_amount_unit(stock.get('f82'))}",
#                 "小单流出": f"{convert_amount_unit(stock.get('f83'))}"
#             }
#             result.append(stock_info)
#         return result
#     else:
#         raise Exception(f"未获取到股票 {stock_info.secid} 的主力资金流向数据")

# async def get_main_fund_flow_markdown(stock_info: StockInfo):
#     """获取主力资金流向并转换为markdown"""
#     fund_flow_data = await get_main_fund_flow(stock_info)
#     if not fund_flow_data:
#         return ""
#     flow_data = fund_flow_data[0]
#     header = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - 当日资金流向"
#     return f"""{header}
# - **成交额**: {flow_data.get('成交额', '--')}
# - **主力净流入**: {flow_data.get('主力净流入', '--')}
# - **超大单净流入**: {flow_data.get('超大单净流入', '--')}
# - **大单净流入**: {flow_data.get('大单净流入', '--')}
# - **中单净流入**: {flow_data.get('中单净流入', '--')}
# - **小单净流入**: {flow_data.get('小单净流入', '--')}
# - **主力净流入占比**: {flow_data.get('主力净流入占比', '--')}
# - **超大单净流入占比**: {flow_data.get('超大单净比', '--')}
# - **大单净流入占比**: {flow_data.get('大单净比', '--')}
# - **中单净流入占比**: {flow_data.get('中单净比', '--')}
# - **小单净流入占比**: {flow_data.get('小单净比', '--')}
# - **超大单流入**: {flow_data.get('超大单流入', '--')}
# - **超大单流出**: {flow_data.get('超大单流出', '--')}
# - **大单流入**: {flow_data.get('大单流入', '--')}
# - **大单流出**: {flow_data.get('大单流出', '--')}
# - **中单流入**: {flow_data.get('中单流入', '--')}
# - **中单流出**: {flow_data.get('中单流出', '--')}
# - **小单流入**: {flow_data.get('小单流入', '--')}
# - **小单流出**: {flow_data.get('小单流出', '--')}
# """

# async def get_trade_distribution_markdown(secids="0.002371"):
#     """获取实时成交分布并转换为markdown"""
#     fund_flow_data = await get_main_fund_flow(secids)
#     if not fund_flow_data:
#         return ""
#     flow_data = fund_flow_data[0]
#     return f"""## 实时成交分布
# - **超大单流入**: {flow_data.get('超大单流入', '--')}
# - **超大单流出**: {flow_data.get('超大单流出', '--')}
# - **大单流入**: {flow_data.get('大单流入', '--')}
# - **大单流出**: {flow_data.get('大单流出', '--')}
# - **中单流入**: {flow_data.get('中单流入', '--')}
# - **中单流出**: {flow_data.get('中单流出', '--')}
# - **小单流入**: {flow_data.get('小单流入', '--')}
# - **小单流出**: {flow_data.get('小单流出', '--')}"""


# if __name__ == "__main__":
#     import asyncio
#
#     async def main():
#         stock_info: StockInfo = get_stock_info_by_name("北方华创")
#         result = await get_main_fund_flow_markdown(stock_info)
#         print(result)
#
#     asyncio.run(main())
