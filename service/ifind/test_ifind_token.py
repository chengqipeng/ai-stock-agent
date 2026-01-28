import asyncio

from service.ifind import refresh_token, choose_stocks
from get_client_token import THSTokenClient
from service.ifind.get_real_time_quotation import RealTimeQuotation
from service.ifind.smart_stock_picking import SmartStockPicking
from service.ifind.get_announcement_query import AnnouncementQuery


async def main():
    # 替换为您的refresh_token

    client = THSTokenClient(refresh_token)
    
    try:
        # 获取当前有效的access_token
        print("获取当前access_token...")
        token_result = await client.get_access_token()
        print(f"结果: {token_result}")

        access_token = token_result.get("data", {}).get("access_token")

        # 调用智能选股接口
        stock_picker = SmartStockPicking(access_token)
        result = await stock_picker.search(searchstring=choose_stocks, searchtype="stock")

        stock_lists = stock_picker.parse_tables(result.get('tables'))
        print(stock_lists)

        real_time_quotation = RealTimeQuotation(access_token)
        codes = [stock['股票代码'] for stock in stock_lists]
        quotation_result = await real_time_quotation.get_quotation(codes=codes)
        print(f"实时行情: {quotation_result}")

        # 调用公告查询接口
        announcement = AnnouncementQuery(access_token)
        announcement_result = await announcement.query(
            codes=codes,
            functionpara={
                "beginrDate": "2025-10-01",
                "endrDate": "2026-01-31",
                "keyWord": "半年度报告"
            }
        )
        print(announcement_result)
        # print(f"公告查询: {announcement.parse_tables(announcement_result)}")
        
        # 如果需要获取新的access_token（会使旧token失效）
        # print("获取新的access_token...")
        # result = await client.update_access_token()
        # print(f"结果: {result}")
        
    except Exception as e:
        print(f"请求失败: {e}")

if __name__ == "__main__":
    asyncio.run(main())