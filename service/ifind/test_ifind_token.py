import asyncio

from common.constants.stock_constants import refresh_token, choose_stocks
from get_client_token import THSTokenClient
from service.ifind.get_basic_data import BasicDataService
from service.ifind.get_date_sequence import DateSequenceService
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
        #announcement = AnnouncementQuery(access_token)
        #announcement_result = await announcement.query_with_financial_report(codes)

        #print(announcement_result)
        #print(f"公告查询: {await AnnouncementQuery.parse_result_with_pdf(announcement_result)}")

        # indipara = [{
        #     "indicator": "ths_roe_stock",
        #     "indiparams": ["8"]
        # }, {
        #     "indicator": "ths_roe_avg_by_ths_stock",
        #     "indiparams": ["8"]
        # }]
        #
        # basic_data_service = BasicDataService(access_token)
        # basic_data_result = await basic_data_service.get_basic_data(codes, indipara)
        # print(basic_data_result)
        indipara = [{"indicator": "ths_op_stock", "indiparams": ["", "1"]},
                     {"indicator": "ths_operating_total_revenue_stock", "indiparams": ["", "1"]},
                     {"indicator": "ths_operating_total_cost_stock", "indiparams": ["", "1"]},
                     {"indicator": "ths_np_atoopc_stock", "indiparams": ["", "1"]},
                     {"indicator": "ths_mo_product_name_stock", "indiparams": []}]
        date_sequence_service = DateSequenceService(access_token)
        date_sequence_service.get_date_sequence(codes, indipara)
        
        # 如果需要获取新的access_token（会使旧token失效）
        # print("获取新的access_token...")
        # result = await client.update_access_token()
        # print(f"结果: {result}")
        print("0000")
        
    except Exception as e:
        print(f"请求失败: {e}")

if __name__ == "__main__":
    asyncio.run(main())