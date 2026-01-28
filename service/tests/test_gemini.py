import asyncio

from common.prompt.stock_analysis_prompt import STOCK_ANALYSIS_PROMPT
from common.utils.string_formatter import StringFormatter
from service.ifind import refresh_token, choose_stocks
from service.ifind.get_client_token import THSTokenClient
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
        quotation_result = real_time_quotation.parse_tables(quotation_result)
        print(f"实时行情: {quotation_result}")

        prompt = StringFormatter.format(STOCK_ANALYSIS_PROMPT, codes[0], quotation_result)
        print(prompt)
        
    except Exception as e:
        print(f"请求失败: {e}")

if __name__ == "__main__":
    asyncio.run(main())