import asyncio

from common.utils.stock_info_utils import get_stock_info_by_name
from service.can_slim.A_Earnings_Increases_service import execute_A_Earnings_Increases

if __name__ == "__main__":
    stock_name = "北方华创"
    stock_info = get_stock_info_by_name(stock_name)
    result = asyncio.run(execute_A_Earnings_Increases(stock_info, False))
    print(result)
