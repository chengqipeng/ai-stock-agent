import asyncio

from common.constants.stocks_data import get_stock_code
from service.can_slim.C_Quarterly_Earnings_service import execute_C_Quarterly_Earnings

if __name__ == "__main__":
    stock_name = "北方华创"
    stock_code = get_stock_code(stock_name)
    result = asyncio.run(execute_C_Quarterly_Earnings(stock_code, stock_name, False))
    print(result)
