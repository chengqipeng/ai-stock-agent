import asyncio

from common.constants.stocks_data import get_stock_code
from service.can_slim.A_Earnings_Increases_service import execute_A_Earnings_Increases

if __name__ == "__main__":
    stock_name = "北方华创"
    stock_code = get_stock_code(stock_name)
    result = asyncio.run(execute_A_Earnings_Increases(stock_code, stock_name, False))
    print(result)
