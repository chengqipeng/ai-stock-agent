import asyncio

from common.constants.stocks_data import get_stock_code
from service.can_slim.N_Products_Management_Highs_service import execute_N_Products_Management_Highs

if __name__ == "__main__":
    stock_name = "北方华创"
    stock_code = get_stock_code(stock_name)
    result = asyncio.run(execute_N_Products_Management_Highs(stock_code, stock_name, False))
    print(result)
