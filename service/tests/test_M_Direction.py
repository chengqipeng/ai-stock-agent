import asyncio

from common.utils.stock_info_utils import get_stock_info_by_name
from service.can_slim.M_Direction_service import execute_M_Direction

if __name__ == "__main__":
    stock_name = "北方华创"
    stock_info = get_stock_info_by_name(stock_name)
    result = asyncio.run(execute_M_Direction(stock_info, False))
    print(result)
