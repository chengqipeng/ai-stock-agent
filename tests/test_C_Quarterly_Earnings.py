import asyncio

from common.utils.stock_info_utils import get_stock_info_by_name
from service.can_slim.can_slim_service import execute_can_slim_analysis

if __name__ == "__main__":
    stock_name = "北方华创"
    stock_info = get_stock_info_by_name(stock_name)
    result = asyncio.run(execute_can_slim_analysis('C', stock_info, False))
    print(result)
