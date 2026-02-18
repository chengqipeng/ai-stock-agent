import asyncio
from common.utils.stock_info_utils import get_stock_info_by_name
from service.can_slim.can_slim_service import execute_can_slim_analysis

if __name__ == "__main__":
    stock_name = "北方华创"
    stock_info = get_stock_info_by_name(stock_name)
    
    # 测试抗跌性分析
    result = asyncio.run(execute_can_slim_analysis('L', stock_info))
    print("抗跌性分析结果:")
    print(result)
