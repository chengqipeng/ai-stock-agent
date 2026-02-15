import asyncio
import json
from service.eastmoney.technical.abs.stock_indicator_base import get_stock_history_kline_max_min
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name


async def get_index_kline_data(stock_name, days=250):
    """获取指数K线数据的通用方法"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, days)


async def get_stock_relative_strength(stock_info: StockInfo, days=250):
    """
    获取个股相对指数的强度数据（个股收盘价/指数收盘价）
    
    Args:
        stock_name: 股票名称
        days: 获取天数，默认250天（约一年）
        
    Returns:
        dict: {日期: 相对强度值}
    """

    # 获取个股数据
    stock_data = await get_stock_history_kline_max_min(stock_info, days)
    
    # 获取指数数据
    if not stock_info.indices_stock_name:
        raise ValueError(f"股票 {stock_info.stock_code_normalize} 没有关联的指数信息")
    
    index_data = await get_index_kline_data(stock_info.indices_stock_name, days)
    
    # 计算相对强度
    result = {}
    for date, stock_values in stock_data.items():
        if date in index_data:
            result[date] = round(stock_values['close_price'] / index_data[date]['close_price'], 4)
    
    return result

async def main():
    # 测试相对强度
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    result = await get_stock_relative_strength(stock_info)
    print("相对强度数据:")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
