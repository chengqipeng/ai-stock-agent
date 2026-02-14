import asyncio
import json
from service.eastmoney.technical.abs.stock_indicator_base import get_stock_history_kline_max_min
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name


async def get_shang_zheng_kline_data(stock_name="上证指数"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def get_shen_zheng_kline_data(stock_name="深证成指"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def get_chuang_ye_kline_data(stock_name="创业板指"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def get_shen_zheng_100_kline_data(stock_name="深证100"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def get_shang_shen_zheng_300_kline_data(stock_name="沪深300"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def get_shang_shen_50_kline_data(stock_name="上证50"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def get_b_kline_data(stock_name="Ｂ股指数"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def get_ke_chuang_kline_data(stock_name="科创综指"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def get_ke_chuang_50_kline_data(stock_name="创业板50"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def get_zhong_zheng_A500_kline_data(stock_name="中证A500"):
    """获取股票K线数据，提供默认值"""
    stock_info: StockInfo = get_stock_info_by_name(stock_name)
    return await get_stock_history_kline_max_min(stock_info, 250)

async def main():
    result = await get_ke_chuang_50_kline_data()
    print("K线数据:")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
