import asyncio

from common.utils.stock_info_utils import get_stock_info_by_name
from service.k_strategy.stock_k_strategy_service import get_k_strategy_analysis

async def main():
    stock_info = get_stock_info_by_name("中国卫通")
    prompt = await get_k_strategy_analysis(stock_info)

    print(prompt)

if __name__ == "__main__":
    asyncio.run(main())
