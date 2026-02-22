import asyncio

from common.utils.stock_info_utils import get_stock_info_by_name
from service.strategy_engine.stock_strategy_engine_service import get_strategy_engine_analysis

async def main():
    stock_info = get_stock_info_by_name("中国卫通")
    prompt = await get_strategy_engine_analysis(stock_info)

    print(prompt)

if __name__ == "__main__":
    asyncio.run(main())
