import asyncio
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from common.prompt.strategy_engine.stock_strategy_engine_prompt import get_strategy_engine_prompt


async def get_strategy_engine_analysis(stock_info: StockInfo) -> str:
    return await get_strategy_engine_prompt(stock_info)


if __name__ == '__main__':
    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        result = await get_strategy_engine_analysis(stock_info)
        print(result)

    asyncio.run(main())
