import asyncio

from common.prompt.strategy_engine.stock_indicator_prompt import get_stock_indicator_prompt
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name

async def get_strategy_engine_analysis(stock_info: StockInfo) -> str:
    return await get_stock_indicator_prompt(stock_info)


if __name__ == '__main__':
    async def main():
        stock_info: StockInfo = get_stock_info_by_name('雷科防务')
        result = await get_strategy_engine_analysis(stock_info)
        print(result)
        #print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
