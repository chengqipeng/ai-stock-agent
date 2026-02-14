import asyncio

from common.utils.stock_info_utils import get_stock_info_by_name
from service.eastmoney.stock_structure_markdown import get_stock_markdown
from service.processor.operation_advice import get_operation_advice


async def main():
    stock_name = "北方华创"
    stock_info = get_stock_info_by_name(stock_name)
    advice_type = 1  # 1-4选择操作建议类型
    holding_price = None  # 如果advice_type为3或4，设置持仓价格
    
    main_stock_result = await get_stock_markdown(stock_info)
    operation_advice = get_operation_advice(advice_type, holding_price)
    if operation_advice:
        main_stock_result += f"# {operation_advice}\n"
    
    print("\n\n")
    print(main_stock_result)


if __name__ == "__main__":
    asyncio.run(main())
