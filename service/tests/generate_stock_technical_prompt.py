import asyncio

from common.constants.stocks_data import get_stock_code
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_structure_markdown import get_stock_markdown
from service.eastmoney.stock_similar_company import get_similar_companies_data
from service.eastmoney.stock_technical_markdown import get_technical_analysis_prompt
from service.tests.processor.operation_advice import get_operation_advice


async def main():
    stock_name = "北方华创"
    stock_code = get_stock_code(stock_name)
    advice_type = 2  # 1-4选择操作建议类型
    holding_price = None  # 如果advice_type为3或4，设置持仓价格
    
    technical_stock_result = await get_technical_analysis_prompt(normalize_stock_code(stock_code), stock_code, stock_name)
    operation_advice = get_operation_advice(advice_type, holding_price)
    if operation_advice:
        technical_stock_result += f"\n\n# {operation_advice}\n"
    
    print("\n\n")
    print(technical_stock_result)


if __name__ == "__main__":
    asyncio.run(main())
