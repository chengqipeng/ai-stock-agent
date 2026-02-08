import asyncio

from common.constants.stocks_data import get_stock_code
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_structure_markdown import get_stock_markdown
from service.eastmoney.stock_similar_company import get_similar_companies_data
from service.eastmoney.stock_technical_markdown import get_technical_analysis_prompt


async def main():
    """
    目前不持有该股票，结合已提供的数据和上面的分析结论，本周该如何操作
    目前不持有该股票，结合已提供的数据和上面的分析结论，下周该如何操作
    目前该股票的持仓价格是<北方华创>，结合已提供的数据和上面的分析结论，下周该如何操作
    目前该股票的持仓价格是<北方华创>，结合已提供的数据和上面的分析结论，本周该如何操作
    """
    stock_name = "北方华创"
    stock_code = get_stock_code(stock_name)
    technical_stock_result = await get_technical_analysis_prompt(normalize_stock_code(stock_code), stock_code, stock_name)
    print("\n\n")
    print(technical_stock_result)


if __name__ == "__main__":
    asyncio.run(main())
