import asyncio

from common.constants.stocks_data import get_stock_code
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_markdown_result import get_stock_markdown
from service.eastmoney.stock_similar_company import get_similar_companies_data
from service.gemini.gemini_app_client import gemini_generate


async def main():
    """
    目前不持有该股票，结合已提供的数据和你的分析，本周我该如何操作
    """
    stock_name = "三花智控"
    stock_code = get_stock_code(stock_name)
    similar_company_num = 5

    #similar_prompt = await get_similar_companies_data(stock_name, stock_code, similar_company_num)

    main_stock_result = await get_stock_markdown(normalize_stock_code(stock_code), stock_name)
    #main_stock_result += similar_prompt
    #print("\n\n")
    #print(main_stock_result)

    result = await gemini_generate(prompt=f"{main_stock_result}")
    print(result)




if __name__ == "__main__":
    asyncio.run(main())
