import asyncio
from service.tests.test_gemini_prompt import get_stock_markdown, normalize_stock_code

async def main():
    stock_code = "002371.SZ"
    stock_name = "北方华创"
    result = await get_stock_markdown(normalize_stock_code(stock_code), stock_name)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
