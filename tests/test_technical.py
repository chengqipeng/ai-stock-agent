import asyncio

from common.utils.stock_info_utils import get_stock_info_by_name
from service.llm.deepseek_client import DeepSeekClient
from service.technical.stock_technical_service import get_technical_indicators_prompt

async def main():
    stock_info = get_stock_info_by_name("中国卫通")
    prompt = await get_technical_indicators_prompt(stock_info)

    print(prompt)

    # client = DeepSeekClient()
    # response = await client.chat(
    #     messages=[{"role": "user", "content": prompt}],
    #     model="deepseek-reasoner",
    #     temperature=1.0
    # )
    # print(response.get("choices", [{}])[0].get("message", {}).get("content", ""))

if __name__ == "__main__":
    asyncio.run(main())
