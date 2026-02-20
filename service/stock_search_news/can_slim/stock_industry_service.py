import asyncio

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.stock_info.stock_busi_desc import get_stock_board_type
from service.llm.deepseek_client import DeepSeekClient
from common.prompt.stock_industry_prompt import get_industry_prompt

async def get_industry_result(stock_info: StockInfo) -> str:
    """调用DeepSeek大模型并返回content结果"""
    industry_data = await get_stock_board_type(stock_info)
    prompt = await get_industry_prompt(stock_info, industry_data)
    client = DeepSeekClient()
    response = await client.chat(
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )
    return response.get("choices", [{}])[0].get("message", {}).get("content", "")


if __name__ == '__main__':
    stock_info = get_stock_info_by_name("北方华创")
    result = asyncio.run(get_industry_result(stock_info))
    print(result)
