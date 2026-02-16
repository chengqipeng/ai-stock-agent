from common.prompt.can_slim.L_or_Laggard_prompt import get_L_or_Laggard_prompt
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.llm.deepseek_client import DeepSeekClient

async def execute_L_or_Laggard(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """
    执行L领军股或落后股分析
    
    Args:
        stock_info: 股票信息对象
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    prompt = await get_L_or_Laggard_prompt(stock_info)

    print(prompt)
    print("\n =============================== \n")
    
    model = "deepseek-reasoner" if deep_thinking else "deepseek-chat"
    client = DeepSeekClient()
    
    result = ""
    async for content in client.chat_stream(
        messages=[{"role": "user", "content": prompt}],
        model=model
    ):
        result += content

    return result

if __name__ == "__main__":
    import asyncio
    
    async def main():
        stock_name = "北方华创"
        stock_info = get_stock_info_by_name(stock_name)
        result = await execute_L_or_Laggard(stock_info)
        print(result)
    
    asyncio.run(main())
