from common.prompt.can_slim.C_Quarterly_Earnings_prompt import get_C_Quarterly_Earnings_prompt
from service.llm.deepseek_client import DeepSeekClient


async def execute_C_Quarterly_Earnings(secucode: str, stock_name: str, deep_thinking: bool = False) -> str:
    """
    执行C季度盈利分析
    
    Args:
        secucode: 股票代码
        stock_name: 股票名称
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    prompt = await get_C_Quarterly_Earnings_prompt(secucode, stock_name)

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

    print(result)
    
    return result
