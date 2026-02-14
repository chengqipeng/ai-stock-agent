from service.prompt.can_slim.A_Earnings_Increases_prompt import get_A_Earnings_Increases_prompt
from service.llm.deepseek_client import DeepSeekClient


async def execute_A_Earnings_Increases(secucode: str, stock_name: str, deep_thinking: bool = False) -> str:
    """
    执行A年度盈利增长分析
    
    Args:
        secucode: 股票代码
        stock_name: 股票名称
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    prompt = await get_A_Earnings_Increases_prompt(secucode, stock_name)

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
