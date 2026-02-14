from common.utils.stock_info_utils import StockInfo
from service.llm.deepseek_client import DeepSeekClient
from service.prompt.can_slim.C_Quarterly_Earnings_prompt import get_C_Quarterly_Earnings_prompt


async def execute_C_Quarterly_Earnings(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """
    执行C季度盈利分析
    
    Args:
        secucode: 股票代码
        stock_name: 股票名称
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    prompt = await get_C_Quarterly_Earnings_prompt(stock_info)

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
