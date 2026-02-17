from common.prompt.can_slim.M_Direction_prompt import build_M_Direction_prompt
from common.utils.stock_info_utils import StockInfo
from service.llm.deepseek_client import DeepSeekClient

async def execute_M_Direction(stock_info: StockInfo, deep_thinking: bool = False) -> str:
    """
    执行M市场方向分析
    
    Args:
        stock_info: 股票信息对象
        deep_thinking: 是否使用思考模式，默认False
    
    Returns:
        分析结果字符串
    """
    prompt = await build_M_Direction_prompt(stock_info)

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
