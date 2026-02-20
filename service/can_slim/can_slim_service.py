"""CAN SLIM分析服务统一出口"""
import logging
from typing import Dict, Callable

logger = logging.getLogger(__name__)
from common.utils.stock_info_utils import StockInfo
from service.can_slim.A_Earnings_Increases_service import AEarningsIncreasesService
from service.can_slim.C_Quarterly_Earnings_service import CQuarterlyEarningsService
from service.can_slim.I_Sponsorship_service import ISponsorshipService
from service.can_slim.L_or_Laggard_service import LOrLaggardService
from service.can_slim.M_Direction_service import MDirectionService
from service.can_slim.N_Products_Management_Highs_service import NProductsManagementHighsService
from service.can_slim.S_Demand_service import SDemandService


# CAN SLIM维度服务类映射
CAN_SLIM_SERVICES: Dict[str, type] = {
    'A': AEarningsIncreasesService,
    'C': CQuarterlyEarningsService,
    'N': NProductsManagementHighsService,
    'S': SDemandService,
    'L': LOrLaggardService,
    'I': ISponsorshipService,
    'M': MDirectionService
}


async def execute_can_slim_analysis(
    dimension: str,
    stock_info: StockInfo,
    deep_thinking: bool = False
) -> str:
    """
    执行CAN SLIM分析的统一入口
    
    Args:
        dimension: CAN SLIM维度 ('A', 'C', 'N', 'S', 'L', 'I', 'M')
        stock_info: 股票信息对象
        deep_thinking: 是否使用深度思考模式
        
    Returns:
        分析结果字符串
        
    Raises:
        ValueError: 当维度参数无效时
    """
    try:
        dimension = dimension.upper()

        if dimension not in CAN_SLIM_SERVICES:
            raise ValueError(
                f"无效的CAN SLIM维度: {dimension}. "
                f"有效维度: {', '.join(CAN_SLIM_SERVICES.keys())}"
            )

        service_class = CAN_SLIM_SERVICES[dimension]
        service = service_class(stock_info)
        return await service.execute(deep_thinking)
    except Exception:
        logger.exception("execute_can_slim_analysis 执行失败: dimension=%s, stock=%s", dimension, stock_info.stock_name)
        raise


async def execute_can_slim_score(
    dimension: str,
    stock_info: StockInfo,
    deep_thinking: bool = False
) -> str:
    """
    执行CAN SLIM打分分析的统一入口
    
    Args:
        dimension: CAN SLIM维度 ('A', 'C', 'N', 'S', 'L', 'I', 'M')
        stock_info: 股票信息对象
        deep_thinking: 是否使用深度思考模式
        
    Returns:
        打分结果JSON字符串
        
    Raises:
        ValueError: 当维度参数无效时
    """
    dimension = dimension.upper()
    
    if dimension not in CAN_SLIM_SERVICES:
        raise ValueError(
            f"无效的CAN SLIM维度: {dimension}. "
            f"有效维度: {', '.join(CAN_SLIM_SERVICES.keys())}"
        )
    
    service_class = CAN_SLIM_SERVICES[dimension]
    service = service_class(stock_info)
    
    # 收集数据
    service.data_cache = await service.collect_data()
    await service.process_data()
    
    # 构建打分提示词
    prompt = service.build_prompt(use_score_output=True)
    
    # 调用LLM
    from service.llm.deepseek_client import DeepSeekClient
    model = "deepseek-reasoner" if deep_thinking else "deepseek-chat"
    client = DeepSeekClient()
    
    result = ""
    async for content in client.chat_stream(
        messages=[{"role": "user", "content": prompt}],
        model=model
    ):
        result += content
    
    return result


async def execute_overall_analysis(
    stock_info: StockInfo,
    all_analysis_result: str,
    deep_thinking: bool = False
) -> tuple[str, str]:
    """执行整体CAN SLIM综合分析，返回 (prompt, result)"""
    from common.prompt.can_slim.ALL_CAN_SLIM_prompt import get_ALL_CAN_SLIM
    from service.llm.deepseek_client import DeepSeekClient

    prompt = await get_ALL_CAN_SLIM(stock_info, all_analysis_result)
    model = "deepseek-reasoner" if deep_thinking else "deepseek-chat"
    client = DeepSeekClient()
    result = ""
    async for content in client.chat_stream(
        messages=[{"role": "user", "content": prompt}],
        model=model
    ):
        result += content
    return prompt, result


async def execute_can_slim_completion(
    dimension: str,
    stock_info: StockInfo,
    deep_thinking: bool = False
) -> str:
    """
    执行CAN SLIM完整分析（打分+分析结论）的统一入口
    
    Args:
        dimension: CAN SLIM维度 ('A', 'C', 'N', 'S', 'L', 'I', 'M')
        stock_info: 股票信息对象
        deep_thinking: 是否使用深度思考模式
        
    Returns:
        完整分析结果JSON字符串（包含打分和分析结论）
        
    Raises:
        ValueError: 当维度参数无效时
    """
    dimension = dimension.upper()
    
    if dimension not in CAN_SLIM_SERVICES:
        raise ValueError(
            f"无效的CAN SLIM维度: {dimension}. "
            f"有效维度: {', '.join(CAN_SLIM_SERVICES.keys())}"
        )
    
    service_class = CAN_SLIM_SERVICES[dimension]
    service = service_class(stock_info)
    return await service.execute(deep_thinking)
