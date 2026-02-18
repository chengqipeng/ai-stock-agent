"""CAN SLIM分析服务统一出口"""
from typing import Dict, Callable
from common.utils.stock_info_utils import StockInfo
from common.constants.can_slim_final_outputs import SCORE_OUTPUT, COMPLETION_OUTPUT
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
    dimension = dimension.upper()
    
    if dimension not in CAN_SLIM_SERVICES:
        raise ValueError(
            f"无效的CAN SLIM维度: {dimension}. "
            f"有效维度: {', '.join(CAN_SLIM_SERVICES.keys())}"
        )
    
    service_class = CAN_SLIM_SERVICES[dimension]
    service = service_class(stock_info)
    return await service.execute(deep_thinking)


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
    
    # 临时覆盖最终输出指令为打分格式
    original_method = service.get_final_output_instruction
    service.get_final_output_instruction = lambda: SCORE_OUTPUT
    
    result = await service.execute(deep_thinking)
    
    # 恢复原方法
    service.get_final_output_instruction = original_method
    
    return result


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
    
    # 临时覆盖最终输出指令为完整输出格式
    original_method = service.get_final_output_instruction
    service.get_final_output_instruction = lambda: COMPLETION_OUTPUT
    
    result = await service.execute(deep_thinking)
    
    # 恢复原方法
    service.get_final_output_instruction = original_method
    
    return result
