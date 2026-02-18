"""CAN SLIM分析服务统一出口"""
from typing import Dict, Callable
from common.utils.stock_info_utils import StockInfo
from service.can_slim.A_Earnings_Increases_service import execute_A_Earnings_Increases
from service.can_slim.C_Quarterly_Earnings_service import execute_C_Quarterly_Earnings
from service.can_slim.I_Sponsorship_service import execute_I_Sponsorship
from service.can_slim.L_or_Laggard_service import execute_L_or_Laggard
from service.can_slim.M_Direction_service import execute_M_Direction
from service.can_slim.N_Products_Management_Highs_service import execute_N_Products_Management_Highs
from service.can_slim.S_Demand_service import execute_S_Demand


# CAN SLIM维度映射
CAN_SLIM_DIMENSIONS: Dict[str, Callable] = {
    'A': execute_A_Earnings_Increases,
    'C': execute_C_Quarterly_Earnings,
    'N': execute_N_Products_Management_Highs,
    'S': execute_S_Demand,
    'L': execute_L_or_Laggard,
    'I': execute_I_Sponsorship,
    'M': execute_M_Direction
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
    
    if dimension not in CAN_SLIM_DIMENSIONS:
        raise ValueError(
            f"无效的CAN SLIM维度: {dimension}. "
            f"有效维度: {', '.join(CAN_SLIM_DIMENSIONS.keys())}"
        )
    
    execute_func = CAN_SLIM_DIMENSIONS[dimension]
    return await execute_func(stock_info, deep_thinking)
