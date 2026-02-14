from common.constants.stocks_data import get_stock_code


class StockInfo:
    def __init__(self, secid, stock_code, stock_code_normalize, stock_name):
        self.secid = secid
        self.stock_code = stock_code
        self.stock_code_normalize = stock_code_normalize
        self.stock_name = stock_name


def get_stock_info_by_name(stock_name):
    """
    通过股票名称获取股票信息
    
    Args:
        stock_name: 股票名称
        
    Returns:
        StockInfo: 包含以下字段的对象:
            - secid: 格式如 "0.000001" 或 "1.600000"
            - stock_code: 格式如 "000001"
            - stock_code_normalize: 格式如 "000001.SZ" 或 "600000.SH"
            - stock_name: 股票名称
        如果未找到则返回 None
    """
    try:
        stock_code_normalize = get_stock_code(stock_name)
        stock_code, market_suffix = stock_code_normalize.split('.')
        market_prefix = "0" if market_suffix == "SZ" else "1"
        secid = f"{market_prefix}.{stock_code}"
        
        return StockInfo(secid, stock_code, stock_code_normalize, stock_name)
    except (ValueError, AttributeError):
        return None
