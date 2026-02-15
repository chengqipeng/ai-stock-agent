from common.constants.stocks_data import get_stock_code, STOCK_INDICES_DICT


class StockInfo:
    def __init__(self, secid, stock_code, stock_code_normalize, stock_name, indices_stock_code=None, indices_stock_name=None):
        self.secid = secid
        self.stock_code = stock_code
        self.stock_code_normalize = stock_code_normalize
        self.stock_name = stock_name
        self.indices_stock_code = indices_stock_code
        self.indices_stock_name = indices_stock_name


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
            - indices_stock_code: 指数代码（如果有多个取最后一个）
            - indices_stock_name: 指数名称（如果有多个取最后一个）
        如果未找到则返回 None
    """
    try:
        stock_code_normalize = get_stock_code(stock_name)
        stock_code, market_suffix = stock_code_normalize.split('.')
        market_prefix = "0" if market_suffix == "SZ" else "1"
        secid = f"{market_prefix}.{stock_code}"
        
        # 从 STOCK_INDICES_DICT 中查找对应的股票信息
        indices_stock_code = None
        indices_stock_name = None
        stock_data = STOCK_INDICES_DICT.get(stock_code_normalize)
        if stock_data:
            indices_codes = stock_data.get('indices_stock_codes', [])
            indices_names = stock_data.get('indices_stock_names', [])
            if indices_codes:
                indices_stock_code = indices_codes[-1]
            if indices_names:
                indices_stock_name = indices_names[-1]
        
        return StockInfo(secid, stock_code, stock_code_normalize, stock_name, indices_stock_code, indices_stock_name)
    except (ValueError, AttributeError):
        return None


if __name__ == '__main__':
    # 测试有多个指数的股票
    stock_info = get_stock_info_by_name("聚和材料")
    if stock_info:
        print(f"股票名称: {stock_info.stock_name}")
        print(f"股票代码: {stock_info.stock_code_normalize}")
        print(f"secid: {stock_info.secid}")
        print(f"指数代码: {stock_info.indices_stock_code}")
        print(f"指数名称: {stock_info.indices_stock_name}")
    
    print("\n---\n")
    
    # 测试只有一个指数的股票
    stock_info2 = get_stock_info_by_name("上证指数")
    if stock_info2:
        print(f"股票名称: {stock_info2.stock_name}")
        print(f"股票代码: {stock_info2.stock_code_normalize}")
        print(f"指数代码: {stock_info2.indices_stock_code}")
        print(f"指数名称: {stock_info2.indices_stock_name}")
