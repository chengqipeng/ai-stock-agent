def convert_amount_unit(amount):
    """根据金额大小自动转换单位：大于亿转换为亿，大于万转换为万"""
    if amount is None:
        return "--"
    if abs(amount) >= 100000000:  # >= 1亿
        return f"{round(amount / 100000000, 4)}亿"
    elif abs(amount) >= 10000:  # >= 1万
        return f"{round(amount / 10000, 4)}万"
    else:
        return str(amount)

def convert_amount_org_holder(amount):
    """根据金额大小自动转换单位：大于亿转换为亿，大于万转换为万"""
    if amount is None:
        return "--"
    if abs(amount) >= 100000000:  # >= 1亿
        return f"{round(amount / 100000000, 4)}"
    if abs(amount) >= 10000:  # >= 1万
        return f"{round(amount / 10000, 4)}"
    else:
        return str(amount)

def convert_amount_org_holder_1(amount):
    """根据金额大小自动转换单位：大于亿转换为亿，大于万转换为万"""
    if amount is None:
        return "--"
    if abs(amount) >= 10000:  # >= 1亿
        return f"{round(amount / 100000000, 4)}"
    else:
        return str(amount)

def normalize_stock_code(code):
    """自动添加市场前缀: SH结尾添加1., SZ结尾添加0."""
    code = code.strip()
    if code.endswith('.SH'):
        return f"1.{code.split('.')[0]}"
    elif code.endswith('.SZ'):
        return f"0.{code.split('.')[0]}"
    return code
