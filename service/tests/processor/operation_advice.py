def get_operation_advice(advice_type, holding_price=None):
    """生成操作建议提示词"""
    advices = {
        1: "目前不持有该股票，结合已提供的数据和上面的分析结论，本周该如何操作",
        2: "目前不持有该股票，结合已提供的数据和上面的分析结论，下周该如何操作",
        3: f"目前该股票的持仓价格是<{holding_price}>，结合已提供的数据和上面的分析结论，下周该如何操作",
        4: f"目前该股票的持仓价格是<{holding_price}>，结合已提供的数据和上面的分析结论，本周该如何操作"
    }
    return advices.get(advice_type, "")
