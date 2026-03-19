"""检查财报数据中实际存在的字段名"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils.stock_info_utils import StockInfo
from service.jqka10.stock_finance_data_10jqka import get_financial_data_from_db


def _make_stock_info(code_normalize, name):
    code, suffix = code_normalize.split(".")
    market_prefix = "0" if suffix == "SZ" else "1"
    return StockInfo(secid=f"{market_prefix}.{code}", stock_code=code,
                     stock_code_normalize=code_normalize, stock_name=name)


info = _make_stock_info("300602.SZ", "飞荣达")
records = get_financial_data_from_db(info, limit=3)

for i, rec in enumerate(records):
    print(f"\n=== 第{i+1}条: {rec.get('报告期', '?')} ===")
    for k, v in rec.items():
        # 只打印包含"营业"或"扣非"或"收入"或"利润"的字段
        if any(kw in k for kw in ['营业', '扣非', '收入', '利润', '报告']):
            print(f"  {k}: {v}")
