"""
对比验证：同花顺 vs 东方财富 个股历史资金流向数据

选取10只股票，验证两个接口：
1. 接口签名一致性（参数、返回结构）
2. 基础字段对齐（date, close_price, change_pct）
3. 资金流向分类差异说明（两平台大单/中单/小单阈值不同，数值差异属正常）
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils.stock_info_utils import get_stock_info_by_name, StockInfo
from service.jqka10.stock_history_fund_flow_10jqka import (
    get_fund_flow_history as jqka_raw,
    get_fund_flow_history_json as jqka_json,
    get_fund_flow_history_json_cn as jqka_json_cn,
)
from service.eastmoney.stock_info.stock_history_flow import (
    get_fund_flow_history_json as em_json,
    get_fund_flow_history_json_cn as em_json_cn,
)

TEST_STOCKS = [
    "生益科技", "平安银行", "贵州茅台", "宁德时代", "比亚迪",
    "中国平安", "招商银行", "立讯精密", "隆基绿能", "紫金矿业",
]

# 这些字段两个平台应该完全一致
EXACT_FIELDS = ["date", "close_price"]
# 涨跌幅允许微小误差（四舍五入差异）
APPROX_FIELDS = {"change_pct": 0.05}


async def verify_interface_consistency(stock_info: StockInfo) -> list[str]:
    """验证接口签名和返回结构一致性"""
    errors = []

    # 1. get_fund_flow_history_json 返回结构
    j = await jqka_json(stock_info, page_size=3)
    e = await em_json(stock_info, page_size=3)

    for key in ["stock_name", "stock_code", "data"]:
        if key not in j:
            errors.append(f"jqka_json 缺少 key: {key}")
        if key not in e:
            errors.append(f"em_json 缺少 key: {key}")

    if j.get("data"):
        j_keys = set(j["data"][0].keys())
        e_keys = set(e["data"][0].keys())
        # 东方财富额外有 high_price/low_price/change_hand/trading_volume/trading_amount
        em_extra = {"high_price", "low_price", "change_hand", "trading_volume", "trading_amount"}
        expected_common = e_keys - em_extra
        missing = expected_common - j_keys
        if missing:
            errors.append(f"jqka_json data 缺少字段: {missing}")

    # 2. get_fund_flow_history_json_cn 返回结构
    j_cn = await jqka_json_cn(stock_info, page_size=1)
    e_cn = await em_json_cn(stock_info, page_size=1)

    for key in ["股票名称", "股票代码", "数据"]:
        if key not in j_cn:
            errors.append(f"jqka_json_cn 缺少 key: {key}")

    # 3. fields 过滤
    j_filtered = await jqka_json(stock_info, fields=["date", "main_net"], page_size=1)
    if j_filtered["data"]:
        actual_keys = set(j_filtered["data"][0].keys())
        if actual_keys != {"date", "main_net"}:
            errors.append(f"fields 过滤失败，期望 date/main_net，实际 {actual_keys}")

    return errors


async def verify_data_alignment(stock_info: StockInfo) -> dict:
    """验证基础字段对齐"""
    j = await jqka_json(stock_info, page_size=30)
    e = await em_json(stock_info, page_size=150)

    em_map = {item["date"]: item for item in e["data"]}
    jqka_map = {item["date"]: item for item in j["data"]}

    common_dates = sorted(set(jqka_map.keys()) & set(em_map.keys()), reverse=True)
    mismatches = []

    for date in common_dates:
        jd, ed = jqka_map[date], em_map[date]
        row_issues = {}

        for field in EXACT_FIELDS:
            if jd.get(field) != ed.get(field):
                row_issues[field] = {"jqka": jd.get(field), "em": ed.get(field)}

        for field, tol in APPROX_FIELDS.items():
            jv, ev = jd.get(field), ed.get(field)
            if jv is not None and ev is not None and abs(jv - ev) > tol:
                row_issues[field] = {"jqka": jv, "em": ev}

        if row_issues:
            mismatches.append({"date": date, "issues": row_issues})

    return {
        "common_dates": len(common_dates),
        "mismatches": mismatches,
    }


async def compare_one_stock(stock_name: str):
    """完整验证单只股票"""
    stock_info = get_stock_info_by_name(stock_name)
    if not stock_info:
        print(f"  [{stock_name}] ❌ 未找到股票信息")
        return False

    code = stock_info.stock_code_normalize

    # 接口一致性
    iface_errors = await verify_interface_consistency(stock_info)
    if iface_errors:
        print(f"  [{stock_name}({code})] ❌ 接口不一致:")
        for err in iface_errors:
            print(f"    - {err}")
        return False

    # 数据对齐
    alignment = await verify_data_alignment(stock_info)
    common = alignment["common_dates"]
    mm = alignment["mismatches"]

    if mm:
        print(f"  [{stock_name}({code})] ❌ 基础字段不一致 ({len(mm)}/{common}天):")
        for m in mm[:3]:
            print(f"    {m['date']}: {json.dumps(m['issues'], ensure_ascii=False)}")
        return False

    print(f"  [{stock_name}({code})] ✅ 接口一致, 基础字段对齐 ({common}天)")
    return True


async def main():
    print("=" * 80)
    print("  同花顺 vs 东方财富 历史资金流向 — 接口一致性 & 基础字段对比")
    print("  注：大单/中单/小单金额因两平台分类阈值不同，差异属正常现象")
    print("=" * 80)

    passed = 0
    for name in TEST_STOCKS:
        ok = await compare_one_stock(name)
        if ok:
            passed += 1

    total = len(TEST_STOCKS)
    print(f"\n{'=' * 80}")
    print(f"  结果: {passed}/{total} 通过")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    asyncio.run(main())
