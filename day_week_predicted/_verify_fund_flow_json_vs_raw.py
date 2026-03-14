"""
验证 get_fund_flow_history 与 get_fund_flow_history_json 数据一致性。

对比逻辑：
  - get_fund_flow_history 返回原始 list[dict]，万元单位
  - get_fund_flow_history_json 返回 {"data": [...]}，元单位
  - 通过 _json_to_raw 转换后，两者应一致
"""
import asyncio
import json

from common.utils.stock_info_utils import get_stock_info_by_name
from service.jqka10.stock_history_fund_flow_10jqka import (
    get_fund_flow_history,
    get_fund_flow_history_json,
)


def _json_to_raw(json_data: list[dict]) -> list[dict]:
    """与 fund_flow_scheduler 中的转换函数一致"""
    rows = []
    for item in json_data:
        main_net = item.get("main_net", 0) or 0
        mid_net = item.get("mid_net", 0) or 0
        small_net = item.get("small_net", 0) or 0
        net_flow = main_net + mid_net + small_net
        rows.append({
            "date":          item.get("date", ""),
            "close_price":   item.get("close_price"),
            "change_pct":    item.get("change_pct"),
            "net_flow":      round(net_flow / 10000, 4) if net_flow else None,
            "main_net_5day": None,
            "big_net":       round(main_net / 10000, 4) if main_net else None,
            "big_net_pct":   item.get("main_pct"),
            "mid_net":       round(mid_net / 10000, 4) if mid_net else None,
            "mid_net_pct":   item.get("mid_pct"),
            "small_net":     round(small_net / 10000, 4) if small_net else None,
            "small_net_pct": item.get("small_pct"),
        })
    return rows


async def main():
    stock_info = get_stock_info_by_name("生益科技")
    print(f"=== 验证股票: {stock_info.stock_name}({stock_info.stock_code}) ===\n")

    # 两个接口都只取 5 条方便对比
    raw_data = await get_fund_flow_history(stock_info)
    raw_data = raw_data[:5]

    json_result = await get_fund_flow_history_json(stock_info, page_size=5)
    json_data = json_result.get("data", [])
    converted = _json_to_raw(json_data)

    print(f"raw 条数: {len(raw_data)}, json 转换后条数: {len(converted)}\n")

    # 逐条对比
    compare_fields = ["date", "close_price", "change_pct", "big_net", "big_net_pct",
                      "mid_net", "mid_net_pct", "small_net", "small_net_pct"]
    all_match = True
    for i, (r, c) in enumerate(zip(raw_data, converted)):
        diffs = []
        for f in compare_fields:
            rv, cv = r.get(f), c.get(f)
            if rv != cv:
                # 浮点容差
                if isinstance(rv, (int, float)) and isinstance(cv, (int, float)):
                    if abs(rv - cv) < 0.01:
                        continue
                diffs.append(f"  {f}: raw={rv} vs json={cv}")
        if diffs:
            all_match = False
            print(f"[第{i+1}条 {r.get('date')}] 差异:")
            print("\n".join(diffs))
        else:
            print(f"[第{i+1}条 {r.get('date')}] ✓ 一致")

    # 额外对比 net_flow（raw 有原始值，json 是计算值）
    print("\n--- net_flow 对比（raw原始 vs json计算） ---")
    for i, (r, c) in enumerate(zip(raw_data, converted)):
        rv = r.get("net_flow")
        cv = c.get("net_flow")
        match = "✓" if rv == cv or (rv and cv and abs(rv - cv) < 0.01) else "✗"
        print(f"  {r.get('date')}: raw={rv} vs json={cv} {match}")

    print(f"\n{'✓ 全部一致' if all_match else '✗ 存在差异'}")


if __name__ == "__main__":
    asyncio.run(main())
