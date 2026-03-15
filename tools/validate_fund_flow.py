"""
资金流向数据强一致性验证工具

对所有个股执行资金流向数据校验，检测：
1. close_price 与 K线表不一致（容差 0.02）
2. change_pct 与 K线表不一致（容差 0.5 个百分点）
3. 关键字段缺失：close_price / change_pct / net_flow 为 NULL

首日交易数据允许误差，跳过校验。
数据源：东方财富（首次全量120条）+ 同花顺（每日增量30条），
两者已通过归一化对齐字段语义（big_net=主力）。
资金守恒/占比守恒不作为校验规则（同花顺统计口径不同，天然不守恒）。

Usage:
    python -m tools.validate_fund_flow
    python -m tools.validate_fund_flow --stock 300502.SZ
    python -m tools.validate_fund_flow --top 50
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dao.stock_kline_dao import get_all_stock_codes
from dao.stock_fund_flow_dao import check_fund_flow_db


def main():
    parser = argparse.ArgumentParser(description="资金流向数据强一致性验证")
    parser.add_argument("--stock", type=str, help="指定股票代码(逗号分隔)")
    parser.add_argument("--top", type=int, help="只检查前N只股票")
    args = parser.parse_args()

    if args.stock:
        stock_codes = [c.strip() for c in args.stock.split(",")]
    else:
        stock_codes = get_all_stock_codes()

    if args.top:
        stock_codes = stock_codes[:args.top]

    total = len(stock_codes)
    print(f"\n{'='*72}")
    print(f"  资金流向强一致性验证  (共 {total} 只股票)")
    print(f"{'='*72}\n")

    clean_count = 0
    anomaly_stocks = []

    for i, code in enumerate(stock_codes):
        issues = check_fund_flow_db(code)
        if not issues:
            clean_count += 1
            if (i + 1) % 100 == 0:
                print(f"  [{i+1}/{total}] 已检测 {i+1} 只，异常 {len(anomaly_stocks)} 只...")
            continue

        anomaly_stocks.append((code, issues))

        # 按异常类型分组统计
        type_counts = {}
        for iss in issues:
            t = iss["type"]
            type_counts[t] = type_counts.get(t, 0) + 1
        type_summary = ", ".join(f"{t}×{c}" for t, c in type_counts.items())

        print(f"  [{i+1}/{total}] {code}  ✗ {len(issues)}条异常  ({type_summary})")
        # 最多展示前3条明细
        for iss in issues[:3]:
            print(f"           [{iss['type']}] {iss['date']}  {iss['detail']}")
        if len(issues) > 3:
            print(f"           ... 还有 {len(issues)-3} 条")

    # ── 汇总报告 ──
    print(f"\n{'='*72}")
    print(f"  验证结果汇总")
    print(f"{'='*72}")
    print(f"  总股票数:   {total}")
    print(f"  数据正常:   {clean_count}")
    print(f"  存在异常:   {len(anomaly_stocks)}")

    if anomaly_stocks:
        # 按异常条数排序
        anomaly_stocks.sort(key=lambda x: len(x[1]), reverse=True)
        print(f"\n  ── 异常股票列表 (共{len(anomaly_stocks)}只) ──")
        print(f"  {'股票代码':12s} {'异常数':>6s}  {'异常类型分布'}")
        print(f"  {'-'*60}")
        for code, issues in anomaly_stocks:
            type_counts = {}
            for iss in issues:
                t = iss["type"]
                type_counts[t] = type_counts.get(t, 0) + 1
            type_summary = ", ".join(f"{t}×{c}" for t, c in type_counts.items())
            print(f"  {code:12s} {len(issues):6d}  {type_summary}")

        # 全局异常类型统计
        global_types = {}
        total_issues = 0
        for _, issues in anomaly_stocks:
            total_issues += len(issues)
            for iss in issues:
                t = iss["type"]
                global_types[t] = global_types.get(t, 0) + 1

        print(f"\n  ── 异常类型全局统计 (共{total_issues}条) ──")
        for t, c in sorted(global_types.items(), key=lambda x: -x[1]):
            label = {
                "ff_price_mismatch": "收盘价不一致",
                "ff_chg_pct_mismatch": "涨跌幅不一致",
                "ff_null_field": "关键字段缺失",
            }.get(t, t)
            print(f"  {label:20s} ({t}):  {c} 条")

    print()

    if anomaly_stocks:
        sys.exit(1)


if __name__ == "__main__":
    main()
