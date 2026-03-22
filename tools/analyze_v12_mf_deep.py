#!/usr/bin/env python3
"""
深入检查 money_flow 信号的分布 — 为什么前半段几乎没有？
"""
import json
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).resolve().parent.parent / "data_results"

def main():
    with open(DATA_DIR / "v12_high_confidence_analysis.json", 'r') as f:
        data = json.load(f)
    
    records = data['records']
    
    # 每周的money_flow信号出现情况
    by_week = defaultdict(lambda: {'mf': 0, 'no_mf': 0, 'mf_correct': 0, 'no_mf_correct': 0})
    
    for r in records:
        wk = r['week']
        has_mf = r.get('signal_scores', {}).get('money_flow', 0) != 0
        if has_mf:
            by_week[wk]['mf'] += 1
            if r['is_correct']:
                by_week[wk]['mf_correct'] += 1
        else:
            by_week[wk]['no_mf'] += 1
            if r['is_correct']:
                by_week[wk]['no_mf_correct'] += 1

    print("每周 money_flow 信号分布:")
    print(f"{'周':<12s} {'有MF':>6s} {'无MF':>6s} {'MF占比':>8s} {'有MF准确率':>10s} {'无MF准确率':>10s}")
    print("-" * 60)
    
    total_mf = 0
    total_no_mf = 0
    
    for wk in sorted(by_week.keys()):
        d = by_week[wk]
        total = d['mf'] + d['no_mf']
        mf_pct = d['mf'] / total if total > 0 else 0
        mf_acc = d['mf_correct'] / d['mf'] if d['mf'] > 0 else 0
        no_mf_acc = d['no_mf_correct'] / d['no_mf'] if d['no_mf'] > 0 else 0
        
        total_mf += d['mf']
        total_no_mf += d['no_mf']
        
        marker = '📊' if d['mf'] > 0 else '  '
        print(f"{marker} {wk:<10s} {d['mf']:>6d} {d['no_mf']:>6d} {mf_pct:>7.1%} "
              f"{mf_acc:>9.1%} {no_mf_acc:>9.1%}")
    
    print(f"\n总计: 有MF {total_mf}条, 无MF {total_no_mf}条")
    print(f"MF占比: {total_mf/(total_mf+total_no_mf):.1%}")
    
    # 检查：money_flow信号是否集中在特定的超级周
    print(f"\nmoney_flow信号最集中的5周:")
    sorted_weeks = sorted(by_week.items(), key=lambda x: x[1]['mf'], reverse=True)
    top5_mf = 0
    for wk, d in sorted_weeks[:5]:
        total = d['mf'] + d['no_mf']
        mf_acc = d['mf_correct'] / d['mf'] if d['mf'] > 0 else 0
        top5_mf += d['mf']
        print(f"  {wk}: {d['mf']}条有MF (总{total}条), MF准确率{mf_acc:.1%}")
    
    print(f"\n  Top5周占全部MF信号的 {top5_mf}/{total_mf} = {top5_mf/total_mf:.1%}")
    
    # 排除top5周后的money_flow准确率
    top5_weeks = set(wk for wk, _ in sorted_weeks[:5])
    rest_mf_total = 0
    rest_mf_correct = 0
    rest_no_mf_total = 0
    rest_no_mf_correct = 0
    
    for wk, d in by_week.items():
        if wk in top5_weeks:
            continue
        rest_mf_total += d['mf']
        rest_mf_correct += d['mf_correct']
        rest_no_mf_total += d['no_mf']
        rest_no_mf_correct += d['no_mf_correct']
    
    print(f"\n排除Top5周后:")
    if rest_mf_total > 0:
        print(f"  有MF: {rest_mf_correct/rest_mf_total:.1%} ({rest_mf_total}条)")
    else:
        print(f"  有MF: 无数据")
    if rest_no_mf_total > 0:
        print(f"  无MF: {rest_no_mf_correct/rest_no_mf_total:.1%} ({rest_no_mf_total}条)")


if __name__ == '__main__':
    main()
