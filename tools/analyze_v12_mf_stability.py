#!/usr/bin/env python3
"""
验证 money_flow 信号区分力的时间稳定性
======================================
关键问题：money_flow 88% vs 无money_flow 50% 的差距，
是否在不同时间段都成立？还是被少数超级周拉高？
"""
import json
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).resolve().parent.parent / "data_results"


def main():
    with open(DATA_DIR / "v12_high_confidence_analysis.json", 'r') as f:
        data = json.load(f)
    
    records = data['records']
    
    # 按周分组，每周分别计算有/无money_flow的准确率
    by_week = defaultdict(lambda: {
        'mf_total': 0, 'mf_correct': 0,
        'no_mf_total': 0, 'no_mf_correct': 0
    })

    for r in records:
        wk = r['week']
        has_mf = r.get('signal_scores', {}).get('money_flow', 0) != 0
        
        if has_mf:
            by_week[wk]['mf_total'] += 1
            if r['is_correct']:
                by_week[wk]['mf_correct'] += 1
        else:
            by_week[wk]['no_mf_total'] += 1
            if r['is_correct']:
                by_week[wk]['no_mf_correct'] += 1
    
    print("=" * 80)
    print("money_flow 信号区分力 — 周度稳定性检验")
    print("=" * 80)
    
    print(f"\n{'周':<12s} {'有MF准确率':>10s} {'有MF样本':>8s} {'无MF准确率':>10s} {'无MF样本':>8s} {'差距':>8s}")
    print("-" * 60)
    
    mf_wins = 0  # money_flow更准的周数
    no_mf_wins = 0
    tie = 0
    
    # 只看有足够样本的周
    valid_weeks = []
    for wk in sorted(by_week.keys()):
        d = by_week[wk]
        if d['mf_total'] >= 3 and d['no_mf_total'] >= 3:
            valid_weeks.append(wk)
    
    for wk in valid_weeks:
        d = by_week[wk]
        mf_acc = d['mf_correct'] / d['mf_total'] if d['mf_total'] > 0 else 0
        no_mf_acc = d['no_mf_correct'] / d['no_mf_total'] if d['no_mf_total'] > 0 else 0
        diff = mf_acc - no_mf_acc
        
        if diff > 0.05:
            mf_wins += 1
            marker = '✅'
        elif diff < -0.05:
            no_mf_wins += 1
            marker = '❌'
        else:
            tie += 1
            marker = '➡️'
        
        print(f"{marker} {wk:<10s} {mf_acc:>9.1%} {d['mf_total']:>8d} "
              f"{no_mf_acc:>9.1%} {d['no_mf_total']:>8d} {diff:>+7.1%}")
    
    total_valid = mf_wins + no_mf_wins + tie
    print(f"\n统计:")
    print(f"  money_flow更准的周: {mf_wins}/{total_valid} ({mf_wins/total_valid:.0%})")
    print(f"  无money_flow更准的周: {no_mf_wins}/{total_valid} ({no_mf_wins/total_valid:.0%})")
    print(f"  持平: {tie}/{total_valid} ({tie/total_valid:.0%})")
    
    # 排除超级周后的对比
    print(f"\n排除超级周（预测>500条的周）后:")
    normal_mf_total = 0
    normal_mf_correct = 0
    normal_no_mf_total = 0
    normal_no_mf_correct = 0
    
    for wk in sorted(by_week.keys()):
        d = by_week[wk]
        week_total = d['mf_total'] + d['no_mf_total']
        if week_total > 500:
            continue
        normal_mf_total += d['mf_total']
        normal_mf_correct += d['mf_correct']
        normal_no_mf_total += d['no_mf_total']
        normal_no_mf_correct += d['no_mf_correct']
    
    if normal_mf_total > 0 and normal_no_mf_total > 0:
        mf_acc = normal_mf_correct / normal_mf_total
        no_mf_acc = normal_no_mf_correct / normal_no_mf_total
        print(f"  有money_flow: {mf_acc:.1%} ({normal_mf_total}条)")
        print(f"  无money_flow: {no_mf_acc:.1%} ({normal_no_mf_total}条)")
        print(f"  差距: {mf_acc - no_mf_acc:+.1%}")
    
    # 按时间段（前半/后半）
    all_weeks = sorted(by_week.keys())
    mid = len(all_weeks) // 2
    first_half = set(all_weeks[:mid])
    second_half = set(all_weeks[mid:])
    
    for period_name, period_weeks in [('前半段', first_half), ('后半段', second_half)]:
        mf_t = sum(by_week[w]['mf_total'] for w in period_weeks)
        mf_c = sum(by_week[w]['mf_correct'] for w in period_weeks)
        no_t = sum(by_week[w]['no_mf_total'] for w in period_weeks)
        no_c = sum(by_week[w]['no_mf_correct'] for w in period_weeks)
        
        if mf_t > 0 and no_t > 0:
            print(f"\n  {period_name}:")
            print(f"    有money_flow: {mf_c/mf_t:.1%} ({mf_t}条)")
            print(f"    无money_flow: {no_c/no_t:.1%} ({no_t}条)")
            print(f"    差距: {mf_c/mf_t - no_c/no_t:+.1%}")
    
    # money_flow信号与大盘暴跌的关系
    print(f"\n\nmoney_flow信号出现率 vs 大盘状态:")
    week_detail = data.get('weekly_detail', [])
    mkt_map = {wd['week']: wd.get('mkt_pct', 0) for wd in week_detail}
    
    crash_mf_rate = []
    normal_mf_rate = []
    
    for wk in sorted(by_week.keys()):
        d = by_week[wk]
        total = d['mf_total'] + d['no_mf_total']
        if total < 5:
            continue
        mf_rate = d['mf_total'] / total
        mkt = mkt_map.get(wk, 0)
        
        if mkt < -2:
            crash_mf_rate.append(mf_rate)
        else:
            normal_mf_rate.append(mf_rate)
    
    if crash_mf_rate:
        print(f"  大盘跌>2%的周: money_flow出现率 {sum(crash_mf_rate)/len(crash_mf_rate):.1%} ({len(crash_mf_rate)}周)")
    if normal_mf_rate:
        print(f"  大盘正常的周:  money_flow出现率 {sum(normal_mf_rate)/len(normal_mf_rate):.1%} ({len(normal_mf_rate)}周)")


if __name__ == '__main__':
    main()
