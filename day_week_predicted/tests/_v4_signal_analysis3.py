#!/usr/bin/env python3
"""分析d4(前4天)预测能力 vs d3(前3天)。"""
import sys, logging
from collections import defaultdict
from datetime import datetime
sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from day_week_predicted.backtest.concept_strength_weekly_v4_backtest import (
    _preload_v4_data, _compound_return, _mean,
)

def main():
    from day_week_predicted.tests.test_concept_strength_weekly_v4_100boards import (
        fetch_boards_from_db, _check_db_available
    )
    if not _check_db_available():
        print("DB不可达"); return

    board_stock_map, all_codes = fetch_boards_from_db(min_stocks=20, target_boards=100)
    data = _preload_v4_data(all_codes, '2025-08-01', '2026-03-13')
    print(f"股票: {len(all_codes)}")

    # 构建周数据，同时计算d3和d4
    market_klines = data.get('market_klines', [])
    market_bt = [k for k in market_klines if '2025-08-01' <= k['date'] <= '2026-03-13']
    market_wg = defaultdict(list)
    for k in market_bt:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        market_wg[iw].append(k)

    records = []
    for code in all_codes:
        klines = data['stock_klines'].get(code, [])
        if not klines:
            continue
        bt = [k for k in klines if '2025-08-01' <= k['date'] <= '2026-03-13']
        if len(bt) < 5:
            continue
        wg = defaultdict(list)
        for k in bt:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)
        for iw, days in wg.items():
            days.sort(key=lambda x: x['date'])
            if len(days) < 3:
                continue
            pcts = [d['change_percent'] for d in days]
            weekly_chg = _compound_return(pcts)
            weekly_up = weekly_chg >= 0
            d3 = _compound_return(pcts[:3])
            d4 = _compound_return(pcts[:4]) if len(days) >= 4 else None
            # 停牌检测
            is_susp = all(p == 0 for p in pcts[:3])
            records.append({
                'code': code, 'iso_week': iw, 'n_days': len(days),
                'd3': d3, 'd4': d4, 'weekly_chg': weekly_chg,
                'weekly_up': weekly_up, 'is_susp': is_susp,
                'daily': pcts,
            })

    print(f"周样本: {len(records)}")
    has_d4 = [r for r in records if r['d4'] is not None]
    print(f"有d4的样本: {len(has_d4)} ({len(has_d4)/len(records)*100:.1f}%)")

    # ── d3 vs d4 准确率对比 ──
    print("\n" + "="*80)
    print("  d3 vs d4 预测准确率对比")
    print("="*80)

    # 非停牌样本
    non_susp = [r for r in records if not r['is_susp']]
    non_susp_d4 = [r for r in non_susp if r['d4'] is not None]

    d3_ok = sum(1 for r in non_susp if (r['d3'] >= 0) == r['weekly_up'])
    print(f"\n  非停牌d3准确率: {d3_ok/len(non_susp)*100:.1f}% ({len(non_susp)})")

    d4_ok = sum(1 for r in non_susp_d4 if (r['d4'] >= 0) == r['weekly_up'])
    print(f"  非停牌d4准确率: {d4_ok/len(non_susp_d4)*100:.1f}% ({len(non_susp_d4)})")

    # 按d3区间分析d4的提升
    for zone, lo, hi in [('strong', 2.0, 999), ('medium', 0.8, 2.0), ('fuzzy', 0, 0.8)]:
        z_d3 = [r for r in non_susp if lo <= abs(r['d3']) < hi if not (zone == 'strong' and abs(r['d3']) < hi)]
        if zone == 'strong':
            z_d3 = [r for r in non_susp if abs(r['d3']) > 2.0]
        elif zone == 'medium':
            z_d3 = [r for r in non_susp if 0.8 < abs(r['d3']) <= 2.0]
        else:
            z_d3 = [r for r in non_susp if abs(r['d3']) <= 0.8]

        z_d4 = [r for r in z_d3 if r['d4'] is not None]
        if not z_d3 or not z_d4:
            continue
        ok3 = sum(1 for r in z_d3 if (r['d3'] >= 0) == r['weekly_up'])
        ok4 = sum(1 for r in z_d4 if (r['d4'] >= 0) == r['weekly_up'])
        # d4在该区间的准确率
        print(f"  {zone}: d3={ok3/len(z_d3)*100:.1f}%({len(z_d3)}) "
              f"d4={ok4/len(z_d4)*100:.1f}%({len(z_d4)})")

    # ── d4区间分析 ──
    print("\n" + "-"*60)
    print("  d4区间准确率")
    print("-"*60)
    for zone, lo, hi in [('d4_strong', 2.0, 999), ('d4_medium', 0.8, 2.0), ('d4_fuzzy', 0, 0.8)]:
        if zone == 'd4_strong':
            z = [r for r in non_susp_d4 if abs(r['d4']) > 2.0]
        elif zone == 'd4_medium':
            z = [r for r in non_susp_d4 if 0.8 < abs(r['d4']) <= 2.0]
        else:
            z = [r for r in non_susp_d4 if abs(r['d4']) <= 0.8]
        if not z:
            continue
        ok = sum(1 for r in z if (r['d4'] >= 0) == r['weekly_up'])
        print(f"  {zone}: {ok/len(z)*100:.1f}% ({len(z)})")

    # ── 如果用d4做预测，总准确率 ──
    print("\n" + "-"*60)
    print("  d4预测总准确率估算")
    print("-"*60)
    # 对有d4的用d4，没d4的用d3
    total_ok = 0
    total_n = 0
    for r in records:
        if r['is_susp']:
            pred_up = True
        elif r['d4'] is not None:
            pred_up = r['d4'] >= 0
        else:
            pred_up = r['d3'] >= 0
        if pred_up == r['weekly_up']:
            total_ok += 1
        total_n += 1
    print(f"  d4优先: {total_ok}/{total_n} = {total_ok/total_n*100:.1f}%")

    # 纯d3
    total_ok2 = 0
    for r in records:
        if r['is_susp']:
            pred_up = True
        else:
            pred_up = r['d3'] >= 0
        if pred_up == r['weekly_up']:
            total_ok2 += 1
    print(f"  纯d3:   {total_ok2}/{total_n} = {total_ok2/total_n*100:.1f}%")

    # ── 分析: 4天中的最大连续方向 ──
    print("\n" + "-"*60)
    print("  4天模式分析(非停牌有d4)")
    print("-"*60)
    pattern_stats = defaultdict(lambda: [0, 0])
    for r in non_susp_d4:
        pats = ''.join(['U' if d >= 0 else 'D' for d in r['daily'][:4]])
        pattern_stats[pats][1] += 1
        if r['weekly_up']:
            pattern_stats[pats][0] += 1

    # 只显示样本数>100的模式
    print(f"  {'模式':<6s} {'涨率':>8s} {'样本':>6s} {'最优预测':>8s} {'准确率':>8s}")
    for pat in sorted(pattern_stats.keys()):
        up, n = pattern_stats[pat]
        if n < 100:
            continue
        ur = up / n * 100
        pred = '涨' if ur >= 50 else '跌'
        acc = max(ur, 100 - ur)
        print(f"  {pat:<6s} {ur:>6.1f}%  {n:>5d}   {pred:>4s}   {acc:>6.1f}%")

    print("\n  完成。")

if __name__ == '__main__':
    main()
