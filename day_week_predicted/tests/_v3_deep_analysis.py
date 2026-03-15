"""深度分析v3回测数据，寻找提升到85%的策略。"""
import sys
sys.path.insert(0, '.')

from collections import defaultdict
from dao import get_connection
from day_week_predicted.backtest.concept_strength_weekly_v3_backtest import (
    _preload_v3_data, _build_weekly_records_v3, _compute_stock_stats_v3,
    predict_weekly_direction_v3, _compound_return, _mean, _std,
)
from day_week_predicted.tests.test_concept_strength_weekly_v3_60boards import (
    fetch_boards_from_db, _add_suffix,
)

START_DATE = '2025-11-01'
END_DATE = '2026-03-13'

print("获取板块...")
board_stock_map, all_stock_codes = fetch_boards_from_db(min_stocks=20, target_boards=60)
print(f"板块: {len(board_stock_map)}, 股票: {len(all_stock_codes)}")

print("加载数据...")
data = _preload_v3_data(all_stock_codes, START_DATE, END_DATE)

print("构建周数据...")
weekly = _build_weekly_records_v3(all_stock_codes, data, START_DATE, END_DATE, board_stock_map)
print(f"周样本: {len(weekly)}")

stock_stats = _compute_stock_stats_v3(weekly)

# ── 分析1: 每个置信度区间的错误模式 ──
print("\n" + "="*80)
print("分析1: 各区间详细错误模式")
print("="*80)

zones = {'strong_up': [], 'strong_down': [], 'medium_up': [], 'medium_down': [],
         'fuzzy_up_bias': [], 'fuzzy_down': []}

for w in weekly:
    sig = w['concept_signal']
    ss = stock_stats.get(w['code'])
    d3 = w['d3_chg']
    
    # 确定自适应阈值
    vol_strong = 2.0
    vol_mid = 0.8
    if ss:
        vol = ss.get('weekly_volatility', 2.0)
        if vol > 5.0: vol_strong, vol_mid = 3.5, 1.5
        elif vol > 4.0: vol_strong, vol_mid = 3.0, 1.2
        elif vol > 3.0: vol_strong, vol_mid = 2.5, 1.0
    
    if abs(d3) > vol_strong:
        if d3 > 0: zones['strong_up'].append(w)
        else: zones['strong_down'].append(w)
    elif abs(d3) > vol_mid:
        if d3 > 0: zones['medium_up'].append(w)
        else: zones['medium_down'].append(w)
    else:
        if d3 > -0.3: zones['fuzzy_up_bias'].append(w)
        else: zones['fuzzy_down'].append(w)

for zone_name, records in zones.items():
    correct = sum(1 for r in records if 
                  (predict_weekly_direction_v3(r['d3_chg'], r['concept_signal'],
                   stock_stats.get(r['code']), r.get('d3_daily'),
                   r.get('market_d3_chg', 0.0))[0] == r['weekly_up']))
    total = len(records)
    acc = correct/total*100 if total else 0
    actual_up = sum(1 for r in records if r['weekly_up'])
    print(f"\n{zone_name}: {total}条, 准确率{acc:.1f}%, 实际涨{actual_up/total*100:.1f}%")

# ── 分析2: 模糊区细分 ──
print("\n" + "="*80)
print("分析2: 模糊区细分分析")
print("="*80)

fuzzy_all = zones['fuzzy_up_bias'] + zones['fuzzy_down']
# 按d3_chg细分
bins = [(-999, -0.5), (-0.5, -0.3), (-0.3, -0.1), (-0.1, 0.0), 
        (0.0, 0.1), (0.1, 0.3), (0.3, 0.5), (0.5, 999)]
for lo, hi in bins:
    subset = [r for r in fuzzy_all if lo <= r['d3_chg'] < hi]
    if not subset: continue
    up_count = sum(1 for r in subset if r['weekly_up'])
    up_rate = up_count/len(subset)*100
    print(f"  d3 [{lo:+.1f}, {hi:+.1f}): {len(subset)}条, 实际涨{up_rate:.1f}%")

# ── 分析3: 概念信号在各区间的有效性 ──
print("\n" + "="*80)
print("分析3: 概念信号在medium区的有效性")
print("="*80)

medium_all = zones['medium_up'] + zones['medium_down']
# 当d3方向和概念方向不一致时
disagree_count = 0
disagree_correct_follow_d3 = 0
disagree_correct_follow_concept = 0
for r in medium_all:
    sig = r['concept_signal']
    if not sig: continue
    cs = sig['composite_score']
    d3_up = r['d3_chg'] > 0
    concept_up = cs > 0
    if d3_up != concept_up:
        disagree_count += 1
        if d3_up == r['weekly_up']:
            disagree_correct_follow_d3 += 1
        if concept_up == r['weekly_up']:
            disagree_correct_follow_concept += 1

print(f"Medium区d3与概念不一致: {disagree_count}条")
if disagree_count > 0:
    print(f"  跟d3准确率: {disagree_correct_follow_d3/disagree_count*100:.1f}%")
    print(f"  跟概念准确率: {disagree_correct_follow_concept/disagree_count*100:.1f}%")

# ── 分析4: 个股历史准确率的预测价值 ──
print("\n" + "="*80)
print("分析4: 个股d3准确率的预测价值")
print("="*80)

# 按个股d3_direction_accuracy分组
d3_acc_bins = [(0, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.01)]
for lo, hi in d3_acc_bins:
    subset = []
    for w in weekly:
        ss = stock_stats.get(w['code'])
        if ss and lo <= ss['d3_direction_accuracy'] < hi:
            subset.append(w)
    if not subset: continue
    correct = sum(1 for r in subset if 
                  (predict_weekly_direction_v3(r['d3_chg'], r['concept_signal'],
                   stock_stats.get(r['code']), r.get('d3_daily'),
                   r.get('market_d3_chg', 0.0))[0] == r['weekly_up']))
    print(f"  d3_acc [{lo:.1f}, {hi:.1f}): {len(subset)}条, 准确率{correct/len(subset)*100:.1f}%")

# ── 分析5: 周波动率与准确率 ──
print("\n" + "="*80)
print("分析5: 个股周波动率与准确率")
print("="*80)

vol_bins = [(0, 2), (2, 3), (3, 4), (4, 5), (5, 8), (8, 999)]
for lo, hi in vol_bins:
    subset = []
    for w in weekly:
        ss = stock_stats.get(w['code'])
        if ss and lo <= ss['weekly_volatility'] < hi:
            subset.append(w)
    if not subset: continue
    correct = sum(1 for r in subset if 
                  (predict_weekly_direction_v3(r['d3_chg'], r['concept_signal'],
                   stock_stats.get(r['code']), r.get('d3_daily'),
                   r.get('market_d3_chg', 0.0))[0] == r['weekly_up']))
    print(f"  vol [{lo}, {hi}): {len(subset)}条, 准确率{correct/len(subset)*100:.1f}%")

# ── 分析6: 大盘方向对模糊区的影响 ──
print("\n" + "="*80)
print("分析6: 大盘d3方向对模糊区的影响")
print("="*80)

for zone_name in ['fuzzy_up_bias', 'fuzzy_down']:
    records = zones[zone_name]
    mkt_up = [r for r in records if r.get('market_d3_chg', 0) > 0]
    mkt_down = [r for r in records if r.get('market_d3_chg', 0) <= 0]
    for label, subset in [('大盘涨', mkt_up), ('大盘跌', mkt_down)]:
        if not subset: continue
        actual_up = sum(1 for r in subset if r['weekly_up'])
        print(f"  {zone_name} + {label}: {len(subset)}条, 实际涨{actual_up/len(subset)*100:.1f}%")

# ── 分析7: 前3天每日方向一致性 ──
print("\n" + "="*80)
print("分析7: 前3天每日方向一致性")
print("="*80)

for zone_name in ['fuzzy_up_bias', 'fuzzy_down', 'medium_up', 'medium_down']:
    records = zones[zone_name]
    # 3天方向一致 vs 不一致
    consistent = [r for r in records if r.get('d3_daily') and 
                  all(d > 0 for d in r['d3_daily']) or all(d < 0 for d in r['d3_daily'])]
    mixed = [r for r in records if r not in consistent]
    
    for label, subset in [('一致', consistent), ('混合', mixed)]:
        if not subset: continue
        actual_up = sum(1 for r in subset if r['weekly_up'])
        d3_correct = sum(1 for r in subset if (r['d3_chg'] > 0) == r['weekly_up'])
        print(f"  {zone_name} {label}: {len(subset)}条, d3准确{d3_correct/len(subset)*100:.1f}%, 涨{actual_up/len(subset)*100:.1f}%")

# ── 分析8: 最后一天(d3)方向 vs 前3天累计方向 ──
print("\n" + "="*80)
print("分析8: 第3天单日方向 vs 前3天累计方向")
print("="*80)

for zone_name in ['fuzzy_up_bias', 'fuzzy_down']:
    records = zones[zone_name]
    for r in records:
        if not r.get('d3_daily') or len(r['d3_daily']) < 3:
            continue
    
    # d3第3天方向
    d3_day3_correct = sum(1 for r in records if r.get('d3_daily') and len(r['d3_daily']) >= 3
                          and (r['d3_daily'][2] > 0) == r['weekly_up'])
    d3_day3_total = sum(1 for r in records if r.get('d3_daily') and len(r['d3_daily']) >= 3)
    if d3_day3_total:
        print(f"  {zone_name}: 第3天方向准确率 {d3_day3_correct/d3_day3_total*100:.1f}% ({d3_day3_total}条)")
    
    # d3累计方向
    d3_cum_correct = sum(1 for r in records if (r['d3_chg'] > 0) == r['weekly_up'])
    print(f"  {zone_name}: 累计d3方向准确率 {d3_cum_correct/len(records)*100:.1f}% ({len(records)}条)")

# ── 分析9: 周内剩余天数(d4+d5)的反转率 ──
print("\n" + "="*80)
print("分析9: 各区间d4+d5反转率")
print("="*80)

for zone_name, records in zones.items():
    reversal = 0
    total_with_45 = 0
    for r in records:
        if r['n_days'] < 5: continue
        total_with_45 += 1
        d3_up = r['d3_chg'] > 0
        d45_chg = _compound_return(r['daily_changes'][3:])
        d45_reverses = (d45_chg > 0) != d3_up
        if d45_reverses:
            reversal += 1
    if total_with_45:
        print(f"  {zone_name}: d4+d5反转率 {reversal/total_with_45*100:.1f}% ({total_with_45}条)")

# ── 分析10: 寻找medium区可以提升到high的条件 ──
print("\n" + "="*80)
print("分析10: Medium区提升分析")
print("="*80)

for zone_name in ['medium_up', 'medium_down']:
    records = zones[zone_name]
    # 按concept_score强度分组
    strong_concept = [r for r in records if r['concept_signal'] and 
                      abs(r['concept_signal']['composite_score']) > 2.0]
    weak_concept = [r for r in records if r['concept_signal'] and 
                    abs(r['concept_signal']['composite_score']) <= 2.0]
    
    for label, subset in [('强概念(|cs|>2)', strong_concept), ('弱概念(|cs|≤2)', weak_concept)]:
        if not subset: continue
        d3_correct = sum(1 for r in subset if (r['d3_chg'] > 0) == r['weekly_up'])
        print(f"  {zone_name} {label}: {len(subset)}条, d3准确{d3_correct/len(subset)*100:.1f}%")

# ── 分析11: 高波动股票的特殊处理 ──
print("\n" + "="*80)
print("分析11: 高波动股票(vol>5)的模糊区分析")
print("="*80)

high_vol_fuzzy = []
low_vol_fuzzy = []
for w in fuzzy_all:
    ss = stock_stats.get(w['code'])
    if ss and ss['weekly_volatility'] > 5:
        high_vol_fuzzy.append(w)
    else:
        low_vol_fuzzy.append(w)

for label, subset in [('高波动(vol>5)', high_vol_fuzzy), ('低波动(vol≤5)', low_vol_fuzzy)]:
    if not subset: continue
    actual_up = sum(1 for r in subset if r['weekly_up'])
    d3_correct = sum(1 for r in subset if (r['d3_chg'] > 0) == r['weekly_up'])
    print(f"  {label}: {len(subset)}条, 涨{actual_up/len(subset)*100:.1f}%, d3准确{d3_correct/len(subset)*100:.1f}%")

# ── 分析12: 概念信号在strong区的反向信号 ──
print("\n" + "="*80)
print("分析12: Strong区概念反向信号")
print("="*80)

for zone_name in ['strong_up', 'strong_down']:
    records = zones[zone_name]
    # d3方向和概念方向不一致
    disagree = [r for r in records if r['concept_signal'] and 
                (r['d3_chg'] > 0) != (r['concept_signal']['composite_score'] > 0)]
    agree = [r for r in records if r['concept_signal'] and 
             (r['d3_chg'] > 0) == (r['concept_signal']['composite_score'] > 0)]
    
    for label, subset in [('d3概念一致', agree), ('d3概念不一致', disagree)]:
        if not subset: continue
        d3_correct = sum(1 for r in subset if (r['d3_chg'] > 0) == r['weekly_up'])
        print(f"  {zone_name} {label}: {len(subset)}条, d3准确{d3_correct/len(subset)*100:.1f}%")

print("\n" + "="*80)
print("分析完成")
