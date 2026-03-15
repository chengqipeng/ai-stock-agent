"""分析4: 探索突破82%天花板的策略。"""
import sys
sys.path.insert(0, '.')

from collections import defaultdict
from day_week_predicted.backtest.concept_strength_weekly_v3_backtest import (
    _preload_v3_data, _build_weekly_records_v3, _compute_stock_stats_v3,
    _compound_return, _mean, _std,
)
from day_week_predicted.tests.test_concept_strength_weekly_v3_60boards import (
    fetch_boards_from_db,
)

START_DATE = '2025-11-01'
END_DATE = '2026-03-13'

print("加载数据...")
board_stock_map, all_stock_codes = fetch_boards_from_db(min_stocks=20, target_boards=60)
data = _preload_v3_data(all_stock_codes, START_DATE, END_DATE)
weekly = _build_weekly_records_v3(all_stock_codes, data, START_DATE, END_DATE, board_stock_map)
print(f"周样本: {len(weekly)}")

stock_stats = _compute_stock_stats_v3(weekly)

# ── 分析1: 周涨跌幅分布 ──
print("\n" + "="*80)
print("分析1: 周涨跌幅分布")
print("="*80)

wk_chgs = [w['weekly_change'] for w in weekly]
bins = [(-999, -5), (-5, -3), (-3, -1), (-1, -0.5), (-0.5, -0.1), (-0.1, 0),
        (0, 0.1), (0.1, 0.5), (0.5, 1), (1, 3), (3, 5), (5, 999)]
for lo, hi in bins:
    subset = [w for w in weekly if lo <= w['weekly_change'] < hi]
    if not subset: continue
    # 这些样本中d3方向预测正确的比例
    d3_correct = sum(1 for w in subset if (w['d3_chg'] >= 0) == w['weekly_up'])
    print(f"  周涨幅[{lo:+.1f},{hi:+.1f}): {len(subset):5d}条, d3准确{d3_correct/len(subset)*100:.1f}%")

# ── 分析2: 小幅周变化的特征 ──
print("\n" + "="*80)
print("分析2: 小幅周变化(|weekly|<0.5%)的特征")
print("="*80)

small_weekly = [w for w in weekly if abs(w['weekly_change']) < 0.5 and w['weekly_change'] != 0]
print(f"小幅变化: {len(small_weekly)}条")
up_count = sum(1 for w in small_weekly if w['weekly_up'])
print(f"  涨: {up_count}/{len(small_weekly)} = {up_count/len(small_weekly)*100:.1f}%")

# d3方向在小幅变化中的准确率
d3_correct = sum(1 for w in small_weekly if (w['d3_chg'] >= 0) == w['weekly_up'])
print(f"  d3准确: {d3_correct/len(small_weekly)*100:.1f}%")

# ── 分析3: 使用d3幅度预测周变化幅度 ──
print("\n" + "="*80)
print("分析3: d3幅度与周变化幅度的关系")
print("="*80)

# 大d3 → 大周变化 → 方向更确定
d3_bins = [(0, 0.5), (0.5, 1), (1, 2), (2, 3), (3, 5), (5, 999)]
for lo, hi in d3_bins:
    subset = [w for w in weekly if lo <= abs(w['d3_chg']) < hi]
    if not subset: continue
    avg_abs_weekly = _mean([abs(w['weekly_change']) for w in subset])
    d3_correct = sum(1 for w in subset if (w['d3_chg'] >= 0) == w['weekly_up'])
    print(f"  |d3|[{lo},{hi}): {len(subset)}条, 平均|周变化|={avg_abs_weekly:.2f}%, d3准确{d3_correct/len(subset)*100:.1f}%")

# ── 分析4: 多信号组合投票 ──
print("\n" + "="*80)
print("分析4: 多信号组合投票")
print("="*80)

# 信号: d3方向, 大盘方向, 概念方向, 资金流方向, 均值回归方向
def get_signals(w):
    signals = {}
    signals['d3'] = 1 if w['d3_chg'] >= 0 else 0
    signals['market'] = 1 if w.get('market_d3_chg', 0) > 0 else 0
    sig = w['concept_signal']
    if sig:
        signals['concept'] = 1 if sig['composite_score'] > 0 else 0
        signals['fund_flow'] = 1 if sig.get('fund_flow_signal', 0) > 0 else 0
        signals['mr'] = 1 if sig.get('mr_signal', 0) > 0 else 0
        signals['board_momentum'] = 1 if sig.get('board_momentum_5d', 0) > 0 else 0
    return signals

# 测试不同投票阈值
for min_votes in [1, 2, 3, 4, 5]:
    correct = 0
    total = 0
    for w in weekly:
        sigs = get_signals(w)
        votes_up = sum(sigs.values())
        total_votes = len(sigs)
        pred = votes_up >= min_votes
        total += 1
        if pred == w['weekly_up']:
            correct += 1
    print(f"  投票≥{min_votes}: {correct/total*100:.1f}%")

# ── 分析5: 加权投票 ──
print("\n" + "="*80)
print("分析5: 加权投票")
print("="*80)

# d3权重最高
weights = {'d3': 3, 'market': 1, 'concept': 0, 'fund_flow': 0, 'mr': 1, 'board_momentum': 0}
correct = 0
for w in weekly:
    sigs = get_signals(w)
    score = sum(sigs.get(k, 0) * v for k, v in weights.items())
    total_weight = sum(v for k, v in weights.items() if k in sigs)
    pred = score > total_weight / 2
    if pred == w['weekly_up']:
        correct += 1
print(f"  加权(d3=3,mkt=1,mr=1): {correct/len(weekly)*100:.1f}%")

# ── 分析6: 前2天 vs 前3天 ──
print("\n" + "="*80)
print("分析6: 前2天 vs 前3天方向")
print("="*80)

d2_correct = 0
d3_correct = 0
for w in weekly:
    d3_daily = w.get('d3_daily', [])
    if len(d3_daily) >= 2:
        d2_chg = _compound_return(d3_daily[:2])
        if (d2_chg >= 0) == w['weekly_up']:
            d2_correct += 1
    if (w['d3_chg'] >= 0) == w['weekly_up']:
        d3_correct += 1

print(f"  前2天方向: {d2_correct/len(weekly)*100:.1f}%")
print(f"  前3天方向: {d3_correct/len(weekly)*100:.1f}%")

# ── 分析7: 第3天方向 ──
print("\n" + "="*80)
print("分析7: 各单日方向准确率")
print("="*80)

for day_idx in [0, 1, 2]:
    correct = 0
    total = 0
    for w in weekly:
        d3_daily = w.get('d3_daily', [])
        if len(d3_daily) > day_idx:
            total += 1
            if (d3_daily[day_idx] >= 0) == w['weekly_up']:
                correct += 1
    if total:
        print(f"  第{day_idx+1}天方向: {correct/total*100:.1f}%")

# ── 分析8: 概念信号反向使用 ──
print("\n" + "="*80)
print("分析8: 概念信号反向使用")
print("="*80)

# 在模糊区，概念信号是反向的（60.6% vs 54.5%）
# 如果在模糊区反向使用概念信号
fuzzy = [w for w in weekly if abs(w['d3_chg']) <= 0.8]
concept_reverse_correct = 0
for w in fuzzy:
    sig = w['concept_signal']
    if sig and abs(sig['composite_score']) > 0.5:
        # 反向使用
        pred = sig['composite_score'] < 0  # 概念看跌 → 预测涨
    else:
        pred = w['d3_chg'] >= 0
    if pred == w['weekly_up']:
        concept_reverse_correct += 1
print(f"  模糊区概念反向: {concept_reverse_correct/len(fuzzy)*100:.1f}%")

# ── 分析9: 综合最优策略 ──
print("\n" + "="*80)
print("分析9: 综合最优策略搜索")
print("="*80)

# 策略: strong/medium跟d3, fuzzy用d3+概念反向+大盘
best_acc = 0
best_params = None

for concept_reverse in [True, False]:
    for use_market in [True, False]:
        for fuzzy_threshold in [-0.5, -0.3, -0.1, 0.0, 0.1]:
            correct = 0
            for w in weekly:
                ss = stock_stats.get(w['code'])
                d3 = w['d3_chg']
                vol_strong = 2.0
                vol_mid = 0.8
                if ss:
                    vol = ss.get('weekly_volatility', 2.0)
                    if vol > 5.0: vol_strong, vol_mid = 3.5, 1.5
                    elif vol > 4.0: vol_strong, vol_mid = 3.0, 1.2
                    elif vol > 3.0: vol_strong, vol_mid = 2.5, 1.0
                
                if abs(d3) > vol_strong:
                    pred = d3 > 0
                elif abs(d3) > vol_mid:
                    pred = d3 > 0
                else:
                    # fuzzy zone
                    votes = 0
                    total_v = 0
                    
                    # d3方向
                    if d3 >= fuzzy_threshold:
                        votes += 2
                    total_v += 2
                    
                    # 概念信号
                    sig = w['concept_signal']
                    if sig and abs(sig['composite_score']) > 0.5:
                        if concept_reverse:
                            if sig['composite_score'] < 0: votes += 1
                        else:
                            if sig['composite_score'] > 0: votes += 1
                        total_v += 1
                    
                    # 大盘
                    if use_market:
                        if w.get('market_d3_chg', 0) > 0:
                            votes += 1
                        total_v += 1
                    
                    pred = votes > total_v / 2
                
                if pred == w['weekly_up']:
                    correct += 1
            
            acc = correct / len(weekly) * 100
            if acc > best_acc:
                best_acc = acc
                best_params = (concept_reverse, use_market, fuzzy_threshold)

print(f"最优: {best_acc:.1f}%, params={best_params}")

# ── 分析10: 完全不同的方法 - 用d3幅度+方向的回归 ──
print("\n" + "="*80)
print("分析10: d3幅度分段最优策略")
print("="*80)

# 对每个d3幅度区间，找最优预测
d3_fine_bins = []
for i in range(-20, 21):
    lo = i * 0.5
    hi = (i + 1) * 0.5
    d3_fine_bins.append((lo, hi))

total_optimal = 0
total_samples = 0
for lo, hi in d3_fine_bins:
    subset = [w for w in weekly if lo <= w['d3_chg'] < hi]
    if not subset: continue
    up = sum(1 for w in subset if w['weekly_up'])
    down = len(subset) - up
    optimal = max(up, down)
    total_optimal += optimal
    total_samples += len(subset)

# 加上极端值
extreme_up = [w for w in weekly if w['d3_chg'] >= 10]
extreme_down = [w for w in weekly if w['d3_chg'] < -10]
for subset in [extreme_up, extreme_down]:
    if subset:
        up = sum(1 for w in subset if w['weekly_up'])
        down = len(subset) - up
        total_optimal += max(up, down)
        total_samples += len(subset)

print(f"d3分段最优: {total_optimal}/{len(weekly)} = {total_optimal/len(weekly)*100:.1f}%")

# ── 分析11: 用d3_chg的精确值做回归预测 ──
print("\n" + "="*80)
print("分析11: d3_chg精确阈值搜索")
print("="*80)

# 找最优的单一阈值: d3 >= threshold → UP
best_threshold_acc = 0
best_threshold = 0
for t in [i * 0.1 for i in range(-30, 31)]:
    correct = sum(1 for w in weekly if (w['d3_chg'] >= t) == w['weekly_up'])
    acc = correct / len(weekly) * 100
    if acc > best_threshold_acc:
        best_threshold_acc = acc
        best_threshold = t

print(f"最优单一阈值: d3>={best_threshold:.1f}, 准确率{best_threshold_acc:.1f}%")

# ── 分析12: 多阈值策略 ──
print("\n" + "="*80)
print("分析12: 多阈值策略")
print("="*80)

# 不同d3区间用不同的预测方向
# 基于分析2的数据: d3 [0, 0.001) 有98.3%涨率（停牌）
# 排除停牌后重新分析
active = [w for w in weekly if not (w.get('d3_daily') and all(d == 0 for d in w['d3_daily']))]
print(f"活跃样本: {len(active)}")

# 活跃样本的d3分段
for lo, hi in [(-10, -3), (-3, -1), (-1, -0.5), (-0.5, 0), (0, 0.5), (0.5, 1), (1, 3), (3, 10)]:
    subset = [w for w in active if lo <= w['d3_chg'] < hi]
    if not subset: continue
    up = sum(1 for w in subset if w['weekly_up'])
    print(f"  d3[{lo:+.1f},{hi:+.1f}): {len(subset)}条, 涨{up/len(subset)*100:.1f}%")

print("\n完成")
