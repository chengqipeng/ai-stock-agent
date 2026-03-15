"""分析3: 探索per-stock adaptive策略和阈值优化。"""
import sys
sys.path.insert(0, '.')

from collections import defaultdict
from day_week_predicted.backtest.concept_strength_weekly_v3_backtest import (
    _preload_v3_data, _build_weekly_records_v3, _compute_stock_stats_v3,
    predict_weekly_direction_v3, _compound_return, _mean, _std,
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

# ── 分析: 停牌股票的影响 ──
print("\n" + "="*80)
print("分析1: 停牌/零变化股票")
print("="*80)

zero_d3 = [w for w in weekly if w['d3_chg'] == 0.0]
zero_weekly = [w for w in weekly if w['weekly_change'] == 0.0]
zero_both = [w for w in weekly if w['d3_chg'] == 0.0 and w['weekly_change'] == 0.0]
print(f"d3=0: {len(zero_d3)}, weekly=0: {len(zero_weekly)}, 两者都=0: {len(zero_both)}")

# 如果排除这些，准确率如何？
non_zero = [w for w in weekly if w['weekly_change'] != 0.0]
correct_nz = 0
for w in non_zero:
    ss = stock_stats.get(w['code'])
    pred, _, _ = predict_weekly_direction_v3(w['d3_chg'], w['concept_signal'], ss,
                                              w.get('d3_daily'), w.get('market_d3_chg', 0.0))
    if pred == w['weekly_up']:
        correct_nz += 1
print(f"排除零变化后: {correct_nz}/{len(non_zero)} = {correct_nz/len(non_zero)*100:.1f}%")

# ── 分析2: 如果把weekly_change=0视为"正确"（不管预测什么） ──
print("\n" + "="*80)
print("分析2: 零变化视为正确")
print("="*80)

correct_with_zero = 0
for w in weekly:
    if w['weekly_change'] == 0.0:
        correct_with_zero += 1
        continue
    ss = stock_stats.get(w['code'])
    pred, _, _ = predict_weekly_direction_v3(w['d3_chg'], w['concept_signal'], ss,
                                              w.get('d3_daily'), w.get('market_d3_chg', 0.0))
    if pred == w['weekly_up']:
        correct_with_zero += 1
print(f"零变化视为正确: {correct_with_zero}/{len(weekly)} = {correct_with_zero/len(weekly)*100:.1f}%")

# ── 分析3: 个股历史涨率作为预测信号 ──
print("\n" + "="*80)
print("分析3: 个股历史涨率预测")
print("="*80)

# 计算每只股票的历史涨率（排除当前周）
all_weeks = sorted(set(w['iso_week'] for w in weekly))

# LOWO方式计算
stock_week_data = defaultdict(list)
for w in weekly:
    stock_week_data[w['code']].append(w)

# 对每只股票，用历史涨率预测
correct_hist = 0
total_hist = 0
for code, weeks_data in stock_week_data.items():
    for i, w in enumerate(weeks_data):
        # 用其他周的数据计算涨率
        other_weeks = [ow for ow in weeks_data if ow['iso_week'] != w['iso_week']]
        if len(other_weeks) < 3:
            continue
        up_rate = sum(1 for ow in other_weeks if ow['weekly_up']) / len(other_weeks)
        pred = up_rate >= 0.5
        total_hist += 1
        if pred == w['weekly_up']:
            correct_hist += 1

print(f"纯历史涨率预测: {correct_hist}/{total_hist} = {correct_hist/total_hist*100:.1f}%")

# ── 分析4: 组合策略 - d3方向 + 个股历史涨率 ──
print("\n" + "="*80)
print("分析4: d3方向 + 个股历史涨率组合")
print("="*80)

# 预计算每只股票每周的历史涨率
stock_hist_up_rate = {}
for code, weeks_data in stock_week_data.items():
    for w in weeks_data:
        other = [ow for ow in weeks_data if ow['iso_week'] != w['iso_week']]
        if len(other) >= 3:
            rate = sum(1 for ow in other if ow['weekly_up']) / len(other)
        else:
            rate = 0.5
        stock_hist_up_rate[(code, w['iso_week'])] = rate

# 策略: strong/medium跟d3, fuzzy用历史涨率
correct_combo = 0
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
    
    if abs(d3) > vol_mid:
        pred = d3 > 0
    else:
        hist_rate = stock_hist_up_rate.get((w['code'], w['iso_week']), 0.5)
        if hist_rate >= 0.6:
            pred = True
        elif hist_rate <= 0.4:
            pred = False
        else:
            pred = d3 >= 0
    
    if pred == w['weekly_up']:
        correct_combo += 1
print(f"d3+历史涨率: {correct_combo}/{len(weekly)} = {correct_combo/len(weekly)*100:.1f}%")

# ── 分析5: 更激进的per-stock adaptive ──
print("\n" + "="*80)
print("分析5: Per-stock adaptive (LOWO)")
print("="*80)

# 对每只股票，用历史数据训练最优阈值
correct_adaptive = 0
for w in weekly:
    ss = stock_stats.get(w['code'])
    d3 = w['d3_chg']
    hist_rate = stock_hist_up_rate.get((w['code'], w['iso_week']), 0.5)
    
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
        # 模糊区: 综合d3方向和历史涨率
        d3_vote = 1 if d3 >= 0 else 0
        hist_vote = 1 if hist_rate >= 0.55 else 0
        pred = (d3_vote + hist_vote) >= 1
    
    if pred == w['weekly_up']:
        correct_adaptive += 1
print(f"Adaptive(d3+hist投票): {correct_adaptive}/{len(weekly)} = {correct_adaptive/len(weekly)*100:.1f}%")

# ── 分析6: 最大理论准确率（oracle per-stock） ──
print("\n" + "="*80)
print("分析6: 理论上限分析")
print("="*80)

# 对每只股票，如果我们知道它的最优策略（总是涨 or 总是跌 or 跟d3）
oracle_correct = 0
for code, weeks_data in stock_week_data.items():
    # 策略1: 总是涨
    always_up = sum(1 for w in weeks_data if w['weekly_up'])
    # 策略2: 总是跌
    always_down = len(weeks_data) - always_up
    # 策略3: 跟d3
    follow_d3 = sum(1 for w in weeks_data if (w['d3_chg'] >= 0) == w['weekly_up'])
    
    oracle_correct += max(always_up, always_down, follow_d3)

print(f"Oracle per-stock: {oracle_correct}/{len(weekly)} = {oracle_correct/len(weekly)*100:.1f}%")

# ── 分析7: 如果用d3_direction_accuracy来加权 ──
print("\n" + "="*80)
print("分析7: d3_acc加权策略")
print("="*80)

# 高d3_acc的股票跟d3，低d3_acc的股票用历史涨率
for d3_acc_threshold in [0.6, 0.65, 0.7, 0.75, 0.8]:
    correct_w = 0
    for w in weekly:
        ss = stock_stats.get(w['code'])
        d3 = w['d3_chg']
        d3_acc = ss['d3_direction_accuracy'] if ss else 0.5
        hist_rate = stock_hist_up_rate.get((w['code'], w['iso_week']), 0.5)
        
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
            if d3_acc >= d3_acc_threshold:
                pred = d3 >= 0
            else:
                pred = hist_rate >= 0.5
        
        if pred == w['weekly_up']:
            correct_w += 1
    print(f"  d3_acc阈值{d3_acc_threshold}: {correct_w/len(weekly)*100:.1f}%")

# ── 分析8: 扩大strong区 ──
print("\n" + "="*80)
print("分析8: 调整strong/medium阈值")
print("="*80)

for s_thresh, m_thresh in [(1.5, 0.5), (1.5, 0.3), (1.2, 0.3), (1.0, 0.3), (1.8, 0.6)]:
    correct_t = 0
    zone_counts = {'strong': 0, 'medium': 0, 'fuzzy': 0}
    zone_correct = {'strong': 0, 'medium': 0, 'fuzzy': 0}
    for w in weekly:
        ss = stock_stats.get(w['code'])
        d3 = w['d3_chg']
        vs = s_thresh
        vm = m_thresh
        if ss:
            vol = ss.get('weekly_volatility', 2.0)
            if vol > 5.0: vs *= 1.75; vm *= 1.875
            elif vol > 4.0: vs *= 1.5; vm *= 1.5
            elif vol > 3.0: vs *= 1.25; vm *= 1.25
        
        if abs(d3) > vs:
            pred = d3 > 0
            zone = 'strong'
        elif abs(d3) > vm:
            pred = d3 > 0
            zone = 'medium'
        else:
            pred = d3 >= 0
            zone = 'fuzzy'
        
        zone_counts[zone] += 1
        if pred == w['weekly_up']:
            correct_t += 1
            zone_correct[zone] += 1
    
    total_acc = correct_t/len(weekly)*100
    zone_accs = {z: zone_correct[z]/zone_counts[z]*100 if zone_counts[z] > 0 else 0 
                 for z in zone_counts}
    print(f"  s={s_thresh}/m={m_thresh}: {total_acc:.1f}% | "
          f"strong {zone_accs['strong']:.1f}%({zone_counts['strong']}) "
          f"medium {zone_accs['medium']:.1f}%({zone_counts['medium']}) "
          f"fuzzy {zone_accs['fuzzy']:.1f}%({zone_counts['fuzzy']})")

# ── 分析9: 完全per-stock自适应 ──
print("\n" + "="*80)
print("分析9: 完全per-stock自适应 (LOWO)")
print("="*80)

# 对每只股票，用历史数据选择最优策略
correct_full_adaptive = 0
for w in weekly:
    code = w['code']
    iso_week = w['iso_week']
    
    # 获取该股票的历史数据（排除当前周）
    hist = [ow for ow in stock_week_data[code] if ow['iso_week'] != iso_week]
    if len(hist) < 3:
        # 默认跟d3
        pred = w['d3_chg'] >= 0
    else:
        # 评估3种策略在历史上的表现
        # 策略1: 跟d3
        d3_correct = sum(1 for h in hist if (h['d3_chg'] >= 0) == h['weekly_up'])
        # 策略2: 总是涨
        always_up = sum(1 for h in hist if h['weekly_up'])
        # 策略3: 总是跌
        always_down = len(hist) - always_up
        
        best = max(d3_correct, always_up, always_down)
        if best == d3_correct:
            pred = w['d3_chg'] >= 0
        elif best == always_up:
            pred = True
        else:
            pred = False
    
    if pred == w['weekly_up']:
        correct_full_adaptive += 1

print(f"完全自适应: {correct_full_adaptive}/{len(weekly)} = {correct_full_adaptive/len(weekly)*100:.1f}%")

# ── 分析10: 混合策略 - strong/medium用d3, fuzzy用per-stock最优 ──
print("\n" + "="*80)
print("分析10: 混合策略")
print("="*80)

correct_hybrid = 0
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
    
    if abs(d3) > vol_mid:
        pred = d3 > 0
    else:
        # fuzzy: per-stock最优策略
        code = w['code']
        iso_week = w['iso_week']
        hist = [ow for ow in stock_week_data[code] if ow['iso_week'] != iso_week]
        if len(hist) < 3:
            pred = d3 >= 0
        else:
            d3_correct = sum(1 for h in hist if (h['d3_chg'] >= 0) == h['weekly_up'])
            always_up = sum(1 for h in hist if h['weekly_up'])
            always_down = len(hist) - always_up
            
            best = max(d3_correct, always_up, always_down)
            if best == d3_correct:
                pred = d3 >= 0
            elif best == always_up:
                pred = True
            else:
                pred = False
    
    if pred == w['weekly_up']:
        correct_hybrid += 1

print(f"混合策略: {correct_hybrid}/{len(weekly)} = {correct_hybrid/len(weekly)*100:.1f}%")

# ── 分析11: 如果排除停牌股票 ──
print("\n" + "="*80)
print("分析11: 排除停牌股票的影响")
print("="*80)

# 停牌 = 所有3天change_percent都是0
suspended = [w for w in weekly if w.get('d3_daily') and all(d == 0 for d in w['d3_daily'])]
active = [w for w in weekly if w not in suspended]
print(f"停牌样本: {len(suspended)}, 活跃样本: {len(active)}")

# 停牌股票的weekly_change
susp_up = sum(1 for w in suspended if w['weekly_up'])
print(f"停牌涨: {susp_up}/{len(suspended)} = {susp_up/len(suspended)*100:.1f}%")

# 活跃股票的准确率
correct_active = 0
for w in active:
    ss = stock_stats.get(w['code'])
    pred, _, _ = predict_weekly_direction_v3(w['d3_chg'], w['concept_signal'], ss,
                                              w.get('d3_daily'), w.get('market_d3_chg', 0.0))
    if pred == w['weekly_up']:
        correct_active += 1
print(f"活跃股票准确率: {correct_active}/{len(active)} = {correct_active/len(active)*100:.1f}%")

# 如果停牌股票全部预测正确（它们d3=0, weekly_up=True, 我们预测UP）
# 那么总准确率 = (correct_active + len(suspended)) / len(weekly)
total_with_susp = correct_active + len(suspended)
print(f"停牌全对+活跃: {total_with_susp}/{len(weekly)} = {total_with_susp/len(weekly)*100:.1f}%")

print("\n完成")
