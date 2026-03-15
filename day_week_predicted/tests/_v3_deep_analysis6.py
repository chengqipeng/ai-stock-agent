"""分析6: mr_signal反向使用 + d3组合的最优策略。"""
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

# ── 分析1: d3 + mr_signal 组合 ──
print("\n" + "="*80)
print("分析1: d3 + mr_signal(反向) 组合")
print("="*80)

# mr_signal > 0 意味着股价低于均线 → 实际上预示继续跌
# mr_signal < 0 意味着股价高于均线 → 实际上预示继续涨（动量效应）

for w_d3 in [1.0, 1.5, 2.0, 3.0]:
    for w_mr in [-0.3, -0.5, -0.8, -1.0, -1.5]:
        for bias in [-0.1, 0, 0.1, 0.2]:
            correct = 0
            for w in weekly:
                sig = w['concept_signal']
                mr = sig.get('mr_signal', 0) if sig else 0
                score = w_d3 * w['d3_chg'] + w_mr * mr + bias
                pred = score >= 0
                if pred == w['weekly_up']:
                    correct += 1
            acc = correct / len(weekly) * 100
            if acc > 82.5:
                print(f"  w_d3={w_d3}, w_mr={w_mr}, bias={bias}: {acc:.1f}%")

# ── 分析2: d3方向 + mr方向(反向) 投票 ──
print("\n" + "="*80)
print("分析2: d3方向 + mr反向 投票")
print("="*80)

# d3>=0 且 mr<=0 → 强涨
# d3>=0 且 mr>0 → 弱涨
# d3<0 且 mr<=0 → 弱跌
# d3<0 且 mr>0 → 强跌
combos = defaultdict(lambda: [0, 0])
for w in weekly:
    sig = w['concept_signal']
    mr = sig.get('mr_signal', 0) if sig else 0
    d3_up = w['d3_chg'] >= 0
    mr_down = mr <= 0  # mr反向: mr<=0 → 看涨
    
    key = f"d3{'↑' if d3_up else '↓'}+mr{'↑' if mr_down else '↓'}"
    combos[key][1] += 1
    if w['weekly_up']:
        combos[key][0] += 1

for key, (up, total) in sorted(combos.items()):
    print(f"  {key}: {total}条, 涨{up/total*100:.1f}%")

# ── 分析3: 在medium区使用mr反向 ──
print("\n" + "="*80)
print("分析3: medium区 d3+mr反向")
print("="*80)

correct_base = 0
correct_mr = 0
for w in weekly:
    ss = stock_stats.get(w['code'])
    d3 = w['d3_chg']
    sig = w['concept_signal']
    mr = sig.get('mr_signal', 0) if sig else 0
    
    vol_strong = 2.0
    vol_mid = 0.8
    if ss:
        vol = ss.get('weekly_volatility', 2.0)
        if vol > 5.0: vol_strong, vol_mid = 3.5, 1.5
        elif vol > 4.0: vol_strong, vol_mid = 3.0, 1.2
        elif vol > 3.0: vol_strong, vol_mid = 2.5, 1.0
    
    # 基准: 跟d3
    pred_base = d3 >= 0
    if pred_base == w['weekly_up']:
        correct_base += 1
    
    # mr增强
    if abs(d3) > vol_strong:
        pred = d3 > 0
    elif abs(d3) > vol_mid:
        # medium: d3方向，但如果mr强烈反对则翻转
        pred = d3 > 0
        if abs(mr) > 0.5:
            mr_agrees = (mr < 0) == (d3 > 0)  # mr反向使用
            if not mr_agrees and abs(d3) < vol_strong * 0.7:
                pred = not pred  # 翻转
    else:
        # fuzzy: 用d3+mr组合
        score = d3 - mr * 0.5
        pred = score >= 0
    
    if pred == w['weekly_up']:
        correct_mr += 1

print(f"基准(全部跟d3): {correct_base/len(weekly)*100:.1f}%")
print(f"mr增强: {correct_mr/len(weekly)*100:.1f}%")

# ── 分析4: 纯d3+mr线性组合搜索 ──
print("\n" + "="*80)
print("分析4: d3+mr线性组合网格搜索")
print("="*80)

best_acc = 0
best_params = None
for w_d3 in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0]:
    for w_mr in [-2.0, -1.5, -1.0, -0.8, -0.5, -0.3, -0.1, 0]:
        for bias in [-0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3]:
            correct = 0
            for w in weekly:
                sig = w['concept_signal']
                mr = sig.get('mr_signal', 0) if sig else 0
                score = w_d3 * w['d3_chg'] + w_mr * mr + bias
                pred = score >= 0
                if pred == w['weekly_up']:
                    correct += 1
            acc = correct / len(weekly) * 100
            if acc > best_acc:
                best_acc = acc
                best_params = (w_d3, w_mr, bias)

print(f"最优: {best_acc:.1f}%, w_d3={best_params[0]}, w_mr={best_params[1]}, bias={best_params[2]}")

# ── 分析5: d3 + d1 + mr 三特征 ──
print("\n" + "="*80)
print("分析5: d3 + d1 + mr 三特征")
print("="*80)

best_acc3 = 0
best_params3 = None
for w_d3 in [1.0, 2.0, 3.0]:
    for w_d1 in [0, 0.5, 1.0]:
        for w_mr in [-1.0, -0.5, -0.3, 0]:
            for bias in [-0.1, 0, 0.1, 0.2]:
                correct = 0
                for w in weekly:
                    sig = w['concept_signal']
                    mr = sig.get('mr_signal', 0) if sig else 0
                    d1 = w.get('d3_daily', [0])[0]
                    score = w_d3 * w['d3_chg'] + w_d1 * d1 + w_mr * mr + bias
                    pred = score >= 0
                    if pred == w['weekly_up']:
                        correct += 1
                acc = correct / len(weekly) * 100
                if acc > best_acc3:
                    best_acc3 = acc
                    best_params3 = (w_d3, w_d1, w_mr, bias)

print(f"最优: {best_acc3:.1f}%, params={best_params3}")

# ── 分析6: LOWO验证最优策略 ──
print("\n" + "="*80)
print("分析6: LOWO验证")
print("="*80)

# 用最优参数做LOWO
all_weeks = sorted(set(w['iso_week'] for w in weekly))
week_accs = []
total_correct = 0
total_count = 0

for held_week in all_weeks:
    correct = 0
    count = 0
    for w in weekly:
        if w['iso_week'] == held_week:
            sig = w['concept_signal']
            mr = sig.get('mr_signal', 0) if sig else 0
            # 用最优线性参数
            wd3, wmr, b = best_params
            score = wd3 * w['d3_chg'] + wmr * mr + b
            pred = score >= 0
            count += 1
            if pred == w['weekly_up']:
                correct += 1
    
    if count > 0:
        acc = correct / count * 100
        week_accs.append(acc)
        total_correct += correct
        total_count += count

overall = total_correct / total_count * 100
avg_week = _mean(week_accs)
print(f"LOWO总体: {overall:.1f}%")
print(f"LOWO平均周: {avg_week:.1f}%")
print(f"最低周: {min(week_accs):.1f}%, 最高周: {max(week_accs):.1f}%")

# ── 分析7: 分区策略 + mr ──
print("\n" + "="*80)
print("分析7: 分区策略 + mr反向")
print("="*80)

# strong: 跟d3 (91%)
# medium: d3 + mr反向加权
# fuzzy: d3 + mr反向加权

for mr_weight_med in [0, -0.3, -0.5, -0.8]:
    for mr_weight_fuzzy in [0, -0.3, -0.5, -0.8, -1.0]:
        for fuzzy_bias in [-0.1, 0, 0.1, 0.2, 0.3]:
            correct = 0
            for w in weekly:
                ss = stock_stats.get(w['code'])
                d3 = w['d3_chg']
                sig = w['concept_signal']
                mr = sig.get('mr_signal', 0) if sig else 0
                
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
                    score = d3 + mr_weight_med * mr
                    pred = score > 0
                else:
                    score = d3 + mr_weight_fuzzy * mr + fuzzy_bias
                    pred = score >= 0
                
                if pred == w['weekly_up']:
                    correct += 1
            
            acc = correct / len(weekly) * 100
            if acc > 82.0:
                print(f"  mr_med={mr_weight_med}, mr_fuzzy={mr_weight_fuzzy}, bias={fuzzy_bias}: {acc:.1f}%")

print("\n完成")
