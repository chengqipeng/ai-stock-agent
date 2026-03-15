"""分析5: 机器学习方法探索 + 特征工程。"""
import sys
sys.path.insert(0, '.')

from collections import defaultdict
import math
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

# ── 分析1: 特征提取 ──
print("\n" + "="*80)
print("分析1: 特征重要性分析")
print("="*80)

# 提取所有特征
features_list = []
labels = []
for w in weekly:
    sig = w['concept_signal']
    ss = stock_stats.get(w['code'])
    d3_daily = w.get('d3_daily', [0, 0, 0])
    
    f = {
        'd3_chg': w['d3_chg'],
        'd1': d3_daily[0] if len(d3_daily) > 0 else 0,
        'd2': d3_daily[1] if len(d3_daily) > 1 else 0,
        'd3': d3_daily[2] if len(d3_daily) > 2 else 0,
        'abs_d3': abs(w['d3_chg']),
        'market_d3': w.get('market_d3_chg', 0),
        'd3_positive': 1 if w['d3_chg'] >= 0 else 0,
        'market_positive': 1 if w.get('market_d3_chg', 0) > 0 else 0,
    }
    
    if sig:
        f['composite_score'] = sig['composite_score']
        f['board_market_score'] = sig['board_market_score']
        f['stock_board_score'] = sig['stock_board_score']
        f['board_momentum'] = sig.get('board_momentum_5d', 0)
        f['fund_flow'] = sig.get('fund_flow_signal', 0)
        f['mr_signal'] = sig.get('mr_signal', 0)
        f['concept_consensus'] = sig.get('concept_consensus', 0.5)
        f['trend_consistency'] = sig.get('trend_consistency', 0.5)
    else:
        f['composite_score'] = 0
        f['board_market_score'] = 50
        f['stock_board_score'] = 50
        f['board_momentum'] = 0
        f['fund_flow'] = 0
        f['mr_signal'] = 0
        f['concept_consensus'] = 0.5
        f['trend_consistency'] = 0.5
    
    if ss:
        f['weekly_vol'] = ss['weekly_volatility']
        f['d3_acc'] = ss['d3_direction_accuracy']
        f['concept_eff'] = ss['concept_effectiveness']
        f['mr_eff'] = ss['mr_effectiveness']
    else:
        f['weekly_vol'] = 2.0
        f['d3_acc'] = 0.5
        f['concept_eff'] = 0.5
        f['mr_eff'] = 0.5
    
    # 衍生特征
    f['d3_x_market'] = f['d3_chg'] * f['market_d3']
    f['d3_agree_market'] = 1 if (f['d3_chg'] >= 0) == (f['market_d3'] > 0) else 0
    f['d3_agree_concept'] = 1 if (f['d3_chg'] >= 0) == (f['composite_score'] > 0) else 0
    f['all_days_same_dir'] = 1 if (all(d > 0 for d in d3_daily) or all(d < 0 for d in d3_daily)) else 0
    f['d3_trend'] = d3_daily[2] - d3_daily[0] if len(d3_daily) >= 3 else 0
    
    features_list.append(f)
    labels.append(1 if w['weekly_up'] else 0)

# 计算每个特征与label的相关性
print("\n特征与周涨跌的相关性:")
feature_names = list(features_list[0].keys())
for fname in feature_names:
    vals = [f[fname] for f in features_list]
    # 计算点二列相关
    mean_v = _mean(vals)
    mean_l = _mean(labels)
    
    cov = _mean([v * l for v, l in zip(vals, labels)]) - mean_v * mean_l
    std_v = _std(vals)
    std_l = _std(labels)
    
    if std_v > 0 and std_l > 0:
        corr = cov / (std_v * std_l)
    else:
        corr = 0
    
    # 也计算该特征>0时的涨率
    pos_subset = [(f, l) for f, l in zip(features_list, labels) if f[fname] > 0]
    neg_subset = [(f, l) for f, l in zip(features_list, labels) if f[fname] <= 0]
    pos_rate = _mean([l for _, l in pos_subset]) * 100 if pos_subset else 0
    neg_rate = _mean([l for _, l in neg_subset]) * 100 if neg_subset else 0
    
    print(f"  {fname:25s}: corr={corr:+.3f}, >0涨率={pos_rate:.1f}%({len(pos_subset)}), ≤0涨率={neg_rate:.1f}%({len(neg_subset)})")

# ── 分析2: 简单决策树 ──
print("\n" + "="*80)
print("分析2: 手动决策树")
print("="*80)

# 最优单特征分割
for fname in ['d3_chg', 'abs_d3', 'composite_score', 'market_d3', 'd3_x_market']:
    best_acc = 0
    best_t = 0
    vals = [f[fname] for f in features_list]
    # 尝试不同阈值
    sorted_vals = sorted(set(round(v, 2) for v in vals))
    for t in sorted_vals[::max(1, len(sorted_vals)//50)]:
        correct = sum(1 for f, l in zip(features_list, labels) 
                      if (f[fname] >= t) == (l == 1))
        acc = correct / len(labels) * 100
        if acc > best_acc:
            best_acc = acc
            best_t = t
    print(f"  {fname}: 最优阈值={best_t:.2f}, 准确率={best_acc:.1f}%")

# ── 分析3: 两层决策树 ──
print("\n" + "="*80)
print("分析3: 两层决策树 (d3_chg + 第二特征)")
print("="*80)

# 第一层: d3_chg >= 0
# 第二层: 对d3>=0和d3<0分别找最优第二特征
for d3_dir in [True, False]:
    subset_idx = [i for i, f in enumerate(features_list) if (f['d3_chg'] >= 0) == d3_dir]
    subset_labels = [labels[i] for i in subset_idx]
    subset_features = [features_list[i] for i in subset_idx]
    
    base_correct = sum(1 for l in subset_labels if l == (1 if d3_dir else 0))
    base_acc = base_correct / len(subset_labels) * 100
    
    print(f"\n  d3{'>=0' if d3_dir else '<0'}: {len(subset_idx)}条, 基准(跟d3)={base_acc:.1f}%")
    
    for fname in ['composite_score', 'market_d3', 'fund_flow', 'mr_signal', 
                   'board_momentum', 'd3_acc', 'weekly_vol', 'd3_trend',
                   'concept_consensus', 'all_days_same_dir']:
        best_acc = base_acc
        best_t = None
        best_flip = False
        
        vals = [f[fname] for f in subset_features]
        sorted_vals = sorted(set(round(v, 2) for v in vals))
        
        for t in sorted_vals[::max(1, len(sorted_vals)//20)]:
            # 不翻转: 特征>=t → 跟d3, 特征<t → 反d3
            correct1 = sum(1 for f, l in zip(subset_features, subset_labels)
                          if ((f[fname] >= t) and l == (1 if d3_dir else 0)) or
                             ((f[fname] < t) and l == (0 if d3_dir else 1)))
            # 翻转: 特征>=t → 反d3, 特征<t → 跟d3
            correct2 = len(subset_labels) - correct1
            
            acc1 = correct1 / len(subset_labels) * 100
            acc2 = correct2 / len(subset_labels) * 100
            
            if acc1 > best_acc:
                best_acc = acc1
                best_t = t
                best_flip = False
            if acc2 > best_acc:
                best_acc = acc2
                best_t = t
                best_flip = True
        
        if best_t is not None and best_acc > base_acc + 0.1:
            print(f"    {fname}: 阈值={best_t:.2f}, 准确率={best_acc:.1f}% (+{best_acc-base_acc:.1f}%), flip={best_flip}")

# ── 分析4: 尝试简单逻辑回归（手动实现） ──
print("\n" + "="*80)
print("分析4: 简单逻辑回归")
print("="*80)

# 用d3_chg和market_d3做简单逻辑回归
# sigmoid(w1*d3 + w2*market + b) > 0.5 → UP
# 网格搜索
best_lr_acc = 0
best_lr_params = None

for w1 in [0.5, 1.0, 1.5, 2.0, 3.0, 5.0]:
    for w2 in [0, 0.1, 0.2, 0.5]:
        for b in [-0.2, -0.1, 0, 0.1, 0.2, 0.3]:
            correct = 0
            for f, l in zip(features_list, labels):
                score = w1 * f['d3_chg'] + w2 * f['market_d3'] + b
                pred = 1 if score >= 0 else 0
                if pred == l:
                    correct += 1
            acc = correct / len(labels) * 100
            if acc > best_lr_acc:
                best_lr_acc = acc
                best_lr_params = (w1, w2, b)

print(f"最优线性: {best_lr_acc:.1f}%, params={best_lr_params}")

# ── 分析5: 非线性 - d3_chg的绝对值作为置信度 ──
print("\n" + "="*80)
print("分析5: 非线性策略")
print("="*80)

# 策略: 当|d3|大时跟d3, 当|d3|小时用其他信号
# 关键: 找到在|d3|小时最有效的信号

fuzzy = [(f, l) for f, l in zip(features_list, labels) if f['abs_d3'] <= 0.8]
print(f"模糊区: {len(fuzzy)}条")

# 在模糊区测试每个特征
for fname in feature_names:
    if fname in ['d3_chg', 'abs_d3', 'd3_positive']:
        continue
    
    # 特征>0 → UP
    correct_pos = sum(1 for f, l in fuzzy if (f[fname] > 0) == (l == 1))
    # 特征>0 → DOWN (反向)
    correct_neg = len(fuzzy) - correct_pos
    
    best = max(correct_pos, correct_neg)
    direction = "正向" if correct_pos >= correct_neg else "反向"
    print(f"  {fname:25s}: {direction} {best/len(fuzzy)*100:.1f}%")

# ── 分析6: 模糊区最优组合 ──
print("\n" + "="*80)
print("分析6: 模糊区最优特征组合")
print("="*80)

# 在模糊区，用d3方向 + 最佳第二特征
# 从分析5中选择最好的特征
best_combo_acc = 0
best_combo = None

for fname2 in ['market_d3', 'mr_signal', 'fund_flow', 'board_momentum', 
               'composite_score', 'd3_trend', 'concept_consensus', 'd3_acc']:
    for w_d3 in [1, 2, 3]:
        for w_f2 in [-1, -0.5, 0, 0.5, 1]:
            for bias in [-0.5, -0.3, -0.1, 0, 0.1, 0.3, 0.5]:
                correct = 0
                for f, l in fuzzy:
                    score = w_d3 * f['d3_chg'] + w_f2 * f[fname2] + bias
                    pred = 1 if score >= 0 else 0
                    if pred == l:
                        correct += 1
                acc = correct / len(fuzzy) * 100
                if acc > best_combo_acc:
                    best_combo_acc = acc
                    best_combo = (fname2, w_d3, w_f2, bias)

print(f"模糊区最优组合: {best_combo_acc:.1f}%, {best_combo}")

# 计算总体准确率
non_fuzzy_correct = 0
for f, l in zip(features_list, labels):
    if f['abs_d3'] > 0.8:
        if (f['d3_chg'] >= 0) == (l == 1):
            non_fuzzy_correct += 1

fuzzy_correct_combo = 0
fname2, w_d3, w_f2, bias = best_combo
for f, l in fuzzy:
    score = w_d3 * f['d3_chg'] + w_f2 * f[fname2] + bias
    pred = 1 if score >= 0 else 0
    if pred == l:
        fuzzy_correct_combo += 1

total_combo = non_fuzzy_correct + fuzzy_correct_combo
print(f"总体: {total_combo}/{len(weekly)} = {total_combo/len(weekly)*100:.1f}%")

print("\n完成")
