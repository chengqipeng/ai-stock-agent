"""深度分析2: 验证d3近零区域的异常高涨率，并探索更多优化策略。"""
import sys
sys.path.insert(0, '.')

from collections import defaultdict
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

print("加载数据...")
data = _preload_v3_data(all_stock_codes, START_DATE, END_DATE)

print("构建周数据...")
weekly = _build_weekly_records_v3(all_stock_codes, data, START_DATE, END_DATE, board_stock_map)
print(f"周样本: {len(weekly)}")

stock_stats = _compute_stock_stats_v3(weekly)

# ── 验证1: d3 [0, 0.1) 的87.7%涨率 ──
print("\n" + "="*80)
print("验证1: d3近零区域细分")
print("="*80)

# 更细的分箱
fine_bins = [(-0.8, -0.6), (-0.6, -0.4), (-0.4, -0.2), (-0.2, -0.1), 
             (-0.1, -0.05), (-0.05, 0.0), (0.0, 0.001), (0.001, 0.05),
             (0.05, 0.1), (0.1, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8)]

fuzzy_all = [w for w in weekly if abs(w['d3_chg']) <= 0.8]

for lo, hi in fine_bins:
    subset = [r for r in fuzzy_all if lo <= r['d3_chg'] < hi]
    if not subset: continue
    up_count = sum(1 for r in subset if r['weekly_up'])
    up_rate = up_count/len(subset)*100
    # 也看weekly_change的分布
    avg_wk = _mean([r['weekly_change'] for r in subset])
    print(f"  d3 [{lo:+.3f}, {hi:+.3f}): {len(subset):5d}条, 涨{up_rate:5.1f}%, 平均周涨{avg_wk:+.2f}%")

# ── 验证2: d3=0.0000 的情况 ──
print("\n" + "="*80)
print("验证2: d3精确为0的情况")
print("="*80)

d3_zero = [r for r in weekly if r['d3_chg'] == 0.0]
print(f"d3=0.0000: {len(d3_zero)}条")
if d3_zero:
    up = sum(1 for r in d3_zero if r['weekly_up'])
    print(f"  涨: {up}/{len(d3_zero)} = {up/len(d3_zero)*100:.1f}%")
    # 看weekly_change分布
    wk_chgs = [r['weekly_change'] for r in d3_zero]
    print(f"  周涨幅: min={min(wk_chgs):.2f}%, max={max(wk_chgs):.2f}%, avg={_mean(wk_chgs):.2f}%")
    # 看d3_daily
    for r in d3_zero[:5]:
        print(f"    {r['code']} {r['iso_week']} d3_daily={r['d3_daily']} weekly={r['weekly_change']:.2f}%")

# ── 验证3: weekly_up的定义 ──
print("\n" + "="*80)
print("验证3: weekly_up定义检查")
print("="*80)

# weekly_up = weekly_chg >= 0, 所以weekly_chg=0也算涨
zero_weekly = [r for r in weekly if r['weekly_change'] == 0.0]
print(f"weekly_change=0: {len(zero_weekly)}条, weekly_up={sum(1 for r in zero_weekly if r['weekly_up'])}")

# d3_chg=0 但 weekly_change != 0 的情况
d3_near_zero = [r for r in weekly if abs(r['d3_chg']) < 0.001]
print(f"d3_chg近零(<0.001): {len(d3_near_zero)}条")
if d3_near_zero:
    up = sum(1 for r in d3_near_zero if r['weekly_up'])
    print(f"  涨: {up}/{len(d3_near_zero)} = {up/len(d3_near_zero)*100:.1f}%")

# ── 分析4: 如果在模糊区用"总是预测涨"策略 ──
print("\n" + "="*80)
print("分析4: 模糊区不同策略对比")
print("="*80)

# 当前策略(v3.12): d3>-0.3 → UP, d3<-0.3 → DOWN
current_correct = 0
always_up_correct = 0
d3_follow_correct = 0
optimal_correct = 0

for r in fuzzy_all:
    actual = r['weekly_up']
    # 当前
    if r['d3_chg'] > -0.3:
        if actual: current_correct += 1
    else:
        if not actual: current_correct += 1
    # 总是涨
    if actual: always_up_correct += 1
    # 跟d3
    if (r['d3_chg'] >= 0) == actual: d3_follow_correct += 1
    # 最优(oracle)
    optimal_correct += 1

print(f"模糊区总数: {len(fuzzy_all)}")
print(f"  当前(d3>-0.3→UP): {current_correct/len(fuzzy_all)*100:.1f}%")
print(f"  总是预测涨:       {always_up_correct/len(fuzzy_all)*100:.1f}%")
print(f"  跟随d3方向:       {d3_follow_correct/len(fuzzy_all)*100:.1f}%")

# ── 分析5: 模糊区按大盘+d3方向组合 ──
print("\n" + "="*80)
print("分析5: 模糊区按大盘方向+d3方向组合")
print("="*80)

combos = defaultdict(lambda: [0, 0])  # [up_count, total]
for r in fuzzy_all:
    mkt_up = r.get('market_d3_chg', 0) > 0
    d3_up = r['d3_chg'] >= 0
    key = f"大盘{'涨' if mkt_up else '跌'}+d3{'涨' if d3_up else '跌'}"
    combos[key][1] += 1
    if r['weekly_up']:
        combos[key][0] += 1

for key, (up, total) in sorted(combos.items()):
    print(f"  {key}: {total}条, 涨{up/total*100:.1f}%")

# ── 分析6: 模糊区按个股历史涨率 ──
print("\n" + "="*80)
print("分析6: 模糊区按个股历史涨率分组")
print("="*80)

# 计算每只股票的历史涨率
stock_up_rate = defaultdict(lambda: [0, 0])
for w in weekly:
    stock_up_rate[w['code']][1] += 1
    if w['weekly_up']:
        stock_up_rate[w['code']][0] += 1

up_rate_bins = [(0, 0.4), (0.4, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 1.01)]
for lo, hi in up_rate_bins:
    subset = []
    for r in fuzzy_all:
        ur = stock_up_rate[r['code']]
        rate = ur[0]/ur[1] if ur[1] > 0 else 0.5
        if lo <= rate < hi:
            subset.append(r)
    if not subset: continue
    actual_up = sum(1 for r in subset if r['weekly_up'])
    print(f"  涨率[{lo:.1f},{hi:.1f}): {len(subset)}条, 实际涨{actual_up/len(subset)*100:.1f}%")

# ── 分析7: 如果用个股d3_acc来决定模糊区策略 ──
print("\n" + "="*80)
print("分析7: 模糊区按d3_acc分组")
print("="*80)

d3_acc_bins = [(0, 0.55), (0.55, 0.65), (0.65, 0.75), (0.75, 0.85), (0.85, 1.01)]
for lo, hi in d3_acc_bins:
    subset = []
    for r in fuzzy_all:
        ss = stock_stats.get(r['code'])
        acc = ss['d3_direction_accuracy'] if ss else 0.5
        if lo <= acc < hi:
            subset.append(r)
    if not subset: continue
    actual_up = sum(1 for r in subset if r['weekly_up'])
    d3_correct = sum(1 for r in subset if (r['d3_chg'] >= 0) == r['weekly_up'])
    print(f"  d3_acc[{lo:.2f},{hi:.2f}): {len(subset)}条, 涨{actual_up/len(subset)*100:.1f}%, d3准确{d3_correct/len(subset)*100:.1f}%")

# ── 分析8: 模糊区 - 概念信号方向 ──
print("\n" + "="*80)
print("分析8: 模糊区概念信号方向")
print("="*80)

concept_up = [r for r in fuzzy_all if r['concept_signal'] and r['concept_signal']['composite_score'] > 0]
concept_down = [r for r in fuzzy_all if r['concept_signal'] and r['concept_signal']['composite_score'] < 0]
concept_neutral = [r for r in fuzzy_all if r['concept_signal'] and r['concept_signal']['composite_score'] == 0]

for label, subset in [('概念看涨', concept_up), ('概念看跌', concept_down), ('概念中性', concept_neutral)]:
    if not subset: continue
    actual_up = sum(1 for r in subset if r['weekly_up'])
    print(f"  {label}: {len(subset)}条, 实际涨{actual_up/len(subset)*100:.1f}%")

# ── 分析9: 模拟不同策略的总准确率 ──
print("\n" + "="*80)
print("分析9: 模拟不同策略的总准确率")
print("="*80)

# 基准: strong + medium 不变，只改fuzzy
strong_medium_correct = 0
strong_medium_total = 0
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
    
    if abs(d3) > vol_mid:  # strong or medium
        strong_medium_total += 1
        pred, _, _ = predict_weekly_direction_v3(d3, w['concept_signal'], ss, 
                                                  w.get('d3_daily'), w.get('market_d3_chg', 0.0))
        if pred == w['weekly_up']:
            strong_medium_correct += 1

print(f"Strong+Medium: {strong_medium_correct}/{strong_medium_total} = {strong_medium_correct/strong_medium_total*100:.1f}%")
print(f"Fuzzy: {len(fuzzy_all)}条")
print(f"总样本: {len(weekly)}")

# 需要fuzzy达到多少才能总体85%?
target_total = int(len(weekly) * 0.85)
needed_fuzzy = target_total - strong_medium_correct
print(f"需要总正确: {target_total}")
print(f"需要fuzzy正确: {needed_fuzzy}/{len(fuzzy_all)} = {needed_fuzzy/len(fuzzy_all)*100:.1f}%")

# 策略A: 总是预测涨
always_up = sum(1 for r in fuzzy_all if r['weekly_up'])
total_a = strong_medium_correct + always_up
print(f"\n策略A(fuzzy总是涨): fuzzy {always_up/len(fuzzy_all)*100:.1f}%, 总体 {total_a/len(weekly)*100:.1f}%")

# 策略B: d3>=0 → UP, d3<0 → 看大盘
strat_b_correct = 0
for r in fuzzy_all:
    if r['d3_chg'] >= 0:
        if r['weekly_up']: strat_b_correct += 1
    else:
        mkt_up = r.get('market_d3_chg', 0) > 0
        if mkt_up:
            if r['weekly_up']: strat_b_correct += 1
        else:
            if not r['weekly_up']: strat_b_correct += 1
total_b = strong_medium_correct + strat_b_correct
print(f"策略B(d3>=0→UP, d3<0看大盘): fuzzy {strat_b_correct/len(fuzzy_all)*100:.1f}%, 总体 {total_b/len(weekly)*100:.1f}%")

# 策略C: d3>=0 → UP, d3<0 且 d3_acc高 → DOWN, 否则 → UP
strat_c_correct = 0
for r in fuzzy_all:
    ss = stock_stats.get(r['code'])
    d3_acc = ss['d3_direction_accuracy'] if ss else 0.5
    if r['d3_chg'] >= 0:
        if r['weekly_up']: strat_c_correct += 1
    elif d3_acc >= 0.75:
        if not r['weekly_up']: strat_c_correct += 1
    else:
        if r['weekly_up']: strat_c_correct += 1
total_c = strong_medium_correct + strat_c_correct
print(f"策略C(d3>=0→UP, d3<0+高d3acc→DOWN, 否则UP): fuzzy {strat_c_correct/len(fuzzy_all)*100:.1f}%, 总体 {total_c/len(weekly)*100:.1f}%")

# 策略D: 总是预测涨（包括d3<0）
strat_d_correct = sum(1 for r in fuzzy_all if r['weekly_up'])
total_d = strong_medium_correct + strat_d_correct
print(f"策略D(fuzzy全部UP): fuzzy {strat_d_correct/len(fuzzy_all)*100:.1f}%, 总体 {total_d/len(weekly)*100:.1f}%")

# 策略E: d3方向 + 大盘方向投票
strat_e_correct = 0
for r in fuzzy_all:
    votes_up = 0
    if r['d3_chg'] >= 0: votes_up += 1
    if r.get('market_d3_chg', 0) > 0: votes_up += 1
    pred_up = votes_up >= 1  # 至少1票涨就预测涨
    if pred_up == r['weekly_up']: strat_e_correct += 1
total_e = strong_medium_correct + strat_e_correct
print(f"策略E(d3+大盘投票≥1→UP): fuzzy {strat_e_correct/len(fuzzy_all)*100:.1f}%, 总体 {total_e/len(weekly)*100:.1f}%")

# 策略F: 按d3细分最优
strat_f_correct = 0
for r in fuzzy_all:
    d3 = r['d3_chg']
    if d3 >= 0:
        # d3>=0 总是预测涨
        if r['weekly_up']: strat_f_correct += 1
    elif d3 >= -0.5:
        # d3 [-0.5, 0): 涨率约42%, 预测跌
        if not r['weekly_up']: strat_f_correct += 1
    else:
        # d3 < -0.5: 涨率33%, 预测跌
        if not r['weekly_up']: strat_f_correct += 1
total_f = strong_medium_correct + strat_f_correct
print(f"策略F(d3>=0→UP, d3<0→DOWN): fuzzy {strat_f_correct/len(fuzzy_all)*100:.1f}%, 总体 {total_f/len(weekly)*100:.1f}%")

# 策略G: 扩大medium区（降低vol_mid到0.5）
print("\n" + "="*80)
print("分析10: 扩大medium区的效果")
print("="*80)

for new_mid in [0.3, 0.4, 0.5, 0.6, 0.7]:
    correct_g = 0
    for w in weekly:
        ss = stock_stats.get(w['code'])
        d3 = w['d3_chg']
        vol_strong = 2.0
        vol_mid_g = new_mid
        if ss:
            vol = ss.get('weekly_volatility', 2.0)
            if vol > 5.0: vol_strong = 3.5; vol_mid_g = new_mid * 1.875
            elif vol > 4.0: vol_strong = 3.0; vol_mid_g = new_mid * 1.5
            elif vol > 3.0: vol_strong = 2.5; vol_mid_g = new_mid * 1.25
        
        if abs(d3) > vol_strong:
            pred = d3 > 0
        elif abs(d3) > vol_mid_g:
            pred = d3 > 0
        else:
            # fuzzy: 跟d3方向
            pred = d3 >= 0
        
        if pred == w['weekly_up']:
            correct_g += 1
    print(f"  vol_mid={new_mid}: {correct_g/len(weekly)*100:.1f}%")

# 策略H: medium区3天方向一致时提升到high
print("\n" + "="*80)
print("分析11: medium区3天一致性提升")
print("="*80)

correct_h = 0
for w in weekly:
    ss = stock_stats.get(w['code'])
    d3 = w['d3_chg']
    sig = w['concept_signal']
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
        # 3天一致性检查
        d3_daily = w.get('d3_daily', [])
        if d3_daily and len(d3_daily) >= 3:
            all_same = all(d > 0 for d in d3_daily) or all(d < 0 for d in d3_daily)
            if all_same:
                pass  # 已经跟d3方向，准确率更高
    else:
        pred = d3 >= 0
    
    if pred == w['weekly_up']:
        correct_h += 1
print(f"  策略H(fuzzy跟d3): {correct_h/len(weekly)*100:.1f}%")

# ── 最终: 尝试组合最优策略 ──
print("\n" + "="*80)
print("最终: 组合策略模拟")
print("="*80)

# 策略X: strong跟d3, medium跟d3, fuzzy d3>=0→UP d3<0→DOWN
correct_x = 0
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
    
    pred = d3 >= 0  # 所有区间都跟d3方向
    if pred == w['weekly_up']:
        correct_x += 1
print(f"  策略X(全部跟d3): {correct_x/len(weekly)*100:.1f}%")

# 策略Y: strong跟d3, medium跟d3, fuzzy全部UP
correct_y = 0
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
        pred = True  # fuzzy全部UP
    
    if pred == w['weekly_up']:
        correct_y += 1
print(f"  策略Y(strong/medium跟d3, fuzzy全UP): {correct_y/len(weekly)*100:.1f}%")

# 策略Z: 降低strong阈值到1.5
correct_z = 0
for w in weekly:
    ss = stock_stats.get(w['code'])
    d3 = w['d3_chg']
    vol_strong = 1.5
    vol_mid = 0.5
    if ss:
        vol = ss.get('weekly_volatility', 2.0)
        if vol > 5.0: vol_strong, vol_mid = 3.0, 1.0
        elif vol > 4.0: vol_strong, vol_mid = 2.5, 0.8
        elif vol > 3.0: vol_strong, vol_mid = 2.0, 0.7
    
    if abs(d3) > vol_strong:
        pred = d3 > 0
    elif abs(d3) > vol_mid:
        pred = d3 > 0
    else:
        pred = d3 >= 0
    
    if pred == w['weekly_up']:
        correct_z += 1
print(f"  策略Z(lower thresholds 1.5/0.5): {correct_z/len(weekly)*100:.1f}%")

print("\n完成")
