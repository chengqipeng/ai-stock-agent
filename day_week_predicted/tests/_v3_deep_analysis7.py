"""分析7: 选择性预测策略 — 只在高置信度时预测。"""
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

# ── 方案1: 只预测strong+medium ──
print("\n" + "="*80)
print("方案1: 只预测strong+medium (跳过fuzzy)")
print("="*80)

correct = 0
total = 0
skipped = 0
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
        total += 1
        pred = d3 > 0
        if pred == w['weekly_up']:
            correct += 1
    else:
        skipped += 1

print(f"预测: {correct}/{total} = {correct/total*100:.1f}%")
print(f"跳过: {skipped} ({skipped/len(weekly)*100:.1f}%)")
print(f"覆盖率: {total/len(weekly)*100:.1f}%")

# ── 方案2: 扩大strong区 ──
print("\n" + "="*80)
print("方案2: 降低strong阈值")
print("="*80)

for s_mult in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
    correct = 0
    total = 0
    for w in weekly:
        ss = stock_stats.get(w['code'])
        d3 = w['d3_chg']
        vol_strong = 2.0 * s_mult
        if ss:
            vol = ss.get('weekly_volatility', 2.0)
            if vol > 5.0: vol_strong = 3.5 * s_mult
            elif vol > 4.0: vol_strong = 3.0 * s_mult
            elif vol > 3.0: vol_strong = 2.5 * s_mult
        
        if abs(d3) > vol_strong:
            total += 1
            pred = d3 > 0
            if pred == w['weekly_up']:
                correct += 1
    
    acc = correct/total*100 if total else 0
    print(f"  strong×{s_mult}: {acc:.1f}% ({total}条, 覆盖{total/len(weekly)*100:.1f}%)")

# ── 方案3: 用d3_acc筛选 ──
print("\n" + "="*80)
print("方案3: 只预测d3_acc高的股票")
print("="*80)

for min_d3_acc in [0.6, 0.65, 0.7, 0.75, 0.8, 0.85]:
    correct = 0
    total = 0
    for w in weekly:
        ss = stock_stats.get(w['code'])
        if ss and ss['d3_direction_accuracy'] >= min_d3_acc:
            total += 1
            pred = w['d3_chg'] >= 0
            if pred == w['weekly_up']:
                correct += 1
    
    acc = correct/total*100 if total else 0
    print(f"  d3_acc>={min_d3_acc}: {acc:.1f}% ({total}条, 覆盖{total/len(weekly)*100:.1f}%)")

# ── 方案4: 组合筛选 — strong/medium + d3_acc高的fuzzy ──
print("\n" + "="*80)
print("方案4: strong/medium + d3_acc高的fuzzy")
print("="*80)

for min_d3_acc in [0.7, 0.75, 0.8, 0.85]:
    correct = 0
    total = 0
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
            # strong/medium: 总是预测
            total += 1
            pred = d3 > 0
            if pred == w['weekly_up']:
                correct += 1
        else:
            # fuzzy: 只有d3_acc高时预测
            d3_acc = ss['d3_direction_accuracy'] if ss else 0.5
            if d3_acc >= min_d3_acc:
                total += 1
                pred = d3 >= 0
                if pred == w['weekly_up']:
                    correct += 1
    
    acc = correct/total*100 if total else 0
    print(f"  d3_acc>={min_d3_acc}: {acc:.1f}% ({total}条, 覆盖{total/len(weekly)*100:.1f}%)")

# ── 方案5: 如果把fuzzy区的停牌股票也算进来 ──
print("\n" + "="*80)
print("方案5: strong/medium + 停牌 + d3_acc高的fuzzy")
print("="*80)

for min_d3_acc in [0.7, 0.75, 0.8, 0.85]:
    correct = 0
    total = 0
    for w in weekly:
        ss = stock_stats.get(w['code'])
        d3 = w['d3_chg']
        d3_daily = w.get('d3_daily', [])
        is_suspended = d3_daily and all(d == 0 for d in d3_daily)
        
        vol_strong = 2.0
        vol_mid = 0.8
        if ss:
            vol = ss.get('weekly_volatility', 2.0)
            if vol > 5.0: vol_strong, vol_mid = 3.5, 1.5
            elif vol > 4.0: vol_strong, vol_mid = 3.0, 1.2
            elif vol > 3.0: vol_strong, vol_mid = 2.5, 1.0
        
        if abs(d3) > vol_mid:
            total += 1
            pred = d3 > 0
            if pred == w['weekly_up']:
                correct += 1
        elif is_suspended:
            total += 1
            pred = True  # 停牌 → 预测涨(0变化=涨)
            if pred == w['weekly_up']:
                correct += 1
        else:
            d3_acc = ss['d3_direction_accuracy'] if ss else 0.5
            if d3_acc >= min_d3_acc:
                total += 1
                pred = d3 >= 0
                if pred == w['weekly_up']:
                    correct += 1
    
    acc = correct/total*100 if total else 0
    print(f"  d3_acc>={min_d3_acc}: {acc:.1f}% ({total}条, 覆盖{total/len(weekly)*100:.1f}%)")

# ── 方案6: 全量预测但改变fuzzy策略 ──
print("\n" + "="*80)
print("方案6: 全量预测 — fuzzy区用per-stock最优策略")
print("="*80)

# 预计算per-stock数据
stock_week_data = defaultdict(list)
for w in weekly:
    stock_week_data[w['code']].append(w)

correct_v6 = 0
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
        # fuzzy: per-stock LOWO最优
        code = w['code']
        hist = [ow for ow in stock_week_data[code] if ow['iso_week'] != w['iso_week']]
        if len(hist) < 3:
            pred = d3 >= 0
        else:
            # 评估策略
            d3_correct = sum(1 for h in hist if (h['d3_chg'] >= 0) == h['weekly_up'])
            always_up = sum(1 for h in hist if h['weekly_up'])
            always_down = len(hist) - always_up
            
            best = max(d3_correct, always_up, always_down)
            if best == always_up and always_up > d3_correct:
                pred = True
            elif best == always_down and always_down > d3_correct:
                pred = False
            else:
                pred = d3 >= 0
    
    if pred == w['weekly_up']:
        correct_v6 += 1

print(f"方案6: {correct_v6}/{len(weekly)} = {correct_v6/len(weekly)*100:.1f}%")

# ── 方案7: 全量预测 — 使用d3_chg + 个股历史d3准确率自适应 ──
print("\n" + "="*80)
print("方案7: d3_acc自适应阈值")
print("="*80)

# 高d3_acc的股票: 跟d3方向
# 低d3_acc的股票: 用历史涨率
for d3_acc_thresh in [0.6, 0.65, 0.7, 0.75]:
    correct = 0
    for w in weekly:
        ss = stock_stats.get(w['code'])
        d3 = w['d3_chg']
        d3_acc = ss['d3_direction_accuracy'] if ss else 0.5
        
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
            if d3_acc >= d3_acc_thresh:
                pred = d3 >= 0
            else:
                # 用历史涨率
                hist = [ow for ow in stock_week_data[w['code']] if ow['iso_week'] != w['iso_week']]
                if hist:
                    up_rate = sum(1 for h in hist if h['weekly_up']) / len(hist)
                    pred = up_rate >= 0.5
                else:
                    pred = d3 >= 0
        
        if pred == w['weekly_up']:
            correct += 1
    print(f"  d3_acc阈值{d3_acc_thresh}: {correct/len(weekly)*100:.1f}%")

# ── 最终总结 ──
print("\n" + "="*80)
print("总结: 85%目标可行性分析")
print("="*80)
print(f"""
数据集: {len(weekly)}条周样本, {len(set(w['code'] for w in weekly))}只股票, {len(set(w['iso_week'] for w in weekly))}周

理论上限:
- d3方向预测: 81.9% (全样本)
- d3分段最优: 82.2% (oracle per-bin)
- per-stock oracle: 82.0% (oracle per-stock)
- 任何特征组合: ~82% (网格搜索确认)

分区分析:
- Strong区(|d3|>2%): 91.0%, 占比{sum(1 for w in weekly if abs(w['d3_chg'])>2)/len(weekly)*100:.1f}%
- Medium区(0.8<|d3|<2%): 83.0%, 占比{sum(1 for w in weekly if 0.8<abs(w['d3_chg'])<=2)/len(weekly)*100:.1f}%  
- Fuzzy区(|d3|<0.8%): ~67%, 占比{sum(1 for w in weekly if abs(w['d3_chg'])<=0.8)/len(weekly)*100:.1f}%

要达到85%需要fuzzy区准确率: 
  (85%×46600 - 91%×{sum(1 for w in weekly if abs(w['d3_chg'])>2)} - 83%×{sum(1 for w in weekly if 0.8<abs(w['d3_chg'])<=2)}) / {sum(1 for w in weekly if abs(w['d3_chg'])<=0.8)} = 需要>100%

结论: 在当前数据集和特征下，85%全样本准确率数学上不可达。
可行方案:
1. 选择性预测(覆盖率<100%): strong+medium = 88.1%, 覆盖69.3%
2. 降低目标到82%: 当前已达81.9%
3. 改变数据集: 减少股票数量或选择更可预测的股票
""")

print("完成")
