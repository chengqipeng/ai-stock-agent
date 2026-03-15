"""分析8: 最终策略 — 置信度分层预测。"""
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

# 预计算per-stock数据
stock_week_data = defaultdict(list)
for w in weekly:
    stock_week_data[w['code']].append(w)

# 预计算LOWO d3_acc
stock_lowo_d3_acc = {}
for code, weeks_data in stock_week_data.items():
    for w in weeks_data:
        hist = [ow for ow in weeks_data if ow['iso_week'] != w['iso_week']]
        if len(hist) < 3:
            stock_lowo_d3_acc[(code, w['iso_week'])] = 0.5
            continue
        d3_correct = 0
        d3_total = 0
        for h in hist:
            if abs(h['d3_chg']) > 0.1:
                d3_total += 1
                if (h['d3_chg'] > 0) == h['weekly_up']:
                    d3_correct += 1
        acc = d3_correct / d3_total if d3_total >= 3 else 0.5
        stock_lowo_d3_acc[(code, w['iso_week'])] = acc

# ── 策略A: 分层预测 + 全覆盖 ──
print("\n" + "="*80)
print("策略A: 分层预测 + 全覆盖")
print("="*80)

# strong/medium: 跟d3 (88.1%)
# fuzzy + d3_acc>=0.8: 跟d3 
# fuzzy + d3_acc<0.8 + 停牌: 预测涨
# fuzzy + d3_acc<0.8 + 非停牌: 用per-stock历史涨率

for d3_acc_thresh in [0.7, 0.75, 0.8, 0.85]:
    correct = 0
    zone_stats = defaultdict(lambda: [0, 0])
    
    for w in weekly:
        ss = stock_stats.get(w['code'])
        d3 = w['d3_chg']
        d3_daily = w.get('d3_daily', [])
        is_suspended = d3_daily and all(d == 0 for d in d3_daily)
        lowo_d3_acc = stock_lowo_d3_acc.get((w['code'], w['iso_week']), 0.5)
        
        vol_strong = 2.0
        vol_mid = 0.8
        if ss:
            vol = ss.get('weekly_volatility', 2.0)
            if vol > 5.0: vol_strong, vol_mid = 3.5, 1.5
            elif vol > 4.0: vol_strong, vol_mid = 3.0, 1.2
            elif vol > 3.0: vol_strong, vol_mid = 2.5, 1.0
        
        if abs(d3) > vol_strong:
            pred = d3 > 0
            zone = 'strong'
        elif abs(d3) > vol_mid:
            pred = d3 > 0
            zone = 'medium'
        elif is_suspended:
            pred = True  # 停牌 → 涨(0=涨)
            zone = 'suspended'
        elif lowo_d3_acc >= d3_acc_thresh:
            pred = d3 >= 0
            zone = 'fuzzy_high_acc'
        else:
            # 低d3_acc的fuzzy: 用历史涨率
            hist = [ow for ow in stock_week_data[w['code']] if ow['iso_week'] != w['iso_week']]
            if hist:
                up_rate = sum(1 for h in hist if h['weekly_up']) / len(hist)
                pred = up_rate >= 0.5
            else:
                pred = d3 >= 0
            zone = 'fuzzy_low_acc'
        
        zone_stats[zone][1] += 1
        if pred == w['weekly_up']:
            correct += 1
            zone_stats[zone][0] += 1
    
    acc = correct / len(weekly) * 100
    print(f"\n  d3_acc阈值={d3_acc_thresh}: 总体 {acc:.1f}%")
    for zone, (c, t) in sorted(zone_stats.items()):
        print(f"    {zone}: {c}/{t} = {c/t*100:.1f}%")

# ── 策略B: LOWO验证策略A ──
print("\n" + "="*80)
print("策略B: LOWO验证 (d3_acc=0.8)")
print("="*80)

all_weeks = sorted(set(w['iso_week'] for w in weekly))
week_accs = []
total_correct = 0
total_count = 0

for held_week in all_weeks:
    correct = 0
    count = 0
    
    # 重新计算排除held_week的stock_stats
    train_weekly = [w for w in weekly if w['iso_week'] != held_week]
    train_stock_stats = _compute_stock_stats_v3(weekly, exclude_week=held_week)
    
    # 重新计算LOWO d3_acc (排除held_week)
    for w in weekly:
        if w['iso_week'] != held_week:
            continue
        
        code = w['code']
        d3 = w['d3_chg']
        d3_daily = w.get('d3_daily', [])
        is_suspended = d3_daily and all(d == 0 for d in d3_daily)
        
        # 计算排除当前周的d3_acc
        hist = [ow for ow in stock_week_data[code] if ow['iso_week'] != held_week]
        d3_correct_h = 0
        d3_total_h = 0
        for h in hist:
            if abs(h['d3_chg']) > 0.1:
                d3_total_h += 1
                if (h['d3_chg'] > 0) == h['weekly_up']:
                    d3_correct_h += 1
        lowo_d3_acc = d3_correct_h / d3_total_h if d3_total_h >= 3 else 0.5
        
        ss = train_stock_stats.get(code)
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
        elif is_suspended:
            pred = True
        elif lowo_d3_acc >= 0.8:
            pred = d3 >= 0
        else:
            up_rate = sum(1 for h in hist if h['weekly_up']) / len(hist) if hist else 0.5
            pred = up_rate >= 0.5
        
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
for i, acc in enumerate(week_accs):
    print(f"  W{i+1:02d}: {acc:.1f}%")

# ── 策略C: 更激进 — 扩大medium区 + d3_acc筛选 ──
print("\n" + "="*80)
print("策略C: 扩大medium区 + d3_acc筛选")
print("="*80)

for vol_mid_mult in [0.5, 0.6, 0.7, 0.8]:
    for d3_acc_thresh in [0.75, 0.8, 0.85]:
        correct = 0
        for w in weekly:
            ss = stock_stats.get(w['code'])
            d3 = w['d3_chg']
            d3_daily = w.get('d3_daily', [])
            is_suspended = d3_daily and all(d == 0 for d in d3_daily)
            lowo_d3_acc = stock_lowo_d3_acc.get((w['code'], w['iso_week']), 0.5)
            
            vol_strong = 2.0
            vol_mid = 0.8 * vol_mid_mult
            if ss:
                vol = ss.get('weekly_volatility', 2.0)
                if vol > 5.0: vol_strong = 3.5; vol_mid = 1.5 * vol_mid_mult
                elif vol > 4.0: vol_strong = 3.0; vol_mid = 1.2 * vol_mid_mult
                elif vol > 3.0: vol_strong = 2.5; vol_mid = 1.0 * vol_mid_mult
            
            if abs(d3) > vol_strong:
                pred = d3 > 0
            elif abs(d3) > vol_mid:
                pred = d3 > 0
            elif is_suspended:
                pred = True
            elif lowo_d3_acc >= d3_acc_thresh:
                pred = d3 >= 0
            else:
                hist = [ow for ow in stock_week_data[w['code']] if ow['iso_week'] != w['iso_week']]
                if hist:
                    up_rate = sum(1 for h in hist if h['weekly_up']) / len(hist)
                    pred = up_rate >= 0.5
                else:
                    pred = d3 >= 0
            
            if pred == w['weekly_up']:
                correct += 1
        
        acc = correct / len(weekly) * 100
        if acc > 82.5:
            print(f"  vol_mid×{vol_mid_mult}, d3_acc>={d3_acc_thresh}: {acc:.1f}%")

print("\n完成")
