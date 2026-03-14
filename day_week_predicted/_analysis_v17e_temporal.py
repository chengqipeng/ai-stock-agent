#!/usr/bin/env python3
"""
v17e: 分析基准率的时间稳定性。
如果基准率在前半和后半不同，说明市场regime变化。
"""
import json
from collections import defaultdict
from datetime import datetime

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']
total = len(details)

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_ge0'] = d['_actual'] >= 0
    d['_le0'] = d['_actual'] <= 0

dates = sorted(set(d['评分日'] for d in details))
mid_date = dates[len(dates) // 2]

print(f"前半: {dates[0]} ~ {mid_date}")
print(f"后半: {mid_date} ~ {dates[-1]}")

train = [d for d in details if d['评分日'] <= mid_date]
test = [d for d in details if d['评分日'] > mid_date]

print(f"\n=== 基准率时间稳定性 ===")
for sec in sectors:
    t1 = [d for d in train if d['板块'] == sec]
    t2 = [d for d in test if d['板块'] == sec]
    
    ge0_1 = sum(1 for d in t1 if d['_ge0'])
    ge0_2 = sum(1 for d in t2 if d['_ge0'])
    
    r1 = ge0_1/len(t1)*100 if t1 else 0
    r2 = ge0_2/len(t2)*100 if t2 else 0
    
    shift = '↑' if r2 > r1 + 3 else ('↓' if r2 < r1 - 3 else '→')
    print(f"  {sec}: 前半>=0%={ge0_1}/{len(t1)}({r1:.1f}%) "
          f"后半>=0%={ge0_2}/{len(t2)}({r2:.1f}%) {shift}")

# 按周分析基准率变化
print(f"\n=== 按周分析基准率 ===")
from collections import OrderedDict

# 按周分组
week_data = defaultdict(lambda: defaultdict(lambda: {'ge0': 0, 'n': 0}))
for d in details:
    dt = datetime.strptime(d['评分日'], '%Y-%m-%d')
    week = dt.strftime('%Y-W%W')
    sec = d['板块']
    week_data[week][sec]['n'] += 1
    if d['_ge0']:
        week_data[week][sec]['ge0'] += 1

weeks = sorted(week_data.keys())
print(f"周数: {len(weeks)}")

for sec in sectors:
    rates = []
    for w in weeks:
        s = week_data[w][sec]
        if s['n'] > 0:
            rates.append(s['ge0']/s['n']*100)
    
    if rates:
        avg = sum(rates) / len(rates)
        std = (sum((r - avg)**2 for r in rates) / len(rates)) ** 0.5
        print(f"  {sec}: 周均>=0%率={avg:.1f}% ±{std:.1f}%")
        # 前半 vs 后半
        mid_w = len(rates) // 2
        avg1 = sum(rates[:mid_w]) / mid_w if mid_w > 0 else 0
        avg2 = sum(rates[mid_w:]) / (len(rates) - mid_w) if len(rates) > mid_w else 0
        print(f"    前半周均={avg1:.1f}% 后半周均={avg2:.1f}%")

# ═══════════════════════════════════════════════════════════
# 关键分析: 如果我们用"自适应基准率"（滚动20天基准率）
# ═══════════════════════════════════════════════════════════
print(f"\n=== 自适应基准率策略 ===")

for window in [10, 15, 20, 30]:
    ok = 0
    n_tested = 0
    
    for test_date in dates:
        past_dates = sorted([d2 for d2 in dates if d2 < test_date])[-window:]
        if len(past_dates) < 5:
            continue
        
        past_set = set(past_dates)
        test_data = [d for d in details if d['评分日'] == test_date]
        
        for sec in sectors:
            sec_past = [d for d in details if d['评分日'] in past_set and d['板块'] == sec]
            sec_test = [d for d in test_data if d['板块'] == sec]
            
            if not sec_test:
                continue
            
            if sec_past:
                ge0 = sum(1 for d in sec_past if d['_ge0'])
                rate = ge0 / len(sec_past)
            else:
                rate = 0.5
            
            pred = '上涨' if rate > 0.5 else '下跌'
            
            for d in sec_test:
                if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                    ok += 1
                n_tested += 1
    
    print(f"  window={window}: {ok}/{n_tested} ({ok/n_tested*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 自适应基准率 + z反转
# ═══════════════════════════════════════════════════════════
print(f"\n=== 自适应基准率 + z反转 ===")

for window in [15, 20, 30]:
    for z_t in [0.5, 0.8, 1.0]:
        ok = 0
        n_tested = 0
        
        for test_date in dates:
            past_dates = sorted([d2 for d2 in dates if d2 < test_date])[-window:]
            if len(past_dates) < 5:
                continue
            
            past_set = set(past_dates)
            test_data = [d for d in details if d['评分日'] == test_date]
            
            for sec in sectors:
                sec_past = [d for d in details if d['评分日'] in past_set and d['板块'] == sec]
                sec_test = [d for d in test_data if d['板块'] == sec]
                
                if not sec_test:
                    continue
                
                if sec_past:
                    ge0 = sum(1 for d in sec_past if d['_ge0'])
                    rate = ge0 / len(sec_past)
                else:
                    rate = 0.5
                
                for d in sec_test:
                    z = d.get('z_today', 0)
                    if z > z_t:
                        pred = '下跌'
                    elif z < -z_t:
                        pred = '上涨'
                    else:
                        pred = '上涨' if rate > 0.5 else '下跌'
                    
                    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                        ok += 1
                    n_tested += 1
        
        print(f"  window={window} z={z_t}: {ok}/{n_tested} ({ok/n_tested*100:.1f}%)")

print(f"\n分析完成")
