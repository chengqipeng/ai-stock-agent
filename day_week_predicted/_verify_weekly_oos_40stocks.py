#!/usr/bin/env python3
"""
样本外40只股票周预测验证：
1. 读取回测结果，提取周预测数据
2. 独立LOWO交叉验证确认泛化准确率
3. 与原始50只股票结果对比
"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from collections import defaultdict

# 加载OOS回测结果
with open('data_results/backtest_weekly_oos_40stocks_result.json') as f:
    bt_data = json.load(f)

details = bt_data['逐日详情']
print(f"{'='*70}")
print(f"样本外40只股票 周预测泛化验证")
print(f"{'='*70}")
print(f"日频样本数: {len(details)}")
print(f"日频宽松准确率: {bt_data['总体准确率(宽松)']}")

# 构建周数据
def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_dt'] = datetime.strptime(d['评分日'], '%Y-%m-%d')
    d['_iso_week'] = d['_dt'].isocalendar()[:2]

stock_week = defaultdict(list)
for d in details:
    stock_week[(d['代码'], d['_iso_week'])].append(d)

weekly = []
for (code, iw), days in stock_week.items():
    days.sort(key=lambda x: x['评分日'])
    if len(days) < 2:
        continue
    cum = 1.0
    for d in days:
        cum *= (1 + d['_actual'] / 100)
    wchg = (cum - 1) * 100
    weekly.append({
        'code': code, 'sector': days[0]['板块'], 'iw': iw,
        'wchg': wchg, 'wup': wchg >= 0,
        'mon_actual': days[0]['_actual'],
        'd3_chg': sum(d['_actual'] for d in days[:min(3, len(days))]),
        'mon_comb': days[0]['融合信号'],
        'n': len(days),
    })

sorted_weeks = sorted(set(r['iw'] for r in weekly))
nw = len(weekly)
mid = len(sorted_weeks) // 2
first_half = set(sorted_weeks[:mid])
second_half = set(sorted_weeks[mid:])
train = [r for r in weekly if r['iw'] in first_half]
test = [r for r in weekly if r['iw'] in second_half]

print(f"周样本数: {nw}")
print(f"周数: {len(sorted_weeks)}")
print(f"前半/后半: {len(train)}/{len(test)}")

# LOWO交叉验证
def lowo(records, pred_fn, sorted_wks):
    ok = 0
    n = 0
    for hw in sorted_wks:
        tr = [r for r in records if r['iw'] != hw]
        te = [r for r in records if r['iw'] == hw]
        if not te:
            continue
        for r in te:
            p = pred_fn(r, tr)
            if (p and r['wup']) or (not p and not r['wup']):
                ok += 1
            n += 1
    return ok, n

# 策略B: 周一混合(0.5)
def pred_b(r, tr):
    if r['mon_actual'] > 0.5:
        return True
    elif r['mon_actual'] < -0.5:
        return False
    sr = [t for t in tr if t['sector'] == r['sector']]
    up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
    return up_rate > 0.5

# 策略C: 前3天>0
def pred_c(r, tr):
    return r['d3_chg'] > 0

strategies = [
    ('B:周一混合(0.5)', pred_b),
    ('C:前3天涨跌>0', pred_c),
]

print(f"\n{'='*70}")
print(f"三重验证（LOWO / 前→后 / 滚动）")
print(f"{'='*70}")
print(f"{'策略':<22} {'LOWO':>10} {'前→后':>10} {'滚动':>10}")
print("-" * 55)

results = {}
for name, fn in strategies:
    # LOWO
    ok_l, n_l = lowo(weekly, fn, sorted_weeks)
    # 前→后
    ok_h = sum(1 for r in test if (fn(r, train) and r['wup']) or (not fn(r, train) and not r['wup']))
    # 滚动
    ok_r = 0
    n_r = 0
    for i in range(2, len(sorted_weeks)):
        tw = set(sorted_weeks[:i])
        pw = sorted_weeks[i]
        tr = [r for r in weekly if r['iw'] in tw]
        te = [r for r in weekly if r['iw'] == pw]
        for r in te:
            if (fn(r, tr) and r['wup']) or (not fn(r, tr) and not r['wup']):
                ok_r += 1
            n_r += 1

    lowo_pct = ok_l / n_l * 100
    fwd_pct = ok_h / len(test) * 100
    roll_pct = ok_r / n_r * 100
    results[name] = (lowo_pct, fwd_pct, roll_pct)
    print(f"{name:<22} {lowo_pct:>8.1f}%  {fwd_pct:>8.1f}%  {roll_pct:>8.1f}%")

# 按板块明细
sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
print(f"\n{'='*70}")
print(f"按板块明细 (LOWO)")
print(f"{'='*70}")
print(f"{'板块':<10} {'样本':>5} {'B:周一混合':>12} {'C:前3天':>12}")
print("-" * 42)
for sector in sectors:
    sr = [r for r in weekly if r['sector'] == sector]
    ok_b = 0
    ok_c = 0
    n = 0
    for hw in sorted_weeks:
        tr = [r for r in sr if r['iw'] != hw]
        te = [r for r in sr if r['iw'] == hw]
        if not te or len(tr) < 3:
            continue
        for r in te:
            if (pred_b(r, tr) and r['wup']) or (not pred_b(r, tr) and not r['wup']):
                ok_b += 1
            if (pred_c(r, tr) and r['wup']) or (not pred_c(r, tr) and not r['wup']):
                ok_c += 1
            n += 1
    if n > 0:
        print(f"{sector:<10} {n:>5} {ok_b/n*100:>10.1f}% {ok_c/n*100:>10.1f}%")

# 对比原始50只
print(f"\n{'='*70}")
print(f"与原始50只股票对比")
print(f"{'='*70}")
print(f"{'指标':<25} {'原始50只':>12} {'OOS 40只':>12} {'差异':>8}")
print("-" * 60)
orig_b = (68.0, 69.9, 67.7)
orig_c = (81.0, 77.3, 80.2)
oos_b = results['B:周一混合(0.5)']
oos_c = results['C:前3天涨跌>0']

for label, o, n in [
    ('B策略 LOWO', orig_b[0], oos_b[0]),
    ('B策略 前→后', orig_b[1], oos_b[1]),
    ('B策略 滚动', orig_b[2], oos_b[2]),
    ('C策略 LOWO', orig_c[0], oos_c[0]),
    ('C策略 前→后', orig_c[1], oos_c[1]),
    ('C策略 滚动', orig_c[2], oos_c[2]),
]:
    diff = n - o
    marker = '✅' if n >= 65 else '⚠️'
    print(f"{label:<25} {o:>10.1f}% {n:>10.1f}% {diff:>+6.1f}% {marker}")

# 最终结论
print(f"\n{'='*70}")
print(f"最终结论")
print(f"{'='*70}")
b_pass = all(v >= 65 for v in oos_b)
c_pass = all(v >= 65 for v in oos_c)
print(f"策略B (周一收盘后): {'✅ 全部≥65%' if b_pass else '⚠️ 部分未达标'}")
print(f"  LOWO={oos_b[0]:.1f}%, 前→后={oos_b[1]:.1f}%, 滚动={oos_b[2]:.1f}%")
print(f"策略C (周三收盘后): {'✅ 全部≥65%' if c_pass else '⚠️ 部分未达标'}")
print(f"  LOWO={oos_c[0]:.1f}%, 前→后={oos_c[1]:.1f}%, 滚动={oos_c[2]:.1f}%")
print(f"\n日频准确率: {bt_data['总体准确率(宽松)']} (原始50只: 59.1%)")
print(f"周预测在完全未见过的40只股票上验证{'通过' if b_pass and c_pass else '部分通过'}!")
