#!/usr/bin/env python3
"""
验证周预测优化：
1. 调用 _compute_weekly_predictions 函数验证输出
2. 对比 v19e 分析结果确认一致性
3. 使用LOWO交叉验证确认泛化准确率
"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from collections import defaultdict
from day_week_predicted.backtest.prediction_enhanced_backtest import _compute_weekly_predictions

# 加载已有回测结果
with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    bt_data = json.load(f)

details = bt_data['逐日详情']
print(f"日频样本数: {len(details)}")
print(f"日频宽松准确率: {bt_data['总体准确率(宽松)']}")

# 转换为 _compute_weekly_predictions 需要的格式
all_day_results = []
for d in details:
    actual_chg = float(d['实际涨跌'].replace('%', '').replace('+', ''))
    all_day_results.append({
        'stock_code': d['代码'],
        'stock_name': d['名称'],
        'sector': d['板块'],
        'score_date': d['评分日'],
        'actual_change_pct': actual_chg,
        'decision': {
            '融合信号': d['融合信号'],
            '技术信号': d['技术信号'],
            '同行信号': d['同行信号'],
            'RS信号': d['RS信号'],
            '美股隔夜': d.get('美股隔夜', 0),
        },
    })

print(f"\n{'='*60}")
print("调用 _compute_weekly_predictions ...")
print(f"{'='*60}")

result = _compute_weekly_predictions(all_day_results)

print(f"\n周样本数: {result['周样本数']}")
print(f"周数: {result['周数']}")

print(f"\n策略汇总:")
for name, rate in result['策略汇总'].items():
    print(f"  {name}: {rate}")

print(f"\n推荐策略:")
for k, v in result['推荐策略'].items():
    print(f"  {k}: {v}")

print(f"\n按板块:")
for sec, stats in result['按板块'].items():
    print(f"  {sec}: 周样本={stats['周样本数']}, "
          f"B:周一混合={stats['B:周一混合']}, "
          f"C:前3天方向={stats['C:前3天方向']}")

# ── 独立LOWO交叉验证 ──
print(f"\n{'='*60}")
print("独立LOWO交叉验证（确认泛化准确率）")
print(f"{'='*60}")

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

# LOWO
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

# 策略B2: 周一涨跌>-0.5
def pred_b2(r, tr):
    return r['mon_actual'] > -0.5

# 策略C: 前3天>0
def pred_c(r, tr):
    return r['d3_chg'] > 0

# 策略C2: 前3天>0.5
def pred_c2(r, tr):
    return r['d3_chg'] > 0.5

strategies = [
    ('B:周一混合(0.5)', pred_b),
    ('B2:周一涨跌>-0.5', pred_b2),
    ('C:前3天涨跌>0', pred_c),
    ('C2:前3天涨跌>0.5', pred_c2),
]

print(f"\n{'策略':<22} {'LOWO':>10} {'前→后':>10} {'滚动':>10}")
print("-" * 55)

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

    print(f"{name:<22} {ok_l/n_l*100:>8.1f}%  {ok_h/len(test)*100:>8.1f}%  {ok_r/n_r*100:>8.1f}%")

# 板块明细
print(f"\n按板块明细 (LOWO):")
sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
print(f"{'板块':<8} {'B:周一混合':>10} {'C:前3天':>10}")
print("-" * 32)
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
        print(f"{sector:<8} {ok_b/n*100:>9.1f}% {ok_c/n*100:>9.1f}%")

print(f"\n{'='*60}")
print("验证结论")
print(f"{'='*60}")
print(f"目标: 周预测泛化准确率 > 65%")
print(f"策略B(周一收盘后): LOWO={ok_l/n_l*100:.1f}% — 需确认上方数据")
print(f"策略C(周三收盘后): 预期77-81%")
print(f"\n✅ 优化已集成到 prediction_enhanced_backtest.py")
print(f"   函数: _compute_weekly_predictions()")
print(f"   输出: 回测结果JSON中的 '周预测分析' 字段")
