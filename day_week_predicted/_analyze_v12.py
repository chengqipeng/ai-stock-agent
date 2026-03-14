#!/usr/bin/env python3
"""v12深度分析：找出可优化的样本群体"""
import json
from collections import defaultdict
from datetime import datetime

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']
total = len(details)
loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"当前基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print()

# 分析1: 错误预测的分布
wrong = [d for d in details if d['宽松正确'] == '✗']
print(f"错误预测: {len(wrong)}个")

# 按板块+置信度+预测方向分组错误
wrong_groups = defaultdict(list)
for d in wrong:
    key = (d['板块'], d['置信度'], d['预测方向'])
    wrong_groups[key].append(d)

print("\n错误预测分组 (板块, 置信度, 预测方向) → 数量:")
for key, items in sorted(wrong_groups.items(), key=lambda x: -len(x[1])):
    sec, conf, pred = key
    # 如果翻转这些预测，有多少会变正确？
    flip_correct = 0
    for d in items:
        actual_chg = float(d['实际涨跌'].replace('%', '').replace('+', ''))
        if pred == '上涨':
            # 翻转为下跌，actual<=0即正确
            if actual_chg <= 0:
                flip_correct += 1
        else:
            # 翻转为上涨，actual>=0即正确
            if actual_chg >= 0:
                flip_correct += 1
    if len(items) >= 10:
        print(f"  {sec:6s} {conf:6s} 预测{pred}: {len(items):3d}个错误, 翻转后{flip_correct}个正确 (净增{flip_correct - (len(items)-flip_correct)})")

# 分析2: 正确预测中，翻转会变错的
correct = [d for d in details if d['宽松正确'] == '✓']
correct_groups = defaultdict(list)
for d in correct:
    key = (d['板块'], d['置信度'], d['预测方向'])
    correct_groups[key].append(d)

print(f"\n\n=== 分析2: 每个(板块,置信度,方向)组合的准确率 ===")
all_groups = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    key = (d['板块'], d['置信度'], d['预测方向'])
    all_groups[key]['n'] += 1
    if d['宽松正确'] == '✓':
        all_groups[key]['ok'] += 1

for key, stats in sorted(all_groups.items(), key=lambda x: x[1]['ok']/max(x[1]['n'],1)):
    sec, conf, pred = key
    rate = stats['ok'] / stats['n'] * 100
    n = stats['n']
    if n >= 10:
        marker = "★★★" if rate < 45 else ("★★" if rate < 50 else ("★" if rate < 55 else ""))
        print(f"  {sec:6s} {conf:6s} 预测{pred}: {stats['ok']:3d}/{n:3d} ({rate:.1f}%) {marker}")

# 分析3: 星期效应详细分析
print(f"\n\n=== 分析3: 星期效应 (评分日→预测日) ===")
weekday_stats = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    try:
        wd = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
        weekday_names = {0: '周一→周二', 1: '周二→周三', 2: '周三→周四', 3: '周四→周五', 4: '周五→周一'}
        key = weekday_names.get(wd, f'wd{wd}')
        weekday_stats[key]['n'] += 1
        if d['宽松正确'] == '✓':
            weekday_stats[key]['ok'] += 1
    except:
        pass

for key, stats in sorted(weekday_stats.items()):
    rate = stats['ok'] / stats['n'] * 100
    print(f"  {key}: {stats['ok']}/{stats['n']} ({rate:.1f}%)")

# 分析3b: 星期+板块交叉
print(f"\n=== 星期+板块交叉 (只显示<55%或>62%的) ===")
wd_sec_stats = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    try:
        wd = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
        weekday_names = {0: '周一', 1: '周二', 2: '周三', 3: '周四', 4: '周五'}
        key = (weekday_names.get(wd, f'wd{wd}'), d['板块'])
        wd_sec_stats[key]['n'] += 1
        if d['宽松正确'] == '✓':
            wd_sec_stats[key]['ok'] += 1
    except:
        pass

for key, stats in sorted(wd_sec_stats.items(), key=lambda x: x[1]['ok']/max(x[1]['n'],1)):
    rate = stats['ok'] / stats['n'] * 100
    if stats['n'] >= 5 and (rate < 55 or rate > 62):
        print(f"  {key[0]} {key[1]:6s}: {stats['ok']}/{stats['n']} ({rate:.1f}%)")

# 分析4: combined信号强度 vs 准确率
print(f"\n\n=== 分析4: combined信号强度 vs 准确率 ===")
combined_bins = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    c = abs(d['融合信号'])
    if c > 3.0:
        b = '>3.0'
    elif c > 2.0:
        b = '2.0-3.0'
    elif c > 1.5:
        b = '1.5-2.0'
    elif c > 1.0:
        b = '1.0-1.5'
    elif c > 0.5:
        b = '0.5-1.0'
    elif c > 0.2:
        b = '0.2-0.5'
    else:
        b = '<0.2'
    combined_bins[b]['n'] += 1
    if d['宽松正确'] == '✓':
        combined_bins[b]['ok'] += 1

for b in ['<0.2', '0.2-0.5', '0.5-1.0', '1.0-1.5', '1.5-2.0', '2.0-3.0', '>3.0']:
    stats = combined_bins.get(b, {'ok': 0, 'n': 0})
    if stats['n'] > 0:
        rate = stats['ok'] / stats['n'] * 100
        print(f"  |combined| {b:8s}: {stats['ok']:3d}/{stats['n']:3d} ({rate:.1f}%)")

# 分析5: 预测上涨但实际大跌 / 预测下跌但实际大涨 的特征
print(f"\n\n=== 分析5: 大错误样本特征 ===")
big_wrong_up = [d for d in wrong if d['预测方向'] == '上涨' and float(d['实际涨跌'].replace('%','').replace('+','')) < -1.0]
big_wrong_down = [d for d in wrong if d['预测方向'] == '下跌' and float(d['实际涨跌'].replace('%','').replace('+','')) > 1.0]
print(f"  预测上涨但实际跌>1%: {len(big_wrong_up)}个")
print(f"  预测下跌但实际涨>1%: {len(big_wrong_down)}个")

# 分析6: 评分区间 + 预测方向 交叉
print(f"\n\n=== 分析6: 评分区间 + 预测方向 ===")
score_dir_stats = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    s = d['评分']
    if s >= 60:
        b = '≥60'
    elif s >= 55:
        b = '55-59'
    elif s >= 50:
        b = '50-54'
    elif s >= 45:
        b = '45-49'
    elif s >= 40:
        b = '40-44'
    else:
        b = '<40'
    key = (b, d['预测方向'])
    score_dir_stats[key]['n'] += 1
    if d['宽松正确'] == '✓':
        score_dir_stats[key]['ok'] += 1

for key, stats in sorted(score_dir_stats.items()):
    rate = stats['ok'] / stats['n'] * 100 if stats['n'] > 0 else 0
    marker = "★" if rate < 55 and stats['n'] >= 20 else ""
    print(f"  评分{key[0]:5s} 预测{key[1]}: {stats['ok']:3d}/{stats['n']:3d} ({rate:.1f}%) {marker}")
