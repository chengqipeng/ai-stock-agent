#!/usr/bin/env python3
"""分析v16b回测结果，寻找提升到65%的机会。"""
import json
from collections import defaultdict

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json', 'r') as f:
    data = json.load(f)

details = data['逐日详情']
print(f"总样本: {len(details)}")

# 1. 按板块×星期 分析
print("\n=== 板块×星期 准确率 ===")
from datetime import datetime
sector_wd = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    sec = d['板块']
    wd = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
    wd_name = ['周一','周二','周三','周四','周五'][wd]
    key = f"{sec}_{wd_name}"
    sector_wd[key]['n'] += 1
    if d['宽松正确'] == '✓':
        sector_wd[key]['ok'] += 1

for sec in ['科技','有色金属','汽车','新能源','医药','化工','制造']:
    print(f"\n{sec}:")
    for wd_name in ['周一','周二','周三','周四','周五']:
        key = f"{sec}_{wd_name}"
        s = sector_wd[key]
        if s['n'] > 0:
            rate = s['ok']/s['n']*100
            marker = '✓' if rate >= 65 else ('~' if rate >= 60 else '✗')
            print(f"  {wd_name}: {s['ok']}/{s['n']} ({rate:.1f}%) {marker}")

# 2. 按板块×combined区间 分析
print("\n=== 板块×combined区间 准确率 ===")
sector_comb = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    sec = d['板块']
    comb = d['融合信号']
    if comb > 2.0:
        bucket = '>2.0'
    elif comb > 1.0:
        bucket = '1.0~2.0'
    elif comb > 0.5:
        bucket = '0.5~1.0'
    elif comb > 0.0:
        bucket = '0.0~0.5'
    elif comb > -0.5:
        bucket = '-0.5~0.0'
    elif comb > -1.0:
        bucket = '-1.0~-0.5'
    elif comb > -2.0:
        bucket = '-2.0~-1.0'
    else:
        bucket = '<-2.0'
    key = f"{sec}_{bucket}"
    sector_comb[key]['n'] += 1
    if d['宽松正确'] == '✓':
        sector_comb[key]['ok'] += 1

for sec in ['科技','有色金属','汽车','新能源','医药','化工','制造']:
    print(f"\n{sec}:")
    for bucket in ['>2.0','1.0~2.0','0.5~1.0','0.0~0.5','-0.5~0.0','-1.0~-0.5','-2.0~-1.0','<-2.0']:
        key = f"{sec}_{bucket}"
        s = sector_comb[key]
        if s['n'] > 0:
            rate = s['ok']/s['n']*100
            marker = '✓' if rate >= 65 else ('~' if rate >= 60 else '✗')
            print(f"  combined {bucket}: {s['ok']}/{s['n']} ({rate:.1f}%) {marker}")

# 3. 按板块×评分区间 分析
print("\n=== 板块×评分区间 准确率 ===")
sector_score = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    sec = d['板块']
    score = d['评分']
    if score >= 55:
        bucket = '≥55'
    elif score >= 50:
        bucket = '50-54'
    elif score >= 45:
        bucket = '45-49'
    elif score >= 40:
        bucket = '40-44'
    elif score >= 35:
        bucket = '35-39'
    else:
        bucket = '<35'
    key = f"{sec}_{bucket}"
    sector_score[key]['n'] += 1
    if d['宽松正确'] == '✓':
        sector_score[key]['ok'] += 1

for sec in ['科技','有色金属','汽车','新能源','医药','化工','制造']:
    print(f"\n{sec}:")
    for bucket in ['<35','35-39','40-44','45-49','50-54','≥55']:
        key = f"{sec}_{bucket}"
        s = sector_score[key]
        if s['n'] > 0:
            rate = s['ok']/s['n']*100
            marker = '✓' if rate >= 65 else ('~' if rate >= 60 else '✗')
            print(f"  评分{bucket}: {s['ok']}/{s['n']} ({rate:.1f}%) {marker}")

# 4. 按板块×z_today区间 分析
print("\n=== 板块×z_today区间 准确率 ===")
sector_z = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    sec = d['板块']
    z = d.get('z_today', 0)
    if z > 2.0:
        bucket = '>2.0'
    elif z > 1.0:
        bucket = '1.0~2.0'
    elif z > 0.5:
        bucket = '0.5~1.0'
    elif z > -0.5:
        bucket = '-0.5~0.5'
    elif z > -1.0:
        bucket = '-1.0~-0.5'
    elif z > -2.0:
        bucket = '-2.0~-1.0'
    else:
        bucket = '<-2.0'
    key = f"{sec}_{bucket}"
    sector_z[key]['n'] += 1
    if d['宽松正确'] == '✓':
        sector_z[key]['ok'] += 1

for sec in ['科技','有色金属','汽车','新能源','医药','化工','制造']:
    print(f"\n{sec}:")
    for bucket in ['<-2.0','-2.0~-1.0','-1.0~-0.5','-0.5~0.5','0.5~1.0','1.0~2.0','>2.0']:
        key = f"{sec}_{bucket}"
        s = sector_z[key]
        if s['n'] > 0:
            rate = s['ok']/s['n']*100
            marker = '✓' if rate >= 65 else ('~' if rate >= 60 else '✗')
            print(f"  z_today {bucket}: {s['ok']}/{s['n']} ({rate:.1f}%) {marker}")

# 5. 错误分析: 哪些预测方向错得最多
print("\n=== 错误分析: 预测上涨但实际下跌 ===")
wrong_up = defaultdict(lambda: {'n': 0, 'total_up': 0})
wrong_down = defaultdict(lambda: {'n': 0, 'total_down': 0})
for d in details:
    sec = d['板块']
    if d['预测方向'] == '上涨':
        wrong_up[sec]['total_up'] += 1
        if d['宽松正确'] == '✗':
            wrong_up[sec]['n'] += 1
    else:
        wrong_down[sec]['total_down'] += 1
        if d['宽松正确'] == '✗':
            wrong_down[sec]['n'] += 1

for sec in ['科技','有色金属','汽车','新能源','医药','化工','制造']:
    wu = wrong_up[sec]
    wd = wrong_down[sec]
    print(f"  {sec}: 预测上涨错误 {wu['n']}/{wu['total_up']} ({wu['n']/wu['total_up']*100:.1f}% 错误率), "
          f"预测下跌错误 {wd['n']}/{wd['total_down']} ({wd['n']/wd['total_down']*100:.1f}% 错误率)" if wu['total_up'] > 0 and wd['total_down'] > 0 else f"  {sec}: 数据不足")

# 6. 预测方向分布
print("\n=== 预测方向分布 ===")
for sec in ['科技','有色金属','汽车','新能源','医药','化工','制造']:
    up_n = sum(1 for d in details if d['板块'] == sec and d['预测方向'] == '上涨')
    down_n = sum(1 for d in details if d['板块'] == sec and d['预测方向'] == '下跌')
    total = up_n + down_n
    print(f"  {sec}: 上涨{up_n}({up_n/total*100:.0f}%) 下跌{down_n}({down_n/total*100:.0f}%)")

# 7. 实际涨跌分布（基准率）
print("\n=== 实际涨跌分布（宽松模式: >=0%=涨, <=0%=跌）===")
for sec in ['科技','有色金属','汽车','新能源','医药','化工','制造']:
    sec_data = [d for d in details if d['板块'] == sec]
    actual_up = sum(1 for d in sec_data if float(d['实际涨跌'].rstrip('%')) >= 0)
    total = len(sec_data)
    print(f"  {sec}: 实际>=0% {actual_up}/{total} ({actual_up/total*100:.1f}%)")
