#!/usr/bin/env python3
"""v12b: 找出每个板块中最可优化的子群体"""
import json
from collections import defaultdict
from datetime import datetime

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']

# 对每个板块，分析 combined方向 vs 实际方向
print("=== 板块 + combined方向 + 预测方向 ===")
for sector in ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']:
    sec_data = [d for d in details if d['板块'] == sector]
    if not sec_data:
        continue
    
    ok = sum(1 for d in sec_data if d['宽松正确'] == '✓')
    print(f"\n{sector}: {ok}/{len(sec_data)} ({ok/len(sec_data)*100:.1f}%)")
    
    # 按 combined方向 分组
    for cdir in ['combined>0', 'combined<0']:
        if cdir == 'combined>0':
            group = [d for d in sec_data if d['融合信号'] > 0]
        else:
            group = [d for d in sec_data if d['融合信号'] <= 0]
        
        if not group:
            continue
        
        g_ok = sum(1 for d in group if d['宽松正确'] == '✓')
        
        # 如果全部预测上涨
        all_up_ok = sum(1 for d in group if float(d['实际涨跌'].replace('%','').replace('+','')) >= 0)
        # 如果全部预测下跌
        all_down_ok = sum(1 for d in group if float(d['实际涨跌'].replace('%','').replace('+','')) <= 0)
        
        print(f"  {cdir}: {g_ok}/{len(group)} ({g_ok/len(group)*100:.1f}%) "
              f"| 全涨={all_up_ok}/{len(group)} ({all_up_ok/len(group)*100:.1f}%) "
              f"| 全跌={all_down_ok}/{len(group)} ({all_down_ok/len(group)*100:.1f}%)")

# 分析: 如果对每个板块都用最优的全涨/全跌策略
print("\n\n=== 板块全涨/全跌最优策略 ===")
total_optimal = 0
total_n = 0
for sector in ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']:
    sec_data = [d for d in details if d['板块'] == sector]
    if not sec_data:
        continue
    
    all_up = sum(1 for d in sec_data if float(d['实际涨跌'].replace('%','').replace('+','')) >= 0)
    all_down = sum(1 for d in sec_data if float(d['实际涨跌'].replace('%','').replace('+','')) <= 0)
    best = max(all_up, all_down)
    best_dir = '全涨' if all_up >= all_down else '全跌'
    total_optimal += best
    total_n += len(sec_data)
    print(f"  {sector}: {best_dir} {best}/{len(sec_data)} ({best/len(sec_data)*100:.1f}%)")

print(f"  总计: {total_optimal}/{total_n} ({total_optimal/total_n*100:.1f}%)")

# 分析: 每个板块中，哪些个股最差？
print("\n\n=== 个股准确率排名（最差的前15只）===")
stock_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'sector': ''})
for d in details:
    key = d['代码']
    stock_stats[key]['n'] += 1
    stock_stats[key]['sector'] = d['板块']
    stock_stats[key]['name'] = d['名称']
    if d['宽松正确'] == '✓':
        stock_stats[key]['ok'] += 1

sorted_stocks = sorted(stock_stats.items(), key=lambda x: x[1]['ok']/max(x[1]['n'],1))
for code, stats in sorted_stocks[:15]:
    rate = stats['ok'] / stats['n'] * 100
    print(f"  {stats['name']:8s}({code})[{stats['sector']}]: {stats['ok']}/{stats['n']} ({rate:.1f}%)")

# 分析: 如果对最差的个股全部翻转预测
print("\n\n=== 如果对最差个股(准确率<50%)全部翻转 ===")
flip_gain = 0
for code, stats in sorted_stocks:
    rate = stats['ok'] / stats['n'] * 100
    if rate < 50:
        # 翻转后的准确率
        stock_data = [d for d in details if d['代码'] == code]
        flip_ok = 0
        for d in stock_data:
            actual_chg = float(d['实际涨跌'].replace('%','').replace('+',''))
            if d['预测方向'] == '上涨':
                if actual_chg <= 0:
                    flip_ok += 1
            else:
                if actual_chg >= 0:
                    flip_ok += 1
        gain = flip_ok - stats['ok']
        flip_gain += gain
        print(f"  {stats['name']:8s}: 当前{stats['ok']}/{stats['n']} ({rate:.1f}%) → 翻转后{flip_ok}/{stats['n']} ({flip_ok/stats['n']*100:.1f}%) 净增{gain}")

print(f"  总净增: {flip_gain}")

# 分析: 新能源板块详细（58.8%，最差的大板块之一）
print("\n\n=== 新能源板块个股详情 ===")
for code, stats in sorted(stock_stats.items(), key=lambda x: x[1]['ok']/max(x[1]['n'],1)):
    if stats['sector'] == '新能源':
        rate = stats['ok'] / stats['n'] * 100
        # 全涨/全跌
        stock_data = [d for d in details if d['代码'] == code]
        all_up = sum(1 for d in stock_data if float(d['实际涨跌'].replace('%','').replace('+','')) >= 0)
        all_down = sum(1 for d in stock_data if float(d['实际涨跌'].replace('%','').replace('+','')) <= 0)
        print(f"  {stats['name']:8s}: 当前{stats['ok']}/{stats['n']} ({rate:.1f}%) | 全涨={all_up} ({all_up/stats['n']*100:.1f}%) | 全跌={all_down} ({all_down/stats['n']*100:.1f}%)")
