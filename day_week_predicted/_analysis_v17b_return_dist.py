#!/usr/bin/env python3
"""
分析实际收益率分布，寻找宽松模式的优化空间。
关键洞察：宽松模式下 0% 对两个方向都算正确。
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

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])

# 1. 整体收益率分布
print(f"总样本: {total}")
ge0 = sum(1 for d in details if d['_actual'] >= 0)
le0 = sum(1 for d in details if d['_actual'] <= 0)
eq0 = sum(1 for d in details if d['_actual'] == 0)
gt0 = sum(1 for d in details if d['_actual'] > 0)
lt0 = sum(1 for d in details if d['_actual'] < 0)
print(f">=0%: {ge0} ({ge0/total*100:.1f}%)")
print(f"<=0%: {le0} ({le0/total*100:.1f}%)")
print(f"=0%: {eq0} ({eq0/total*100:.1f}%)")
print(f">0%: {gt0} ({gt0/total*100:.1f}%)")
print(f"<0%: {lt0} ({lt0/total*100:.1f}%)")
print(f"ge0+le0 = {ge0+le0} (>total因为0%双算)")

# 2. 按板块的收益率分布
sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
print(f"\n=== 板块收益率分布 ===")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    ge0 = sum(1 for d in sec_data if d['_actual'] >= 0)
    le0 = sum(1 for d in sec_data if d['_actual'] <= 0)
    eq0 = sum(1 for d in sec_data if d['_actual'] == 0)
    # 宽松模式最大可能正确率 = max(ge0, le0) / n
    # 但如果我们能完美预测，每个样本都选对方向，最大正确率 = (ge0 + le0) / n（因为0%双算）
    # 不对，每个样本只能选一个方向，所以最大 = sum(max(1 if ge0, 1 if le0) for each sample)
    # 对于 >0% 的样本，只有预测上涨才对
    # 对于 <0% 的样本，只有预测下跌才对
    # 对于 =0% 的样本，预测任何方向都对
    # 所以完美预测的正确率 = (gt0 + lt0 + eq0) / n = n / n = 100%
    # 但如果只能选一个方向（全部上涨或全部下跌），最大 = max(ge0, le0) / n
    
    # 关键：如果我们能识别出 =0% 的样本，无论预测什么都对
    # 但我们不能提前知道
    
    # 更有用的分析：收益率在不同区间的分布
    bins = [(-999, -3), (-3, -1), (-1, -0.3), (-0.3, 0), (0, 0.3), (0.3, 1), (1, 3), (3, 999)]
    print(f"\n{sec} (n={n}, >=0%={ge0}({ge0/n*100:.1f}%), <=0%={le0}({le0/n*100:.1f}%)):")
    for lo, hi in bins:
        cnt = sum(1 for d in sec_data if lo <= d['_actual'] < hi)
        if lo == -999:
            label = f"<{hi}%"
        elif hi == 999:
            label = f">{lo}%"
        else:
            label = f"{lo}%~{hi}%"
        print(f"  {label}: {cnt} ({cnt/n*100:.1f}%)")

# 3. 关键分析：当前v16b的错误分布
print(f"\n=== v16b错误分析 ===")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    
    # 预测上涨但实际<0%
    wrong_up = [d for d in sec_data if d['预测方向'] == '上涨' and d['_actual'] < 0]
    # 预测下跌但实际>0%
    wrong_down = [d for d in sec_data if d['预测方向'] == '下跌' and d['_actual'] > 0]
    # 正确的
    correct = [d for d in sec_data if d['宽松正确'] == '✓']
    
    pred_up = sum(1 for d in sec_data if d['预测方向'] == '上涨')
    pred_down = sum(1 for d in sec_data if d['预测方向'] == '下跌')
    
    print(f"\n{sec}: 预测上涨{pred_up} 预测下跌{pred_down}")
    print(f"  错误(预测涨实际跌): {len(wrong_up)} ({len(wrong_up)/pred_up*100:.1f}% of 涨预测)" if pred_up > 0 else "")
    print(f"  错误(预测跌实际涨): {len(wrong_down)} ({len(wrong_down)/pred_down*100:.1f}% of 跌预测)" if pred_down > 0 else "")
    
    # 错误样本的实际涨跌分布
    if wrong_up:
        avg_wrong_up = sum(d['_actual'] for d in wrong_up) / len(wrong_up)
        print(f"  预测涨错误的平均实际涨跌: {avg_wrong_up:.2f}%")
    if wrong_down:
        avg_wrong_down = sum(d['_actual'] for d in wrong_down) / len(wrong_down)
        print(f"  预测跌错误的平均实际涨跌: {avg_wrong_down:.2f}%")

# 4. 如果全部预测上涨 vs 全部预测下跌
print(f"\n=== 极端策略对比 ===")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    all_up = sum(1 for d in sec_data if d['_actual'] >= 0)
    all_down = sum(1 for d in sec_data if d['_actual'] <= 0)
    print(f"  {sec}: 全涨={all_up}/{n}({all_up/n*100:.1f}%) 全跌={all_down}/{n}({all_down/n*100:.1f}%)")

# 5. 分析：如果我们能完美区分"大涨"和"大跌"，但对小幅波动用基准率
print(f"\n=== 按涨跌幅度分析 ===")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    
    # 大涨(>1%): 预测上涨一定对
    big_up = sum(1 for d in sec_data if d['_actual'] > 1)
    # 大跌(<-1%): 预测下跌一定对
    big_down = sum(1 for d in sec_data if d['_actual'] < -1)
    # 小幅(-1%~1%): 用基准率
    small = [d for d in sec_data if -1 <= d['_actual'] <= 1]
    small_ge0 = sum(1 for d in small if d['_actual'] >= 0)
    small_le0 = sum(1 for d in small if d['_actual'] <= 0)
    
    # 理想情况：大涨预测涨，大跌预测跌，小幅用最优方向
    ideal = big_up + big_down + max(small_ge0, small_le0)
    print(f"  {sec}: 大涨{big_up} 大跌{big_down} 小幅{len(small)}(>=0:{small_ge0} <=0:{small_le0}) "
          f"理想={ideal}/{n}({ideal/n*100:.1f}%)")
