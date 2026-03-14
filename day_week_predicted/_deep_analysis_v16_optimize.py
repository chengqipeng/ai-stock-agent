#!/usr/bin/env python3
"""
v16 深度优化分析：基于2777样本逐条分析，找到最优决策边界

策略：不做复杂的sector×confidence×direction分层，
而是找到每个板块最简单有效的决策规则。

核心思路：
1. 对每个板块，分析 combined 信号值 vs 实际涨跌的最优阈值
2. 分析 z_today 反转信号的最优阈值
3. 分析评分区间的最优决策
4. 找到最少规则覆盖最多正确样本的方案
"""
import json
import logging
from collections import defaultdict
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']
total = len(details)


def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))


loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"{'=' * 80}")
print(f"当前v15基线: {loose_ok}/{total} ({loose_ok / total * 100:.1f}%)")
print(f"目标: {int(total * 0.65)}/{total} (65.0%)")
print(f"需要额外正确: {int(total * 0.65) - loose_ok}")
print(f"{'=' * 80}")

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']

# ═══════════════════════════════════════════════════════════
# 分析一：每个板块的 combined 信号分布 vs 实际涨跌
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"一、各板块 combined 信号值 vs 实际涨跌分布")
print(f"{'=' * 80}")

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    print(f"\n  {sector} ({len(sec_data)}样本):")
    
    # 按 combined 信号值分桶
    buckets = defaultdict(lambda: {'up': 0, 'down': 0, 'flat': 0, 'n': 0})
    for d in sec_data:
        combined = d.get('融合信号', 0)
        actual = parse_chg(d['实际涨跌'])
        
        # 分桶
        if combined > 2.0:
            b = '>2.0'
        elif combined > 1.0:
            b = '1.0~2.0'
        elif combined > 0.5:
            b = '0.5~1.0'
        elif combined > 0.0:
            b = '0.0~0.5'
        elif combined > -0.5:
            b = '-0.5~0.0'
        elif combined > -1.0:
            b = '-1.0~-0.5'
        elif combined > -2.0:
            b = '-2.0~-1.0'
        else:
            b = '<-2.0'
        
        buckets[b]['n'] += 1
        if actual > 0:
            buckets[b]['up'] += 1
        elif actual < 0:
            buckets[b]['down'] += 1
        else:
            buckets[b]['flat'] += 1
    
    for b in ['>2.0', '1.0~2.0', '0.5~1.0', '0.0~0.5', '-0.5~0.0', '-1.0~-0.5', '-2.0~-1.0', '<-2.0']:
        s = buckets[b]
        if s['n'] > 0:
            up_rate = s['up'] / s['n'] * 100
            ge0_rate = (s['up'] + s['flat']) / s['n'] * 100
            print(f"    combined {b:12s}: n={s['n']:4d} 涨={s['up']:3d}({up_rate:.1f}%) >=0={s['up']+s['flat']:3d}({ge0_rate:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析二：z_today 反转效应精确分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"二、z_today 反转效应精确分析")
print(f"{'=' * 80}")

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    print(f"\n  {sector}:")
    
    z_buckets = defaultdict(lambda: {'up': 0, 'down': 0, 'flat': 0, 'n': 0})
    for d in sec_data:
        z = d.get('z_today', 0)
        actual = parse_chg(d['实际涨跌'])
        
        if z > 2.0:
            b = '>2.0'
        elif z > 1.0:
            b = '1.0~2.0'
        elif z > 0.5:
            b = '0.5~1.0'
        elif z > 0.0:
            b = '0.0~0.5'
        elif z > -0.5:
            b = '-0.5~0.0'
        elif z > -1.0:
            b = '-1.0~-0.5'
        elif z > -2.0:
            b = '-2.0~-1.0'
        else:
            b = '<-2.0'
        
        z_buckets[b]['n'] += 1
        if actual > 0:
            z_buckets[b]['up'] += 1
        elif actual < 0:
            z_buckets[b]['down'] += 1
        else:
            z_buckets[b]['flat'] += 1
    
    for b in ['>2.0', '1.0~2.0', '0.5~1.0', '0.0~0.5', '-0.5~0.0', '-1.0~-0.5', '-2.0~-1.0', '<-2.0']:
        s = z_buckets[b]
        if s['n'] > 0:
            up_rate = s['up'] / s['n'] * 100
            ge0_rate = (s['up'] + s['flat']) / s['n'] * 100
            le0_rate = (s['down'] + s['flat']) / s['n'] * 100
            print(f"    z_today {b:12s}: n={s['n']:4d} 涨={up_rate:.1f}% >=0={ge0_rate:.1f}% <=0={le0_rate:.1f}%")

# ═══════════════════════════════════════════════════════════
# 分析三：评分区间 vs 实际涨跌（按板块）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"三、评分区间 vs 实际涨跌（按板块）")
print(f"{'=' * 80}")

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    print(f"\n  {sector}:")
    
    score_buckets = defaultdict(lambda: {'up': 0, 'down': 0, 'flat': 0, 'n': 0})
    for d in sec_data:
        score = d.get('评分', 50)
        actual = parse_chg(d['实际涨跌'])
        
        if score >= 60:
            b = '>=60'
        elif score >= 55:
            b = '55-59'
        elif score >= 50:
            b = '50-54'
        elif score >= 45:
            b = '45-49'
        elif score >= 40:
            b = '40-44'
        elif score >= 35:
            b = '35-39'
        else:
            b = '<35'
        
        score_buckets[b]['n'] += 1
        if actual > 0:
            score_buckets[b]['up'] += 1
        elif actual < 0:
            score_buckets[b]['down'] += 1
        else:
            score_buckets[b]['flat'] += 1
    
    for b in ['>=60', '55-59', '50-54', '45-49', '40-44', '35-39', '<35']:
        s = score_buckets[b]
        if s['n'] > 0:
            up_rate = s['up'] / s['n'] * 100
            ge0_rate = (s['up'] + s['flat']) / s['n'] * 100
            le0_rate = (s['down'] + s['flat']) / s['n'] * 100
            print(f"    评分{b:6s}: n={s['n']:4d} 涨={up_rate:.1f}% >=0={ge0_rate:.1f}% <=0={le0_rate:.1f}%")

# ═══════════════════════════════════════════════════════════
# 分析四：同行信号 vs 实际涨跌（按板块）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"四、同行信号 vs 实际涨跌（按板块）")
print(f"{'=' * 80}")

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    print(f"\n  {sector}:")
    
    peer_buckets = defaultdict(lambda: {'up': 0, 'down': 0, 'flat': 0, 'n': 0, 'model_ok': 0})
    for d in sec_data:
        peer = d.get('同行信号', 0)
        actual = parse_chg(d['实际涨跌'])
        
        if peer > 1.0:
            b = '强看涨(>1)'
        elif peer > 0.3:
            b = '看涨(0.3~1)'
        elif peer > -0.3:
            b = '中性(-0.3~0.3)'
        elif peer > -1.0:
            b = '看跌(-1~-0.3)'
        else:
            b = '强看跌(<-1)'
        
        peer_buckets[b]['n'] += 1
        if actual > 0:
            peer_buckets[b]['up'] += 1
        elif actual < 0:
            peer_buckets[b]['down'] += 1
        else:
            peer_buckets[b]['flat'] += 1
        if d['宽松正确'] == '✓':
            peer_buckets[b]['model_ok'] += 1
    
    for b in ['强看涨(>1)', '看涨(0.3~1)', '中性(-0.3~0.3)', '看跌(-1~-0.3)', '强看跌(<-1)']:
        s = peer_buckets[b]
        if s['n'] > 0:
            up_rate = s['up'] / s['n'] * 100
            ge0_rate = (s['up'] + s['flat']) / s['n'] * 100
            model_rate = s['model_ok'] / s['n'] * 100
            print(f"    同行{b:16s}: n={s['n']:4d} 涨={up_rate:.1f}% >=0={ge0_rate:.1f}% 模型准确={model_rate:.1f}%")

# ═══════════════════════════════════════════════════════════
# 分析五：最优简单策略模拟（每板块找最优单一规则）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"五、最优简单策略模拟")
print(f"{'=' * 80}")

# 策略1: 全部预测上涨（利用宽松模式>=0%即正确）
all_up_ok = sum(1 for d in details if parse_chg(d['实际涨跌']) >= 0)
print(f"\n  策略1-全部预测上涨: {all_up_ok}/{total} ({all_up_ok/total*100:.1f}%)")

# 策略2: 全部预测下跌（利用宽松模式<=0%即正确）
all_down_ok = sum(1 for d in details if parse_chg(d['实际涨跌']) <= 0)
print(f"  策略2-全部预测下跌: {all_down_ok}/{total} ({all_down_ok/total*100:.1f}%)")

# 策略3: 按板块基准率选择
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    up_ok = sum(1 for d in sec_data if parse_chg(d['实际涨跌']) >= 0)
    down_ok = sum(1 for d in sec_data if parse_chg(d['实际涨跌']) <= 0)
    best = max(up_ok, down_ok)
    best_dir = '上涨' if up_ok >= down_ok else '下跌'
    print(f"  {sector}: 全涨={up_ok}/{len(sec_data)}({up_ok/len(sec_data)*100:.1f}%) "
          f"全跌={down_ok}/{len(sec_data)}({down_ok/len(sec_data)*100:.1f}%) "
          f"最优={best_dir}({best/len(sec_data)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析六：组合策略模拟（板块基准+z_today反转+combined信号）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"六、组合策略模拟")
print(f"{'=' * 80}")

# 对每个板块，测试不同的决策规则组合
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    n_sec = len(sec_data)
    
    # 基准: 全涨 vs 全跌
    up_base = sum(1 for d in sec_data if parse_chg(d['实际涨跌']) >= 0)
    down_base = sum(1 for d in sec_data if parse_chg(d['实际涨跌']) <= 0)
    base_dir = '上涨' if up_base >= down_base else '下跌'
    base_ok = max(up_base, down_base)
    
    print(f"\n  {sector} (n={n_sec}, 基准={base_dir} {base_ok}/{n_sec}={base_ok/n_sec*100:.1f}%):")
    
    # 测试: 基准方向 + z_today反转
    for z_thresh in [1.0, 1.5, 2.0, 2.5]:
        ok = 0
        for d in sec_data:
            z = d.get('z_today', 0)
            actual = parse_chg(d['实际涨跌'])
            
            if z > z_thresh:
                pred = '下跌'  # 大涨后反转
            elif z < -z_thresh:
                pred = '上涨'  # 大跌后反弹
            else:
                pred = base_dir
            
            if (pred == '上涨' and actual >= 0) or (pred == '下跌' and actual <= 0):
                ok += 1
        
        delta = ok - base_ok
        print(f"    基准+z反转(阈值{z_thresh}): {ok}/{n_sec}({ok/n_sec*100:.1f}%) delta={delta:+d}")
    
    # 测试: combined信号方向 + z_today反转
    for c_thresh in [0.0, 0.3, 0.5, 1.0]:
        for z_thresh in [1.5, 2.0]:
            ok = 0
            for d in sec_data:
                z = d.get('z_today', 0)
                combined = d.get('融合信号', 0)
                actual = parse_chg(d['实际涨跌'])
                
                if z > z_thresh:
                    pred = '下跌'
                elif z < -z_thresh:
                    pred = '上涨'
                elif combined > c_thresh:
                    pred = '上涨'
                elif combined < -c_thresh:
                    pred = '下跌'
                else:
                    pred = base_dir
                
                if (pred == '上涨' and actual >= 0) or (pred == '下跌' and actual <= 0):
                    ok += 1
            
            delta = ok - base_ok
            print(f"    combined(>{c_thresh})+z反转({z_thresh}): {ok}/{n_sec}({ok/n_sec*100:.1f}%) delta={delta:+d}")

# ═══════════════════════════════════════════════════════════
# 分析七：当前v15预测错误样本的特征分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"七、v15预测错误样本特征分析")
print(f"{'=' * 80}")

wrong = [d for d in details if d['宽松正确'] != '✓']
print(f"  错误样本总数: {len(wrong)}")

for sector in sectors:
    sec_wrong = [d for d in wrong if d['板块'] == sector]
    sec_total = [d for d in details if d['板块'] == sector]
    print(f"\n  {sector}: {len(sec_wrong)}/{len(sec_total)}错误 ({len(sec_wrong)/len(sec_total)*100:.1f}%)")
    
    # 错误样本的预测方向分布
    pred_up_wrong = [d for d in sec_wrong if d['预测方向'] == '上涨']
    pred_down_wrong = [d for d in sec_wrong if d['预测方向'] == '下跌']
    print(f"    预测上涨但错: {len(pred_up_wrong)} | 预测下跌但错: {len(pred_down_wrong)}")
    
    # 错误样本的z_today分布
    z_wrong_high = [d for d in sec_wrong if abs(d.get('z_today', 0)) > 1.5]
    z_wrong_med = [d for d in sec_wrong if 0.5 < abs(d.get('z_today', 0)) <= 1.5]
    z_wrong_low = [d for d in sec_wrong if abs(d.get('z_today', 0)) <= 0.5]
    print(f"    z_today分布: |z|>1.5={len(z_wrong_high)} 0.5<|z|<=1.5={len(z_wrong_med)} |z|<=0.5={len(z_wrong_low)}")
    
    # 错误样本的置信度分布
    for conf in ['high', 'medium', 'low']:
        conf_wrong = [d for d in sec_wrong if d.get('置信度') == conf]
        conf_total = [d for d in sec_total if d.get('置信度') == conf]
        if conf_total:
            print(f"    {conf}: {len(conf_wrong)}/{len(conf_total)}错误 ({len(conf_wrong)/len(conf_total)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析八：最优混合策略（每板块独立优化）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"八、最优混合策略搜索（每板块独立优化）")
print(f"{'=' * 80}")

# 对每个板块，搜索最优的 (base_dir, z_thresh, combined_thresh, score_thresh) 组合
best_total = 0
best_config = {}

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    n_sec = len(sec_data)
    best_ok = 0
    best_params = None
    
    for base_dir in ['上涨', '下跌']:
        for z_thresh in [1.0, 1.2, 1.5, 2.0, 2.5, 3.0, 999]:  # 999=不用z反转
            for c_thresh in [0.0, 0.3, 0.5, 0.8, 1.0, 1.5, 999]:  # 999=不用combined
                for score_low in [0, 35, 40]:
                    for score_high in [55, 60, 65, 100]:
                        ok = 0
                        for d in sec_data:
                            z = d.get('z_today', 0)
                            combined = d.get('融合信号', 0)
                            score = d.get('评分', 50)
                            actual = parse_chg(d['实际涨跌'])
                            
                            # 决策逻辑
                            pred = base_dir
                            
                            # z_today反转
                            if z_thresh < 999:
                                if z > z_thresh:
                                    pred = '下跌'
                                elif z < -z_thresh:
                                    pred = '上涨'
                            
                            # combined信号覆盖（仅在z不触发时）
                            if c_thresh < 999 and abs(z) <= z_thresh:
                                if combined > c_thresh:
                                    pred = '上涨'
                                elif combined < -c_thresh:
                                    pred = '下跌'
                            
                            # 评分极端值覆盖
                            if score < score_low:
                                pred = '下跌'
                            elif score > score_high:
                                pred = '上涨'
                            
                            if (pred == '上涨' and actual >= 0) or (pred == '下跌' and actual <= 0):
                                ok += 1
                        
                        if ok > best_ok:
                            best_ok = ok
                            best_params = {
                                'base_dir': base_dir,
                                'z_thresh': z_thresh,
                                'c_thresh': c_thresh,
                                'score_low': score_low,
                                'score_high': score_high,
                            }
    
    best_total += best_ok
    best_config[sector] = best_params
    print(f"  {sector}: {best_ok}/{n_sec} ({best_ok/n_sec*100:.1f}%) params={best_params}")

print(f"\n  总计最优: {best_total}/{total} ({best_total/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析九：星期效应精确分析（按板块）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"九、星期效应精确分析（评分日星期→预测日涨跌）")
print(f"{'=' * 80}")

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    print(f"\n  {sector}:")
    
    for wd in range(5):
        wd_name = ['周一', '周二', '周三', '周四', '周五'][wd]
        wd_data = []
        for d in sec_data:
            try:
                dt = datetime.strptime(d['评分日'], '%Y-%m-%d')
                if dt.weekday() == wd:
                    wd_data.append(d)
            except:
                pass
        
        if wd_data:
            up = sum(1 for d in wd_data if parse_chg(d['实际涨跌']) > 0)
            ge0 = sum(1 for d in wd_data if parse_chg(d['实际涨跌']) >= 0)
            le0 = sum(1 for d in wd_data if parse_chg(d['实际涨跌']) <= 0)
            n_wd = len(wd_data)
            model_ok = sum(1 for d in wd_data if d['宽松正确'] == '✓')
            print(f"    {wd_name}评分→: n={n_wd:3d} 涨={up/n_wd*100:.1f}% >=0={ge0/n_wd*100:.1f}% <=0={le0/n_wd*100:.1f}% 模型={model_ok/n_wd*100:.1f}%")

# ═══════════════════════════════════════════════════════════
# 分析十：美股隔夜信号精确分析（按板块）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"十、美股隔夜信号精确分析")
print(f"{'=' * 80}")

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    print(f"\n  {sector}:")
    
    us_buckets = defaultdict(lambda: {'up': 0, 'down': 0, 'flat': 0, 'n': 0})
    for d in sec_data:
        us = d.get('美股涨跌(%)', None)
        if us is None:
            continue
        actual = parse_chg(d['实际涨跌'])
        
        if us > 1.0:
            b = '美涨>1%'
        elif us > 0.3:
            b = '美涨0.3~1%'
        elif us > -0.3:
            b = '美平'
        elif us > -1.0:
            b = '美跌-1~-0.3%'
        else:
            b = '美跌<-1%'
        
        us_buckets[b]['n'] += 1
        if actual > 0:
            us_buckets[b]['up'] += 1
        elif actual < 0:
            us_buckets[b]['down'] += 1
        else:
            us_buckets[b]['flat'] += 1
    
    for b in ['美涨>1%', '美涨0.3~1%', '美平', '美跌-1~-0.3%', '美跌<-1%']:
        s = us_buckets[b]
        if s['n'] > 0:
            up_rate = s['up'] / s['n'] * 100
            ge0_rate = (s['up'] + s['flat']) / s['n'] * 100
            le0_rate = (s['down'] + s['flat']) / s['n'] * 100
            print(f"    {b:14s}: n={s['n']:3d} 涨={up_rate:.1f}% >=0={ge0_rate:.1f}% <=0={le0_rate:.1f}%")

print(f"\n{'=' * 80}")
print(f"分析完成")
print(f"{'=' * 80}")
