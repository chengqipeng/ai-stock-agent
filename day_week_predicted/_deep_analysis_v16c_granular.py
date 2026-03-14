#!/usr/bin/env python3
"""
v16c 细粒度最优决策搜索：
对每个 (板块, 星期, combined区间, peer区间, 美股区间) 组合找最优方向。
关键：避免过拟合 — 只在样本量>=5时使用细粒度规则，否则回退到粗粒度。
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
    try:
        d['_wd'] = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
    except:
        d['_wd'] = -1

def combined_bucket(v):
    if v > 1.5: return 'C++'
    if v > 0.5: return 'C+'
    if v > -0.5: return 'C0'
    if v > -1.5: return 'C-'
    return 'C--'

def peer_bucket(v):
    if v > 1.0: return 'P+'
    if v > -1.0: return 'P0'
    return 'P-'

def us_bucket(v):
    if v is None: return 'U?'
    if v > 0.5: return 'U+'
    if v > -0.5: return 'U0'
    return 'U-'

def z_bucket(v):
    if v > 1.5: return 'Z++'
    if v > 0.5: return 'Z+'
    if v > -0.5: return 'Z0'
    if v > -1.5: return 'Z-'
    return 'Z--'

# ═══════════════════════════════════════════════════════════
# 方法1: (板块, combined_bucket) 最优方向
# ═══════════════════════════════════════════════════════════
print(f"{'=' * 80}")
print(f"方法1: (板块, combined_bucket) 最优方向")
print(f"{'=' * 80}")

total_ok = 0
for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    n = len(sec)
    ok = 0
    
    # 统计每个bucket的最优方向
    buckets = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in sec:
        cb = combined_bucket(d.get('融合信号', 0))
        buckets[cb]['n'] += 1
        if d['_ge0']: buckets[cb]['ge0'] += 1
        if d['_le0']: buckets[cb]['le0'] += 1
    
    for cb, s in buckets.items():
        best_dir = '上涨' if s['ge0'] >= s['le0'] else '下跌'
        best_n = max(s['ge0'], s['le0'])
        ok += best_n
        print(f"  {sector} {cb}: n={s['n']} 上涨={s['ge0']}({s['ge0']/s['n']*100:.0f}%) 下跌={s['le0']}({s['le0']/s['n']*100:.0f}%) → {best_dir}")
    
    total_ok += ok
    print(f"  {sector} 小计: {ok}/{n} ({ok/n*100:.1f}%)")

print(f"\n  总计: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法2: (板块, combined_bucket, peer_bucket) 最优方向
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法2: (板块, combined_bucket, peer_bucket) 最优方向")
print(f"{'=' * 80}")

total_ok = 0
for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    n = len(sec)
    ok = 0
    
    buckets = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in sec:
        cb = combined_bucket(d.get('融合信号', 0))
        pb = peer_bucket(d.get('同行信号', 0))
        key = f"{cb}_{pb}"
        buckets[key]['n'] += 1
        if d['_ge0']: buckets[key]['ge0'] += 1
        if d['_le0']: buckets[key]['le0'] += 1
    
    for key, s in sorted(buckets.items()):
        best_dir = '上涨' if s['ge0'] >= s['le0'] else '下跌'
        best_n = max(s['ge0'], s['le0'])
        ok += best_n
    
    total_ok += ok
    print(f"  {sector}: {ok}/{n} ({ok/n*100:.1f}%)")

print(f"\n  总计: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法3: (板块, combined_bucket, peer_bucket, weekday) 最优方向
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法3: (板块, combined_bucket, peer_bucket, weekday) 最优方向")
print(f"{'=' * 80}")

total_ok = 0
for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    n = len(sec)
    ok = 0
    
    buckets = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in sec:
        cb = combined_bucket(d.get('融合信号', 0))
        pb = peer_bucket(d.get('同行信号', 0))
        wd = d['_wd']
        key = f"{cb}_{pb}_{wd}"
        buckets[key]['n'] += 1
        if d['_ge0']: buckets[key]['ge0'] += 1
        if d['_le0']: buckets[key]['le0'] += 1
    
    for key, s in sorted(buckets.items()):
        best_n = max(s['ge0'], s['le0'])
        ok += best_n
    
    total_ok += ok
    print(f"  {sector}: {ok}/{n} ({ok/n*100:.1f}%)")

print(f"\n  总计: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法4: (板块, combined_bucket, peer_bucket, us_bucket) 最优方向
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法4: (板块, combined_bucket, peer_bucket, us_bucket) 最优方向")
print(f"{'=' * 80}")

total_ok = 0
for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    n = len(sec)
    ok = 0
    
    buckets = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in sec:
        cb = combined_bucket(d.get('融合信号', 0))
        pb = peer_bucket(d.get('同行信号', 0))
        ub = us_bucket(d.get('美股涨跌(%)'))
        key = f"{cb}_{pb}_{ub}"
        buckets[key]['n'] += 1
        if d['_ge0']: buckets[key]['ge0'] += 1
        if d['_le0']: buckets[key]['le0'] += 1
    
    for key, s in sorted(buckets.items()):
        best_n = max(s['ge0'], s['le0'])
        ok += best_n
    
    total_ok += ok
    print(f"  {sector}: {ok}/{n} ({ok/n*100:.1f}%)")

print(f"\n  总计: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法5: 全维度 (板块, combined, peer, us, weekday, z_today)
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法5: 全维度 (板块, combined, peer, us, weekday, z_today)")
print(f"{'=' * 80}")

total_ok = 0
total_buckets = 0
small_buckets = 0
for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    n = len(sec)
    ok = 0
    
    buckets = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in sec:
        cb = combined_bucket(d.get('融合信号', 0))
        pb = peer_bucket(d.get('同行信号', 0))
        ub = us_bucket(d.get('美股涨跌(%)'))
        wd = d['_wd']
        zb = z_bucket(d.get('z_today', 0))
        key = f"{cb}_{pb}_{ub}_{wd}_{zb}"
        buckets[key]['n'] += 1
        if d['_ge0']: buckets[key]['ge0'] += 1
        if d['_le0']: buckets[key]['le0'] += 1
    
    for key, s in sorted(buckets.items()):
        best_n = max(s['ge0'], s['le0'])
        ok += best_n
        total_buckets += 1
        if s['n'] < 5:
            small_buckets += 1
    
    total_ok += ok
    print(f"  {sector}: {ok}/{n} ({ok/n*100:.1f}%)")

print(f"\n  总计: {total_ok}/{total} ({total_ok/total*100:.1f}%)")
print(f"  总桶数: {total_buckets}, 小桶(<5样本): {small_buckets}")

# ═══════════════════════════════════════════════════════════
# 方法6: 带最小样本量约束的细粒度决策
# 细粒度桶样本>=min_n时用细粒度，否则回退到粗粒度
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法6: 带最小样本量约束的层级决策")
print(f"{'=' * 80}")

for min_n in [3, 5, 8, 10]:
    total_ok = 0
    for sector in sectors:
        sec = [d for d in details if d['板块'] == sector]
        n = len(sec)
        
        # 预计算各粒度的统计
        fine = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
        medium = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
        coarse = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
        
        for d in sec:
            cb = combined_bucket(d.get('融合信号', 0))
            pb = peer_bucket(d.get('同行信号', 0))
            ub = us_bucket(d.get('美股涨跌(%)'))
            wd = d['_wd']
            
            fine_key = f"{cb}_{pb}_{ub}_{wd}"
            med_key = f"{cb}_{pb}_{wd}"
            coarse_key = f"{cb}_{wd}"
            
            for store, key in [(fine, fine_key), (medium, med_key), (coarse, coarse_key)]:
                store[key]['n'] += 1
                if d['_ge0']: store[key]['ge0'] += 1
                if d['_le0']: store[key]['le0'] += 1
        
        ok = 0
        for d in sec:
            cb = combined_bucket(d.get('融合信号', 0))
            pb = peer_bucket(d.get('同行信号', 0))
            ub = us_bucket(d.get('美股涨跌(%)'))
            wd = d['_wd']
            
            fine_key = f"{cb}_{pb}_{ub}_{wd}"
            med_key = f"{cb}_{pb}_{wd}"
            coarse_key = f"{cb}_{wd}"
            
            # 层级回退
            if fine[fine_key]['n'] >= min_n:
                s = fine[fine_key]
            elif medium[med_key]['n'] >= min_n:
                s = medium[med_key]
            else:
                s = coarse[coarse_key]
            
            pred = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            
            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                ok += 1
        
        total_ok += ok
    
    print(f"  min_n={min_n}: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法7: 交叉验证估计（留一法近似）
# 用于估计实际泛化性能
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法7: 留一日交叉验证（估计泛化性能）")
print(f"{'=' * 80}")

# 按日期分组
dates = sorted(set(d['评分日'] for d in details))
print(f"  总日期数: {len(dates)}")

for granularity in ['coarse', 'medium', 'fine']:
    total_ok = 0
    
    for test_date in dates:
        train = [d for d in details if d['评分日'] != test_date]
        test = [d for d in details if d['评分日'] == test_date]
        
        # 在训练集上统计
        stats = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
        for d in train:
            sector = d['板块']
            cb = combined_bucket(d.get('融合信号', 0))
            pb = peer_bucket(d.get('同行信号', 0))
            wd = d['_wd']
            
            if granularity == 'coarse':
                key = f"{sector}_{cb}"
            elif granularity == 'medium':
                key = f"{sector}_{cb}_{pb}"
            else:
                key = f"{sector}_{cb}_{pb}_{wd}"
            
            stats[key]['n'] += 1
            if d['_ge0']: stats[key]['ge0'] += 1
            if d['_le0']: stats[key]['le0'] += 1
        
        # 在测试集上预测
        for d in test:
            sector = d['板块']
            cb = combined_bucket(d.get('融合信号', 0))
            pb = peer_bucket(d.get('同行信号', 0))
            wd = d['_wd']
            
            if granularity == 'coarse':
                key = f"{sector}_{cb}"
            elif granularity == 'medium':
                key = f"{sector}_{cb}_{pb}"
            else:
                key = f"{sector}_{cb}_{pb}_{wd}"
            
            s = stats.get(key, {'ge0': 1, 'le0': 1, 'n': 2})
            pred = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            
            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                total_ok += 1
    
    print(f"  {granularity}: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

print(f"\n{'=' * 80}")
print(f"分析完成")
print(f"{'=' * 80}")
