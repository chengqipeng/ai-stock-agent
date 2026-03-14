#!/usr/bin/env python3
"""
v17 新方法探索：
1. 时间序列分割交叉验证（前半训练，后半测试）
2. 留一股票交叉验证
3. 探索新特征：多日动量组合、波动率调整后的均值回归
4. 探索自适应方法：滚动窗口学习
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

def z_bucket(v):
    if v > 1.5: return 'Z++'
    if v > 0.5: return 'Z+'
    if v > -0.5: return 'Z0'
    if v > -1.5: return 'Z-'
    return 'Z--'

dates = sorted(set(d['评分日'] for d in details))
stocks = sorted(set(d['代码'] for d in details))
mid_date = dates[len(dates) // 2]

print(f"总样本: {total}, 日期数: {len(dates)}, 股票数: {len(stocks)}")
print(f"中间日期: {mid_date}")
print(f"前半: {dates[0]} ~ {mid_date}, 后半: {mid_date} ~ {dates[-1]}")

# ═══════════════════════════════════════════════════════════
# 方法A: 时间序列分割（前半训练→后半测试）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法A: 时间序列分割交叉验证")
print(f"{'=' * 80}")

train = [d for d in details if d['评分日'] <= mid_date]
test = [d for d in details if d['评分日'] > mid_date]
print(f"训练集: {len(train)}, 测试集: {len(test)}")

for granularity_name, key_fn in [
    ('sector+combined', lambda d: f"{d['板块']}_{combined_bucket(d.get('融合信号', 0))}"),
    ('sector+combined+wd', lambda d: f"{d['板块']}_{combined_bucket(d.get('融合信号', 0))}_{d['_wd']}"),
    ('sector+z_today_bucket', lambda d: f"{d['板块']}_{z_bucket(d.get('z_today', 0))}"),
    ('sector+combined+z', lambda d: f"{d['板块']}_{combined_bucket(d.get('融合信号', 0))}_{z_bucket(d.get('z_today', 0))}"),
]:
    stats = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in train:
        key = key_fn(d)
        stats[key]['n'] += 1
        if d['_ge0']: stats[key]['ge0'] += 1
        if d['_le0']: stats[key]['le0'] += 1
    
    ok = 0
    for d in test:
        key = key_fn(d)
        s = stats.get(key, {'ge0': 1, 'le0': 1, 'n': 2})
        pred = '上涨' if s['ge0'] >= s['le0'] else '下跌'
        if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
            ok += 1
    
    print(f"  {granularity_name}: {ok}/{len(test)} ({ok/len(test)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法B: 滚动窗口学习（用过去N天的数据学习规则）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法B: 滚动窗口学习")
print(f"{'=' * 80}")

for window_days in [10, 15, 20, 30]:
    ok = 0
    n_tested = 0
    
    for i, test_date in enumerate(dates):
        # 用过去window_days天的数据
        past_dates = [d for d in dates if d < test_date][-window_days:]
        if len(past_dates) < 5:
            continue
        
        train_data = [d for d in details if d['评分日'] in set(past_dates)]
        test_data = [d for d in details if d['评分日'] == test_date]
        
        # 学习 sector+combined 规则
        stats = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
        for d in train_data:
            key = f"{d['板块']}_{combined_bucket(d.get('融合信号', 0))}"
            stats[key]['n'] += 1
            if d['_ge0']: stats[key]['ge0'] += 1
            if d['_le0']: stats[key]['le0'] += 1
        
        for d in test_data:
            key = f"{d['板块']}_{combined_bucket(d.get('融合信号', 0))}"
            s = stats.get(key, {'ge0': 1, 'le0': 1, 'n': 2})
            pred = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                ok += 1
            n_tested += 1
    
    print(f"  window={window_days}天: {ok}/{n_tested} ({ok/n_tested*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法C: 纯均值回归策略（不同z_today阈值）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法C: 纯均值回归策略")
print(f"{'=' * 80}")

for z_thresh in [0.0, 0.3, 0.5, 0.8, 1.0]:
    ok = 0
    for d in details:
        z = d.get('z_today', 0)
        if z > z_thresh:
            pred = '下跌'  # 今天涨了→明天跌
        elif z < -z_thresh:
            pred = '上涨'  # 今天跌了→明天涨
        else:
            # 中性区间用板块基准率
            sec = d['板块']
            base_rates = {'化工': 0.61, '有色金属': 0.579, '新能源': 0.51,
                         '制造': 0.479, '科技': 0.458, '汽车': 0.453, '医药': 0.446}
            pred = '上涨' if base_rates.get(sec, 0.5) > 0.5 else '下跌'
        
        if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
            ok += 1
    
    print(f"  z_thresh={z_thresh}: {ok}/{total} ({ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法D: 板块特化均值回归（每个板块不同z阈值）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法D: 板块特化均值回归")
print(f"{'=' * 80}")

# 先找每个板块的最优z阈值
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    best_ok = 0
    best_thresh = 0
    best_default = '上涨'
    
    for z_thresh in [x * 0.1 for x in range(0, 20)]:
        for default_dir in ['上涨', '下跌']:
            ok = 0
            for d in sec_data:
                z = d.get('z_today', 0)
                if z > z_thresh:
                    pred = '下跌'
                elif z < -z_thresh:
                    pred = '上涨'
                else:
                    pred = default_dir
                
                if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                    ok += 1
            
            if ok > best_ok:
                best_ok = ok
                best_thresh = z_thresh
                best_default = default_dir
    
    print(f"  {sec}: 最优z={best_thresh:.1f} default={best_default} → {best_ok}/{n} ({best_ok/n*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法E: 组合策略 - 均值回归 + combined信号 + 板块基准
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法E: 组合策略探索")
print(f"{'=' * 80}")

# 策略1: z_today反转 + combined方向一致时才预测
ok = 0
for d in details:
    z = d.get('z_today', 0)
    comb = d.get('融合信号', 0)
    sec = d['板块']
    base_rates = {'化工': 0.61, '有色金属': 0.579, '新能源': 0.51,
                 '制造': 0.479, '科技': 0.458, '汽车': 0.453, '医药': 0.446}
    
    # z反转信号
    z_dir = None
    if z > 0.5:
        z_dir = '下跌'
    elif z < -0.5:
        z_dir = '上涨'
    
    # combined信号
    c_dir = None
    if comb > 0.5:
        c_dir = '上涨'
    elif comb < -0.5:
        c_dir = '下跌'
    
    # 决策
    if z_dir and c_dir and z_dir == c_dir:
        pred = z_dir  # 两个信号一致
    elif z_dir and not c_dir:
        pred = z_dir  # 只有z信号
    elif c_dir and not z_dir:
        pred = c_dir  # 只有combined信号
    else:
        pred = '上涨' if base_rates.get(sec, 0.5) > 0.5 else '下跌'
    
    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
        ok += 1
print(f"  策略1(z+combined一致): {ok}/{total} ({ok/total*100:.1f}%)")

# 策略2: 纯板块基准率（baseline）
ok = 0
for d in details:
    sec = d['板块']
    base_rates = {'化工': 0.61, '有色金属': 0.579, '新能源': 0.51,
                 '制造': 0.479, '科技': 0.458, '汽车': 0.453, '医药': 0.446}
    pred = '上涨' if base_rates.get(sec, 0.5) > 0.5 else '下跌'
    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
        ok += 1
print(f"  策略2(纯基准率): {ok}/{total} ({ok/total*100:.1f}%)")

# 策略3: z_today反转 + 板块基准率（无combined）
for z_t in [0.3, 0.5, 0.8, 1.0, 1.2]:
    ok = 0
    for d in details:
        z = d.get('z_today', 0)
        sec = d['板块']
        base_rates = {'化工': 0.61, '有色金属': 0.579, '新能源': 0.51,
                     '制造': 0.479, '科技': 0.458, '汽车': 0.453, '医药': 0.446}
        
        if z > z_t:
            pred = '下跌'
        elif z < -z_t:
            pred = '上涨'
        else:
            pred = '上涨' if base_rates.get(sec, 0.5) > 0.5 else '下跌'
        
        if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
            ok += 1
    print(f"  策略3(z反转+基准, z={z_t}): {ok}/{total} ({ok/total*100:.1f}%)")

# 策略4: 板块特化 - 偏涨板块always上涨, 偏跌板块用z反转
ok = 0
for d in details:
    z = d.get('z_today', 0)
    sec = d['板块']
    
    if sec in ('化工', '有色金属'):
        # 强偏涨板块: always上涨，除非z极端
        if z > 1.5:
            pred = '下跌'
        else:
            pred = '上涨'
    elif sec == '新能源':
        # 中性板块
        if z > 0.8:
            pred = '下跌'
        elif z < -0.8:
            pred = '上涨'
        else:
            pred = '上涨'
    else:
        # 偏跌板块: 科技/汽车/医药/制造
        if z < -0.8:
            pred = '上涨'
        elif z > 0.8:
            pred = '下跌'
        else:
            pred = '下跌'
    
    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
        ok += 1
print(f"  策略4(板块特化z反转): {ok}/{total} ({ok/total*100:.1f}%)")

# 策略5: 每个板块独立最优combined阈值
print(f"\n  策略5: 板块独立最优combined阈值")
total_ok = 0
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    best_ok = 0
    best_params = {}
    
    for bull_t in [x * 0.25 for x in range(-4, 8)]:
        for bear_t in [x * 0.25 for x in range(-8, 4)]:
            if bear_t >= bull_t:
                continue
            for default_dir in ['上涨', '下跌']:
                ok = 0
                for d in sec_data:
                    comb = d.get('融合信号', 0)
                    if comb > bull_t:
                        pred = '上涨'
                    elif comb < bear_t:
                        pred = '下跌'
                    else:
                        pred = default_dir
                    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                        ok += 1
                if ok > best_ok:
                    best_ok = ok
                    best_params = {'bull': bull_t, 'bear': bear_t, 'default': default_dir}
    
    total_ok += best_ok
    print(f"    {sec}: bull>{best_params['bull']:.2f} bear<{best_params['bear']:.2f} "
          f"default={best_params['default']} → {best_ok}/{n} ({best_ok/n*100:.1f}%)")
print(f"    总计: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法F: 时间序列分割验证板块独立最优阈值
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法F: 时间序列分割验证板块独立最优阈值")
print(f"{'=' * 80}")

train_set = [d for d in details if d['评分日'] <= mid_date]
test_set = [d for d in details if d['评分日'] > mid_date]

total_ok = 0
total_test = 0
for sec in sectors:
    sec_train = [d for d in train_set if d['板块'] == sec]
    sec_test = [d for d in test_set if d['板块'] == sec]
    
    # 在训练集上找最优阈值
    best_ok = 0
    best_params = {'bull': 0.5, 'bear': -0.5, 'default': '上涨'}
    
    for bull_t in [x * 0.25 for x in range(-4, 8)]:
        for bear_t in [x * 0.25 for x in range(-8, 4)]:
            if bear_t >= bull_t:
                continue
            for default_dir in ['上涨', '下跌']:
                ok = 0
                for d in sec_train:
                    comb = d.get('融合信号', 0)
                    if comb > bull_t:
                        pred = '上涨'
                    elif comb < bear_t:
                        pred = '下跌'
                    else:
                        pred = default_dir
                    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                        ok += 1
                if ok > best_ok:
                    best_ok = ok
                    best_params = {'bull': bull_t, 'bear': bear_t, 'default': default_dir}
    
    # 在测试集上验证
    ok_test = 0
    for d in sec_test:
        comb = d.get('融合信号', 0)
        if comb > best_params['bull']:
            pred = '上涨'
        elif comb < best_params['bear']:
            pred = '下跌'
        else:
            pred = best_params['default']
        if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
            ok_test += 1
    
    total_ok += ok_test
    total_test += len(sec_test)
    train_rate = best_ok / len(sec_train) * 100 if sec_train else 0
    test_rate = ok_test / len(sec_test) * 100 if sec_test else 0
    print(f"  {sec}: train={best_ok}/{len(sec_train)}({train_rate:.1f}%) "
          f"test={ok_test}/{len(sec_test)}({test_rate:.1f}%) "
          f"params=bull>{best_params['bull']:.2f} bear<{best_params['bear']:.2f} "
          f"default={best_params['default']}")

print(f"  总计: {total_ok}/{total_test} ({total_ok/total_test*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 方法G: 滚动窗口 + 板块独立最优阈值
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"方法G: 滚动窗口 + 板块独立最优阈值")
print(f"{'=' * 80}")

for window in [15, 20, 30]:
    total_ok = 0
    total_tested = 0
    
    for test_date in dates:
        past_dates = sorted([d2 for d2 in dates if d2 < test_date])[-window:]
        if len(past_dates) < 8:
            continue
        
        past_set = set(past_dates)
        test_data = [d for d in details if d['评分日'] == test_date]
        
        for sec in sectors:
            sec_train = [d for d in details if d['评分日'] in past_set and d['板块'] == sec]
            sec_test = [d for d in test_data if d['板块'] == sec]
            
            if not sec_test or len(sec_train) < 5:
                # 不够数据，用基准率
                base_rates = {'化工': 0.61, '有色金属': 0.579, '新能源': 0.51,
                             '制造': 0.479, '科技': 0.458, '汽车': 0.453, '医药': 0.446}
                default = '上涨' if base_rates.get(sec, 0.5) > 0.5 else '下跌'
                for d in sec_test:
                    if (default == '上涨' and d['_ge0']) or (default == '下跌' and d['_le0']):
                        total_ok += 1
                    total_tested += 1
                continue
            
            # 在训练集上找最优combined阈值
            best_ok = 0
            best_params = {'bull': 0.5, 'bear': -0.5, 'default': '上涨'}
            
            for bull_t in [x * 0.5 for x in range(-2, 6)]:
                for bear_t in [x * 0.5 for x in range(-6, 2)]:
                    if bear_t >= bull_t:
                        continue
                    for default_dir in ['上涨', '下跌']:
                        ok = 0
                        for d in sec_train:
                            comb = d.get('融合信号', 0)
                            if comb > bull_t:
                                pred = '上涨'
                            elif comb < bear_t:
                                pred = '下跌'
                            else:
                                pred = default_dir
                            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                                ok += 1
                        if ok > best_ok:
                            best_ok = ok
                            best_params = {'bull': bull_t, 'bear': bear_t, 'default': default_dir}
            
            for d in sec_test:
                comb = d.get('融合信号', 0)
                if comb > best_params['bull']:
                    pred = '上涨'
                elif comb < best_params['bear']:
                    pred = '下跌'
                else:
                    pred = best_params['default']
                if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                    total_ok += 1
                total_tested += 1
    
    print(f"  window={window}: {total_ok}/{total_tested} ({total_ok/total_tested*100:.1f}%)")

print(f"\n{'=' * 80}")
print(f"分析完成")
print(f"{'=' * 80}")
