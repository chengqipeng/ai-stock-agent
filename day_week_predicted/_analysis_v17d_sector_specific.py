#!/usr/bin/env python3
"""
v17d: 板块特化决策 - 每个板块用不同的主信号。
基于v17c分析结果，设计板块特化策略。
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

# ═══════════════════════════════════════════════════════════
# 策略v17d: 板块特化决策
# ═══════════════════════════════════════════════════════════

def predict_v17d(d):
    """板块特化决策"""
    sec = d['板块']
    z = d.get('z_today', 0)
    comb = d.get('融合信号', 0)
    peer = d.get('同行信号', 0)
    us = d.get('美股涨跌(%)')
    us_val = us if us is not None else 0
    score = d.get('评分', 50)
    wd = d['_wd']
    
    if sec == '化工':
        # 基准率61%涨，低评分更强(69-72%)
        # z<0时66.2%涨（反转）
        # peer<0时62.6%涨（反转）
        # US>0时63.5%涨
        if score < 50:
            return '上涨'  # 低评分强反转 69-72%
        if z < -0.3:
            return '上涨'  # z反转 66.2%
        if peer < -1.0:
            return '上涨'  # peer反转 62.6%
        if comb < -1.0:
            return '下跌'  # 极端看跌信号
        return '上涨'  # 基准率61%
    
    elif sec == '有色金属':
        # 基准率57.9%涨
        # US>0时64.7%涨
        # peer<0时58.4%涨（反转）
        # z不太有用
        if us_val > 0.5:
            return '上涨'  # US看涨 64.7%
        if score < 40:
            return '上涨'  # 低评分反转 62.7%
        if comb < -1.5:
            return '下跌'  # 极端看跌
        return '上涨'  # 基准率57.9%
    
    elif sec == '科技':
        # 基准率45.8%涨 → 54.2%跌
        # US>0时39%涨 → 61%跌（反转）
        # z>0时40.8%涨 → 59.2%跌（反转）
        # score>=60时32%涨 → 68%跌
        # score 50-55时37.8%涨 → 62.2%跌
        if score >= 55:
            return '下跌'  # 高评分反转 62-68%
        if us_val > 0.5 and z > 0:
            return '下跌'  # US涨+今天涨 → 明天跌
        if z > 0.5:
            return '下跌'  # z反转
        if comb > 1.5:
            return '上涨'  # 极端看涨信号
        if z < -1.0:
            return '上涨'  # 大跌反弹
        return '下跌'  # 基准率54.2%
    
    elif sec == '汽车':
        # 基准率45.3%涨 → 54.7%跌
        # US>0时38.3%涨 → 61.7%跌（反转）
        # score 45-50时35.4%涨 → 64.6%跌
        if us_val > 0.5:
            return '下跌'  # US反转 61.7%
        if 45 <= score < 50:
            return '下跌'  # 中等评分 64.6%
        if comb > 1.0:
            return '上涨'  # 强看涨信号
        if z < -1.0:
            return '上涨'  # 大跌反弹
        return '下跌'  # 基准率54.7%
    
    elif sec == '新能源':
        # 基准率51%涨 → 49.4%跌，接近50/50
        # 没有特别强的单一信号
        # score 50-55时59.8%涨
        if 50 <= score < 55:
            return '上涨'  # 中等偏高评分 59.8%
        if z > 0.8:
            return '下跌'  # z反转
        if z < -0.8:
            return '上涨'  # z反转
        if comb > 0.5:
            return '上涨'
        if comb < -0.5:
            return '下跌'
        return '上涨'  # 基准率51%
    
    elif sec == '医药':
        # 基准率44.6%涨 → 56.4%跌
        # peer<0时39.3%涨 → 60.7%跌（aligned）
        # z<0时43.1%涨 → 58.3%跌
        # score 45-50时36%涨 → 65.3%跌
        if 45 <= score < 50:
            return '下跌'  # 65.3%
        if peer < -1.0:
            return '下跌'  # aligned 60.7%
        if z < -0.5:
            return '下跌'  # 58.3%
        if comb > 1.0:
            return '上涨'  # 强看涨
        return '下跌'  # 基准率56.4%
    
    elif sec == '制造':
        # 基准率47.9%涨 → 52.7%跌
        # score>=60时72.7%涨（小样本n=22）
        # score<40时52.5%涨
        # score 45-50时41.9%涨 → 59.7%跌
        if score >= 60:
            return '上涨'  # 72.7%（小样本）
        if 45 <= score < 50:
            return '下跌'  # 59.7%
        if comb > 1.0:
            return '上涨'
        if z < -1.0:
            return '上涨'  # 大跌反弹
        return '下跌'  # 基准率52.7%
    
    return '上涨' if comb > 0 else '下跌'

# 测试
ok = 0
for d in details:
    pred = predict_v17d(d)
    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
        ok += 1
print(f"v17d 全样本: {ok}/{total} ({ok/total*100:.1f}%)")

# 按板块
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    ok = sum(1 for d in sec_data if 
             (predict_v17d(d) == '上涨' and d['_ge0']) or 
             (predict_v17d(d) == '下跌' and d['_le0']))
    print(f"  {sec}: {ok}/{n} ({ok/n*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 时间序列验证
# ═══════════════════════════════════════════════════════════
dates = sorted(set(d['评分日'] for d in details))
mid_date = dates[len(dates) // 2]
train = [d for d in details if d['评分日'] <= mid_date]
test = [d for d in details if d['评分日'] > mid_date]

print(f"\n时间序列验证:")
print(f"训练集: {len(train)}, 测试集: {len(test)}")

ok_train = sum(1 for d in train if 
               (predict_v17d(d) == '上涨' and d['_ge0']) or 
               (predict_v17d(d) == '下跌' and d['_le0']))
ok_test = sum(1 for d in test if 
              (predict_v17d(d) == '上涨' and d['_ge0']) or 
              (predict_v17d(d) == '下跌' and d['_le0']))
print(f"  训练集: {ok_train}/{len(train)} ({ok_train/len(train)*100:.1f}%)")
print(f"  测试集: {ok_test}/{len(test)} ({ok_test/len(test)*100:.1f}%)")

# 按板块的测试集表现
for sec in sectors:
    sec_test = [d for d in test if d['板块'] == sec]
    n = len(sec_test)
    if n == 0:
        continue
    ok = sum(1 for d in sec_test if 
             (predict_v17d(d) == '上涨' and d['_ge0']) or 
             (predict_v17d(d) == '下跌' and d['_le0']))
    print(f"  {sec} 测试集: {ok}/{n} ({ok/n*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 对比: 当前v16b
# ═══════════════════════════════════════════════════════════
print(f"\n对比v16b:")
ok_v16b = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"  v16b全样本: {ok_v16b}/{total} ({ok_v16b/total*100:.1f}%)")

ok_v16b_train = sum(1 for d in train if d['宽松正确'] == '✓')
ok_v16b_test = sum(1 for d in test if d['宽松正确'] == '✓')
print(f"  v16b训练集: {ok_v16b_train}/{len(train)} ({ok_v16b_train/len(train)*100:.1f}%)")
print(f"  v16b测试集: {ok_v16b_test}/{len(test)} ({ok_v16b_test/len(test)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# v17d改进版: 加入星期效应
# ═══════════════════════════════════════════════════════════
def predict_v17d_v2(d):
    """v17d改进: 加入最强星期效应"""
    base = predict_v17d(d)
    sec = d['板块']
    wd = d['_wd']
    comb = d.get('融合信号', 0)
    
    # 只使用分析2中最强的星期效应（>=75%准确率，>=20样本）
    # 有色金属 US-_wd2 → 下跌 95% (n=20) — 但这是US+wd组合
    # 医药 score0_wd4 → 下跌 83.3% (n=24)
    # 有色金属 peer-_wd3 → 上涨 85.7% (n=28)
    # 有色金属 score-_wd1 → 上涨 83.3% (n=24)
    
    # 简化: 只用最稳健的星期效应
    if sec == '医药' and wd == 4:  # 周五
        return '下跌'
    if sec == '有色金属' and wd == 1:  # 周二
        return '上涨'
    
    return base

ok = 0
for d in details:
    pred = predict_v17d_v2(d)
    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
        ok += 1
print(f"\nv17d_v2 全样本: {ok}/{total} ({ok/total*100:.1f}%)")

ok_test = sum(1 for d in test if 
              (predict_v17d_v2(d) == '上涨' and d['_ge0']) or 
              (predict_v17d_v2(d) == '下跌' and d['_le0']))
print(f"v17d_v2 测试集: {ok_test}/{len(test)} ({ok_test/len(test)*100:.1f}%)")
