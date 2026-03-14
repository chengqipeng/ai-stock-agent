#!/usr/bin/env python3
"""
v17 概念板块增强深度分析：
在v15基础上，深入分析概念板块信号作为"修正层"的精确效果。

核心思路：
1. 不是用概念信号替代模型预测，而是在特定条件下修正
2. 分析概念信号的多维度组合（概念涨跌比 × 概念资金流 × 行业同行信号）
3. 按板块精细化分析概念信号的最优使用方式
4. 时间序列验证（前半训练→后半测试）避免过拟合
5. 分析概念板块成分股的"走势相关性加权"投票
"""
import json
import logging
from collections import defaultdict
from datetime import datetime

logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

print(f"{'=' * 80}")
print(f"v17 概念板块增强深度分析")
print(f"{'=' * 80}")

# ═══════════════════════════════════════════════════════════
# 加载数据
# ═══════════════════════════════════════════════════════════
with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    bt_data = json.load(f)
details = bt_data['逐日详情']
total = len(details)

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_ge0'] = d['_actual'] >= 0
    d['_le0'] = d['_actual'] <= 0
    try:
        d['_wd'] = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
    except:
        d['_wd'] = -1

loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"当前基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"目标65%: {int(total*0.65)}/{total}, 差距: {int(total*0.65) - loose_ok}个样本")

# 概念板块映射
with open('data_results/industry_analysis/stock_boards_map.json') as f:
    boards_data = json.load(f)

stock_concepts = {}
for code, info in boards_data['stocks'].items():
    stock_concepts[code] = {
        'concept_boards': info.get('concept_boards', []),
        'top_concepts': info.get('top_concepts', []),
    }

concept_stocks = boards_data['concept_boards']

bt_codes = set(d['代码'] for d in details)
sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']

# ═══════════════════════════════════════════════════════════
# 从DB加载概念板块成分股K线聚合 + 资金流聚合
# ═══════════════════════════════════════════════════════════
print(f"\n加载概念板块成分股聚合数据...")

from dao import get_connection

conn = get_connection(use_dict_cursor=True)
cursor = conn.cursor()

all_dates = sorted(set(d['评分日'] for d in details))
date_min = all_dates[0]
date_max = all_dates[-1]

relevant_concepts = set()
for code in bt_codes:
    for c in stock_concepts.get(code, {}).get('concept_boards', []):
        relevant_concepts.add(c)

# K线聚合
concept_daily_stats = {}
for cname in relevant_concepts:
    codes = concept_stocks.get(cname, [])
    if not codes or len(codes) < 5:
        continue
    codes_batch = codes[:200]
    placeholders = ','.join(['%s'] * len(codes_batch))
    try:
        cursor.execute(f"""
            SELECT date,
                   COUNT(*) as total_stocks,
                   SUM(CASE WHEN change_percent > 0.3 THEN 1 ELSE 0 END) as up_count,
                   SUM(CASE WHEN change_percent < -0.3 THEN 1 ELSE 0 END) as down_count,
                   AVG(change_percent) as avg_chg
            FROM stock_kline
            WHERE stock_code IN ({placeholders})
            AND date >= %s AND date <= %s
            AND trading_volume > 0
            GROUP BY date ORDER BY date
        """, codes_batch + [date_min, date_max])
        for row in cursor.fetchall():
            dt = row['date']
            total_s = row['total_stocks']
            concept_daily_stats[(cname, dt)] = {
                'up_ratio': row['up_count'] / total_s if total_s > 0 else 0.5,
                'avg_chg': float(row['avg_chg'] or 0),
                'total': total_s,
            }
    except Exception as e:
        pass

# 资金流聚合
concept_daily_fund = {}
for cname in relevant_concepts:
    codes = concept_stocks.get(cname, [])
    if not codes or len(codes) < 5:
        continue
    codes_batch = codes[:200]
    placeholders = ','.join(['%s'] * len(codes_batch))
    try:
        cursor.execute(f"""
            SELECT date,
                   COUNT(*) as total_stocks,
                   SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) as inflow_count,
                   AVG(big_net_pct) as avg_big_net_pct
            FROM stock_fund_flow
            WHERE stock_code IN ({placeholders})
            AND date >= %s AND date <= %s
            GROUP BY date ORDER BY date
        """, codes_batch + [date_min, date_max])
        for row in cursor.fetchall():
            dt = row['date']
            total_s = row['total_stocks']
            concept_daily_fund[(cname, dt)] = {
                'inflow_ratio': row['inflow_count'] / total_s if total_s > 0 else 0.5,
                'avg_big_net_pct': float(row['avg_big_net_pct'] or 0),
                'total': total_s,
            }
    except Exception as e:
        pass

cursor.close()
conn.close()

print(f"概念K线聚合: {len(concept_daily_stats)}条, 资金流聚合: {len(concept_daily_fund)}条")

# ═══════════════════════════════════════════════════════════
# 为每个回测样本计算概念信号
# ═══════════════════════════════════════════════════════════
print(f"\n计算概念信号...")

for d in details:
    code = d['代码']
    score_date = d['评分日']
    concepts = stock_concepts.get(code, {}).get('concept_boards', [])
    top_concepts = stock_concepts.get(code, {}).get('top_concepts', [])
    
    # 概念涨跌投票
    concept_up = 0
    concept_down = 0
    concept_total = 0
    concept_avg_chg = []
    
    # top概念（最相关）单独统计
    top_up = 0
    top_down = 0
    top_total = 0
    top_avg_chg = []
    
    # 概念资金流投票
    fund_inflow = 0
    fund_outflow = 0
    fund_total = 0
    fund_avg_net = []
    
    for cname in concepts:
        stats = concept_daily_stats.get((cname, score_date))
        if stats and stats['total'] >= 5:
            concept_total += 1
            concept_avg_chg.append(stats['avg_chg'])
            if stats['up_ratio'] > 0.6:
                concept_up += 1
            elif stats['up_ratio'] < 0.4:
                concept_down += 1
            
            if cname in top_concepts:
                top_total += 1
                top_avg_chg.append(stats['avg_chg'])
                if stats['up_ratio'] > 0.6:
                    top_up += 1
                elif stats['up_ratio'] < 0.4:
                    top_down += 1
        
        fund = concept_daily_fund.get((cname, score_date))
        if fund and fund['total'] >= 5:
            fund_total += 1
            fund_avg_net.append(fund['avg_big_net_pct'])
            if fund['inflow_ratio'] > 0.6:
                fund_inflow += 1
            elif fund['inflow_ratio'] < 0.4:
                fund_outflow += 1
    
    # 概念涨跌比
    if concept_total > 0:
        d['_concept_up_ratio'] = concept_up / concept_total
        d['_concept_down_ratio'] = concept_down / concept_total
        d['_concept_avg_chg'] = sum(concept_avg_chg) / len(concept_avg_chg)
        d['_concept_vote'] = concept_up - concept_down
        d['_concept_total'] = concept_total
    else:
        d['_concept_up_ratio'] = 0.5
        d['_concept_down_ratio'] = 0.5
        d['_concept_avg_chg'] = 0
        d['_concept_vote'] = 0
        d['_concept_total'] = 0
    
    # top概念信号
    if top_total > 0:
        d['_top_concept_up'] = top_up
        d['_top_concept_down'] = top_down
        d['_top_concept_avg_chg'] = sum(top_avg_chg) / len(top_avg_chg)
    else:
        d['_top_concept_up'] = 0
        d['_top_concept_down'] = 0
        d['_top_concept_avg_chg'] = 0
    
    # 概念资金流
    if fund_total > 0:
        d['_concept_fund_inflow_ratio'] = fund_inflow / fund_total
        d['_concept_fund_outflow_ratio'] = fund_outflow / fund_total
        d['_concept_fund_avg_net'] = sum(fund_avg_net) / len(fund_avg_net)
        d['_concept_fund_vote'] = fund_inflow - fund_outflow
    else:
        d['_concept_fund_inflow_ratio'] = 0.5
        d['_concept_fund_outflow_ratio'] = 0.5
        d['_concept_fund_avg_net'] = 0
        d['_concept_fund_vote'] = 0

print(f"概念信号计算完成")

# ═══════════════════════════════════════════════════════════
# 分析1: 概念信号各维度的方向预测力（按板块）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析1: 概念信号各维度的方向预测力")
print(f"{'=' * 80}")

# 概念涨跌比 → 次日方向
print(f"\n── 概念涨跌比 → 次日方向 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    
    # 概念多数看涨(up_ratio > 0.5)
    c_bullish = [d for d in sec_data if d['_concept_up_ratio'] > 0.5 and d['_concept_total'] >= 3]
    c_bearish = [d for d in sec_data if d['_concept_down_ratio'] > 0.5 and d['_concept_total'] >= 3]
    c_neutral = [d for d in sec_data if d['_concept_up_ratio'] <= 0.5 and d['_concept_down_ratio'] <= 0.5 and d['_concept_total'] >= 3]
    
    for label, group in [('概念看涨', c_bullish), ('概念看跌', c_bearish), ('概念中性', c_neutral)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            le0 = sum(1 for d in group if d['_le0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%) "
                  f"次日<=0%={le0}({le0/len(group)*100:.1f}%)")

# 概念资金流 → 次日方向
print(f"\n── 概念资金流 → 次日方向 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    
    f_inflow = [d for d in sec_data if d['_concept_fund_inflow_ratio'] > 0.5]
    f_outflow = [d for d in sec_data if d['_concept_fund_outflow_ratio'] > 0.5]
    
    for label, group in [('概念资金流入', f_inflow), ('概念资金流出', f_outflow)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")

# 概念平均涨跌 → 次日方向（反转效应）
print(f"\n── 概念平均涨跌 → 次日方向（反转效应）──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_concept_total'] >= 3]
    
    c_up_strong = [d for d in sec_data if d['_concept_avg_chg'] > 1.0]
    c_up_mild = [d for d in sec_data if 0 < d['_concept_avg_chg'] <= 1.0]
    c_down_mild = [d for d in sec_data if -1.0 <= d['_concept_avg_chg'] < 0]
    c_down_strong = [d for d in sec_data if d['_concept_avg_chg'] < -1.0]
    
    for label, group in [('概念均涨>1%', c_up_strong), ('概念均涨0~1%', c_up_mild),
                          ('概念均跌-1~0%', c_down_mild), ('概念均跌<-1%', c_down_strong)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析2: 概念信号 × 行业同行信号 组合效果
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析2: 概念信号 × 行业同行信号 组合效果")
print(f"{'=' * 80}")

def get_concept_dir(d):
    if d['_concept_up_ratio'] > 0.5 and d['_concept_total'] >= 3:
        return '概念涨'
    elif d['_concept_down_ratio'] > 0.5 and d['_concept_total'] >= 3:
        return '概念跌'
    return '概念中'

def get_peer_dir(d):
    p = d.get('同行信号', 0)
    if p > 0.5: return '同行涨'
    if p < -0.5: return '同行跌'
    return '同行中'

def get_concept_fund_dir(d):
    if d['_concept_fund_inflow_ratio'] > 0.5:
        return '概念流入'
    elif d['_concept_fund_outflow_ratio'] > 0.5:
        return '概念流出'
    return '概念流中'

for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    print(f"\n  {sec}:")
    
    combos = defaultdict(lambda: {'n': 0, 'ge0': 0, 'le0': 0, 'model_ok': 0})
    for d in sec_data:
        cd = get_concept_dir(d)
        pd = get_peer_dir(d)
        key = f"{cd}+{pd}"
        combos[key]['n'] += 1
        if d['_ge0']: combos[key]['ge0'] += 1
        if d['_le0']: combos[key]['le0'] += 1
        if d['宽松正确'] == '✓': combos[key]['model_ok'] += 1
    
    for key in sorted(combos.keys()):
        s = combos[key]
        if s['n'] >= 10:
            best = max(s['ge0'], s['le0'])
            best_dir = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            model_rate = s['model_ok'] / s['n'] * 100
            optimal_rate = best / s['n'] * 100
            print(f"    {key}(n={s['n']}): 最优→{best_dir} {optimal_rate:.1f}% | 当前模型 {model_rate:.1f}%")

# ═══════════════════════════════════════════════════════════
# 分析3: 概念信号 × combined信号 组合效果
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析3: 概念信号 × combined信号 组合效果")
print(f"{'=' * 80}")

def get_combined_dir(d):
    c = d.get('融合信号', 0)
    if c > 0.5: return 'comb+'
    if c < -0.5: return 'comb-'
    return 'comb0'

for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    print(f"\n  {sec}:")
    
    combos = defaultdict(lambda: {'n': 0, 'ge0': 0, 'le0': 0, 'model_ok': 0})
    for d in sec_data:
        cd = get_concept_dir(d)
        cb = get_combined_dir(d)
        key = f"{cd}+{cb}"
        combos[key]['n'] += 1
        if d['_ge0']: combos[key]['ge0'] += 1
        if d['_le0']: combos[key]['le0'] += 1
        if d['宽松正确'] == '✓': combos[key]['model_ok'] += 1
    
    for key in sorted(combos.keys()):
        s = combos[key]
        if s['n'] >= 10:
            best = max(s['ge0'], s['le0'])
            best_dir = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            model_rate = s['model_ok'] / s['n'] * 100
            optimal_rate = best / s['n'] * 100
            gap = optimal_rate - model_rate
            marker = '★' if gap > 5 else ''
            print(f"    {key}(n={s['n']}): 最优→{best_dir} {optimal_rate:.1f}% | 模型 {model_rate:.1f}% | gap={gap:+.1f}pp {marker}")

# ═══════════════════════════════════════════════════════════
# 分析4: 概念资金流反转 × 板块 × combined 三维组合
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析4: 概念资金流 × combined 三维组合")
print(f"{'=' * 80}")

for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    print(f"\n  {sec}:")
    
    combos = defaultdict(lambda: {'n': 0, 'ge0': 0, 'le0': 0, 'model_ok': 0})
    for d in sec_data:
        fd = get_concept_fund_dir(d)
        cb = get_combined_dir(d)
        key = f"{fd}+{cb}"
        combos[key]['n'] += 1
        if d['_ge0']: combos[key]['ge0'] += 1
        if d['_le0']: combos[key]['le0'] += 1
        if d['宽松正确'] == '✓': combos[key]['model_ok'] += 1
    
    for key in sorted(combos.keys()):
        s = combos[key]
        if s['n'] >= 10:
            best = max(s['ge0'], s['le0'])
            best_dir = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            model_rate = s['model_ok'] / s['n'] * 100
            optimal_rate = best / s['n'] * 100
            gap = optimal_rate - model_rate
            marker = '★' if gap > 5 else ''
            print(f"    {key}(n={s['n']}): 最优→{best_dir} {optimal_rate:.1f}% | 模型 {model_rate:.1f}% | gap={gap:+.1f}pp {marker}")

# ═══════════════════════════════════════════════════════════
# 分析5: top概念（最相关概念）的预测力
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析5: top概念（最相关概念）的预测力")
print(f"{'=' * 80}")

for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    
    # top概念看涨 vs 看跌
    top_up = [d for d in sec_data if d['_top_concept_up'] > d['_top_concept_down']]
    top_down = [d for d in sec_data if d['_top_concept_down'] > d['_top_concept_up']]
    top_neutral = [d for d in sec_data if d['_top_concept_up'] == d['_top_concept_down']]
    
    for label, group in [('top概念涨', top_up), ('top概念跌', top_down), ('top概念中', top_neutral)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析6: 在样本级别模拟最优概念修正策略
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析6: 模拟最优概念修正策略（in-sample上限）")
print(f"{'=' * 80}")

# 策略: 对每个(板块, concept_dir, combined_dir)组合，选择最优方向
# 这是in-sample上限
total_optimal = 0
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    
    combos = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in sec_data:
        cd = get_concept_dir(d)
        cb = get_combined_dir(d)
        key = f"{cd}_{cb}"
        combos[key]['n'] += 1
        if d['_ge0']: combos[key]['ge0'] += 1
        if d['_le0']: combos[key]['le0'] += 1
    
    ok = sum(max(s['ge0'], s['le0']) for s in combos.values())
    total_optimal += ok
    print(f"  {sec}: {ok}/{n} ({ok/n*100:.1f}%)")

print(f"  总计(in-sample): {total_optimal}/{total} ({total_optimal/total*100:.1f}%)")

# 加入概念资金流
total_optimal2 = 0
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    
    combos = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in sec_data:
        cd = get_concept_dir(d)
        fd = get_concept_fund_dir(d)
        cb = get_combined_dir(d)
        key = f"{cd}_{fd}_{cb}"
        combos[key]['n'] += 1
        if d['_ge0']: combos[key]['ge0'] += 1
        if d['_le0']: combos[key]['le0'] += 1
    
    ok = sum(max(s['ge0'], s['le0']) for s in combos.values())
    total_optimal2 += ok
    print(f"  {sec}(+fund): {ok}/{n} ({ok/n*100:.1f}%)")

print(f"  总计(+fund in-sample): {total_optimal2}/{total} ({total_optimal2/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析7: 时间序列验证（前半训练→后半测试）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析7: 时间序列验证（前半训练→后半测试）")
print(f"{'=' * 80}")

mid_date = all_dates[len(all_dates) // 2]
train = [d for d in details if d['评分日'] <= mid_date]
test = [d for d in details if d['评分日'] > mid_date]
print(f"训练集: {len(train)}, 测试集: {len(test)}")

# 策略1: sector + concept_dir + combined_dir
for strategy_name, key_fn in [
    ('sector+concept+combined', lambda d: f"{d['板块']}_{get_concept_dir(d)}_{get_combined_dir(d)}"),
    ('sector+concept_fund+combined', lambda d: f"{d['板块']}_{get_concept_fund_dir(d)}_{get_combined_dir(d)}"),
    ('sector+concept+peer', lambda d: f"{d['板块']}_{get_concept_dir(d)}_{get_peer_dir(d)}"),
    ('sector+concept+concept_fund', lambda d: f"{d['板块']}_{get_concept_dir(d)}_{get_concept_fund_dir(d)}"),
    ('sector+combined(baseline)', lambda d: f"{d['板块']}_{get_combined_dir(d)}"),
    ('sector+concept+combined+wd', lambda d: f"{d['板块']}_{get_concept_dir(d)}_{get_combined_dir(d)}_{d['_wd']}"),
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
    
    print(f"  {strategy_name}: {ok}/{len(test)} ({ok/len(test)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析8: 留一日交叉验证
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析8: 留一日交叉验证")
print(f"{'=' * 80}")

for strategy_name, key_fn in [
    ('sector+combined(baseline)', lambda d: f"{d['板块']}_{get_combined_dir(d)}"),
    ('sector+concept+combined', lambda d: f"{d['板块']}_{get_concept_dir(d)}_{get_combined_dir(d)}"),
    ('sector+concept_fund+combined', lambda d: f"{d['板块']}_{get_concept_fund_dir(d)}_{get_combined_dir(d)}"),
    ('sector+concept+peer', lambda d: f"{d['板块']}_{get_concept_dir(d)}_{get_peer_dir(d)}"),
]:
    total_ok = 0
    for test_date in all_dates:
        train_data = [d for d in details if d['评分日'] != test_date]
        test_data = [d for d in details if d['评分日'] == test_date]
        
        stats = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
        for d in train_data:
            key = key_fn(d)
            stats[key]['n'] += 1
            if d['_ge0']: stats[key]['ge0'] += 1
            if d['_le0']: stats[key]['le0'] += 1
        
        for d in test_data:
            key = key_fn(d)
            s = stats.get(key, {'ge0': 1, 'le0': 1, 'n': 2})
            pred = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                total_ok += 1
    
    print(f"  {strategy_name}: {total_ok}/{total} ({total_ok/total*100:.1f}%)")
