#!/usr/bin/env python3
"""
深度分析v20b：概念板块数据对周预测的增强分析

目标：分析概念板块信号能否提升周预测策略B和C的准确率

数据源（全部本地，无需DB）：
1. 50只股票的日频回测结果（backtest_prediction_enhanced_v9_50stocks_result.json）
2. 概念板块映射（stock_boards_map.json）
3. 用回测中50只股票的实际涨跌构建概念板块代理信号

分析维度：
Part 1: 数据准备 — 加载回测结果 + 概念板块映射
Part 2: 概念板块代理信号构建
Part 3: 概念信号与周涨跌的相关性分析
Part 4: 概念信号对策略B的增强效果
Part 5: 概念信号对策略C的增强效果
Part 6: LOWO交叉验证
Part 7: 前半→后半测试
Part 8: 概念信号最有效场景分析
Part 9: 总结
"""
import json
import numpy as np
from datetime import datetime
from collections import defaultdict

# ══════════════════════════════════════════════════════════════
# Part 1: 数据准备
# ══════════════════════════════════════════════════════════════
print("=" * 70)
print("  v20b 概念板块 × 周预测增强分析")
print("=" * 70)

# 1a. 加载50只股票回测结果
with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    bt_data = json.load(f)
details = bt_data['逐日详情']

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_dt'] = datetime.strptime(d['评分日'], '%Y-%m-%d')
    d['_wd'] = d['_dt'].weekday()
    d['_iso_week'] = d['_dt'].isocalendar()[:2]

stock_codes = sorted(set(d['代码'] for d in details))
print(f"回测股票数: {len(stock_codes)}")
print(f"回测记录数: {len(details)}")

# 1b. 加载概念板块映射
with open('data_results/industry_analysis/stock_boards_map.json') as f:
    boards_data = json.load(f)

stock_concepts = {}  # code -> [concept_name, ...]
concept_stocks = {}  # concept_name -> [code, ...]

for code, info in boards_data['stocks'].items():
    cbs = info.get('concept_boards', [])
    if cbs:
        stock_concepts[code] = cbs
    for cb in cbs:
        if cb not in concept_stocks:
            concept_stocks[cb] = []
        concept_stocks[cb].append(code)

# 只保留回测股票集合中的映射
bt_codes = set(stock_codes)
n_with_concept = sum(1 for c in stock_codes if c in stock_concepts)
print(f"有概念板块数据的股票: {n_with_concept}/{len(stock_codes)}")

# 统计概念板块覆盖
concept_coverage = {}
for code in stock_codes:
    cbs = stock_concepts.get(code, [])
    for cb in cbs:
        # 该概念板块中有多少只股票在回测集中
        peers_in_bt = [c for c in concept_stocks.get(cb, []) if c in bt_codes and c != code]
        if cb not in concept_coverage:
            concept_coverage[cb] = {'total': len(concept_stocks.get(cb, [])), 'in_bt': set()}
        concept_coverage[cb]['in_bt'].add(code)
        concept_coverage[cb]['in_bt'].update(peers_in_bt)

useful_concepts = {k: v for k, v in concept_coverage.items() if len(v['in_bt']) >= 3}
print(f"概念板块总数: {len(concept_coverage)}")
print(f"有≥3只回测股票的概念板块: {len(useful_concepts)}")

avg_concepts = np.mean([len(stock_concepts.get(c, [])) for c in stock_codes])
print(f"平均每只股票所属概念板块数: {avg_concepts:.1f}")

# 1c. 构建每日每股票的涨跌数据索引
stock_date_chg = {}  # (code, date) -> actual_change_pct
for d in details:
    stock_date_chg[(d['代码'], d['评分日'])] = d['_actual']

all_dates = sorted(set(d['评分日'] for d in details))
print(f"回测日期范围: {all_dates[0]} ~ {all_dates[-1]}, 共{len(all_dates)}个交易日")


# ══════════════════════════════════════════════════════════════
# Part 2: 概念板块代理信号构建
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 2: 概念板块代理信号构建")
print(f"{'='*70}")

def compute_concept_proxy_signals(code, date_str, lookback=5):
    """用回测集中同概念板块股票的实际涨跌构建概念信号。
    
    对每个概念板块，取该板块中其他回测股票在date_str及之前lookback天的平均涨跌。
    然后对所有概念板块取平均。
    
    返回:
        dict: {
            'concept_momentum': 概念板块平均动量,
            'concept_consensus': 看涨概念板块占比,
            'concept_peer_chg': 概念同行平均涨跌,
            'n_concepts': 有效概念板块数,
            'n_peers': 有效同行股票数,
        }
    """
    concepts = stock_concepts.get(code, [])
    if not concepts:
        return None
    
    # 找到date_str在all_dates中的位置
    try:
        date_idx = all_dates.index(date_str)
    except ValueError:
        return None
    
    # lookback天的日期
    lb_start = max(0, date_idx - lookback + 1)
    lb_dates = all_dates[lb_start:date_idx + 1]
    
    board_momentums = []
    board_peer_chgs = []
    board_ups = 0
    board_total = 0
    all_peers = set()
    
    for cb in concepts:
        # 该概念板块中的其他回测股票
        peers = [c for c in concept_stocks.get(cb, []) if c in bt_codes and c != code]
        if len(peers) < 2:
            continue
        
        # 计算这些peer在lookback期间的平均涨跌
        peer_chgs = []
        for d in lb_dates:
            day_chgs = [stock_date_chg.get((p, d), None) for p in peers]
            day_chgs = [c for c in day_chgs if c is not None]
            if day_chgs:
                peer_chgs.append(np.mean(day_chgs))
        
        if not peer_chgs:
            continue
        
        board_total += 1
        avg_momentum = np.mean(peer_chgs)
        board_momentums.append(avg_momentum)
        all_peers.update(peers)
        
        if avg_momentum > 0:
            board_ups += 1
        
        # 当天的概念同行涨跌
        today_chgs = [stock_date_chg.get((p, date_str), None) for p in peers]
        today_chgs = [c for c in today_chgs if c is not None]
        if today_chgs:
            board_peer_chgs.append(np.mean(today_chgs))
    
    if board_total == 0:
        return None
    
    return {
        'concept_momentum': np.mean(board_momentums),
        'concept_consensus': board_ups / board_total,
        'concept_peer_chg': np.mean(board_peer_chgs) if board_peer_chgs else 0,
        'n_concepts': board_total,
        'n_peers': len(all_peers),
    }

# 预计算所有(stock, date)的概念信号
print("预计算概念信号...")
concept_signal_cache = {}
total_pairs = 0
valid_pairs = 0
for d in details:
    key = (d['代码'], d['评分日'])
    if key not in concept_signal_cache:
        sig = compute_concept_proxy_signals(d['代码'], d['评分日'], lookback=5)
        concept_signal_cache[key] = sig
        total_pairs += 1
        if sig:
            valid_pairs += 1

print(f"计算完成: {valid_pairs}/{total_pairs} 有效概念信号 ({round(valid_pairs/total_pairs*100,1)}%)")

# 也预计算lookback=3的版本（用于策略C的周三信号）
concept_signal_cache_3d = {}
for d in details:
    key = (d['代码'], d['评分日'])
    if key not in concept_signal_cache_3d:
        sig = compute_concept_proxy_signals(d['代码'], d['评分日'], lookback=3)
        concept_signal_cache_3d[key] = sig

print("3日lookback版本计算完成")


# ══════════════════════════════════════════════════════════════
# Part 2b: 构建周数据 + 概念信号
# ══════════════════════════════════════════════════════════════
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
    wup = wchg >= 0
    
    d0 = days[0]
    sector = d0['板块']
    
    rec = {
        'code': code, 'sector': sector, 'iw': iw,
        'n': len(days), 'wchg': wchg, 'wup': wup,
        'mon_actual': d0['_actual'],
        'mon_comb': d0['融合信号'],
        'd3_chg': sum(d['_actual'] for d in days[:min(3, len(days))]),
    }
    
    # 周一概念信号（5日lookback）
    mon_sig = concept_signal_cache.get((code, d0['评分日']))
    if mon_sig:
        for k, v in mon_sig.items():
            rec[f'cm_{k}'] = v
        rec['has_concept'] = True
    else:
        rec['has_concept'] = False
    
    # 周三概念信号（3日lookback）
    if len(days) >= 3:
        wed_sig = concept_signal_cache_3d.get((code, days[2]['评分日']))
        if wed_sig:
            for k, v in wed_sig.items():
                rec[f'cw_{k}'] = v
            rec['has_concept_wed'] = True
        else:
            rec['has_concept_wed'] = False
    else:
        rec['has_concept_wed'] = False
    
    weekly.append(rec)

nw = len(weekly)
n_with_concept = sum(1 for r in weekly if r.get('has_concept'))
n_with_concept_wed = sum(1 for r in weekly if r.get('has_concept_wed'))
sorted_weeks = sorted(set(r['iw'] for r in weekly))
mid = len(sorted_weeks) // 2
first_half_weeks = set(sorted_weeks[:mid])
second_half_weeks = set(sorted_weeks[mid:])
train = [r for r in weekly if r['iw'] in first_half_weeks]
test = [r for r in weekly if r['iw'] in second_half_weeks]

print(f"\n周样本总数: {nw}")
print(f"有周一概念信号: {n_with_concept} ({round(n_with_concept/nw*100,1)}%)")
print(f"有周三概念信号: {n_with_concept_wed} ({round(n_with_concept_wed/nw*100,1)}%)")
print(f"训练集: {len(train)}, 测试集: {len(test)}")


# ══════════════════════════════════════════════════════════════
# Part 3: 概念信号与周涨跌的相关性分析
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 3: 概念信号与周涨跌的相关性分析")
print(f"{'='*70}")

concept_records = [r for r in weekly if r.get('has_concept')]

concept_features = [
    ('cm_concept_momentum', '概念动量(5日均涨跌)'),
    ('cm_concept_consensus', '概念共识度(看涨占比)'),
    ('cm_concept_peer_chg', '概念同行当日涨跌'),
]

print(f"\n周一概念信号与周涨跌的相关性 ({len(concept_records)} 样本):")
print(f"  {'信号':<26} {'相关系数':>8} {'涨时均值':>10} {'跌时均值':>10} {'差异':>8}")
print(f"  {'-'*66}")

for feat_key, feat_name in concept_features:
    vals = [r.get(feat_key, 0) for r in concept_records]
    wchgs = [r['wchg'] for r in concept_records]
    
    corr = np.corrcoef(vals, wchgs)[0, 1] if len(vals) > 2 else 0
    up_vals = [r.get(feat_key, 0) for r in concept_records if r['wup']]
    dn_vals = [r.get(feat_key, 0) for r in concept_records if not r['wup']]
    
    up_mean = np.mean(up_vals) if up_vals else 0
    dn_mean = np.mean(dn_vals) if dn_vals else 0
    print(f"  {feat_name:<26} {corr:>8.4f} {up_mean:>10.4f} {dn_mean:>10.4f} {up_mean-dn_mean:>8.4f}")

# 概念共识度分组
print(f"\n概念共识度分组 vs 周涨概率:")
print(f"  {'共识度区间':<14} {'样本':>6} {'周涨':>6} {'周涨率':>8} {'平均周涨跌':>10}")
print(f"  {'-'*48}")
for lo, hi, label in [(0, 0.3, '<30%'), (0.3, 0.5, '30-50%'), (0.5, 0.7, '50-70%'), (0.7, 1.01, '≥70%')]:
    grp = [r for r in concept_records if lo <= r.get('cm_concept_consensus', 0.5) < hi]
    if not grp:
        continue
    n_up = sum(1 for r in grp if r['wup'])
    avg_wchg = np.mean([r['wchg'] for r in grp])
    print(f"  {label:<14} {len(grp):>6} {n_up:>6} {round(n_up/len(grp)*100,1):>7.1f}% {avg_wchg:>9.2f}%")

# 概念动量分组
print(f"\n概念动量分组 vs 周涨概率:")
print(f"  {'动量区间':<14} {'样本':>6} {'周涨':>6} {'周涨率':>8} {'平均周涨跌':>10}")
print(f"  {'-'*48}")
for lo, hi, label in [(-99, -0.5, '<-0.5%'), (-0.5, 0, '-0.5~0%'), (0, 0.5, '0~0.5%'), (0.5, 99, '>0.5%')]:
    grp = [r for r in concept_records if lo <= r.get('cm_concept_momentum', 0) < hi]
    if not grp:
        continue
    n_up = sum(1 for r in grp if r['wup'])
    avg_wchg = np.mean([r['wchg'] for r in grp])
    print(f"  {label:<14} {len(grp):>6} {n_up:>6} {round(n_up/len(grp)*100,1):>7.1f}% {avg_wchg:>9.2f}%")


# ══════════════════════════════════════════════════════════════
# Part 4: 概念信号对策略B的增强效果
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 4: 概念信号对策略B的增强效果")
print(f"{'='*70}")

# 板块基准率
sector_up_rate = {}
for sec in set(r['sector'] for r in weekly):
    sr = [r for r in weekly if r['sector'] == sec]
    sector_up_rate[sec] = sum(1 for r in sr if r['wup']) / len(sr)

def strat_B_orig(r):
    if r['mon_actual'] > 0.5: return True
    elif r['mon_actual'] < -0.5: return False
    else: return sector_up_rate.get(r['sector'], 0.5) > 0.5

def strat_B_cv1(r):
    """模糊区用概念共识度"""
    if r['mon_actual'] > 0.5: return True
    elif r['mon_actual'] < -0.5: return False
    if r.get('has_concept'):
        return r.get('cm_concept_consensus', 0.5) > 0.5
    return sector_up_rate.get(r['sector'], 0.5) > 0.5

def strat_B_cv2(r):
    """概念动量修正强信号"""
    if r['mon_actual'] > 0.5:
        if r.get('has_concept') and r.get('cm_concept_momentum', 0) < -0.5:
            return False  # 周一涨但概念弱→反转
        return True
    elif r['mon_actual'] < -0.5:
        if r.get('has_concept') and r.get('cm_concept_momentum', 0) > 0.5:
            return True  # 周一跌但概念强→反弹
        return False
    if r.get('has_concept'):
        return r.get('cm_concept_consensus', 0.5) > 0.5
    return sector_up_rate.get(r['sector'], 0.5) > 0.5

def strat_B_cv3(r):
    """综合概念评分"""
    cs = 0
    if r.get('has_concept'):
        if r.get('cm_concept_consensus', 0.5) > 0.65: cs += 1
        elif r.get('cm_concept_consensus', 0.5) < 0.35: cs -= 1
        if r.get('cm_concept_momentum', 0) > 0.3: cs += 1
        elif r.get('cm_concept_momentum', 0) < -0.3: cs -= 1
        if r.get('cm_concept_peer_chg', 0) > 0.3: cs += 1
        elif r.get('cm_concept_peer_chg', 0) < -0.3: cs -= 1
    
    if r['mon_actual'] > 0.5:
        return cs > -2  # 只有极端看空才反转
    elif r['mon_actual'] < -0.5:
        return cs >= 2  # 只有极端看多才反转
    if cs > 0: return True
    elif cs < 0: return False
    return sector_up_rate.get(r['sector'], 0.5) > 0.5

def eval_strat(records, fn, label=""):
    ok = sum(1 for r in records if (fn(r) and r['wup']) or (not fn(r) and not r['wup']))
    n = len(records)
    return ok, n, round(ok/n*100, 1) if n > 0 else 0

strats_B = [
    ('B原始', strat_B_orig),
    ('B+概念v1(共识替代)', strat_B_cv1),
    ('B+概念v2(动量修正)', strat_B_cv2),
    ('B+概念v3(综合评分)', strat_B_cv3),
]

print(f"\n全样本准确率 ({nw} 周):")
print(f"  {'策略':<24} {'准确':>6}/{nw:<6} {'准确率':>8}")
print(f"  {'-'*46}")
for name, fn in strats_B:
    ok, n, acc = eval_strat(weekly, fn)
    print(f"  {name:<24} {ok:>6}/{n:<6} {acc:>7.1f}%")

print(f"\n仅有概念数据的样本 ({n_with_concept}):")
print(f"  {'策略':<24} {'准确':>6}/{n_with_concept:<6} {'准确率':>8}")
print(f"  {'-'*46}")
for name, fn in strats_B:
    ok, n, acc = eval_strat(concept_records, fn)
    print(f"  {name:<24} {ok:>6}/{n:<6} {acc:>7.1f}%")


# ══════════════════════════════════════════════════════════════
# Part 5: 概念信号对策略C的增强效果
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 5: 概念信号对策略C的增强效果")
print(f"{'='*70}")

def strat_C_orig(r):
    return r['d3_chg'] > 0

def strat_C_cv1(r):
    """模糊区用周三概念共识度"""
    if abs(r['d3_chg']) > 1.0:
        return r['d3_chg'] > 0
    if r.get('has_concept_wed'):
        return r.get('cw_concept_consensus', 0.5) > 0.5
    return r['d3_chg'] > 0

def strat_C_cv2(r):
    """概念动量修正边界"""
    if r['d3_chg'] > 1.0: return True
    elif r['d3_chg'] < -1.0: return False
    if r.get('has_concept_wed'):
        mom = r.get('cw_concept_momentum', 0)
        if mom > 0.2: return True
        elif mom < -0.2: return False
    return r['d3_chg'] > 0

def strat_C_cv3(r):
    """概念反转修正"""
    pred_up = r['d3_chg'] > 0
    if not r.get('has_concept_wed'):
        return pred_up
    consensus = r.get('cw_concept_consensus', 0.5)
    momentum = r.get('cw_concept_momentum', 0)
    if pred_up and consensus < 0.25 and momentum < -0.3:
        return False
    if not pred_up and consensus > 0.75 and momentum > 0.3:
        return True
    return pred_up

def strat_C_cv4(r):
    """综合概念评分修正"""
    cs = 0
    if r.get('has_concept_wed'):
        if r.get('cw_concept_consensus', 0.5) > 0.6: cs += 1
        elif r.get('cw_concept_consensus', 0.5) < 0.4: cs -= 1
        if r.get('cw_concept_momentum', 0) > 0.2: cs += 1
        elif r.get('cw_concept_momentum', 0) < -0.2: cs -= 1
    
    if abs(r['d3_chg']) > 1.5:
        return r['d3_chg'] > 0
    if abs(r['d3_chg']) <= 0.5:
        if cs > 0: return True
        elif cs < 0: return False
    if r['d3_chg'] > 0 and cs <= -2: return False
    if r['d3_chg'] <= 0 and cs >= 2: return True
    return r['d3_chg'] > 0

strats_C = [
    ('C原始', strat_C_orig),
    ('C+概念v1(模糊区共识)', strat_C_cv1),
    ('C+概念v2(动量修正)', strat_C_cv2),
    ('C+概念v3(反转修正)', strat_C_cv3),
    ('C+概念v4(综合修正)', strat_C_cv4),
]

print(f"\n全样本准确率 ({nw} 周):")
print(f"  {'策略':<24} {'准确':>6}/{nw:<6} {'准确率':>8}")
print(f"  {'-'*46}")
for name, fn in strats_C:
    ok, n, acc = eval_strat(weekly, fn)
    print(f"  {name:<24} {ok:>6}/{n:<6} {acc:>7.1f}%")

# 按板块对比
print(f"\n按板块 — B原始 vs 最佳B+概念:")
print(f"  {'板块':<10} {'样本':>5} {'B原始':>8} {'B+v3':>8} {'C原始':>8} {'C+v4':>8}")
print(f"  {'-'*52}")
for sec in sorted(set(r['sector'] for r in weekly)):
    sr = [r for r in weekly if r['sector'] == sec]
    sn = len(sr)
    b0 = sum(1 for r in sr if (strat_B_orig(r) and r['wup']) or (not strat_B_orig(r) and not r['wup']))
    b3 = sum(1 for r in sr if (strat_B_cv3(r) and r['wup']) or (not strat_B_cv3(r) and not r['wup']))
    c0 = sum(1 for r in sr if (strat_C_orig(r) and r['wup']) or (not strat_C_orig(r) and not r['wup']))
    c4 = sum(1 for r in sr if (strat_C_cv4(r) and r['wup']) or (not strat_C_cv4(r) and not r['wup']))
    print(f"  {sec:<10} {sn:>5} {round(b0/sn*100,1):>7.1f}% {round(b3/sn*100,1):>7.1f}% "
          f"{round(c0/sn*100,1):>7.1f}% {round(c4/sn*100,1):>7.1f}%")


# ══════════════════════════════════════════════════════════════
# Part 6: LOWO交叉验证
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 6: LOWO交叉验证 — 无泄露验证")
print(f"{'='*70}")

def lowo_cv(records, predict_fn, sorted_wks):
    total_ok = 0
    total_n = 0
    for hold_wk in sorted_wks:
        tr = [r for r in records if r['iw'] != hold_wk]
        te = [r for r in records if r['iw'] == hold_wk]
        if not te or len(tr) < 10:
            continue
        tr_sur = {}
        for sec in set(r['sector'] for r in tr):
            sr = [r for r in tr if r['sector'] == sec]
            tr_sur[sec] = sum(1 for r in sr if r['wup']) / len(sr)
        for r in te:
            pred = predict_fn(r, tr_sur)
            if (pred and r['wup']) or (not pred and not r['wup']):
                total_ok += 1
            total_n += 1
    return total_ok / total_n * 100 if total_n > 0 else 0, total_n

def lw_B_orig(r, sur):
    if r['mon_actual'] > 0.5: return True
    elif r['mon_actual'] < -0.5: return False
    return sur.get(r['sector'], 0.5) > 0.5

def lw_B_cv3(r, sur):
    cs = 0
    if r.get('has_concept'):
        if r.get('cm_concept_consensus', 0.5) > 0.65: cs += 1
        elif r.get('cm_concept_consensus', 0.5) < 0.35: cs -= 1
        if r.get('cm_concept_momentum', 0) > 0.3: cs += 1
        elif r.get('cm_concept_momentum', 0) < -0.3: cs -= 1
        if r.get('cm_concept_peer_chg', 0) > 0.3: cs += 1
        elif r.get('cm_concept_peer_chg', 0) < -0.3: cs -= 1
    if r['mon_actual'] > 0.5: return cs > -2
    elif r['mon_actual'] < -0.5: return cs >= 2
    if cs > 0: return True
    elif cs < 0: return False
    return sur.get(r['sector'], 0.5) > 0.5

def lw_C_orig(r, sur):
    return r['d3_chg'] > 0

def lw_C_cv4(r, sur):
    cs = 0
    if r.get('has_concept_wed'):
        if r.get('cw_concept_consensus', 0.5) > 0.6: cs += 1
        elif r.get('cw_concept_consensus', 0.5) < 0.4: cs -= 1
        if r.get('cw_concept_momentum', 0) > 0.2: cs += 1
        elif r.get('cw_concept_momentum', 0) < -0.2: cs -= 1
    if abs(r['d3_chg']) > 1.5: return r['d3_chg'] > 0
    if abs(r['d3_chg']) <= 0.5:
        if cs > 0: return True
        elif cs < 0: return False
    if r['d3_chg'] > 0 and cs <= -2: return False
    if r['d3_chg'] <= 0 and cs >= 2: return True
    return r['d3_chg'] > 0

lowo_strats = [
    ('B原始', lw_B_orig),
    ('B+概念v3', lw_B_cv3),
    ('C原始', lw_C_orig),
    ('C+概念v4', lw_C_cv4),
]

print(f"\nLOWO交叉验证:")
print(f"  {'策略':<16} {'LOWO准确率':>12} {'样本':>8}")
print(f"  {'-'*40}")
for name, fn in lowo_strats:
    acc, n = lowo_cv(weekly, fn, sorted_weeks)
    print(f"  {name:<16} {acc:>11.1f}% {n:>8}")


# ══════════════════════════════════════════════════════════════
# Part 7: 前半→后半测试
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 7: 前半→后半测试 — 泛化能力")
print(f"{'='*70}")

train_sur = {}
for sec in set(r['sector'] for r in train):
    sr = [r for r in train if r['sector'] == sec]
    train_sur[sec] = sum(1 for r in sr if r['wup']) / len(sr)

print(f"\n  {'策略':<16} {'前半':>10} {'后半':>10} {'差异':>8}")
print(f"  {'-'*48}")
for name, fn in lowo_strats:
    tr_ok = sum(1 for r in train if (fn(r, train_sur) and r['wup']) or (not fn(r, train_sur) and not r['wup']))
    te_ok = sum(1 for r in test if (fn(r, train_sur) and r['wup']) or (not fn(r, train_sur) and not r['wup']))
    tr_acc = round(tr_ok/len(train)*100, 1) if train else 0
    te_acc = round(te_ok/len(test)*100, 1) if test else 0
    print(f"  {name:<16} {tr_acc:>9.1f}% {te_acc:>9.1f}% {te_acc-tr_acc:>+7.1f}%")


# ══════════════════════════════════════════════════════════════
# Part 8: 概念信号最有效场景分析
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 8: 概念信号最有效场景分析")
print(f"{'='*70}")

# 8a. B策略模糊区
fuzzy_B = [r for r in concept_records if abs(r['mon_actual']) <= 0.5]
print(f"\nB策略模糊区（周一涨跌±0.5%内）: {len(fuzzy_B)} 样本")
if fuzzy_B:
    b0_ok = sum(1 for r in fuzzy_B if (strat_B_orig(r) and r['wup']) or (not strat_B_orig(r) and not r['wup']))
    b3_ok = sum(1 for r in fuzzy_B if (strat_B_cv3(r) and r['wup']) or (not strat_B_cv3(r) and not r['wup']))
    print(f"  B原始:    {round(b0_ok/len(fuzzy_B)*100,1)}%")
    print(f"  B+概念v3: {round(b3_ok/len(fuzzy_B)*100,1)}%")
    print(f"  提升: {round((b3_ok-b0_ok)/len(fuzzy_B)*100,1):+.1f}%")

# 8b. C策略模糊区
fuzzy_C = [r for r in weekly if abs(r['d3_chg']) <= 1.0 and r.get('has_concept_wed')]
print(f"\nC策略模糊区（前3天涨跌±1%内）: {len(fuzzy_C)} 样本")
if fuzzy_C:
    c0_ok = sum(1 for r in fuzzy_C if (strat_C_orig(r) and r['wup']) or (not strat_C_orig(r) and not r['wup']))
    c4_ok = sum(1 for r in fuzzy_C if (strat_C_cv4(r) and r['wup']) or (not strat_C_cv4(r) and not r['wup']))
    print(f"  C原始:    {round(c0_ok/len(fuzzy_C)*100,1)}%")
    print(f"  C+概念v4: {round(c4_ok/len(fuzzy_C)*100,1)}%")
    print(f"  提升: {round((c4_ok-c0_ok)/len(fuzzy_C)*100,1):+.1f}%")

# 8c. 修正分析
print(f"\n概念信号修正分析（B策略）:")
b_fix = b_break = 0
for r in concept_records:
    o = (strat_B_orig(r) and r['wup']) or (not strat_B_orig(r) and not r['wup'])
    v = (strat_B_cv3(r) and r['wup']) or (not strat_B_cv3(r) and not r['wup'])
    if not o and v: b_fix += 1
    elif o and not v: b_break += 1
print(f"  原始错→概念修正对: {b_fix}")
print(f"  原始对→概念修正错: {b_break}")
print(f"  净改善: {b_fix - b_break}")

print(f"\n概念信号修正分析（C策略）:")
c_fix = c_break = 0
for r in [r for r in weekly if r.get('has_concept_wed')]:
    o = (strat_C_orig(r) and r['wup']) or (not strat_C_orig(r) and not r['wup'])
    v = (strat_C_cv4(r) and r['wup']) or (not strat_C_cv4(r) and not r['wup'])
    if not o and v: c_fix += 1
    elif o and not v: c_break += 1
print(f"  原始错→概念修正对: {c_fix}")
print(f"  原始对→概念修正错: {c_break}")
print(f"  净改善: {c_fix - c_break}")

# 8d. 概念板块数量影响
print(f"\n概念板块数量 vs 信号有效性:")
print(f"  {'概念数':<12} {'样本':>6} {'B原始':>8} {'B+v3':>8} {'差异':>8}")
print(f"  {'-'*46}")
for lo, hi, label in [(1, 3, '1-2'), (3, 6, '3-5'), (6, 10, '6-9'), (10, 99, '≥10')]:
    grp = [r for r in concept_records if lo <= r.get('cm_n_concepts', 0) < hi]
    if len(grp) < 5: continue
    gn = len(grp)
    b0 = sum(1 for r in grp if (strat_B_orig(r) and r['wup']) or (not strat_B_orig(r) and not r['wup']))
    b3 = sum(1 for r in grp if (strat_B_cv3(r) and r['wup']) or (not strat_B_cv3(r) and not r['wup']))
    d = round(b3/gn*100 - b0/gn*100, 1)
    print(f"  {label:<12} {gn:>6} {round(b0/gn*100,1):>7.1f}% {round(b3/gn*100,1):>7.1f}% {d:>+7.1f}%")


# ══════════════════════════════════════════════════════════════
# Part 9: 总结
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 9: 总结")
print(f"{'='*70}")

# 汇总最终结果
print(f"\n最终结果汇总:")
print(f"  数据: {len(stock_codes)}只股票, {nw}周样本, {n_with_concept}有概念信号")
print(f"  概念信号: 基于同概念板块回测股票的实际涨跌构建代理信号")
print(f"  概念维度: 动量(5日均涨跌), 共识度(看涨板块占比), 同行当日涨跌")

print(f"\n  全样本准确率对比:")
for name, fn in strats_B:
    ok, n, acc = eval_strat(weekly, fn)
    print(f"    {name:<24} {acc:.1f}%")
for name, fn in strats_C:
    ok, n, acc = eval_strat(weekly, fn)
    print(f"    {name:<24} {acc:.1f}%")

print(f"\n  说明:")
print(f"  - 概念信号使用回测集内同概念板块股票的实际涨跌作为代理")
print(f"  - 由于回测集仅50只股票，概念板块覆盖有限，信号可能不够精确")
print(f"  - 建议后续接入DB的概念板块K线数据获取更准确的概念信号")
print(f"  - 概念信号主要在'模糊区'（原始策略不确定的区域）发挥作用")

print(f"\n{'='*70}")
print(f"  分析完成")
print(f"{'='*70}")
