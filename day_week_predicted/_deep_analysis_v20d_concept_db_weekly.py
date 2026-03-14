#!/usr/bin/env python3
"""
深度分析v20d：基于DB概念板块K线的周预测增强验证

与v20b的区别：
- v20b使用回测集内同概念股票的实际涨跌作为代理信号（受限于50只股票覆盖）
- v20d直接使用DB中的概念板块K线数据（concept_board_kline表），信号更准确

验证内容：
1. 概念板块数据覆盖率检查
2. 概念信号与周涨跌的相关性
3. 策略B+概念 vs 策略B原始
4. 策略C+概念 vs 策略C原始
5. LOWO交叉验证（无泄露）
6. 按板块分析概念信号有效性
7. 模糊区概念信号修正效果
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import numpy as np
from datetime import datetime
from collections import defaultdict

from service.analysis.concept_weekly_signal import (
    batch_preload_concept_data,
    compute_concept_signal_for_date,
    predict_weekly_B_with_concept,
    predict_weekly_C_with_concept,
)

# ══════════════════════════════════════════════════════════════
# Part 1: 数据准备
# ══════════════════════════════════════════════════════════════
print("=" * 70)
print("  v20d 基于DB概念板块K线的周预测增强验证")
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
all_dates = sorted(set(d['评分日'] for d in details))
print(f"回测股票数: {len(stock_codes)}")
print(f"回测日期范围: {all_dates[0]} ~ {all_dates[-1]}, 共{len(all_dates)}个交易日")

# 1b. 预加载概念板块数据（从DB）
print(f"\n加载概念板块数据...")
concept_data = batch_preload_concept_data(
    stock_codes, all_dates[0], all_dates[-1]
)
stock_boards = concept_data['stock_boards']
board_kline_map = concept_data['board_kline_map']

n_with_boards = len(stock_boards)
n_boards_total = len(set(
    b['board_code'] for boards in stock_boards.values() for b in boards
))
n_boards_with_kline = sum(1 for bc in set(
    b['board_code'] for boards in stock_boards.values() for b in boards
) if bc in board_kline_map)

print(f"有概念板块的股票: {n_with_boards}/{len(stock_codes)}")
print(f"涉及概念板块总数: {n_boards_total}")
print(f"有K线数据的板块: {n_boards_with_kline}/{n_boards_total}")

# 每只股票的概念板块数
for code in stock_codes[:5]:
    boards = stock_boards.get(code, [])
    with_kline = sum(1 for b in boards if b['board_code'] in board_kline_map)
    print(f"  {code}: {len(boards)}个概念板块, {with_kline}个有K线")

# 1c. 构建周数据 + 概念信号
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
        'mon_date': d0['评分日'],
        'wed_date': days[2]['评分日'] if len(days) >= 3 else d0['评分日'],
    }

    # 周一概念信号（5日lookback）
    boards = stock_boards.get(code, [])
    sig_mon = compute_concept_signal_for_date(
        code, d0['评分日'], board_kline_map, boards, lookback=5
    )
    rec['concept_mon'] = sig_mon

    # 周三概念信号（3日lookback）
    if len(days) >= 3:
        sig_wed = compute_concept_signal_for_date(
            code, days[2]['评分日'], board_kline_map, boards, lookback=3
        )
        rec['concept_wed'] = sig_wed
    else:
        rec['concept_wed'] = None

    weekly.append(rec)

nw = len(weekly)
n_cm = sum(1 for r in weekly if r['concept_mon'])
n_cw = sum(1 for r in weekly if r['concept_wed'])
print(f"\n周样本总数: {nw}")
print(f"有周一概念信号: {n_cm} ({round(n_cm/nw*100,1)}%)")
print(f"有周三概念信号: {n_cw} ({round(n_cw/nw*100,1)}%)")


# ══════════════════════════════════════════════════════════════
# Part 2: 概念信号与周涨跌的相关性
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 2: 概念信号与周涨跌的相关性")
print(f"{'='*70}")

concept_records = [r for r in weekly if r['concept_mon']]

features = [
    ('concept_momentum', '概念动量(5日均涨跌)'),
    ('concept_consensus', '概念共识度(看涨占比)'),
    ('concept_strength', '概念强度(5日累计)'),
]

print(f"\n周一概念信号 vs 周涨跌 ({len(concept_records)} 样本):")
print(f"  {'信号':<26} {'相关系数':>8} {'涨时均值':>10} {'跌时均值':>10} {'差异':>8}")
print(f"  {'-'*66}")

for feat_key, feat_name in features:
    vals = [r['concept_mon'][feat_key] for r in concept_records]
    wchgs = [r['wchg'] for r in concept_records]
    corr = np.corrcoef(vals, wchgs)[0, 1] if len(vals) > 2 else 0
    up_vals = [r['concept_mon'][feat_key] for r in concept_records if r['wup']]
    dn_vals = [r['concept_mon'][feat_key] for r in concept_records if not r['wup']]
    up_mean = np.mean(up_vals) if up_vals else 0
    dn_mean = np.mean(dn_vals) if dn_vals else 0
    print(f"  {feat_name:<26} {corr:>8.4f} {up_mean:>10.4f} {dn_mean:>10.4f} "
          f"{up_mean-dn_mean:>8.4f}")

# 概念共识度分组
print(f"\n概念共识度分组 vs 周涨概率:")
print(f"  {'共识度区间':<14} {'样本':>6} {'周涨':>6} {'周涨率':>8} {'平均周涨跌':>10}")
print(f"  {'-'*48}")
for lo, hi, label in [(0, 0.3, '<30%'), (0.3, 0.5, '30-50%'),
                       (0.5, 0.7, '50-70%'), (0.7, 1.01, '≥70%')]:
    grp = [r for r in concept_records
           if lo <= r['concept_mon']['concept_consensus'] < hi]
    if not grp:
        continue
    n_up = sum(1 for r in grp if r['wup'])
    avg_wchg = np.mean([r['wchg'] for r in grp])
    print(f"  {label:<14} {len(grp):>6} {n_up:>6} "
          f"{round(n_up/len(grp)*100,1):>7.1f}% {avg_wchg:>9.2f}%")

# 概念动量分组
print(f"\n概念动量分组 vs 周涨概率:")
print(f"  {'动量区间':<14} {'样本':>6} {'周涨':>6} {'周涨率':>8} {'平均周涨跌':>10}")
print(f"  {'-'*48}")
for lo, hi, label in [(-99, -0.5, '<-0.5%'), (-0.5, 0, '-0.5~0%'),
                       (0, 0.5, '0~0.5%'), (0.5, 99, '>0.5%')]:
    grp = [r for r in concept_records
           if lo <= r['concept_mon']['concept_momentum'] < hi]
    if not grp:
        continue
    n_up = sum(1 for r in grp if r['wup'])
    avg_wchg = np.mean([r['wchg'] for r in grp])
    print(f"  {label:<14} {len(grp):>6} {n_up:>6} "
          f"{round(n_up/len(grp)*100,1):>7.1f}% {avg_wchg:>9.2f}%")


# ══════════════════════════════════════════════════════════════
# Part 3: 策略B+概念 vs 策略B原始
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 3: 策略B+概念 vs 策略B原始")
print(f"{'='*70}")

# 板块基准率
sector_up_rate = {}
for sec in set(r['sector'] for r in weekly):
    sr = [r for r in weekly if r['sector'] == sec]
    sector_up_rate[sec] = sum(1 for r in sr if r['wup']) / len(sr)

def strat_B_orig(r):
    if r['mon_actual'] > 0.5: return True
    elif r['mon_actual'] < -0.5: return False
    return sector_up_rate.get(r['sector'], 0.5) > 0.5

def strat_B_concept(r):
    pred, _ = predict_weekly_B_with_concept(
        r['mon_actual'],
        sector_up_rate.get(r['sector'], 0.5),
        r['concept_mon'],
    )
    return pred

def eval_strat(records, fn, label=""):
    ok = sum(1 for r in records
             if (fn(r) and r['wup']) or (not fn(r) and not r['wup']))
    n = len(records)
    return ok, n, round(ok/n*100, 1) if n > 0 else 0

strats_B = [('B原始', strat_B_orig), ('B+概念(DB)', strat_B_concept)]

print(f"\n全样本准确率 ({nw} 周):")
print(f"  {'策略':<20} {'准确':>6}/{nw:<6} {'准确率':>8}")
print(f"  {'-'*42}")
for name, fn in strats_B:
    ok, n, acc = eval_strat(weekly, fn)
    print(f"  {name:<20} {ok:>6}/{n:<6} {acc:>7.1f}%")

print(f"\n仅有概念数据的样本 ({n_cm}):")
for name, fn in strats_B:
    ok, n, acc = eval_strat(concept_records, fn)
    print(f"  {name:<20} {ok:>6}/{n:<6} {acc:>7.1f}%")


# ══════════════════════════════════════════════════════════════
# Part 4: 策略C+概念 vs 策略C原始
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 4: 策略C+概念 vs 策略C原始")
print(f"{'='*70}")

def strat_C_orig(r):
    return r['d3_chg'] > 0

def strat_C_concept(r):
    pred, _ = predict_weekly_C_with_concept(r['d3_chg'], r['concept_wed'])
    return pred

strats_C = [('C原始', strat_C_orig), ('C+概念(DB)', strat_C_concept)]

print(f"\n全样本准确率 ({nw} 周):")
print(f"  {'策略':<20} {'准确':>6}/{nw:<6} {'准确率':>8}")
print(f"  {'-'*42}")
for name, fn in strats_C:
    ok, n, acc = eval_strat(weekly, fn)
    print(f"  {name:<20} {ok:>6}/{n:<6} {acc:>7.1f}%")

# 按板块对比
print(f"\n按板块对比:")
print(f"  {'板块':<10} {'样本':>5} {'B原始':>8} {'B+概念':>8} "
      f"{'C原始':>8} {'C+概念':>8}")
print(f"  {'-'*56}")
for sec in sorted(set(r['sector'] for r in weekly)):
    sr = [r for r in weekly if r['sector'] == sec]
    sn = len(sr)
    b0 = sum(1 for r in sr if (strat_B_orig(r) and r['wup']) or
             (not strat_B_orig(r) and not r['wup']))
    bc = sum(1 for r in sr if (strat_B_concept(r) and r['wup']) or
             (not strat_B_concept(r) and not r['wup']))
    c0 = sum(1 for r in sr if (strat_C_orig(r) and r['wup']) or
             (not strat_C_orig(r) and not r['wup']))
    cc = sum(1 for r in sr if (strat_C_concept(r) and r['wup']) or
             (not strat_C_concept(r) and not r['wup']))
    print(f"  {sec:<10} {sn:>5} {round(b0/sn*100,1):>7.1f}% "
          f"{round(bc/sn*100,1):>7.1f}% {round(c0/sn*100,1):>7.1f}% "
          f"{round(cc/sn*100,1):>7.1f}%")


# ══════════════════════════════════════════════════════════════
# Part 5: LOWO交叉验证
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 5: LOWO交叉验证（无泄露）")
print(f"{'='*70}")

sorted_weeks = sorted(set(r['iw'] for r in weekly))

def lowo_cv(records, predict_fn):
    total_ok = 0
    total_n = 0
    for hold_wk in sorted_weeks:
        tr = [r for r in records if r['iw'] != hold_wk]
        te = [r for r in records if r['iw'] == hold_wk]
        if not te or len(tr) < 10:
            continue
        # 训练集板块基准率
        tr_sur = {}
        for sec in set(r['sector'] for r in tr):
            sr = [r for r in tr if r['sector'] == sec]
            tr_sur[sec] = sum(1 for r in sr if r['wup']) / len(sr)
        for r in te:
            pred = predict_fn(r, tr_sur)
            if (pred and r['wup']) or (not pred and not r['wup']):
                total_ok += 1
            total_n += 1
    return round(total_ok / total_n * 100, 1) if total_n > 0 else 0, total_n

def lw_B_orig(r, sur):
    if r['mon_actual'] > 0.5: return True
    elif r['mon_actual'] < -0.5: return False
    return sur.get(r['sector'], 0.5) > 0.5

def lw_B_concept(r, sur):
    pred, _ = predict_weekly_B_with_concept(
        r['mon_actual'], sur.get(r['sector'], 0.5), r['concept_mon'],
    )
    return pred

def lw_C_orig(r, sur):
    return r['d3_chg'] > 0

def lw_C_concept(r, sur):
    pred, _ = predict_weekly_C_with_concept(r['d3_chg'], r['concept_wed'])
    return pred

lowo_strats = [
    ('B原始', lw_B_orig),
    ('B+概念(DB)', lw_B_concept),
    ('C原始', lw_C_orig),
    ('C+概念(DB)', lw_C_concept),
]

print(f"\nLOWO交叉验证:")
print(f"  {'策略':<16} {'LOWO准确率':>12} {'样本':>8}")
print(f"  {'-'*40}")
for name, fn in lowo_strats:
    acc, n = lowo_cv(weekly, fn)
    print(f"  {name:<16} {acc:>11.1f}% {n:>8}")


# ══════════════════════════════════════════════════════════════
# Part 6: 前半→后半测试
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 6: 前半→后半测试（泛化能力）")
print(f"{'='*70}")

mid = len(sorted_weeks) // 2
first_half_weeks = set(sorted_weeks[:mid])
second_half_weeks = set(sorted_weeks[mid:])
train = [r for r in weekly if r['iw'] in first_half_weeks]
test = [r for r in weekly if r['iw'] in second_half_weeks]

train_sur = {}
for sec in set(r['sector'] for r in train):
    sr = [r for r in train if r['sector'] == sec]
    train_sur[sec] = sum(1 for r in sr if r['wup']) / len(sr)

print(f"  训练集: {len(train)} 周, 测试集: {len(test)} 周")
print(f"\n  {'策略':<16} {'前半':>10} {'后半':>10} {'差异':>8}")
print(f"  {'-'*48}")
for name, fn in lowo_strats:
    tr_ok = sum(1 for r in train
                if (fn(r, train_sur) and r['wup']) or
                   (not fn(r, train_sur) and not r['wup']))
    te_ok = sum(1 for r in test
                if (fn(r, train_sur) and r['wup']) or
                   (not fn(r, train_sur) and not r['wup']))
    tr_acc = round(tr_ok/len(train)*100, 1) if train else 0
    te_acc = round(te_ok/len(test)*100, 1) if test else 0
    print(f"  {name:<16} {tr_acc:>9.1f}% {te_acc:>9.1f}% "
          f"{te_acc-tr_acc:>+7.1f}%")


# ══════════════════════════════════════════════════════════════
# Part 7: 模糊区概念信号修正效果
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 7: 模糊区概念信号修正效果")
print(f"{'='*70}")

# B策略模糊区
fuzzy_B = [r for r in weekly if abs(r['mon_actual']) <= 0.5 and r['concept_mon']]
print(f"\nB策略模糊区（周一涨跌±0.5%内，有概念数据）: {len(fuzzy_B)} 样本")
if fuzzy_B:
    b0_ok = sum(1 for r in fuzzy_B
                if (strat_B_orig(r) and r['wup']) or
                   (not strat_B_orig(r) and not r['wup']))
    bc_ok = sum(1 for r in fuzzy_B
                if (strat_B_concept(r) and r['wup']) or
                   (not strat_B_concept(r) and not r['wup']))
    print(f"  B原始:    {round(b0_ok/len(fuzzy_B)*100,1)}%")
    print(f"  B+概念:   {round(bc_ok/len(fuzzy_B)*100,1)}%")
    print(f"  提升: {round((bc_ok-b0_ok)/len(fuzzy_B)*100,1):+.1f}%")

# C策略模糊区
fuzzy_C = [r for r in weekly if abs(r['d3_chg']) <= 1.0 and r['concept_wed']]
print(f"\nC策略模糊区（前3天涨跌±1%内，有概念数据）: {len(fuzzy_C)} 样本")
if fuzzy_C:
    c0_ok = sum(1 for r in fuzzy_C
                if (strat_C_orig(r) and r['wup']) or
                   (not strat_C_orig(r) and not r['wup']))
    cc_ok = sum(1 for r in fuzzy_C
                if (strat_C_concept(r) and r['wup']) or
                   (not strat_C_concept(r) and not r['wup']))
    print(f"  C原始:    {round(c0_ok/len(fuzzy_C)*100,1)}%")
    print(f"  C+概念:   {round(cc_ok/len(fuzzy_C)*100,1)}%")
    print(f"  提升: {round((cc_ok-c0_ok)/len(fuzzy_C)*100,1):+.1f}%")

# 修正分析
print(f"\n概念信号修正分析（B策略）:")
b_fix = b_break = 0
for r in concept_records:
    o = (strat_B_orig(r) and r['wup']) or (not strat_B_orig(r) and not r['wup'])
    v = (strat_B_concept(r) and r['wup']) or (not strat_B_concept(r) and not r['wup'])
    if not o and v: b_fix += 1
    elif o and not v: b_break += 1
print(f"  原始错→概念修正对: {b_fix}")
print(f"  原始对→概念修正错: {b_break}")
print(f"  净改善: {b_fix - b_break}")

print(f"\n概念信号修正分析（C策略）:")
c_fix = c_break = 0
for r in [r for r in weekly if r['concept_wed']]:
    o = (strat_C_orig(r) and r['wup']) or (not strat_C_orig(r) and not r['wup'])
    v = (strat_C_concept(r) and r['wup']) or (not strat_C_concept(r) and not r['wup'])
    if not o and v: c_fix += 1
    elif o and not v: c_break += 1
print(f"  原始错→概念修正对: {c_fix}")
print(f"  原始对→概念修正错: {c_break}")
print(f"  净改善: {c_fix - c_break}")


# ══════════════════════════════════════════════════════════════
# Part 8: 总结
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  Part 8: 总结")
print(f"{'='*70}")

print(f"\n数据概况:")
print(f"  回测股票: {len(stock_codes)}只")
print(f"  周样本: {nw}周")
print(f"  概念板块覆盖: {n_with_boards}只股票有概念数据")
print(f"  概念板块K线: {n_boards_with_kline}/{n_boards_total}个板块有K线")
print(f"  周一概念信号: {n_cm}/{nw} ({round(n_cm/nw*100,1)}%)")
print(f"  周三概念信号: {n_cw}/{nw} ({round(n_cw/nw*100,1)}%)")

print(f"\n全样本准确率:")
for name, fn in strats_B + strats_C:
    ok, n, acc = eval_strat(weekly, fn)
    print(f"  {name:<20} {acc:.1f}%")

print(f"\n概念信号来源: DB概念板块K线(concept_board_kline表)")
print(f"概念维度: 动量(近N日均涨跌), 共识度(看涨板块占比), 强度(累计涨跌)")
print(f"修正逻辑: 模糊区用概念信号替代板块基准率, 强信号区仅极端反向时修正")

print(f"\n{'='*70}")
print(f"  分析完成")
print(f"{'='*70}")
