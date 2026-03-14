#!/usr/bin/env python3
"""
深度分析v20：C策略准确率65%+的逻辑拆解

核心问题：C策略（前3天涨跌>0→周涨）为什么能达到~82%？
这个准确率的定义是否合理？是否存在逻辑陷阱？

分析维度：
Part 1: C策略的数学本质 — 前3天方向与全周方向的相关性
Part 2: 准确率定义拆解 — 四象限分析（TP/TN/FP/FN）
Part 3: 基准率问题 — 如果全猜涨/全猜跌，准确率是多少？
Part 4: 信息泄露检查 — 前3天已包含在全周中，是否有重叠偏差
Part 5: 前3天占全周比重 — 前3天涨跌占全周涨跌的比例
Part 6: 边界敏感性 — 阈值0附近的样本分布
Part 7: 不同市场环境下的表现 — 牛市/熊市/震荡
Part 8: 与随机策略对比 — 蒙特卡洛模拟
Part 9: 实际可操作性 — 周三收盘后到周五收盘的收益
Part 10: 结论 — C策略的真实价值评估
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
from datetime import datetime
from collections import defaultdict
import random

random.seed(42)

from dao.stock_kline_dao import get_kline_data, get_all_stock_codes
from common.utils.sector_mapping_utils import parse_industry_list_md

# ── 排除已用过的90只 ──
EXCLUDE = {
    '002371.SZ','300308.SZ','002916.SZ','603986.SH','688981.SH',
    '002475.SZ','300502.SZ','002049.SZ',
    '002155.SZ','601899.SH','600549.SH','600547.SH','600489.SH',
    '600988.SH','300748.SZ',
    '002594.SZ','600066.SH','601689.SH','002920.SZ','002050.SZ',
    '603596.SH','601127.SH',
    '300750.SZ','300763.SZ','002709.SZ','002074.SZ','300073.SZ',
    '600406.SH','002202.SZ','300450.SZ',
    '600276.SH','600436.SH','603259.SH','000963.SZ','688271.SH',
    '300759.SZ','000538.SZ',
    '600309.SH','002440.SZ','002497.SZ','600426.SH','002648.SZ',
    '600989.SH','002250.SZ',
    '600031.SH','300124.SZ','000157.SZ','601100.SH','000425.SZ',
    '600150.SH',
    '688256.SH','002156.SZ','688012.SH','002384.SZ','000725.SZ',
    '688008.SH','002241.SZ',
    '603993.SH','600362.SH','600219.SH','002460.SZ','600111.SH',
    '600104.SH','601799.SH','603348.SH','002906.SZ','601058.SH',
    '601012.SH','300274.SZ','688599.SH','300014.SZ','002129.SZ',
    '300037.SZ',
    '300760.SZ','300122.SZ','002007.SZ','300347.SZ','600196.SH',
    '300015.SZ',
    '002601.SZ','600486.SH','002064.SZ','603260.SH','000830.SZ',
    '002008.SZ','601766.SH','600835.SH','601698.SH','002097.SZ',
    '601882.SH',
}

START_DATE = '2025-12-10'
END_DATE = '2026-03-10'
MIN_KLINES = 150
TARGET_PER_SECTOR = 15
SECTORS_7 = {'科技','有色金属','汽车','新能源','医药','化工','制造'}

# ══════════════════════════════════════════════════════════════
# 数据准备：复用v2选股逻辑，获取100只股票的周数据
# ══════════════════════════════════════════════════════════════
print("=" * 70)
print("深度分析v20：C策略准确率逻辑拆解")
print("=" * 70)

sector_mapping = parse_industry_list_md()
all_codes = get_all_stock_codes()

codes_by_sector = defaultdict(list)
for code in all_codes:
    if code in EXCLUDE:
        continue
    sec = sector_mapping.get(code)
    if sec in SECTORS_7:
        codes_by_sector[sec].append(code)

selected = []
for sec in sorted(SECTORS_7):
    cands = codes_by_sector[sec][:]
    random.shuffle(cands)
    count = 0
    for code in cands:
        if count >= TARGET_PER_SECTOR:
            break
        kl = get_kline_data(code, start_date='2025-06-01', end_date=END_DATE)
        kl = [k for k in kl if (k.get('trading_volume') or 0) > 0]
        if len(kl) >= MIN_KLINES:
            selected.append(code)
            count += 1
selected = selected[:100]
print(f"选取 {len(selected)} 只股票")


# ── 构建所有周数据（含详细拆分） ──
all_weekly = []

for idx, code in enumerate(selected):
    kl = get_kline_data(code, start_date='2025-06-01', end_date=END_DATE)
    kl = [k for k in kl if (k.get('trading_volume') or 0) > 0]
    kl.sort(key=lambda x: x['date'])
    sector = sector_mapping.get(code, '未知')

    # 计算每日涨跌幅
    for i, k in enumerate(kl):
        if i == 0:
            k['_chg'] = 0
        else:
            prev_close = kl[i-1]['close_price']
            k['_chg'] = (k['close_price'] - prev_close) / prev_close * 100 if prev_close > 0 else 0

    bt_kl = [k for k in kl if START_DATE <= k['date'] <= END_DATE]
    if len(bt_kl) < 10:
        continue

    week_days = defaultdict(list)
    for k in bt_kl:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        week_days[iw].append(k)

    for iw in sorted(week_days.keys()):
        days = sorted(week_days[iw], key=lambda x: x['date'])
        if len(days) < 3:
            continue

        # 全周累计涨跌（累乘）
        cum_all = 1.0
        for d in days:
            cum_all *= (1 + d['_chg'] / 100)
        wchg = (cum_all - 1) * 100
        wup = wchg >= 0

        # 前3天累计涨跌（之和）
        d3_chg = sum(d['_chg'] for d in days[:3])
        d3_up = d3_chg > 0

        # 前3天累乘
        cum_d3 = 1.0
        for d in days[:3]:
            cum_d3 *= (1 + d['_chg'] / 100)
        d3_chg_cum = (cum_d3 - 1) * 100

        # 后N天（周四+周五）累计涨跌
        remaining = days[3:]
        cum_rem = 1.0
        for d in remaining:
            cum_rem *= (1 + d['_chg'] / 100)
        rem_chg = (cum_rem - 1) * 100
        rem_up = rem_chg >= 0

        # 各天涨跌
        day_chgs = [d['_chg'] for d in days]

        c_ok = (d3_up and wup) or (not d3_up and not wup)

        all_weekly.append({
            'code': code, 'sector': sector, 'iw': iw,
            'n_days': len(days),
            'wchg': wchg, 'wup': wup,
            'd3_chg': d3_chg, 'd3_up': d3_up,
            'd3_chg_cum': d3_chg_cum,
            'rem_chg': rem_chg, 'rem_up': rem_up,
            'day_chgs': day_chgs,
            'c_ok': c_ok,
        })

    if (idx + 1) % 25 == 0:
        print(f"  已处理 {idx+1}/{len(selected)}...")

nw = len(all_weekly)
c_ok_all = sum(1 for r in all_weekly if r['c_ok'])
print(f"数据准备完成: {nw} 周样本, C策略准确率={round(c_ok_all/nw*100,1)}%")


# ══════════════════════════════════════════════════════════════
# Part 1: C策略的数学本质
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 1: C策略的数学本质 — 前3天方向与全周方向的相关性")
print(f"{'='*70}")

d3_arr = np.array([r['d3_chg'] for r in all_weekly])
wk_arr = np.array([r['wchg'] for r in all_weekly])
rem_arr = np.array([r['rem_chg'] for r in all_weekly])

# Pearson相关系数
corr_d3_wk = np.corrcoef(d3_arr, wk_arr)[0, 1]
corr_d3_rem = np.corrcoef(d3_arr, rem_arr)[0, 1]
corr_rem_wk = np.corrcoef(rem_arr, wk_arr)[0, 1]

print(f"前3天涨跌 vs 全周涨跌 相关系数: {corr_d3_wk:.4f}")
print(f"前3天涨跌 vs 后2天涨跌 相关系数: {corr_d3_rem:.4f}")
print(f"后2天涨跌 vs 全周涨跌 相关系数: {corr_rem_wk:.4f}")

# R² — 前3天能解释全周多少方差
r2_d3_wk = corr_d3_wk ** 2
print(f"\n前3天涨跌解释全周方差的比例 (R²): {r2_d3_wk:.4f} ({r2_d3_wk*100:.1f}%)")
print(f"  → 前3天涨跌变化能解释全周涨跌变化的 {r2_d3_wk*100:.1f}%")

# 方向一致率
dir_same = sum(1 for r in all_weekly if (r['d3_chg'] >= 0) == (r['wchg'] >= 0))
print(f"\n前3天方向与全周方向一致率: {dir_same}/{nw} ({round(dir_same/nw*100,1)}%)")
print(f"  → 这就是C策略的准确率定义")

# 关键洞察：前3天是全周的子集
print(f"\n★ 关键洞察：")
print(f"  全周涨跌 = f(前3天涨跌, 后2天涨跌)")
print(f"  前3天涨跌已经是全周涨跌的主要组成部分")
print(f"  这不是'预测'，而是'部分决定整体'的数学关系")


# ══════════════════════════════════════════════════════════════
# Part 2: 准确率定义拆解 — 四象限分析
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 2: 准确率定义拆解 — 四象限分析（TP/TN/FP/FN）")
print(f"{'='*70}")

tp = sum(1 for r in all_weekly if r['d3_up'] and r['wup'])       # 预测涨，实际涨
fp = sum(1 for r in all_weekly if r['d3_up'] and not r['wup'])   # 预测涨，实际跌
tn = sum(1 for r in all_weekly if not r['d3_up'] and not r['wup'])  # 预测跌，实际跌
fn = sum(1 for r in all_weekly if not r['d3_up'] and r['wup'])   # 预测跌，实际涨

print(f"                    实际周涨    实际周跌    合计")
print(f"  预测涨(d3>0)      TP={tp:<6}  FP={fp:<6}  {tp+fp}")
print(f"  预测跌(d3≤0)      FN={fn:<6}  TN={tn:<6}  {fn+tn}")
print(f"  合计              {tp+fn:<10}{fp+tn:<10}{nw}")

accuracy = (tp + tn) / nw * 100
precision = tp / (tp + fp) * 100 if (tp + fp) > 0 else 0
recall = tp / (tp + fn) * 100 if (tp + fn) > 0 else 0
specificity = tn / (tn + fp) * 100 if (tn + fp) > 0 else 0

print(f"\n  准确率 (TP+TN)/N:  {accuracy:.1f}%")
print(f"  精确率 TP/(TP+FP): {precision:.1f}%  (预测涨时真涨的比例)")
print(f"  召回率 TP/(TP+FN): {recall:.1f}%  (实际涨被正确预测的比例)")
print(f"  特异度 TN/(TN+FP): {specificity:.1f}%  (实际跌被正确预测的比例)")

# 预测分布
pred_up_pct = (tp + fp) / nw * 100
actual_up_pct = (tp + fn) / nw * 100
print(f"\n  预测涨的比例: {pred_up_pct:.1f}%")
print(f"  实际涨的比例: {actual_up_pct:.1f}%")
print(f"  → 预测分布与实际分布的差异: {abs(pred_up_pct - actual_up_pct):.1f}个百分点")


# ══════════════════════════════════════════════════════════════
# Part 3: 基准率问题 — 朴素策略对比
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 3: 基准率问题 — 朴素策略对比")
print(f"{'='*70}")

n_wup = sum(1 for r in all_weekly if r['wup'])
n_wdn = nw - n_wup
base_up = n_wup / nw * 100
base_dn = n_wdn / nw * 100

print(f"实际周涨: {n_wup}/{nw} ({base_up:.1f}%)")
print(f"实际周跌: {n_wdn}/{nw} ({base_dn:.1f}%)")

# 朴素策略
always_up = base_up  # 全猜涨
always_dn = base_dn  # 全猜跌
coin_flip = 50.0     # 随机猜

print(f"\n朴素策略准确率:")
print(f"  全猜涨:   {always_up:.1f}%")
print(f"  全猜跌:   {always_dn:.1f}%")
print(f"  随机猜:   {coin_flip:.1f}%")
print(f"  C策略:    {accuracy:.1f}%")
print(f"\nC策略 vs 最佳朴素策略(全猜涨)的提升: +{accuracy - max(always_up, always_dn):.1f}个百分点")
print(f"C策略 vs 随机猜的提升: +{accuracy - coin_flip:.1f}个百分点")

# 但这里有个关键问题
print(f"\n★ 关键问题：")
print(f"  C策略的'预测'发生在周三收盘后，此时已知前3天数据")
print(f"  前3天数据是全周5天的60%")
print(f"  如果前3天涨了2%，后2天需要跌超过2%才能让全周为跌")
print(f"  这在统计上是小概率事件 → 所以准确率自然很高")


# ══════════════════════════════════════════════════════════════
# Part 4: 信息泄露检查 — 前3天是全周的子集
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 4: 信息泄露/重叠检查 — 前3天是全周的子集")
print(f"{'='*70}")

# 核心问题：C策略用前3天预测全周，但前3天本身就是全周的一部分
# 这不是传统意义上的"预测"，而是"已知部分推断整体"

# 计算：后2天需要多大的反转才能改变全周方向
reversals = []
for r in all_weekly:
    # 如果前3天涨(d3_chg>0)，后2天需要跌多少才能让全周为跌？
    # 全周涨跌 ≈ 前3天 + 后2天（简化近似）
    # 要让全周跌，需要 后2天 < -前3天
    needed_reversal = -r['d3_chg']  # 后2天需要达到的涨跌才能反转
    actual_rem = r['rem_chg']
    reversed_ok = (r['d3_chg'] > 0 and actual_rem < needed_reversal) or \
                  (r['d3_chg'] <= 0 and actual_rem > needed_reversal)
    reversals.append({
        'd3': r['d3_chg'],
        'needed': needed_reversal,
        'actual_rem': actual_rem,
        'reversed': reversed_ok,
        'abs_d3': abs(r['d3_chg']),
    })

n_reversed = sum(1 for r in reversals if r['reversed'])
print(f"后2天成功反转前3天方向的比例: {n_reversed}/{nw} ({round(n_reversed/nw*100,1)}%)")
print(f"后2天未能反转的比例: {nw-n_reversed}/{nw} ({round((nw-n_reversed)/nw*100,1)}%)")
print(f"  → 这就是C策略准确率的来源")

# 按前3天涨跌幅度分组
print(f"\n按前3天涨跌幅度分组的反转率:")
print(f"  {'前3天幅度':<16} {'样本':>5} {'反转':>5} {'反转率':>8} {'C准确率':>8}")
print(f"  {'-'*48}")
bins = [(-999, -3, '<-3%'), (-3, -1, '-3~-1%'), (-1, 0, '-1~0%'),
        (0, 1, '0~1%'), (1, 3, '1~3%'), (3, 999, '>3%')]
for lo, hi, label in bins:
    grp = [r for r in reversals if lo <= r['d3'] < hi]
    if not grp:
        continue
    rev = sum(1 for r in grp if r['reversed'])
    c_acc = 100 - round(rev / len(grp) * 100, 1)  # C准确率 = 1 - 反转率
    print(f"  {label:<16} {len(grp):>5} {rev:>5} {round(rev/len(grp)*100,1):>7.1f}% {c_acc:>7.1f}%")

print(f"\n★ 关键发现：")
print(f"  前3天涨跌幅度越大，后2天越难反转 → C策略准确率越高")
print(f"  前3天涨跌幅度越小（接近0），后2天越容易反转 → C策略准确率越低")
print(f"  这是数学上的必然，不是'预测能力'")


# ══════════════════════════════════════════════════════════════
# Part 5: 前3天占全周比重 — 方差分解
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 5: 前3天占全周比重 — 方差分解")
print(f"{'='*70}")

# 前3天涨跌占全周涨跌的绝对比重
ratios = []
for r in all_weekly:
    if abs(r['wchg']) > 0.01:  # 避免除以0
        ratio = r['d3_chg_cum'] / r['wchg']
        ratios.append(ratio)

ratios_arr = np.array(ratios)
print(f"前3天涨跌/全周涨跌 的比值分布 (排除全周≈0的样本):")
print(f"  样本数: {len(ratios)}")
print(f"  均值:   {np.mean(ratios_arr):.3f}")
print(f"  中位数: {np.median(ratios_arr):.3f}")
print(f"  25%分位: {np.percentile(ratios_arr, 25):.3f}")
print(f"  75%分位: {np.percentile(ratios_arr, 75):.3f}")

# 方差贡献
var_d3 = np.var(d3_arr)
var_wk = np.var(wk_arr)
var_rem = np.var(rem_arr)
cov_d3_rem = np.cov(d3_arr, rem_arr)[0, 1]

print(f"\n方差分解:")
print(f"  前3天涨跌方差:  {var_d3:.4f}")
print(f"  后2天涨跌方差:  {var_rem:.4f}")
print(f"  全周涨跌方差:   {var_wk:.4f}")
print(f"  前3天与后2天协方差: {cov_d3_rem:.4f}")
print(f"  理论全周方差 ≈ Var(d3) + Var(rem) + 2*Cov = {var_d3 + var_rem + 2*cov_d3_rem:.4f}")

d3_var_pct = var_d3 / var_wk * 100
rem_var_pct = var_rem / var_wk * 100
print(f"\n  前3天方差占全周方差: {d3_var_pct:.1f}%")
print(f"  后2天方差占全周方差: {rem_var_pct:.1f}%")
print(f"  → 前3天贡献了全周波动的大部分")

# 平均绝对涨跌
avg_abs_d3 = np.mean(np.abs(d3_arr))
avg_abs_rem = np.mean(np.abs(rem_arr))
avg_abs_wk = np.mean(np.abs(wk_arr))
print(f"\n平均绝对涨跌:")
print(f"  前3天: {avg_abs_d3:.3f}%")
print(f"  后2天: {avg_abs_rem:.3f}%")
print(f"  全周:  {avg_abs_wk:.3f}%")
print(f"  前3天/全周 = {avg_abs_d3/avg_abs_wk*100:.1f}%")


# ══════════════════════════════════════════════════════════════
# Part 6: 边界敏感性 — 阈值0附近的样本分布
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 6: 边界敏感性 — 阈值0附近的样本分布")
print(f"{'='*70}")

# 前3天涨跌接近0的样本，C策略的准确率如何？
thresholds = [0.1, 0.3, 0.5, 1.0, 2.0, 3.0]
print(f"前3天涨跌在±阈值内的样本（'模糊区'）:")
print(f"  {'阈值':<8} {'模糊区样本':>10} {'占比':>8} {'模糊区C准确率':>14} {'非模糊区C准确率':>14}")
print(f"  {'-'*60}")
for th in thresholds:
    fuzzy = [r for r in all_weekly if abs(r['d3_chg']) <= th]
    clear = [r for r in all_weekly if abs(r['d3_chg']) > th]
    f_ok = sum(1 for r in fuzzy if r['c_ok'])
    c_ok_clear = sum(1 for r in clear if r['c_ok'])
    f_acc = round(f_ok / len(fuzzy) * 100, 1) if fuzzy else 0
    c_acc = round(c_ok_clear / len(clear) * 100, 1) if clear else 0
    print(f"  ±{th:<6} {len(fuzzy):>10} {round(len(fuzzy)/nw*100,1):>7.1f}% "
          f"{f_acc:>13.1f}% {c_acc:>13.1f}%")

# 如果排除模糊区，C策略准确率会更高
clear_1 = [r for r in all_weekly if abs(r['d3_chg']) > 1.0]
clear_1_ok = sum(1 for r in clear_1 if r['c_ok'])
print(f"\n排除±1%模糊区后:")
print(f"  样本: {len(clear_1)}/{nw} ({round(len(clear_1)/nw*100,1)}%)")
print(f"  C准确率: {round(clear_1_ok/len(clear_1)*100,1)}%")

# 全周涨跌接近0的样本
print(f"\n全周涨跌在±阈值内的样本:")
print(f"  {'阈值':<8} {'样本':>6} {'占比':>8}")
print(f"  {'-'*26}")
for th in [0.1, 0.5, 1.0, 2.0]:
    cnt = sum(1 for r in all_weekly if abs(r['wchg']) <= th)
    print(f"  ±{th:<6} {cnt:>6} {round(cnt/nw*100,1):>7.1f}%")


# ══════════════════════════════════════════════════════════════
# Part 7: 不同市场环境下的表现
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 7: 不同市场环境下的表现 — 按周分类")
print(f"{'='*70}")

# 按周统计市场整体涨跌
weeks = sorted(set(r['iw'] for r in all_weekly))
print(f"  {'周':<12} {'样本':>5} {'周涨比':>8} {'平均涨跌':>10} {'C准确率':>10} {'市场状态':<8}")
print(f"  {'-'*58}")
for iw in weeks:
    wr = [r for r in all_weekly if r['iw'] == iw]
    up_pct = sum(1 for r in wr if r['wup']) / len(wr) * 100
    avg_chg = np.mean([r['wchg'] for r in wr])
    c_ok_w = sum(1 for r in wr if r['c_ok'])
    c_acc = round(c_ok_w / len(wr) * 100, 1)
    if up_pct > 70:
        state = '强势'
    elif up_pct < 30:
        state = '弱势'
    else:
        state = '震荡'
    print(f"  {iw[0]}-W{iw[1]:02d}   {len(wr):>5} {up_pct:>7.1f}% {avg_chg:>9.2f}% {c_acc:>9.1f}% {state}")

# 按市场状态分组
strong_weeks = [iw for iw in weeks
                if sum(1 for r in all_weekly if r['iw'] == iw and r['wup']) /
                   sum(1 for r in all_weekly if r['iw'] == iw) > 0.7]
weak_weeks = [iw for iw in weeks
              if sum(1 for r in all_weekly if r['iw'] == iw and r['wup']) /
                 sum(1 for r in all_weekly if r['iw'] == iw) < 0.3]
mixed_weeks = [iw for iw in weeks if iw not in strong_weeks and iw not in weak_weeks]

for label, wk_set in [('强势周', strong_weeks), ('弱势周', weak_weeks), ('震荡周', mixed_weeks)]:
    samples = [r for r in all_weekly if r['iw'] in wk_set]
    if not samples:
        continue
    ok = sum(1 for r in samples if r['c_ok'])
    print(f"\n{label} ({len(wk_set)}周, {len(samples)}样本):")
    print(f"  C准确率: {round(ok/len(samples)*100,1)}%")
    print(f"  周涨比: {round(sum(1 for r in samples if r['wup'])/len(samples)*100,1)}%")

print(f"\n★ 分析：")
print(f"  强势周：大部分股票涨，前3天也涨 → C策略自然准确")
print(f"  弱势周：大部分股票跌，前3天也跌 → C策略自然准确")
print(f"  震荡周：方向不明确 → C策略准确率下降")
print(f"  C策略在极端市场（强势/弱势）表现最好，因为趋势延续性强")


# ══════════════════════════════════════════════════════════════
# Part 8: 与随机策略对比 — 蒙特卡洛模拟
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 8: 与随机策略对比 — 蒙特卡洛模拟")
print(f"{'='*70}")

np.random.seed(42)
n_sim = 10000
actual_ups = np.array([r['wup'] for r in all_weekly])

# 随机策略：随机猜涨跌
random_accs = []
for _ in range(n_sim):
    preds = np.random.random(nw) > 0.5
    acc = np.mean(preds == actual_ups) * 100
    random_accs.append(acc)

random_accs = np.array(random_accs)
print(f"蒙特卡洛模拟 ({n_sim}次随机策略):")
print(f"  随机策略准确率: 均值={np.mean(random_accs):.1f}%, "
      f"标准差={np.std(random_accs):.2f}%")
print(f"  95%区间: [{np.percentile(random_accs, 2.5):.1f}%, "
      f"{np.percentile(random_accs, 97.5):.1f}%]")
print(f"  最高: {np.max(random_accs):.1f}%, 最低: {np.min(random_accs):.1f}%")
print(f"  C策略: {accuracy:.1f}%")

# C策略超过随机策略的z-score
z_score = (accuracy - np.mean(random_accs)) / np.std(random_accs)
print(f"\n  C策略 z-score: {z_score:.1f}")
print(f"  → C策略显著优于随机猜测")

# 但更公平的对比：用前3天信息的随机策略
# 即：随机打乱前3天涨跌与全周涨跌的对应关系
print(f"\n打乱配对的蒙特卡洛（保持前3天分布不变，打乱与全周的对应）:")
shuffle_accs = []
d3_ups = np.array([r['d3_up'] for r in all_weekly])
for _ in range(n_sim):
    shuffled = np.random.permutation(actual_ups)
    acc = np.mean(d3_ups == shuffled) * 100
    shuffle_accs.append(acc)

shuffle_accs = np.array(shuffle_accs)
print(f"  打乱后准确率: 均值={np.mean(shuffle_accs):.1f}%, "
      f"标准差={np.std(shuffle_accs):.2f}%")
print(f"  95%区间: [{np.percentile(shuffle_accs, 2.5):.1f}%, "
      f"{np.percentile(shuffle_accs, 97.5):.1f}%]")
print(f"  C策略: {accuracy:.1f}%")
z2 = (accuracy - np.mean(shuffle_accs)) / np.std(shuffle_accs)
print(f"  z-score: {z2:.1f}")
print(f"  → 打乱对应关系后准确率大幅下降，说明前3天与全周的关联是真实的")
print(f"  → 但这种关联来自'部分包含整体'，不是独立预测")


# ══════════════════════════════════════════════════════════════
# Part 9: 实际可操作性 — 真正的预测价值
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 9: 实际可操作性 — 真正的预测价值")
print(f"{'='*70}")

# 真正有价值的预测：用前3天预测后2天的方向
# 这才是没有信息泄露的"纯预测"
d3_pred_rem = sum(1 for r in all_weekly
                  if (r['d3_up'] and r['rem_up']) or
                     (not r['d3_up'] and not r['rem_up']))
d3_pred_rem_pct = round(d3_pred_rem / nw * 100, 1)

print(f"用前3天方向预测后2天方向:")
print(f"  准确率: {d3_pred_rem}/{nw} ({d3_pred_rem_pct}%)")

# 后2天的方向基准率
rem_up_n = sum(1 for r in all_weekly if r['rem_up'])
rem_base = round(rem_up_n / nw * 100, 1)
print(f"  后2天涨的基准率: {rem_base}%")
print(f"  → 前3天对后2天的预测提升: {d3_pred_rem_pct - max(rem_base, 100-rem_base):.1f}个百分点")

# 四象限：前3天 vs 后2天
tp2 = sum(1 for r in all_weekly if r['d3_up'] and r['rem_up'])
fp2 = sum(1 for r in all_weekly if r['d3_up'] and not r['rem_up'])
tn2 = sum(1 for r in all_weekly if not r['d3_up'] and not r['rem_up'])
fn2 = sum(1 for r in all_weekly if not r['d3_up'] and r['rem_up'])

print(f"\n前3天 vs 后2天 四象限:")
print(f"                    后2天涨     后2天跌     合计")
print(f"  前3天涨(d3>0)     TP={tp2:<6}  FP={fp2:<6}  {tp2+fp2}")
print(f"  前3天跌(d3≤0)     FN={fn2:<6}  TN={tn2:<6}  {fn2+tn2}")

# 实际交易价值：周三收盘买入，周五收盘卖出
print(f"\n实际交易模拟（周三收盘后操作）:")
# 策略：前3天涨>0 → 周四开盘买入，周五收盘卖出
# 策略：前3天跌≤0 → 不操作（或做空）
buy_returns = []
for r in all_weekly:
    if r['d3_up'] and len(r['day_chgs']) >= 4:
        # 后2天的收益
        ret = r['rem_chg']
        buy_returns.append(ret)

if buy_returns:
    buy_arr = np.array(buy_returns)
    win_rate = sum(1 for r in buy_returns if r > 0) / len(buy_returns) * 100
    print(f"  前3天涨→买入后2天:")
    print(f"    交易次数: {len(buy_returns)}")
    print(f"    胜率: {win_rate:.1f}%")
    print(f"    平均收益: {np.mean(buy_arr):.3f}%")
    print(f"    中位收益: {np.median(buy_arr):.3f}%")
    print(f"    最大收益: {np.max(buy_arr):.2f}%")
    print(f"    最大亏损: {np.min(buy_arr):.2f}%")
    print(f"    累计收益: {np.sum(buy_arr):.2f}%")

# 反向：前3天跌→后2天的表现
sell_returns = []
for r in all_weekly:
    if not r['d3_up'] and len(r['day_chgs']) >= 4:
        ret = r['rem_chg']
        sell_returns.append(ret)

if sell_returns:
    sell_arr = np.array(sell_returns)
    win_rate_s = sum(1 for r in sell_returns if r < 0) / len(sell_returns) * 100
    print(f"\n  前3天跌→后2天:")
    print(f"    交易次数: {len(sell_returns)}")
    print(f"    后2天跌的比例: {win_rate_s:.1f}%")
    print(f"    平均涨跌: {np.mean(sell_arr):.3f}%")

print(f"\n★ 关键结论：")
print(f"  C策略82%的准确率来自'前3天是全周的一部分'这个数学事实")
print(f"  真正的预测价值（前3天→后2天）准确率只有 {d3_pred_rem_pct}%")
print(f"  这才是C策略的'真实预测能力'")


# ══════════════════════════════════════════════════════════════
# Part 10: 结论 — C策略的真实价值评估
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"Part 10: 结论 — C策略的真实价值评估")
print(f"{'='*70}")

print(f"""
┌─────────────────────────────────────────────────────────────────┐
│                    C策略深度分析总结                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. C策略定义：前3天累计涨跌>0 → 预测全周涨                       │
│     准确率: ~82%（100只股票，12周验证）                            │
│                                                                 │
│  2. 准确率为什么这么高？                                          │
│     ✗ 不是因为发现了市场规律                                      │
│     ✗ 不是因为前3天能"预测"后2天                                  │
│     ✓ 因为前3天是全周5天的子集（60%的交易日）                      │
│     ✓ 前3天涨跌占全周涨跌方差的大部分                              │
│     ✓ 后2天很难完全反转前3天的方向                                 │
│                                                                 │
│  3. 信息泄露问题：                                                │
│     C策略用"部分"预测"整体"，而"部分"包含在"整体"中                │
│     这是一种结构性的信息重叠，不是传统意义的预测                    │
│     类比：知道考试前60%的题答对了，预测总分及格 → 准确率自然高      │
│                                                                 │
│  4. 真实预测能力（前3天→后2天方向）:                               │
│     准确率: {d3_pred_rem_pct}%                                              │
│     这才是C策略的"纯预测"能力                                     │
│                                                                 │
│  5. 实际交易价值：                                                │
│     周三收盘后根据前3天方向操作后2天                               │
│     胜率和收益需要看Part 9的具体数字                               │
│                                                                 │
│  6. 结论：                                                       │
│     C策略82%的准确率在统计上是真实的                               │
│     但这个准确率的"含金量"需要分两层理解：                         │
│     - 作为"全周方向判断"：82%是可靠的（周三就能知道本周大概率方向） │
│     - 作为"预测能力"：真实预测力只有{d3_pred_rem_pct}%（前3天→后2天）         │
│                                                                 │
│  7. 实用建议：                                                   │
│     ✓ C策略适合用于"周中确认"：周三确认本周方向，决定是否持有到周五 │
│     ✓ 不适合用于"周初预测"：无法在周一就知道前3天的涨跌            │
│     ✓ 真正的预测价值在于：前3天涨→后2天大概率不会大跌（惯性效应）  │
│     ✓ 可以结合B策略（周一）做两阶段决策                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
""")

# 数值总结
print(f"数值总结:")
print(f"  C策略准确率（全周方向）:     {accuracy:.1f}%")
print(f"  前3天→后2天方向准确率:       {d3_pred_rem_pct}%")
print(f"  前3天与全周相关系数:         {corr_d3_wk:.4f}")
print(f"  前3天与后2天相关系数:        {corr_d3_rem:.4f}")
print(f"  前3天方差占全周方差:         {d3_var_pct:.1f}%")
print(f"  最佳朴素策略（全猜涨）:      {max(always_up, always_dn):.1f}%")
print(f"  随机策略均值:                {np.mean(random_accs):.1f}%")

print(f"\n{'='*70}")
print(f"分析完成")
print(f"{'='*70}")
