#!/usr/bin/env python3
"""
C策略（前3天涨跌>0）100只股票大规模验证 v2

改进：
1. 周涨跌计算与v19e一致：用每日收盘价涨跌累乘
2. 前3天涨跌与v19e一致：用每日实际涨跌百分比之和
3. 板块均匀分布：每板块14-15只
4. 混合沪深两市大中小盘
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from collections import defaultdict
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
TARGET_PER_SECTOR = 15  # 每板块15只，7板块=105只，取前100
SECTORS_7 = {'科技','有色金属','汽车','新能源','医药','化工','制造'}

print(f"{'='*70}")
print(f"C策略大规模验证 v2：100只新股票，均匀覆盖7板块")
print(f"策略：前3天累计涨跌>0 → 预测周涨，否则预测周跌")
print(f"回测区间：{START_DATE} ~ {END_DATE}")
print(f"{'='*70}")

sector_mapping = parse_industry_list_md()
all_codes = get_all_stock_codes()
print(f"板块映射: {len(sector_mapping)}, DB股票: {len(all_codes)}")

# ── 按板块筛选候选 ──
import random
random.seed(42)  # 可复现

sector_candidates = defaultdict(list)
codes_by_sector = defaultdict(list)
for code in all_codes:
    if code in EXCLUDE:
        continue
    sec = sector_mapping.get(code)
    if sec in SECTORS_7:
        codes_by_sector[sec].append(code)

print(f"\n各板块DB候选（排除已用）:")
for sec in sorted(SECTORS_7):
    print(f"  {sec}: {len(codes_by_sector[sec])} 只")

# 每板块随机打乱后逐个检查K线，取前15只合格的
print(f"\n筛选K线充足的股票...")
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
    print(f"  {sec}: 选取 {count} 只")

selected = selected[:100]
print(f"\n最终选取: {len(selected)} 只")
sel_count = defaultdict(int)
for c in selected:
    sel_count[sector_mapping[c]] += 1
for sec in sorted(sel_count):
    print(f"  {sec}: {sel_count[sec]} 只")

# ── 计算C策略（与v19e完全一致的计算方式） ──
print(f"\n{'='*70}")
print(f"计算C策略...")
print(f"{'='*70}")

all_weekly = []
stock_results = []

for idx, code in enumerate(selected):
    kl = get_kline_data(code, start_date='2025-06-01', end_date=END_DATE)
    kl = [k for k in kl if (k.get('trading_volume') or 0) > 0]
    kl.sort(key=lambda x: x['date'])
    sector = sector_mapping.get(code, '未知')

    # 回测区间K线
    bt_kl = [k for k in kl if START_DATE <= k['date'] <= END_DATE]
    if len(bt_kl) < 10:
        continue

    # 计算每日涨跌幅（与v19e一致：基于前一日收盘价）
    all_with_chg = []
    for i, k in enumerate(kl):
        if i == 0:
            k['_chg'] = 0
        else:
            prev_close = kl[i-1]['close_price']
            if prev_close > 0:
                k['_chg'] = (k['close_price'] - prev_close) / prev_close * 100
            else:
                k['_chg'] = 0
        all_with_chg.append(k)

    # 回测区间内按ISO周分组
    bt_with_chg = [k for k in all_with_chg if START_DATE <= k['date'] <= END_DATE]
    week_days = defaultdict(list)
    for k in bt_with_chg:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        week_days[iw].append(k)

    s_weeks = 0
    s_ok = 0
    for iw in sorted(week_days.keys()):
        days = sorted(week_days[iw], key=lambda x: x['date'])
        if len(days) < 3:
            continue

        # 周累计涨跌（v19e方式：收盘价涨跌累乘）
        cum = 1.0
        for d in days:
            cum *= (1 + d['_chg'] / 100)
        wchg = (cum - 1) * 100
        wup = wchg >= 0

        # 前3天涨跌之和（v19e方式）
        d3_chg = sum(d['_chg'] for d in days[:3])

        pred_up = d3_chg > 0
        c_ok = (pred_up and wup) or (not pred_up and not wup)

        all_weekly.append({
            'code': code, 'sector': sector, 'iw': iw,
            'wchg': wchg, 'wup': wup,
            'd3_chg': d3_chg, 'c_ok': c_ok,
            'n_days': len(days),
        })
        s_weeks += 1
        if c_ok:
            s_ok += 1

    if s_weeks > 0:
        stock_results.append({
            'code': code, 'sector': sector,
            'weeks': s_weeks, 'c_ok': s_ok,
            'c_pct': round(s_ok / s_weeks * 100, 1),
        })
    if (idx + 1) % 20 == 0:
        print(f"  已处理 {idx+1}/{len(selected)}...")

print(f"  完成: {len(stock_results)} 只, {len(all_weekly)} 周样本")

# ── 统计输出 ──
nw = len(all_weekly)
c_ok_all = sum(1 for r in all_weekly if r['c_ok'])
sorted_weeks = sorted(set(r['iw'] for r in all_weekly))

print(f"\n{'='*70}")
print(f"C策略全样本结果")
print(f"{'='*70}")
print(f"股票数: {len(stock_results)}, 周样本: {nw}, 周数: {len(sorted_weeks)}")
print(f"C策略准确率: {c_ok_all}/{nw} ({round(c_ok_all/nw*100,1)}%)")
n_wup = sum(1 for r in all_weekly if r['wup'])
print(f"周涨比例: {n_wup}/{nw} ({round(n_wup/nw*100,1)}%)")

# 按板块
print(f"\n{'板块':<10} {'股票':>4} {'周样本':>6} {'准确率':>14} {'周涨比':>8}")
print("-" * 46)
for sec in sorted(SECTORS_7):
    sr = [r for r in all_weekly if r['sector'] == sec]
    if not sr: continue
    ok = sum(1 for r in sr if r['c_ok'])
    up = sum(1 for r in sr if r['wup'])
    ns = len(set(r['code'] for r in sr))
    print(f"{sec:<10} {ns:>4} {len(sr):>6} "
          f"{ok}/{len(sr)} ({round(ok/len(sr)*100,1)}%) "
          f"{round(up/len(sr)*100,1)}%")

# 按周
print(f"\n{'周':>12} {'样本':>5} {'正确':>5} {'准确率':>8}")
print("-" * 34)
for iw in sorted_weeks:
    wr = [r for r in all_weekly if r['iw'] == iw]
    ok = sum(1 for r in wr if r['c_ok'])
    print(f"{iw[0]}-W{iw[1]:02d}   {len(wr):>5} {ok:>5} "
          f"{round(ok/len(wr)*100,1):>6.1f}%")

# ── 泛化验证 ──
mid = len(sorted_weeks) // 2
second_half = set(sorted_weeks[mid:])
test_r = [r for r in all_weekly if r['iw'] in second_half]
test_ok = sum(1 for r in test_r if r['c_ok'])
fwd = round(test_ok / len(test_r) * 100, 1) if test_r else 0

roll_ok = roll_n = 0
for i in range(2, len(sorted_weeks)):
    te = [r for r in all_weekly if r['iw'] == sorted_weeks[i]]
    for r in te:
        if r['c_ok']: roll_ok += 1
        roll_n += 1
roll = round(roll_ok / roll_n * 100, 1) if roll_n else 0

print(f"\n{'='*70}")
print(f"泛化验证")
print(f"{'='*70}")
print(f"全样本: {c_ok_all}/{nw} ({round(c_ok_all/nw*100,1)}%)")
print(f"前→后:  {test_ok}/{len(test_r)} ({fwd}%)")
print(f"滚动:   {roll_ok}/{roll_n} ({roll}%)")

# ── 股票准确率分布 ──
print(f"\n{'='*70}")
print(f"股票准确率分布")
print(f"{'='*70}")
bins = [(90,101,'≥90%'),(80,90,'80-90%'),(70,80,'70-80%'),
        (65,70,'65-70%'),(50,65,'50-65%'),(0,50,'<50%')]
for lo, hi, label in bins:
    cnt = sum(1 for s in stock_results if lo <= s['c_pct'] < hi)
    bar = '█' * max(1, cnt // 2)
    print(f"  {label:<8} {cnt:>3} 只 ({round(cnt/len(stock_results)*100,1):>5.1f}%) {bar}")

ge65 = sum(1 for s in stock_results if s['c_pct'] >= 65)
print(f"\n≥65%: {ge65}/{len(stock_results)} ({round(ge65/len(stock_results)*100,1)}%)")

# ── 各股票明细 ──
print(f"\n{'='*70}")
print(f"各股票明细")
print(f"{'='*70}")
stock_results.sort(key=lambda x: (x['sector'], -x['c_pct']))
cur_sec = ''
for s in stock_results:
    if s['sector'] != cur_sec:
        cur_sec = s['sector']
        print(f"\n  [{cur_sec}]")
    m = '★' if s['c_pct'] >= 80 else ('✓' if s['c_pct'] >= 65 else ' ')
    print(f"    {s['code']:<12} {s['weeks']:>2}周 "
          f"{s['c_ok']:>2}/{s['weeks']} ({s['c_pct']:>5.1f}%) {m}")

# ── 结论 ──
overall = round(c_ok_all / nw * 100, 1)
print(f"\n{'='*70}")
print(f"最终结论")
print(f"{'='*70}")
print(f"C策略在 {len(stock_results)} 只全新股票（{len(sorted_weeks)}周）上:")
print(f"  全样本: {overall}%  前→后: {fwd}%  滚动: {roll}%")
if overall >= 65 and fwd >= 65 and roll >= 65:
    print(f"  ✅ 三重验证全部≥65%")
else:
    print(f"  ⚠️ 部分未达65%")
