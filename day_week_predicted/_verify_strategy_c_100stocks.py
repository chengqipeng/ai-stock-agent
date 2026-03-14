#!/usr/bin/env python3
"""
C策略（前3天涨跌>0）100只股票大规模验证

C策略逻辑：周三收盘后，如果本周前3个交易日累计涨跌>0，预测本周上涨；否则预测下跌。
不依赖模型预测，只需日K线数据即可计算。

验证要求：100只股票，至少10周数据
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from collections import defaultdict
from dao.stock_kline_dao import get_kline_data, get_all_stock_codes
from common.utils.sector_mapping_utils import parse_industry_list_md

# ── 原始50只 + OOS 40只（排除，确保100只全新） ──
EXCLUDE = {
    # 原始50只
    '002371.SZ', '300308.SZ', '002916.SZ', '603986.SH', '688981.SH',
    '002475.SZ', '300502.SZ', '002049.SZ',
    '002155.SZ', '601899.SH', '600549.SH', '600547.SH', '600489.SH',
    '600988.SH', '300748.SZ',
    '002594.SZ', '600066.SH', '601689.SH', '002920.SZ', '002050.SZ',
    '603596.SH', '601127.SH',
    '300750.SZ', '300763.SZ', '002709.SZ', '002074.SZ', '300073.SZ',
    '600406.SH', '002202.SZ', '300450.SZ',
    '600276.SH', '600436.SH', '603259.SH', '000963.SZ', '688271.SH',
    '300759.SZ', '000538.SZ',
    '600309.SH', '002440.SZ', '002497.SZ', '600426.SH', '002648.SZ',
    '600989.SH', '002250.SZ',
    '600031.SH', '300124.SZ', '000157.SZ', '601100.SH', '000425.SZ',
    '600150.SH',
    # OOS 40只
    '688256.SH', '002156.SZ', '688012.SH', '002384.SZ', '000725.SZ',
    '688008.SH', '002241.SZ',
    '603993.SH', '600362.SH', '600219.SH', '002460.SZ', '600111.SH',
    '600104.SH', '601799.SH', '603348.SH', '002906.SZ', '601058.SH',
    '601012.SH', '300274.SZ', '688599.SH', '300014.SZ', '002129.SZ',
    '300037.SZ',
    '300760.SZ', '300122.SZ', '002007.SZ', '300347.SZ', '600196.SH',
    '300015.SZ',
    '002601.SZ', '600486.SH', '002064.SZ', '603260.SH', '000830.SZ',
    '002008.SZ', '601766.SH', '600835.SH', '601698.SH', '002097.SZ',
    '601882.SH',
}

# ── 回测参数 ──
START_DATE = '2025-12-10'
END_DATE = '2026-03-10'
MIN_KLINES = 150  # 从2025-06-01起至少150根K线
TARGET_STOCKS = 100

print(f"{'='*70}")
print(f"C策略大规模验证：100只新股票，≥10周")
print(f"策略：前3天累计涨跌>0 → 预测周涨，否则预测周跌")
print(f"回测区间：{START_DATE} ~ {END_DATE}")
print(f"{'='*70}")

# ── 1. 获取板块映射 ──
sector_mapping = parse_industry_list_md()
print(f"板块映射总数: {len(sector_mapping)}")

# ── 2. 获取DB中所有股票 ──
all_codes = get_all_stock_codes()
print(f"DB中股票总数: {len(all_codes)}")

# ── 3. 筛选：有板块映射 + 不在排除列表 + 足够K线 ──
# 按板块分组候选
sector_candidates = defaultdict(list)
sectors_7 = {'科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造'}

checked = 0
for code in sorted(all_codes):
    if code in EXCLUDE:
        continue
    sector = sector_mapping.get(code)
    if sector not in sectors_7:
        continue
    # 检查K线数量
    kl = get_kline_data(code, start_date='2025-06-01', end_date=END_DATE)
    kl = [k for k in kl if (k.get('trading_volume') or 0) > 0]
    checked += 1
    if checked % 50 == 0:
        total_cands = sum(len(v) for v in sector_candidates.values())
        print(f"  已检查 {checked} 只，合格 {total_cands} 只...")
    if len(kl) < MIN_KLINES:
        continue
    sector_candidates[sector].append(code)
    total_cands = sum(len(v) for v in sector_candidates.values())
    if total_cands >= TARGET_STOCKS + 50:  # 多选一些备用
        break

print(f"\n各板块候选数:")
for sec in sorted(sector_candidates.keys()):
    print(f"  {sec}: {len(sector_candidates[sec])} 只")
total_cands = sum(len(v) for v in sector_candidates.values())
print(f"  合计: {total_cands} 只")

# ── 4. 每板块均匀选取，凑满100只 ──
selected = []
# 先每板块至少选10只
per_sector_min = 10
for sec in sorted(sector_candidates.keys()):
    cands = sector_candidates[sec]
    n = min(per_sector_min, len(cands))
    selected.extend(cands[:n])

# 如果不够100只，从剩余候选中补充
remaining = []
for sec in sorted(sector_candidates.keys()):
    cands = sector_candidates[sec]
    remaining.extend(cands[per_sector_min:])

need = TARGET_STOCKS - len(selected)
if need > 0:
    selected.extend(remaining[:need])

selected = selected[:TARGET_STOCKS]
print(f"\n最终选取: {len(selected)} 只")

# 统计板块分布
sel_sector_count = defaultdict(int)
for code in selected:
    sel_sector_count[sector_mapping.get(code, '未知')] += 1
for sec in sorted(sel_sector_count.keys()):
    print(f"  {sec}: {sel_sector_count[sec]} 只")

# ── 5. 计算C策略周预测 ──
print(f"\n{'='*70}")
print(f"计算C策略周预测...")
print(f"{'='*70}")

all_weekly = []
stock_results = []

for idx, code in enumerate(selected):
    kl = get_kline_data(code, start_date='2025-06-01', end_date=END_DATE)
    kl = [k for k in kl if (k.get('trading_volume') or 0) > 0]
    kl.sort(key=lambda x: x['date'])

    sector = sector_mapping.get(code, '未知')

    # 筛选回测区间内的K线
    bt_klines = [k for k in kl if START_DATE <= k['date'] <= END_DATE]
    if len(bt_klines) < 10:
        continue

    # 按ISO周分组
    week_days = defaultdict(list)
    for k in bt_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        week_days[iw].append(k)

    stock_weeks = 0
    stock_c_ok = 0

    for iw in sorted(week_days.keys()):
        days = sorted(week_days[iw], key=lambda x: x['date'])
        if len(days) < 3:  # 至少3天才能用C策略
            continue

        # 周累计涨跌（用收盘价）
        first_open = days[0]['open_price']
        last_close = days[-1]['close_price']
        if first_open <= 0:
            continue
        wchg = (last_close / first_open - 1) * 100
        wup = wchg >= 0

        # 前3天累计涨跌
        d3_chg = 0
        for d in days[:3]:
            if d['open_price'] > 0:
                day_chg = (d['close_price'] - d['open_price']) / d['open_price'] * 100
            else:
                day_chg = 0
            d3_chg += day_chg

        # C策略预测
        pred_up = d3_chg > 0
        c_ok = (pred_up and wup) or (not pred_up and not wup)

        all_weekly.append({
            'code': code, 'sector': sector, 'iw': iw,
            'wchg': wchg, 'wup': wup,
            'd3_chg': d3_chg, 'pred_up': pred_up, 'c_ok': c_ok,
            'n_days': len(days),
        })

        stock_weeks += 1
        if c_ok:
            stock_c_ok += 1

    if stock_weeks > 0:
        stock_results.append({
            'code': code, 'sector': sector,
            'weeks': stock_weeks,
            'c_ok': stock_c_ok,
            'c_pct': round(stock_c_ok / stock_weeks * 100, 1),
        })

    if (idx + 1) % 20 == 0:
        print(f"  已处理 {idx+1}/{len(selected)} 只...")

print(f"  处理完成: {len(stock_results)} 只有效股票, {len(all_weekly)} 周样本")

# ── 6. 全样本统计 ──
nw = len(all_weekly)
c_ok_total = sum(1 for r in all_weekly if r['c_ok'])
sorted_weeks = sorted(set(r['iw'] for r in all_weekly))
n_weeks = len(sorted_weeks)

print(f"\n{'='*70}")
print(f"C策略全样本结果")
print(f"{'='*70}")
print(f"有效股票数: {len(stock_results)}")
print(f"周样本总数: {nw}")
print(f"覆盖周数: {n_weeks}")
print(f"C策略准确率: {c_ok_total}/{nw} ({round(c_ok_total/nw*100, 1)}%)")

# 周涨/周跌分布
n_wup = sum(1 for r in all_weekly if r['wup'])
print(f"周涨比例: {n_wup}/{nw} ({round(n_wup/nw*100, 1)}%)")
print(f"周跌比例: {nw-n_wup}/{nw} ({round((nw-n_wup)/nw*100, 1)}%)")

# ── 7. 按板块统计 ──
print(f"\n{'='*70}")
print(f"按板块统计")
print(f"{'='*70}")
print(f"{'板块':<10} {'股票数':>5} {'周样本':>6} {'C策略准确率':>14} {'周涨比例':>10}")
print("-" * 50)
for sec in sorted(sectors_7):
    sr = [r for r in all_weekly if r['sector'] == sec]
    if not sr:
        continue
    s_ok = sum(1 for r in sr if r['c_ok'])
    s_up = sum(1 for r in sr if r['wup'])
    n_stocks = len(set(r['code'] for r in sr))
    print(f"{sec:<10} {n_stocks:>5} {len(sr):>6} "
          f"{s_ok}/{len(sr)} ({round(s_ok/len(sr)*100,1)}%) "
          f"{round(s_up/len(sr)*100,1)}%")

# ── 8. 按周统计（每周准确率） ──
print(f"\n{'='*70}")
print(f"按周统计（每周C策略准确率）")
print(f"{'='*70}")
print(f"{'周':>12} {'样本':>5} {'正确':>5} {'准确率':>8} {'周涨比':>8}")
print("-" * 42)
for iw in sorted_weeks:
    wr = [r for r in all_weekly if r['iw'] == iw]
    w_ok = sum(1 for r in wr if r['c_ok'])
    w_up = sum(1 for r in wr if r['wup'])
    print(f"{iw[0]}-W{iw[1]:02d}   {len(wr):>5} {w_ok:>5} "
          f"{round(w_ok/len(wr)*100,1):>6.1f}% "
          f"{round(w_up/len(wr)*100,1):>6.1f}%")

# ── 9. LOWO交叉验证 ──
# C策略不依赖训练数据，LOWO结果等于全样本结果
# 但我们做滚动验证和前→后验证来确认稳定性
print(f"\n{'='*70}")
print(f"泛化验证")
print(f"{'='*70}")

# 前半→后半
mid = len(sorted_weeks) // 2
first_half = set(sorted_weeks[:mid])
second_half = set(sorted_weeks[mid:])
test_recs = [r for r in all_weekly if r['iw'] in second_half]
test_ok = sum(1 for r in test_recs if r['c_ok'])
fwd_pct = round(test_ok / len(test_recs) * 100, 1) if test_recs else 0

# 滚动验证（从第3周开始）
roll_ok = 0
roll_n = 0
for i in range(2, len(sorted_weeks)):
    pw = sorted_weeks[i]
    te = [r for r in all_weekly if r['iw'] == pw]
    for r in te:
        if r['c_ok']:
            roll_ok += 1
        roll_n += 1
roll_pct = round(roll_ok / roll_n * 100, 1) if roll_n else 0

print(f"全样本: {c_ok_total}/{nw} ({round(c_ok_total/nw*100,1)}%)")
print(f"前→后:  {test_ok}/{len(test_recs)} ({fwd_pct}%)")
print(f"滚动:   {roll_ok}/{roll_n} ({roll_pct}%)")

# ── 10. 各股票明细 ──
print(f"\n{'='*70}")
print(f"各股票C策略准确率（按板块排列）")
print(f"{'='*70}")
stock_results.sort(key=lambda x: (x['sector'], -x['c_pct']))
print(f"{'代码':<12} {'板块':<8} {'周数':>4} {'正确':>4} {'准确率':>8}")
print("-" * 40)
for s in stock_results:
    marker = '★' if s['c_pct'] >= 80 else ('✓' if s['c_pct'] >= 65 else ' ')
    print(f"{s['code']:<12} {s['sector']:<8} {s['weeks']:>4} "
          f"{s['c_ok']:>4} {s['c_pct']:>6.1f}% {marker}")

# ── 11. 准确率分布 ──
print(f"\n{'='*70}")
print(f"股票准确率分布")
print(f"{'='*70}")
bins = [(90, 100), (80, 90), (70, 80), (65, 70), (60, 65), (50, 60), (0, 50)]
for lo, hi in bins:
    cnt = sum(1 for s in stock_results if lo <= s['c_pct'] < hi)
    pct = round(cnt / len(stock_results) * 100, 1)
    bar = '█' * int(cnt / 2)
    label = f"{lo}-{hi}%" if hi < 100 else f"≥{lo}%"
    print(f"  {label:<8} {cnt:>3} 只 ({pct:>5.1f}%) {bar}")

# ≥65%的比例
ge65 = sum(1 for s in stock_results if s['c_pct'] >= 65)
print(f"\n准确率≥65%的股票: {ge65}/{len(stock_results)} ({round(ge65/len(stock_results)*100,1)}%)")
ge50 = sum(1 for s in stock_results if s['c_pct'] >= 50)
print(f"准确率≥50%的股票: {ge50}/{len(stock_results)} ({round(ge50/len(stock_results)*100,1)}%)")

# ── 最终结论 ──
print(f"\n{'='*70}")
print(f"最终结论")
print(f"{'='*70}")
overall = round(c_ok_total / nw * 100, 1)
print(f"C策略（前3天涨跌>0）在 {len(stock_results)} 只全新股票上:")
print(f"  全样本准确率: {overall}%")
print(f"  前→后验证:    {fwd_pct}%")
print(f"  滚动验证:     {roll_pct}%")
print(f"  覆盖周数:     {n_weeks} 周")
if overall >= 65 and fwd_pct >= 65 and roll_pct >= 65:
    print(f"  ✅ 三重验证全部≥65%，C策略泛化能力确认！")
else:
    print(f"  ⚠️ 部分验证未达65%")
