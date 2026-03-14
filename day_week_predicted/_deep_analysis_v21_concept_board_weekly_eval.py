#!/usr/bin/env python3
"""
v21 按概念板块维度评估周预测准确率

将每条周预测记录展开到其所属的所有概念板块，
按概念板块分组统计策略B和策略C的准确率，
找出哪些概念板块的周预测准确率最高/最低。

用法: python _deep_analysis_v21_concept_board_weekly_eval.py

数据源（全部本地，不依赖DB）：
1. 50只股票的日频回测结果 (backtest_prediction_enhanced_v9_50stocks_result.json)
2. stock_boards_map.json 中的概念板块映射
"""
import sys, os, json, math
from datetime import datetime
from collections import defaultdict

def mean(lst):
    return sum(lst) / len(lst) if lst else 0.0

def median(lst):
    if not lst: return 0.0
    s = sorted(lst)
    n = len(s)
    if n % 2 == 1: return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

def corrcoef(xs, ys):
    n = len(xs)
    if n < 3: return 0.0
    mx, my = mean(xs), mean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0: return 0.0
    return cov / (sx * sy)

P = print

# ══════════════════════════════════════════════════════════════
P("=" * 70)
P("  v21 按概念板块维度评估周预测准确率")
P("=" * 70)

# ── Part 1: 加载数据 ──
P("\n[1/9] 加载回测结果...")
with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    bt_data = json.load(f)
details = bt_data['逐日详情']
for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_dt'] = datetime.strptime(d['评分日'], '%Y-%m-%d')
    d['_wd'] = d['_dt'].weekday()
    d['_iso_week'] = d['_dt'].isocalendar()[:2]

stock_codes = sorted(set(d['代码'] for d in details))
all_dates = sorted(set(d['评分日'] for d in details))
P(f"  股票: {len(stock_codes)}, 记录: {len(details)}, "
  f"日期: {all_dates[0]}~{all_dates[-1]}")

P("[1/9] 加载概念板块映射...")
with open('data_results/industry_analysis/stock_boards_map.json') as f:
    boards_data = json.load(f)

stock_concepts = {}
concept_stocks = defaultdict(list)
for code in stock_codes:
    info = boards_data['stocks'].get(code, {})
    cbs = info.get('concept_boards', [])
    if cbs:
        stock_concepts[code] = cbs
        for cb in cbs:
            concept_stocks[cb].append(code)

n_with = sum(1 for c in stock_codes if c in stock_concepts)
P(f"  有概念板块: {n_with}/{len(stock_codes)}只, "
  f"概念板块总数: {len(concept_stocks)}")

# ── Part 2: 构建周数据 ──
P("\n[2/9] 构建周数据...")
stock_date_chg = {(d['代码'], d['评分日']): d['_actual'] for d in details}
bt_codes = set(stock_codes)

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
    d0 = days[0]
    weekly.append({
        'code': code, 'sector': d0['板块'], 'iw': iw,
        'n': len(days), 'wchg': wchg, 'wup': wchg >= 0,
        'mon_actual': d0['_actual'],
        'mon_comb': d0['融合信号'],
        'd3_chg': sum(d['_actual'] for d in days[:min(3, len(days))]),
        'mon_date': d0['评分日'],
        'concepts': stock_concepts.get(code, []),
    })

nw = len(weekly)
sector_up_rate = {}
for sec in set(r['sector'] for r in weekly):
    sr = [r for r in weekly if r['sector'] == sec]
    sector_up_rate[sec] = sum(1 for r in sr if r['wup']) / len(sr)
P(f"  周样本: {nw}, 有概念: {sum(1 for r in weekly if r['concepts'])}")

# ── Part 3: 概念板块代理信号 ──
P("\n[3/9] 计算概念板块代理信号...")

def concept_proxy(code, date_str, lookback=5):
    concepts = stock_concepts.get(code, [])
    if not concepts:
        return None
    try:
        di = all_dates.index(date_str)
    except ValueError:
        return None
    lb_dates = all_dates[max(0, di - lookback + 1):di + 1]
    board_moms, boards_up, boards_n = [], 0, 0
    for cb in concepts:
        peers = [c for c in concept_stocks.get(cb, [])
                 if c in bt_codes and c != code]
        if len(peers) < 2:
            continue
        chgs = []
        for dt in lb_dates:
            dc = [stock_date_chg.get((p, dt)) for p in peers]
            dc = [c for c in dc if c is not None]
            if dc:
                chgs.append(mean(dc))
        if not chgs:
            continue
        boards_n += 1
        m = mean(chgs)
        board_moms.append(m)
        if m > 0:
            boards_up += 1
    if boards_n == 0:
        return None
    return {
        'momentum': mean(board_moms),
        'consensus': boards_up / boards_n,
        'n_boards': boards_n,
    }

for r in weekly:
    r['csig'] = concept_proxy(r['code'], r['mon_date'], 5)
n_csig = sum(1 for r in weekly if r['csig'])
P(f"  有概念信号: {n_csig}/{nw} ({round(n_csig/nw*100,1)}%)")

# ── Part 4: 按概念板块统计周预测准确率 ──
P(f"\n[4/9] 按概念板块统计周预测准确率")
P("=" * 70)

def pred_B(r):
    if r['mon_actual'] > 0.5: return True
    if r['mon_actual'] < -0.5: return False
    return sector_up_rate.get(r['sector'], 0.5) > 0.5

def pred_C(r):
    return r['d3_chg'] > 0

def ok(pred_up, wup):
    return (pred_up and wup) or (not pred_up and not wup)

# 按概念板块展开
cb_stats = defaultdict(lambda: {'n': 0, 'b_ok': 0, 'c_ok': 0,
                                 'wup': 0, 'wchgs': [], 'codes': set()})
for r in weekly:
    for cb in r['concepts']:
        s = cb_stats[cb]
        s['n'] += 1
        s['codes'].add(r['code'])
        s['wchgs'].append(r['wchg'])
        if r['wup']: s['wup'] += 1
        if ok(pred_B(r), r['wup']): s['b_ok'] += 1
        if ok(pred_C(r), r['wup']): s['c_ok'] += 1

valid = {cb: s for cb, s in cb_stats.items() if s['n'] >= 5}
P(f"  概念板块总数: {len(cb_stats)}, 样本≥5: {len(valid)}")

# 按B准确率排序
P(f"\n{'─'*82}")
P(f"  按策略B准确率排序 (样本≥5, 共{len(valid)}个概念板块)")
P(f"{'─'*82}")
P(f"  {'概念板块':<18} {'样本':>5} {'股票':>4} {'周涨率':>7} "
  f"{'B准确率':>8} {'C准确率':>8} {'均周涨跌':>9}")
P(f"  {'─'*74}")

for cb, s in sorted(valid.items(), key=lambda x: -x[1]['b_ok']/x[1]['n']):
    n = s['n']
    P(f"  {cb:<18} {n:>5} {len(s['codes']):>4} "
      f"{s['wup']/n*100:>6.1f}% {s['b_ok']/n*100:>7.1f}% "
      f"{s['c_ok']/n*100:>7.1f}% {mean(s['wchgs']):>+8.2f}%")

# ── Part 5: 概念动量/共识度 vs 准确率 ──
P(f"\n[5/9] 概念动量/共识度 vs 周预测准确率")
P("=" * 70)

csig_recs = [r for r in weekly if r['csig']]
P(f"  有概念信号样本: {len(csig_recs)}")

# 相关性
moms = [r['csig']['momentum'] for r in csig_recs]
cons = [r['csig']['consensus'] for r in csig_recs]
wchgs = [r['wchg'] for r in csig_recs]
P(f"\n  概念动量 vs 周涨跌 相关系数: {corrcoef(moms, wchgs):.4f}")
P(f"  概念共识度 vs 周涨跌 相关系数: {corrcoef(cons, wchgs):.4f}")

P(f"\n  概念动量分组:")
P(f"  {'区间':<12} {'样本':>6} {'周涨率':>7} {'B准确':>7} "
  f"{'C准确':>7} {'均涨跌':>8}")
P(f"  {'─'*52}")
for lo, hi, lb in [(-99, -0.5, '<-0.5%'), (-0.5, 0, '-0.5~0'),
                    (0, 0.5, '0~0.5%'), (0.5, 99, '>0.5%')]:
    g = [r for r in csig_recs if lo <= r['csig']['momentum'] < hi]
    if len(g) < 3: continue
    gn = len(g)
    P(f"  {lb:<12} {gn:>6} {sum(r['wup'] for r in g)/gn*100:>6.1f}% "
      f"{sum(ok(pred_B(r),r['wup']) for r in g)/gn*100:>6.1f}% "
      f"{sum(ok(pred_C(r),r['wup']) for r in g)/gn*100:>6.1f}% "
      f"{mean([r['wchg'] for r in g]):>+7.2f}%")

P(f"\n  概念共识度分组:")
P(f"  {'区间':<12} {'样本':>6} {'周涨率':>7} {'B准确':>7} "
  f"{'C准确':>7} {'均涨跌':>8}")
P(f"  {'─'*52}")
for lo, hi, lb in [(0, 0.3, '<30%'), (0.3, 0.5, '30-50%'),
                    (0.5, 0.7, '50-70%'), (0.7, 1.01, '≥70%')]:
    g = [r for r in csig_recs if lo <= r['csig']['consensus'] < hi]
    if len(g) < 3: continue
    gn = len(g)
    P(f"  {lb:<12} {gn:>6} {sum(r['wup'] for r in g)/gn*100:>6.1f}% "
      f"{sum(ok(pred_B(r),r['wup']) for r in g)/gn*100:>6.1f}% "
      f"{sum(ok(pred_C(r),r['wup']) for r in g)/gn*100:>6.1f}% "
      f"{mean([r['wchg'] for r in g]):>+7.2f}%")

# ── Part 6: 概念板块内一致性 ──
P(f"\n[6/9] 概念板块内股票周涨跌一致性")
P("=" * 70)

bw_groups = defaultdict(list)
for r in weekly:
    for cb in r['concepts']:
        bw_groups[(cb, r['iw'])].append(r)

P(f"  {'概念板块':<18} {'周数':>5} {'全涨':>5} {'全跌':>5} "
  f"{'一致率':>7} {'均股票':>6}")
P(f"  {'─'*52}")

for cb in sorted(valid.keys()):
    wks = []
    for iw in set(r['iw'] for r in weekly):
        grp = bw_groups.get((cb, iw), [])
        if len(grp) < 2: continue
        nu = sum(1 for r in grp if r['wup'])
        wks.append({'n': len(grp), 'all_same': nu == 0 or nu == len(grp),
                     'all_up': nu == len(grp), 'all_dn': nu == 0})
    if len(wks) < 3: continue
    nw_ = len(wks)
    nc = sum(w['all_same'] for w in wks)
    P(f"  {cb:<18} {nw_:>5} {sum(w['all_up'] for w in wks):>5} "
      f"{sum(w['all_dn'] for w in wks):>5} "
      f"{nc/nw_*100:>6.1f}% {mean([w['n'] for w in wks]):>5.1f}")

# ── Part 7: 策略优化对比 ──
P(f"\n[7/9] 策略优化对比")
P("=" * 70)

concept_up_rate = {cb: s['wup']/s['n']
                   for cb, s in cb_stats.items() if s['n'] >= 5}

def pred_B_cb(r):
    """模糊区用概念板块基准率"""
    if r['mon_actual'] > 0.5: return True
    if r['mon_actual'] < -0.5: return False
    rates = [concept_up_rate[cb] for cb in r['concepts']
             if cb in concept_up_rate]
    if rates:
        return mean(rates) > 0.5
    return sector_up_rate.get(r['sector'], 0.5) > 0.5

def pred_B_csig(r):
    """模糊区用概念代理信号"""
    if r['mon_actual'] > 0.5: return True
    if r['mon_actual'] < -0.5: return False
    cs = r.get('csig')
    if cs:
        score = 0
        if cs['consensus'] > 0.65: score += 1
        elif cs['consensus'] < 0.35: score -= 1
        if cs['momentum'] > 0.3: score += 1
        elif cs['momentum'] < -0.3: score -= 1
        if score > 0: return True
        if score < 0: return False
    return sector_up_rate.get(r['sector'], 0.5) > 0.5

strats = [
    ('B原始(行业基准率)', pred_B),
    ('B+概念板块基准率', pred_B_cb),
    ('B+概念代理信号', pred_B_csig),
    ('C原始(前3天)', pred_C),
]

P(f"\n  全样本 ({nw} 周):")
P(f"  {'策略':<22} {'准确':>6}/{nw:<5} {'准确率':>8}")
P(f"  {'─'*44}")
for nm, fn in strats:
    o = sum(1 for r in weekly if ok(fn(r), r['wup']))
    P(f"  {nm:<22} {o:>6}/{nw:<5} {o/nw*100:>7.1f}%")

fuzzy = [r for r in weekly if abs(r['mon_actual']) <= 0.5]
P(f"\n  模糊区 ({len(fuzzy)} 周, |周一涨跌|≤0.5%):")
P(f"  {'策略':<22} {'准确':>6}/{len(fuzzy):<5} {'准确率':>8}")
P(f"  {'─'*44}")
for nm, fn in strats[:3]:
    o = sum(1 for r in fuzzy if ok(fn(r), r['wup']))
    P(f"  {nm:<22} {o:>6}/{len(fuzzy):<5} {o/len(fuzzy)*100:>7.1f}%")

# ── Part 8: LOWO交叉验证 ──
P(f"\n[8/9] LOWO交叉验证")
P("=" * 70)

sorted_wks = sorted(set(r['iw'] for r in weekly))

def lowo(records, pfn):
    tok, tn = 0, 0
    for hw in sorted_wks:
        tr = [r for r in records if r['iw'] != hw]
        te = [r for r in records if r['iw'] == hw]
        if not te or len(tr) < 10: continue
        # 训练集基准率
        t_sur = {}
        for sec in set(r['sector'] for r in tr):
            sr = [r for r in tr if r['sector'] == sec]
            t_sur[sec] = sum(1 for r in sr if r['wup']) / len(sr)
        t_cur = defaultdict(lambda: [0, 0])
        for r in tr:
            for cb in r['concepts']:
                t_cur[cb][1] += 1
                if r['wup']: t_cur[cb][0] += 1
        t_cr = {cb: v[0]/v[1] for cb, v in t_cur.items() if v[1] >= 5}
        for r in te:
            p = pfn(r, t_sur, t_cr)
            if ok(p, r['wup']): tok += 1
            tn += 1
    return round(tok/tn*100, 1) if tn else 0, tn

def lw_B(r, sur, cr):
    if r['mon_actual'] > 0.5: return True
    if r['mon_actual'] < -0.5: return False
    return sur.get(r['sector'], 0.5) > 0.5

def lw_B_cb(r, sur, cr):
    if r['mon_actual'] > 0.5: return True
    if r['mon_actual'] < -0.5: return False
    rates = [cr[cb] for cb in r['concepts'] if cb in cr]
    if rates: return mean(rates) > 0.5
    return sur.get(r['sector'], 0.5) > 0.5

def lw_B_csig(r, sur, cr):
    if r['mon_actual'] > 0.5: return True
    if r['mon_actual'] < -0.5: return False
    cs = r.get('csig')
    if cs:
        sc = 0
        if cs['consensus'] > 0.65: sc += 1
        elif cs['consensus'] < 0.35: sc -= 1
        if cs['momentum'] > 0.3: sc += 1
        elif cs['momentum'] < -0.3: sc -= 1
        if sc > 0: return True
        if sc < 0: return False
    return sur.get(r['sector'], 0.5) > 0.5

def lw_C(r, sur, cr):
    return r['d3_chg'] > 0

lw_strats = [
    ('B原始', lw_B),
    ('B+概念基准率', lw_B_cb),
    ('B+概念代理信号', lw_B_csig),
    ('C原始', lw_C),
]

P(f"\n  {'策略':<18} {'LOWO准确率':>12} {'样本':>8}")
P(f"  {'─'*42}")
for nm, fn in lw_strats:
    acc, n = lowo(weekly, fn)
    P(f"  {nm:<18} {acc:>11.1f}% {n:>8}")

# ── Part 9: 总结 ──
P(f"\n[9/9] 总结")
P("=" * 70)

P(f"\n  回测: {len(stock_codes)}只股票, {nw}周样本")
P(f"  概念覆盖: {n_with}只有概念, {len(concept_stocks)}个概念板块, "
  f"{len(valid)}个样本≥5")

P(f"\n  全样本准确率:")
for nm, fn in strats:
    o = sum(1 for r in weekly if ok(fn(r), r['wup']))
    P(f"    {nm:<22} {o/nw*100:.1f}%")

if valid:
    ba = [s['b_ok']/s['n']*100 for s in valid.values()]
    ca = [s['c_ok']/s['n']*100 for s in valid.values()]
    P(f"\n  概念板块维度准确率分布 ({len(valid)}个板块):")
    P(f"    B策略: 均值{mean(ba):.1f}%, 中位{median(ba):.1f}%, "
      f"[{min(ba):.1f}%~{max(ba):.1f}%]")
    P(f"    C策略: 均值{mean(ca):.1f}%, 中位{median(ca):.1f}%, "
      f"[{min(ca):.1f}%~{max(ca):.1f}%]")

    hb = [(cb, s['b_ok']/s['n']*100) for cb, s in valid.items()
          if s['b_ok']/s['n']*100 >= 70]
    if hb:
        P(f"\n  B策略≥70%的概念板块 ({len(hb)}个):")
        for cb, a in sorted(hb, key=lambda x: -x[1]):
            s = valid[cb]
            names = [boards_data['stocks'].get(c, {}).get('name', c)
                     for c in sorted(s['codes'])[:4]]
            P(f"    {cb:<18} {a:.1f}% ({s['b_ok']}/{s['n']}) "
              f"股票: {', '.join(names)}")

    hc = [(cb, s['c_ok']/s['n']*100) for cb, s in valid.items()
          if s['c_ok']/s['n']*100 >= 85]
    if hc:
        P(f"\n  C策略≥85%的概念板块 ({len(hc)}个):")
        for cb, a in sorted(hc, key=lambda x: -x[1]):
            s = valid[cb]
            names = [boards_data['stocks'].get(c, {}).get('name', c)
                     for c in sorted(s['codes'])[:4]]
            P(f"    {cb:<18} {a:.1f}% ({s['c_ok']}/{s['n']}) "
              f"股票: {', '.join(names)}")

P(f"\n{'='*70}")
P(f"  分析完成")
P(f"{'='*70}")
