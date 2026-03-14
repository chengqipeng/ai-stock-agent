#!/usr/bin/env python3
"""
v19e 周预测 - 严格无泄露版本

关键区分：
- 事前特征（周初可知）：周一的信号、上周数据、历史统计
- 事后特征（周末才知）：周内涨跌天数、max_up/dn、avg_z等
- 实时特征（周中可知）：前N天的实际涨跌

v19d发现：
1. up_day_ratio/max_up/max_dn等事后特征LOWO达80%+，但有数据泄露
2. 前2天实际涨跌预测整周：训练72.3%，测试75.3%（无泄露，但需等到周三）
3. 前3天实际涨跌>0→涨：训练77.2%，测试77.3%（需等到周四）
4. 周一实际涨跌>0→涨：训练63.0%，测试69.9%（周一收盘即可知）

本脚本严格区分三种预测时点：
A. 周初预测（周一开盘前）：只用上周数据+周一信号
B. 周一收盘预测：加上周一实际涨跌
C. 周三收盘预测：加上前3天实际涨跌
"""
import json
from collections import defaultdict
from datetime import datetime

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

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']

# ── 构建周数据 ──
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
    n = len(days)
    d0 = days[0]

    rec = {
        'code': code, 'sector': d0['板块'], 'iw': iw,
        'n': n, 'wchg': wchg, 'wup': wchg >= 0, 'wdn': wchg <= 0,
        'days': days,
    }

    # ═══ 事前特征（周一开盘前可知）═══
    rec['mon_comb'] = d0['融合信号']
    rec['mon_tech'] = d0['技术信号']
    rec['mon_peer'] = d0['同行信号']
    rec['mon_rs'] = d0['RS信号']
    rec['mon_us'] = d0['美股隔夜']
    rec['mon_z'] = d0['z_today']
    rec['mon_vol'] = d0['波动率状态']
    rec['mon_score'] = d0['评分']
    rec['mon_conf'] = d0['置信度']
    rec['mon_pred'] = d0['预测方向']

    # ═══ 周一收盘后可知 ═══
    rec['mon_actual'] = d0['_actual']
    rec['mon_correct'] = 1 if d0['宽松正确'] == '✓' else 0

    # ═══ 前2天收盘后可知 ═══
    if n >= 2:
        rec['d2_chg'] = sum(d['_actual'] for d in days[:2])
        rec['d2_avg_comb'] = sum(d['融合信号'] for d in days[:2]) / 2
        rec['d2_up_count'] = sum(1 for d in days[:2] if d['_actual'] > 0)
    else:
        rec['d2_chg'] = d0['_actual']
        rec['d2_avg_comb'] = d0['融合信号']
        rec['d2_up_count'] = 1 if d0['_actual'] > 0 else 0

    # ═══ 前3天收盘后可知 ═══
    if n >= 3:
        rec['d3_chg'] = sum(d['_actual'] for d in days[:3])
        rec['d3_avg_comb'] = sum(d['融合信号'] for d in days[:3]) / 3
        rec['d3_up_count'] = sum(1 for d in days[:3] if d['_actual'] > 0)
        rec['d3_avg_z'] = sum(d['z_today'] for d in days[:3]) / 3
        rec['d3_avg_score'] = sum(d['评分'] for d in days[:3]) / 3
    else:
        rec['d3_chg'] = sum(d['_actual'] for d in days[:min(3, n)])
        rec['d3_avg_comb'] = sum(d['融合信号'] for d in days[:min(3, n)]) / min(3, n)
        rec['d3_up_count'] = sum(1 for d in days[:min(3, n)] if d['_actual'] > 0)
        rec['d3_avg_z'] = sum(d['z_today'] for d in days[:min(3, n)]) / min(3, n)
        rec['d3_avg_score'] = sum(d['评分'] for d in days[:min(3, n)]) / min(3, n)

    weekly.append(rec)

nw = len(weekly)
sorted_weeks = sorted(set(r['iw'] for r in weekly))
mid = len(sorted_weeks) // 2
first_half_weeks = set(sorted_weeks[:mid])
second_half_weeks = set(sorted_weeks[mid:])

# 添加上周信息
week_sector_chg = {}
for r in weekly:
    week_sector_chg[(r['sector'], r['iw'])] = r['wchg']

for r in weekly:
    idx = sorted_weeks.index(r['iw'])
    if idx > 0:
        prev_iw = sorted_weeks[idx - 1]
        r['prev_sector_chg'] = week_sector_chg.get((r['sector'], prev_iw), 0)
    else:
        r['prev_sector_chg'] = 0

train = [r for r in weekly if r['iw'] in first_half_weeks]
test = [r for r in weekly if r['iw'] in second_half_weeks]

print("=" * 70)
print("  v19e 周预测 - 严格无泄露版本")
print("=" * 70)
print(f"周样本: {nw}, 训练: {len(train)}, 测试: {len(test)}")


# ═══════════════════════════════════════════════════════════════
# Part 1: 三种时点的特征集定义
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 1: 三种预测时点")
print("=" * 70)

# 时点A: 周一开盘前
feats_A = ['mon_comb', 'mon_tech', 'mon_peer', 'mon_rs', 'mon_us',
           'mon_z', 'mon_vol', 'mon_score', 'prev_sector_chg']

# 时点B: 周一收盘后
feats_B = feats_A + ['mon_actual', 'mon_correct']

# 时点C: 周三收盘后（前3天）
feats_C = feats_B + ['d2_chg', 'd3_chg', 'd3_up_count', 'd3_avg_z', 'd3_avg_score', 'd3_avg_comb']

print(f"时点A(周一开盘前): {len(feats_A)}个特征")
print(f"时点B(周一收盘后): {len(feats_B)}个特征")
print(f"时点C(周三收盘后): {len(feats_C)}个特征")


# ═══════════════════════════════════════════════════════════════
# Part 2: LOWO交叉验证框架
# ═══════════════════════════════════════════════════════════════

def lowo_cv(records, predict_fn, sorted_wks):
    total_ok = 0
    total_n = 0
    for hold_wk in sorted_wks:
        tr = [r for r in records if r['iw'] != hold_wk]
        te = [r for r in records if r['iw'] == hold_wk]
        if not te or len(tr) < 10:
            continue
        for r in te:
            pred = predict_fn(r, tr)
            if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
                total_ok += 1
            total_n += 1
    return total_ok / total_n * 100 if total_n > 0 else 0, total_n

def half_test(records_train, records_test, predict_fn):
    ok = 0
    for r in records_test:
        pred = predict_fn(r, records_train)
        if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
            ok += 1
    return ok / len(records_test) * 100 if records_test else 0, len(records_test)

def rolling_test(records, predict_fn, sorted_wks, min_train=2):
    total_ok = 0
    total_n = 0
    for i in range(min_train, len(sorted_wks)):
        tw = set(sorted_wks[:i])
        pw = sorted_wks[i]
        tr = [r for r in records if r['iw'] in tw]
        te = [r for r in records if r['iw'] == pw]
        for r in te:
            pred = predict_fn(r, tr)
            if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
                total_ok += 1
            total_n += 1
    return total_ok / total_n * 100 if total_n > 0 else 0, total_n


# ═══════════════════════════════════════════════════════════════
# Part 3: 各时点策略构建与评估
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 2-3: 各时点策略评估")
print("=" * 70)

def make_vote_strat(feat_list, top_k=3):
    """构建多特征投票策略"""
    def strat(r, train_r):
        sr = [t for t in train_r if t['sector'] == r['sector']]
        if len(sr) < 5:
            up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
            return 'up' if up_rate > 0.5 else 'dn'
        feat_accs = []
        for feat in feat_list:
            vals = sorted(set(t[feat] for t in sr))
            if len(vals) < 3:
                continue
            best_acc = 0
            best_th = 0
            best_dir = 'gt'
            for th in vals[1:-1]:
                for d in ['gt', 'le']:
                    ok = sum(1 for t in sr if
                             ((t[feat] > th if d == 'gt' else t[feat] <= th) and t['wup']) or
                             ((t[feat] <= th if d == 'gt' else t[feat] > th) and t['wdn']))
                    acc = ok / len(sr) * 100
                    if acc > best_acc:
                        best_acc = acc
                        best_th = th
                        best_dir = d
            feat_accs.append((feat, best_th, best_dir, best_acc))
        feat_accs.sort(key=lambda x: -x[3])
        top = feat_accs[:top_k]
        if not top:
            up_rate = sum(1 for t in sr if t['wup']) / len(sr)
            return 'up' if up_rate > 0.5 else 'dn'
        votes_up = 0
        votes_dn = 0
        for feat, th, d, _ in top:
            v = r[feat]
            if d == 'gt':
                if v > th:
                    votes_up += 1
                else:
                    votes_dn += 1
            else:
                if v <= th:
                    votes_up += 1
                else:
                    votes_dn += 1
        return 'up' if votes_up >= votes_dn else 'dn'
    return strat


def make_single_best_strat(feat_list):
    """板块最优单特征策略"""
    def strat(r, train_r):
        sr = [t for t in train_r if t['sector'] == r['sector']]
        if len(sr) < 5:
            up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
            return 'up' if up_rate > 0.5 else 'dn'
        best_overall = 0
        best_feat = None
        best_th = 0
        best_dir = 'gt'
        for feat in feat_list:
            vals = sorted(set(t[feat] for t in sr))
            if len(vals) < 3:
                continue
            for th in vals[1:-1]:
                for d in ['gt', 'le']:
                    ok = sum(1 for t in sr if
                             ((t[feat] > th if d == 'gt' else t[feat] <= th) and t['wup']) or
                             ((t[feat] <= th if d == 'gt' else t[feat] > th) and t['wdn']))
                    acc = ok / len(sr) * 100
                    if acc > best_overall:
                        best_overall = acc
                        best_feat = feat
                        best_th = th
                        best_dir = d
        if best_feat is None:
            up_rate = sum(1 for t in sr if t['wup']) / len(sr)
            return 'up' if up_rate > 0.5 else 'dn'
        v = r[best_feat]
        if best_dir == 'gt':
            return 'up' if v > best_th else 'dn'
        else:
            return 'up' if v <= best_th else 'dn'
    return strat


# 简单策略
def strat_all_up(r, tr):
    return 'up'

def strat_base_rate(r, tr):
    sr = [t for t in tr if t['sector'] == r['sector']]
    up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
    return 'up' if up_rate > 0.5 else 'dn'

def strat_mon_actual(r, tr, th=0):
    """周一涨跌>th→涨"""
    return 'up' if r['mon_actual'] > th else 'dn'

def strat_d2_chg(r, tr, th=0):
    """前2天涨跌>th→涨"""
    return 'up' if r['d2_chg'] > th else 'dn'

def strat_d3_chg(r, tr, th=0):
    """前3天涨跌>th→涨"""
    return 'up' if r['d3_chg'] > th else 'dn'

# 周一涨跌+板块基准率混合
def strat_mon_hybrid(r, tr, th=0.5):
    """周一涨跌强信号用实际，弱信号用基准率"""
    sr = [t for t in tr if t['sector'] == r['sector']]
    up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
    if r['mon_actual'] > th:
        return 'up'
    elif r['mon_actual'] < -th:
        return 'dn'
    return 'up' if up_rate > 0.5 else 'dn'

# 前3天涨跌+板块基准率混合
def strat_d3_hybrid(r, tr, th=0.5):
    sr = [t for t in tr if t['sector'] == r['sector']]
    up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
    if r['d3_chg'] > th:
        return 'up'
    elif r['d3_chg'] < -th:
        return 'dn'
    return 'up' if up_rate > 0.5 else 'dn'


# ═══ 评估所有策略 ═══
results = {}

strategies = [
    # 基线
    ("全涨", strat_all_up, "基线"),
    ("板块基准率", strat_base_rate, "基线"),
    # 时点A: 周一开盘前
    ("A:投票3(事前)", make_vote_strat(feats_A, 3), "A"),
    ("A:投票5(事前)", make_vote_strat(feats_A, 5), "A"),
    ("A:最优单特征(事前)", make_single_best_strat(feats_A), "A"),
    # 时点B: 周一收盘后
    ("B:周一涨跌>0", lambda r, tr: strat_mon_actual(r, tr, 0), "B"),
    ("B:周一涨跌>-0.5", lambda r, tr: strat_mon_actual(r, tr, -0.5), "B"),
    ("B:周一混合(0.5)", lambda r, tr: strat_mon_hybrid(r, tr, 0.5), "B"),
    ("B:投票3(+周一)", make_vote_strat(feats_B, 3), "B"),
    ("B:投票5(+周一)", make_vote_strat(feats_B, 5), "B"),
    ("B:最优单特征(+周一)", make_single_best_strat(feats_B), "B"),
    # 时点C: 周三收盘后
    ("C:前3天涨跌>0", lambda r, tr: strat_d3_chg(r, tr, 0), "C"),
    ("C:前3天涨跌>0.5", lambda r, tr: strat_d3_chg(r, tr, 0.5), "C"),
    ("C:前3天混合(0.5)", lambda r, tr: strat_d3_hybrid(r, tr, 0.5), "C"),
    ("C:投票3(+前3天)", make_vote_strat(feats_C, 3), "C"),
    ("C:投票5(+前3天)", make_vote_strat(feats_C, 5), "C"),
    ("C:最优单特征(+前3天)", make_single_best_strat(feats_C), "C"),
]

print(f"\n{'策略':<24} {'LOWO':>6} {'前→后':>6} {'滚动':>6} {'时点':>4}")
print("-" * 55)

for name, fn, timepoint in strategies:
    lowo_acc, _ = lowo_cv(weekly, fn, sorted_weeks)
    half_acc, _ = half_test(train, test, fn)
    roll_acc, _ = rolling_test(weekly, fn, sorted_weeks)
    results[name] = {
        'lowo': lowo_acc, 'half': half_acc, 'roll': roll_acc, 'tp': timepoint
    }
    mark = " ★" if min(lowo_acc, half_acc, roll_acc) >= 65 else ""
    print(f"{name:<24} {lowo_acc:>5.1f}% {half_acc:>5.1f}% {roll_acc:>5.1f}%  {timepoint}{mark}")


# ═══════════════════════════════════════════════════════════════
# Part 4: 板块级明细（最优策略）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 4: 板块级明细")
print("=" * 70)

# 选出每个时点的最优策略
for timepoint in ['A', 'B', 'C']:
    tp_results = {k: v for k, v in results.items() if v['tp'] == timepoint}
    if not tp_results:
        continue
    best_name = max(tp_results, key=lambda k: tp_results[k]['lowo'])
    best_fn = [fn for n, fn, tp in strategies if n == best_name][0]

    print(f"\n── 时点{timepoint} 最优: {best_name} ──")
    print(f"{'板块':<8} {'LOWO':>6} {'前→后':>6}")
    print("-" * 25)

    for sector in sectors:
        sr = [r for r in weekly if r['sector'] == sector]
        s_train = [r for r in train if r['sector'] == sector]
        s_test = [r for r in test if r['sector'] == sector]

        # LOWO
        ok = 0
        n = 0
        for hold_wk in sorted_weeks:
            tr = [r for r in sr if r['iw'] != hold_wk]
            te = [r for r in sr if r['iw'] == hold_wk]
            if not te or len(tr) < 5:
                continue
            for r in te:
                pred = best_fn(r, tr)
                if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
                    ok += 1
                n += 1
        lowo_acc = ok / n * 100 if n > 0 else 0

        # 前→后
        ok2 = 0
        for r in s_test:
            pred = best_fn(r, s_train)
            if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
                ok2 += 1
        half_acc = ok2 / len(s_test) * 100 if s_test else 0

        print(f"{sector:<8} {lowo_acc:>5.1f}% {half_acc:>5.1f}%")


# ═══════════════════════════════════════════════════════════════
# Part 5: 板块自适应最优策略（每个板块选最优时点+策略）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 5: 板块自适应最优策略")
print("=" * 70)

# 对每个板块，在LOWO中搜索最优的(特征, 阈值, 方向)
for timepoint, feat_list in [('A', feats_A), ('B', feats_B), ('C', feats_C)]:
    print(f"\n── 时点{timepoint} 板块最优 ──")
    total_ok = 0
    total_n = 0
    total_ok_half = 0
    total_n_half = 0

    for sector in sectors:
        sr = [r for r in weekly if r['sector'] == sector]
        s_train = [r for r in train if r['sector'] == sector]
        s_test = [r for r in test if r['sector'] == sector]

        # LOWO搜索最优单特征
        strat = make_single_best_strat(feat_list)
        ok = 0
        n = 0
        for hold_wk in sorted_weeks:
            tr = [r for r in sr if r['iw'] != hold_wk]
            te = [r for r in sr if r['iw'] == hold_wk]
            if not te or len(tr) < 5:
                continue
            for r in te:
                pred = strat(r, tr)
                if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
                    ok += 1
                n += 1
        lowo_acc = ok / n * 100 if n > 0 else 0
        total_ok += ok
        total_n += n

        # 前→后
        ok2 = 0
        for r in s_test:
            pred = strat(r, s_train)
            if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
                ok2 += 1
        half_acc = ok2 / len(s_test) * 100 if s_test else 0
        total_ok_half += ok2
        total_n_half += len(s_test)

        print(f"  {sector:<8} LOWO={lowo_acc:>5.1f}% 前→后={half_acc:>5.1f}%")

    print(f"  {'合计':<8} LOWO={total_ok/total_n*100:>5.1f}% 前→后={total_ok_half/total_n_half*100:>5.1f}%")


# ═══════════════════════════════════════════════════════════════
# Part 6: 最终结论
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 6: 最终结论")
print("=" * 70)

print("""
预测时点说明:
  A = 周一开盘前（只用历史+周一信号）
  B = 周一收盘后（加上周一实际涨跌）
  C = 周三收盘后（加上前3天实际涨跌）

评估方式:
  LOWO = Leave-One-Week-Out交叉验证（最严格）
  前→后 = 前6周训练→后6周测试
  滚动 = 前N周训练→第N+1周预测
""")

# 按时点汇总最优
for tp in ['基线', 'A', 'B', 'C']:
    tp_res = {k: v for k, v in results.items() if v['tp'] == tp}
    if not tp_res:
        continue
    print(f"时点{tp}:")
    for name in sorted(tp_res, key=lambda k: -tp_res[k]['lowo']):
        r = tp_res[name]
        mark = " ★★★" if min(r['lowo'], r['half'], r['roll']) >= 65 else \
               " ★★" if min(r['lowo'], r['half'], r['roll']) >= 60 else \
               " ★" if r['lowo'] >= 65 else ""
        print(f"  {name:<24} LOWO={r['lowo']:>5.1f}% 前→后={r['half']:>5.1f}% 滚动={r['roll']:>5.1f}%{mark}")
    print()

# 是否达标
target = 65
achieved = [k for k, v in results.items() if min(v['lowo'], v['half'], v['roll']) >= target]
if achieved:
    print(f"✅ 达标策略(三种验证均≥{target}%):")
    for name in achieved:
        r = results[name]
        print(f"  {name}: LOWO={r['lowo']:.1f}% 前→后={r['half']:.1f}% 滚动={r['roll']:.1f}%")
else:
    print(f"❌ 无策略在三种验证方式上均达到{target}%")
    # 找最接近的
    best = max(results.items(), key=lambda x: min(x[1]['lowo'], x[1]['half'], x[1]['roll']))
    print(f"最接近: {best[0]} min={min(best[1]['lowo'], best[1]['half'], best[1]['roll']):.1f}%")

print("\n完成。")
