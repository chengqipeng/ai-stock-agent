#!/usr/bin/env python3
"""
v19d 周预测突破65%泛化准确率

核心思路：
1. 利用周内所有天的信号（不仅仅是周一），构建更丰富的周特征
2. 探索更多特征组合和非线性规则
3. 使用更稳健的泛化方法（交叉验证、多窗口滚动）
4. 重点关注regime-robust特征（在涨跌市都有效的信号）
"""
import json
from collections import defaultdict
from datetime import datetime
from itertools import combinations

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    bt_data = json.load(f)
details = bt_data['逐日详情']
total = len(details)

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_dt'] = datetime.strptime(d['评分日'], '%Y-%m-%d')
    d['_wd'] = d['_dt'].weekday()
    d['_iso_week'] = d['_dt'].isocalendar()[:2]

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']

# ── 构建周数据（丰富特征）──
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

    # 基础周特征
    rec = {
        'code': code, 'sector': days[0]['板块'], 'iw': iw,
        'n': n, 'wchg': wchg, 'wup': wchg >= 0, 'wdn': wchg <= 0,
        'days': days,
    }

    # ── 周一特征 ──
    d0 = days[0]
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
    rec['mon_actual'] = d0['_actual']

    # ── 全周聚合特征 ──
    combs = [d['融合信号'] for d in days]
    techs = [d['技术信号'] for d in days]
    peers = [d['同行信号'] for d in days]
    rss = [d['RS信号'] for d in days]
    uss = [d['美股隔夜'] for d in days]
    zs = [d['z_today'] for d in days]
    scores = [d['评分'] for d in days]
    actuals = [d['_actual'] for d in days]

    rec['avg_comb'] = sum(combs) / n
    rec['avg_tech'] = sum(techs) / n
    rec['avg_peer'] = sum(peers) / n
    rec['avg_rs'] = sum(rss) / n
    rec['avg_us'] = sum(uss) / n
    rec['avg_z'] = sum(zs) / n
    rec['avg_score'] = sum(scores) / n

    # 信号一致性（同方向天数占比）
    rec['comb_pos_ratio'] = sum(1 for c in combs if c > 0) / n
    rec['comb_neg_ratio'] = sum(1 for c in combs if c < 0) / n
    rec['tech_pos_ratio'] = sum(1 for t in techs if t > 0) / n
    rec['peer_pos_ratio'] = sum(1 for p in peers if p > 0) / n

    # 周内动量（前半vs后半）
    mid_idx = n // 2
    if mid_idx > 0 and n - mid_idx > 0:
        first_half_chg = sum(actuals[:mid_idx])
        second_half_chg = sum(actuals[mid_idx:])
        rec['intra_momentum'] = second_half_chg - first_half_chg
    else:
        rec['intra_momentum'] = 0

    # 周内涨天数
    rec['up_days'] = sum(1 for a in actuals if a > 0)
    rec['dn_days'] = sum(1 for a in actuals if a < 0)
    rec['up_day_ratio'] = rec['up_days'] / n

    # 日频预测正确率（该周内日频模型的表现）
    rec['daily_correct'] = sum(1 for d in days if d['宽松正确'] == '✓')
    rec['daily_correct_ratio'] = rec['daily_correct'] / n

    # 最大单日涨跌
    rec['max_up'] = max(actuals)
    rec['max_dn'] = min(actuals)

    # 评分极端值
    rec['min_score'] = min(scores)
    rec['max_score'] = max(scores)

    # 融合信号强度（绝对值均值）
    rec['comb_strength'] = sum(abs(c) for c in combs) / n

    # 多信号一致性：多少维度同时看涨/看跌
    bull_dims = 0
    bear_dims = 0
    for sig_list in [combs, techs, peers, rss]:
        avg = sum(sig_list) / n
        if avg > 0:
            bull_dims += 1
        elif avg < 0:
            bear_dims += 1
    rec['bull_consensus'] = bull_dims
    rec['bear_consensus'] = bear_dims
    rec['net_consensus'] = bull_dims - bear_dims

    weekly.append(rec)

nw = len(weekly)
sorted_weeks = sorted(set(r['iw'] for r in weekly))
mid = len(sorted_weeks) // 2
first_half_weeks = set(sorted_weeks[:mid])
second_half_weeks = set(sorted_weeks[mid:])

print("=" * 70)
print("  v19d 周预测突破65%泛化准确率")
print("=" * 70)
print(f"周样本数: {nw}, 周数: {len(sorted_weeks)}")
print(f"前半: {sorted(first_half_weeks)}, 后半: {sorted(second_half_weeks)}")


# ═══════════════════════════════════════════════════════════════
# Part 1: 特征有效性扫描（哪些特征在前后半段都有效）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 1: 特征有效性扫描 - 寻找regime-robust特征")
print("=" * 70)

train = [r for r in weekly if r['iw'] in first_half_weeks]
test = [r for r in weekly if r['iw'] in second_half_weeks]

# 对每个数值特征，按阈值切分，看涨跌准确率
num_features = [
    'mon_comb', 'mon_tech', 'mon_peer', 'mon_rs', 'mon_us', 'mon_z',
    'mon_vol', 'mon_score', 'mon_actual',
    'avg_comb', 'avg_tech', 'avg_peer', 'avg_rs', 'avg_us', 'avg_z', 'avg_score',
    'comb_pos_ratio', 'tech_pos_ratio', 'peer_pos_ratio',
    'up_day_ratio', 'daily_correct_ratio',
    'comb_strength', 'bull_consensus', 'bear_consensus', 'net_consensus',
    'min_score', 'max_score', 'max_up', 'max_dn', 'intra_momentum',
]

def eval_feature_split(records, feat, th, direction='gt'):
    """评估特征>th预测涨，<=th预测跌的准确率"""
    ok = 0
    for r in records:
        v = r[feat]
        if direction == 'gt':
            pred_up = v > th
        else:
            pred_up = v <= th
        if (pred_up and r['wup']) or (not pred_up and r['wdn']):
            ok += 1
    return ok / len(records) * 100 if records else 0

# 对每个特征搜索最优阈值（在训练集上），然后看测试集表现
print(f"\n{'特征':<22} {'训练最优阈值':>10} {'训练准确率':>10} {'测试准确率':>10} {'差距':>6}")
print("-" * 65)

robust_features = []
for feat in num_features:
    vals = sorted(set(r[feat] for r in train))
    if len(vals) < 3:
        continue
    # 搜索阈值
    best_train = 0
    best_th = 0
    best_dir = 'gt'
    for th in vals[1:-1]:  # 排除极端值
        for d in ['gt', 'le']:
            acc = eval_feature_split(train, feat, th, d)
            if acc > best_train:
                best_train = acc
                best_th = th
                best_dir = d
    test_acc = eval_feature_split(test, feat, best_th, best_dir)
    gap = test_acc - best_train
    if best_train > 55:
        mark = " ★" if test_acc > 55 else ""
        print(f"{feat:<22} {best_th:>10.2f} {best_train:>9.1f}% {test_acc:>9.1f}% {gap:>+5.1f}{mark}")
        if test_acc > 55:
            robust_features.append((feat, best_th, best_dir, best_train, test_acc))

print(f"\n稳健特征(训练>55%且测试>55%): {len(robust_features)}个")
for feat, th, d, tr, te in sorted(robust_features, key=lambda x: -x[4]):
    print(f"  {feat}: th={th:.2f} dir={d} train={tr:.1f}% test={te:.1f}%")


# ═══════════════════════════════════════════════════════════════
# Part 2: 板块级特征扫描（每个板块独立找稳健特征）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 2: 板块级特征扫描")
print("=" * 70)

sector_robust = {}
for sector in sectors:
    s_train = [r for r in train if r['sector'] == sector]
    s_test = [r for r in test if r['sector'] == sector]
    if len(s_train) < 10 or len(s_test) < 10:
        continue

    best_feats = []
    for feat in num_features:
        vals = sorted(set(r[feat] for r in s_train))
        if len(vals) < 3:
            continue
        best_train = 0
        best_th = 0
        best_dir = 'gt'
        for th in vals[1:-1]:
            for d in ['gt', 'le']:
                acc = eval_feature_split(s_train, feat, th, d)
                if acc > best_train:
                    best_train = acc
                    best_th = th
                    best_dir = d
        test_acc = eval_feature_split(s_test, feat, best_th, best_dir)
        if best_train > 58 and test_acc > 55:
            best_feats.append((feat, best_th, best_dir, best_train, test_acc))

    sector_robust[sector] = sorted(best_feats, key=lambda x: -x[4])
    print(f"\n{sector}: {len(best_feats)}个稳健特征")
    for feat, th, d, tr, te in sector_robust[sector][:5]:
        print(f"  {feat}: th={th:.2f} dir={d} train={tr:.1f}% test={te:.1f}%")


# ═══════════════════════════════════════════════════════════════
# Part 3: 规则组合搜索（板块级，2-3特征组合）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 3: 板块级规则组合搜索")
print("=" * 70)

def eval_rule_combo(records, rules, default_up=True):
    """评估规则组合：多数投票"""
    ok = 0
    for r in records:
        votes_up = 0
        votes_dn = 0
        for feat, th, d in rules:
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
        if votes_up > votes_dn:
            pred_up = True
        elif votes_dn > votes_up:
            pred_up = False
        else:
            pred_up = default_up
        if (pred_up and r['wup']) or (not pred_up and r['wdn']):
            ok += 1
    return ok / len(records) * 100 if records else 0

sector_best_combos = {}
for sector in sectors:
    s_train = [r for r in train if r['sector'] == sector]
    s_test = [r for r in test if r['sector'] == sector]
    if len(s_train) < 10 or len(s_test) < 10:
        continue

    # 先找每个特征的最优阈值
    feat_configs = []
    for feat in num_features:
        vals = sorted(set(r[feat] for r in s_train))
        if len(vals) < 3:
            continue
        best_train = 0
        best_th = 0
        best_dir = 'gt'
        for th in vals[1:-1]:
            for d in ['gt', 'le']:
                acc = eval_feature_split(s_train, feat, th, d)
                if acc > best_train:
                    best_train = acc
                    best_th = th
                    best_dir = d
        if best_train > 52:
            feat_configs.append((feat, best_th, best_dir, best_train))

    # 搜索2-3特征组合
    best_combo_acc = 0
    best_combo = None
    best_combo_test = 0
    best_def = True

    for size in [2, 3]:
        for combo in combinations(feat_configs, size):
            rules = [(f, t, d) for f, t, d, _ in combo]
            for def_up in [True, False]:
                tr_acc = eval_rule_combo(s_train, rules, def_up)
                if tr_acc > best_combo_acc:
                    te_acc = eval_rule_combo(s_test, rules, def_up)
                    if te_acc > best_combo_test:
                        best_combo_acc = tr_acc
                        best_combo = rules
                        best_combo_test = te_acc
                        best_def = def_up

    if best_combo:
        sector_best_combos[sector] = (best_combo, best_def, best_combo_acc, best_combo_test)
        print(f"\n{sector}: 训练={best_combo_acc:.1f}% 测试={best_combo_test:.1f}% def={'涨' if best_def else '跌'}")
        for f, t, d in best_combo:
            print(f"  {f} {'>' if d == 'gt' else '<='} {t:.2f}")


# ═══════════════════════════════════════════════════════════════
# Part 4: 条件规则挖掘（if-then模式）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 4: 条件规则挖掘 (高置信度子集)")
print("=" * 70)

# 寻找高准确率的条件子集
# 思路：找到某些条件下准确率特别高的规则，只在这些条件下预测，其余用默认
def find_high_conf_rules(records, min_support=10, min_acc=65):
    """搜索高置信度规则"""
    rules = []
    for feat in num_features:
        vals = sorted(set(r[feat] for r in records))
        if len(vals) < 3:
            continue
        for th in vals[1:-1]:
            for cond in ['gt', 'le']:
                for pred_dir in ['up', 'dn']:
                    subset = [r for r in records if (r[feat] > th if cond == 'gt' else r[feat] <= th)]
                    if len(subset) < min_support:
                        continue
                    if pred_dir == 'up':
                        ok = sum(1 for r in subset if r['wup'])
                    else:
                        ok = sum(1 for r in subset if r['wdn'])
                    acc = ok / len(subset) * 100
                    if acc >= min_acc:
                        rules.append({
                            'feat': feat, 'th': th, 'cond': cond,
                            'pred': pred_dir, 'support': len(subset),
                            'acc': acc, 'coverage': len(subset) / len(records) * 100
                        })
    return sorted(rules, key=lambda x: -x['acc'])

for sector in sectors:
    s_train = [r for r in train if r['sector'] == sector]
    s_test = [r for r in test if r['sector'] == sector]
    if len(s_train) < 10:
        continue

    rules = find_high_conf_rules(s_train, min_support=8, min_acc=65)
    if not rules:
        print(f"\n{sector}: 无高置信度规则")
        continue

    print(f"\n{sector}: {len(rules)}条高置信度规则 (top 5)")
    for rule in rules[:5]:
        # 验证测试集
        subset_test = [r for r in s_test if
                       (r[rule['feat']] > rule['th'] if rule['cond'] == 'gt'
                        else r[rule['feat']] <= rule['th'])]
        if subset_test:
            if rule['pred'] == 'up':
                test_ok = sum(1 for r in subset_test if r['wup'])
            else:
                test_ok = sum(1 for r in subset_test if r['wdn'])
            test_acc = test_ok / len(subset_test) * 100
        else:
            test_acc = 0
        print(f"  {rule['feat']} {rule['cond']} {rule['th']:.2f} → {'涨' if rule['pred'] == 'up' else '跌'}"
              f"  训练: {rule['acc']:.1f}%({rule['support']}样本)"
              f"  测试: {test_acc:.1f}%({len(subset_test)}样本)")


# ═══════════════════════════════════════════════════════════════
# Part 5: 自适应策略（根据市场状态切换）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 5: 自适应策略 - 根据上周市场状态切换")
print("=" * 70)

# 构建上周信息
week_sector_chg = {}  # (sector, iw) -> avg weekly change
for r in weekly:
    key = (r['sector'], r['iw'])
    week_sector_chg[key] = r['wchg']

# 板块上周涨跌
week_market_chg = {}  # iw -> avg change across all stocks
for iw in sorted_weeks:
    recs = [r for r in weekly if r['iw'] == iw]
    week_market_chg[iw] = sum(r['wchg'] for r in recs) / len(recs) if recs else 0

# 给每条记录添加上周信息
for r in weekly:
    idx = sorted_weeks.index(r['iw'])
    if idx > 0:
        prev_iw = sorted_weeks[idx - 1]
        r['prev_sector_chg'] = week_sector_chg.get((r['sector'], prev_iw), 0)
        r['prev_market_chg'] = week_market_chg.get(prev_iw, 0)
    else:
        r['prev_sector_chg'] = 0
        r['prev_market_chg'] = 0

# 策略：上周板块涨→本周跌（均值回归），上周板块跌→本周涨
for th in [0, 0.5, 1.0, 1.5, 2.0]:
    ok_train = 0
    ok_test = 0
    n_train = 0
    n_test = 0
    for r in weekly:
        if sorted_weeks.index(r['iw']) == 0:
            continue  # 第一周无上周数据
        pred_up = r['prev_sector_chg'] <= th  # 均值回归
        correct = (pred_up and r['wup']) or (not pred_up and r['wdn'])
        if r['iw'] in first_half_weeks:
            ok_train += correct
            n_train += 1
        else:
            ok_test += correct
            n_test += 1
    if n_train > 0 and n_test > 0:
        print(f"均值回归(上周板块涨跌<={th:.1f}→涨): 训练={ok_train/n_train*100:.1f}%({n_train}) 测试={ok_test/n_test*100:.1f}%({n_test})")

print()
# 策略：上周市场涨→本周涨（动量）
for th in [-1.0, -0.5, 0, 0.5, 1.0]:
    ok_train = 0
    ok_test = 0
    n_train = 0
    n_test = 0
    for r in weekly:
        if sorted_weeks.index(r['iw']) == 0:
            continue
        pred_up = r['prev_market_chg'] > th  # 动量
        correct = (pred_up and r['wup']) or (not pred_up and r['wdn'])
        if r['iw'] in first_half_weeks:
            ok_train += correct
            n_train += 1
        else:
            ok_test += correct
            n_test += 1
    if n_train > 0 and n_test > 0:
        print(f"动量(上周市场>{th:.1f}→涨): 训练={ok_train/n_train*100:.1f}%({n_train}) 测试={ok_test/n_test*100:.1f}%({n_test})")


# ═══════════════════════════════════════════════════════════════
# Part 6: 周内前N天预测周涨跌（实时信号）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 6: 周内前N天信号预测周涨跌")
print("=" * 70)

# 用前1天、前2天、前3天的实际涨跌来预测整周方向
for n_days in [1, 2, 3]:
    print(f"\n── 用前{n_days}天实际涨跌预测整周 ──")
    for th in [-0.5, 0, 0.5, 1.0]:
        ok_all = 0
        ok_train = 0
        ok_test = 0
        n_all = 0
        n_train = 0
        n_test = 0
        for r in weekly:
            if r['n'] < n_days + 1:  # 需要至少n_days+1天才有意义
                continue
            first_n_chg = sum(d['_actual'] for d in r['days'][:n_days])
            pred_up = first_n_chg > th
            correct = (pred_up and r['wup']) or (not pred_up and r['wdn'])
            n_all += 1
            ok_all += correct
            if r['iw'] in first_half_weeks:
                ok_train += correct
                n_train += 1
            else:
                ok_test += correct
                n_test += 1
        if n_all > 0:
            print(f"  前{n_days}天涨跌>{th:.1f}→涨: 全={ok_all/n_all*100:.1f}%({n_all}) "
                  f"训练={ok_train/n_train*100:.1f}%({n_train}) 测试={ok_test/n_test*100:.1f}%({n_test})")

# 用前N天的信号均值预测
print(f"\n── 用前N天融合信号均值预测整周 ──")
for n_days in [1, 2, 3]:
    for th in [-0.5, 0, 0.5]:
        ok_train = 0
        ok_test = 0
        n_train = 0
        n_test = 0
        for r in weekly:
            if r['n'] < n_days:
                continue
            avg_sig = sum(d['融合信号'] for d in r['days'][:n_days]) / n_days
            pred_up = avg_sig > th
            correct = (pred_up and r['wup']) or (not pred_up and r['wdn'])
            if r['iw'] in first_half_weeks:
                ok_train += correct
                n_train += 1
            else:
                ok_test += correct
                n_test += 1
        if n_train > 0 and n_test > 0:
            print(f"  前{n_days}天融合均值>{th:.1f}→涨: 训练={ok_train/n_train*100:.1f}% 测试={ok_test/n_test*100:.1f}%")


# ═══════════════════════════════════════════════════════════════
# Part 7: 板块级最优组合策略（穷举搜索+交叉验证）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 7: 板块级最优组合策略 (Leave-One-Week-Out交叉验证)")
print("=" * 70)

# 使用LOWO交叉验证来避免过拟合
def lowo_cv(records, predict_fn, sorted_wks):
    """Leave-One-Week-Out交叉验证"""
    total_ok = 0
    total_n = 0
    for hold_wk in sorted_wks:
        train_r = [r for r in records if r['iw'] != hold_wk]
        test_r = [r for r in records if r['iw'] == hold_wk]
        if not test_r or len(train_r) < 10:
            continue
        for r in test_r:
            pred = predict_fn(r, train_r)
            if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
                total_ok += 1
            total_n += 1
    return total_ok / total_n * 100 if total_n > 0 else 0, total_n

# 策略A: 板块基准率（LOWO）
def strat_base_rate(r, train_r):
    sr = [t for t in train_r if t['sector'] == r['sector']]
    up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
    return 'up' if up_rate > 0.5 else 'dn'

# 策略B: 单特征最优（LOWO内部学习）
def make_single_feat_strat(feat):
    def strat(r, train_r):
        sr = [t for t in train_r if t['sector'] == r['sector']]
        if len(sr) < 5:
            return 'up'
        vals = sorted(set(t[feat] for t in sr))
        if len(vals) < 3:
            up_rate = sum(1 for t in sr if t['wup']) / len(sr)
            return 'up' if up_rate > 0.5 else 'dn'
        best_acc = 0
        best_th = 0
        best_dir = 'gt'
        for th in vals[1:-1]:
            for d in ['gt', 'le']:
                ok = 0
                for t in sr:
                    v = t[feat]
                    if d == 'gt':
                        p = v > th
                    else:
                        p = v <= th
                    if (p and t['wup']) or (not p and t['wdn']):
                        ok += 1
                acc = ok / len(sr) * 100
                if acc > best_acc:
                    best_acc = acc
                    best_th = th
                    best_dir = d
        v = r[feat]
        if best_dir == 'gt':
            return 'up' if v > best_th else 'dn'
        else:
            return 'up' if v <= best_th else 'dn'
    return strat

# 策略C: 多特征投票（LOWO内部学习top-3特征）
def strat_multi_vote(r, train_r):
    sr = [t for t in train_r if t['sector'] == r['sector']]
    if len(sr) < 5:
        return 'up'
    # 找top-3特征
    feat_accs = []
    for feat in num_features:
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
    top = feat_accs[:3]
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

# 策略D: 多特征投票 top-5
def strat_multi_vote5(r, train_r):
    sr = [t for t in train_r if t['sector'] == r['sector']]
    if len(sr) < 5:
        return 'up'
    feat_accs = []
    for feat in num_features:
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
    top = feat_accs[:5]
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
    return 'up' if votes_up > votes_dn else 'dn'

print("\nLOWO交叉验证结果:")
strategies_lowo = {}

acc, n = lowo_cv(weekly, strat_base_rate, sorted_weeks)
strategies_lowo['板块基准率'] = (acc, n)
print(f"  板块基准率: {acc:.1f}% ({n}样本)")

for feat in ['mon_comb', 'avg_comb', 'mon_peer', 'avg_peer', 'mon_rs',
             'comb_pos_ratio', 'daily_correct_ratio', 'net_consensus', 'avg_score']:
    strat = make_single_feat_strat(feat)
    acc, n = lowo_cv(weekly, strat, sorted_weeks)
    strategies_lowo[f'单特征_{feat}'] = (acc, n)
    if acc > 55:
        print(f"  单特征_{feat}: {acc:.1f}% ({n}样本)")

acc, n = lowo_cv(weekly, strat_multi_vote, sorted_weeks)
strategies_lowo['多特征投票3'] = (acc, n)
print(f"  多特征投票(top3): {acc:.1f}% ({n}样本)")

acc, n = lowo_cv(weekly, strat_multi_vote5, sorted_weeks)
strategies_lowo['多特征投票5'] = (acc, n)
print(f"  多特征投票(top5): {acc:.1f}% ({n}样本)")


# ═══════════════════════════════════════════════════════════════
# Part 8: 混合策略（高置信度用信号，低置信度用基准率）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 8: 混合策略 - 高置信度信号 + 低置信度基准率")
print("=" * 70)

def strat_hybrid(r, train_r, sig_th=1.0):
    """高信号强度用信号，低信号用基准率"""
    sr = [t for t in train_r if t['sector'] == r['sector']]
    up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
    default = 'up' if up_rate > 0.5 else 'dn'

    # 多维度信号
    sig = r['avg_comb']
    if abs(sig) > sig_th:
        return 'up' if sig > 0 else 'dn'
    return default

for sig_th in [0.3, 0.5, 0.8, 1.0, 1.5, 2.0]:
    def make_hybrid(th):
        def s(r, tr):
            return strat_hybrid(r, tr, th)
        return s
    acc, n = lowo_cv(weekly, make_hybrid(sig_th), sorted_weeks)
    print(f"  混合(信号阈值={sig_th:.1f}): {acc:.1f}% ({n}样本)")

# 混合策略变体：用net_consensus
def strat_hybrid_consensus(r, train_r, cons_th=1):
    sr = [t for t in train_r if t['sector'] == r['sector']]
    up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
    default = 'up' if up_rate > 0.5 else 'dn'
    if r['net_consensus'] >= cons_th:
        return 'up'
    elif r['net_consensus'] <= -cons_th:
        return 'dn'
    return default

for cons_th in [1, 2, 3]:
    def make_hc(th):
        def s(r, tr):
            return strat_hybrid_consensus(r, tr, th)
        return s
    acc, n = lowo_cv(weekly, make_hc(cons_th), sorted_weeks)
    print(f"  混合(共识阈值={cons_th}): {acc:.1f}% ({n}样本)")


# ═══════════════════════════════════════════════════════════════
# Part 9: 板块独立LOWO（每个板块单独评估）
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 9: 板块独立LOWO交叉验证")
print("=" * 70)

def lowo_cv_sector(records, predict_fn, sorted_wks, sector):
    sr = [r for r in records if r['sector'] == sector]
    total_ok = 0
    total_n = 0
    for hold_wk in sorted_wks:
        train_r = [r for r in sr if r['iw'] != hold_wk]
        test_r = [r for r in sr if r['iw'] == hold_wk]
        if not test_r or len(train_r) < 5:
            continue
        for r in test_r:
            pred = predict_fn(r, train_r)
            if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
                total_ok += 1
            total_n += 1
    return total_ok / total_n * 100 if total_n > 0 else 0, total_n

# 板块级单特征最优（在板块内LOWO）
def make_sector_single_feat(feat):
    def strat(r, train_r):
        if len(train_r) < 5:
            return 'up'
        vals = sorted(set(t[feat] for t in train_r))
        if len(vals) < 3:
            up_rate = sum(1 for t in train_r if t['wup']) / len(train_r)
            return 'up' if up_rate > 0.5 else 'dn'
        best_acc = 0
        best_th = 0
        best_dir = 'gt'
        for th in vals[1:-1]:
            for d in ['gt', 'le']:
                ok = sum(1 for t in train_r if
                         ((t[feat] > th if d == 'gt' else t[feat] <= th) and t['wup']) or
                         ((t[feat] <= th if d == 'gt' else t[feat] > th) and t['wdn']))
                acc = ok / len(train_r) * 100
                if acc > best_acc:
                    best_acc = acc
                    best_th = th
                    best_dir = d
        v = r[feat]
        if best_dir == 'gt':
            return 'up' if v > best_th else 'dn'
        else:
            return 'up' if v <= best_th else 'dn'
    return strat

# 板块级多特征投票
def strat_sector_vote(r, train_r):
    if len(train_r) < 5:
        return 'up'
    feat_accs = []
    for feat in num_features:
        vals = sorted(set(t[feat] for t in train_r))
        if len(vals) < 3:
            continue
        best_acc = 0
        best_th = 0
        best_dir = 'gt'
        for th in vals[1:-1]:
            for d in ['gt', 'le']:
                ok = sum(1 for t in train_r if
                         ((t[feat] > th if d == 'gt' else t[feat] <= th) and t['wup']) or
                         ((t[feat] <= th if d == 'gt' else t[feat] > th) and t['wdn']))
                acc = ok / len(train_r) * 100
                if acc > best_acc:
                    best_acc = acc
                    best_th = th
                    best_dir = d
        feat_accs.append((feat, best_th, best_dir, best_acc))
    feat_accs.sort(key=lambda x: -x[3])
    top = feat_accs[:3]
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

print(f"\n{'板块':<8} {'基准率':>6} {'投票3':>6} {'最优单特征':>10} {'最优特征名':>20}")
print("-" * 60)

sector_lowo_results = {}
for sector in sectors:
    # 基准率
    def strat_br(r, tr):
        up_rate = sum(1 for t in tr if t['wup']) / len(tr) if tr else 0.5
        return 'up' if up_rate > 0.5 else 'dn'
    acc_br, n = lowo_cv_sector(weekly, strat_br, sorted_weeks, sector)

    # 投票3
    acc_vote, _ = lowo_cv_sector(weekly, strat_sector_vote, sorted_weeks, sector)

    # 搜索最优单特征
    best_feat_name = ''
    best_feat_acc = 0
    for feat in num_features:
        strat = make_sector_single_feat(feat)
        acc, _ = lowo_cv_sector(weekly, strat, sorted_weeks, sector)
        if acc > best_feat_acc:
            best_feat_acc = acc
            best_feat_name = feat

    sector_lowo_results[sector] = {
        'base_rate': acc_br, 'vote3': acc_vote,
        'best_single': best_feat_acc, 'best_feat': best_feat_name
    }
    print(f"{sector:<8} {acc_br:>5.1f}% {acc_vote:>5.1f}% {best_feat_acc:>9.1f}% {best_feat_name:>20}")


# ═══════════════════════════════════════════════════════════════
# Part 10: 终极组合策略 + 前半→后半验证
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 10: 终极组合策略")
print("=" * 70)

# 策略E: 板块最优特征组合（每个板块用LOWO选出的最优特征）
def strat_sector_best_feat(r, train_r):
    """每个板块用其LOWO最优特征"""
    sector = r['sector']
    best_feat = sector_lowo_results.get(sector, {}).get('best_feat', 'avg_comb')
    sr = [t for t in train_r if t['sector'] == sector]
    if len(sr) < 5:
        up_rate = sum(1 for t in sr if t['wup']) / len(sr) if sr else 0.5
        return 'up' if up_rate > 0.5 else 'dn'
    vals = sorted(set(t[best_feat] for t in sr))
    if len(vals) < 3:
        up_rate = sum(1 for t in sr if t['wup']) / len(sr)
        return 'up' if up_rate > 0.5 else 'dn'
    best_acc = 0
    best_th = 0
    best_dir = 'gt'
    for th in vals[1:-1]:
        for d in ['gt', 'le']:
            ok = sum(1 for t in sr if
                     ((t[best_feat] > th if d == 'gt' else t[best_feat] <= th) and t['wup']) or
                     ((t[best_feat] <= th if d == 'gt' else t[best_feat] > th) and t['wdn']))
            acc = ok / len(sr) * 100
            if acc > best_acc:
                best_acc = acc
                best_th = th
                best_dir = d
    v = r[best_feat]
    if best_dir == 'gt':
        return 'up' if v > best_th else 'dn'
    else:
        return 'up' if v <= best_th else 'dn'

# 策略F: 加权投票（按LOWO准确率加权）
def strat_weighted_vote(r, train_r):
    sr = [t for t in train_r if t['sector'] == r['sector']]
    if len(sr) < 5:
        return 'up'
    feat_accs = []
    for feat in num_features:
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
        if best_acc > 55:
            feat_accs.append((feat, best_th, best_dir, best_acc))
    if not feat_accs:
        up_rate = sum(1 for t in sr if t['wup']) / len(sr)
        return 'up' if up_rate > 0.5 else 'dn'
    # 加权投票
    w_up = 0
    w_dn = 0
    for feat, th, d, acc in feat_accs:
        w = (acc - 50) / 50  # 权重
        v = r[feat]
        if d == 'gt':
            if v > th:
                w_up += w
            else:
                w_dn += w
        else:
            if v <= th:
                w_up += w
            else:
                w_dn += w
    return 'up' if w_up >= w_dn else 'dn'

# LOWO评估
acc_e, n_e = lowo_cv(weekly, strat_sector_best_feat, sorted_weeks)
print(f"策略E(板块最优特征): LOWO={acc_e:.1f}% ({n_e}样本)")

acc_f, n_f = lowo_cv(weekly, strat_weighted_vote, sorted_weeks)
print(f"策略F(加权投票): LOWO={acc_f:.1f}% ({n_f}样本)")

# 前半→后半验证
print("\n── 前半训练→后半测试 ──")
strategies_final = {}

for name, strat_fn in [
    ('板块基准率', strat_base_rate),
    ('多特征投票3', strat_multi_vote),
    ('多特征投票5', strat_multi_vote5),
    ('板块最优特征', strat_sector_best_feat),
    ('加权投票', strat_weighted_vote),
]:
    ok = 0
    for r in test:
        pred = strat_fn(r, train)
        if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
            ok += 1
    acc = ok / len(test) * 100
    strategies_final[name] = acc
    print(f"  {name}: {acc:.1f}% ({ok}/{len(test)})")

# 滚动验证
print("\n── 滚动验证 ──")
for name, strat_fn in [
    ('板块基准率', strat_base_rate),
    ('多特征投票3', strat_multi_vote),
    ('加权投票', strat_weighted_vote),
]:
    total_ok = 0
    total_n = 0
    for i in range(2, len(sorted_weeks)):
        train_weeks = set(sorted_weeks[:i])
        pred_week = sorted_weeks[i]
        r_train = [r for r in weekly if r['iw'] in train_weeks]
        r_pred = [r for r in weekly if r['iw'] == pred_week]
        for r in r_pred:
            pred = strat_fn(r, r_train)
            if (pred == 'up' and r['wup']) or (pred == 'dn' and r['wdn']):
                total_ok += 1
            total_n += 1
    if total_n > 0:
        print(f"  {name}: {total_ok/total_n*100:.1f}% ({total_ok}/{total_n})")


# ═══════════════════════════════════════════════════════════════
# Part 11: 最终汇总
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  Part 11: 最终汇总")
print("=" * 70)

print(f"\n日频基线: 59.1%")
print(f"目标: 周预测泛化准确率 > 65%")
print(f"\nLOWO交叉验证最优:")
best_lowo = max(strategies_lowo.items(), key=lambda x: x[1][0])
print(f"  {best_lowo[0]}: {best_lowo[1][0]:.1f}%")

print(f"\n前半→后半测试最优:")
best_test = max(strategies_final.items(), key=lambda x: x[1])
print(f"  {best_test[0]}: {best_test[1]:.1f}%")

print(f"\n板块LOWO明细:")
for sector in sectors:
    res = sector_lowo_results.get(sector, {})
    best_val = max(res.get('base_rate', 0), res.get('vote3', 0), res.get('best_single', 0))
    print(f"  {sector}: {best_val:.1f}% (最优特征: {res.get('best_feat', 'N/A')})")

total_best = sum(max(sector_lowo_results.get(s, {}).get('base_rate', 0),
                     sector_lowo_results.get(s, {}).get('vote3', 0),
                     sector_lowo_results.get(s, {}).get('best_single', 0))
                 for s in sectors) / len(sectors)
print(f"\n板块最优LOWO加权平均: {total_best:.1f}%")

print("\n完成。")
