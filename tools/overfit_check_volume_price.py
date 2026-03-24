#!/usr/bin/env python3
"""
量价V3b规则 — 全面过拟合检测
==============================
对V3b中通过验证的7条规则进行6项独立的过拟合检测：

1. 前后半段对比（temporal split）：前半段 vs 后半段准确率差距
2. 5折时间交叉验证：5个不重叠时间段的准确率稳定性
3. 滚动窗口测试：每2个月为一个窗口，观察准确率随时间的漂移
4. 随机打乱测试（permutation test）：打乱未来收益后重新计算准确率，
   如果打乱后准确率接近原始值，说明规则无真实预测力
5. 股票子集稳定性：随机抽50%股票重复10次，检查准确率方差
6. 阈值敏感性：微调每个条件的阈值±20%，检查准确率是否剧烈变化

用法：
    source .venv/bin/activate
    python -m tools.overfit_check_volume_price
"""
import json
import logging
import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dao import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _f(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def load_all_stock_codes():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline "
        "WHERE stock_code NOT LIKE '4%%' AND stock_code NOT LIKE '8%%' "
        "AND stock_code NOT LIKE '9%%' ORDER BY stock_code")
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return codes


def load_kline_batch(stock_codes, start_date, end_date):
    result = defaultdict(list)
    bs = 200
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        for attempt in range(3):
            try:
                conn = get_connection(use_dict_cursor=True)
                cur = conn.cursor()
                cur.execute(
                    f"SELECT stock_code, `date`, close_price, open_price, high_price, "
                    f"low_price, trading_volume, change_percent, change_hand "
                    f"FROM stock_kline WHERE stock_code IN ({ph}) "
                    f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
                    batch + [start_date, end_date])
                for row in cur.fetchall():
                    result[row['stock_code']].append({
                        'd': str(row['date']),
                        'c': _f(row['close_price']),
                        'o': _f(row['open_price']),
                        'h': _f(row['high_price']),
                        'l': _f(row['low_price']),
                        'v': _f(row['trading_volume']),
                        'p': _f(row['change_percent']),
                        't': _f(row.get('change_hand')),
                    })
                cur.close()
                conn.close()
                break
            except Exception as e:
                logger.warning("  batch %d retry %d: %s", i // bs, attempt + 1, e)
                time.sleep(2)
        if i % 2000 == 0 and i > 0:
            logger.info("  loaded %d/%d batches", i // bs, len(stock_codes) // bs + 1)
    return dict(result)


# ═══════════════════════════════════════════════════════════════
# 特征计算（与V3b完全一致）
# ═══════════════════════════════════════════════════════════════

def compute_feat(klines, idx):
    hist = klines[:idx + 1]
    n = len(hist)
    if n < 60:
        return None
    c = [k['c'] for k in hist]
    v = [k['v'] for k in hist]
    p = [k['p'] for k in hist]
    h = [k['h'] for k in hist]
    l = [k['l'] for k in hist]
    o = [k['o'] for k in hist]
    t = [k['t'] for k in hist]
    if c[-1] <= 0 or v[-1] <= 0:
        return None
    vh = [x for x in h[-60:] if x > 0]
    vl = [x for x in l[-60:] if x > 0]
    if not vh or not vl:
        return None
    h60, l60 = max(vh), min(vl)
    if h60 <= l60:
        return None
    pos = (c[-1] - l60) / (h60 - l60)
    ma5 = sum(c[-5:]) / 5
    ma20 = sum(c[-20:]) / 20
    ma60 = sum(c[-60:]) / 60
    vol_20 = sum(v[-20:]) / 20
    vol_5 = sum(v[-5:]) / 5
    if vol_20 <= 0:
        return None
    vr5 = vol_5 / vol_20
    turn_5 = sum(t[-5:]) / 5
    turn_20 = sum(t[-20:]) / 20
    tr = turn_5 / turn_20 if turn_20 > 0 else 1
    r5 = (c[-1] / c[-6] - 1) * 100 if n >= 6 and c[-6] > 0 else 0
    r20 = (c[-1] / c[-21] - 1) * 100 if n >= 21 and c[-21] > 0 else 0
    rets_20 = [(c[i] / c[i-1] - 1) * 100 for i in range(n-20, n) if c[i-1] > 0]
    if rets_20:
        mr = sum(rets_20) / len(rets_20)
        vol_val = (sum((r - mr)**2 for r in rets_20) / len(rets_20)) ** 0.5
    else:
        vol_val = 0
    cup = cdn = 0
    for i in range(n - 1, max(n - 15, 0), -1):
        if p[i] > 0:
            if cdn == 0: cup += 1
            else: break
        elif p[i] < 0:
            if cup == 0: cdn += 1
            else: break
        else: break
    ush = lsh = 0
    for i in range(-5, 0):
        body = abs(c[i] - o[i])
        if body > 0:
            upper = h[i] - max(c[i], o[i]) if h[i] > 0 else 0
            lower = min(c[i], o[i]) - l[i] if l[i] > 0 else 0
            if upper > body * 1.5: ush += 1
            if lower > body * 1.5: lsh += 1
    vcon = all(v[-i] <= v[-i-1] * 1.15 for i in range(1, 4) if v[-i-1] > 0)
    mu1 = max(p[-5:])
    ma20d = (c[-1] / ma20 - 1) * 100 if ma20 > 0 else 0
    return {
        'pos': pos, 'vr5': vr5, 'tr': tr, 't5': turn_5,
        'r5': r5, 'r20': r20, 'vol': vol_val,
        'cup': cup, 'cdn': cdn, 'ush': ush, 'lsh': lsh,
        'vcon': vcon, 'mu1': mu1, 'ma20d': ma20d,
        'ma_bull': ma5 > ma20 > ma60, 'ma_bear': ma5 < ma20 < ma60,
    }


# ═══════════════════════════════════════════════════════════════
# V3b通过验证的7条规则（完全复制，不做任何修改）
# ═══════════════════════════════════════════════════════════════

PASSED_RULES = {
    'BULL_N_极端超跌_下影线': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.2 and f['vr5'] <= 0.6
            and f['r5'] < -5 and f['cdn'] >= 3 and f['lsh'] >= 1
        ),
    },
    'BULL_J_极端超跌_偏离MA20': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.2 and f['vr5'] <= 0.6
            and f['r5'] < -5 and f['ma20d'] < -8
        ),
    },
    'BULL_O_低位缩量_偏离MA20_连跌': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33 and f['vr5'] <= 0.7
            and f['ma20d'] < -8 and f['cdn'] >= 3
        ),
    },
    'BULL_G_极端超跌': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.2 and f['vr5'] <= 0.6
            and f['r5'] < -5 and f['cdn'] >= 3
        ),
    },
    'BULL_H_低位缩量_偏离MA20大': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33 and f['vr5'] <= 0.7
            and f['ma20d'] < -8
        ),
    },
    'BULL_K_低位缩量_偏离MA20_下影线': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33 and f['vr5'] <= 0.7
            and f['ma20d'] < -8 and f['lsh'] >= 1
        ),
    },
    'BULL_P_低位深度缩量_20日大跌': {
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.25 and f['vr5'] <= 0.6
            and f['r20'] < -15
        ),
    },
}

# 阈值敏感性测试用的参数化版本
def make_param_filter(rule_name, param_mult=1.0):
    """生成参数微调后的过滤函数，param_mult=1.0为原始值"""
    m = param_mult
    filters = {
        'BULL_N_极端超跌_下影线': lambda f: (
            f['pos'] <= 0.2 * m and f['vr5'] <= 0.6 * m
            and f['r5'] < -5 / m and f['cdn'] >= 3 and f['lsh'] >= 1
        ),
        'BULL_J_极端超跌_偏离MA20': lambda f: (
            f['pos'] <= 0.2 * m and f['vr5'] <= 0.6 * m
            and f['r5'] < -5 / m and f['ma20d'] < -8 / m
        ),
        'BULL_O_低位缩量_偏离MA20_连跌': lambda f: (
            f['pos'] <= 0.33 * m and f['vr5'] <= 0.7 * m
            and f['ma20d'] < -8 / m and f['cdn'] >= 3
        ),
        'BULL_G_极端超跌': lambda f: (
            f['pos'] <= 0.2 * m and f['vr5'] <= 0.6 * m
            and f['r5'] < -5 / m and f['cdn'] >= 3
        ),
        'BULL_H_低位缩量_偏离MA20大': lambda f: (
            f['pos'] <= 0.33 * m and f['vr5'] <= 0.7 * m
            and f['ma20d'] < -8 / m
        ),
        'BULL_K_低位缩量_偏离MA20_下影线': lambda f: (
            f['pos'] <= 0.33 * m and f['vr5'] <= 0.7 * m
            and f['ma20d'] < -8 / m and f['lsh'] >= 1
        ),
        'BULL_P_低位深度缩量_20日大跌': lambda f: (
            f['pos'] <= 0.25 * m and f['vr5'] <= 0.6 * m
            and f['r20'] < -15 / m
        ),
    }
    return filters.get(rule_name)


# ═══════════════════════════════════════════════════════════════
# 主检测逻辑
# ═══════════════════════════════════════════════════════════════

def acc(rets):
    """看涨方向准确率"""
    if not rets:
        return 0, 0
    n = len(rets)
    c = sum(1 for r in rets if r > 0)
    return round(c / n, 4), n


def run_overfit_check():
    t0 = time.time()
    print("=" * 85)
    print("量价V3b规则 — 全面过拟合检测（6项独立测试）")
    print("=" * 85)

    # ── 加载全量数据 ──
    logger.info("[0] 加载全量数据...")
    all_codes = load_all_stock_codes()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=450)).strftime('%Y-%m-%d')
    kline_data = load_kline_batch(all_codes, start_date, end_date)
    logger.info("  %d只股票", len(kline_data))

    # ── 预扫描：为每条规则收集(date, code, r5, feat) ──
    logger.info("[0] 预扫描所有规则...")
    rule_data = {name: [] for name in PASSED_RULES}
    # 同时保存所有特征用于permutation test
    all_feats_and_rets = []  # [(code, date, month, feat, r5)]
    total = 0

    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        for i in range(60, len(klines) - 10):
            total += 1
            feat = compute_feat(klines, i)
            if feat is None:
                continue
            base_c = klines[i]['c']
            if base_c <= 0 or i + 5 >= len(klines) or klines[i + 5]['c'] <= 0:
                continue
            r5 = round((klines[i + 5]['c'] / base_c - 1) * 100, 2)
            date_str = klines[i]['d']
            month = date_str[:7]

            all_feats_and_rets.append((code, date_str, month, feat, r5))

            for name, rule in PASSED_RULES.items():
                try:
                    if rule['filter'](feat):
                        rule_data[name].append({
                            'code': code, 'date': date_str, 'month': month,
                            'r5': r5, 'feat': feat,
                        })
                except Exception:
                    pass

    logger.info("  扫描%d日, 总特征%d条", total, len(all_feats_and_rets))
    all_months = sorted(set(r[2] for r in all_feats_and_rets))
    all_stock_codes = sorted(set(r[0] for r in all_feats_and_rets))

    print(f"\n数据: {len(kline_data)}只股票, {start_date}~{end_date}")
    print(f"月份: {all_months[0]}~{all_months[-1]} ({len(all_months)}个月)")

    report = {}

    # ═══════════════════════════════════════════════════════════
    # 测试1: 前后半段对比
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 85}")
    print("📊 测试1: 前后半段对比（过拟合信号：前半段>>后半段）")
    print(f"{'═' * 85}")

    mid = len(all_months) // 2
    first_half = set(all_months[:mid])
    second_half = set(all_months[mid:])

    print(f"  前半段: {all_months[0]}~{all_months[mid-1]}")
    print(f"  后半段: {all_months[mid]}~{all_months[-1]}")
    print(f"\n  {'规则':<35s} {'全量':>6s} {'前半段':>7s} {'后半段':>7s} {'差距':>7s} {'判定':>6s}")
    print(f"  {'─' * 70}")

    test1 = {}
    for name in sorted(PASSED_RULES.keys()):
        recs = rule_data[name]
        a_all, n_all = acc([r['r5'] for r in recs])
        a_1st, n_1st = acc([r['r5'] for r in recs if r['month'] in first_half])
        a_2nd, n_2nd = acc([r['r5'] for r in recs if r['month'] in second_half])
        gap = a_1st - a_2nd
        # 过拟合信号：前半段比后半段高>8pp
        signal = '⚠️过拟合' if gap > 0.08 else ('✅稳定' if abs(gap) < 0.05 else '→可接受')
        print(f"  {name:<35s} {a_all:>6.1%} {a_1st:>7.1%} {a_2nd:>7.1%} {gap:>+7.1%} {signal:>6s}")
        test1[name] = {'all': a_all, 'first': a_1st, 'second': a_2nd,
                        'gap': round(gap, 4), 'signal': signal}
    report['test1_temporal_split'] = test1

    # ═══════════════════════════════════════════════════════════
    # 测试2: 5折时间交叉验证
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 85}")
    print("📊 测试2: 5折时间交叉验证（过拟合信号：折间方差大）")
    print(f"{'═' * 85}")

    n_folds = 5
    fold_size = len(all_months) // n_folds
    folds_months = []
    for fi in range(n_folds):
        s = fi * fold_size
        e = s + fold_size if fi < n_folds - 1 else len(all_months)
        folds_months.append(set(all_months[s:e]))

    fold_labels = []
    for fi in range(n_folds):
        s = fi * fold_size
        e = min(s + fold_size, len(all_months)) - 1
        fold_labels.append(f"{all_months[s][:7]}~{all_months[e][:7]}")

    print(f"  折: {' | '.join(fold_labels)}")
    print(f"\n  {'规则':<35s}", end='')
    for fl in fold_labels:
        print(f" {fl[-5:]:>7s}", end='')
    print(f" {'std':>6s} {'range':>6s} {'判定':>8s}")
    print(f"  {'─' * 80}")

    test2 = {}
    for name in sorted(PASSED_RULES.keys()):
        recs = rule_data[name]
        fold_accs = []
        for fm in folds_months:
            fr = [r['r5'] for r in recs if r['month'] in fm]
            a, n = acc(fr)
            fold_accs.append(a if n >= 10 else None)

        valid = [a for a in fold_accs if a is not None]
        if len(valid) >= 3:
            mean_a = sum(valid) / len(valid)
            std_a = (sum((a - mean_a)**2 for a in valid) / len(valid)) ** 0.5
            range_a = max(valid) - min(valid)
        else:
            std_a = range_a = 0

        signal = '⚠️不稳定' if range_a > 0.20 else ('✅稳定' if range_a < 0.12 else '→可接受')

        print(f"  {name:<35s}", end='')
        for a in fold_accs:
            print(f" {a:>7.1%}" if a is not None else "    N/A", end='')
        print(f" {std_a:>6.1%} {range_a:>6.1%} {signal:>8s}")

        test2[name] = {'fold_accs': fold_accs, 'std': round(std_a, 4),
                        'range': round(range_a, 4), 'signal': signal}
    report['test2_5fold_cv'] = test2

    # ═══════════════════════════════════════════════════════════
    # 测试3: 滚动窗口（每2个月）
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 85}")
    print("📊 测试3: 滚动窗口（2个月一窗口，检查趋势漂移）")
    print(f"{'═' * 85}")

    window_size = 2
    windows = []
    for i in range(0, len(all_months) - window_size + 1, 1):
        wm = set(all_months[i:i + window_size])
        label = f"{all_months[i]}~{all_months[min(i+window_size-1, len(all_months)-1)]}"
        windows.append((label, wm))

    test3 = {}
    for name in sorted(PASSED_RULES.keys()):
        recs = rule_data[name]
        w_accs = []
        for label, wm in windows:
            wr = [r['r5'] for r in recs if r['month'] in wm]
            a, n = acc(wr)
            w_accs.append((label, a, n))

        # 检查是否有下降趋势
        valid_accs = [a for _, a, n in w_accs if n >= 20]
        if len(valid_accs) >= 4:
            first_q = sum(valid_accs[:len(valid_accs)//3]) / (len(valid_accs)//3)
            last_q = sum(valid_accs[-(len(valid_accs)//3):]) / (len(valid_accs)//3)
            drift = last_q - first_q
        else:
            drift = 0

        signal = '⚠️衰减' if drift < -0.10 else ('✅稳定' if abs(drift) < 0.05 else '→可接受')

        print(f"\n  {name}:")
        line = "    "
        for label, a, n in w_accs:
            if n >= 10:
                line += f"{label[-5:]}={a:.0%}({n}) "
        print(line)
        print(f"    趋势漂移: {drift:+.1%} {signal}")

        test3[name] = {'drift': round(drift, 4), 'signal': signal,
                        'windows': [(l, a, n) for l, a, n in w_accs]}
    report['test3_rolling_window'] = test3

    # ═══════════════════════════════════════════════════════════
    # 测试4: 随机打乱测试（Permutation Test）
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 85}")
    print("📊 测试4: 随机打乱测试（打乱收益后准确率应≈50%）")
    print(f"{'═' * 85}")

    random.seed(42)
    n_perms = 100

    # 正确的permutation test：打乱全局收益标签，然后重新用规则匹配计算准确率
    # 这样可以检验规则的预测力是否显著优于随机
    # 构建 (feat, r5) 的索引映射
    all_r5_values = [r[4] for r in all_feats_and_rets]  # 全局收益列表
    # 为每条规则记录匹配的索引位置
    rule_match_indices = {name: [] for name in PASSED_RULES}
    for gi, (code, date_str, month, feat, r5) in enumerate(all_feats_and_rets):
        for name, rule in PASSED_RULES.items():
            try:
                if rule['filter'](feat):
                    rule_match_indices[name].append(gi)
            except Exception:
                pass

    print(f"  打乱{n_perms}次全局收益标签，检验规则预测力是否显著")
    print(f"\n  {'规则':<35s} {'原始':>6s} {'打乱均值':>8s} {'打乱std':>7s} {'p值':>6s} {'判定':>8s}")
    print(f"  {'─' * 72}")

    test4 = {}
    for name in sorted(PASSED_RULES.keys()):
        indices = rule_match_indices[name]
        orig_rets = [all_r5_values[i] for i in indices]
        orig_acc_val, n_orig = acc(orig_rets)

        perm_accs = []
        for _ in range(n_perms):
            shuffled_r5 = all_r5_values[:]
            random.shuffle(shuffled_r5)
            perm_rets = [shuffled_r5[i] for i in indices]
            pa, _ = acc(perm_rets)
            perm_accs.append(pa)

        perm_mean = sum(perm_accs) / len(perm_accs)
        perm_std = (sum((a - perm_mean)**2 for a in perm_accs) / len(perm_accs)) ** 0.5
        # p值：打乱后准确率≥原始准确率的比例
        p_value = sum(1 for a in perm_accs if a >= orig_acc_val) / n_perms

        signal = '✅显著' if p_value < 0.01 else ('⚠️不显著' if p_value > 0.05 else '→边缘')

        print(f"  {name:<35s} {orig_acc_val:>6.1%} {perm_mean:>8.1%} {perm_std:>7.1%} "
              f"{p_value:>6.2f} {signal:>8s}")

        test4[name] = {'orig': orig_acc_val, 'perm_mean': round(perm_mean, 4),
                        'perm_std': round(perm_std, 4), 'p_value': p_value, 'signal': signal}
    report['test4_permutation'] = test4

    # ═══════════════════════════════════════════════════════════
    # 测试5: 股票子集稳定性
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 85}")
    print("📊 测试5: 股票子集稳定性（随机抽50%股票×10次）")
    print(f"{'═' * 85}")

    random.seed(123)
    n_subsets = 10

    print(f"\n  {'规则':<35s} {'原始':>6s} {'子集均值':>8s} {'子集std':>7s} {'子集min':>7s} {'判定':>8s}")
    print(f"  {'─' * 73}")

    test5 = {}
    for name in sorted(PASSED_RULES.keys()):
        recs = rule_data[name]
        orig_acc_val, _ = acc([r['r5'] for r in recs])

        subset_accs = []
        for _ in range(n_subsets):
            subset_codes = set(random.sample(all_stock_codes, len(all_stock_codes) // 2))
            sr = [r['r5'] for r in recs if r['code'] in subset_codes]
            sa, sn = acc(sr)
            if sn >= 20:
                subset_accs.append(sa)

        if subset_accs:
            s_mean = sum(subset_accs) / len(subset_accs)
            s_std = (sum((a - s_mean)**2 for a in subset_accs) / len(subset_accs)) ** 0.5
            s_min = min(subset_accs)
        else:
            s_mean = s_std = s_min = 0

        signal = '⚠️不稳定' if s_std > 0.05 else ('✅稳定' if s_std < 0.02 else '→可接受')

        print(f"  {name:<35s} {orig_acc_val:>6.1%} {s_mean:>8.1%} {s_std:>7.1%} "
              f"{s_min:>7.1%} {signal:>8s}")

        test5[name] = {'orig': orig_acc_val, 'subset_mean': round(s_mean, 4),
                        'subset_std': round(s_std, 4), 'subset_min': round(s_min, 4),
                        'signal': signal}
    report['test5_stock_subset'] = test5

    # ═══════════════════════════════════════════════════════════
    # 测试6: 阈值敏感性
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 85}")
    print("📊 测试6: 阈值敏感性（阈值±20%后准确率变化）")
    print(f"{'═' * 85}")

    param_mults = [0.8, 0.9, 1.0, 1.1, 1.2]

    print(f"\n  {'规则':<35s}", end='')
    for m in param_mults:
        print(f" {'×'+str(m):>7s}", end='')
    print(f" {'range':>7s} {'判定':>8s}")
    print(f"  {'─' * 80}")

    test6 = {}
    for name in sorted(PASSED_RULES.keys()):
        param_accs = []
        for m in param_mults:
            filt = make_param_filter(name, m)
            if filt is None:
                param_accs.append(None)
                continue
            matched_rets = []
            for code, date_str, month, feat, r5 in all_feats_and_rets:
                try:
                    if filt(feat):
                        matched_rets.append(r5)
                except Exception:
                    pass
            a, n = acc(matched_rets)
            param_accs.append(a if n >= 30 else None)

        valid_pa = [a for a in param_accs if a is not None]
        pa_range = max(valid_pa) - min(valid_pa) if len(valid_pa) >= 2 else 0

        signal = '⚠️敏感' if pa_range > 0.10 else ('✅稳健' if pa_range < 0.05 else '→可接受')

        print(f"  {name:<35s}", end='')
        for a in param_accs:
            print(f" {a:>7.1%}" if a is not None else "    N/A", end='')
        print(f" {pa_range:>7.1%} {signal:>8s}")

        test6[name] = {'param_accs': param_accs, 'range': round(pa_range, 4), 'signal': signal}
    report['test6_threshold_sensitivity'] = test6

    # ═══════════════════════════════════════════════════════════
    # 综合判定
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 85}")
    print("📋 综合过拟合判定")
    print(f"{'═' * 85}")

    print(f"\n  {'规则':<35s} {'T1时间':>6s} {'T2折间':>6s} {'T3漂移':>6s} "
          f"{'T4打乱':>6s} {'T5子集':>6s} {'T6阈值':>6s} {'综合':>8s}")
    print(f"  {'─' * 82}")

    final = {}
    for name in sorted(PASSED_RULES.keys()):
        t1 = test1[name]['signal']
        t2 = test2[name]['signal']
        t3 = test3[name]['signal']
        t4 = test4[name]['signal']
        t5 = test5[name]['signal']
        t6 = test6[name]['signal']

        signals = [t1, t2, t3, t4, t5, t6]
        n_warn = sum(1 for s in signals if '⚠️' in s)
        n_ok = sum(1 for s in signals if '✅' in s)

        if n_warn >= 3:
            verdict = '❌过拟合'
        elif n_warn >= 2:
            verdict = '⚠️风险'
        elif n_ok >= 4:
            verdict = '✅可靠'
        else:
            verdict = '→一般'

        def short(s):
            if '✅' in s: return '✅'
            if '⚠️' in s: return '⚠️'
            return '→'

        print(f"  {name:<35s} {short(t1):>6s} {short(t2):>6s} {short(t3):>6s} "
              f"{short(t4):>6s} {short(t5):>6s} {short(t6):>6s} {verdict:>8s}")

        final[name] = {
            'tests': {'T1': t1, 'T2': t2, 'T3': t3, 'T4': t4, 'T5': t5, 'T6': t6},
            'n_warn': n_warn, 'n_ok': n_ok, 'verdict': verdict,
        }
    report['final_verdict'] = final

    # 保存
    output_path = OUTPUT_DIR / "volume_price_overfit_check.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print(f"📁 报告: {output_path}")
    print("=" * 85)
    return report


if __name__ == '__main__':
    run_overfit_check()
