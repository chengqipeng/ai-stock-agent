#!/usr/bin/env python3
"""
量价规则最终版本验证
====================
对深度分析中发现的最优规则进行最终验证：
  1. 5折时间交叉验证
  2. 股票子集稳定性
  3. Permutation test
  4. 市场环境分段（修复上证指数查询）
  5. 多规则融合的交叉验证
  6. 输出最终可用规则定义

用法：
    source .venv/bin/activate
    python -m tools.volume_price_final_validate
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
    rets_20 = [(c[i] / c[i - 1] - 1) * 100 for i in range(n - 20, n) if c[i - 1] > 0]
    if rets_20:
        mr = sum(rets_20) / len(rets_20)
        vol_val = (sum((r - mr) ** 2 for r in rets_20) / len(rets_20)) ** 0.5
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
    vcon = all(v[-i] <= v[-i - 1] * 1.15 for i in range(1, 4) if v[-i - 1] > 0)
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
# 最终规则定义
# ═══════════════════════════════════════════════════════════════

# 深度分析发现的最优规则（网格搜索 + 后半段验证）
FINAL_RULES = {
    # 第1名：最优后半段准确率，样本量充足
    'FINAL_A_低位连跌_偏离MA20': {
        'desc': '60日低位≤25%, 缩量(vr≤0.8), 偏离MA20>10%, 连跌≥2日',
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.25 and f['vr5'] <= 0.8
            and f['ma20d'] < -10 and f['cdn'] >= 2
        ),
    },
    # 第2名：放宽位置到30%，更多样本
    'FINAL_B_低位连跌_偏离MA20_宽': {
        'desc': '60日低位≤30%, 缩量(vr≤0.8), 偏离MA20>10%, 连跌≥2日',
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.30 and f['vr5'] <= 0.8
            and f['ma20d'] < -10 and f['cdn'] >= 2
        ),
    },
    # 第3名：放宽到33%，最大样本量
    'FINAL_C_低位连跌_偏离MA20_最宽': {
        'desc': '60日低位≤33%, 缩量(vr≤0.8), 偏离MA20>10%, 连跌≥2日',
        'dir': 'UP',
        'filter': lambda f: (
            f['pos'] <= 0.33 and f['vr5'] <= 0.8
            and f['ma20d'] < -10 and f['cdn'] >= 2
        ),
    },
    # 融合规则：5条V3b规则同时触发≥3条
    'FINAL_D_多规则融合_3': {
        'desc': '5条V3b候选规则中≥3条同时触发',
        'dir': 'UP',
        'filter': lambda f: sum([
            f['pos'] <= 0.33 and f['vr5'] <= 0.7 and f['ma20d'] < -8 and f['cdn'] >= 3,  # O
            f['pos'] <= 0.33 and f['vr5'] <= 0.7 and f['ma20d'] < -8,  # H
            f['pos'] <= 0.33 and f['vr5'] <= 0.7 and f['ma20d'] < -8 and f['lsh'] >= 1,  # K
            f['pos'] <= 0.2 and f['vr5'] <= 0.6 and f['r5'] < -5 and f['ma20d'] < -8,  # J
            f['pos'] <= 0.2 and f['vr5'] <= 0.6 and f['r5'] < -5 and f['cdn'] >= 3,  # G
        ]) >= 3,
    },
    # 融合规则：≥4条同时触发（更严格）
    'FINAL_E_多规则融合_4': {
        'desc': '5条V3b候选规则中≥4条同时触发',
        'dir': 'UP',
        'filter': lambda f: sum([
            f['pos'] <= 0.33 and f['vr5'] <= 0.7 and f['ma20d'] < -8 and f['cdn'] >= 3,
            f['pos'] <= 0.33 and f['vr5'] <= 0.7 and f['ma20d'] < -8,
            f['pos'] <= 0.33 and f['vr5'] <= 0.7 and f['ma20d'] < -8 and f['lsh'] >= 1,
            f['pos'] <= 0.2 and f['vr5'] <= 0.6 and f['r5'] < -5 and f['ma20d'] < -8,
            f['pos'] <= 0.2 and f['vr5'] <= 0.6 and f['r5'] < -5 and f['cdn'] >= 3,
        ]) >= 4,
    },
}


def acc(rets):
    if not rets:
        return 0, 0
    n = len(rets)
    c = sum(1 for r in rets if r > 0)
    return round(c / n, 4), n


def run_final_validate():
    t0 = time.time()
    print("=" * 90)
    print("量价规则最终版本 — 全面验证")
    print("=" * 90)

    # ── 加载数据 ──
    logger.info("[0] 加载全量数据...")
    all_codes = load_all_stock_codes()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=450)).strftime('%Y-%m-%d')
    kline_data = load_kline_batch(all_codes, start_date, end_date)
    logger.info("  %d只股票", len(kline_data))

    # ── 加载上证指数（尝试多个代码） ──
    market_monthly = {}
    for idx_code in ['000001', '1A0001', '999999', 'sh000001']:
        conn = get_connection(use_dict_cursor=True)
        cur = conn.cursor()
        cur.execute("SELECT `date`, close_price FROM stock_kline WHERE stock_code = %s ORDER BY `date`", [idx_code])
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if len(rows) > 100:
            by_month = defaultdict(list)
            for r in rows:
                m = str(r['date'])[:7]
                by_month[m].append(_f(r['close_price']))
            for m, prices in by_month.items():
                if len(prices) >= 2 and prices[0] > 0:
                    market_monthly[m] = round((prices[-1] / prices[0] - 1) * 100, 2)
            logger.info("  上证指数代码=%s, %d个月", idx_code, len(market_monthly))
            break

    # ── 预扫描 ──
    logger.info("[0] 预扫描...")
    all_records = []  # (code, date, month, feat, r1, r3, r5, r10)
    total = 0
    for code, klines in kline_data.items():
        if len(klines) < 80:
            continue
        for i in range(60, len(klines) - 10):
            total += 1
            feat = compute_feat(klines, i)
            if feat is None:
                continue
            bc = klines[i]['c']
            if bc <= 0:
                continue
            r1 = round((klines[i + 1]['c'] / bc - 1) * 100, 3) if i + 1 < len(klines) and klines[i + 1]['c'] > 0 else None
            r3 = round((klines[i + 3]['c'] / bc - 1) * 100, 3) if i + 3 < len(klines) and klines[i + 3]['c'] > 0 else None
            r5 = round((klines[i + 5]['c'] / bc - 1) * 100, 3) if i + 5 < len(klines) and klines[i + 5]['c'] > 0 else None
            r10 = round((klines[i + 10]['c'] / bc - 1) * 100, 3) if i + 10 < len(klines) and klines[i + 10]['c'] > 0 else None
            if r5 is None:
                continue
            all_records.append((code, klines[i]['d'], klines[i]['d'][:7], feat, r1, r3, r5, r10))
    logger.info("  扫描%d日, 有效%d条", total, len(all_records))

    all_months = sorted(set(r[2] for r in all_records))
    all_stock_codes = sorted(set(r[0] for r in all_records))
    mid = len(all_months) // 2
    second_half = set(all_months[mid:])

    # 为每条规则匹配
    rule_data = {}
    rule_indices = {}  # 用于permutation test
    for name, rule in FINAL_RULES.items():
        matched = []
        indices = []
        for gi, rec in enumerate(all_records):
            try:
                if rule['filter'](rec[3]):
                    matched.append(rec)
                    indices.append(gi)
            except Exception:
                pass
        rule_data[name] = matched
        rule_indices[name] = indices
        logger.info("  %s: %d条", name, len(matched))

    report = {}

    # ═══════════════════════════════════════════════════════════
    # 基础统计
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 基础统计")
    print(f"{'═' * 90}")
    print(f"  数据: {len(kline_data)}只股票, {start_date}~{end_date}, {len(all_months)}个月")
    print(f"  前半段: {all_months[0]}~{all_months[mid-1]} | 后半段: {all_months[mid]}~{all_months[-1]}")

    print(f"\n  {'规则':<30s} {'全量acc':>8s} {'全量n':>7s} {'后半段acc':>9s} {'后半段n':>8s} "
          f"{'1日':>5s} {'3日':>5s} {'5日':>5s} {'10日':>5s} {'avg':>7s} {'med':>7s}")
    print(f"  {'─' * 100}")

    base_report = {}
    for name in FINAL_RULES:
        recs = rule_data[name]
        r5_all = [r[6] for r in recs]
        r5_h2 = [r[6] for r in recs if r[2] in second_half]
        r1_all = [r[4] for r in recs if r[4] is not None]
        r3_all = [r[5] for r in recs if r[5] is not None]
        r10_all = [r[7] for r in recs if r[7] is not None]

        a_all, n_all = acc(r5_all)
        a_h2, n_h2 = acc(r5_h2)
        a_r1, _ = acc(r1_all)
        a_r3, _ = acc(r3_all)
        a_r10, _ = acc(r10_all)
        avg = round(sum(r5_all) / len(r5_all), 2) if r5_all else 0
        sr = sorted(r5_all)
        med = round(sr[len(sr) // 2], 2) if sr else 0

        print(f"  {name:<30s} {a_all:>8.1%} {n_all:>7d} {a_h2:>9.1%} {n_h2:>8d} "
              f"{a_r1:>5.1%} {a_r3:>5.1%} {a_all:>5.1%} {a_r10:>5.1%} {avg:>+7.2f} {med:>+7.2f}")

        base_report[name] = {
            'all_acc': a_all, 'all_n': n_all, 'h2_acc': a_h2, 'h2_n': n_h2,
            'r1_acc': a_r1, 'r3_acc': a_r3, 'r5_acc': a_all, 'r10_acc': a_r10,
            'avg': avg, 'med': med,
        }
    report['base'] = base_report

    # ═══════════════════════════════════════════════════════════
    # 5折时间交叉验证
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 5折时间交叉验证")
    print(f"{'═' * 90}")

    n_folds = 5
    fold_size = len(all_months) // n_folds
    folds = []
    for fi in range(n_folds):
        s = fi * fold_size
        e = s + fold_size if fi < n_folds - 1 else len(all_months)
        folds.append(set(all_months[s:e]))

    print(f"\n  {'规则':<30s}", end='')
    for fi in range(n_folds):
        print(f" {'折'+str(fi+1):>7s}", end='')
    print(f" {'std':>6s} {'range':>6s} {'min':>6s}")
    print(f"  {'─' * 75}")

    cv_report = {}
    for name in FINAL_RULES:
        recs = rule_data[name]
        fold_accs = []
        for fm in folds:
            fr = [r[6] for r in recs if r[2] in fm]
            a, n = acc(fr)
            fold_accs.append(a if n >= 10 else None)

        valid = [a for a in fold_accs if a is not None]
        if len(valid) >= 3:
            mean_a = sum(valid) / len(valid)
            std_a = (sum((a - mean_a) ** 2 for a in valid) / len(valid)) ** 0.5
            range_a = max(valid) - min(valid)
            min_a = min(valid)
        else:
            std_a = range_a = min_a = 0

        print(f"  {name:<30s}", end='')
        for a in fold_accs:
            print(f" {a:>7.1%}" if a is not None else "    N/A", end='')
        print(f" {std_a:>6.1%} {range_a:>6.1%} {min_a:>6.1%}")

        cv_report[name] = {
            'fold_accs': fold_accs, 'std': round(std_a, 4),
            'range': round(range_a, 4), 'min': round(min_a, 4),
        }
    report['cv_5fold'] = cv_report

    # ═══════════════════════════════════════════════════════════
    # Permutation Test（全局打乱）
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 Permutation Test（打乱全局收益100次）")
    print(f"{'═' * 90}")

    random.seed(42)
    all_r5 = [r[6] for r in all_records]

    print(f"\n  {'规则':<30s} {'原始':>6s} {'打乱均值':>8s} {'打乱std':>7s} {'p值':>6s}")
    print(f"  {'─' * 60}")

    perm_report = {}
    for name in FINAL_RULES:
        indices = rule_indices[name]
        orig_rets = [all_r5[i] for i in indices]
        orig_acc_val, _ = acc(orig_rets)

        perm_accs = []
        for _ in range(100):
            shuffled = all_r5[:]
            random.shuffle(shuffled)
            pr = [shuffled[i] for i in indices]
            pa, _ = acc(pr)
            perm_accs.append(pa)

        pm = sum(perm_accs) / len(perm_accs)
        ps = (sum((a - pm) ** 2 for a in perm_accs) / len(perm_accs)) ** 0.5
        pv = sum(1 for a in perm_accs if a >= orig_acc_val) / 100

        print(f"  {name:<30s} {orig_acc_val:>6.1%} {pm:>8.1%} {ps:>7.1%} {pv:>6.2f}")

        perm_report[name] = {
            'orig': orig_acc_val, 'perm_mean': round(pm, 4),
            'perm_std': round(ps, 4), 'p_value': pv,
        }
    report['permutation'] = perm_report

    # ═══════════════════════════════════════════════════════════
    # 股票子集稳定性
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 股票子集稳定性（50%股票×10次）")
    print(f"{'═' * 90}")

    random.seed(123)

    print(f"\n  {'规则':<30s} {'原始':>6s} {'子集均值':>8s} {'子集std':>7s} {'子集min':>7s}")
    print(f"  {'─' * 60}")

    subset_report = {}
    for name in FINAL_RULES:
        recs = rule_data[name]
        orig_val, _ = acc([r[6] for r in recs])

        sub_accs = []
        for _ in range(10):
            sc = set(random.sample(all_stock_codes, len(all_stock_codes) // 2))
            sr = [r[6] for r in recs if r[0] in sc]
            sa, sn = acc(sr)
            if sn >= 20:
                sub_accs.append(sa)

        sm = sum(sub_accs) / len(sub_accs) if sub_accs else 0
        ss = (sum((a - sm) ** 2 for a in sub_accs) / len(sub_accs)) ** 0.5 if sub_accs else 0
        smin = min(sub_accs) if sub_accs else 0

        print(f"  {name:<30s} {orig_val:>6.1%} {sm:>8.1%} {ss:>7.1%} {smin:>7.1%}")

        subset_report[name] = {
            'orig': orig_val, 'mean': round(sm, 4),
            'std': round(ss, 4), 'min': round(smin, 4),
        }
    report['stock_subset'] = subset_report

    # ═══════════════════════════════════════════════════════════
    # 市场环境分段
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 市场环境分段")
    print(f"{'═' * 90}")

    if market_monthly:
        bull_m = set(m for m, p in market_monthly.items() if p > 2)
        bear_m = set(m for m, p in market_monthly.items() if p < -2)
        flat_m = set(m for m, p in market_monthly.items() if -2 <= p <= 2)
        print(f"  上涨月: {len(bull_m)}个, 下跌月: {len(bear_m)}个, 震荡月: {len(flat_m)}个")
        if market_monthly:
            for m in sorted(market_monthly.keys()):
                if m in set(all_months):
                    tag = '📈' if m in bull_m else ('📉' if m in bear_m else '➡️')
                    print(f"    {m}: {market_monthly[m]:+.1f}% {tag}")

        print(f"\n  {'规则':<30s} {'上涨月':>7s}{'(n)':>5s} {'下跌月':>7s}{'(n)':>5s} {'震荡月':>7s}{'(n)':>5s}")
        print(f"  {'─' * 65}")

        env_report = {}
        for name in FINAL_RULES:
            recs = rule_data[name]
            br = [r[6] for r in recs if r[2] in bull_m]
            ber = [r[6] for r in recs if r[2] in bear_m]
            fr = [r[6] for r in recs if r[2] in flat_m]
            ab, nb = acc(br)
            abe, nbe = acc(ber)
            af, nf = acc(fr)
            print(f"  {name:<30s} {ab:>7.1%}{nb:>5d} {abe:>7.1%}{nbe:>5d} {af:>7.1%}{nf:>5d}")
            env_report[name] = {
                'bull': {'acc': ab, 'n': nb},
                'bear': {'acc': abe, 'n': nbe},
                'flat': {'acc': af, 'n': nf},
            }
        report['market_env'] = env_report
    else:
        print("  未找到上证指数数据，跳过市场环境分析")

    # ═══════════════════════════════════════════════════════════
    # 滚动窗口
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 滚动窗口（逐月准确率）")
    print(f"{'═' * 90}")

    roll_report = {}
    for name in FINAL_RULES:
        recs = rule_data[name]
        monthly = defaultdict(list)
        for r in recs:
            monthly[r[2]].append(r[6])

        print(f"\n  {name}:")
        line = "    "
        m_accs = []
        for m in all_months:
            mrs = monthly.get(m, [])
            if len(mrs) >= 10:
                a, n = acc(mrs)
                line += f"{m[-5:]}={a:.0%}({n}) "
                m_accs.append(a)
        print(line)

        # 月胜率
        m_win = sum(1 for a in m_accs if a > 0.5) / len(m_accs) if m_accs else 0
        m_65 = sum(1 for a in m_accs if a >= 0.65) / len(m_accs) if m_accs else 0
        print(f"    月胜率(>50%): {m_win:.0%} | 月达标率(≥65%): {m_65:.0%}")

        roll_report[name] = {
            'monthly_win': round(m_win, 3),
            'monthly_65': round(m_65, 3),
        }
    report['rolling'] = roll_report

    # ═══════════════════════════════════════════════════════════
    # 综合判定
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("🏆 综合判定")
    print(f"{'═' * 90}")

    print(f"\n  {'规则':<30s} {'后半段':>7s} {'CV_min':>7s} {'CV_range':>8s} "
          f"{'perm_p':>7s} {'子集std':>7s} {'月胜率':>6s} {'判定':>8s}")
    print(f"  {'─' * 85}")

    final_verdict = {}
    for name in FINAL_RULES:
        h2_acc = base_report[name]['h2_acc']
        cv_min = cv_report[name]['min']
        cv_range = cv_report[name]['range']
        pv = perm_report[name]['p_value']
        ss = subset_report[name]['std']
        mw = roll_report[name]['monthly_win']

        # 判定逻辑
        issues = 0
        if h2_acc < 0.65:
            issues += 1
        if cv_min < 0.55:
            issues += 1
        if cv_range > 0.25:
            issues += 1
        if pv > 0.01:
            issues += 1
        if ss > 0.03:
            issues += 1
        if mw < 0.7:
            issues += 1

        if issues == 0:
            verdict = '✅可靠'
        elif issues == 1:
            verdict = '✅可用'
        elif issues == 2:
            verdict = '⚠️谨慎'
        else:
            verdict = '❌不推荐'

        print(f"  {name:<30s} {h2_acc:>7.1%} {cv_min:>7.1%} {cv_range:>8.1%} "
              f"{pv:>7.2f} {ss:>7.1%} {mw:>6.0%} {verdict:>8s}")

        final_verdict[name] = {
            'h2_acc': h2_acc, 'cv_min': cv_min, 'cv_range': cv_range,
            'perm_p': pv, 'subset_std': ss, 'monthly_win': mw,
            'issues': issues, 'verdict': verdict,
        }
    report['final_verdict'] = final_verdict

    # ═══════════════════════════════════════════════════════════
    # 最终输出
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📋 最终可用规则定义")
    print(f"{'═' * 90}")

    for name in FINAL_RULES:
        v = final_verdict[name]
        b = base_report[name]
        if '✅' in v['verdict']:
            print(f"\n  {v['verdict']} {name}")
            print(f"     描述: {FINAL_RULES[name]['desc']}")
            print(f"     方向: 看涨(UP) | 持有期: 5日")
            print(f"     全量: {b['all_acc']:.1%} (n={b['all_n']}) | 后半段: {b['h2_acc']:.1%} (n={b['h2_n']})")
            print(f"     CV最低折: {v['cv_min']:.1%} | CV范围: {v['cv_range']:.1%}")
            print(f"     Permutation p={v['perm_p']:.2f} | 子集std={v['subset_std']:.1%}")
            print(f"     月胜率: {v['monthly_win']:.0%} | 均收益: {b['avg']:+.2f}% | 中位收益: {b['med']:+.2f}%")
            print(f"     条件:")
            print(f"       - 60日位置 ≤ {name}对应阈值")
            print(f"       - 5日量比/20日量比 ≤ 0.8（缩量）")
            print(f"       - 收盘价偏离MA20 < -10%（深度偏离）")
            print(f"       - 连跌天数 ≥ 2")

    # 保存
    output_path = OUTPUT_DIR / "volume_price_final_validated.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print(f"📁 报告: {output_path}")
    print("=" * 90)
    return report


if __name__ == '__main__':
    run_final_validate()
