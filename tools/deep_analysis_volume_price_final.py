#!/usr/bin/env python3
"""
量价规则深度分析 — 最终可用版本
================================
对过拟合检测中存活的5条规则进行深度分析：
  可用: BULL_O, BULL_H, BULL_K
  谨慎: BULL_J, BULL_G

分析内容：
  1. 市场环境分段（上证涨/跌/震荡月份分别统计）
  2. 多持有期收益分析（1/3/5/10日）
  3. 条件边界网格搜索（找最优参数组合）
  4. 规则融合（多规则同时触发时的准确率提升）
  5. 后半段（近6个月）单独验证
  6. 输出最终可用版本

用法：
    source .venv/bin/activate
    python -m tools.deep_analysis_volume_price_final
"""
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from itertools import product

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
    # 额外特征
    r3 = (c[-1] / c[-4] - 1) * 100 if n >= 4 and c[-4] > 0 else 0
    r10 = (c[-1] / c[-11] - 1) * 100 if n >= 11 and c[-11] > 0 else 0
    return {
        'pos': pos, 'vr5': vr5, 'tr': tr, 't5': turn_5,
        'r3': r3, 'r5': r5, 'r10': r10, 'r20': r20, 'vol': vol_val,
        'cup': cup, 'cdn': cdn, 'ush': ush, 'lsh': lsh,
        'vcon': vcon, 'mu1': mu1, 'ma20d': ma20d,
        'ma_bull': ma5 > ma20 > ma60, 'ma_bear': ma5 < ma20 < ma60,
    }



# ═══════════════════════════════════════════════════════════════
# 加载上证指数月度涨跌（判断市场环境）
# ═══════════════════════════════════════════════════════════════

def load_market_monthly():
    """加载上证指数月度涨跌幅，返回 {month: pct}"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute(
        "SELECT `date`, close_price FROM stock_kline "
        "WHERE stock_code = '000001' ORDER BY `date`")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    monthly = {}
    by_month = defaultdict(list)
    for r in rows:
        m = str(r['date'])[:7]
        by_month[m].append(_f(r['close_price']))
    for m, prices in by_month.items():
        if len(prices) >= 2 and prices[0] > 0:
            monthly[m] = round((prices[-1] / prices[0] - 1) * 100, 2)
    return monthly


def acc_stats(rets):
    """计算看涨方向准确率和统计"""
    if not rets:
        return {'n': 0, 'acc': 0, 'avg': 0, 'med': 0, 'win_avg': 0, 'loss_avg': 0}
    n = len(rets)
    c = sum(1 for r in rets if r > 0)
    wins = [r for r in rets if r > 0]
    losses = [-r for r in rets if r < 0]
    sr = sorted(rets)
    return {
        'n': n,
        'acc': round(c / n, 4),
        'avg': round(sum(rets) / n, 3),
        'med': round(sr[n // 2], 3),
        'win_avg': round(sum(wins) / len(wins), 3) if wins else 0,
        'loss_avg': round(sum(losses) / len(losses), 3) if losses else 0,
        'p25': round(sr[int(n * 0.25)], 2),
        'p75': round(sr[int(n * 0.75)], 2),
    }


# ═══════════════════════════════════════════════════════════════
# 参数化规则（用于网格搜索）
# ═══════════════════════════════════════════════════════════════

def make_rule(pos_th, vr_th, ma20d_th, cdn_th=0, r5_th=None, lsh_th=0, r20_th=None):
    """生成参数化的看涨规则过滤函数"""
    def filt(f):
        if f['pos'] > pos_th:
            return False
        if f['vr5'] > vr_th:
            return False
        if f['ma20d'] > ma20d_th:
            return False
        if cdn_th > 0 and f['cdn'] < cdn_th:
            return False
        if r5_th is not None and f['r5'] > r5_th:
            return False
        if lsh_th > 0 and f['lsh'] < lsh_th:
            return False
        if r20_th is not None and f['r20'] > r20_th:
            return False
        return True
    return filt



# ═══════════════════════════════════════════════════════════════
# 主分析逻辑
# ═══════════════════════════════════════════════════════════════

def run_deep_analysis():
    t0 = time.time()
    print("=" * 90)
    print("量价规则深度分析 — 最终可用版本")
    print("=" * 90)

    # ── 加载数据 ──
    logger.info("[0] 加载全量数据...")
    all_codes = load_all_stock_codes()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=450)).strftime('%Y-%m-%d')
    kline_data = load_kline_batch(all_codes, start_date, end_date)
    logger.info("  %d只股票", len(kline_data))

    market_monthly = load_market_monthly()

    # ── 预扫描：收集所有特征和多持有期收益 ──
    logger.info("[0] 预扫描...")
    # 每条记录: (code, date, month, feat, r1, r3, r5, r10)
    all_records = []
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

    logger.info("  扫描%d日, 有效记录%d条", total, len(all_records))

    all_months = sorted(set(r[2] for r in all_records))
    mid = len(all_months) // 2
    second_half_months = set(all_months[mid:])

    # ── 5条候选规则 ──
    CANDIDATES = {
        'BULL_O_低位缩量_偏离MA20_连跌': lambda f: f['pos'] <= 0.33 and f['vr5'] <= 0.7 and f['ma20d'] < -8 and f['cdn'] >= 3,
        'BULL_H_低位缩量_偏离MA20大': lambda f: f['pos'] <= 0.33 and f['vr5'] <= 0.7 and f['ma20d'] < -8,
        'BULL_K_低位缩量_偏离MA20_下影线': lambda f: f['pos'] <= 0.33 and f['vr5'] <= 0.7 and f['ma20d'] < -8 and f['lsh'] >= 1,
        'BULL_J_极端超跌_偏离MA20': lambda f: f['pos'] <= 0.2 and f['vr5'] <= 0.6 and f['r5'] < -5 and f['ma20d'] < -8,
        'BULL_G_极端超跌': lambda f: f['pos'] <= 0.2 and f['vr5'] <= 0.6 and f['r5'] < -5 and f['cdn'] >= 3,
    }

    # 为每条规则匹配记录
    rule_data = {}
    for name, filt in CANDIDATES.items():
        matched = []
        for rec in all_records:
            code, date, month, feat, r1, r3, r5, r10 = rec
            try:
                if filt(feat):
                    matched.append(rec)
            except Exception:
                pass
        rule_data[name] = matched
        logger.info("  %s: %d条", name, len(matched))

    report = {}

    # ═══════════════════════════════════════════════════════════
    # 分析1: 市场环境分段
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 分析1: 市场环境分段（上证月涨跌分类）")
    print(f"{'═' * 90}")

    # 分类月份
    bull_months = set(m for m, p in market_monthly.items() if p > 2)
    bear_months = set(m for m, p in market_monthly.items() if p < -2)
    flat_months = set(m for m, p in market_monthly.items() if -2 <= p <= 2)

    print(f"  上涨月(>2%): {len(bull_months)}个, 下跌月(<-2%): {len(bear_months)}个, 震荡月: {len(flat_months)}个")
    print(f"\n  {'规则':<35s} {'全量':>6s} {'上涨月':>7s}{'(n)':>5s} {'下跌月':>7s}{'(n)':>5s} {'震荡月':>7s}{'(n)':>5s}")
    print(f"  {'─' * 80}")

    env_report = {}
    for name in ['BULL_O_低位缩量_偏离MA20_连跌', 'BULL_H_低位缩量_偏离MA20大', 'BULL_K_低位缩量_偏离MA20_下影线',
                  'BULL_J_极端超跌_偏离MA20', 'BULL_G_极端超跌']:
        recs = rule_data[name]
        all_r5 = [r[6] for r in recs]
        bull_r5 = [r[6] for r in recs if r[2] in bull_months]
        bear_r5 = [r[6] for r in recs if r[2] in bear_months]
        flat_r5 = [r[6] for r in recs if r[2] in flat_months]

        sa = acc_stats(all_r5)
        sb = acc_stats(bull_r5)
        sbe = acc_stats(bear_r5)
        sf = acc_stats(flat_r5)

        print(f"  {name:<35s} {sa['acc']:>6.1%} {sb['acc']:>7.1%}{sb['n']:>5d} "
              f"{sbe['acc']:>7.1%}{sbe['n']:>5d} {sf['acc']:>7.1%}{sf['n']:>5d}")

        env_report[name] = {
            'all': sa, 'bull': sb, 'bear': sbe, 'flat': sf,
        }
    report['market_env'] = env_report

    # ═══════════════════════════════════════════════════════════
    # 分析2: 多持有期收益
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 分析2: 多持有期收益分析")
    print(f"{'═' * 90}")

    print(f"\n  {'规则':<35s} {'1日acc':>7s} {'3日acc':>7s} {'5日acc':>7s} {'10日acc':>8s} "
          f"{'5日avg':>7s} {'5日med':>7s} {'5日p25':>7s} {'5日p75':>7s}")
    print(f"  {'─' * 90}")

    hold_report = {}
    for name in ['BULL_O_低位缩量_偏离MA20_连跌', 'BULL_H_低位缩量_偏离MA20大', 'BULL_K_低位缩量_偏离MA20_下影线',
                  'BULL_J_极端超跌_偏离MA20', 'BULL_G_极端超跌']:
        recs = rule_data[name]
        r1s = [r[4] for r in recs if r[4] is not None]
        r3s = [r[5] for r in recs if r[5] is not None]
        r5s = [r[6] for r in recs]
        r10s = [r[7] for r in recs if r[7] is not None]

        s1 = acc_stats(r1s)
        s3 = acc_stats(r3s)
        s5 = acc_stats(r5s)
        s10 = acc_stats(r10s)

        print(f"  {name:<35s} {s1['acc']:>7.1%} {s3['acc']:>7.1%} {s5['acc']:>7.1%} {s10['acc']:>8.1%} "
              f"{s5['avg']:>+7.2f} {s5['med']:>+7.2f} {s5['p25']:>+7.2f} {s5['p75']:>+7.2f}")

        hold_report[name] = {
            'r1': s1, 'r3': s3, 'r5': s5, 'r10': s10,
        }
    report['holding_period'] = hold_report

    # ═══════════════════════════════════════════════════════════
    # 分析3: 后半段（近6个月）单独验证
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print(f"📊 分析3: 后半段验证（{all_months[mid]}~{all_months[-1]}）")
    print(f"{'═' * 90}")

    print(f"\n  {'规则':<35s} {'后半段acc':>9s} {'后半段n':>8s} {'后半段avg':>9s} {'后半段med':>9s}")
    print(f"  {'─' * 72}")

    half2_report = {}
    for name in ['BULL_O_低位缩量_偏离MA20_连跌', 'BULL_H_低位缩量_偏离MA20大', 'BULL_K_低位缩量_偏离MA20_下影线',
                  'BULL_J_极端超跌_偏离MA20', 'BULL_G_极端超跌']:
        recs = rule_data[name]
        h2_r5 = [r[6] for r in recs if r[2] in second_half_months]
        s = acc_stats(h2_r5)
        print(f"  {name:<35s} {s['acc']:>9.1%} {s['n']:>8d} {s['avg']:>+9.2f} {s['med']:>+9.2f}")
        half2_report[name] = s
    report['second_half'] = half2_report

    # ═══════════════════════════════════════════════════════════
    # 分析4: 规则融合（多规则同时触发）
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 分析4: 规则融合（多规则同时触发时的准确率）")
    print(f"{'═' * 90}")

    # 为每条记录标记触发了哪些规则
    fusion_report = {}
    rec_triggers = []
    for rec in all_records:
        code, date, month, feat, r1, r3, r5, r10 = rec
        triggers = set()
        for name, filt in CANDIDATES.items():
            try:
                if filt(feat):
                    triggers.add(name)
            except Exception:
                pass
        if triggers:
            rec_triggers.append((rec, triggers))

    # 按触发数量分组
    for min_count in [1, 2, 3, 4, 5]:
        matched = [(rec, trig) for rec, trig in rec_triggers if len(trig) >= min_count]
        if not matched:
            continue
        rets = [rec[6] for rec, _ in matched]
        s = acc_stats(rets)
        # 后半段
        h2_rets = [rec[6] for rec, _ in matched if rec[2] in second_half_months]
        s2 = acc_stats(h2_rets)
        print(f"  触发≥{min_count}条规则: 全量 {s['acc']:.1%} (n={s['n']}, avg={s['avg']:+.2f}%) "
              f"| 后半段 {s2['acc']:.1%} (n={s2['n']})")
        fusion_report[f'ge_{min_count}'] = {'all': s, 'second_half': s2}

    # 特定组合
    combos = [
        ('H+K', {'BULL_H_低位缩量_偏离MA20大', 'BULL_K_低位缩量_偏离MA20_下影线'}),
        ('H+O', {'BULL_H_低位缩量_偏离MA20大', 'BULL_O_低位缩量_偏离MA20_连跌'}),
        ('O+K', {'BULL_O_低位缩量_偏离MA20_连跌', 'BULL_K_低位缩量_偏离MA20_下影线'}),
        ('H+O+K', {'BULL_H_低位缩量_偏离MA20大', 'BULL_O_低位缩量_偏离MA20_连跌', 'BULL_K_低位缩量_偏离MA20_下影线'}),
        ('J+G', {'BULL_J_极端超跌_偏离MA20', 'BULL_G_极端超跌'}),
    ]
    print()
    for label, required in combos:
        matched = [(rec, trig) for rec, trig in rec_triggers if required.issubset(trig)]
        if not matched:
            continue
        rets = [rec[6] for rec, _ in matched]
        s = acc_stats(rets)
        h2_rets = [rec[6] for rec, _ in matched if rec[2] in second_half_months]
        s2 = acc_stats(h2_rets)
        print(f"  {label:<8s}: 全量 {s['acc']:.1%} (n={s['n']}, avg={s['avg']:+.2f}%) "
              f"| 后半段 {s2['acc']:.1%} (n={s2['n']})")
        fusion_report[label] = {'all': s, 'second_half': s2}
    report['fusion'] = fusion_report

    # ═══════════════════════════════════════════════════════════
    # 分析5: 网格搜索最优参数（基于后半段数据验证）
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📊 分析5: 网格搜索最优参数（后半段验证）")
    print(f"{'═' * 90}")

    # 只用后半段数据做验证
    h2_records = [r for r in all_records if r[2] in second_half_months]
    logger.info("  后半段记录: %d条", len(h2_records))

    # 搜索空间
    pos_vals = [0.25, 0.30, 0.33, 0.35]
    vr_vals = [0.5, 0.6, 0.7, 0.8]
    ma20d_vals = [-6, -8, -10, -12]
    cdn_vals = [0, 2, 3]
    lsh_vals = [0, 1]

    print(f"\n  搜索空间: pos={pos_vals}, vr={vr_vals}, ma20d={ma20d_vals}, cdn={cdn_vals}, lsh={lsh_vals}")
    print(f"  总组合: {len(pos_vals)*len(vr_vals)*len(ma20d_vals)*len(cdn_vals)*len(lsh_vals)}")

    best_rules = []
    for pos_th, vr_th, ma20d_th, cdn_th, lsh_th in product(pos_vals, vr_vals, ma20d_vals, cdn_vals, lsh_vals):
        filt = make_rule(pos_th, vr_th, ma20d_th, cdn_th=cdn_th, lsh_th=lsh_th)
        # 全量
        all_rets = []
        for rec in all_records:
            try:
                if filt(rec[3]):
                    all_rets.append(rec[6])
            except Exception:
                pass
        if len(all_rets) < 100:
            continue
        all_acc = sum(1 for r in all_rets if r > 0) / len(all_rets)

        # 后半段
        h2_rets = []
        for rec in h2_records:
            try:
                if filt(rec[3]):
                    h2_rets.append(rec[6])
            except Exception:
                pass
        if len(h2_rets) < 50:
            continue
        h2_acc = sum(1 for r in h2_rets if r > 0) / len(h2_rets)

        # 只保留后半段≥63%的
        if h2_acc >= 0.63:
            best_rules.append({
                'pos': pos_th, 'vr': vr_th, 'ma20d': ma20d_th,
                'cdn': cdn_th, 'lsh': lsh_th,
                'all_acc': round(all_acc, 4), 'all_n': len(all_rets),
                'h2_acc': round(h2_acc, 4), 'h2_n': len(h2_rets),
            })

    # 按后半段准确率排序
    best_rules.sort(key=lambda x: x['h2_acc'], reverse=True)

    print(f"\n  后半段≥63%的参数组合: {len(best_rules)}个")
    print(f"\n  {'pos':>5s} {'vr':>5s} {'ma20d':>6s} {'cdn':>4s} {'lsh':>4s} "
          f"{'全量acc':>8s} {'全量n':>7s} {'后半段acc':>9s} {'后半段n':>8s}")
    print(f"  {'─' * 65}")

    for r in best_rules[:20]:
        print(f"  {r['pos']:>5.2f} {r['vr']:>5.1f} {r['ma20d']:>6d} {r['cdn']:>4d} {r['lsh']:>4d} "
              f"{r['all_acc']:>8.1%} {r['all_n']:>7d} {r['h2_acc']:>9.1%} {r['h2_n']:>8d}")

    report['grid_search'] = best_rules[:20]

    # ═══════════════════════════════════════════════════════════
    # 分析6: 最终可用版本（综合以上分析）
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("📋 分析6: 最终可用版本")
    print(f"{'═' * 90}")

    # 从网格搜索中选出最优参数，要求：
    # 1. 后半段acc≥65%
    # 2. 全量n≥500（足够样本）
    # 3. 后半段n≥200
    final_candidates = [r for r in best_rules
                        if r['h2_acc'] >= 0.65 and r['all_n'] >= 500 and r['h2_n'] >= 200]

    if not final_candidates:
        # 放宽到63%
        final_candidates = [r for r in best_rules
                            if r['h2_acc'] >= 0.63 and r['all_n'] >= 300 and r['h2_n'] >= 100]

    print(f"\n  严格筛选（后半段≥65%, 全量n≥500, 后半段n≥200）: {len([r for r in best_rules if r['h2_acc'] >= 0.65 and r['all_n'] >= 500 and r['h2_n'] >= 200])}个")

    # 对最终候选做完整验证
    print(f"\n  最终候选规则完整验证:")
    print(f"  {'─' * 85}")

    final_rules = []
    for params in final_candidates[:10]:
        filt = make_rule(params['pos'], params['vr'], params['ma20d'],
                         cdn_th=params['cdn'], lsh_th=params['lsh'])
        # 全量多持有期
        matched_all = [(rec[6], rec[4], rec[5], rec[7], rec[2]) for rec in all_records
                       if filt(rec[3])]
        r5_all = [m[0] for m in matched_all]
        r1_all = [m[1] for m in matched_all if m[1] is not None]
        r3_all = [m[2] for m in matched_all if m[2] is not None]
        r10_all = [m[3] for m in matched_all if m[3] is not None]

        # 后半段
        r5_h2 = [m[0] for m in matched_all if m[4] in second_half_months]

        # 月度一致性
        monthly = defaultdict(list)
        for m in matched_all:
            monthly[m[4]].append(m[0])
        m_accs = []
        for month, mrs in monthly.items():
            if len(mrs) >= 10:
                m_accs.append(sum(1 for r in mrs if r > 0) / len(mrs))
        m_win = sum(1 for a in m_accs if a > 0.5) / len(m_accs) if m_accs else 0

        # 市场环境
        bull_r5 = [m[0] for m in matched_all if m[4] in bull_months]
        bear_r5 = [m[0] for m in matched_all if m[4] in bear_months]
        flat_r5 = [m[0] for m in matched_all if m[4] in flat_months]

        s_all = acc_stats(r5_all)
        s_h2 = acc_stats(r5_h2)
        s_bull = acc_stats(bull_r5)
        s_bear = acc_stats(bear_r5)

        label = f"pos≤{params['pos']},vr≤{params['vr']},ma20d<{params['ma20d']}"
        if params['cdn'] > 0:
            label += f",cdn≥{params['cdn']}"
        if params['lsh'] > 0:
            label += f",lsh≥{params['lsh']}"

        print(f"\n  📌 {label}")
        print(f"     全量: {s_all['acc']:.1%} (n={s_all['n']}) | 后半段: {s_h2['acc']:.1%} (n={s_h2['n']})")
        print(f"     1日: {acc_stats(r1_all)['acc']:.1%} | 3日: {acc_stats(r3_all)['acc']:.1%} "
              f"| 5日: {s_all['acc']:.1%} | 10日: {acc_stats(r10_all)['acc']:.1%}")
        print(f"     上涨月: {s_bull['acc']:.1%}(n={s_bull['n']}) | 下跌月: {s_bear['acc']:.1%}(n={s_bear['n']}) "
              f"| 震荡月: {acc_stats(flat_r5)['acc']:.1%}(n={len(flat_r5)})")
        print(f"     月胜率: {m_win:.0%} | 均收益: {s_all['avg']:+.2f}% | 中位收益: {s_all['med']:+.2f}%")

        final_rules.append({
            'params': params,
            'label': label,
            'all': s_all,
            'second_half': s_h2,
            'monthly_win': round(m_win, 3),
            'bull': s_bull,
            'bear': s_bear,
            'flat': acc_stats(flat_r5),
        })

    report['final_rules'] = final_rules

    # ═══════════════════════════════════════════════════════════
    # 最终推荐
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'═' * 90}")
    print("🏆 最终推荐可用规则")
    print(f"{'═' * 90}")

    # 选出后半段最优 + 样本量足够 + 月胜率高的
    if final_rules:
        # 按后半段准确率排序
        ranked = sorted(final_rules, key=lambda x: (x['second_half']['acc'], x['monthly_win']), reverse=True)
        for i, rule in enumerate(ranked[:5]):
            tier = '🥇' if i == 0 else ('🥈' if i == 1 else '🥉' if i == 2 else '  ')
            print(f"\n  {tier} 第{i+1}名: {rule['label']}")
            print(f"     后半段准确率: {rule['second_half']['acc']:.1%} (n={rule['second_half']['n']})")
            print(f"     全量准确率: {rule['all']['acc']:.1%} (n={rule['all']['n']})")
            print(f"     月胜率: {rule['monthly_win']:.0%}")
            print(f"     下跌月准确率: {rule['bear']['acc']:.1%} (n={rule['bear']['n']})")
    else:
        print("\n  无规则满足严格条件，请参考网格搜索结果")

    # 保存
    output_path = OUTPUT_DIR / "volume_price_final_analysis.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print(f"📁 报告: {output_path}")
    print("=" * 90)
    return report


if __name__ == '__main__':
    run_deep_analysis()
