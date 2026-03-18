#!/usr/bin/env python3
"""
V5精炼回测 — 仅应用经CV验证通过的优化
======================================
基于nw_v5_optimization_backtest.py的结果，筛选出有效优化:

已验证有效:
  ✅ P0: 移除R2(CV62.0%,gap+11.4%)和R8(CV66.7%,gap+6.6%) → 准确率+1.2%
  ✅ P4a: 跌>2%+尾日跌>3%→涨 (全样本70.6%, CV75.9%) — 作为独立新规则
  ✅ R6c扩展: R6c在V4中被R6a遮挡，移除R2/R8后R3覆盖扩大(681→681)

已验证无效（不采用）:
  ❌ P2: 连续两周跌→涨 (56.7%~58.4%, 接近随机)
  ❌ P3: 大盘涨+逆势 (37.1%~55.9%, 大部分低于随机)
  ❌ P5: 北交所规则 (无数据，已排除)
  ❌ R10/R11/R12/R13: 全部<56%准确率

V5精炼方案:
  = V4基线 - R2 - R8 + R_tail(尾日恐慌涨信号)
  预期: 准确率82.8%+ 覆盖率8.7%+

用法:
    python -m day_week_predicted.backtest.nw_v5_refined_backtest
"""
import sys, logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float,
    _compound_return, _get_stock_index,
)

N_WEEKS = 29
MIN_TRAIN_WEEKS = 12


# ═══════════════════════════════════════════════════════════
# 规则集定义
# ═══════════════════════════════════════════════════════════

V4_RULES = [
    # R1: 大盘深跌>3% + 个股跌>2% → 涨 (89.6%, CV89.5%)
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
    # R2: 上证+大盘跌1-3%+跌>5%+非高位 → 涨 (CV62.0%, gap+11.4%) — 过拟合
    {'name': 'R2:上证+大盘跌+跌>5%+非高位→涨', 'pred_up': True, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -5 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and not (f['pos60'] is not None and f['pos60'] >= 0.7))},
    # R3: 上证+大盘跌1-3%+跌>3%+前周跌 → 涨 (67.5%, CV71.4%)
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8))},
    # R5a: 深证+大盘微跌+跌>2%+连跌≥3天 → 涨 (CV90.6%)
    {'name': 'R5a:深证+大盘微跌+跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3)},
    # R5b: 深证+大盘微跌+跌>2%+低位 → 涨 (CV86.4%)
    {'name': 'R5b:深证+大盘微跌+跌+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2)},
    # R5c: 深证+大盘微跌+跌>2% → 涨 (CV79.6%)
    {'name': 'R5c:深证+大盘微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2)},
    # R6a: 深证+大盘跌+涨>5% → 跌 (CV63.6%, gap+9.4%) — Tier2
    {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},
    # R6c: 深证+大盘跌+涨>2%+连涨≥3天 → 跌 (69.6%, CV60.4%)
    {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2 and f['cu'] >= 3)},
    # R7: 跌>3%+连涨≥3天+非高位 → 跌 (CV73.2%)
    {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},
    # R8: 上证+大盘微跌+涨+前周跌 → 跌 (CV66.7%, gap+6.6%) — 过拟合
    {'name': 'R8:上证+大盘微跌+涨+前周跌→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SH' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] > 2
                         and f['prev_chg'] is not None and f['prev_chg'] < -3)},
]

V5_REFINED_RULES = [
    # ══════════════════════════════════════════════════
    # Tier 1: 涨信号 — 仅保留CV验证通过的规则
    # ══════════════════════════════════════════════════

    # R1: 大盘深跌>3% + 个股跌>2% → 涨 (CV89.5%, 最稳健)
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},

    # R3: 上证+大盘跌1-3%+跌>3%+前周跌 → 涨 (CV71.4%)
    # 注: 移除R2后，R3覆盖范围扩大（原被R2遮挡的样本现在由R3捕获）
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8))},

    # R5a: 深证+大盘微跌+跌>2%+连跌≥3天 → 涨 (CV90.6%)
    {'name': 'R5a:深证+大盘微跌+跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3)},

    # R5b: 深证+大盘微跌+跌>2%+低位 → 涨 (CV86.4%)
    {'name': 'R5b:深证+大盘微跌+跌+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2)},

    # R5c: 深证+大盘微跌+跌>2% → 涨 (CV79.6%)
    {'name': 'R5c:深证+大盘微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2)},

    # R_tail(新): 跌>2%+尾日跌>3% → 涨 (全样本70.6%, CV75.9%)
    # 逻辑: 尾日恐慌性大跌是短期超卖信号，下周反弹概率高
    {'name': 'R_tail:跌+尾日恐慌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['last_day'] < -3},

    # ══════════════════════════════════════════════════
    # Tier 1: 跌信号
    # ══════════════════════════════════════════════════

    # R6c: 深证+大盘跌+涨>2%+连涨≥3天 → 跌 (CV60.4%)
    # 注: CV偏低但逻辑合理，保留Tier1但需关注
    {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2 and f['cu'] >= 3)},

    # ══════════════════════════════════════════════════
    # Tier 2: 中等置信信号
    # ══════════════════════════════════════════════════

    # R6a: 深证+大盘跌+涨>5% → 跌 (CV63.6%) — 保留Tier2
    {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},

    # R7: 跌>3%+连涨≥3天+非高位 → 跌 (CV73.2%)
    {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},
]

# R_tail放在不同位置的变体（测试优先级影响）
V5_TAIL_FIRST = list(V5_REFINED_RULES)  # R_tail在R5c之后

V5_TAIL_AFTER_R1 = [
    V5_REFINED_RULES[0],  # R1
    # R_tail紧跟R1（优先级更高）
    {'name': 'R_tail:跌+尾日恐慌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['last_day'] < -3},
] + V5_REFINED_RULES[1:5] + V5_REFINED_RULES[6:]  # 跳过原R_tail位置


# ═══════════════════════════════════════════════════════════
# 数据加载 & 样本构建（复用V5优化回测的逻辑）
# ═══════════════════════════════════════════════════════════

def load_data(n_weeks):
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    all_codes = _get_all_stock_codes()
    logger.info("股票数: %d", len(all_codes))

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    stock_klines = defaultdict(list)
    bs = 500
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,close_price,change_percent,trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'],
                'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
            })
        logger.info("  加载K线: %d/%d ...", min(i + bs, len(all_codes)), len(all_codes))

    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })
    conn.close()

    return {
        'all_codes': all_codes,
        'stock_klines': dict(stock_klines),
        'market_klines': dict(mkt_kl),
        'latest_date': latest_date,
        'dt_end': dt_end,
    }


def build_samples(data, n_weeks):
    dt_end = data['dt_end']
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)
    samples = []

    mkt_by_week = {}
    for ic, kl in data['market_klines'].items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    processed = 0
    for code in data['all_codes']:
        klines = data['stock_klines'].get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
        idx_bw = mkt_by_week.get(stock_idx, {})

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            wg[dt.isocalendar()[:2]].append(k)

        sorted_weeks = sorted(wg.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])

        for i in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[i]
            iw_next = sorted_weeks[i + 1]
            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])
            if len(this_days) < 3 or len(next_days) < 3:
                continue
            dt_this = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_this < dt_cutoff:
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            this_chg = _compound_return(this_pcts)
            next_chg = _compound_return([d['change_percent'] for d in next_days])

            mw = idx_bw.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            prev_chg = None
            if i > 0:
                prev_iw = sorted_weeks[i - 1]
                prev_days = sorted(wg[prev_iw], key=lambda x: x['date'])
                if len(prev_days) >= 3:
                    prev_chg = _compound_return([d['change_percent'] for d in prev_days])

            cd, cu = 0, 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else:
                    break

            last_day = this_pcts[-1] if this_pcts else 0

            samples.append({
                'code': code, 'suffix': suffix,
                'iw_this': iw_this, 'iw_next': iw_next,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'next_chg': next_chg, 'actual_up': next_chg >= 0,
            })

        processed += 1
        if processed % 1000 == 0:
            logger.info("  构建样本: %d/%d ...", processed, len(data['all_codes']))

    return samples


# ═══════════════════════════════════════════════════════════
# 回测引擎
# ═══════════════════════════════════════════════════════════

def match_rule(feat, rules):
    for rule in rules:
        try:
            if rule['check'](feat):
                return rule
        except (TypeError, KeyError):
            continue
    return None


def eval_rules(samples, rules, label=''):
    total_pred, total_correct = 0, 0
    by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_tier = defaultdict(lambda: {'correct': 0, 'total': 0})

    for s in samples:
        rule = match_rule(s, rules)
        if rule:
            is_correct = rule['pred_up'] == s['actual_up']
            total_pred += 1
            if is_correct:
                total_correct += 1
            by_rule[rule['name']]['total'] += 1
            if is_correct:
                by_rule[rule['name']]['correct'] += 1
            by_tier[rule['tier']]['total'] += 1
            if is_correct:
                by_tier[rule['tier']]['correct'] += 1

    return {
        'label': label,
        'total_samples': len(samples),
        'total_pred': total_pred,
        'total_correct': total_correct,
        'by_rule': dict(by_rule),
        'by_tier': dict(by_tier),
    }


def run_cv(samples, rules, label=''):
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    if len(all_weeks) < MIN_TRAIN_WEEKS + 1:
        return None

    cv_total, cv_correct = 0, 0
    cv_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    cv_by_week = {}

    for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
        test_week = all_weeks[test_idx]
        test_samples = [s for s in samples if s['iw_this'] == test_week]
        if not test_samples:
            continue

        wt, wc = 0, 0
        for s in test_samples:
            rule = match_rule(s, rules)
            if rule:
                wt += 1
                cv_by_rule[rule['name']]['total'] += 1
                if rule['pred_up'] == s['actual_up']:
                    wc += 1
                    cv_by_rule[rule['name']]['correct'] += 1

        cv_total += wt
        cv_correct += wc
        cv_by_week[test_week] = (wc, wt)

    return {
        'label': label,
        'cv_total': cv_total, 'cv_correct': cv_correct,
        'cv_by_rule': dict(cv_by_rule), 'cv_by_week': cv_by_week,
    }


# ═══════════════════════════════════════════════════════════
# R_tail位置优化分析
# ═══════════════════════════════════════════════════════════

def analyze_tail_overlap(samples):
    """分析R_tail与现有规则的重叠情况。"""
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"
    logger.info("")
    logger.info("  ── R_tail重叠分析 ──")

    # R_tail条件: this_chg < -2 and last_day < -3
    tail_samples = [s for s in samples if s['this_chg'] < -2 and s['last_day'] < -3]
    logger.info("  R_tail候选样本: %d", len(tail_samples))

    # 检查这些样本中有多少已被V4规则覆盖
    v4_rules = [r for r in V4_RULES if 'R2:' not in r['name'] and 'R8:' not in r['name']]
    covered, uncovered = 0, 0
    covered_correct, uncovered_correct = 0, 0
    for s in tail_samples:
        rule = match_rule(s, v4_rules)
        if rule:
            covered += 1
            if rule['pred_up'] == s['actual_up']:
                covered_correct += 1
        else:
            uncovered += 1
            # R_tail预测涨
            if s['actual_up']:
                uncovered_correct += 1

    logger.info("  已被V4(无R2R8)覆盖: %d (准确率%s)", covered, _p(covered_correct, covered))
    logger.info("  R_tail新增覆盖: %d (准确率%s)", uncovered, _p(uncovered_correct, uncovered))
    logger.info("  → R_tail应放在现有规则之后，仅捕获未覆盖样本")


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def run_backtest(n_weeks=N_WEEKS):
    t0 = datetime.now()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("=" * 80)
    logger.info("  V5精炼回测 — 仅应用CV验证通过的优化")
    logger.info("=" * 80)

    # 1. 加载数据
    logger.info("\n[1/4] 加载数据...")
    data = load_data(n_weeks)
    logger.info("[2/4] 构建样本...")
    samples = build_samples(data, n_weeks)
    logger.info("  总样本数: %d", len(samples))
    if not samples:
        logger.error("  无有效样本")
        return

    # 2. 全样本对比
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ [3/4] 全样本对比 ══")
    logger.info("=" * 80)

    configs = [
        ('V4-基线', V4_RULES),
        ('V5a-移除R2R8', [r for r in V4_RULES if 'R2:' not in r['name'] and 'R8:' not in r['name']]),
        ('V5b-精炼(推荐)', V5_REFINED_RULES),
        ('V5c-tail优先', V5_TAIL_AFTER_R1),
    ]

    full_results = {}
    for label, rules in configs:
        result = eval_rules(samples, rules, label)
        full_results[label] = result
        acc = result['total_correct'] / result['total_pred'] * 100 if result['total_pred'] > 0 else 0
        cov = result['total_pred'] / result['total_samples'] * 100 if result['total_samples'] > 0 else 0
        logger.info("  %-25s 准确率%.1f%% (%d/%d) 覆盖率%.1f%%",
                    label, acc, result['total_correct'], result['total_pred'], cov)

    # 按规则详细
    for label in ['V4-基线', 'V5b-精炼(推荐)']:
        logger.info("")
        logger.info("  ── %s 按规则 ──", label)
        r = full_results[label]
        for rn in sorted(r['by_rule'].keys()):
            st = r['by_rule'][rn]
            logger.info("    %-50s %s (%d/%d)", rn,
                        _p(st['correct'], st['total']), st['correct'], st['total'])

    # 3. R_tail重叠分析
    analyze_tail_overlap(samples)

    # 4. 时间序列CV
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ [4/4] 时间序列交叉验证 ══")
    logger.info("=" * 80)

    for label, rules in configs:
        cv = run_cv(samples, rules, label)
        if cv is None:
            continue
        fr = full_results[label]
        full_acc = fr['total_correct'] / fr['total_pred'] * 100 if fr['total_pred'] > 0 else 0
        cv_acc = cv['cv_correct'] / cv['cv_total'] * 100 if cv['cv_total'] > 0 else 0
        gap = full_acc - cv_acc
        logger.info("  %-25s 全样本%.1f%% → CV%.1f%% (%d/%d) gap%+.1f%%",
                    label, full_acc, cv_acc, cv['cv_correct'], cv['cv_total'], gap)

        # 按规则CV
        for rn in sorted(cv['cv_by_rule'].keys()):
            st = cv['cv_by_rule'][rn]
            full_st = fr['by_rule'].get(rn, {'correct': 0, 'total': 0})
            cv_r_acc = st['correct'] / st['total'] * 100 if st['total'] > 0 else 0
            full_r_acc = full_st['correct'] / full_st['total'] * 100 if full_st['total'] > 0 else 0
            r_gap = full_r_acc - cv_r_acc
            flag = '⚠️' if r_gap > 5 else '✅'
            logger.info("    %s %-42s 全样本%s(%d) CV%s(%d) gap%+.1f%%",
                        flag, rn,
                        _p(full_st['correct'], full_st['total']), full_st['total'],
                        _p(st['correct'], st['total']), st['total'], r_gap)
        logger.info("")

    # ═══════════════════════════════════════════════════════════
    # 综合结论
    # ═══════════════════════════════════════════════════════════
    logger.info("=" * 80)
    logger.info("  ══ 综合结论与建议 ══")
    logger.info("=" * 80)
    logger.info("")
    logger.info("  V4→V5精炼 变更清单:")
    logger.info("    1. 移除R2(上证+大盘跌+跌>5%+非高位→涨) — CV62.0%,过拟合gap+11.4%%")
    logger.info("    2. 移除R8(上证+大盘微跌+涨+前周跌→跌) — CV66.7%,过拟合gap+6.6%%")
    logger.info("    3. 新增R_tail(跌>2%+尾日跌>3%→涨) — CV75.9%%,稳健")
    logger.info("    4. 保留其余规则不变")
    logger.info("")

    for label in ['V4-基线', 'V5b-精炼(推荐)']:
        r = full_results[label]
        acc = r['total_correct'] / r['total_pred'] * 100 if r['total_pred'] > 0 else 0
        cov = r['total_pred'] / r['total_samples'] * 100 if r['total_samples'] > 0 else 0
        logger.info("  %-25s 准确率%.1f%% 覆盖率%.1f%% 预测数%d",
                    label, acc, cov, r['total_pred'])

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  总耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest(n_weeks=29)
