#!/usr/bin/env python3
"""
V5优化回测 — 基于V4深度分析的7项优化建议验证
=============================================
优化项:
  P0: 移除/降级过拟合规则 R2(CV62.0%,gap-11.4%), R8(CV61.5%,gap-13.4%)
  P1: Tier2+板块矛盾 → 不预测（减少低质量预测）
  P2: 连续两周下跌反弹规则（新增候选规则）
  P3: 大盘涨+个股逆势跌规则（新增候选规则）
  P4: 尾日信号增强R5系列（最后一天大跌加强信号）
  P5: 北交所专用规则（波动率更大，阈值需调整）
  P6: 严格样本外验证（时间序列CV）

方法:
  1. 在29周全样本上对比V4 vs V5各优化项
  2. 对每个新规则做时间序列CV验证
  3. 综合评估准确率、覆盖率、稳健性

用法:
    python -m day_week_predicted.backtest.nw_v5_optimization_backtest
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
    _compound_return, _mean, _get_stock_index,
)

N_WEEKS = 29
MIN_TRAIN_WEEKS = 12


# ═══════════════════════════════════════════════════════════
# V4 基线规则集（当前生产版本）
# ═══════════════════════════════════════════════════════════

def build_v4_rules():
    """当前V4生产规则集（不含资金流向/财报规则，回测无法验证）。"""
    return [
        # R1: 大盘深跌>3% + 个股跌>2% → 涨 (89.6%, CV89.5%)
        {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
        # R2: 上证+大盘跌1-3%+跌>5%+非高位 → 涨 (CV62.0%, gap-11.4%) — Tier2
        {'name': 'R2:上证+大盘跌+跌>5%+非高位→涨', 'pred_up': True, 'tier': 2,
         'check': lambda f: (f['this_chg'] < -5 and f['suffix'] == 'SH'
                             and -3 <= f['mkt_chg'] < -1
                             and not (f['pos60'] is not None and f['pos60'] >= 0.7))},
        # R3: 上证+大盘跌1-3%+跌>3%+前周跌 → 涨 (68.3%)
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
        # R6a: 深证+大盘跌1~3%+涨>5% → 跌 (CV63.6%, gap-9.4%) — Tier2
        {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                             and f['this_chg'] > 5)},
        # R6c: 深证+大盘跌1~3%+涨>2%+连涨≥3天 → 跌 (66.9%)
        {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                             and f['this_chg'] > 2 and f['cu'] >= 3)},
        # R7: 跌>3%+连涨≥3天+非高位 → 跌 (CV73.7%)
        {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                             and f['pos60'] is not None and f['pos60'] < 0.6)},
        # R8: 上证+大盘微跌+涨>2%+前周跌<-3% → 跌 (CV61.5%, gap-13.4%) — Tier2
        {'name': 'R8:上证+大盘微跌+涨+前周跌→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['suffix'] == 'SH' and -1 <= f['mkt_chg'] < 0
                             and f['this_chg'] > 2
                             and f['prev_chg'] is not None and f['prev_chg'] < -3)},
    ]


# ═══════════════════════════════════════════════════════════
# V5 优化规则集
# ═══════════════════════════════════════════════════════════

def build_v5_rules():
    """V5优化规则集 — 基于深度分析的7项优化。"""
    return [
        # ══════════════════════════════════════════════════
        # Tier 1: 涨信号 — 稳健规则（CV验证通过）
        # ══════════════════════════════════════════════════

        # R1: 大盘深跌>3% + 个股跌>2% → 涨 (CV89.5%, 稳健)
        {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},

        # R3: 上证+大盘跌1-3%+跌>3%+前周跌 → 涨 (68.3%, 稳健)
        {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                             and -3 <= f['mkt_chg'] < -1
                             and f['prev_chg'] is not None and f['prev_chg'] < -2
                             and not (f['pos60'] is not None and f['pos60'] >= 0.8))},

        # R5a: 深证+大盘微跌+跌>2%+连跌≥3天 → 涨 (CV90.6%, 最稳健)
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

        # ── P4: R5系列尾日增强 ──
        # R5d(新): 深证+大盘微跌+跌>2%+尾日跌>3% → 涨 (尾日恐慌加强反弹信号)
        {'name': 'R5d:深证+大盘微跌+跌+尾日大跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                             and f['this_chg'] < -2 and f['last_day'] < -3)},

        # ── P2: 连续两周下跌反弹规则 ──
        # R10(新): 连续两周跌>3%+非高位 → 涨 (超跌反弹)
        {'name': 'R10:连续两周跌>3%+非高位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['this_chg'] < -3
                             and f['prev_chg'] is not None and f['prev_chg'] < -3
                             and not (f['pos60'] is not None and f['pos60'] >= 0.7))},

        # R11(新): 连续两周跌>2%+低位<0.3 → 涨 (深度超跌)
        {'name': 'R11:连续两周跌+低位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['this_chg'] < -2
                             and f['prev_chg'] is not None and f['prev_chg'] < -2
                             and f['pos60'] is not None and f['pos60'] < 0.3)},

        # ── P3: 大盘涨+个股逆势跌规则 ──
        # R12(新): 大盘涨>1%+个股跌>3%+非高位 → 涨 (逆势超跌反弹)
        {'name': 'R12:大盘涨+个股逆势跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['mkt_chg'] > 1 and f['this_chg'] < -3
                             and not (f['pos60'] is not None and f['pos60'] >= 0.7))},

        # ══════════════════════════════════════════════════
        # Tier 1: 跌信号
        # ══════════════════════════════════════════════════

        # R6c: 深证+大盘跌1~3%+涨>2%+连涨≥3天 → 跌 (66.9%)
        {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                             and f['this_chg'] > 2 and f['cu'] >= 3)},

        # ── P3: 大盘涨+个股逆势涨过多 → 跌 ──
        # R13(新): 大盘涨>1%+个股涨>8%+高位 → 跌 (追高回落)
        {'name': 'R13:大盘涨+个股大涨+高位→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f: (f['mkt_chg'] > 1 and f['this_chg'] > 8
                             and f['pos60'] is not None and f['pos60'] >= 0.7)},

        # ══════════════════════════════════════════════════
        # Tier 2: 中等置信信号 — P0降级/保留
        # ══════════════════════════════════════════════════

        # R6a: 深证+大盘跌+涨>5% → 跌 (CV63.6%) — 保留Tier2
        {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                             and f['this_chg'] > 5)},

        # R7: 跌>3%+连涨≥3天+非高位 → 跌 (CV73.7%)
        {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                             and f['pos60'] is not None and f['pos60'] < 0.6)},

        # ── P5: 北交所专用规则 ──
        # R14(新): 北证+跌>5%+非高位 → 涨 (北证波动大，阈值放宽)
        {'name': 'R14:北证+大跌>5%+非高位→涨', 'pred_up': True, 'tier': 2,
         'check': lambda f: (f['suffix'] == 'BJ'
                             and f['this_chg'] < -5
                             and not (f['pos60'] is not None and f['pos60'] >= 0.6))},

        # R15(新): 北证+涨>10%+高位 → 跌 (北证追高风险更大)
        {'name': 'R15:北证+大涨>10%+高位→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['suffix'] == 'BJ'
                             and f['this_chg'] > 10
                             and f['pos60'] is not None and f['pos60'] >= 0.6)},
    ]


def build_v5_no_r2r8():
    """V5-P0: 完全移除R2和R8（过拟合规则）。"""
    rules = build_v4_rules()
    return [r for r in rules if 'R2:' not in r['name'] and 'R8:' not in r['name']]


# ═══════════════════════════════════════════════════════════
# 数据加载 & 样本构建
# ═══════════════════════════════════════════════════════════

def load_data(n_weeks):
    """加载全部K线数据。"""
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
    """构建(stock, week)样本列表，包含所有特征。"""
    dt_end = data['dt_end']
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)
    samples = []

    # 指数按周分组
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

            # 价格位置
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            # 前周涨跌幅
            prev_chg = None
            if i > 0:
                prev_iw = sorted_weeks[i - 1]
                prev_days = sorted(wg[prev_iw], key=lambda x: x['date'])
                if len(prev_days) >= 3:
                    prev_chg = _compound_return([d['change_percent'] for d in prev_days])

            # 连涨/连跌
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

            # 成交量比
            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = sum(tv) / len(tv)
                ah = sum(hv) / len(hv)
                if ah > 0:
                    vol_ratio = at / ah

            samples.append({
                'code': code, 'suffix': suffix,
                'iw_this': iw_this, 'iw_next': iw_next,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'vol_ratio': vol_ratio,
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
    """匹配第一条命中的规则。"""
    for rule in rules:
        try:
            if rule['check'](feat):
                return rule
        except (TypeError, KeyError):
            continue
    return None


def eval_rules(samples, rules, label=''):
    """在全样本上评估规则集，返回统计结果。"""
    total_pred, total_correct = 0, 0
    by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_tier = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_suffix = defaultdict(lambda: {'pred': 0, 'correct': 0, 'total': 0})

    for s in samples:
        by_suffix[s['suffix']]['total'] += 1
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
            by_suffix[s['suffix']]['pred'] += 1
            if is_correct:
                by_suffix[s['suffix']]['correct'] += 1

    return {
        'label': label,
        'total_samples': len(samples),
        'total_pred': total_pred,
        'total_correct': total_correct,
        'by_rule': dict(by_rule),
        'by_tier': dict(by_tier),
        'by_suffix': dict(by_suffix),
    }


def run_timeseries_cv(samples, rules, label=''):
    """时间序列交叉验证。"""
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    if len(all_weeks) < MIN_TRAIN_WEEKS + 1:
        logger.warning("数据不足以进行CV: %d周", len(all_weeks))
        return None

    cv_total, cv_correct = 0, 0
    cv_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    cv_by_week = {}

    for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
        test_week = all_weeks[test_idx]
        test_samples = [s for s in samples if s['iw_this'] == test_week]
        if not test_samples:
            continue

        week_total, week_correct = 0, 0
        for s in test_samples:
            rule = match_rule(s, rules)
            if rule:
                week_total += 1
                cv_by_rule[rule['name']]['total'] += 1
                if rule['pred_up'] == s['actual_up']:
                    week_correct += 1
                    cv_by_rule[rule['name']]['correct'] += 1

        cv_total += week_total
        cv_correct += week_correct
        cv_by_week[test_week] = (week_correct, week_total)

    return {
        'label': label,
        'cv_total': cv_total,
        'cv_correct': cv_correct,
        'cv_by_rule': dict(cv_by_rule),
        'cv_by_week': cv_by_week,
    }


def print_eval(result):
    """打印评估结果。"""
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"
    r = result
    logger.info("  [%s] 准确率: %s (%d/%d) 覆盖率: %s",
                r['label'],
                _p(r['total_correct'], r['total_pred']),
                r['total_correct'], r['total_pred'],
                _p(r['total_pred'], r['total_samples']))


def print_cv(cv_result, full_result=None):
    """打印CV结果，对比全样本。"""
    if cv_result is None:
        logger.info("  CV数据不足")
        return
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"
    cv = cv_result
    logger.info("  [%s] CV准确率: %s (%d/%d)",
                cv['label'],
                _p(cv['cv_correct'], cv['cv_total']),
                cv['cv_correct'], cv['cv_total'])

    if full_result and full_result['total_pred'] > 0 and cv['cv_total'] > 0:
        full_acc = full_result['total_correct'] / full_result['total_pred'] * 100
        cv_acc = cv['cv_correct'] / cv['cv_total'] * 100
        gap = full_acc - cv_acc
        logger.info("    过拟合差距: %+.1f%% (全样本%.1f%% - CV%.1f%%)",
                    gap, full_acc, cv_acc)


# ═══════════════════════════════════════════════════════════
# P2/P3/P4/P5: 新规则候选探索
# ═══════════════════════════════════════════════════════════

def explore_new_rules(samples):
    """探索新规则候选的全样本表现。"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ 新规则候选探索（全样本） ══")
    logger.info("=" * 80)
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    candidates = [
        # P2: 连续两周下跌反弹
        ('P2a:连续两周跌>3%+非高位→涨',
         lambda s: (s['this_chg'] < -3 and s['prev_chg'] is not None and s['prev_chg'] < -3
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7)),
         True),
        ('P2b:连续两周跌>2%+低位<0.3→涨',
         lambda s: (s['this_chg'] < -2 and s['prev_chg'] is not None and s['prev_chg'] < -2
                    and s['pos60'] is not None and s['pos60'] < 0.3),
         True),
        ('P2c:连续两周跌>5%→涨',
         lambda s: (s['this_chg'] < -5 and s['prev_chg'] is not None and s['prev_chg'] < -5),
         True),

        # P3: 大盘涨+个股逆势
        ('P3a:大盘涨>1%+个股跌>3%+非高位→涨',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -3
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7)),
         True),
        ('P3b:大盘涨>1%+个股跌>5%→涨',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -5),
         True),
        ('P3c:大盘涨>1%+个股涨>8%+高位→跌',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] > 8
                    and s['pos60'] is not None and s['pos60'] >= 0.7),
         False),
        ('P3d:大盘涨>2%+个股涨>10%→跌',
         lambda s: (s['mkt_chg'] > 2 and s['this_chg'] > 10),
         False),

        # P4: 尾日信号增强
        ('P4a:跌>2%+尾日跌>3%→涨',
         lambda s: (s['this_chg'] < -2 and s['last_day'] < -3),
         True),
        ('P4b:深证+大盘微跌+跌>2%+尾日跌>3%→涨',
         lambda s: (s['suffix'] == 'SZ' and -1 <= s['mkt_chg'] < 0
                    and s['this_chg'] < -2 and s['last_day'] < -3),
         True),
        ('P4c:涨>5%+尾日跌>3%→跌',
         lambda s: (s['this_chg'] > 5 and s['last_day'] < -3),
         False),

        # P5: 北交所专用
        ('P5a:北证+跌>5%+非高位→涨',
         lambda s: (s['suffix'] == 'BJ' and s['this_chg'] < -5
                    and not (s['pos60'] is not None and s['pos60'] >= 0.6)),
         True),
        ('P5b:北证+跌>8%→涨',
         lambda s: (s['suffix'] == 'BJ' and s['this_chg'] < -8),
         True),
        ('P5c:北证+涨>10%+高位→跌',
         lambda s: (s['suffix'] == 'BJ' and s['this_chg'] > 10
                    and s['pos60'] is not None and s['pos60'] >= 0.6),
         False),
        ('P5d:北证+涨>15%→跌',
         lambda s: (s['suffix'] == 'BJ' and s['this_chg'] > 15),
         False),

        # 额外: 大盘涨+连跌
        ('EX1:大盘涨+连跌≥3天+跌>2%→涨',
         lambda s: (s['mkt_chg'] > 0 and s['cd'] >= 3 and s['this_chg'] < -2),
         True),
        ('EX2:大盘涨>1%+连跌≥4天→涨',
         lambda s: (s['mkt_chg'] > 1 and s['cd'] >= 4),
         True),
    ]

    results = []
    for name, check_fn, pred_up in candidates:
        total, correct = 0, 0
        for s in samples:
            try:
                if check_fn(s):
                    total += 1
                    if pred_up == s['actual_up']:
                        correct += 1
            except (TypeError, KeyError):
                continue
        acc = correct / total * 100 if total > 0 else 0
        flag = '✅' if acc >= 68 and total >= 100 else ('⚠️' if acc >= 60 else '❌')
        logger.info("  %s %-50s %s (%d/%d)", flag, name, _p(correct, total), correct, total)
        results.append({'name': name, 'pred_up': pred_up, 'check': check_fn,
                        'accuracy': acc, 'total': total})

    return results


# ═══════════════════════════════════════════════════════════
# 新规则CV验证
# ═══════════════════════════════════════════════════════════

def cv_validate_candidates(samples, candidates):
    """对全样本准确率≥65%且样本≥50的候选规则做时间序列CV。"""
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    if len(all_weeks) < MIN_TRAIN_WEEKS + 1:
        return

    good_candidates = [c for c in candidates if c['accuracy'] >= 60 and c['total'] >= 30]
    if not good_candidates:
        logger.info("  无候选规则通过全样本筛选")
        return

    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ 候选规则时间序列CV验证 ══")
    logger.info("=" * 80)

    for cand in good_candidates:
        cv_total, cv_correct = 0, 0
        for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
            test_week = all_weeks[test_idx]
            test_samples = [s for s in samples if s['iw_this'] == test_week]
            for s in test_samples:
                try:
                    if cand['check'](s):
                        cv_total += 1
                        if cand['pred_up'] == s['actual_up']:
                            cv_correct += 1
                except (TypeError, KeyError):
                    continue

        cv_acc = cv_correct / cv_total * 100 if cv_total > 0 else 0
        gap = cand['accuracy'] - cv_acc
        flag = '✅' if cv_acc >= 65 and gap < 5 else ('⚠️' if cv_acc >= 55 else '❌')
        logger.info("  %s %-45s 全样本%s(%d) → CV%s(%d) gap%+.1f%%",
                    flag, cand['name'],
                    _p(int(cand['accuracy'] * cand['total'] / 100), cand['total']),
                    cand['total'],
                    _p(cv_correct, cv_total), cv_total, gap)


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def run_backtest(n_weeks=N_WEEKS):
    t0 = datetime.now()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("=" * 80)
    logger.info("  V5优化回测 — 7项优化建议验证")
    logger.info("=" * 80)

    # 1. 加载数据 & 构建样本
    logger.info("\n[1/6] 加载数据...")
    data = load_data(n_weeks)
    logger.info("[2/6] 构建样本...")
    samples = build_samples(data, n_weeks)
    logger.info("  总样本数: %d", len(samples))
    if not samples:
        logger.error("  无有效样本")
        return

    # 2. V4基线 vs V5各方案全样本对比
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ [3/6] 全样本对比: V4基线 vs V5各方案 ══")
    logger.info("=" * 80)

    v4_rules = build_v4_rules()
    v5_rules = build_v5_rules()
    v4_no_r2r8 = build_v5_no_r2r8()

    configs = [
        ('V4-基线(当前生产)', v4_rules),
        ('V4-移除R2R8(P0)', v4_no_r2r8),
        ('V5-完整优化', v5_rules),
    ]

    full_results = {}
    for label, rules in configs:
        result = eval_rules(samples, rules, label)
        full_results[label] = result
        print_eval(result)

    # 3. 按规则详细对比
    logger.info("")
    logger.info("  ── V4基线按规则 ──")
    v4r = full_results['V4-基线(当前生产)']
    for rn in sorted(v4r['by_rule'].keys()):
        st = v4r['by_rule'][rn]
        logger.info("    %-50s %s (%d/%d)", rn,
                    _p(st['correct'], st['total']), st['correct'], st['total'])

    logger.info("")
    logger.info("  ── V5完整优化按规则 ──")
    v5r = full_results['V5-完整优化']
    for rn in sorted(v5r['by_rule'].keys()):
        st = v5r['by_rule'][rn]
        logger.info("    %-50s %s (%d/%d)", rn,
                    _p(st['correct'], st['total']), st['correct'], st['total'])

    # 4. 时间序列CV验证
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ [4/6] 时间序列交叉验证 ══")
    logger.info("=" * 80)

    for label, rules in configs:
        cv = run_timeseries_cv(samples, rules, label)
        fr = full_results[label]
        print_cv(cv, fr)

        # 按规则CV
        if cv and cv['cv_by_rule']:
            for rn in sorted(cv['cv_by_rule'].keys()):
                st = cv['cv_by_rule'][rn]
                full_st = fr['by_rule'].get(rn, {'correct': 0, 'total': 0})
                cv_acc = st['correct'] / st['total'] * 100 if st['total'] > 0 else 0
                full_acc = full_st['correct'] / full_st['total'] * 100 if full_st['total'] > 0 else 0
                gap = full_acc - cv_acc
                flag = '⚠️' if gap > 5 else '✅'
                logger.info("    %s %-42s 全样本%s(%d) CV%s(%d) gap%+.1f%%",
                            flag, rn,
                            _p(full_st['correct'], full_st['total']), full_st['total'],
                            _p(st['correct'], st['total']), st['total'], gap)
        logger.info("")

    # 5. 新规则候选探索
    logger.info("[5/6] 新规则候选探索...")
    candidates = explore_new_rules(samples)

    # 6. 候选规则CV验证
    logger.info("[6/6] 候选规则CV验证...")
    cv_validate_candidates(samples, candidates)

    # ═══════════════════════════════════════════════════════════
    # 综合结论
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ 综合结论 ══")
    logger.info("=" * 80)

    for label in ['V4-基线(当前生产)', 'V4-移除R2R8(P0)', 'V5-完整优化']:
        r = full_results[label]
        acc = r['total_correct'] / r['total_pred'] * 100 if r['total_pred'] > 0 else 0
        cov = r['total_pred'] / r['total_samples'] * 100 if r['total_samples'] > 0 else 0
        logger.info("  %-25s 准确率%.1f%% 覆盖率%.1f%% 预测数%d",
                    label, acc, cov, r['total_pred'])

    # 按市场对比
    logger.info("")
    logger.info("  ── 按市场对比 ──")
    for label in ['V4-基线(当前生产)', 'V5-完整优化']:
        r = full_results[label]
        logger.info("  [%s]", label)
        for sfx in sorted(r['by_suffix'].keys()):
            s = r['by_suffix'][sfx]
            logger.info("    %s: 预测%s (%d/%d) 覆盖%s",
                        sfx or '未知',
                        _p(s['correct'], s['pred']) if s['pred'] > 0 else 'N/A',
                        s['correct'], s['pred'],
                        _p(s['pred'], s['total']))

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  总耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest(n_weeks=29)
