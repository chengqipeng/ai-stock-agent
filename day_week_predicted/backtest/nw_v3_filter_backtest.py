#!/usr/bin/env python3
"""
下周预测 v3过滤器回测 — 验证从月预测同步的过滤器对周预测准确率的影响
====================================================================
v3过滤器（同步月预测86.3%配置，适配周级时间尺度）：
  1. 熊市过滤：大盘连续两周下跌 → 弱涨信号不可靠
  2. 高位过滤：price_pos_60>0.5 → 弱涨信号不可靠
  3. 资金流出过滤：ff_signal<-0.3 → 弱涨信号不可靠
  4. 板块正动量过滤：board_momentum>0.8 → 追涨信号不可靠
  5. 板块置信度增强：负动量-1~-3%也确认涨信号

仅对涨信号+非Tier1高置信度生效。

用法:
    .venv/bin/python -m day_week_predicted.backtest.nw_v3_filter_backtest
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
    _compound_return, _get_stock_index, _mean,
)

N_WEEKS = 29

# ═══════════════════════════════════════════════════════════
# 生产规则集（与 weekly_prediction_service._NW_RULES 一致）
# ═══════════════════════════════════════════════════════════
PROD_RULES = [
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8))},
    {'name': 'R5a:深证+大盘微跌+跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3)},
    {'name': 'R5b:深证+大盘微跌+跌+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2)},
    {'name': 'R5c:深证+大盘微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2)},
    {'name': '跌>2%+主力流入+放量→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -2
                         and (f.get('ff_signal') or 0) > 0.3
                         and (f.get('vol_ratio') or 0) > 1.2)},
    {'name': '涨>3%+资金流入+量价齐升→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] > 3
                         and (f.get('ff_signal') or 0) > 0.2
                         and (f.get('vol_ratio') or 0) > 1.0
                         and (f.get('vol_price_corr') or 0) > 0.3)},
    {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},
    {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2 and f['cu'] >= 3)},
    {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},
    {'name': '资金流出+缩量→跌', 'pred_up': False, 'tier': 3,
     'check': lambda f: ((f.get('ff_signal') or 0) < -0.4
                         and f.get('vol_ratio') is not None
                         and (f.get('vol_ratio') or 0) < 0.7
                         and f['this_chg'] < 0)},
    {'name': '财报利好+资金流入→涨', 'pred_up': True, 'tier': 3,
     'check': lambda f: ((f.get('finance_score') or 0) > 0.5
                         and (f.get('ff_signal') or 0) > 0.2)},
]


def match_rule(feat, rules):
    for rule in rules:
        try:
            if rule['check'](feat):
                return rule
        except (TypeError, KeyError):
            continue
    return None


# ═══════════════════════════════════════════════════════════
# 数据加载
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

    # 股票K线
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

    # 大盘K线
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

    # 资金流向
    stock_fund_flows = defaultdict(list)
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,big_net,net_flow,big_net_pct,main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_fund_flows[r['stock_code']].append({
                'date': r['date'],
                'big_net': _to_float(r.get('big_net', 0)),
                'net_flow': _to_float(r.get('net_flow', 0)),
                'big_net_pct': _to_float(r.get('big_net_pct', 0)),
                'main_net_5day': _to_float(r.get('main_net_5day', 0)),
            })

    # 概念板块关联
    stock_boards = defaultdict(list)
    cur.execute(
        "SELECT stock_code, board_code, board_name FROM stock_concept_board_stock")
    for r in cur.fetchall():
        stock_boards[r['stock_code']].append({
            'board_code': r['board_code'],
            'board_name': r['board_name'],
        })

    # 板块K线
    board_codes = set()
    for bl in stock_boards.values():
        for b in bl:
            board_codes.add(b['board_code'])
    board_klines = defaultdict(list)
    if board_codes:
        bc_list = list(board_codes)
        for i in range(0, len(bc_list), bs):
            batch = bc_list[i:i + bs]
            ph = ','.join(['%s'] * len(batch))
            cur.execute(
                f"SELECT board_code,`date`,change_percent "
                f"FROM concept_board_kline WHERE board_code IN ({ph}) "
                f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
                batch + [start_date, latest_date])
            for r in cur.fetchall():
                board_klines[r['board_code']].append({
                    'date': r['date'],
                    'change_percent': _to_float(r['change_percent']),
                })

    conn.close()

    return {
        'all_codes': all_codes,
        'stock_klines': dict(stock_klines),
        'market_klines': dict(mkt_kl),
        'stock_fund_flows': dict(stock_fund_flows),
        'stock_boards': dict(stock_boards),
        'board_klines': dict(board_klines),
        'latest_date': latest_date,
        'dt_end': dt_end,
    }


# ═══════════════════════════════════════════════════════════
# 样本构建（含资金流向+板块动量）
# ═══════════════════════════════════════════════════════════

def _compute_ff_signal_for_week(ff_list, week_dates):
    """计算某周的资金流向信号"""
    week_set = set(week_dates)
    week_ff = [f for f in ff_list if f['date'] in week_set]
    if not week_ff:
        return None
    big_pcts = [f['big_net_pct'] for f in week_ff if f['big_net_pct'] != 0]
    if not big_pcts:
        return 0.0
    avg = sum(big_pcts) / len(big_pcts)
    return max(-1.0, min(1.0, avg / 5.0))


def _compute_board_momentum(boards, board_klines, latest_date):
    """计算板块动量（5日均涨跌幅）"""
    if not boards:
        return None
    momentums = []
    for b in boards[:5]:
        bk = board_klines.get(b['board_code'], [])
        valid = [k for k in bk if k['date'] <= latest_date]
        if len(valid) >= 5:
            avg_chg = sum(k['change_percent'] for k in valid[-5:]) / 5
            momentums.append(avg_chg)
    if not momentums:
        return None
    return sum(momentums) / len(momentums)


def build_samples(data, n_weeks):
    dt_end = data['dt_end']
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)
    samples = []

    # 大盘按周分组
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
        ff_list = data['stock_fund_flows'].get(code, [])
        # stock_concept_board_stock uses 6-digit codes without suffix
        code_6 = code.split('.')[0] if '.' in code else code
        boards = data['stock_boards'].get(code_6, [])

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            wg[dt.isocalendar()[:2]].append(k)

        sorted_weeks = sorted(wg.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])

        # 大盘也按周分组（用于计算大盘前一周涨跌幅）
        mkt_sorted = sorted(data['market_klines'].get(stock_idx, []),
                            key=lambda x: x['date'])

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
            last_date = this_days[-1]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            # pos60
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            # prev_chg (个股前一周)
            prev_chg = None
            if i > 0:
                prev_iw = sorted_weeks[i - 1]
                prev_days = sorted(wg[prev_iw], key=lambda x: x['date'])
                if len(prev_days) >= 3:
                    prev_chg = _compound_return([d['change_percent'] for d in prev_days])

            # 大盘前一周涨跌幅
            mkt_prev_chg = None
            prev_mw = idx_bw.get(sorted_weeks[i - 1] if i > 0 else None, [])
            if prev_mw and len(prev_mw) >= 3:
                mkt_prev_chg = _compound_return(
                    [k['change_percent'] for k in sorted(prev_mw, key=lambda x: x['date'])])

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

            # 资金流向信号
            week_dates = [d['date'] for d in this_days]
            ff_signal = _compute_ff_signal_for_week(ff_list, week_dates)

            # 板块动量
            board_momentum = _compute_board_momentum(
                boards, data['board_klines'], last_date)

            # 成交量比
            vol_ratio = None
            if len(hist) >= 20:
                this_vols = [d['volume'] for d in this_days if d['volume'] > 0]
                hist_vols = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
                if this_vols and hist_vols:
                    avg_this = sum(this_vols) / len(this_vols)
                    avg_hist = sum(hist_vols) / len(hist_vols)
                    if avg_hist > 0:
                        vol_ratio = avg_this / avg_hist

            samples.append({
                'code': code, 'suffix': suffix,
                'iw_this': iw_this, 'iw_next': iw_next,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'mkt_prev_chg': mkt_prev_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'ff_signal': ff_signal,
                'board_momentum': board_momentum,
                'vol_ratio': vol_ratio,
                'next_chg': next_chg, 'actual_up': next_chg >= 0,
            })

        processed += 1
        if processed % 1000 == 0:
            logger.info("  构建样本: %d/%d ...", processed, len(data['all_codes']))

    return samples


# ═══════════════════════════════════════════════════════════
# v3过滤器
# ═══════════════════════════════════════════════════════════

def apply_v3_filter(sample, rule):
    """检查v3过滤器是否应跳过该预测。
    对涨信号生效（Tier1也检查，但阈值更严格）。
    返回 (should_skip, reason)
    """
    if not rule['pred_up']:
        return False, ''

    # Tier 1 用更严格的阈值（这些规则本身准确率高，只过滤极端情况）
    # Tier 2+ 用标准阈值
    is_tier1 = rule['tier'] == 1

    # 1. 熊市过滤：大盘连续两周下跌
    mkt_prev = sample.get('mkt_prev_chg')
    mkt_chg = sample['mkt_chg']
    bear_thr = -2 if is_tier1 else -1  # Tier1更严格
    if mkt_prev is not None and mkt_chg < bear_thr and mkt_prev < bear_thr:
        return True, '熊市'

    # 2. 高位过滤
    pos60 = sample.get('pos60')
    pos_thr = 0.7 if is_tier1 else 0.5  # Tier1更严格
    if pos60 is not None and pos60 > pos_thr:
        return True, '高位'

    # 3. 资金流出过滤
    ff = sample.get('ff_signal')
    ff_thr = -0.5 if is_tier1 else -0.3  # Tier1更严格
    if ff is not None and ff < ff_thr:
        return True, '资金流出'

    # 4. 板块正动量过滤（追涨信号不可靠）
    bm = sample.get('board_momentum')
    bm_thr = 1.2 if is_tier1 else 0.8  # Tier1更严格
    if bm is not None and bm > bm_thr:
        return True, '板块正动量'

    return False, ''


# ═══════════════════════════════════════════════════════════
# 回测引擎
# ═══════════════════════════════════════════════════════════

def eval_baseline(samples, rules, label='基线'):
    """无v3过滤器的基线回测"""
    total, correct = 0, 0
    by_rule = defaultdict(lambda: [0, 0])
    for s in samples:
        rule = match_rule(s, rules)
        if rule:
            total += 1
            ok = rule['pred_up'] == s['actual_up']
            if ok:
                correct += 1
            by_rule[rule['name']][1] += 1
            if ok:
                by_rule[rule['name']][0] += 1
    return label, total, correct, dict(by_rule)


def eval_with_v3(samples, rules, label='v3过滤'):
    """带v3过滤器的回测"""
    total, correct = 0, 0
    filtered = 0
    filter_reasons = defaultdict(int)
    filter_would_correct = 0
    filter_would_wrong = 0
    by_rule = defaultdict(lambda: [0, 0])

    for s in samples:
        rule = match_rule(s, rules)
        if rule:
            skip, reason = apply_v3_filter(s, rule)
            if skip:
                filtered += 1
                filter_reasons[reason] += 1
                # 统计被过滤的预测是否正确（评估过滤质量）
                if rule['pred_up'] == s['actual_up']:
                    filter_would_correct += 1
                else:
                    filter_would_wrong += 1
                continue
            total += 1
            ok = rule['pred_up'] == s['actual_up']
            if ok:
                correct += 1
            by_rule[rule['name']][1] += 1
            if ok:
                by_rule[rule['name']][0] += 1

    return {
        'label': label, 'total': total, 'correct': correct,
        'filtered': filtered, 'filter_reasons': dict(filter_reasons),
        'filter_would_correct': filter_would_correct,
        'filter_would_wrong': filter_would_wrong,
        'by_rule': dict(by_rule),
    }


def eval_v3_variants(samples, rules):
    """测试不同v3过滤器组合和阈值"""
    # 单个过滤器（对所有涨信号生效，Tier1用严格阈值）
    filters_strict = {
        '熊市(严)': lambda s, r: (r['pred_up']
                                and s.get('mkt_prev_chg') is not None
                                and s['mkt_chg'] < (-2 if r['tier']==1 else -1)
                                and s['mkt_prev_chg'] < (-2 if r['tier']==1 else -1)),
        '高位(严)': lambda s, r: (r['pred_up']
                                and s.get('pos60') is not None
                                and s['pos60'] > (0.7 if r['tier']==1 else 0.5)),
        '资金流出(严)': lambda s, r: (r['pred_up']
                                    and s.get('ff_signal') is not None
                                    and s['ff_signal'] < (-0.5 if r['tier']==1 else -0.3)),
        '板块正动量(严)': lambda s, r: (r['pred_up']
                                      and s.get('board_momentum') is not None
                                      and s['board_momentum'] > (1.2 if r['tier']==1 else 0.8)),
    }
    # 单个过滤器（统一阈值，不区分Tier）
    filters_uniform = {
        '熊市(-1)': lambda s, r: (r['pred_up']
                                 and s.get('mkt_prev_chg') is not None
                                 and s['mkt_chg'] < -1 and s['mkt_prev_chg'] < -1),
        '高位(0.5)': lambda s, r: (r['pred_up']
                                  and s.get('pos60') is not None and s['pos60'] > 0.5),
        '高位(0.7)': lambda s, r: (r['pred_up']
                                  and s.get('pos60') is not None and s['pos60'] > 0.7),
        '高位(0.8)': lambda s, r: (r['pred_up']
                                  and s.get('pos60') is not None and s['pos60'] > 0.8),
        '资金流出(-0.3)': lambda s, r: (r['pred_up']
                                       and s.get('ff_signal') is not None and s['ff_signal'] < -0.3),
        '资金流出(-0.5)': lambda s, r: (r['pred_up']
                                       and s.get('ff_signal') is not None and s['ff_signal'] < -0.5),
        '板块正动量(0.5)': lambda s, r: (r['pred_up']
                                        and s.get('board_momentum') is not None
                                        and s['board_momentum'] > 0.5),
        '板块正动量(0.8)': lambda s, r: (r['pred_up']
                                        and s.get('board_momentum') is not None
                                        and s['board_momentum'] > 0.8),
        '板块正动量(1.2)': lambda s, r: (r['pred_up']
                                        and s.get('board_momentum') is not None
                                        and s['board_momentum'] > 1.2),
    }

    all_filters = {**filters_strict, **filters_uniform}
    results = {}
    for fname, fcheck in all_filters.items():
        total, correct, filtered = 0, 0, 0
        f_correct, f_wrong = 0, 0
        for s in samples:
            rule = match_rule(s, rules)
            if rule:
                if fcheck(s, rule):
                    filtered += 1
                    if rule['pred_up'] == s['actual_up']:
                        f_correct += 1
                    else:
                        f_wrong += 1
                    continue
                total += 1
                if rule['pred_up'] == s['actual_up']:
                    correct += 1
        results[fname] = {
            'total': total, 'correct': correct, 'filtered': filtered,
            'f_correct': f_correct, 'f_wrong': f_wrong,
        }
    return results


def run_cv(samples, rules, use_v3=False, label=''):
    """时间序列交叉验证"""
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    min_train = 12
    if len(all_weeks) < min_train + 1:
        return None

    cv_total, cv_correct, cv_filtered = 0, 0, 0
    for test_idx in range(min_train, len(all_weeks)):
        test_week = all_weeks[test_idx]
        test_samples = [s for s in samples if s['iw_this'] == test_week]
        for s in test_samples:
            rule = match_rule(s, rules)
            if rule:
                if use_v3:
                    skip, _ = apply_v3_filter(s, rule)
                    if skip:
                        cv_filtered += 1
                        continue
                cv_total += 1
                if rule['pred_up'] == s['actual_up']:
                    cv_correct += 1

    return {
        'label': label, 'cv_total': cv_total, 'cv_correct': cv_correct,
        'cv_filtered': cv_filtered,
    }


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def run_backtest():
    t0 = datetime.now()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("=" * 80)
    logger.info("  下周预测 v3过滤器回测")
    logger.info("=" * 80)

    # 1. 加载数据
    logger.info("\n[1/3] 加载数据...")
    data = load_data(N_WEEKS)
    logger.info("[2/3] 构建样本...")
    samples = build_samples(data, N_WEEKS)
    logger.info("  总样本数: %d", len(samples))
    if not samples:
        logger.error("  无有效样本")
        return

    # 统计样本中各信号的覆盖率
    has_ff = sum(1 for s in samples if s.get('ff_signal') is not None)
    has_bm = sum(1 for s in samples if s.get('board_momentum') is not None)
    has_pos = sum(1 for s in samples if s.get('pos60') is not None)
    has_mkt_prev = sum(1 for s in samples if s.get('mkt_prev_chg') is not None)
    logger.info("  信号覆盖: ff_signal=%d(%.1f%%) board_momentum=%d(%.1f%%) "
                "pos60=%d(%.1f%%) mkt_prev=%d(%.1f%%)",
                has_ff, has_ff/len(samples)*100,
                has_bm, has_bm/len(samples)*100,
                has_pos, has_pos/len(samples)*100,
                has_mkt_prev, has_mkt_prev/len(samples)*100)

    # 2. 全样本对比
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ [3/3] 全样本回测 ══")
    logger.info("=" * 80)

    # 基线
    bl_label, bl_total, bl_correct, bl_by_rule = eval_baseline(samples, PROD_RULES)
    bl_acc = bl_correct / bl_total * 100 if bl_total > 0 else 0
    bl_cov = bl_total / len(samples) * 100 if samples else 0
    logger.info("")
    logger.info("  ── 基线（无v3过滤） ──")
    logger.info("  准确率: %.1f%% (%d/%d)  覆盖率: %.1f%%", bl_acc, bl_correct, bl_total, bl_cov)

    # v3过滤
    v3 = eval_with_v3(samples, PROD_RULES)
    v3_acc = v3['correct'] / v3['total'] * 100 if v3['total'] > 0 else 0
    v3_cov = v3['total'] / len(samples) * 100 if samples else 0
    logger.info("")
    logger.info("  ── v3过滤（全部4个过滤器） ──")
    logger.info("  准确率: %.1f%% (%d/%d)  覆盖率: %.1f%%  过滤: %d",
                v3_acc, v3['correct'], v3['total'], v3_cov, v3['filtered'])
    logger.info("  准确率变化: %+.1f%%", v3_acc - bl_acc)
    logger.info("  被过滤预测中: 本来正确%d 本来错误%d → 过滤质量%.1f%%",
                v3['filter_would_correct'], v3['filter_would_wrong'],
                v3['filter_would_wrong'] / v3['filtered'] * 100 if v3['filtered'] > 0 else 0)
    logger.info("  过滤原因: %s", v3['filter_reasons'])

    # 单个过滤器效果
    logger.info("")
    logger.info("  ── 单个过滤器效果 ──")
    variants = eval_v3_variants(samples, PROD_RULES)
    for fname, vr in variants.items():
        vacc = vr['correct'] / vr['total'] * 100 if vr['total'] > 0 else 0
        logger.info("  %-12s 准确率%.1f%%(%+.1f%%) 过滤%d(正确%d/错误%d) 过滤质量%.1f%%",
                    fname, vacc, vacc - bl_acc, vr['filtered'],
                    vr['f_correct'], vr['f_wrong'],
                    vr['f_wrong'] / vr['filtered'] * 100 if vr['filtered'] > 0 else 0)

    # 按规则对比
    logger.info("")
    logger.info("  ── 按规则对比 ──")
    logger.info("  %-45s %15s %15s", "规则", "基线", "v3过滤后")
    all_rules = set(list(bl_by_rule.keys()) + list(v3['by_rule'].keys()))
    for rn in sorted(all_rules):
        bl_r = bl_by_rule.get(rn, [0, 0])
        v3_r = v3['by_rule'].get(rn, [0, 0])
        bl_ra = _p(bl_r[0], bl_r[1])
        v3_ra = _p(v3_r[0], v3_r[1])
        logger.info("  %-45s %6s(%4d) %6s(%4d)",
                    rn, bl_ra, bl_r[1], v3_ra, v3_r[1])

    # 3. 时间序列CV
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ══ 时间序列交叉验证 ══")
    logger.info("=" * 80)

    cv_bl = run_cv(samples, PROD_RULES, use_v3=False, label='基线')
    cv_v3 = run_cv(samples, PROD_RULES, use_v3=True, label='v3过滤')

    if cv_bl and cv_v3:
        cv_bl_acc = cv_bl['cv_correct'] / cv_bl['cv_total'] * 100 if cv_bl['cv_total'] > 0 else 0
        cv_v3_acc = cv_v3['cv_correct'] / cv_v3['cv_total'] * 100 if cv_v3['cv_total'] > 0 else 0
        logger.info("  基线CV:   %.1f%% (%d/%d)", cv_bl_acc, cv_bl['cv_correct'], cv_bl['cv_total'])
        logger.info("  v3过滤CV: %.1f%% (%d/%d) 过滤%d",
                    cv_v3_acc, cv_v3['cv_correct'], cv_v3['cv_total'], cv_v3['cv_filtered'])
        logger.info("  CV准确率变化: %+.1f%%", cv_v3_acc - cv_bl_acc)
        logger.info("")
        logger.info("  全样本 vs CV 过拟合检查:")
        logger.info("    基线: 全样本%.1f%% → CV%.1f%% gap%+.1f%%",
                    bl_acc, cv_bl_acc, bl_acc - cv_bl_acc)
        logger.info("    v3:   全样本%.1f%% → CV%.1f%% gap%+.1f%%",
                    v3_acc, cv_v3_acc, v3_acc - cv_v3_acc)

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  总耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest()
