#!/usr/bin/env python3
"""
V6优化回测 — 基于大盘分场景深度分析的结论
==========================================
基于nw_v6_market_regime_backtest.py的分析结果:

关键发现:
  1. R_tail(跌+尾日恐慌→涨) 全样本仅46.4%，严重拖累V5 → 必须移除
  2. 后置过滤器比新规则更有效:
     - 低位(<0.4): +7.7% 保留58%
     - 连跌≥2天: +4.4% 保留60%
     - 缩量(<0.8): +4.3% 保留45%
     - 非高位(<0.6): +3.9% 保留81%
  3. MD6(大盘跌+涨>5%+高位+放量→跌) CV65.3% — 唯一Tier1新规则
  4. 跌信号后置: 放量(>1.3)+3.4%, 高换手(>1.3)+3.2%

V6优化方案:
  A. 移除R_tail（准确率<50%）
  B. 涨信号增加后置过滤: cd>=2 + pos60<0.6（已在生产V7中实现）
  C. 新增MD6: 大盘跌+涨>5%+高位+放量→跌
  D. 探索: 涨信号+缩量/低位组合过滤

用法:
    python -m day_week_predicted.backtest.nw_v6_optimized_backtest
"""
import sys, logging, math
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

def _safe_mean(lst):
    return sum(lst) / len(lst) if lst else 0

# V5基线（含R_tail）
V5_RULES = [
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
    {'name': 'R_tail:跌+尾日恐慌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['last_day'] < -3},
    {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2 and f['cu'] >= 3)},
    {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},
    {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},
]

# V7生产版（移除R_tail，只保留CV>75%的涨信号）
V7_PROD = [
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
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
]

# ═══════════════════════════════════════════════════════════
# V6方案A: V5去R_tail + 后置过滤(cd>=2+pos<0.6)
# ═══════════════════════════════════════════════════════════
V6A_RULES = [
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -2 and f['mkt_chg'] < -3
                         and f['cd'] >= 2
                         and (f['pos60'] is None or f['pos60'] < 0.6))},
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8)
                         and f['cd'] >= 2)},
    {'name': 'R5a:深证+大盘微跌+跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3
                         and (f['pos60'] is None or f['pos60'] < 0.6))},
    {'name': 'R5b:深证+大盘微跌+跌+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2
                         and f['cd'] >= 2)},
    {'name': 'R5c:深证+大盘微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['cd'] >= 2
                         and (f['pos60'] is None or f['pos60'] < 0.6))},
    {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2 and f['cu'] >= 3)},
    {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},
    {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},
]

# ═══════════════════════════════════════════════════════════
# V6方案B: V6A + 新增MD6(大盘跌+涨>5%+高位+放量→跌)
# ═══════════════════════════════════════════════════════════
V6B_RULES = list(V6A_RULES) + [
    {'name': 'MD6:大盘跌+涨>5%+高位+放量→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['mkt_chg'] < -1 and f['this_chg'] > 5
                         and f['pos60'] is not None and f['pos60'] >= 0.6
                         and f['vol_ratio'] is not None and f['vol_ratio'] > 1.3)},
]

# ═══════════════════════════════════════════════════════════
# V6方案C: V6B + 涨信号增加缩量过滤
# ═══════════════════════════════════════════════════════════
V6C_RULES = [
    {'name': 'R1:大盘深跌+个股跌+cd2+非高位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -2 and f['mkt_chg'] < -3
                         and f['cd'] >= 2
                         and (f['pos60'] is None or f['pos60'] < 0.6))},
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌+cd2→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8)
                         and f['cd'] >= 2)},
    {'name': 'R5a:深证+大盘微跌+跌+连跌3天+非高位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3
                         and (f['pos60'] is None or f['pos60'] < 0.6))},
    {'name': 'R5b:深证+大盘微跌+跌+低位+cd2→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2
                         and f['cd'] >= 2)},
    {'name': 'R5c:深证+大盘微跌+跌>2%+cd2+非高位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['cd'] >= 2
                         and (f['pos60'] is None or f['pos60'] < 0.6))},
    # 跌信号
    {'name': 'MD6:大盘跌+涨>5%+高位+放量→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['mkt_chg'] < -1 and f['this_chg'] > 5
                         and f['pos60'] is not None and f['pos60'] >= 0.6
                         and f['vol_ratio'] is not None and f['vol_ratio'] > 1.3)},
    {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2 and f['cu'] >= 3)},
    {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},
    {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},
]

# ═══════════════════════════════════════════════════════════
# V6方案D: V6C + 涨信号低位增强(pos<0.4提升最大)
# ═══════════════════════════════════════════════════════════
V6D_RULES = [
    # 涨信号 — 低位增强版
    {'name': 'R1:大盘深跌+个股跌+cd2+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -2 and f['mkt_chg'] < -3
                         and f['cd'] >= 2
                         and f['pos60'] is not None and f['pos60'] < 0.4)},
    {'name': 'R1b:大盘深跌+个股跌+cd2+非高位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -2 and f['mkt_chg'] < -3
                         and f['cd'] >= 2
                         and (f['pos60'] is None or f['pos60'] < 0.6))},
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌+cd2→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8)
                         and f['cd'] >= 2)},
    {'name': 'R5a:深证+大盘微跌+跌+连跌3天+非高位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2 and f['cd'] >= 3
                         and (f['pos60'] is None or f['pos60'] < 0.6))},
    {'name': 'R5b:深证+大盘微跌+跌+低位+cd2→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2
                         and f['cd'] >= 2)},
    {'name': 'R5c:深证+大盘微跌+跌>2%+cd2+非高位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['cd'] >= 2
                         and (f['pos60'] is None or f['pos60'] < 0.6))},
    # 跌信号
    {'name': 'MD6:大盘跌+涨>5%+高位+放量→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['mkt_chg'] < -1 and f['this_chg'] > 5
                         and f['pos60'] is not None and f['pos60'] >= 0.6
                         and f['vol_ratio'] is not None and f['vol_ratio'] > 1.3)},
    {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2 and f['cu'] >= 3)},
    {'name': 'R6a:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SZ' and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},
    {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3 and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},
]


# ═══════════════════════════════════════════════════════════
# 数据加载 & 样本构建
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
            f"SELECT stock_code,`date`,close_price,change_percent,"
            f"trading_volume,amplitude,change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'],
                'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
                'amplitude': _to_float(r['amplitude']),
                'turnover': _to_float(r['change_hand']),
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

            # 量比
            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = _safe_mean(tv)
                ah = _safe_mean(hv)
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

    return {
        'label': label,
        'total_samples': len(samples),
        'total_pred': total_pred,
        'total_correct': total_correct,
        'by_rule': dict(by_rule),
    }


def run_cv(samples, rules, label=''):
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    if len(all_weeks) < MIN_TRAIN_WEEKS + 1:
        return None

    cv_total, cv_correct = 0, 0
    cv_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})

    for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
        test_week = all_weeks[test_idx]
        for s in samples:
            if s['iw_this'] != test_week:
                continue
            rule = match_rule(s, rules)
            if rule:
                cv_total += 1
                cv_by_rule[rule['name']]['total'] += 1
                if rule['pred_up'] == s['actual_up']:
                    cv_correct += 1
                    cv_by_rule[rule['name']]['correct'] += 1

    return {
        'label': label,
        'cv_total': cv_total, 'cv_correct': cv_correct,
        'cv_by_rule': dict(cv_by_rule),
    }


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def run_backtest(n_weeks=N_WEEKS):
    t0 = datetime.now()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("=" * 90)
    logger.info("  V6优化回测 — 基于大盘分场景分析的结论")
    logger.info("=" * 90)

    logger.info("\n[1/3] 加载数据...")
    data = load_data(n_weeks)
    logger.info("[2/3] 构建样本...")
    samples = build_samples(data, n_weeks)
    logger.info("  总样本数: %d", len(samples))

    configs = [
        ('V5(含R_tail)', V5_RULES),
        ('V7生产版(精简)', V7_PROD),
        ('V6A:去R_tail+后置过滤', V6A_RULES),
        ('V6B:V6A+MD6跌信号', V6B_RULES),
        ('V6C:V6B+涨信号增强', V6C_RULES),
        ('V6D:V6C+低位分层', V6D_RULES),
    ]

    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ [3/3] 全样本 + CV对比 ══")
    logger.info("=" * 90)

    all_results = {}
    for label, rules in configs:
        result = eval_rules(samples, rules, label)
        cv = run_cv(samples, rules, label)
        all_results[label] = (result, cv)

        acc = result['total_correct'] / result['total_pred'] * 100 if result['total_pred'] > 0 else 0
        cov = result['total_pred'] / result['total_samples'] * 100
        cv_acc = cv['cv_correct'] / cv['cv_total'] * 100 if cv and cv['cv_total'] > 0 else 0
        gap = acc - cv_acc if cv else 0

        logger.info("")
        logger.info("  ── %s ──", label)
        logger.info("  全样本: 准确率%.1f%% 覆盖率%.1f%% (%d/%d)",
                    acc, cov, result['total_correct'], result['total_pred'])
        if cv:
            logger.info("  CV:     准确率%.1f%% (%d/%d) gap%+.1f%%",
                        cv_acc, cv['cv_correct'], cv['cv_total'], gap)

        # 按规则
        for rn in sorted(result['by_rule'].keys()):
            st = result['by_rule'][rn]
            cv_st = cv['cv_by_rule'].get(rn, {'correct': 0, 'total': 0}) if cv else {'correct': 0, 'total': 0}
            r_acc = st['correct'] / st['total'] * 100 if st['total'] > 0 else 0
            cv_r_acc = cv_st['correct'] / cv_st['total'] * 100 if cv_st['total'] > 0 else 0
            r_gap = r_acc - cv_r_acc
            flag = '⚠️' if r_gap > 5 else '✅'
            logger.info("    %s %-50s 全样本%s(%d) CV%s(%d) gap%+.1f%%",
                        flag, rn,
                        _p(st['correct'], st['total']), st['total'],
                        _p(cv_st['correct'], cv_st['total']), cv_st['total'], r_gap)

    # 汇总对比表
    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ 汇总对比 ══")
    logger.info("=" * 90)
    logger.info("  %-30s %8s %8s %8s %8s %8s", "方案", "准确率", "CV准确率", "gap", "覆盖率", "预测数")
    for label, rules in configs:
        result, cv = all_results[label]
        acc = result['total_correct'] / result['total_pred'] * 100 if result['total_pred'] > 0 else 0
        cov = result['total_pred'] / result['total_samples'] * 100
        cv_acc = cv['cv_correct'] / cv['cv_total'] * 100 if cv and cv['cv_total'] > 0 else 0
        gap = acc - cv_acc if cv else 0
        logger.info("  %-30s %7.1f%% %7.1f%% %+7.1f%% %7.1f%% %8d",
                    label, acc, cv_acc, gap, cov, result['total_pred'])

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  总耗时: %.1fs", elapsed)
    logger.info("=" * 90)


if __name__ == '__main__':
    run_backtest(n_weeks=29)
