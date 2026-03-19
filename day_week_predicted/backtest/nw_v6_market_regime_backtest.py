#!/usr/bin/env python3
"""
V6 大盘分场景深度分析回测 — 在V5基础上引入新量化因子
=====================================================
核心思路:
  V5的规则主要覆盖"大盘跌+个股跌→涨"的超跌反弹场景，
  但对"大盘涨"和"大盘平盘"场景覆盖不足。

  V6新增量化因子:
  1. 成交量因子: vol_ratio(量比), vol_shrink(缩量), vol_expand(放量)
  2. 振幅因子: week_amplitude(周振幅), intraday_range(日内波动)
  3. 大盘相对强弱: relative_strength = this_chg - mkt_chg
  4. 动量因子: momentum_3w(3周动量), prev2_chg(前两周涨跌)
  5. 换手率因子: turnover_ratio(本周换手/历史换手)
  6. 量价背离: vol_price_diverge(价涨量缩/价跌量增)

  分场景分析:
  - 大盘涨(mkt_chg > 1%): 个股逆势跌/顺势涨过多的规律
  - 大盘跌(mkt_chg < -1%): 超跌反弹/抗跌强势的规律
  - 大盘平盘(-1% <= mkt_chg <= 1%): 个股自身因子主导

用法:
    python -m day_week_predicted.backtest.nw_v6_market_regime_backtest
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
# V5基线规则（对照组）
# ═══════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════
# 数据加载 — 增加成交额、振幅、换手率
# ═══════════════════════════════════════════════════════════

def load_data(n_weeks):
    """加载K线数据，包含成交量/成交额/振幅/换手率等扩展字段。"""
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    all_codes = _get_all_stock_codes()
    logger.info("股票数: %d", len(all_codes))

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 个股K线 — 增加 amplitude, change_hand, trading_amount
    stock_klines = defaultdict(list)
    bs = 500
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,open_price,close_price,high_price,low_price,"
            f"change_percent,trading_volume,trading_amount,amplitude,change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'],
                'open': _to_float(r['open_price']),
                'close': _to_float(r['close_price']),
                'high': _to_float(r['high_price']),
                'low': _to_float(r['low_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
                'amount': _to_float(r['trading_amount']),
                'amplitude': _to_float(r['amplitude']),
                'turnover': _to_float(r['change_hand']),
            })
        logger.info("  加载K线: %d/%d ...", min(i + bs, len(all_codes)), len(all_codes))

    # 大盘指数K线
    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent,trading_volume "
        f"FROM stock_kline WHERE stock_code IN ({ph}) "
        f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
            'volume': _to_float(r['trading_volume']),
        })

    # 资金流向数据
    stock_fund_flows = defaultdict(list)
    ff_start = dt_start.strftime('%Y-%m-%d')
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,big_net_pct,main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [ff_start, latest_date])
        for r in cur.fetchall():
            stock_fund_flows[r['stock_code']].append({
                'date': r['date'],
                'big_net_pct': _to_float(r['big_net_pct']),
                'main_net_5day': _to_float(r['main_net_5day']),
            })
    logger.info("  资金流向: %d 只有数据", len(stock_fund_flows))

    conn.close()

    return {
        'all_codes': all_codes,
        'stock_klines': dict(stock_klines),
        'market_klines': dict(mkt_kl),
        'stock_fund_flows': dict(stock_fund_flows),
        'latest_date': latest_date,
        'dt_end': dt_end,
    }


# ═══════════════════════════════════════════════════════════
# 样本构建 — 增加新量化因子
# ═══════════════════════════════════════════════════════════

def _safe_mean(lst):
    return sum(lst) / len(lst) if lst else 0

def _safe_std(lst):
    if len(lst) < 2:
        return 0
    m = _safe_mean(lst)
    return math.sqrt(sum((x - m) ** 2 for x in lst) / (len(lst) - 1))


def build_samples(data, n_weeks):
    """构建样本，包含V5原有因子 + V6新增量化因子。"""
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

    # 资金流向按周分组
    ff_data = data.get('stock_fund_flows', {})

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

        # 资金流向按周分组
        ff_list = ff_data.get(code, [])
        ff_by_week = defaultdict(list)
        for ff in ff_list:
            try:
                dt = datetime.strptime(ff['date'], '%Y-%m-%d')
                ff_by_week[dt.isocalendar()[:2]].append(ff)
            except (ValueError, TypeError):
                continue

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

            # 大盘周涨跌
            mw = idx_bw.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            # ── V5原有因子 ──
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

            # ── V6新增因子 ──

            # 1. 成交量因子
            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = _safe_mean(tv)
                ah = _safe_mean(hv)
                if ah > 0:
                    vol_ratio = at / ah

            # 2. 振幅因子: 周内日均振幅
            week_amp = None
            amps = [d['amplitude'] for d in this_days if d.get('amplitude') and d['amplitude'] > 0]
            if amps:
                week_amp = _safe_mean(amps)

            # 3. 换手率因子
            turnover_ratio = None
            tw = [d['turnover'] for d in this_days if d.get('turnover') and d['turnover'] > 0]
            ht = [k['turnover'] for k in hist[-20:] if k.get('turnover') and k['turnover'] > 0]
            if tw and ht:
                at_t = _safe_mean(tw)
                ah_t = _safe_mean(ht)
                if ah_t > 0:
                    turnover_ratio = at_t / ah_t

            # 4. 大盘相对强弱
            relative_strength = this_chg - mkt_chg

            # 5. 量价背离: 价涨量缩 or 价跌量增
            vol_price_diverge = 0  # -1=价涨量缩(看跌), +1=价跌量增(看涨), 0=无背离
            if vol_ratio is not None:
                if this_chg > 1 and vol_ratio < 0.75:
                    vol_price_diverge = -1  # 价涨量缩
                elif this_chg < -1 and vol_ratio > 1.3:
                    vol_price_diverge = 1   # 价跌量增(恐慌抛售后可能反弹)

            # 6. 3周动量
            momentum_3w = None
            if i >= 2:
                prev2_iw = sorted_weeks[i - 2]
                prev2_days = sorted(wg[prev2_iw], key=lambda x: x['date'])
                if len(prev2_days) >= 3:
                    prev2_chg = _compound_return([d['change_percent'] for d in prev2_days])
                    if prev_chg is not None:
                        momentum_3w = prev2_chg + prev_chg + this_chg
                else:
                    prev2_chg = None
            else:
                prev2_chg = None

            # 7. 前两周涨跌
            prev2_chg_val = None
            if i >= 2:
                prev2_iw = sorted_weeks[i - 2]
                prev2_days = sorted(wg[prev2_iw], key=lambda x: x['date'])
                if len(prev2_days) >= 3:
                    prev2_chg_val = _compound_return([d['change_percent'] for d in prev2_days])

            # 8. 周内最大单日涨幅和跌幅
            max_day_up = max(this_pcts) if this_pcts else 0
            max_day_down = min(this_pcts) if this_pcts else 0

            # 9. 资金流向因子: 本周大单净占比均值
            ff_week = ff_by_week.get(iw_this, [])
            big_net_pct_avg = None
            if ff_week:
                pcts = [f['big_net_pct'] for f in ff_week if f['big_net_pct'] != 0]
                if pcts:
                    big_net_pct_avg = _safe_mean(pcts)

            # 10. 大盘周内波动(指数日涨跌幅标准差)
            mkt_vol_std = None
            if mw and len(mw) >= 3:
                mkt_pcts = [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
                mkt_vol_std = _safe_std(mkt_pcts)

            # 11. 个股波动率(本周日涨跌幅标准差)
            stock_vol_std = _safe_std(this_pcts) if len(this_pcts) >= 3 else None

            # 12. 大盘尾日涨跌
            mkt_last_day = None
            if mw and len(mw) >= 1:
                mkt_sorted = sorted(mw, key=lambda x: x['date'])
                mkt_last_day = mkt_sorted[-1]['change_percent']

            samples.append({
                # V5原有
                'code': code, 'suffix': suffix,
                'iw_this': iw_this, 'iw_next': iw_next,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                # V6新增
                'vol_ratio': vol_ratio,
                'week_amp': week_amp,
                'turnover_ratio': turnover_ratio,
                'relative_strength': relative_strength,
                'vol_price_diverge': vol_price_diverge,
                'momentum_3w': momentum_3w,
                'prev2_chg': prev2_chg_val,
                'max_day_up': max_day_up,
                'max_day_down': max_day_down,
                'big_net_pct_avg': big_net_pct_avg,
                'mkt_vol_std': mkt_vol_std,
                'stock_vol_std': stock_vol_std,
                'mkt_last_day': mkt_last_day,
                # 标签
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
    """时间序列交叉验证。"""
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    if len(all_weeks) < MIN_TRAIN_WEEKS + 1:
        return None

    cv_total, cv_correct = 0, 0
    cv_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})

    for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
        test_week = all_weeks[test_idx]
        test_samples = [s for s in samples if s['iw_this'] == test_week]
        for s in test_samples:
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
# 第一阶段: 大盘分场景因子分析
# ═══════════════════════════════════════════════════════════

def analyze_market_regimes(samples):
    """按大盘涨/跌/平盘分场景，分析各因子的预测能力。"""
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    regimes = {
        '大盘涨(>1%)': lambda s: s['mkt_chg'] > 1,
        '大盘微涨(0~1%)': lambda s: 0 <= s['mkt_chg'] <= 1,
        '大盘微跌(-1~0%)': lambda s: -1 <= s['mkt_chg'] < 0,
        '大盘跌(-3~-1%)': lambda s: -3 <= s['mkt_chg'] < -1,
        '大盘深跌(<-3%)': lambda s: s['mkt_chg'] < -3,
    }

    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ 大盘分场景基础统计 ══")
    logger.info("=" * 90)

    for rname, rfilt in regimes.items():
        rs = [s for s in samples if rfilt(s)]
        if not rs:
            continue
        up_cnt = sum(1 for s in rs if s['actual_up'])
        base_rate = up_cnt / len(rs) * 100
        logger.info("  %-20s 样本%d 基准涨率%.1f%%", rname, len(rs), base_rate)

    # 按大盘场景 × 因子组合分析
    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ 大盘涨场景(mkt>1%): 新因子探索 ══")
    logger.info("=" * 90)

    mkt_up_samples = [s for s in samples if s['mkt_chg'] > 1]
    _analyze_factor_combos(mkt_up_samples, '大盘涨')

    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ 大盘跌场景(mkt<-1%): 新因子探索 ══")
    logger.info("=" * 90)

    mkt_down_samples = [s for s in samples if s['mkt_chg'] < -1]
    _analyze_factor_combos(mkt_down_samples, '大盘跌')

    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ 大盘平盘场景(-1%~1%): 新因子探索 ══")
    logger.info("=" * 90)

    mkt_flat_samples = [s for s in samples if -1 <= s['mkt_chg'] <= 1]
    _analyze_factor_combos(mkt_flat_samples, '大盘平盘')


def _analyze_factor_combos(samples, regime_label):
    """在特定大盘场景下，探索因子组合的预测能力。"""
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    if not samples:
        logger.info("  无样本")
        return

    base_up = sum(1 for s in samples if s['actual_up'])
    base_rate = base_up / len(samples) * 100
    logger.info("  基准: %d样本, 涨率%.1f%%", len(samples), base_rate)

    # 定义候选因子条件
    candidates = [
        # ── 涨信号候选 ──
        # 超跌反弹类
        ('跌>3%+缩量(<0.8)', True,
         lambda s: s['this_chg'] < -3 and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8),
        ('跌>3%+放量(>1.3)', True,
         lambda s: s['this_chg'] < -3 and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3),
        ('跌>2%+低位(<0.3)+缩量', True,
         lambda s: (s['this_chg'] < -2 and s['pos60'] is not None and s['pos60'] < 0.3
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8)),
        ('跌>2%+量价背离(价跌量增)', True,
         lambda s: s['this_chg'] < -2 and s['vol_price_diverge'] == 1),
        ('跌>2%+连跌≥3天+缩量', True,
         lambda s: s['this_chg'] < -2 and s['cd'] >= 3 and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8),
        ('跌>2%+尾日跌>3%+低位', True,
         lambda s: (s['this_chg'] < -2 and s['last_day'] < -3
                    and s['pos60'] is not None and s['pos60'] < 0.4)),
        ('跌>3%+前周也跌+非高位', True,
         lambda s: (s['this_chg'] < -3 and s['prev_chg'] is not None and s['prev_chg'] < -2
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('3周动量<-8%+非高位', True,
         lambda s: (s['momentum_3w'] is not None and s['momentum_3w'] < -8
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('跌>2%+高振幅(>4%)+低位', True,
         lambda s: (s['this_chg'] < -2 and s['week_amp'] is not None and s['week_amp'] > 4
                    and s['pos60'] is not None and s['pos60'] < 0.3)),
        ('相对强弱<-5%(大幅弱于大盘)+非高位', True,
         lambda s: (s['relative_strength'] < -5
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),
        ('跌>2%+大单净流入(>2%)', True,
         lambda s: (s['this_chg'] < -2 and s['big_net_pct_avg'] is not None
                    and s['big_net_pct_avg'] > 2)),
        ('跌>2%+换手率放大(>1.5)', True,
         lambda s: (s['this_chg'] < -2 and s['turnover_ratio'] is not None
                    and s['turnover_ratio'] > 1.5)),
        ('跌>2%+大盘尾日涨(>0.5%)', True,
         lambda s: (s['this_chg'] < -2 and s['mkt_last_day'] is not None
                    and s['mkt_last_day'] > 0.5)),
        ('最大单日跌>5%+非高位', True,
         lambda s: (s['max_day_down'] < -5
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7))),

        # ── 跌信号候选 ──
        ('涨>5%+高位(>0.7)+放量', False,
         lambda s: (s['this_chg'] > 5 and s['pos60'] is not None and s['pos60'] >= 0.7
                    and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3)),
        ('涨>3%+量价背离(价涨量缩)', False,
         lambda s: s['this_chg'] > 3 and s['vol_price_diverge'] == -1),
        ('涨>5%+连涨≥3天+高位', False,
         lambda s: (s['this_chg'] > 5 and s['cu'] >= 3
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
        ('涨>3%+高换手(>1.5)+高位', False,
         lambda s: (s['this_chg'] > 3 and s['turnover_ratio'] is not None
                    and s['turnover_ratio'] > 1.5
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
        ('3周动量>10%+高位', False,
         lambda s: (s['momentum_3w'] is not None and s['momentum_3w'] > 10
                    and s['pos60'] is not None and s['pos60'] >= 0.7)),
        ('涨>3%+大单净流出(<-2%)', False,
         lambda s: (s['this_chg'] > 3 and s['big_net_pct_avg'] is not None
                    and s['big_net_pct_avg'] < -2)),
        ('相对强弱>5%(大幅强于大盘)+高位', False,
         lambda s: (s['relative_strength'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.7)),
        ('涨>5%+尾日跌>2%(冲高回落)', False,
         lambda s: s['this_chg'] > 5 and s['last_day'] < -2),
        ('最大单日涨>7%+高位', False,
         lambda s: (s['max_day_up'] > 7
                    and s['pos60'] is not None and s['pos60'] >= 0.7)),
        ('涨>3%+高振幅(>5%)+高位', False,
         lambda s: (s['this_chg'] > 3 and s['week_amp'] is not None and s['week_amp'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.6)),
    ]

    results = []
    for name, pred_up, check_fn in candidates:
        total, correct = 0, 0
        for s in samples:
            try:
                if check_fn(s):
                    total += 1
                    if pred_up == s['actual_up']:
                        correct += 1
            except (TypeError, KeyError):
                continue
        if total < 20:
            continue
        acc = correct / total * 100
        # 与基准比较: 涨信号看涨率提升, 跌信号看跌率提升
        if pred_up:
            lift = acc - base_rate
        else:
            lift = acc - (100 - base_rate)
        flag = '✅' if acc >= 65 and total >= 80 else ('⚠️' if acc >= 58 else '❌')
        logger.info("  %s [%s] %-45s %s (%d/%d) lift%+.1f%%",
                    flag, '涨' if pred_up else '跌', name,
                    _p(correct, total), correct, total, lift)
        results.append({
            'name': f"{regime_label}:{name}",
            'pred_up': pred_up, 'check': check_fn,
            'accuracy': acc, 'total': total, 'lift': lift,
        })

    return results


# ═══════════════════════════════════════════════════════════
# 第二阶段: V6候选规则集 + CV验证
# ═══════════════════════════════════════════════════════════

def build_v6_candidates():
    """V6候选规则 — 在V5基础上增加大盘涨/跌场景的新因子规则。"""
    return [
        # ══════════════════════════════════════════════════
        # 大盘涨场景(mkt>1%): 涨信号
        # ══════════════════════════════════════════════════
        ('MU1:大盘涨+跌>3%+非高位→涨(逆势超跌)',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -3
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7)),
         True),
        ('MU2:大盘涨+跌>2%+缩量+低位→涨',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8
                    and s['pos60'] is not None and s['pos60'] < 0.4),
         True),
        ('MU3:大盘涨+跌>2%+量价背离→涨',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2
                    and s['vol_price_diverge'] == 1),
         True),
        ('MU4:大盘涨+跌>2%+连跌≥3天→涨',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2 and s['cd'] >= 3),
         True),
        ('MU5:大盘涨+跌>2%+前周跌+非高位→涨',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] < -2
                    and s['prev_chg'] is not None and s['prev_chg'] < -2
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7)),
         True),

        # 大盘涨场景: 跌信号
        ('MU6:大盘涨+涨>8%+高位→跌(追高回落)',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] > 8
                    and s['pos60'] is not None and s['pos60'] >= 0.7),
         False),
        ('MU7:大盘涨+涨>5%+高位+放量→跌',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.7
                    and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3),
         False),
        ('MU8:大盘涨+涨>5%+量价背离→跌',
         lambda s: (s['mkt_chg'] > 1 and s['this_chg'] > 5
                    and s['vol_price_diverge'] == -1),
         False),

        # ══════════════════════════════════════════════════
        # 大盘跌场景(mkt<-1%): 涨信号增强
        # ══════════════════════════════════════════════════
        ('MD1:大盘跌+跌>3%+缩量+低位→涨(恐慌缩量底)',
         lambda s: (s['mkt_chg'] < -1 and s['this_chg'] < -3
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8
                    and s['pos60'] is not None and s['pos60'] < 0.3),
         True),
        ('MD2:大盘跌+跌>2%+量价背离(价跌量增)+低位→涨',
         lambda s: (s['mkt_chg'] < -1 and s['this_chg'] < -2
                    and s['vol_price_diverge'] == 1
                    and s['pos60'] is not None and s['pos60'] < 0.4),
         True),
        ('MD3:大盘跌+跌>3%+3周动量<-8%→涨(深度超跌)',
         lambda s: (s['mkt_chg'] < -1 and s['this_chg'] < -3
                    and s['momentum_3w'] is not None and s['momentum_3w'] < -8),
         True),
        ('MD4:大盘跌+跌>2%+大单净流入→涨(主力抄底)',
         lambda s: (s['mkt_chg'] < -1 and s['this_chg'] < -2
                    and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] > 2),
         True),
        ('MD5:大盘跌+跌>2%+大盘尾日涨→涨(大盘企稳)',
         lambda s: (s['mkt_chg'] < -1 and s['this_chg'] < -2
                    and s['mkt_last_day'] is not None and s['mkt_last_day'] > 0.5),
         True),

        # 大盘跌场景: 跌信号
        ('MD6:大盘跌+涨>5%+高位+放量→跌(逆势冲高出货)',
         lambda s: (s['mkt_chg'] < -1 and s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.6
                    and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3),
         False),
        ('MD7:大盘跌+涨>3%+大单净流出→跌',
         lambda s: (s['mkt_chg'] < -1 and s['this_chg'] > 3
                    and s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -2),
         False),

        # ══════════════════════════════════════════════════
        # 大盘平盘场景(-1%~1%): 个股因子主导
        # ══════════════════════════════════════════════════
        ('MF1:平盘+跌>3%+缩量+低位→涨',
         lambda s: (-1 <= s['mkt_chg'] <= 1 and s['this_chg'] < -3
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8
                    and s['pos60'] is not None and s['pos60'] < 0.3),
         True),
        ('MF2:平盘+跌>2%+连跌≥3天+低位→涨',
         lambda s: (-1 <= s['mkt_chg'] <= 1 and s['this_chg'] < -2
                    and s['cd'] >= 3 and s['pos60'] is not None and s['pos60'] < 0.4),
         True),
        ('MF3:平盘+跌>2%+3周动量<-8%+非高位→涨',
         lambda s: (-1 <= s['mkt_chg'] <= 1 and s['this_chg'] < -2
                    and s['momentum_3w'] is not None and s['momentum_3w'] < -8
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7)),
         True),
        ('MF4:平盘+涨>5%+高位+量价背离→跌',
         lambda s: (-1 <= s['mkt_chg'] <= 1 and s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.6
                    and s['vol_price_diverge'] == -1),
         False),
        ('MF5:平盘+涨>5%+高位+高换手→跌',
         lambda s: (-1 <= s['mkt_chg'] <= 1 and s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.6
                    and s['turnover_ratio'] is not None and s['turnover_ratio'] > 1.5),
         False),

        # ══════════════════════════════════════════════════
        # 跨场景通用因子规则
        # ══════════════════════════════════════════════════
        ('GEN1:3周动量<-10%+低位(<0.3)→涨',
         lambda s: (s['momentum_3w'] is not None and s['momentum_3w'] < -10
                    and s['pos60'] is not None and s['pos60'] < 0.3),
         True),
        ('GEN2:跌>3%+量价背离(价跌量增)+非高位→涨',
         lambda s: (s['this_chg'] < -3 and s['vol_price_diverge'] == 1
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7)),
         True),
        ('GEN3:涨>5%+高位+量价背离(价涨量缩)→跌',
         lambda s: (s['this_chg'] > 5 and s['vol_price_diverge'] == -1
                    and s['pos60'] is not None and s['pos60'] >= 0.6),
         False),
        ('GEN4:涨>5%+高位(>0.7)+高换手(>1.5)→跌',
         lambda s: (s['this_chg'] > 5
                    and s['pos60'] is not None and s['pos60'] >= 0.7
                    and s['turnover_ratio'] is not None and s['turnover_ratio'] > 1.5),
         False),
        ('GEN5:最大单日跌>5%+非高位+缩量→涨',
         lambda s: (s['max_day_down'] < -5
                    and not (s['pos60'] is not None and s['pos60'] >= 0.7)
                    and s['vol_ratio'] is not None and s['vol_ratio'] < 0.9),
         True),
        ('GEN6:跌>2%+大单净流入(>3%)+低位→涨',
         lambda s: (s['this_chg'] < -2 and s['big_net_pct_avg'] is not None
                    and s['big_net_pct_avg'] > 3
                    and s['pos60'] is not None and s['pos60'] < 0.4),
         True),
    ]


def cv_validate_v6_candidates(samples, candidates):
    """对V6候选规则做全样本评估 + 时间序列CV验证。"""
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"
    all_weeks = sorted(set(s['iw_this'] for s in samples))

    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ V6候选规则: 全样本 + CV验证 ══")
    logger.info("=" * 90)

    passed = []
    for name, check_fn, pred_up in candidates:
        # 全样本
        total, correct = 0, 0
        for s in samples:
            try:
                if check_fn(s):
                    total += 1
                    if pred_up == s['actual_up']:
                        correct += 1
            except (TypeError, KeyError):
                continue
        if total < 30:
            continue
        full_acc = correct / total * 100

        # CV
        cv_total, cv_correct = 0, 0
        for test_idx in range(MIN_TRAIN_WEEKS, len(all_weeks)):
            test_week = all_weeks[test_idx]
            for s in samples:
                if s['iw_this'] != test_week:
                    continue
                try:
                    if check_fn(s):
                        cv_total += 1
                        if pred_up == s['actual_up']:
                            cv_correct += 1
                except (TypeError, KeyError):
                    continue

        cv_acc = cv_correct / cv_total * 100 if cv_total > 0 else 0
        gap = full_acc - cv_acc

        # 判定: CV>=65% 且 gap<8% 且 样本>=50
        is_good = cv_acc >= 65 and gap < 8 and total >= 50
        is_ok = cv_acc >= 58 and gap < 12 and total >= 30
        flag = '✅' if is_good else ('⚠️' if is_ok else '❌')

        logger.info("  %s %-50s 全样本%s(%d) CV%s(%d) gap%+.1f%%",
                    flag, name, _p(correct, total), total,
                    _p(cv_correct, cv_total), cv_total, gap)

        if is_good or is_ok:
            passed.append({
                'name': name, 'pred_up': pred_up, 'check': check_fn,
                'full_acc': full_acc, 'cv_acc': cv_acc, 'gap': gap,
                'total': total, 'cv_total': cv_total,
                'tier': 1 if is_good else 2,
            })

    logger.info("")
    logger.info("  通过CV验证的规则: %d", len(passed))
    for p in passed:
        logger.info("    Tier%d %-50s CV%.1f%% (%d样本)",
                    p['tier'], p['name'], p['cv_acc'], p['cv_total'])

    return passed


# ═══════════════════════════════════════════════════════════
# 第三阶段: 组合V5+V6规则并评估
# ═══════════════════════════════════════════════════════════

def build_v6_combined_rules(v6_passed):
    """将V5规则 + V6通过验证的规则组合成完整规则集。"""
    combined = list(V5_RULES)  # V5基线

    for p in v6_passed:
        combined.append({
            'name': p['name'],
            'pred_up': p['pred_up'],
            'tier': p['tier'],
            'check': p['check'],
        })

    return combined


def analyze_v5_uncovered(samples):
    """分析V5未覆盖样本的特征分布，寻找改进空间。"""
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ V5未覆盖样本分析 ══")
    logger.info("=" * 90)

    covered, uncovered = [], []
    for s in samples:
        rule = match_rule(s, V5_RULES)
        if rule:
            covered.append(s)
        else:
            uncovered.append(s)

    logger.info("  V5覆盖: %d (%.1f%%), 未覆盖: %d (%.1f%%)",
                len(covered), len(covered)/len(samples)*100,
                len(uncovered), len(uncovered)/len(samples)*100)

    # 未覆盖样本的大盘分布
    for label, filt in [
        ('大盘涨>1%', lambda s: s['mkt_chg'] > 1),
        ('大盘0~1%', lambda s: 0 <= s['mkt_chg'] <= 1),
        ('大盘-1~0%', lambda s: -1 <= s['mkt_chg'] < 0),
        ('大盘<-1%', lambda s: s['mkt_chg'] < -1),
    ]:
        sub = [s for s in uncovered if filt(s)]
        if sub:
            up_rate = sum(1 for s in sub if s['actual_up']) / len(sub) * 100
            logger.info("    %-15s %d样本 涨率%.1f%%", label, len(sub), up_rate)

    # 未覆盖样本中，各因子的分布
    logger.info("")
    logger.info("  ── 未覆盖样本因子分布 ──")

    # 按涨跌幅分桶
    for label, filt in [
        ('跌>5%', lambda s: s['this_chg'] < -5),
        ('跌3~5%', lambda s: -5 <= s['this_chg'] < -3),
        ('跌2~3%', lambda s: -3 <= s['this_chg'] < -2),
        ('跌0~2%', lambda s: -2 <= s['this_chg'] < 0),
        ('涨0~2%', lambda s: 0 <= s['this_chg'] < 2),
        ('涨2~5%', lambda s: 2 <= s['this_chg'] < 5),
        ('涨>5%', lambda s: s['this_chg'] >= 5),
    ]:
        sub = [s for s in uncovered if filt(s)]
        if sub:
            up_rate = sum(1 for s in sub if s['actual_up']) / len(sub) * 100
            logger.info("    %-12s %5d样本 涨率%.1f%%", label, len(sub), up_rate)


# ═══════════════════════════════════════════════════════════
# 第四阶段: V5后置过滤器增强分析
# ═══════════════════════════════════════════════════════════

def analyze_post_filters(samples):
    """分析在V5规则匹配后，新因子作为后置过滤器的效果。"""
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ V5后置过滤器增强分析 ══")
    logger.info("=" * 90)
    logger.info("  (在V5规则匹配后，用新因子进一步筛选提高准确率)")

    # 收集V5匹配的样本
    matched = []
    for s in samples:
        rule = match_rule(s, V5_RULES)
        if rule:
            matched.append((s, rule))

    if not matched:
        return

    base_correct = sum(1 for s, r in matched if r['pred_up'] == s['actual_up'])
    base_acc = base_correct / len(matched) * 100
    logger.info("  V5基线: %d预测, 准确率%s", len(matched), _p(base_correct, len(matched)))

    # 涨信号的后置过滤
    up_matched = [(s, r) for s, r in matched if r['pred_up']]
    logger.info("")
    logger.info("  ── 涨信号后置过滤 (%d个) ──", len(up_matched))

    filters = [
        ('+ 缩量(<0.8)', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] < 0.8),
        ('+ 非放量(<1.5)', lambda s: s['vol_ratio'] is None or s['vol_ratio'] < 1.5),
        ('+ 低位(<0.4)', lambda s: s['pos60'] is not None and s['pos60'] < 0.4),
        ('+ 非高位(<0.6)', lambda s: s['pos60'] is None or s['pos60'] < 0.6),
        ('+ 连跌≥2天', lambda s: s['cd'] >= 2),
        ('+ 大单非净流出', lambda s: s['big_net_pct_avg'] is None or s['big_net_pct_avg'] > -2),
        ('+ 低换手(<1.3)', lambda s: s['turnover_ratio'] is None or s['turnover_ratio'] < 1.3),
        ('+ 大盘尾日非大跌(>-1%)', lambda s: s['mkt_last_day'] is None or s['mkt_last_day'] > -1),
        ('+ 非高振幅(<6%)', lambda s: s['week_amp'] is None or s['week_amp'] < 6),
    ]

    for fname, ffn in filters:
        filtered = [(s, r) for s, r in up_matched if ffn(s)]
        if not filtered:
            continue
        fc = sum(1 for s, r in filtered if s['actual_up'])
        fa = fc / len(filtered) * 100
        up_base_c = sum(1 for s, r in up_matched if s['actual_up'])
        up_base_a = up_base_c / len(up_matched) * 100
        delta = fa - up_base_a
        flag = '✅' if delta > 2 else ('⚠️' if delta > 0 else '❌')
        logger.info("    %s %-35s %s (%d/%d) Δ%+.1f%% 保留%.0f%%",
                    flag, fname, _p(fc, len(filtered)),
                    fc, len(filtered), delta,
                    len(filtered)/len(up_matched)*100)

    # 跌信号的后置过滤
    down_matched = [(s, r) for s, r in matched if not r['pred_up']]
    if down_matched:
        logger.info("")
        logger.info("  ── 跌信号后置过滤 (%d个) ──", len(down_matched))

        down_filters = [
            ('+ 放量(>1.3)', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] > 1.3),
            ('+ 高位(>0.6)', lambda s: s['pos60'] is not None and s['pos60'] >= 0.6),
            ('+ 高换手(>1.3)', lambda s: s['turnover_ratio'] is not None and s['turnover_ratio'] > 1.3),
            ('+ 量价背离', lambda s: s['vol_price_diverge'] == -1),
            ('+ 大单净流出(<-1%)', lambda s: s['big_net_pct_avg'] is not None and s['big_net_pct_avg'] < -1),
        ]

        for fname, ffn in down_filters:
            filtered = [(s, r) for s, r in down_matched if ffn(s)]
            if not filtered:
                continue
            fc = sum(1 for s, r in filtered if not s['actual_up'])
            fa = fc / len(filtered) * 100
            dn_base_c = sum(1 for s, r in down_matched if not s['actual_up'])
            dn_base_a = dn_base_c / len(down_matched) * 100
            delta = fa - dn_base_a
            flag = '✅' if delta > 2 else ('⚠️' if delta > 0 else '❌')
            logger.info("    %s %-35s %s (%d/%d) Δ%+.1f%% 保留%.0f%%",
                        flag, fname, _p(fc, len(filtered)),
                        fc, len(filtered), delta,
                        len(filtered)/len(down_matched)*100)


# ═══════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════

def run_backtest(n_weeks=N_WEEKS):
    t0 = datetime.now()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("=" * 90)
    logger.info("  V6 大盘分场景深度分析回测")
    logger.info("  在V5基础上引入量化因子: 量比/振幅/换手/量价背离/动量/资金流向")
    logger.info("  分场景: 大盘涨 / 大盘跌 / 大盘平盘")
    logger.info("=" * 90)

    # ── 1. 加载数据 ──
    logger.info("\n[1/6] 加载数据(含资金流向)...")
    data = load_data(n_weeks)

    logger.info("[2/6] 构建样本(含新量化因子)...")
    samples = build_samples(data, n_weeks)
    logger.info("  总样本数: %d", len(samples))
    if not samples:
        logger.error("  无有效样本")
        return

    # 基础统计
    up_cnt = sum(1 for s in samples if s['actual_up'])
    logger.info("  全样本涨率: %.1f%% (%d/%d)", up_cnt/len(samples)*100, up_cnt, len(samples))

    # 新因子覆盖率
    has_vol = sum(1 for s in samples if s['vol_ratio'] is not None)
    has_amp = sum(1 for s in samples if s['week_amp'] is not None)
    has_turn = sum(1 for s in samples if s['turnover_ratio'] is not None)
    has_mom = sum(1 for s in samples if s['momentum_3w'] is not None)
    has_ff = sum(1 for s in samples if s['big_net_pct_avg'] is not None)
    logger.info("  因子覆盖: 量比%.0f%% 振幅%.0f%% 换手%.0f%% 动量%.0f%% 资金%.0f%%",
                has_vol/len(samples)*100, has_amp/len(samples)*100,
                has_turn/len(samples)*100, has_mom/len(samples)*100,
                has_ff/len(samples)*100)

    # ── 2. V5基线评估 ──
    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ [3/6] V5基线评估 ══")
    logger.info("=" * 90)

    v5_result = eval_rules(samples, V5_RULES, 'V5基线')
    v5_acc = v5_result['total_correct'] / v5_result['total_pred'] * 100 if v5_result['total_pred'] > 0 else 0
    v5_cov = v5_result['total_pred'] / v5_result['total_samples'] * 100
    logger.info("  V5基线: 准确率%.1f%% 覆盖率%.1f%% (%d/%d)",
                v5_acc, v5_cov, v5_result['total_correct'], v5_result['total_pred'])

    for rn in sorted(v5_result['by_rule'].keys()):
        st = v5_result['by_rule'][rn]
        logger.info("    %-50s %s (%d/%d)", rn,
                    _p(st['correct'], st['total']), st['correct'], st['total'])

    v5_cv = run_cv(samples, V5_RULES, 'V5基线')
    if v5_cv:
        cv_acc = v5_cv['cv_correct'] / v5_cv['cv_total'] * 100 if v5_cv['cv_total'] > 0 else 0
        logger.info("  V5 CV: %.1f%% (%d/%d) gap%+.1f%%",
                    cv_acc, v5_cv['cv_correct'], v5_cv['cv_total'], v5_acc - cv_acc)

    # ── 3. V5未覆盖样本分析 ──
    analyze_v5_uncovered(samples)

    # ── 4. 大盘分场景因子分析 ──
    logger.info("\n[4/6] 大盘分场景因子分析...")
    analyze_market_regimes(samples)

    # ── 5. V6候选规则CV验证 ──
    logger.info("\n[5/6] V6候选规则CV验证...")
    v6_candidates = build_v6_candidates()
    v6_passed = cv_validate_v6_candidates(samples, v6_candidates)

    # ── 5b. V5后置过滤器分析 ──
    analyze_post_filters(samples)

    # ── 6. V5+V6组合评估 ──
    logger.info("\n[6/6] V5+V6组合评估...")
    if v6_passed:
        v6_rules = build_v6_combined_rules(v6_passed)

        logger.info("")
        logger.info("=" * 90)
        logger.info("  ══ V5+V6组合 全样本评估 ══")
        logger.info("=" * 90)

        v6_result = eval_rules(samples, v6_rules, 'V5+V6组合')
        v6_acc = v6_result['total_correct'] / v6_result['total_pred'] * 100 if v6_result['total_pred'] > 0 else 0
        v6_cov = v6_result['total_pred'] / v6_result['total_samples'] * 100
        logger.info("  V5+V6: 准确率%.1f%% 覆盖率%.1f%% (%d/%d)",
                    v6_acc, v6_cov, v6_result['total_correct'], v6_result['total_pred'])

        for rn in sorted(v6_result['by_rule'].keys()):
            st = v6_result['by_rule'][rn]
            logger.info("    %-50s %s (%d/%d)", rn,
                        _p(st['correct'], st['total']), st['correct'], st['total'])

        # CV
        v6_cv = run_cv(samples, v6_rules, 'V5+V6组合')
        if v6_cv:
            cv6_acc = v6_cv['cv_correct'] / v6_cv['cv_total'] * 100 if v6_cv['cv_total'] > 0 else 0
            logger.info("  V5+V6 CV: %.1f%% (%d/%d) gap%+.1f%%",
                        cv6_acc, v6_cv['cv_correct'], v6_cv['cv_total'], v6_acc - cv6_acc)

        # 对比
        logger.info("")
        logger.info("=" * 90)
        logger.info("  ══ V5 vs V5+V6 对比 ══")
        logger.info("=" * 90)
        logger.info("  %-20s 准确率    覆盖率    预测数    CV准确率", "")
        logger.info("  %-20s %.1f%%     %.1f%%     %d       %.1f%%",
                    "V5基线", v5_acc, v5_cov, v5_result['total_pred'],
                    v5_cv['cv_correct']/v5_cv['cv_total']*100 if v5_cv and v5_cv['cv_total'] > 0 else 0)
        logger.info("  %-20s %.1f%%     %.1f%%     %d       %.1f%%",
                    "V5+V6组合", v6_acc, v6_cov, v6_result['total_pred'],
                    v6_cv['cv_correct']/v6_cv['cv_total']*100 if v6_cv and v6_cv['cv_total'] > 0 else 0)

        delta_acc = v6_acc - v5_acc
        delta_cov = v6_cov - v5_cov
        delta_pred = v6_result['total_pred'] - v5_result['total_pred']
        logger.info("  %-20s %+.1f%%     %+.1f%%     %+d",
                    "Δ变化", delta_acc, delta_cov, delta_pred)
    else:
        logger.info("  无V6规则通过CV验证，V5保持不变")

    # ── 综合结论 ──
    logger.info("")
    logger.info("=" * 90)
    logger.info("  ══ 综合结论 ══")
    logger.info("=" * 90)
    logger.info("  1. V5基线: 准确率%.1f%% 覆盖率%.1f%%", v5_acc, v5_cov)
    if v6_passed:
        logger.info("  2. 通过CV验证的V6新规则: %d条", len(v6_passed))
        for p in v6_passed:
            logger.info("     Tier%d %-45s CV%.1f%% gap%+.1f%%",
                        p['tier'], p['name'], p['cv_acc'], p['gap'])
        logger.info("  3. V5+V6组合: 准确率%.1f%% 覆盖率%.1f%%", v6_acc, v6_cov)
    else:
        logger.info("  2. 无新规则通过严格CV验证")
    logger.info("  4. 后置过滤器分析见上方详细输出")

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  总耗时: %.1fs", elapsed)
    logger.info("=" * 90)


if __name__ == '__main__':
    run_backtest(n_weeks=29)
