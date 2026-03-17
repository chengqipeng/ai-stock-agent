#!/usr/bin/env python3
"""
V4全场景规则回测V4 - 加入大盘微跌+深证涨信号
=============================================
残差分析核心发现：
  大盘微跌(0~1%)+深证+个股跌>2% → 涨率78.4% (2943样本) ← 巨大金矿
  大盘微跌+深证+跌>2%+连跌≥3天 → 89.1% (514样本)
  大盘微跌+深证+跌>2%+低位<0.2 → 82.4% (848样本)
  大盘跌1~3%+深证+涨>10% → 23.1%涨率(76.9%跌率)

用法：
    python -m day_week_predicted.backtest.nw_v4_rules_backtest_v4
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

# V4v4 最终规则集
V4V4_RULES = [
    # ══════════════════════════════════════════════════
    # Tier 1: 涨信号 (准确率≥68%)
    # ══════════════════════════════════════════════════

    # R1: 大盘深跌>3% + 个股跌>2% → 涨 (89.6%, 6297样本)
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},

    # R2: 上证+大盘跌1-3%+个股跌>5%+非高位 → 涨 (73.4%, 1745样本)
    {'name': 'R2:上证+大盘跌+跌>5%+非高位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -5 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and not (f['pos60'] is not None and f['pos60'] >= 0.7))},

    # R3: 上证+大盘跌1-3%+个股跌>3%+前周跌 → 涨 (68.3%, 436样本)
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8))},

    # R4: 上证+大盘跌1-3%+个股跌>3%+低位 → 涨 (68.8%, 221样本)
    {'name': 'R4:上证+大盘跌+跌>3%+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['pos60'] is not None and f['pos60'] < 0.2)},

    # ── 新增: 大盘微跌+深证涨信号 ──
    # R5a: 大盘微跌+深证+跌>2%+连跌≥3天 → 涨 (89.1%, 514样本)
    {'name': 'R5a:深证+大盘微跌+跌+连跌3天→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ'
                         and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['cd'] >= 3)},

    # R5b: 大盘微跌+深证+跌>2%+低位<0.2 → 涨 (82.4%, 848样本)
    {'name': 'R5b:深证+大盘微跌+跌+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ'
                         and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2
                         and f['pos60'] is not None and f['pos60'] < 0.2)},

    # R5c: 大盘微跌+深证+跌>2% → 涨 (78.4%, 2943样本) — 宽松版
    {'name': 'R5c:深证+大盘微跌+跌>2%→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ'
                         and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] < -2)},

    # ══════════════════════════════════════════════════
    # Tier 1: 跌信号 (准确率≥68%)
    # ══════════════════════════════════════════════════

    # R6: 深证+大盘跌1~3%+个股涨>5% → 跌 (73.0%, 1292样本)
    {'name': 'R6:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ'
                         and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 5)},

    # R6b: 深证+大盘跌1~3%+个股涨>2%+连涨≥4天 → 跌 (77.1%)
    {'name': 'R6b:深证+大盘跌+涨+连涨4天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ'
                         and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2
                         and f['cu'] >= 4)},

    # R6c: 深证+大盘跌1~3%+个股涨>2%+连涨≥3天 → 跌 (74.0%)
    {'name': 'R6c:深证+大盘跌+涨+连涨3天→跌', 'pred_up': False, 'tier': 1,
     'check': lambda f: (f['suffix'] == 'SZ'
                         and -3 <= f['mkt_chg'] < -1
                         and f['this_chg'] > 2
                         and f['cu'] >= 3)},

    # ══════════════════════════════════════════════════
    # Tier 2: 中等置信信号 (准确率60-68%)
    # ══════════════════════════════════════════════════

    # R7: 跌>3%+连涨≥3天+非高位<0.6 → 跌 (68.5%, 340样本)
    {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3
                         and f['cu'] >= 3
                         and f['pos60'] is not None and f['pos60'] < 0.6)},

    # R8: 涨>10%+尾日跌>3%+前周涨>3% → 跌 (63.8%)
    {'name': 'R8:大涨+尾日回落+前周涨→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] > 10
                         and f['last_day'] < -3
                         and f['prev_chg'] is not None and f['prev_chg'] > 3)},

    # R9: 大盘微跌+上证+涨>2%+前周跌<-3% → 跌 (73.4%)
    {'name': 'R9:上证+大盘微跌+涨+前周跌→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['suffix'] == 'SH'
                         and -1 <= f['mkt_chg'] < 0
                         and f['this_chg'] > 2
                         and f['prev_chg'] is not None and f['prev_chg'] < -3)},
]


def run_backtest(n_weeks=N_WEEKS):
    t0 = datetime.now()
    logger.info("=" * 80)
    logger.info("  V4v4全场景规则回测")
    logger.info("=" * 80)

    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)

    all_codes = _get_all_stock_codes()
    logger.info("股票数: %d", len(all_codes))

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    stock_klines = defaultdict(list)
    bs = 200
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
                'date': r['date'], 'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
            })

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

    mkt_by_week = {}
    for ic, kl in mkt_kl.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    logger.info("数据加载完成, 开始回测...")

    all_weeks = 0
    by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_tier = defaultdict(lambda: {'correct': 0, 'total': 0})
    total_pred = 0
    total_correct = 0
    v3_pred = 0
    v3_correct = 0
    by_suffix = defaultdict(lambda: {'pred': 0, 'correct': 0, 'total': 0})

    V3_RULES_CHECK = V4V4_RULES[:4]  # R1-R4 is V3

    processed = 0
    for code in all_codes:
        klines = stock_klines.get(code, [])
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
            actual_up = next_chg >= 0

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
            pk = hist[-5:] if len(hist) >= 5 else hist
            if pk:
                prev_chg = _compound_return([k['change_percent'] for k in pk])

            cd = 0
            cu = 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0:
                        break
                elif p > 0:
                    cu += 1
                    if cd > 0:
                        break
                else:
                    break

            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = sum(tv) / len(tv)
                ah = sum(hv) / len(hv)
                if ah > 0:
                    vol_ratio = at / ah

            last_day = this_pcts[-1] if this_pcts else 0

            all_weeks += 1
            by_suffix[suffix]['total'] += 1

            feat = {
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'vol_ratio': vol_ratio,
                'suffix': suffix, 'last_day': last_day,
            }

            matched = None
            for rule in V4V4_RULES:
                if rule['check'](feat):
                    matched = rule
                    break

            if matched:
                is_correct = matched['pred_up'] == actual_up
                total_pred += 1
                if is_correct:
                    total_correct += 1
                by_rule[matched['name']]['total'] += 1
                if is_correct:
                    by_rule[matched['name']]['correct'] += 1
                by_tier[matched['tier']]['total'] += 1
                if is_correct:
                    by_tier[matched['tier']]['correct'] += 1
                by_suffix[suffix]['pred'] += 1
                if is_correct:
                    by_suffix[suffix]['correct'] += 1

            for rule in V3_RULES_CHECK:
                if rule['check'](feat):
                    is_c = rule['pred_up'] == actual_up
                    v3_pred += 1
                    if is_c:
                        v3_correct += 1
                    break

        processed += 1
        if processed % 1000 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    elapsed = (datetime.now() - t0).total_seconds()
    _p = lambda c, t: f"{c / t * 100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  V4v4全场景规则回测结果")
    logger.info("=" * 80)
    logger.info("  总可评估周数: %d", all_weeks)
    logger.info("  V4v4预测: %s (%d/%d) 覆盖%s",
                _p(total_correct, total_pred), total_correct, total_pred,
                _p(total_pred, all_weeks))
    logger.info("  V3对比:   %s (%d/%d) 覆盖%s",
                _p(v3_correct, v3_pred), v3_correct, v3_pred,
                _p(v3_pred, all_weeks))

    logger.info("")
    logger.info("  ── 按Tier ──")
    for t in sorted(by_tier.keys()):
        s = by_tier[t]
        logger.info("    Tier %d: %s (%d/%d)", t,
                     _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 按规则 ──")
    for rn in sorted(by_rule.keys()):
        s = by_rule[rn]
        logger.info("    %-50s %s (%d/%d)", rn,
                     _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 按市场 ──")
    for sfx in sorted(by_suffix.keys()):
        s = by_suffix[sfx]
        logger.info("    %s: 预测%s (%d/%d) 覆盖%s",
                     sfx, _p(s['correct'], s['pred']),
                     s['correct'], s['pred'],
                     _p(s['pred'], s['total']))

    logger.info("")
    logger.info("  ── 改进 ──")
    if v3_pred > 0 and total_pred > 0:
        v3_acc = v3_correct / v3_pred * 100
        v4_acc = total_correct / total_pred * 100
        logger.info("    准确率: V3 %.1f%% → V4v4 %.1f%% (%+.1f%%)",
                     v3_acc, v4_acc, v4_acc - v3_acc)
        logger.info("    覆盖率: V3 %.1f%% → V4v4 %.1f%% (%+.1f%%)",
                     v3_pred / all_weeks * 100, total_pred / all_weeks * 100,
                     (total_pred - v3_pred) / all_weeks * 100)
        logger.info("    预测数: V3 %d → V4v4 %d (%+d)",
                     v3_pred, total_pred, total_pred - v3_pred)

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest(n_weeks=29)
