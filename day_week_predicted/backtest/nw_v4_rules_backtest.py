#!/usr/bin/env python3
"""
V4全场景规则回测验证
====================
基于全场景因子挖掘结果，设计覆盖所有大盘环境的规则并回测。

V4规则设计（不限大盘环境）：
  涨信号:
    R1: 大盘深跌>3% + 个股跌>2% → 涨 (已有T1a, ~89%)
    R2: 上证大盘跌1-3% + 个股跌>5% + 非高位 → 涨 (已有T1b, ~73%)
    R3: 上证大盘跌1-3% + 个股跌>3% + 前周跌 → 涨 (已有T1c, ~68%)
    R4: 上证大盘跌1-3% + 个股跌>3% + 低位 → 涨 (已有T1d, ~69%)
    R5: 个股跌>8% + 低位<0.3 → 涨 (不限大盘, ~72%)
    R6: 个股跌>5% + 低位<0.2 → 涨 (不限大盘, ~64%)
    R7: 连跌≥4天 + 非高位 → 涨 (~63%)
    R8: 个股跌>3% + 缩量<0.6 + 低位<0.4 → 涨 (恐慌缩量见底)
  跌信号:
    R9: 大盘跌1~3% + 个股涨>3% → 跌 (~35%涨率=65%跌率)
    R10: 个股涨>8% + 高位>0.8 → 跌 (~45%涨率)
    R11: 个股涨>5% + 前周大涨>5% → 跌 (连续大涨后回调)
    R12: 连涨≥4天 + 高位>0.6 → 跌 (~42%涨率)

用法：
    python -m day_week_predicted.backtest.nw_v4_rules_backtest
"""
import sys, logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float,
    _compound_return, _get_stock_index,
)

N_WEEKS = 29

# V4候选规则（按优先级排列，互斥匹配）
V4_RULES = [
    # ── 涨信号 ──
    # R1: 大盘深跌>3% + 个股跌>2% (已验证89%)
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},

    # R2: 上证+大盘跌1-3%+个股跌>5%+非高位 (已验证73%)
    {'name': 'R2:上证+大盘跌+跌>5%+非高位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -5 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and not (f['pos60'] is not None and f['pos60'] >= 0.7))},

    # R3: 上证+大盘跌1-3%+个股跌>3%+前周跌 (已验证68%)
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8))},

    # R4: 上证+大盘跌1-3%+个股跌>3%+低位 (已验证69%)
    {'name': 'R4:上证+大盘跌+跌>3%+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['pos60'] is not None and f['pos60'] < 0.2)},

    # R5: 个股跌>8% + 低位<0.3 → 涨 (不限大盘, 72%)
    {'name': 'R5:跌>8%+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -8
                         and f['pos60'] is not None and f['pos60'] < 0.3)},

    # R6: 个股跌>5% + 低位<0.2 → 涨 (不限大盘, 64%)
    {'name': 'R6:跌>5%+极低位→涨', 'pred_up': True, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -5
                         and f['pos60'] is not None and f['pos60'] < 0.2)},

    # R7: 连跌≥4天 + 非高位 → 涨 (63%)
    {'name': 'R7:连跌≥4天+非高位→涨', 'pred_up': True, 'tier': 2,
     'check': lambda f: (f['cd'] >= 4
                         and not (f['pos60'] is not None and f['pos60'] >= 0.7))},

    # R8: 个股跌>3% + 缩量<0.6 + 低位<0.4 → 涨 (恐慌缩量见底)
    {'name': 'R8:跌+缩量+偏低位→涨', 'pred_up': True, 'tier': 2,
     'check': lambda f: (f['this_chg'] < -3
                         and f['vol_ratio'] is not None and f['vol_ratio'] < 0.6
                         and f['pos60'] is not None and f['pos60'] < 0.4)},

    # ── 跌信号 ──
    # R9: 大盘跌1~3% + 个股涨>3% → 跌 (涨率~35%)
    {'name': 'R9:大盘跌+个股涨>3%→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['mkt_chg'] < -1 and f['this_chg'] > 3)},

    # R10: 个股涨>8% + 高位>0.8 → 跌 (涨率~45%)
    {'name': 'R10:涨>8%+高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] > 8
                         and f['pos60'] is not None and f['pos60'] > 0.8)},

    # R11: 个股涨>5% + 前周大涨>5% → 跌 (连续大涨后回调)
    {'name': 'R11:涨>5%+前周大涨→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['this_chg'] > 5
                         and f['prev_chg'] is not None and f['prev_chg'] > 5)},

    # R12: 连涨≥4天 + 偏高位 → 跌 (42%涨率)
    {'name': 'R12:连涨≥4天+偏高位→跌', 'pred_up': False, 'tier': 2,
     'check': lambda f: (f['cu'] >= 4
                         and f['pos60'] is not None and f['pos60'] > 0.6)},
]


def run_backtest(n_weeks=N_WEEKS, sample_limit=0):
    t0 = datetime.now()
    logger.info("=" * 80)
    logger.info("  V4全场景规则回测 (n_weeks=%d)", n_weeks)
    logger.info("=" * 80)

    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("股票数: %d", len(all_codes))

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    logger.info("加载个股K线...")
    stock_klines = defaultdict(list)
    bs = 200
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i+bs]
        ph = ','.join(['%s']*len(batch))
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

    logger.info("加载指数K线...")
    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH','399001.SZ','899050.SZ'):
        if idx not in idx_codes: idx_codes.append(idx)
    ph = ','.join(['%s']*len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({'date': r['date'], 'change_percent': _to_float(r['change_percent'])})
    conn.close()

    mkt_by_week = {}
    for ic, kl in mkt_kl.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    logger.info("数据加载完成, 开始回测...")

    # 统计
    all_weeks = 0
    by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_tier = defaultdict(lambda: {'correct': 0, 'total': 0})
    total_pred = 0; total_correct = 0
    # V3对比
    v3_pred = 0; v3_correct = 0

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

            cd = 0; cu = 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else: break

            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = sum(tv) / len(tv)
                ah = sum(hv) / len(hv)
                if ah > 0:
                    vol_ratio = at / ah

            all_weeks += 1

            feat = {
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'vol_ratio': vol_ratio,
                'suffix': suffix,
            }

            # V4规则匹配
            matched = None
            for rule in V4_RULES:
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

            # V3对比（只有R1-R4）
            v3_matched = False
            for rule in V4_RULES[:4]:
                if rule['check'](feat):
                    v3_matched = True
                    is_c = rule['pred_up'] == actual_up
                    v3_pred += 1
                    if is_c: v3_correct += 1
                    break

        processed += 1
        if processed % 1000 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    # 输出
    elapsed = (datetime.now() - t0).total_seconds()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  V4全场景规则回测结果")
    logger.info("=" * 80)

    logger.info("  总可评估周数: %d", all_weeks)
    logger.info("  V4预测: %s (%d/%d) 覆盖%s",
                _p(total_correct, total_pred), total_correct, total_pred, _p(total_pred, all_weeks))
    logger.info("  V3对比: %s (%d/%d) 覆盖%s",
                _p(v3_correct, v3_pred), v3_correct, v3_pred, _p(v3_pred, all_weeks))

    logger.info("")
    logger.info("  ── 按Tier ──")
    for t in sorted(by_tier.keys()):
        s = by_tier[t]
        logger.info("    Tier %d: %s (%d/%d)", t, _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 按规则 ──")
    for rn in sorted(by_rule.keys()):
        s = by_rule[rn]
        logger.info("    %-40s %s (%d/%d)", rn, _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 改进 ──")
    if v3_pred > 0 and total_pred > 0:
        v3_acc = v3_correct / v3_pred * 100
        v4_acc = total_correct / total_pred * 100
        logger.info("    准确率: V3 %.1f%% → V4 %.1f%% (%+.1f%%)", v3_acc, v4_acc, v4_acc - v3_acc)
        logger.info("    覆盖率: V3 %.1f%% → V4 %.1f%% (%+.1f%%)",
                    v3_pred/all_weeks*100, total_pred/all_weeks*100,
                    (total_pred-v3_pred)/all_weeks*100)
        logger.info("    预测数: V3 %d → V4 %d (%+d)", v3_pred, total_pred, total_pred - v3_pred)

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest(n_weeks=29, sample_limit=0)
