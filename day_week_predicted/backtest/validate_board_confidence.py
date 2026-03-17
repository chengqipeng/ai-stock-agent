#!/usr/bin/env python3
"""
验证概念板块置信度修正效果
==========================
对比V4规则 + 板块修正 vs 纯V4规则的准确率差异。

用法：
    python -m day_week_predicted.backtest.validate_board_confidence
"""
import sys, logging, random
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
    _nw_extract_features, _nw_match_rule,
    _detect_volume_patterns, _adjust_nw_confidence_by_volume,
    _adjust_nw_confidence_by_board,
)

N_WEEKS = 29
N_STOCKS = 200


def run_validation():
    t0 = datetime.now()
    logger.info("=" * 80)
    logger.info("  概念板块置信度修正效果验证 (%d只×%d周)", N_STOCKS, N_WEEKS)
    logger.info("=" * 80)

    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)

    all_codes = _get_all_stock_codes()
    random.seed(42)
    codes = sorted(random.sample(all_codes, min(N_STOCKS, len(all_codes))))
    logger.info("股票数: %d", len(codes))

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 加载个股K线
    stock_klines = defaultdict(list)
    bs = 200
    for i in range(0, len(codes), bs):
        batch = codes[i:i + bs]
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

    # 加载大盘K线
    idx_codes = list(set(_get_stock_index(c) for c in codes))
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
            'date': r['date'], 'change_percent': _to_float(r['change_percent']),
        })
    mkt_by_week = {}
    for ic, kl in mkt_kl.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    # 加载板块映射
    code_6_list = list(set(c[:6] for c in codes))
    stock_boards = defaultdict(list)
    for i in range(0, len(code_6_list), bs):
        batch = code_6_list[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_code, board_name "
            f"FROM stock_concept_board_stock WHERE stock_code IN ({ph})", batch)
        for r in cur.fetchall():
            sc6 = r['stock_code']
            suffix = '.SZ' if sc6[0] in ('0', '3') else ('.SH' if sc6[0] == '6' else '.BJ')
            full_code = sc6 + suffix
            stock_boards[full_code].append({
                'board_code': r['board_code'], 'board_name': r['board_name'],
            })

    # 加载板块K线
    all_board_codes = list(set(b['board_code'] for bl in stock_boards.values() for b in bl))
    board_klines = defaultdict(list)
    for i in range(0, len(all_board_codes), bs):
        batch = all_board_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT board_code,`date`,change_percent "
            f"FROM concept_board_kline WHERE board_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            board_klines[r['board_code']].append({
                'date': r['date'], 'change_percent': _to_float(r['change_percent']),
            })
    conn.close()
    logger.info("数据加载完成")

    board_by_week = {}
    for bc, kl in board_klines.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        board_by_week[bc] = bw

    # 回测
    # A组: 纯V4规则 (无板块修正)
    # B组: V4规则 + 板块置信度修正
    a_correct = 0
    a_total = 0
    b_correct = 0
    b_total = 0
    by_conf_a = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_conf_b = defaultdict(lambda: {'correct': 0, 'total': 0})
    board_effect = {'confirm': {'correct': 0, 'total': 0},
                    'conflict': {'correct': 0, 'total': 0},
                    'neutral': {'correct': 0, 'total': 0}}

    for code in codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue
        stock_idx = _get_stock_index(code)
        idx_bw = mkt_by_week.get(stock_idx, {})
        boards = stock_boards.get(code, [])

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
            next_chg = _compound_return([d['change_percent'] for d in next_days])
            actual_up = next_chg >= 0

            mw = idx_bw.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            # 价格位置和前周
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

            feat = _nw_extract_features(this_pcts, mkt_chg,
                                        market_index=stock_idx,
                                        price_pos_60=pos60,
                                        prev_week_chg=prev_chg)
            rule = _nw_match_rule(feat)
            if rule is None:
                continue

            pred_up = rule['pred_up']
            tier = rule['tier']
            is_correct = pred_up == actual_up

            # A组: 纯V4
            conf_a = 'high' if tier == 1 else 'reference'
            a_total += 1
            if is_correct:
                a_correct += 1
            by_conf_a[conf_a]['total'] += 1
            if is_correct:
                by_conf_a[conf_a]['correct'] += 1

            # B组: V4 + 板块修正
            conf_b = 'high' if tier == 1 else 'reference'

            # 计算板块因子
            board_momentum = None
            concept_consensus = None
            if boards:
                momentums = []
                boards_up = 0
                valid_boards = 0
                for b in boards:
                    bw = board_by_week.get(b['board_code'], {})
                    bk_this = bw.get(iw_this, [])
                    if len(bk_this) >= 3:
                        bc = _compound_return(
                            [k['change_percent'] for k in sorted(bk_this, key=lambda x: x['date'])])
                        valid_boards += 1
                        if bc > 0:
                            boards_up += 1
                        momentums.append(bc)
                if momentums:
                    board_momentum = round(_mean(momentums), 4)
                if valid_boards > 0:
                    concept_consensus = round(boards_up / valid_boards, 3)

            conf_b, board_note = _adjust_nw_confidence_by_board(
                pred_up, conf_b, board_momentum, concept_consensus)

            b_total += 1
            if is_correct:
                b_correct += 1
            by_conf_b[conf_b]['total'] += 1
            if is_correct:
                by_conf_b[conf_b]['correct'] += 1

            # 板块效果统计
            if board_note and '确认' in board_note:
                board_effect['confirm']['total'] += 1
                if is_correct:
                    board_effect['confirm']['correct'] += 1
            elif board_note and '矛盾' in board_note:
                board_effect['conflict']['total'] += 1
                if is_correct:
                    board_effect['conflict']['correct'] += 1
            else:
                board_effect['neutral']['total'] += 1
                if is_correct:
                    board_effect['neutral']['correct'] += 1

    # 输出结果
    _p = lambda c, t: f"{c / t * 100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  验证结果")
    logger.info("=" * 80)
    logger.info("  A组(纯V4):     %s (%d/%d)", _p(a_correct, a_total), a_correct, a_total)
    logger.info("  B组(V4+板块):  %s (%d/%d)", _p(b_correct, b_total), b_correct, b_total)

    logger.info("")
    logger.info("  ── A组 按置信度 ──")
    for conf in sorted(by_conf_a.keys()):
        s = by_conf_a[conf]
        logger.info("    %-12s %s (%d/%d)", conf, _p(s['correct'], s['total']),
                     s['correct'], s['total'])

    logger.info("")
    logger.info("  ── B组 按置信度 ──")
    for conf in sorted(by_conf_b.keys()):
        s = by_conf_b[conf]
        logger.info("    %-12s %s (%d/%d)", conf, _p(s['correct'], s['total']),
                     s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 板块修正效果 ──")
    for label in ['confirm', 'conflict', 'neutral']:
        s = board_effect[label]
        names = {'confirm': '板块确认', 'conflict': '板块矛盾', 'neutral': '无板块信号'}
        logger.info("    %-12s %s (%d/%d)", names[label],
                     _p(s['correct'], s['total']), s['correct'], s['total'])

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_validation()
