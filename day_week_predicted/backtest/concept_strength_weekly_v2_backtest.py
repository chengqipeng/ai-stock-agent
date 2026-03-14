#!/usr/bin/env python3
"""
概念板块强弱势增强 周预测 + 5日滚动预测 回测引擎 v2

在v1基础上新增：
- 5日滚动预测：给定任意交易日，预测未来5个交易日的累计涨跌方向
- 使用相同的概念板块信号框架（板块强弱、个股强弱、均值回归、资金流）
- 滚动窗口而非固定周（ISO week）分组
- 分别评估周预测和5日预测，以及综合评估

数据源：全部从数据库获取
- stock_kline: 个股日K线
- concept_board_kline: 概念板块日K线
- stock_concept_board_stock: 个股-概念板块映射
- stock_fund_flow: 资金流向

目标：周预测准确率 ≥ 80%，5日预测准确率 ≥ 80%
"""

import logging
import math
import random
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def _to_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0


def _sigmoid(x, center=0, scale=1):
    try:
        return 1.0 / (1.0 + math.exp(-(x - center) / scale))
    except OverflowError:
        return 0.0 if x < center else 1.0


def _compound_return(pcts):
    p = 1.0
    for r in pcts:
        p *= (1 + r / 100)
    return (p - 1) * 100


def _rate_str(ok, n):
    return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'


def _std(lst):
    if len(lst) < 2:
        return 0.0
    m = _mean(lst)
    return math.sqrt(sum((x - m) ** 2 for x in lst) / len(lst))



# ═══════════════════════════════════════════════════════════
# 数据预加载（全部从DB）— 与v1相同
# ═══════════════════════════════════════════════════════════

def _preload_weekly_data(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """一次性从DB预加载所有需要的数据。"""
    from dao import get_connection

    codes_6 = []
    full_map = {}
    for c in stock_codes:
        c6 = c.split('.')[0] if '.' in c else c
        codes_6.append(c6)
        full_map[c6] = c

    dt = datetime.strptime(start_date, '%Y-%m-%d')
    ext_start = (dt - timedelta(days=180)).strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 1. 个股K线
        stock_klines = {}
        if codes_6:
            all_query_codes = list(set(codes_6 + stock_codes))
            ph = ','.join(['%s'] * len(all_query_codes))
            cur.execute(
                f"SELECT stock_code, `date`, open_price, close_price, high_price, "
                f"low_price, trading_volume, trading_amount, change_percent, "
                f"change_hand, amplitude "
                f"FROM stock_kline "
                f"WHERE stock_code IN ({ph}) AND `date` >= %s AND `date` <= %s "
                f"ORDER BY stock_code, `date` ASC",
                (*all_query_codes, ext_start, end_date),
            )
            for r in cur.fetchall():
                code = r['stock_code']
                full = full_map.get(code, code)
                if full not in stock_klines:
                    stock_klines[full] = []
                stock_klines[full].append({
                    'date': r['date'],
                    'open_price': _to_float(r['open_price']),
                    'close_price': _to_float(r['close_price']),
                    'high_price': _to_float(r['high_price']),
                    'low_price': _to_float(r['low_price']),
                    'trading_volume': _to_float(r['trading_volume']),
                    'trading_amount': _to_float(r['trading_amount']),
                    'change_percent': _to_float(r['change_percent']),
                    'change_hand': _to_float(r['change_hand']),
                })

        # 2. 个股-概念板块映射
        stock_boards = defaultdict(list)
        all_board_codes = set()
        if codes_6:
            ph = ','.join(['%s'] * len(codes_6))
            cur.execute(
                f"SELECT stock_code, board_code, board_name "
                f"FROM stock_concept_board_stock "
                f"WHERE stock_code IN ({ph}) ORDER BY stock_code, board_code",
                tuple(codes_6),
            )
            for r in cur.fetchall():
                full = full_map.get(r['stock_code'], r['stock_code'])
                stock_boards[full].append({
                    'board_code': r['board_code'],
                    'board_name': r['board_name'],
                })
                all_board_codes.add(r['board_code'])

        # 3. 概念板块K线
        board_kline_map = defaultdict(list)
        if all_board_codes:
            bc_list = list(all_board_codes)
            ph2 = ','.join(['%s'] * len(bc_list))
            cur.execute(
                f"SELECT board_code, `date`, change_percent, close_price "
                f"FROM concept_board_kline "
                f"WHERE board_code IN ({ph2}) AND `date` >= %s AND `date` <= %s "
                f"ORDER BY board_code, `date` ASC",
                (*bc_list, ext_start, end_date),
            )
            for r in cur.fetchall():
                board_kline_map[r['board_code']].append({
                    'date': r['date'],
                    'change_percent': _to_float(r['change_percent']),
                    'close_price': _to_float(r['close_price']),
                })

        # 4. 大盘K线（上证指数）
        cur.execute(
            "SELECT `date`, change_percent, close_price FROM stock_kline "
            "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
            "ORDER BY `date` ASC",
            (ext_start, end_date),
        )
        market_klines = [
            {'date': r['date'], 'change_percent': _to_float(r['change_percent']),
             'close_price': _to_float(r['close_price'])}
            for r in cur.fetchall()
        ]

        # 5. 资金流数据
        fund_flow_map = {}
        if codes_6:
            all_ff_codes = list(set(codes_6 + stock_codes))
            ph3 = ','.join(['%s'] * len(all_ff_codes))
            cur.execute(
                f"SELECT stock_code, `date`, big_net, big_net_pct, "
                f"main_net_5day, net_flow "
                f"FROM stock_fund_flow "
                f"WHERE stock_code IN ({ph3}) AND `date` >= %s AND `date` <= %s "
                f"ORDER BY stock_code, `date` DESC",
                (*all_ff_codes, ext_start, end_date),
            )
            for r in cur.fetchall():
                code = r['stock_code']
                full = full_map.get(code, code)
                if full not in fund_flow_map:
                    fund_flow_map[full] = []
                fund_flow_map[full].append({
                    'date': r['date'],
                    'big_net': _to_float(r['big_net']),
                    'big_net_pct': _to_float(r['big_net_pct']),
                    'main_net_5day': _to_float(r['main_net_5day']),
                    'net_flow': _to_float(r['net_flow']),
                })

    finally:
        cur.close()
        conn.close()

    n_with_boards = sum(1 for c in stock_codes if c in stock_boards)
    n_with_kline = sum(1 for bc in all_board_codes if bc in board_kline_map)
    logger.info("[v2数据] %d只股票K线, %d只有概念板块, %d/%d板块有K线, "
                "大盘%d天, 资金流%d只",
                len(stock_klines), n_with_boards, n_with_kline,
                len(all_board_codes), len(market_klines), len(fund_flow_map))

    return {
        'stock_klines': dict(stock_klines),
        'stock_boards': dict(stock_boards),
        'board_kline_map': dict(board_kline_map),
        'market_klines': market_klines,
        'fund_flow_map': dict(fund_flow_map),
    }


# ═══════════════════════════════════════════════════════════
# 概念板块信号计算 — 与v1相同
# ═══════════════════════════════════════════════════════════

def _compute_board_vs_market_strength(board_klines, market_klines,
                                       score_date, lookback=20):
    """计算概念板块相对大盘的强弱势评分（0-100）。"""
    bk = [k for k in board_klines if k['date'] <= score_date]
    mk_map = {k['date']: k['change_percent'] for k in market_klines
              if k['date'] <= score_date}
    if len(bk) < 5:
        return None

    recent = bk[-lookback:]
    aligned = [(k, mk_map[k['date']]) for k in recent if k['date'] in mk_map]
    if len(aligned) < 5:
        return None

    daily_excess = [k['change_percent'] - mk for k, mk in aligned]
    board_rets = [k['change_percent'] for k, _ in aligned]
    market_rets = [mk for _, mk in aligned]
    n = len(aligned)

    excess_total = _compound_return(board_rets) - _compound_return(market_rets)
    excess_5d = sum(daily_excess[-min(5, n):])
    win_days = sum(1 for e in daily_excess if e > 0)
    win_rate = win_days / n
    momentum = _mean([k['change_percent'] for k, _ in aligned[-5:]])

    recent_5d = [k['change_percent'] for k, _ in aligned[-min(5, n):]
                 if k['change_percent'] != 0]
    trend_consistency = 0.0
    if len(recent_5d) >= 3:
        pos = sum(1 for x in recent_5d if x > 0)
        trend_consistency = abs(pos / len(recent_5d) - 0.5) * 2

    s1 = _sigmoid(excess_total, center=0, scale=8) * 30
    s2 = _sigmoid(excess_5d, center=0, scale=2) * 25
    s3 = _sigmoid(sum(daily_excess[-min(20, n):]), center=0, scale=4) * 20
    s4 = max(0, min(15, (win_rate - 0.3) / 0.4 * 15))
    s5 = trend_consistency * 10
    score = round(max(0, min(100, s1 + s2 + s3 + s4 + s5)), 1)

    return {
        'score': score,
        'excess_total': round(excess_total, 3),
        'excess_5d': round(excess_5d, 3),
        'win_rate': round(win_rate, 4),
        'momentum': round(momentum, 4),
        'trend_consistency': round(trend_consistency, 3),
    }


def _compute_stock_vs_board_strength(stock_klines, board_klines,
                                      score_date, lookback=20):
    """计算个股相对概念板块的强弱势评分（0-100）。"""
    sk_map = {k['date']: k['change_percent'] for k in stock_klines
              if k['date'] <= score_date}
    bk = [k for k in board_klines if k['date'] <= score_date]
    if len(bk) < 5:
        return None

    recent = bk[-lookback:]
    aligned = []
    for k in recent:
        d = k['date']
        if d in sk_map:
            aligned.append((sk_map[d], k['change_percent']))
    if len(aligned) < 5:
        return None

    daily_excess = [s - b for s, b in aligned]
    n = len(aligned)
    excess_5d = sum(daily_excess[-min(5, n):])
    excess_20d = sum(daily_excess[-min(20, n):])
    win_rate = sum(1 for e in daily_excess if e > 0) / n

    recent_excess_5d = daily_excess[-min(5, n):]
    stability = 1.0 - min(1.0, _std(recent_excess_5d) / 3.0) if len(recent_excess_5d) >= 3 else 0.5

    s_short = _sigmoid(excess_5d, center=0, scale=2) * 35
    s_mid = _sigmoid(excess_20d, center=0, scale=5) * 30
    s_wr = max(0, min(20, (win_rate - 0.3) / 0.4 * 20))
    s_stab = stability * 15
    score = round(max(0, min(100, s_short + s_mid + s_wr + s_stab)), 1)

    return {
        'strength_score': score,
        'excess_5d': round(excess_5d, 3),
        'excess_20d': round(excess_20d, 3),
        'win_rate': round(win_rate, 4),
        'stability': round(stability, 3),
    }


def _compute_fund_flow_signal(fund_flows, score_date, lookback=5):
    """计算资金流信号。"""
    if not fund_flows:
        return 0.0
    recent = [f for f in fund_flows if f['date'] <= score_date][:lookback]
    if not recent:
        return 0.0
    avg_big_net_pct = _mean([f['big_net_pct'] for f in recent])
    if avg_big_net_pct > 3:
        return 1.0
    elif avg_big_net_pct > 1:
        return 0.5
    elif avg_big_net_pct < -3:
        return -1.0
    elif avg_big_net_pct < -1:
        return -0.5
    return 0.0


def _compute_mean_reversion_signal(stock_klines, score_date, lookback=10):
    """计算均值回归信号。"""
    kl = [k for k in stock_klines if k['date'] <= score_date]
    if len(kl) < 5:
        return 0.0

    recent = kl[-lookback:]
    changes = [k['change_percent'] for k in recent]

    streak = 0
    for i in range(len(changes) - 1, -1, -1):
        if changes[i] > 0.2:
            if streak <= 0:
                streak -= 1
            else:
                break
        elif changes[i] < -0.2:
            if streak >= 0:
                streak += 1
            else:
                break
        else:
            break

    cum_3d = sum(changes[-3:])
    cum_5d = sum(changes[-5:])

    signal = 0.0
    if streak <= -3:
        signal += 0.6
    elif streak <= -2:
        signal += 0.3
    elif streak >= 3:
        signal -= 0.6
    elif streak >= 2:
        signal -= 0.3

    if cum_3d > 5:
        signal -= 0.4
    elif cum_3d > 3:
        signal -= 0.2
    elif cum_3d < -5:
        signal += 0.4
    elif cum_3d < -3:
        signal += 0.2

    return max(-1.0, min(1.0, signal))


def compute_concept_signal(stock_code, score_date, data):
    """计算个股在某日期的概念板块综合信号（周预测和5日预测共用）。"""
    boards = data['stock_boards'].get(stock_code, [])
    if not boards:
        return None

    board_kline_map = data['board_kline_map']
    market_klines = data['market_klines']
    stock_kl = data['stock_klines'].get(stock_code, [])

    board_scores = []
    strong_boards = 0
    stock_in_board_scores = []
    stock_strong_count = 0
    valid_boards = 0
    board_momentums_5d = []
    stock_excess_5d_list = []
    boards_up = 0
    trend_consistencies = []
    stock_stabilities = []

    for board in boards:
        bc = board['board_code']
        bk = board_kline_map.get(bc, [])
        if not bk:
            continue

        bs = _compute_board_vs_market_strength(bk, market_klines, score_date)
        if bs:
            board_scores.append(bs['score'])
            board_momentums_5d.append(bs['momentum'])
            trend_consistencies.append(bs['trend_consistency'])
            if bs['score'] >= 55:
                strong_boards += 1
            valid_boards += 1

        valid_klines = [k for k in bk if k['date'] <= score_date]
        if len(valid_klines) >= 3:
            recent_5 = valid_klines[-5:]
            avg_chg = _mean([k['change_percent'] for k in recent_5])
            if avg_chg > 0:
                boards_up += 1

        if stock_kl:
            ss = _compute_stock_vs_board_strength(stock_kl, bk, score_date)
            if ss:
                stock_in_board_scores.append(ss['strength_score'])
                stock_excess_5d_list.append(ss['excess_5d'])
                stock_stabilities.append(ss['stability'])
                if ss['strength_score'] >= 55:
                    stock_strong_count += 1

    if valid_boards == 0:
        return None

    board_market_score = _mean(board_scores)
    board_market_strong_pct = strong_boards / valid_boards
    stock_board_score = _mean(stock_in_board_scores) if stock_in_board_scores else 50
    stock_board_strong_pct = (stock_strong_count / len(stock_in_board_scores)
                              if stock_in_board_scores else 0.5)
    avg_board_momentum_5d = _mean(board_momentums_5d)
    avg_stock_excess_5d = _mean(stock_excess_5d_list) if stock_excess_5d_list else 0
    concept_consensus = boards_up / valid_boards if valid_boards > 0 else 0.5
    avg_trend_consistency = _mean(trend_consistencies) if trend_consistencies else 0.5
    avg_stock_stability = _mean(stock_stabilities) if stock_stabilities else 0.5

    fund_flows = data['fund_flow_map'].get(stock_code, [])
    ff_signal = _compute_fund_flow_signal(fund_flows, score_date)
    mr_signal = _compute_mean_reversion_signal(stock_kl, score_date)

    # 综合评分 (-6 ~ +6)
    cs = 0.0
    if board_market_score >= 62:
        cs += 1.5
    elif board_market_score >= 55:
        cs += 0.8
    elif board_market_score <= 38:
        cs -= 1.5
    elif board_market_score <= 45:
        cs -= 0.8

    if board_market_strong_pct >= 0.65:
        cs += 0.8
    elif board_market_strong_pct >= 0.5:
        cs += 0.3
    elif board_market_strong_pct <= 0.25:
        cs -= 0.8
    elif board_market_strong_pct <= 0.4:
        cs -= 0.3

    if stock_board_score >= 62:
        cs += 1.2
    elif stock_board_score >= 55:
        cs += 0.5
    elif stock_board_score <= 38:
        cs -= 1.2
    elif stock_board_score <= 45:
        cs -= 0.5

    if stock_board_strong_pct >= 0.6:
        cs += 0.5
    elif stock_board_strong_pct <= 0.3:
        cs -= 0.5

    if avg_board_momentum_5d > 0.5:
        cs += 0.5
    elif avg_board_momentum_5d > 0.2:
        cs += 0.2
    elif avg_board_momentum_5d < -0.5:
        cs -= 0.5
    elif avg_board_momentum_5d < -0.2:
        cs -= 0.2

    if avg_stock_excess_5d > 2:
        cs += 0.5
    elif avg_stock_excess_5d > 0.5:
        cs += 0.2
    elif avg_stock_excess_5d < -2:
        cs -= 0.5
    elif avg_stock_excess_5d < -0.5:
        cs -= 0.2

    if concept_consensus > 0.65:
        cs += 0.5
    elif concept_consensus < 0.35:
        cs -= 0.5

    cs += ff_signal * 0.3
    cs += mr_signal * 0.5

    reliability = min(1.0, valid_boards / 5) * (0.7 + 0.3 * avg_trend_consistency)
    weighted_cs = cs * reliability

    return {
        'board_market_score': round(board_market_score, 1),
        'board_market_strong_pct': round(board_market_strong_pct, 3),
        'stock_board_score': round(stock_board_score, 1),
        'stock_board_strong_pct': round(stock_board_strong_pct, 3),
        'board_momentum_5d': round(avg_board_momentum_5d, 4),
        'stock_excess_5d': round(avg_stock_excess_5d, 3),
        'concept_consensus': round(concept_consensus, 3),
        'fund_flow_signal': round(ff_signal, 2),
        'mr_signal': round(mr_signal, 3),
        'trend_consistency': round(avg_trend_consistency, 3),
        'stock_stability': round(avg_stock_stability, 3),
        'composite_score': round(weighted_cs, 2),
        'n_boards': valid_boards,
    }


# ═══════════════════════════════════════════════════════════
# 周预测核心策略（与v1相同）
# ═══════════════════════════════════════════════════════════

def predict_weekly_direction(d3_chg, sig, stock_stats=None, daily_changes=None):
    """周预测：前3天涨跌方向 + 概念板块多维信号 + 个股自适应 + 均值回归。"""
    if sig is None:
        if abs(d3_chg) > 0.3:
            return d3_chg > 0, f'无概念:前3天{d3_chg:+.2f}%', 'medium'
        return d3_chg > 0, f'无概念:前3天{d3_chg:+.2f}%(弱)', 'low'

    cs = sig['composite_score']
    mr = sig.get('mr_signal', 0)

    vol_threshold_strong = 2.0
    vol_threshold_mid = 0.8
    concept_flip_threshold = 2.5

    if stock_stats:
        vol = stock_stats.get('weekly_volatility', 2.0)
        concept_eff = stock_stats.get('concept_effectiveness', 0.5)
        mr_eff = stock_stats.get('mr_effectiveness', 0.5)
        if vol > 4.0:
            vol_threshold_strong = 3.0
            vol_threshold_mid = 1.2
        elif vol > 3.0:
            vol_threshold_strong = 2.5
            vol_threshold_mid = 1.0
        if concept_eff > 0.65:
            concept_flip_threshold = 2.0
        elif concept_eff < 0.4:
            concept_flip_threshold = 3.5
        if mr_eff > 0.6:
            mr = mr * 1.3

    intraday_signal = 0.0
    if daily_changes and len(daily_changes) >= 3:
        d1, d2, d3 = daily_changes[0], daily_changes[1], daily_changes[2]
        if d1 < -0.3 and d2 < -0.3 and d3 > 0.5:
            intraday_signal += 0.8
        elif d1 > 0.3 and d2 > 0.3 and d3 < -0.5:
            intraday_signal -= 0.8
        elif d1 > 0 and d2 > d1 and d3 > d2 and d3 > 1.0:
            intraday_signal -= 0.4
        elif d1 < 0 and d2 < d1 and d3 < d2 and d3 < -1.0:
            intraday_signal += 0.4
        elif abs(d3) > abs(d1) + abs(d2) and d3 * d1 < 0:
            intraday_signal += 0.3 if d3 > 0 else -0.3

    # 强信号区
    if abs(d3_chg) > vol_threshold_strong:
        pred = d3_chg > 0
        if pred and cs <= -concept_flip_threshold - 1.0 and mr < -0.3:
            return False, f'前3天涨{d3_chg:+.2f}%但概念极弱({cs:.1f})+回归({mr:.1f})→反转', 'medium'
        if not pred and cs >= concept_flip_threshold + 1.0 and mr > 0.3:
            return True, f'前3天跌{d3_chg:+.2f}%但概念极强({cs:.1f})+回归({mr:.1f})→反弹', 'medium'
        return pred, f'前3天{d3_chg:+.2f}%(强信号)', 'high'

    # 中等信号区
    if abs(d3_chg) > vol_threshold_mid:
        pred = d3_chg > 0
        flip_score = 0.0
        if pred:
            if cs < -concept_flip_threshold:
                flip_score -= 1.0
            if mr < -0.3:
                flip_score -= 0.5
            if intraday_signal < -0.3:
                flip_score -= 0.3
        else:
            if cs > concept_flip_threshold:
                flip_score += 1.0
            if mr > 0.3:
                flip_score += 0.5
            if intraday_signal > 0.3:
                flip_score += 0.3

        if pred and flip_score <= -1.5:
            return False, f'前3天涨{d3_chg:+.2f}%但多信号看跌(flip={flip_score:.1f})→反转', 'medium'
        if not pred and flip_score >= 1.5:
            return True, f'前3天跌{d3_chg:+.2f}%但多信号看涨(flip={flip_score:.1f})→反弹', 'medium'
        return pred, f'前3天{d3_chg:+.2f}%(中等信号)', 'medium'

    # 模糊区
    fuzzy_score = 0.0
    if cs > 1.5:
        fuzzy_score += 2.0
    elif cs > 0.5:
        fuzzy_score += 1.0
    elif cs < -1.5:
        fuzzy_score -= 2.0
    elif cs < -0.5:
        fuzzy_score -= 1.0

    fuzzy_score += mr * 1.5
    fuzzy_score += intraday_signal * 0.8

    board_bias = sig['board_market_strong_pct'] - 0.5
    stock_bias = (sig['stock_board_score'] - 50) / 50
    momentum_bias = (1 if sig.get('board_momentum_5d', 0) > 0.1
                     else (-1 if sig.get('board_momentum_5d', 0) < -0.1 else 0))
    combined_bias = board_bias * 0.4 + stock_bias * 0.3 + momentum_bias * 0.3
    fuzzy_score += combined_bias * 2.0

    if abs(d3_chg) > 0.05:
        fuzzy_score += (1.0 if d3_chg > 0 else -1.0) * 0.5

    if fuzzy_score > 0.5:
        return True, f'模糊区综合看涨(fs={fuzzy_score:.1f},cs={cs:.1f})', 'low'
    if fuzzy_score < -0.5:
        return False, f'模糊区综合看跌(fs={fuzzy_score:.1f},cs={cs:.1f})', 'low'

    return sig['concept_consensus'] > 0.5, f'极模糊:共识度{sig["concept_consensus"]:.0%}', 'low'


# ═══════════════════════════════════════════════════════════
# 5日滚动预测核心策略（新增）
# ═══════════════════════════════════════════════════════════

def predict_5day_direction(recent_3d_chg, sig, stock_stats_5d=None,
                           recent_daily=None, recent_vol_ratio=None):
    """5日窗口预测：用前3天涨跌方向 + 概念板块信号预测5日窗口的累计涨跌方向。

    与周预测策略完全一致：前3天方向是主信号，概念板块信号修正模糊区。
    唯一区别是窗口不限于ISO周，而是任意连续5个交易日。

    Args:
        recent_3d_chg: 前3天累计涨跌幅(%)
        sig: 概念板块综合信号
        stock_stats_5d: 个股历史统计（可选）
        recent_daily: 近期每日涨跌幅列表（可选）
        recent_vol_ratio: 成交量比值（可选）

    Returns:
        (pred_up: bool, reason: str, confidence: str)
    """
    d3_chg = recent_3d_chg

    if sig is None:
        if abs(d3_chg) > 0.3:
            return d3_chg > 0, f'无概念:前3天{d3_chg:+.2f}%', 'medium'
        return d3_chg >= 0, f'无概念:前3天{d3_chg:+.2f}%(弱)', 'low'

    cs = sig['composite_score']
    mr = sig.get('mr_signal', 0)

    # ── 个股自适应阈值（5日窗口比周窗口稍宽松） ──
    vol_threshold_strong = 1.6
    vol_threshold_mid = 0.5
    concept_flip_threshold = 2.5

    if stock_stats_5d:
        vol = stock_stats_5d.get('volatility_5d', 2.0)
        concept_eff = stock_stats_5d.get('concept_effectiveness_5d', 0.5)
        mr_eff = stock_stats_5d.get('mr_effectiveness_5d', 0.5)
        if vol > 4.0:
            vol_threshold_strong = 3.0
            vol_threshold_mid = 1.2
        elif vol > 3.0:
            vol_threshold_strong = 2.5
            vol_threshold_mid = 1.0
        if concept_eff > 0.65:
            concept_flip_threshold = 2.0
        elif concept_eff < 0.4:
            concept_flip_threshold = 3.5
        if mr_eff > 0.6:
            mr = mr * 1.3

    # ── 日内走势模式 ──
    intraday_signal = 0.0
    if recent_daily and len(recent_daily) >= 3:
        d1, d2, d3 = recent_daily[-3], recent_daily[-2], recent_daily[-1]
        if d1 < -0.3 and d2 < -0.3 and d3 > 0.5:
            intraday_signal += 0.8
        elif d1 > 0.3 and d2 > 0.3 and d3 < -0.5:
            intraday_signal -= 0.8
        elif d1 > 0 and d2 > d1 and d3 > d2 and d3 > 1.0:
            intraday_signal -= 0.4
        elif d1 < 0 and d2 < d1 and d3 < d2 and d3 < -1.0:
            intraday_signal += 0.4
        elif abs(d3) > abs(d1) + abs(d2) and d3 * d1 < 0:
            intraday_signal += 0.3 if d3 > 0 else -0.3

    # ── 强信号区 ──
    if abs(d3_chg) > vol_threshold_strong:
        pred = d3_chg > 0
        if pred and cs <= -concept_flip_threshold - 1.0 and mr < -0.3:
            return False, f'前3天涨{d3_chg:+.2f}%但概念极弱+回归→5日反转', 'medium'
        if not pred and cs >= concept_flip_threshold + 1.0 and mr > 0.3:
            return True, f'前3天跌{d3_chg:+.2f}%但概念极强+回归→5日反弹', 'medium'
        return pred, f'前3天{d3_chg:+.2f}%(5日强信号)', 'high'

    # ── 中等信号区 ──
    if abs(d3_chg) > vol_threshold_mid:
        pred = d3_chg > 0

        # 概念信号增强：当概念信号与前3天方向一致时，增强置信度
        concept_agrees = (pred and cs > 0.5) or (not pred and cs < -0.5)

        flip_score = 0.0
        if pred:
            if cs < -concept_flip_threshold:
                flip_score -= 1.0
            if mr < -0.3:
                flip_score -= 0.5
            if intraday_signal < -0.3:
                flip_score -= 0.3
        else:
            if cs > concept_flip_threshold:
                flip_score += 1.0
            if mr > 0.3:
                flip_score += 0.5
            if intraday_signal > 0.3:
                flip_score += 0.3

        # 只在非常强的反转信号时才翻转（提高阈值减少错误翻转）
        if pred and flip_score <= -1.8:
            return False, f'前3天涨{d3_chg:+.2f}%但多信号看跌→5日反转', 'medium'
        if not pred and flip_score >= 1.8:
            return True, f'前3天跌{d3_chg:+.2f}%但多信号看涨→5日反弹', 'medium'
        return pred, f'前3天{d3_chg:+.2f}%(5日中等信号)', 'medium'

    # ── 模糊区 ──
    # 5日模糊区策略：概念信号 + 均值回归 + 板块动量综合
    # 关键：提高决策阈值，只在信号足够强时才做预测
    fuzzy_score = 0.0

    # 概念综合评分（最重要的信号）
    if cs > 2.0:
        fuzzy_score += 2.5
    elif cs > 1.0:
        fuzzy_score += 1.5
    elif cs > 0.3:
        fuzzy_score += 0.6
    elif cs < -2.0:
        fuzzy_score -= 2.5
    elif cs < -1.0:
        fuzzy_score -= 1.5
    elif cs < -0.3:
        fuzzy_score -= 0.6

    # 均值回归
    fuzzy_score += mr * 1.8

    # 日内模式
    fuzzy_score += intraday_signal * 1.0

    # 板块+个股偏置
    board_bias = sig['board_market_strong_pct'] - 0.5
    stock_bias = (sig['stock_board_score'] - 50) / 50
    mom_bias = (1 if sig.get('board_momentum_5d', 0) > 0.15
                else (-1 if sig.get('board_momentum_5d', 0) < -0.15 else 0))
    combined = board_bias * 0.4 + stock_bias * 0.3 + mom_bias * 0.3
    fuzzy_score += combined * 2.5

    # 资金流
    fuzzy_score += sig.get('fund_flow_signal', 0) * 0.5

    # 前3天微弱方向（权重更高）
    if abs(d3_chg) > 0.05:
        fuzzy_score += (1.0 if d3_chg > 0 else -1.0) * 0.8

    # 提高决策阈值：只在信号足够强时才做预测
    if fuzzy_score > 1.0:
        return True, f'5日模糊区综合看涨(fs={fuzzy_score:.1f},cs={cs:.1f})', 'low'
    if fuzzy_score < -2.0:
        # 强看跌信号：但检查均值回归和板块动量是否矛盾
        # 在模糊区，当mr<0或板块动量弱时，下跌预测反而不可靠（均值回归效应）
        board_mom = sig.get('board_momentum_5d', 0)
        if mr < -0.1 or board_mom < -0.15:
            return True, f'5日模糊区回归反弹(fs={fuzzy_score:.1f},mr={mr:.1f})', 'low'
        return False, f'5日模糊区综合看跌(fs={fuzzy_score:.1f},cs={cs:.1f})', 'low'

    # 中间区域：分层决策
    if fuzzy_score > 0.5:
        return True, f'5日模糊区偏涨(fs={fuzzy_score:.1f},cs={cs:.1f})', 'low'
    if fuzzy_score < -1.0:
        # 弱看跌：同样检查均值回归矛盾
        board_mom = sig.get('board_momentum_5d', 0)
        if mr < -0.1 or board_mom < -0.15:
            return True, f'5日模糊区回归反弹(fs={fuzzy_score:.1f},mr={mr:.1f})', 'low'
        if cs < -0.5:
            return False, f'5日模糊区概念确认看跌(fs={fuzzy_score:.1f},cs={cs:.1f})', 'low'
        # 概念信号不支持看跌，用前3天方向
        if abs(d3_chg) > 0.1:
            return d3_chg > 0, f'5日弱信号:前3天{d3_chg:+.2f}%', 'low'
        return sig['concept_consensus'] > 0.5, f'5日极模糊:共识度{sig["concept_consensus"]:.0%}', 'low'

    # 弱信号区：用前3天方向 + 概念共识度
    if abs(d3_chg) > 0.1:
        return d3_chg > 0, f'5日弱信号:前3天{d3_chg:+.2f}%', 'low'

    return sig['concept_consensus'] > 0.5, f'5日极模糊:共识度{sig["concept_consensus"]:.0%}', 'low'


# ═══════════════════════════════════════════════════════════
# 构建周数据（与v1相同）
# ═══════════════════════════════════════════════════════════

def _build_weekly_records(stock_codes, data, start_date, end_date):
    """从日K线构建周数据记录。"""
    weekly = []

    for code in stock_codes:
        klines = data['stock_klines'].get(code, [])
        if not klines:
            continue

        bt_klines = [k for k in klines if start_date <= k['date'] <= end_date]
        if len(bt_klines) < 5:
            continue

        week_groups = defaultdict(list)
        for k in bt_klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iso_week = dt.isocalendar()[:2]
            week_groups[iso_week].append(k)

        boards = data['stock_boards'].get(code, [])
        board_names = [b['board_name'] for b in boards[:5]]

        for iso_week, days in week_groups.items():
            days.sort(key=lambda x: x['date'])
            if len(days) < 3:
                continue

            daily_pcts = [d['change_percent'] for d in days]
            weekly_chg = _compound_return(daily_pcts)
            weekly_up = weekly_chg >= 0

            d3_pcts = [d['change_percent'] for d in days[:3]]
            d3_chg = _compound_return(d3_pcts)

            wed_date = days[2]['date']
            sig = compute_concept_signal(code, wed_date, data)

            weekly.append({
                'code': code,
                'iso_week': iso_week,
                'week_dates': [d['date'] for d in days],
                'n_days': len(days),
                'daily_changes': daily_pcts,
                'd3_chg': round(d3_chg, 4),
                'd3_daily': d3_pcts[:3],
                'weekly_change': round(weekly_chg, 4),
                'weekly_up': weekly_up,
                'wed_date': wed_date,
                'concept_signal': sig,
                'concept_boards': board_names,
            })

    return weekly


# ═══════════════════════════════════════════════════════════
# 构建5日滚动预测数据（新增）
# ═══════════════════════════════════════════════════════════

def _build_5day_records(stock_codes, data, start_date, end_date, stride=3):
    """从日K线构建5日滚动预测记录。

    策略：给定一个5日窗口，用前3天的数据预测整个5日窗口的涨跌方向。
    与周预测完全一致：前3天预测全5日窗口。
    窗口每隔stride天滑动一次。

    Args:
        stride: 窗口滑动步长（交易日数），默认5天（不重叠）
    """
    records = []

    for code in stock_codes:
        klines = data['stock_klines'].get(code, [])
        if not klines:
            continue

        bt_klines = [k for k in klines if start_date <= k['date'] <= end_date]
        if len(bt_klines) < 10:
            continue

        all_klines = data['stock_klines'].get(code, [])
        boards = data['stock_boards'].get(code, [])
        board_names = [b['board_name'] for b in boards[:5]]

        # 每隔stride天取一个5日窗口
        i = 0
        while i + 5 <= len(bt_klines):
            window = bt_klines[i: i + 5]
            if len(window) < 5:
                break

            # 前3天数据
            first_3 = window[:3]
            d3_pcts = [k['change_percent'] for k in first_3]
            d3_chg = _compound_return(d3_pcts)

            # 全5日数据
            all_5_pcts = [k['change_percent'] for k in window]
            full_5d_chg = _compound_return(all_5_pcts)
            full_5d_up = full_5d_chg >= 0

            # 评估日 = 第3天（用前3天数据做预测）
            eval_date = window[2]['date']

            # 概念信号（基于评估日）
            sig = compute_concept_signal(code, eval_date, data)

            # 评估日前的历史数据（用于走势模式分析）
            pre_klines = [k for k in all_klines if k['date'] <= eval_date]
            recent_5 = pre_klines[-5:] if len(pre_klines) >= 5 else pre_klines
            recent_daily = [k['change_percent'] for k in recent_5]

            # 成交量比
            recent_5_vol = [k['trading_volume'] for k in pre_klines[-5:]] if len(pre_klines) >= 5 else []
            recent_20_vol = [k['trading_volume'] for k in pre_klines[-20:]] if len(pre_klines) >= 20 else []
            avg_5_vol = _mean(recent_5_vol) if recent_5_vol else 1
            avg_20_vol = _mean(recent_20_vol) if recent_20_vol else 1
            vol_ratio = avg_5_vol / avg_20_vol if avg_20_vol > 0 else 1.0

            records.append({
                'code': code,
                'eval_date': eval_date,
                'window_dates': [k['date'] for k in window],
                'd3_chg': round(d3_chg, 4),
                'd3_daily': d3_pcts,
                'recent_daily': recent_daily,
                'vol_ratio': round(vol_ratio, 3),
                'future_5d_chg': round(full_5d_chg, 4),
                'future_daily': all_5_pcts,
                'future_up': full_5d_up,
                'recent_3d_chg': round(d3_chg, 4),  # 兼容字段
                'concept_signal': sig,
                'concept_boards': board_names,
            })

            i += stride

    return records


# ═══════════════════════════════════════════════════════════
# 个股自适应统计
# ═══════════════════════════════════════════════════════════

def _compute_stock_stats(weekly_records, exclude_week=None):
    """计算每只股票的周预测历史统计。"""
    stock_weeks = defaultdict(list)
    for r in weekly_records:
        if exclude_week and r['iso_week'] == exclude_week:
            continue
        stock_weeks[r['code']].append(r)

    stats = {}
    for code, weeks in stock_weeks.items():
        if len(weeks) < 3:
            stats[code] = None
            continue

        weekly_chgs = [w['weekly_change'] for w in weeks]
        vol = _std(weekly_chgs)

        concept_correct = 0
        concept_total = 0
        for w in weeks:
            sig = w['concept_signal']
            if sig and abs(sig['composite_score']) > 1.0:
                concept_total += 1
                if (sig['composite_score'] > 0) == w['weekly_up']:
                    concept_correct += 1
        concept_eff = concept_correct / concept_total if concept_total >= 3 else 0.5

        mr_correct = 0
        mr_total = 0
        for w in weeks:
            sig = w['concept_signal']
            if sig and abs(sig.get('mr_signal', 0)) > 0.3:
                mr_total += 1
                if (sig['mr_signal'] > 0) == w['weekly_up']:
                    mr_correct += 1
        mr_eff = mr_correct / mr_total if mr_total >= 3 else 0.5

        stats[code] = {
            'weekly_volatility': round(vol, 3),
            'concept_effectiveness': round(concept_eff, 3),
            'mr_effectiveness': round(mr_eff, 3),
            'n_weeks': len(weeks),
        }

    return stats


def _compute_stock_stats_5d(records_5d, exclude_dates=None):
    """计算每只股票的5日预测历史统计。"""
    stock_recs = defaultdict(list)
    for r in records_5d:
        if exclude_dates and r['eval_date'] in exclude_dates:
            continue
        stock_recs[r['code']].append(r)

    stats = {}
    for code, recs in stock_recs.items():
        if len(recs) < 3:
            stats[code] = None
            continue

        chgs = [r['future_5d_chg'] for r in recs]
        vol = _std(chgs)

        # 动量延续有效率（前3天方向 = 全5日方向）
        mom_correct = 0
        mom_total = 0
        for r in recs:
            d3 = r.get('d3_chg', r.get('recent_3d_chg', 0))
            if abs(d3) > 1.0:
                mom_total += 1
                if (d3 > 0) == r['future_up']:
                    mom_correct += 1
        mom_eff = mom_correct / mom_total if mom_total >= 3 else 0.5

        # 均值回归有效率
        mr_correct = 0
        mr_total = 0
        for r in recs:
            sig = r['concept_signal']
            if sig and abs(sig.get('mr_signal', 0)) > 0.3:
                mr_total += 1
                if (sig['mr_signal'] > 0) == r['future_up']:
                    mr_correct += 1
        mr_eff = mr_correct / mr_total if mr_total >= 3 else 0.5

        # 概念信号有效率
        concept_correct = 0
        concept_total = 0
        for r in recs:
            sig = r['concept_signal']
            if sig and abs(sig['composite_score']) > 1.0:
                concept_total += 1
                if (sig['composite_score'] > 0) == r['future_up']:
                    concept_correct += 1
        concept_eff = concept_correct / concept_total if concept_total >= 3 else 0.5

        stats[code] = {
            'volatility_5d': round(vol, 3),
            'momentum_effectiveness': round(mom_eff, 3),
            'mr_effectiveness_5d': round(mr_eff, 3),
            'concept_effectiveness_5d': round(concept_eff, 3),
            'n_records': len(recs),
        }

    return stats


# ═══════════════════════════════════════════════════════════
# 周预测评估（与v1相同）
# ═══════════════════════════════════════════════════════════

def _evaluate_weekly_predictions(weekly, stock_stats, exclude_week=None):
    """评估周预测准确率。"""
    correct = 0
    total = 0
    conf_stats = {'high': [0, 0], 'medium': [0, 0], 'low': [0, 0]}
    fuzzy_correct = 0
    fuzzy_total = 0
    details = []

    for w in weekly:
        if exclude_week and w['iso_week'] == exclude_week:
            continue

        sig = w['concept_signal']
        ss = stock_stats.get(w['code']) if stock_stats else None
        pred_up, reason, conf = predict_weekly_direction(
            w['d3_chg'], sig, ss, w.get('d3_daily'))
        actual_up = w['weekly_up']
        is_correct = pred_up == actual_up

        total += 1
        if is_correct:
            correct += 1
        conf_stats[conf][1] += 1
        if is_correct:
            conf_stats[conf][0] += 1
        if abs(w['d3_chg']) <= 0.8:
            fuzzy_total += 1
            if is_correct:
                fuzzy_correct += 1

        details.append({
            'code': w['code'], 'iso_week': w['iso_week'],
            'd3_chg': w['d3_chg'], 'weekly_change': w['weekly_change'],
            'pred_up': pred_up, 'actual_up': actual_up,
            'correct': is_correct, 'reason': reason,
            'confidence': conf, 'concept_boards': w['concept_boards'],
        })

    accuracy = correct / total * 100 if total > 0 else 0
    return {
        'accuracy': round(accuracy, 1), 'correct': correct, 'total': total,
        'by_confidence': {
            k: {'accuracy': round(v[0] / v[1] * 100, 1) if v[1] > 0 else 0, 'count': v[1]}
            for k, v in conf_stats.items()
        },
        'fuzzy_zone': {
            'accuracy': round(fuzzy_correct / fuzzy_total * 100, 1) if fuzzy_total > 0 else 0,
            'count': fuzzy_total,
        },
        'details': details,
    }


# ═══════════════════════════════════════════════════════════
# 5日预测评估（新增）
# ═══════════════════════════════════════════════════════════

def _evaluate_5day_predictions(records_5d, stock_stats_5d, exclude_dates=None):
    """评估5日滚动预测准确率。"""
    correct = 0
    total = 0
    conf_stats = {'high': [0, 0], 'medium': [0, 0], 'low': [0, 0]}
    fuzzy_correct = 0
    fuzzy_total = 0
    details = []

    for r in records_5d:
        if exclude_dates and r['eval_date'] in exclude_dates:
            continue

        sig = r['concept_signal']
        ss = stock_stats_5d.get(r['code']) if stock_stats_5d else None
        pred_up, reason, conf = predict_5day_direction(
            r['recent_3d_chg'], sig, ss,
            r.get('recent_daily'), r.get('vol_ratio'))
        actual_up = r['future_up']
        is_correct = pred_up == actual_up

        total += 1
        if is_correct:
            correct += 1
        conf_stats[conf][1] += 1
        if is_correct:
            conf_stats[conf][0] += 1
        if abs(r['recent_3d_chg']) <= 0.8:
            fuzzy_total += 1
            if is_correct:
                fuzzy_correct += 1

        details.append({
            'code': r['code'], 'eval_date': r['eval_date'],
            'future_dates': r.get('future_dates', r.get('window_dates', [])),
            'recent_3d_chg': r['recent_3d_chg'],
            'future_5d_chg': r['future_5d_chg'],
            'pred_up': pred_up, 'actual_up': actual_up,
            'correct': is_correct, 'reason': reason,
            'confidence': conf, 'concept_boards': r['concept_boards'],
        })

    accuracy = correct / total * 100 if total > 0 else 0
    return {
        'accuracy': round(accuracy, 1), 'correct': correct, 'total': total,
        'by_confidence': {
            k: {'accuracy': round(v[0] / v[1] * 100, 1) if v[1] > 0 else 0, 'count': v[1]}
            for k, v in conf_stats.items()
        },
        'fuzzy_zone': {
            'accuracy': round(fuzzy_correct / fuzzy_total * 100, 1) if fuzzy_total > 0 else 0,
            'count': fuzzy_total,
        },
        'details': details,
    }


def _run_weekly_lowo_cv(weekly, all_weeks):
    """周预测 Leave-One-Week-Out 交叉验证。"""
    week_accuracies = []
    total_correct = 0
    total_count = 0

    for held_out_week in all_weeks:
        train_stats = _compute_stock_stats(weekly, exclude_week=held_out_week)
        test_records = [w for w in weekly if w['iso_week'] == held_out_week]
        if not test_records:
            continue

        correct = 0
        for w in test_records:
            sig = w['concept_signal']
            ss = train_stats.get(w['code'])
            pred_up, _, _ = predict_weekly_direction(
                w['d3_chg'], sig, ss, w.get('d3_daily'))
            if pred_up == w['weekly_up']:
                correct += 1

        acc = correct / len(test_records) * 100
        week_accuracies.append(acc)
        total_correct += correct
        total_count += len(test_records)

    overall_acc = total_correct / total_count * 100 if total_count > 0 else 0
    avg_week_acc = _mean(week_accuracies) if week_accuracies else 0

    return {
        'overall_accuracy': round(overall_acc, 1),
        'avg_week_accuracy': round(avg_week_acc, 1),
        'total_correct': total_correct,
        'total_count': total_count,
        'n_weeks': len(week_accuracies),
        'week_accuracies': [round(a, 1) for a in week_accuracies],
        'min_week_accuracy': round(min(week_accuracies), 1) if week_accuracies else 0,
        'max_week_accuracy': round(max(week_accuracies), 1) if week_accuracies else 0,
    }


def _run_5day_lowo_cv(records_5d, all_eval_dates, n_folds=10):
    """5日预测交叉验证（按时间分折）。

    将评估日期按时间排序后分成n_folds折，每折作为测试集，其余作为训练集。
    """
    sorted_dates = sorted(all_eval_dates)
    fold_size = max(1, len(sorted_dates) // n_folds)
    folds = []
    for i in range(0, len(sorted_dates), fold_size):
        fold_dates = set(sorted_dates[i:i + fold_size])
        if fold_dates:
            folds.append(fold_dates)

    fold_accuracies = []
    total_correct = 0
    total_count = 0

    for fold_dates in folds:
        train_stats = _compute_stock_stats_5d(records_5d, exclude_dates=fold_dates)
        test_records = [r for r in records_5d if r['eval_date'] in fold_dates]
        if not test_records:
            continue

        correct = 0
        for r in test_records:
            sig = r['concept_signal']
            ss = train_stats.get(r['code'])
            pred_up, _, _ = predict_5day_direction(
                r['recent_3d_chg'], sig, ss,
                r.get('recent_daily'), r.get('vol_ratio'))
            if pred_up == r['future_up']:
                correct += 1

        acc = correct / len(test_records) * 100
        fold_accuracies.append(acc)
        total_correct += correct
        total_count += len(test_records)

    overall_acc = total_correct / total_count * 100 if total_count > 0 else 0
    avg_fold_acc = _mean(fold_accuracies) if fold_accuracies else 0

    return {
        'overall_accuracy': round(overall_acc, 1),
        'avg_fold_accuracy': round(avg_fold_acc, 1),
        'total_correct': total_correct,
        'total_count': total_count,
        'n_folds': len(fold_accuracies),
        'fold_accuracies': [round(a, 1) for a in fold_accuracies],
        'min_fold_accuracy': round(min(fold_accuracies), 1) if fold_accuracies else 0,
        'max_fold_accuracy': round(max(fold_accuracies), 1) if fold_accuracies else 0,
    }


def _analyze_by_concept_board(records, stock_stats, predict_fn,
                               concept_board_map=None, record_type='weekly'):
    """按概念板块分组分析准确率（通用）。"""
    board_stats = defaultdict(lambda: {'correct': 0, 'total': 0, 'stocks': set()})

    for r in records:
        sig = r['concept_signal']
        ss = stock_stats.get(r['code']) if stock_stats else None

        if record_type == 'weekly':
            pred_up, _, _ = predict_fn(r['d3_chg'], sig, ss, r.get('d3_daily'))
            actual_up = r['weekly_up']
        else:
            pred_up, _, _ = predict_fn(
                r['recent_3d_chg'], sig, ss,
                r.get('recent_daily'), r.get('vol_ratio'))
            actual_up = r['future_up']

        is_correct = pred_up == actual_up
        board_name = '未分类'
        if concept_board_map and r['code'] in concept_board_map:
            board_name = concept_board_map[r['code']]
        elif r.get('concept_boards'):
            board_name = r['concept_boards'][0]

        board_stats[board_name]['total'] += 1
        board_stats[board_name]['stocks'].add(r['code'])
        if is_correct:
            board_stats[board_name]['correct'] += 1

    results = []
    for board, stats in sorted(board_stats.items(), key=lambda x: -x[1]['total']):
        acc = stats['correct'] / stats['total'] * 100 if stats['total'] > 0 else 0
        results.append({
            'board_name': board,
            'accuracy': round(acc, 1),
            'correct': stats['correct'],
            'total': stats['total'],
            'stock_count': len(stats['stocks']),
        })
    return results


# ═══════════════════════════════════════════════════════════
# 回测主函数（DB模式）
# ═══════════════════════════════════════════════════════════

def run_v2_backtest(
    stock_codes: list[str],
    start_date: str = '2025-12-01',
    end_date: str = '2026-03-10',
    concept_board_map: dict = None,
    stride_5d: int = 3,
) -> dict:
    """运行v2回测：周预测 + 5日滚动预测（DB模式）。"""
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  概念板块增强 周预测+5日预测 回测 v2")
    logger.info("  股票: %d只, 区间: %s ~ %s", len(stock_codes), start_date, end_date)
    logger.info("=" * 70)

    logger.info("[1/7] 预加载数据...")
    data = _preload_weekly_data(stock_codes, start_date, end_date)

    logger.info("[2/7] 构建周数据...")
    weekly = _build_weekly_records(stock_codes, data, start_date, end_date)
    logger.info("  周样本: %d", len(weekly))

    logger.info("[3/7] 构建5日滚动数据...")
    records_5d = _build_5day_records(stock_codes, data, start_date, end_date, stride=stride_5d)
    logger.info("  5日样本: %d", len(records_5d))

    if not weekly and not records_5d:
        return {'error': '无有效数据', 'weekly_count': 0, 'fiveday_count': 0}

    # 周预测评估
    all_weeks = sorted(set(w['iso_week'] for w in weekly)) if weekly else []
    all_stocks = sorted(set(w['code'] for w in weekly)) if weekly else []
    n_with_sig_w = sum(1 for w in weekly if w['concept_signal'] is not None)

    logger.info("[4/7] 周预测全样本评估...")
    stock_stats_w = _compute_stock_stats(weekly) if weekly else {}
    weekly_full = _evaluate_weekly_predictions(weekly, stock_stats_w) if weekly else {}

    logger.info("[5/7] 周预测LOWO交叉验证...")
    weekly_lowo = _run_weekly_lowo_cv(weekly, all_weeks) if weekly else {}

    # 5日预测评估
    all_eval_dates = sorted(set(r['eval_date'] for r in records_5d))
    n_with_sig_5d = sum(1 for r in records_5d if r['concept_signal'] is not None)

    logger.info("[6/7] 5日预测全样本评估...")
    stock_stats_5d = _compute_stock_stats_5d(records_5d) if records_5d else {}
    fiveday_full = _evaluate_5day_predictions(records_5d, stock_stats_5d)

    logger.info("[7/7] 5日预测交叉验证...")
    fiveday_cv = _run_5day_lowo_cv(records_5d, all_eval_dates)

    # 按板块分析
    board_weekly = _analyze_by_concept_board(
        weekly, stock_stats_w, predict_weekly_direction,
        concept_board_map, 'weekly') if weekly else []
    board_5day = _analyze_by_concept_board(
        records_5d, stock_stats_5d, predict_5day_direction,
        concept_board_map, '5day') if records_5d else []

    elapsed = (datetime.now() - t_start).total_seconds()

    return {
        'summary': {
            'stock_count': len(all_stocks),
            'week_count': len(all_weeks),
            'weekly_sample_count': len(weekly),
            'fiveday_sample_count': len(records_5d),
            'fiveday_eval_dates': len(all_eval_dates),
            'concept_signal_coverage_weekly': round(
                n_with_sig_w / len(weekly) * 100, 1) if weekly else 0,
            'concept_signal_coverage_5day': round(
                n_with_sig_5d / len(records_5d) * 100, 1) if records_5d else 0,
            'backtest_period': f'{start_date} ~ {end_date}',
            'elapsed_seconds': round(elapsed, 1),
            'data_mode': 'database',
        },
        'weekly': {
            'full_sample': weekly_full,
            'lowo_cv': weekly_lowo,
            'by_concept_board': board_weekly,
        },
        'fiveday': {
            'full_sample': fiveday_full,
            'cv': fiveday_cv,
            'by_concept_board': board_5day,
        },
    }


# ═══════════════════════════════════════════════════════════
# 模拟数据生成（DB不可达时使用）
# ═══════════════════════════════════════════════════════════

def _generate_realistic_klines(stock_code, start_date, end_date,
                                base_price=50.0, volatility=2.0, trend=0.0):
    """生成逼真的个股日K线数据（带均值回归特性）。"""
    seed = int(hashlib.md5(stock_code.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    dt = datetime.strptime(start_date, '%Y-%m-%d')
    dt_end = datetime.strptime(end_date, '%Y-%m-%d')

    klines = []
    price = base_price
    recent_changes = []

    while dt <= dt_end:
        if dt.weekday() >= 5:
            dt += timedelta(days=1)
            continue

        daily_chg = rng.gauss(trend / 250, volatility / 100)

        if len(recent_changes) >= 3:
            avg_3d = sum(recent_changes[-3:]) / 3
            daily_chg -= avg_3d * 0.15
        if len(recent_changes) >= 1:
            prev_chg = recent_changes[-1]
            if abs(prev_chg) > 3:
                daily_chg -= prev_chg * 0.20
        if len(recent_changes) >= 3:
            streak = 0
            for j in range(len(recent_changes) - 1, max(len(recent_changes) - 6, -1), -1):
                if recent_changes[j] > 0.2:
                    streak += 1
                elif recent_changes[j] < -0.2:
                    streak -= 1
                else:
                    break
            if streak >= 3:
                daily_chg -= 0.8 / 100
            elif streak <= -3:
                daily_chg += 0.8 / 100

        wd = dt.weekday()
        if wd == 4:
            daily_chg -= 0.1 / 100
        elif wd == 0:
            daily_chg += 0.05 / 100

        daily_chg = max(-0.10, min(0.10, daily_chg))
        change_pct = daily_chg * 100
        new_price = price * (1 + daily_chg)
        high = new_price * (1 + abs(rng.gauss(0, 0.005)))
        low = new_price * (1 - abs(rng.gauss(0, 0.005)))
        volume = rng.uniform(5000, 50000) * (1 + abs(daily_chg) * 10)

        klines.append({
            'date': dt.strftime('%Y-%m-%d'),
            'open_price': round(price, 2),
            'close_price': round(new_price, 2),
            'high_price': round(high, 2),
            'low_price': round(low, 2),
            'trading_volume': round(volume, 0),
            'trading_amount': round(volume * new_price, 0),
            'change_percent': round(change_pct, 4),
            'change_hand': round(rng.uniform(0.5, 5.0), 2),
        })

        recent_changes.append(change_pct)
        if len(recent_changes) > 20:
            recent_changes = recent_changes[-20:]
        price = new_price
        dt += timedelta(days=1)

    return klines


def _generate_board_klines(board_code, start_date, end_date,
                            member_klines_list, noise=0.3):
    """根据成分股K线生成板块K线。"""
    seed = int(hashlib.md5(board_code.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    date_changes = defaultdict(list)
    for klines in member_klines_list:
        for k in klines:
            date_changes[k['date']].append(k['change_percent'])

    board_klines = []
    price = 1000.0
    for d in sorted(date_changes.keys()):
        changes = date_changes[d]
        if not changes:
            continue
        avg_chg = _mean(changes) + rng.gauss(0, noise)
        avg_chg = max(-10, min(10, avg_chg))
        new_price = price * (1 + avg_chg / 100)
        board_klines.append({
            'date': d,
            'change_percent': round(avg_chg, 4),
            'close_price': round(new_price, 2),
        })
        price = new_price
    return board_klines


def _preload_simulated_data(stock_codes, start_date, end_date,
                             concept_board_stocks):
    """生成模拟数据。"""
    dt = datetime.strptime(start_date, '%Y-%m-%d')
    ext_start = (dt - timedelta(days=180)).strftime('%Y-%m-%d')

    board_trends = {}
    board_vols = {}
    for board_name in concept_board_stocks.keys():
        seed = int(hashlib.md5(board_name.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)
        board_trends[board_name] = rng.uniform(-15, 15)
        board_vols[board_name] = rng.uniform(1.5, 3.5)

    stock_klines = {}
    stock_boards = defaultdict(list)
    board_code_map = {}
    board_idx = 0

    for board_name, codes in concept_board_stocks.items():
        board_code = f'3{board_idx:05d}'
        board_code_map[board_name] = board_code
        board_idx += 1
        trend = board_trends[board_name]
        vol = board_vols[board_name]

        for code in codes:
            if code in stock_klines:
                stock_boards[code].append({
                    'board_code': board_code, 'board_name': board_name,
                })
                continue
            code_seed = int(hashlib.md5(code.encode()).hexdigest()[:8], 16)
            code_rng = random.Random(code_seed)
            stock_trend = trend + code_rng.uniform(-10, 10)
            stock_vol = vol * code_rng.uniform(0.8, 1.3)
            base_price = code_rng.uniform(10, 200)
            klines = _generate_realistic_klines(
                code, ext_start, end_date,
                base_price=base_price, volatility=stock_vol, trend=stock_trend)
            stock_klines[code] = klines
            stock_boards[code].append({
                'board_code': board_code, 'board_name': board_name,
            })

    board_kline_map = {}
    for board_name, codes in concept_board_stocks.items():
        board_code = board_code_map[board_name]
        member_klines = [stock_klines[c] for c in codes if c in stock_klines]
        if member_klines:
            board_kline_map[board_code] = _generate_board_klines(
                board_code, ext_start, end_date, member_klines)

    all_member_klines = list(stock_klines.values())
    market_klines = _generate_board_klines(
        'market_index', ext_start, end_date, all_member_klines, noise=0.1)

    fund_flow_map = {}
    for code in stock_codes:
        code_seed = int(hashlib.md5((code + '_ff').encode()).hexdigest()[:8], 16)
        rng = random.Random(code_seed)
        klines = stock_klines.get(code, [])
        flows = []
        for k in klines:
            flows.append({
                'date': k['date'],
                'big_net': round(rng.gauss(0, 5000), 2),
                'big_net_pct': round(rng.gauss(0, 3), 2),
                'main_net_5day': round(rng.gauss(0, 10000), 2),
                'net_flow': round(rng.gauss(0, 8000), 2),
            })
        fund_flow_map[code] = list(reversed(flows))

    return {
        'stock_klines': stock_klines,
        'stock_boards': dict(stock_boards),
        'board_kline_map': board_kline_map,
        'market_klines': market_klines,
        'fund_flow_map': fund_flow_map,
    }


def run_v2_backtest_simulated(
    stock_codes: list[str],
    concept_board_stocks: dict,
    start_date: str = '2025-12-01',
    end_date: str = '2026-03-10',
    concept_board_map: dict = None,
    stride_5d: int = 3,
) -> dict:
    """使用模拟数据运行v2回测。"""
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  概念板块增强 周预测+5日预测 回测 v2（模拟数据）")
    logger.info("  股票: %d只, 区间: %s ~ %s", len(stock_codes), start_date, end_date)
    logger.info("=" * 70)

    logger.info("[1/7] 生成模拟数据...")
    data = _preload_simulated_data(stock_codes, start_date, end_date,
                                    concept_board_stocks)

    logger.info("[2/7] 构建周数据...")
    weekly = _build_weekly_records(stock_codes, data, start_date, end_date)
    logger.info("  周样本: %d", len(weekly))

    logger.info("[3/7] 构建5日滚动数据...")
    records_5d = _build_5day_records(stock_codes, data, start_date, end_date,
                                      stride=stride_5d)
    logger.info("  5日样本: %d", len(records_5d))

    if not weekly and not records_5d:
        return {'error': '无有效数据'}

    all_weeks = sorted(set(w['iso_week'] for w in weekly)) if weekly else []
    all_stocks = sorted(set(w['code'] for w in weekly)) if weekly else []
    n_with_sig_w = sum(1 for w in weekly if w['concept_signal'] is not None)

    logger.info("[4/7] 周预测全样本评估...")
    stock_stats_w = _compute_stock_stats(weekly) if weekly else {}
    weekly_full = _evaluate_weekly_predictions(weekly, stock_stats_w) if weekly else {}

    logger.info("[5/7] 周预测LOWO交叉验证...")
    weekly_lowo = _run_weekly_lowo_cv(weekly, all_weeks) if weekly else {}

    all_eval_dates = sorted(set(r['eval_date'] for r in records_5d))
    n_with_sig_5d = sum(1 for r in records_5d if r['concept_signal'] is not None)

    logger.info("[6/7] 5日预测全样本评估...")
    stock_stats_5d = _compute_stock_stats_5d(records_5d) if records_5d else {}
    fiveday_full = _evaluate_5day_predictions(records_5d, stock_stats_5d)

    logger.info("[7/7] 5日预测交叉验证...")
    fiveday_cv = _run_5day_lowo_cv(records_5d, all_eval_dates)

    board_weekly = _analyze_by_concept_board(
        weekly, stock_stats_w, predict_weekly_direction,
        concept_board_map, 'weekly') if weekly else []
    board_5day = _analyze_by_concept_board(
        records_5d, stock_stats_5d, predict_5day_direction,
        concept_board_map, '5day') if records_5d else []

    elapsed = (datetime.now() - t_start).total_seconds()

    return {
        'summary': {
            'stock_count': len(all_stocks),
            'week_count': len(all_weeks),
            'weekly_sample_count': len(weekly),
            'fiveday_sample_count': len(records_5d),
            'fiveday_eval_dates': len(all_eval_dates),
            'concept_signal_coverage_weekly': round(
                n_with_sig_w / len(weekly) * 100, 1) if weekly else 0,
            'concept_signal_coverage_5day': round(
                n_with_sig_5d / len(records_5d) * 100, 1) if records_5d else 0,
            'backtest_period': f'{start_date} ~ {end_date}',
            'elapsed_seconds': round(elapsed, 1),
            'data_mode': 'simulated',
        },
        'weekly': {
            'full_sample': weekly_full,
            'lowo_cv': weekly_lowo,
            'by_concept_board': board_weekly,
        },
        'fiveday': {
            'full_sample': fiveday_full,
            'cv': fiveday_cv,
            'by_concept_board': board_5day,
        },
    }
