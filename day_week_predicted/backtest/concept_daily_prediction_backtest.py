#!/usr/bin/env python3
"""
概念板块增强日预测回测引擎 v1

核心策略：在 prediction_enhanced_backtest 的多因子基础上，深度融合概念板块维度：
1. 概念板块整体走势强弱（板块 vs 大盘超额收益）
2. 个股在概念板块中的相对强弱势（个股 vs 板块超额收益）
3. 概念板块共识度（多板块方向一致性）
4. 概念板块动量（近N日板块平均涨跌）
5. 个股自适应阈值：根据历史概念板块特征动态调整决策参数

数据源：全部从数据库获取
- stock_kline: 个股日K线
- concept_board_kline: 概念板块日K线
- stock_concept_board_stock: 个股-概念板块映射
- stock_concept_strength: 个股板块内强弱评分
- stock_fund_flow: 资金流向

目标：日预测准确率（宽松）≥ 65%
"""

import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

from dao import get_connection

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


def _rate_str(ok, n):
    return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'


def _compound_return(pcts):
    p = 1.0
    for r in pcts:
        p *= (1 + r / 100)
    return (p - 1) * 100


# ═══════════════════════════════════════════════════════════
# 数据预加载（全部从DB）
# ═══════════════════════════════════════════════════════════

def _preload_all_data(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """一次性从DB预加载所有需要的数据。"""
    codes_6 = []
    full_map = {}  # 6位 -> 完整代码
    for c in stock_codes:
        c6 = c.split('.')[0] if '.' in c else c
        codes_6.append(c6)
        full_map[c6] = c

    # 扩展日期范围以支持lookback
    dt = datetime.strptime(start_date, '%Y-%m-%d')
    ext_start = (dt - timedelta(days=180)).strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 1. 个股K线
        stock_klines = {}
        if codes_6:
            # 也查带后缀的代码
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
                # 统一用完整代码
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
        market_klines = []
        for r in cur.fetchall():
            market_klines.append({
                'date': r['date'],
                'change_percent': _to_float(r['change_percent']),
                'close_price': _to_float(r['close_price']),
            })

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

        # 6. 使用stock_concept_strength表获取个股板块内强弱（避免加载大量同行K线）
        stock_strength_map = defaultdict(dict)
        if codes_6:
            ph4 = ','.join(['%s'] * len(codes_6))
            cur.execute(
                f"SELECT stock_code, board_code, strength_score, strength_level, "
                f"excess_5d, excess_20d, excess_total, win_rate, rank_in_board, "
                f"board_total_stocks "
                f"FROM stock_concept_strength "
                f"WHERE stock_code IN ({ph4})",
                tuple(codes_6),
            )
            for r in cur.fetchall():
                full = full_map.get(r['stock_code'], r['stock_code'])
                stock_strength_map[full][r['board_code']] = {
                    'strength_score': _to_float(r['strength_score']),
                    'strength_level': r['strength_level'],
                    'excess_5d': _to_float(r['excess_5d']),
                    'excess_20d': _to_float(r['excess_20d']),
                    'excess_total': _to_float(r['excess_total']),
                    'win_rate': _to_float(r['win_rate']),
                    'rank_in_board': r['rank_in_board'],
                    'board_total_stocks': r['board_total_stocks'],
                }

        # 同行K线不再批量加载，改用板块K线+strength表
        board_peer_klines = {}

    finally:
        cur.close()
        conn.close()

    n_with_boards = sum(1 for c in stock_codes if c in stock_boards)
    n_with_kline = sum(1 for bc in all_board_codes if bc in board_kline_map)
    n_with_strength = sum(1 for c in stock_codes if c in stock_strength_map)
    logger.info("[数据预加载] %d只股票K线, %d只有概念板块, %d/%d板块有K线, "
                "大盘%d天, 资金流%d只, 强弱评分%d只",
                len(stock_klines), n_with_boards, n_with_kline,
                len(all_board_codes), len(market_klines), len(fund_flow_map),
                n_with_strength)

    return {
        'stock_klines': dict(stock_klines),
        'stock_boards': dict(stock_boards),
        'board_kline_map': dict(board_kline_map),
        'market_klines': market_klines,
        'fund_flow_map': dict(fund_flow_map),
        'board_peer_klines': dict(board_peer_klines),
        'stock_strength_map': dict(stock_strength_map),
    }


# ═══════════════════════════════════════════════════════════
# 概念板块信号计算
# ═══════════════════════════════════════════════════════════

def _compute_board_strength(board_klines: list[dict], market_klines: list[dict],
                            score_date: str, lookback: int = 20) -> dict | None:
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
    recent_5 = daily_excess[-min(5, n):]
    excess_5d = sum(recent_5)
    win_days = sum(1 for e in daily_excess if e > 0)
    win_rate = win_days / n
    momentum = _mean([k['change_percent'] for k, _ in aligned[-5:]])

    s1 = _sigmoid(excess_total, center=0, scale=8) * 30
    s2 = _sigmoid(excess_5d, center=0, scale=2) * 25
    s3 = _sigmoid(sum(daily_excess[-min(20, n):]), center=0, scale=4) * 20
    s4 = max(0, min(15, (win_rate - 0.3) / 0.4 * 15))
    score = round(max(0, min(100, s1 + s2 + s3 + s4)), 1)

    return {
        'score': score,
        'excess_total': round(excess_total, 3),
        'excess_5d': round(excess_5d, 3),
        'win_rate': round(win_rate, 4),
        'momentum': round(momentum, 4),
    }


def _compute_stock_in_board_strength(stock_code_6: str, board_klines: list[dict],
                                     peer_klines_in_board: dict,
                                     score_date: str, lookback: int = 20) -> dict | None:
    """计算个股在概念板块中的相对强弱势。"""
    # 找到个股在同行中的K线
    stock_kl = peer_klines_in_board.get(stock_code_6, [])
    if not stock_kl:
        return None

    sk_map = {k['date']: k['change_percent'] for k in stock_kl if k['date'] <= score_date}
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
    excess_total = sum(daily_excess)
    excess_5d = sum(daily_excess[-min(5, n):])
    win_days = sum(1 for e in daily_excess if e > 0)
    win_rate = win_days / n

    # 计算个股在板块内的排名百分位
    peer_excess_totals = []
    for pc, pk in peer_klines_in_board.items():
        pk_map = {k['date']: k['change_percent'] for k in pk if k['date'] <= score_date}
        p_aligned = [(pk_map[k['date']], k['change_percent'])
                     for k in recent if k['date'] in pk_map]
        if len(p_aligned) >= 5:
            peer_excess_totals.append(sum(s - b for s, b in p_aligned))

    rank_pct = 0.5
    if peer_excess_totals:
        rank = sum(1 for e in peer_excess_totals if e < excess_total)
        rank_pct = rank / len(peer_excess_totals)

    score = round(rank_pct * 100, 1)

    return {
        'score': score,
        'excess_total': round(excess_total, 3),
        'excess_5d': round(excess_5d, 3),
        'win_rate': round(win_rate, 4),
        'rank_pct': round(rank_pct, 4),
        'n_peers': len(peer_excess_totals),
    }


def _compute_concept_signals(stock_code: str, score_date: str,
                             stock_boards: list[dict],
                             board_kline_map: dict,
                             market_klines: list[dict],
                             board_peer_klines: dict,
                             stock_strength_data: dict = None) -> dict:
    """计算个股在某日期的全部概念板块信号。"""
    code_6 = stock_code.split('.')[0] if '.' in stock_code else stock_code

    board_strengths = []
    stock_in_board_scores = []
    board_momentums = []
    boards_up = 0
    boards_total = 0

    for board in stock_boards:
        bc = board['board_code']
        bk = board_kline_map.get(bc, [])
        if not bk:
            continue

        valid_bk = [k for k in bk if k['date'] <= score_date]
        if len(valid_bk) < 5:
            continue

        boards_total += 1

        # 板块整体强弱
        bs = _compute_board_strength(bk, market_klines, score_date)
        if bs:
            board_strengths.append(bs['score'])

        # 个股在板块中的强弱（使用预计算的strength数据）
        if stock_strength_data and bc in stock_strength_data:
            sib_score = stock_strength_data[bc].get('strength_score', 50)
            stock_in_board_scores.append(sib_score)
        else:
            # 回退：基于板块K线和个股K线简单计算
            stock_in_board_scores.append(50.0)

        # 板块动量
        recent_5 = valid_bk[-5:]
        avg_chg = _mean([k['change_percent'] for k in recent_5])
        board_momentums.append(avg_chg)
        if avg_chg > 0:
            boards_up += 1

    if boards_total == 0:
        return {'has_concept': False}

    consensus = boards_up / boards_total
    avg_board_strength = _mean(board_strengths) if board_strengths else 50.0
    avg_stock_strength = _mean(stock_in_board_scores) if stock_in_board_scores else 50.0
    avg_momentum = _mean(board_momentums)

    # 强势板块占比
    strong_board_pct = (sum(1 for s in board_strengths if s > 60) /
                        len(board_strengths)) if board_strengths else 0.5
    # 个股在板块中强势的占比
    strong_stock_pct = (sum(1 for s in stock_in_board_scores if s > 60) /
                        len(stock_in_board_scores)) if stock_in_board_scores else 0.5

    return {
        'has_concept': True,
        'n_boards': boards_total,
        'consensus': consensus,
        'avg_board_strength': avg_board_strength,
        'avg_stock_strength': avg_stock_strength,
        'avg_momentum': avg_momentum,
        'strong_board_pct': strong_board_pct,
        'strong_stock_pct': strong_stock_pct,
    }


# ═══════════════════════════════════════════════════════════
# 技术指标计算
# ═══════════════════════════════════════════════════════════

def _ema(data, period):
    if not data:
        return []
    result = [0.0] * len(data)
    k = 2 / (period + 1)
    result[0] = data[0]
    for i in range(1, len(data)):
        result[i] = data[i] * k + result[i - 1] * (1 - k)
    return result


def _calc_macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return []
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    dif = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]
    dea = _ema(dif, signal)
    return [{'DIF': dif[i], 'DEA': dea[i], 'MACD柱': 2 * (dif[i] - dea[i])}
            for i in range(len(closes))]


def _calc_kdj(highs, lows, closes, n=9, m1=3, m2=3):
    if len(closes) < n:
        return []
    result = []
    k_prev, d_prev = 50.0, 50.0
    for i in range(len(closes)):
        if i < n - 1:
            result.append({'K': 50.0, 'D': 50.0, 'J': 50.0})
            continue
        h_n = max(highs[i - n + 1:i + 1])
        l_n = min(lows[i - n + 1:i + 1])
        rsv = (closes[i] - l_n) / (h_n - l_n) * 100 if h_n != l_n else 50
        k = (m1 - 1) / m1 * k_prev + 1 / m1 * rsv
        d = (m2 - 1) / m2 * d_prev + 1 / m2 * k
        j = 3 * k - 2 * d
        k_prev, d_prev = k, d
        result.append({'K': round(k, 2), 'D': round(d, 2), 'J': round(j, 2)})
    return result


def _calc_boll(closes, period=20, mult=2):
    result = [None] * len(closes)
    if len(closes) < period:
        return result
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1:i + 1]
        mid = sum(window) / period
        std = math.sqrt(sum((x - mid) ** 2 for x in window) / period)
        result[i] = {'上轨': mid + mult * std, '中轨': mid, '下轨': mid - mult * std}
    return result


# ═══════════════════════════════════════════════════════════
# 多因子信号计算
# ═══════════════════════════════════════════════════════════

def _compute_technical_factors(klines, end_idx, closes, highs, lows,
                               macd_list, kdj_list, boll_list, n,
                               market_klines, fund_flow, score_date) -> dict:
    """计算技术面因子信号。"""
    k_today = klines[end_idx]
    c_today = k_today['close_price']
    c_yest = klines[end_idx - 1]['close_price'] if end_idx > 0 else c_today
    vol_today = k_today.get('trading_volume', 0) or 0
    chg_today = (c_today - c_yest) / c_yest * 100 if c_yest > 0 else 0

    # 近20日收益率
    daily_returns = []
    for j in range(min(20, end_idx)):
        c_j = klines[end_idx - j]['close_price']
        c_j_prev = klines[end_idx - j - 1]['close_price']
        if c_j_prev > 0:
            daily_returns.append((c_j - c_j_prev) / c_j_prev * 100)

    if len(daily_returns) >= 10:
        avg_ret = _mean(daily_returns)
        vol_std = max(0.5, math.sqrt(sum((r - avg_ret) ** 2 for r in daily_returns) / len(daily_returns)))
    else:
        vol_std = 2.0

    z_today = chg_today / vol_std

    # 因子1: 均值回归
    reversion = 0.0
    if z_today > 2.0: reversion = -3.0
    elif z_today > 1.2: reversion = -1.5
    elif z_today > 0.8: reversion = -0.5
    elif z_today < -2.0: reversion = 3.0
    elif z_today < -1.2: reversion = 1.5
    elif z_today < -0.8: reversion = 0.5

    if end_idx >= 2:
        c_2d = klines[end_idx - 2]['close_price']
        chg_2d = (c_today - c_2d) / c_2d * 100 if c_2d > 0 else 0
        z_2d = chg_2d / (vol_std * 1.41)
        if z_2d > 1.8: reversion -= 2.0
        elif z_2d > 1.0: reversion -= 0.8
        elif z_2d < -1.8: reversion += 2.0
        elif z_2d < -1.0: reversion += 0.8

    if end_idx >= 5:
        c_5d = klines[end_idx - 5]['close_price']
        chg_5d = (c_today - c_5d) / c_5d * 100 if c_5d > 0 else 0
        z_5d = chg_5d / (vol_std * 2.24)
        if z_5d > 1.5: reversion -= 1.0
        elif z_5d < -1.5: reversion += 1.0

    # 因子2: RSI(14)
    rsi_score = 0.0
    gains = [max(r, 0) for r in daily_returns[:14]]
    losses = [max(-r, 0) for r in daily_returns[:14]]
    avg_gain = _mean(gains)
    avg_loss = max(_mean(losses), 0.001)
    rsi_14 = 100 - (100 / (1 + avg_gain / avg_loss))
    if rsi_14 > 80: rsi_score = -2.5
    elif rsi_14 > 70: rsi_score = -1.5
    elif rsi_14 > 65: rsi_score = -0.5
    elif rsi_14 < 20: rsi_score = 2.5
    elif rsi_14 < 30: rsi_score = 1.5
    elif rsi_14 < 35: rsi_score = 0.5

    # 因子3: KDJ
    kdj_score = 0.0
    if kdj_list and len(kdj_list) >= n and n >= 2:
        k_val = kdj_list[n - 1]['K']
        j_val = kdj_list[n - 1]['J']
        if j_val > 100 and k_val > 80: kdj_score = -2.0
        elif j_val > 90 and k_val > 75: kdj_score = -1.0
        elif j_val < 0 and k_val < 20: kdj_score = 2.0
        elif j_val < 10 and k_val < 25: kdj_score = 1.0

    # 因子4: MACD
    macd_score = 0.0
    if macd_list and len(macd_list) >= n and n >= 3:
        bar_today = macd_list[n - 1]['MACD柱']
        bar_yest = macd_list[n - 2]['MACD柱']
        if bar_yest < 0 and bar_today > 0: macd_score += 1.5
        elif bar_yest > 0 and bar_today < 0: macd_score -= 1.5

    # 因子5: BOLL
    boll_score = 0.0
    if boll_list and len(boll_list) >= n and boll_list[n - 1]:
        upper = boll_list[n - 1]['上轨']
        lower = boll_list[n - 1]['下轨']
        bw = upper - lower
        if bw > 0:
            pct = (c_today - lower) / bw
            if pct > 0.95: boll_score = -2.0
            elif pct > 0.85: boll_score = -1.0
            elif pct < 0.05: boll_score = 2.0
            elif pct < 0.15: boll_score = 1.0

    # 因子6: 量价背离
    vp_score = 0.0
    vols_5 = [klines[end_idx - j].get('trading_volume', 0) or 0
              for j in range(min(5, end_idx + 1))]
    avg_vol_5 = _mean(vols_5) if vols_5 else 1
    vol_ratio = vol_today / avg_vol_5 if avg_vol_5 > 0 else 1.0
    if chg_today > 1.0 and vol_ratio < 0.7: vp_score = -1.0
    elif chg_today < -1.0 and vol_ratio < 0.7: vp_score = 1.0
    elif chg_today > 1.5 and vol_ratio > 1.8: vp_score = 0.5
    elif chg_today < -1.5 and vol_ratio > 1.8: vp_score = -0.5

    # 因子7: 大盘环境
    market_score = 0.0
    if market_klines:
        mk_filtered = [k for k in market_klines if k['date'] <= score_date]
        if len(mk_filtered) >= 2:
            idx_chg = mk_filtered[-1]['change_percent']
            if idx_chg < -1.5: market_score = 1.0
            elif idx_chg < -0.8: market_score = 0.3
            elif idx_chg > 1.5: market_score = -0.5
            if len(mk_filtered) >= 6:
                idx_c = mk_filtered[-1]['close_price']
                idx_c5 = mk_filtered[-6]['close_price']
                if idx_c5 > 0:
                    idx_chg5 = (idx_c - idx_c5) / idx_c5 * 100
                    if idx_chg5 > 3: market_score -= 0.5
                    elif idx_chg5 < -3: market_score += 0.5

    # 因子8: 连续涨跌
    streak_score = 0.0
    up_streak = down_streak = 0
    for j in range(min(10, end_idx)):
        idx_j = end_idx - j
        if idx_j <= 0: break
        if klines[idx_j]['close_price'] > klines[idx_j - 1]['close_price']:
            if down_streak > 0: break
            up_streak += 1
        elif klines[idx_j]['close_price'] < klines[idx_j - 1]['close_price']:
            if up_streak > 0: break
            down_streak += 1
        else:
            break
    if up_streak >= 5: streak_score = -2.5
    elif up_streak >= 4: streak_score = -1.5
    elif up_streak >= 3: streak_score = -0.8
    elif down_streak >= 5: streak_score = 2.5
    elif down_streak >= 4: streak_score = 1.5
    elif down_streak >= 3: streak_score = 0.8

    # 因子9: 波动率状态
    vol_regime = 0.0
    if len(daily_returns) >= 10:
        recent_5d = daily_returns[:min(5, len(daily_returns))]
        avg_5d = _mean(recent_5d)
        vol_5d = max(0.3, math.sqrt(sum((r - avg_5d) ** 2 for r in recent_5d) / len(recent_5d)))
        vol_ratio_regime = vol_5d / vol_std if vol_std > 0.3 else 1.0
        if vol_ratio_regime > 1.5: vol_regime = 1.0
        elif vol_ratio_regime < 0.6: vol_regime = -1.0

    # 因子10: 跳空缺口
    gap_signal = 0.0
    open_today = k_today.get('open_price', c_today)
    if c_yest > 0 and open_today > 0:
        gap_pct = (open_today - c_yest) / c_yest * 100
        gap_z = gap_pct / vol_std if vol_std > 0.3 else 0
        if gap_z > 1.5: gap_signal = -2.0
        elif gap_z > 0.8: gap_signal = -1.0
        elif gap_z < -1.5: gap_signal = 2.0
        elif gap_z < -0.8: gap_signal = 1.0

    # 因子11: 日内收盘位置
    intraday_pos = 0.0
    h_today = k_today.get('high_price', c_today)
    l_today = k_today.get('low_price', c_today)
    day_range = h_today - l_today
    if day_range > 0:
        close_pos = (c_today - l_today) / day_range
        if close_pos > 0.9: intraday_pos = -1.5
        elif close_pos > 0.75: intraday_pos = -0.5
        elif close_pos < 0.1: intraday_pos = 1.5
        elif close_pos < 0.25: intraday_pos = 0.5

    # 因子12: DB资金流
    db_fund_signal = 0.0
    if fund_flow:
        recent_ff = [r for r in fund_flow if r['date'] <= score_date][:5]
        if recent_ff:
            big_net_pct = recent_ff[0].get('big_net_pct', 0)
            if big_net_pct > 5: db_fund_signal += 1.5
            elif big_net_pct > 2: db_fund_signal += 0.5
            elif big_net_pct < -5: db_fund_signal -= 1.5
            elif big_net_pct < -2: db_fund_signal -= 0.5
            main_5d = recent_ff[0].get('main_net_5day', 0)
            if main_5d > 5000: db_fund_signal += 1.0
            elif main_5d > 1000: db_fund_signal += 0.3
            elif main_5d < -5000: db_fund_signal -= 1.0
            elif main_5d < -1000: db_fund_signal -= 0.3

    # 因子13: 近10日涨跌比
    recent_up = recent_down = 0
    for j in range(1, min(11, end_idx + 1)):
        c_j = klines[end_idx - j + 1]['close_price']
        c_j_prev = klines[end_idx - j]['close_price']
        if c_j_prev > 0:
            r = (c_j - c_j_prev) / c_j_prev * 100
            if r > 0.3: recent_up += 1
            elif r < -0.3: recent_down += 1
    total_recent = recent_up + recent_down
    up_ratio_10d = recent_up / total_recent if total_recent > 0 else 0.5

    trend_adaptive = 0.0
    if up_ratio_10d >= 0.7: trend_adaptive = 2.0
    elif up_ratio_10d >= 0.6: trend_adaptive = 1.0
    elif up_ratio_10d <= 0.3: trend_adaptive = -2.0
    elif up_ratio_10d <= 0.4: trend_adaptive = -1.0

    return {
        'reversion': reversion, 'rsi': rsi_score, 'kdj': kdj_score,
        'macd': macd_score, 'boll': boll_score, 'vp': vp_score,
        'market': market_score, 'streak': streak_score,
        'vol_regime': vol_regime, 'gap_signal': gap_signal,
        'intraday_pos': intraday_pos, 'db_fund': db_fund_signal,
        'trend_adaptive': trend_adaptive, 'up_ratio_10d': up_ratio_10d,
        'z_today': z_today, 'vol_std': vol_std, 'chg_today': chg_today,
        'vol_ratio': vol_ratio, 'rsi_14': rsi_14,
    }


# ═══════════════════════════════════════════════════════════
# 概念板块增强方向决策（核心算法）
# ═══════════════════════════════════════════════════════════

def _decide_direction_with_concept(tech_factors: dict, concept_signals: dict,
                                   klines: list[dict], end_idx: int,
                                   score_date: str,
                                   stock_history: dict) -> dict:
    """融合技术因子 + 概念板块信号的方向决策。

    核心策略：
    1. 技术因子加权汇总（均值回归为主）
    2. 概念板块信号作为方向增强/修正
    3. 个股自适应：根据历史概念板块特征动态调整
    4. 置信度分层决策

    Args:
        tech_factors: 技术面因子
        concept_signals: 概念板块信号
        klines: K线数据
        end_idx: 当前索引
        score_date: 评分日期
        stock_history: 个股历史预测统计（用于自适应）
    """
    # ── 技术信号加权 ──
    # 基于实测有效性的因子权重
    tech_weights = {
        'reversion': 1.2, 'rsi': 1.0, 'kdj': 0.3, 'macd': 0.3,
        'boll': 0.5, 'vp': 0.8, 'market': 1.2, 'streak': 0.8,
        'vol_regime': 0.8, 'gap_signal': 0.8, 'intraday_pos': 0.6,
        'db_fund': 0.5,
    }
    tech_signal = sum(tech_factors.get(k, 0) * w for k, w in tech_weights.items())

    # ── 概念板块信号 ──
    concept_signal = 0.0
    concept_confidence = 0.0
    has_concept = concept_signals.get('has_concept', False)

    if has_concept:
        consensus = concept_signals['consensus']
        board_strength = concept_signals['avg_board_strength']
        stock_strength = concept_signals['avg_stock_strength']
        momentum = concept_signals['avg_momentum']
        strong_board_pct = concept_signals['strong_board_pct']
        strong_stock_pct = concept_signals['strong_stock_pct']
        n_boards = concept_signals['n_boards']

        # 概念板块共识度信号（最重要）
        if consensus > 0.7:
            concept_signal += 2.0
        elif consensus > 0.6:
            concept_signal += 1.0
        elif consensus > 0.55:
            concept_signal += 0.3
        elif consensus < 0.3:
            concept_signal -= 2.0
        elif consensus < 0.4:
            concept_signal -= 1.0
        elif consensus < 0.45:
            concept_signal -= 0.3

        # 板块整体强弱信号
        if board_strength > 65:
            concept_signal += 1.5
        elif board_strength > 55:
            concept_signal += 0.5
        elif board_strength < 35:
            concept_signal -= 1.5
        elif board_strength < 45:
            concept_signal -= 0.5

        # 个股在板块中的强弱信号
        if stock_strength > 70:
            concept_signal += 1.0
        elif stock_strength > 60:
            concept_signal += 0.3
        elif stock_strength < 30:
            concept_signal -= 1.0
        elif stock_strength < 40:
            concept_signal -= 0.3

        # 板块动量信号
        if momentum > 0.5:
            concept_signal += 0.8
        elif momentum > 0.2:
            concept_signal += 0.3
        elif momentum < -0.5:
            concept_signal -= 0.8
        elif momentum < -0.2:
            concept_signal -= 0.3

        # 强势板块/个股占比信号
        if strong_board_pct > 0.6 and strong_stock_pct > 0.6:
            concept_signal += 1.0
        elif strong_board_pct < 0.3 and strong_stock_pct < 0.3:
            concept_signal -= 1.0

        # 概念信号置信度（板块数越多越可靠）
        concept_confidence = min(1.0, n_boards / 8)

    # ── 趋势自适应信号 ──
    trend_signal = tech_factors.get('trend_adaptive', 0)

    # ── 个股自适应调整 ──
    # 根据历史预测准确率动态调整概念信号权重
    adaptive_concept_weight = 0.35  # 默认概念权重
    adaptive_tech_weight = 0.45     # 默认技术权重
    adaptive_trend_weight = 0.20    # 默认趋势权重

    if stock_history:
        hist_accuracy = stock_history.get('loose_accuracy', 0.5)
        concept_hit_rate = stock_history.get('concept_hit_rate', 0.5)

        # 如果概念信号历史命中率高，增加概念权重
        if concept_hit_rate > 0.65:
            adaptive_concept_weight = 0.45
            adaptive_tech_weight = 0.35
        elif concept_hit_rate < 0.45:
            adaptive_concept_weight = 0.20
            adaptive_tech_weight = 0.55

        # 如果整体准确率低，增加均值回归权重
        if hist_accuracy < 0.55:
            adaptive_tech_weight += 0.05
            adaptive_concept_weight -= 0.05

    # ── 融合信号 ──
    z_today = tech_factors.get('z_today', 0)
    vol_regime = tech_factors.get('vol_regime', 0)

    # 波动率自适应
    if vol_regime > 0:
        adaptive_tech_weight += 0.05
        adaptive_trend_weight -= 0.05
    elif vol_regime < 0:
        adaptive_tech_weight -= 0.05
        adaptive_trend_weight += 0.05

    combined = (
        tech_signal * adaptive_tech_weight +
        concept_signal * adaptive_concept_weight * concept_confidence +
        trend_signal * adaptive_trend_weight +
        z_today * (-0.10)
    )

    # ── 方向决策 ──
    abs_combined = abs(combined)
    confidence = 'high' if abs_combined > 2.0 else ('medium' if abs_combined > 0.8 else 'low')

    # 主决策
    if combined > 0.3:
        direction = '上涨'
    elif combined < -0.3:
        direction = '下跌'
    else:
        # 模糊区：概念信号优先
        if has_concept and abs(concept_signal) > 1.0:
            direction = '上涨' if concept_signal > 0 else '下跌'
        else:
            # 兜底：均值回归
            direction = '上涨' if tech_factors.get('reversion', 0) > 0 else '下跌'

    # ── 修正层1: 概念极端信号修正 ──
    if has_concept and confidence != 'high':
        if concept_signal > 3.0 and direction == '下跌':
            direction = '上涨'  # 概念极强修正
        elif concept_signal < -3.0 and direction == '上涨':
            direction = '下跌'  # 概念极弱修正

    # ── 修正层2: 连续涨跌极端修正 ──
    if abs(z_today) > 2.5 and confidence != 'high':
        direction = '上涨' if z_today < 0 else '下跌'  # 极端均值回归

    # ── 修正层3: 星期效应（仅最强的） ──
    try:
        wd = datetime.strptime(score_date, '%Y-%m-%d').weekday()
        # 周五偏跌（统计显著）
        if wd == 4 and confidence == 'low':
            direction = '下跌'
        # 周二偏跌
        elif wd == 1 and confidence == 'low' and concept_signal < 0:
            direction = '下跌'
    except ValueError:
        pass

    return {
        '方向': direction,
        '融合信号': round(combined, 3),
        '技术信号': round(tech_signal, 3),
        '概念信号': round(concept_signal, 3),
        '趋势信号': round(trend_signal, 2),
        '概念置信度': round(concept_confidence, 2),
        '置信度': confidence,
        'z_today': round(z_today, 2),
        '技术权重': round(adaptive_tech_weight, 2),
        '概念权重': round(adaptive_concept_weight, 2),
    }


# ═══════════════════════════════════════════════════════════
# 主回测函数
# ═══════════════════════════════════════════════════════════

def run_concept_daily_backtest(
    stock_codes: list[str],
    start_date: str = '2025-12-10',
    end_date: str = '2026-03-10',
    min_kline_days: int = 120,
) -> dict:
    """概念板块增强日预测回测。

    Args:
        stock_codes: 股票代码列表（至少60只，覆盖不同概念板块）
        start_date: 回测起始日期
        end_date: 回测截止日期
        min_kline_days: 最少K线天数

    Returns:
        回测结果汇总
    """
    t_start = datetime.now()
    logger.info("开始概念板块增强日预测回测: %d只股票, %s ~ %s",
                len(stock_codes), start_date, end_date)

    # ── 1. 预加载所有数据 ──
    data = _preload_all_data(stock_codes, start_date, end_date)
    stock_klines = data['stock_klines']
    stock_boards = data['stock_boards']
    board_kline_map = data['board_kline_map']
    market_klines = data['market_klines']
    fund_flow_map = data['fund_flow_map']
    board_peer_klines = data['board_peer_klines']
    stock_strength_map = data['stock_strength_map']

    all_day_results = []
    stock_summaries = []
    concept_board_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0, 'stocks': set()})

    # ── 2. 逐股票回测 ──
    skipped = 0
    for code in stock_codes:
        klines = stock_klines.get(code, [])
        # 过滤停牌日
        klines = [k for k in klines if (k.get('trading_volume') or 0) > 0]
        if len(klines) < min_kline_days:
            logger.warning("%s K线不足(%d<%d)，跳过", code, len(klines), min_kline_days)
            skipped += 1
            continue

        # 找到回测起始索引
        start_idx = None
        for i, k in enumerate(klines):
            if k['date'] >= start_date:
                start_idx = i
                break
        if start_idx is None or start_idx < 60:
            logger.warning("%s 起始日期前数据不足，跳过", code)
            skipped += 1
            continue

        boards = stock_boards.get(code, [])
        fund_flow = fund_flow_map.get(code, [])
        strength_data = stock_strength_map.get(code, {})

        # 个股自适应历史统计（滚动窗口）
        stock_history = {'loose_accuracy': 0.5, 'concept_hit_rate': 0.5}
        rolling_results = []

        day_results = []
        for i in range(start_idx, len(klines) - 1):
            score_date = klines[i]['date']
            if score_date > end_date:
                break

            # 技术指标准备
            lookback = 120
            start_lb = max(0, i - lookback + 1)
            closes = [k['close_price'] for k in klines[start_lb:i + 1]]
            highs = [k['high_price'] for k in klines[start_lb:i + 1]]
            lows = [k['low_price'] for k in klines[start_lb:i + 1]]
            n = len(closes)

            if n < 35:
                continue

            macd_list = _calc_macd(closes)
            kdj_list = _calc_kdj(highs, lows, closes)
            boll_list = _calc_boll(closes)

            if not macd_list or len(macd_list) < n:
                continue

            # 技术因子
            tech_factors = _compute_technical_factors(
                klines, i, closes, highs, lows,
                macd_list, kdj_list, boll_list, n,
                market_klines, fund_flow, score_date,
            )

            # 概念板块信号
            concept_signals = _compute_concept_signals(
                code, score_date, boards,
                board_kline_map, market_klines, board_peer_klines,
                stock_strength_data=strength_data,
            )

            # 方向决策
            decision = _decide_direction_with_concept(
                tech_factors, concept_signals,
                klines, i, score_date, stock_history,
            )
            pred_direction = decision['方向']

            # T+1 实际涨跌
            base_close = klines[i]['close_price']
            next_day = klines[i + 1]
            if base_close <= 0:
                continue

            actual_chg = round((next_day['close_price'] - base_close) / base_close * 100, 2)
            if actual_chg > 0.3:
                actual_dir = '上涨'
            elif actual_chg < -0.3:
                actual_dir = '下跌'
            else:
                actual_dir = '横盘震荡'

            dir_ok = (pred_direction == actual_dir)
            loose_ok = dir_ok
            if not dir_ok:
                if pred_direction == '上涨' and actual_chg >= 0:
                    loose_ok = True
                elif pred_direction == '下跌' and actual_chg <= 0:
                    loose_ok = True

            # 概念信号是否命中
            concept_hit = False
            if concept_signals.get('has_concept'):
                cs = decision.get('概念信号', 0)
                if (cs > 0 and actual_chg >= 0) or (cs < 0 and actual_chg <= 0):
                    concept_hit = True

            # 更新滚动历史
            rolling_results.append({'loose_ok': loose_ok, 'concept_hit': concept_hit})
            if len(rolling_results) > 20:
                rolling_results = rolling_results[-20:]
            if len(rolling_results) >= 5:
                stock_history['loose_accuracy'] = (
                    sum(1 for r in rolling_results if r['loose_ok']) / len(rolling_results)
                )
                concept_hits = [r for r in rolling_results if r.get('concept_hit') is not None]
                if concept_hits:
                    stock_history['concept_hit_rate'] = (
                        sum(1 for r in concept_hits if r['concept_hit']) / len(concept_hits)
                    )

            # 记录主要概念板块名称
            main_boards = [b['board_name'] for b in boards[:3]] if boards else []

            day_results.append({
                'stock_code': code,
                'score_date': score_date,
                'next_date': next_day['date'],
                'pred_direction': pred_direction,
                'actual_change_pct': actual_chg,
                'actual_direction': actual_dir,
                'direction_correct': dir_ok,
                'direction_loose_correct': loose_ok,
                'decision': decision,
                'concept_boards': main_boards,
                'has_concept': concept_signals.get('has_concept', False),
            })

            # 按概念板块统计
            for board in boards[:5]:
                bn = board['board_name']
                concept_board_stats[bn]['n'] += 1
                if dir_ok:
                    concept_board_stats[bn]['ok'] += 1
                if loose_ok:
                    concept_board_stats[bn]['loose_ok'] += 1
                concept_board_stats[bn]['stocks'].add(code)

        all_day_results.extend(day_results)

        if day_results:
            n_days = len(day_results)
            l_ok = sum(1 for r in day_results if r['direction_loose_correct'])
            d_ok = sum(1 for r in day_results if r['direction_correct'])
            stock_summaries.append({
                '股票代码': code,
                '概念板块': ', '.join([b['board_name'] for b in boards[:3]]),
                '回测天数': n_days,
                '准确率(宽松)': f'{l_ok}/{n_days} ({round(l_ok / n_days * 100, 1)}%)',
                '准确率(严格)': f'{d_ok}/{n_days} ({round(d_ok / n_days * 100, 1)}%)',
            })
            logger.info("%s [%s] %d天 宽松%.1f%% 严格%.1f%%",
                        code, ', '.join([b['board_name'] for b in boards[:2]]),
                        n_days, l_ok / n_days * 100, d_ok / n_days * 100)

    elapsed = (datetime.now() - t_start).total_seconds()

    if not all_day_results:
        return {'状态': '无有效回测数据', '耗时(秒)': round(elapsed, 1), '跳过股票数': skipped}

    # ── 3. 汇总统计 ──
    return _build_result_summary(
        all_day_results, stock_summaries, concept_board_stats,
        stock_codes, elapsed, start_date, end_date, skipped,
    )


def _build_result_summary(all_day_results, stock_summaries, concept_board_stats,
                          stock_codes, elapsed, start_date, end_date, skipped) -> dict:
    """构建回测结果汇总。"""
    total_n = len(all_day_results)
    total_ok = sum(1 for r in all_day_results if r['direction_correct'])
    total_loose = sum(1 for r in all_day_results if r['direction_loose_correct'])

    # 按预测方向统计
    pred_dir_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0})
    for r in all_day_results:
        pd = r['pred_direction']
        pred_dir_stats[pd]['n'] += 1
        if r['direction_correct']:
            pred_dir_stats[pd]['ok'] += 1
        if r['direction_loose_correct']:
            pred_dir_stats[pd]['loose_ok'] += 1

    pred_dir_summary = {}
    for pd in ['上涨', '下跌']:
        d = pred_dir_stats.get(pd, {'ok': 0, 'n': 0, 'loose_ok': 0})
        pred_dir_summary[pd] = {
            '样本数': d['n'],
            '准确率(宽松)': _rate_str(d['loose_ok'], d['n']),
            '准确率(严格)': _rate_str(d['ok'], d['n']),
        }

    # 有概念 vs 无概念对比
    with_concept = [r for r in all_day_results if r.get('has_concept')]
    without_concept = [r for r in all_day_results if not r.get('has_concept')]
    concept_comparison = {
        '有概念板块数据': {
            '样本数': len(with_concept),
            '准确率(宽松)': _rate_str(
                sum(1 for r in with_concept if r['direction_loose_correct']),
                len(with_concept)),
        },
        '无概念板块数据': {
            '样本数': len(without_concept),
            '准确率(宽松)': _rate_str(
                sum(1 for r in without_concept if r['direction_loose_correct']),
                len(without_concept)),
        },
    }

    # 按置信度统计
    confidence_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0})
    for r in all_day_results:
        conf = r.get('decision', {}).get('置信度', 'low')
        confidence_stats[conf]['n'] += 1
        if r['direction_correct']:
            confidence_stats[conf]['ok'] += 1
        if r['direction_loose_correct']:
            confidence_stats[conf]['loose_ok'] += 1

    confidence_summary = {}
    for conf in ['high', 'medium', 'low']:
        d = confidence_stats.get(conf, {'ok': 0, 'n': 0, 'loose_ok': 0})
        confidence_summary[conf] = {
            '样本数': d['n'],
            '准确率(宽松)': _rate_str(d['loose_ok'], d['n']),
            '准确率(严格)': _rate_str(d['ok'], d['n']),
        }

    # 按概念板块统计（取样本数最多的前20个）
    board_summary = {}
    sorted_boards = sorted(concept_board_stats.items(),
                           key=lambda x: x[1]['n'], reverse=True)[:20]
    for bn, stats in sorted_boards:
        board_summary[bn] = {
            '股票数': len(stats['stocks']),
            '样本数': stats['n'],
            '准确率(宽松)': _rate_str(stats['loose_ok'], stats['n']),
            '准确率(严格)': _rate_str(stats['ok'], stats['n']),
        }

    # 按个股准确率排序
    stock_summaries_sorted = sorted(
        stock_summaries,
        key=lambda x: float(x['准确率(宽松)'].split('(')[1].replace('%)', '')),
        reverse=True,
    )

    # 逐日详情（精简版，只保留前200条）
    detail_list = []
    for r in sorted(all_day_results, key=lambda x: (x['stock_code'], x['score_date']))[:200]:
        detail_list.append({
            '代码': r['stock_code'],
            '评分日': r['score_date'],
            '预测方向': r['pred_direction'],
            '实际涨跌': f"{r['actual_change_pct']:+.2f}%",
            '宽松正确': '✓' if r['direction_loose_correct'] else '✗',
            '融合信号': r['decision']['融合信号'],
            '概念信号': r['decision']['概念信号'],
            '技术信号': r['decision']['技术信号'],
            '置信度': r['decision']['置信度'],
            '概念板块': ', '.join(r.get('concept_boards', [])[:2]),
        })

    return {
        '回测类型': '概念板块增强日预测回测 v1',
        '回测时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        '耗时(秒)': round(elapsed, 1),
        '回测区间': f'{start_date} ~ {end_date}',
        '评判模式': '宽松模式（预测上涨→实际≥0%即正确，预测下跌→实际≤0%即正确）',
        '股票数': len(stock_codes),
        '有效股票数': len(stock_codes) - skipped,
        '跳过股票数': skipped,
        '总样本数': total_n,
        '总体准确率(宽松)': _rate_str(total_loose, total_n),
        '总体准确率(严格)': _rate_str(total_ok, total_n),
        '按预测方向统计': pred_dir_summary,
        '概念板块效果对比': concept_comparison,
        '置信度分析': confidence_summary,
        '按概念板块统计(Top20)': board_summary,
        '各股票汇总(按准确率排序)': stock_summaries_sorted,
        '逐日详情(前200条)': detail_list,
    }
