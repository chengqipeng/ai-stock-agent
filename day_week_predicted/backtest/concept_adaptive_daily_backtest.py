#!/usr/bin/env python3
"""
概念板块自适应日预测回测引擎 v2

核心改进（相比 concept_daily_prediction_backtest v1）：
1. 概念板块走势强弱深度融合：板块动量、板块vs大盘超额、板块趋势一致性
2. 个股在概念板块中的相对强弱：排名百分位、超额收益趋势、胜率
3. 个股自适应算法：根据每只股票的历史预测表现动态调整因子权重
4. 概念板块间交叉验证：多板块信号一致性增强置信度
5. 动态阈值：根据近期市场波动率自适应调整决策阈值

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


def _std(lst):
    if len(lst) < 2:
        return 0.0
    m = _mean(lst)
    return math.sqrt(sum((x - m) ** 2 for x in lst) / len(lst))


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
                f"SELECT board_code, `date`, change_percent, close_price, "
                f"trading_volume, trading_amount "
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

        # 6. 个股板块内强弱评分
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
        'stock_strength_map': dict(stock_strength_map),
    }


# ═══════════════════════════════════════════════════════════
# 概念板块信号计算（深度版）
# ═══════════════════════════════════════════════════════════

def _compute_board_strength_deep(board_klines: list[dict], market_klines: list[dict],
                                  score_date: str) -> dict | None:
    """深度计算概念板块相对大盘的强弱势。

    多维度评分：
    - 超额收益（5日/10日/20日）
    - 动量趋势（加速/减速）
    - 胜率（跑赢大盘天数占比）
    - 波动率对比
    """
    bk = [k for k in board_klines if k['date'] <= score_date]
    mk_map = {k['date']: k['change_percent'] for k in market_klines
              if k['date'] <= score_date}
    if len(bk) < 10:
        return None

    # 取最近30天对齐数据
    recent = bk[-30:]
    aligned = [(k, mk_map.get(k['date'])) for k in recent if k['date'] in mk_map]
    if len(aligned) < 8:
        return None

    daily_excess = [k['change_percent'] - mk for k, mk in aligned]
    board_rets = [k['change_percent'] for k, _ in aligned]
    n = len(aligned)

    # 多周期超额收益
    excess_5d = sum(daily_excess[-min(5, n):])
    excess_10d = sum(daily_excess[-min(10, n):])
    excess_20d = sum(daily_excess[-min(20, n):])

    # 动量加速度：近5日超额 vs 前5日超额
    if n >= 10:
        recent_5 = sum(daily_excess[-5:])
        prev_5 = sum(daily_excess[-10:-5])
        momentum_accel = recent_5 - prev_5
    else:
        momentum_accel = 0.0

    # 胜率
    win_days = sum(1 for e in daily_excess if e > 0)
    win_rate = win_days / n

    # 近5日胜率（短期趋势更重要）
    recent_5_excess = daily_excess[-min(5, n):]
    win_rate_5d = sum(1 for e in recent_5_excess if e > 0) / len(recent_5_excess)

    # 板块动量
    momentum_5d = _mean(board_rets[-min(5, n):])
    momentum_10d = _mean(board_rets[-min(10, n):])

    # 板块波动率
    board_vol = _std(board_rets[-min(20, n):])

    # 综合评分（0-100）
    s1 = _sigmoid(excess_20d, center=0, scale=6) * 25
    s2 = _sigmoid(excess_5d, center=0, scale=2) * 25
    s3 = _sigmoid(momentum_accel, center=0, scale=1.5) * 15
    s4 = max(0, min(20, (win_rate - 0.3) / 0.4 * 20))
    s5 = max(0, min(15, (win_rate_5d - 0.2) / 0.6 * 15))
    score = round(max(0, min(100, s1 + s2 + s3 + s4 + s5)), 1)

    return {
        'score': score,
        'excess_5d': round(excess_5d, 3),
        'excess_10d': round(excess_10d, 3),
        'excess_20d': round(excess_20d, 3),
        'momentum_accel': round(momentum_accel, 3),
        'win_rate': round(win_rate, 4),
        'win_rate_5d': round(win_rate_5d, 4),
        'momentum_5d': round(momentum_5d, 4),
        'momentum_10d': round(momentum_10d, 4),
        'board_vol': round(board_vol, 4),
    }


def _compute_stock_in_board_deep(stock_klines: list[dict], board_klines: list[dict],
                                  score_date: str, strength_data: dict = None) -> dict | None:
    """深度计算个股在概念板块中的相对强弱势。

    维度：
    - 个股vs板块超额收益（5日/10日/20日）
    - 个股动量 vs 板块动量
    - 预计算的strength_score
    - 个股波动率 vs 板块波动率
    """
    sk_map = {k['date']: k['change_percent'] for k in stock_klines if k['date'] <= score_date}
    bk = [k for k in board_klines if k['date'] <= score_date]
    if len(bk) < 8:
        return None

    recent = bk[-30:]
    aligned = []
    for k in recent:
        d = k['date']
        if d in sk_map:
            aligned.append((sk_map[d], k['change_percent']))
    if len(aligned) < 5:
        return None

    daily_excess = [s - b for s, b in aligned]
    stock_rets = [s for s, _ in aligned]
    board_rets = [b for _, b in aligned]
    n = len(aligned)

    excess_5d = sum(daily_excess[-min(5, n):])
    excess_10d = sum(daily_excess[-min(10, n):])
    excess_20d = sum(daily_excess[-min(20, n):])

    win_days = sum(1 for e in daily_excess if e > 0)
    win_rate = win_days / n

    # 个股动量 vs 板块动量
    stock_mom_5d = _mean(stock_rets[-min(5, n):])
    board_mom_5d = _mean(board_rets[-min(5, n):])
    relative_momentum = stock_mom_5d - board_mom_5d

    # 个股波动率 vs 板块波动率
    stock_vol = _std(stock_rets[-min(20, n):])
    board_vol = _std(board_rets[-min(20, n):])
    vol_ratio = stock_vol / board_vol if board_vol > 0.1 else 1.0

    # 使用预计算的strength_score
    pre_score = 50.0
    pre_level = '中性'
    if strength_data:
        pre_score = strength_data.get('strength_score', 50.0)
        pre_level = strength_data.get('strength_level', '中性')

    # 综合评分
    s1 = _sigmoid(excess_20d, center=0, scale=5) * 30
    s2 = _sigmoid(excess_5d, center=0, scale=2) * 25
    s3 = _sigmoid(relative_momentum, center=0, scale=1) * 20
    s4 = max(0, min(25, (win_rate - 0.3) / 0.4 * 25))
    score = round(max(0, min(100, s1 + s2 + s3 + s4)), 1)

    # 融合预计算评分
    final_score = round(score * 0.6 + pre_score * 0.4, 1)

    return {
        'score': final_score,
        'excess_5d': round(excess_5d, 3),
        'excess_10d': round(excess_10d, 3),
        'excess_20d': round(excess_20d, 3),
        'win_rate': round(win_rate, 4),
        'relative_momentum': round(relative_momentum, 4),
        'vol_ratio': round(vol_ratio, 3),
        'pre_score': pre_score,
        'pre_level': pre_level,
    }


def _compute_concept_signals_deep(stock_code: str, score_date: str,
                                   stock_klines: list[dict],
                                   stock_boards: list[dict],
                                   board_kline_map: dict,
                                   market_klines: list[dict],
                                   stock_strength_data: dict = None) -> dict:
    """深度计算个股在某日期的全部概念板块信号。"""
    board_strengths = []
    stock_in_board_scores = []
    board_momentums = []
    board_excess_5d_list = []
    board_win_rates = []
    stock_excess_5d_list = []
    stock_win_rates = []
    boards_up = 0
    boards_total = 0
    strong_boards = 0
    weak_boards = 0

    for board in stock_boards:
        bc = board['board_code']
        bk = board_kline_map.get(bc, [])
        if not bk:
            continue

        valid_bk = [k for k in bk if k['date'] <= score_date]
        if len(valid_bk) < 8:
            continue

        boards_total += 1

        # 板块整体强弱（深度版）
        bs = _compute_board_strength_deep(bk, market_klines, score_date)
        if bs:
            board_strengths.append(bs['score'])
            board_momentums.append(bs['momentum_5d'])
            board_excess_5d_list.append(bs['excess_5d'])
            board_win_rates.append(bs['win_rate'])
            if bs['score'] > 60:
                strong_boards += 1
            elif bs['score'] < 40:
                weak_boards += 1
            if bs['momentum_5d'] > 0:
                boards_up += 1

        # 个股在板块中的强弱（深度版）
        strength_for_board = (stock_strength_data or {}).get(bc)
        sib = _compute_stock_in_board_deep(
            stock_klines, bk, score_date, strength_for_board
        )
        if sib:
            stock_in_board_scores.append(sib['score'])
            stock_excess_5d_list.append(sib['excess_5d'])
            stock_win_rates.append(sib['win_rate'])

    if boards_total == 0:
        return {'has_concept': False}

    consensus = boards_up / boards_total
    avg_board_strength = _mean(board_strengths) if board_strengths else 50.0
    avg_stock_strength = _mean(stock_in_board_scores) if stock_in_board_scores else 50.0
    avg_momentum = _mean(board_momentums) if board_momentums else 0.0

    # 强势板块占比
    strong_board_pct = strong_boards / boards_total if boards_total > 0 else 0.5
    weak_board_pct = weak_boards / boards_total if boards_total > 0 else 0.5

    # 板块超额收益一致性（标准差越小越一致）
    board_excess_consistency = 1.0 / (1.0 + _std(board_excess_5d_list)) if board_excess_5d_list else 0.5

    # 个股超额收益方向一致性
    if stock_excess_5d_list:
        pos_excess = sum(1 for e in stock_excess_5d_list if e > 0)
        stock_excess_consensus = pos_excess / len(stock_excess_5d_list)
    else:
        stock_excess_consensus = 0.5

    # 板块动量方向一致性
    if board_momentums:
        pos_mom = sum(1 for m in board_momentums if m > 0)
        momentum_consensus = pos_mom / len(board_momentums)
    else:
        momentum_consensus = 0.5

    return {
        'has_concept': True,
        'n_boards': boards_total,
        'consensus': consensus,
        'avg_board_strength': avg_board_strength,
        'avg_stock_strength': avg_stock_strength,
        'avg_momentum': avg_momentum,
        'strong_board_pct': strong_board_pct,
        'weak_board_pct': weak_board_pct,
        'board_excess_consistency': board_excess_consistency,
        'stock_excess_consensus': stock_excess_consensus,
        'momentum_consensus': momentum_consensus,
        'avg_board_excess_5d': _mean(board_excess_5d_list) if board_excess_5d_list else 0.0,
        'avg_stock_excess_5d': _mean(stock_excess_5d_list) if stock_excess_5d_list else 0.0,
        'avg_board_win_rate': _mean(board_win_rates) if board_win_rates else 0.5,
        'avg_stock_win_rate': _mean(stock_win_rates) if stock_win_rates else 0.5,
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
        std_val = math.sqrt(sum((x - mid) ** 2 for x in window) / period)
        result[i] = {'上轨': mid + mult * std_val, '中轨': mid, '下轨': mid - mult * std_val}
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

    # 因子1: 均值回归（多周期）
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

    # 因子13: 近10日涨跌比 → 趋势自适应
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

    # 因子14: MA5/MA20交叉
    ma_cross = 0.0
    if n >= 20:
        ma5 = _mean(closes[-5:])
        ma20 = _mean(closes[-20:])
        ma5_prev = _mean(closes[-6:-1]) if n >= 6 else ma5
        ma20_prev = _mean(closes[-21:-1]) if n >= 21 else ma20
        if ma5_prev <= ma20_prev and ma5 > ma20:
            ma_cross = 1.5  # 金叉
        elif ma5_prev >= ma20_prev and ma5 < ma20:
            ma_cross = -1.5  # 死叉
        elif ma5 > ma20:
            ma_cross = 0.3  # 多头排列
        elif ma5 < ma20:
            ma_cross = -0.3  # 空头排列

    return {
        'reversion': reversion, 'rsi': rsi_score, 'kdj': kdj_score,
        'macd': macd_score, 'boll': boll_score, 'vp': vp_score,
        'market': market_score, 'streak': streak_score,
        'vol_regime': vol_regime, 'gap_signal': gap_signal,
        'intraday_pos': intraday_pos, 'db_fund': db_fund_signal,
        'trend_adaptive': trend_adaptive, 'ma_cross': ma_cross,
        'up_ratio_10d': up_ratio_10d, 'z_today': z_today,
        'vol_std': vol_std, 'chg_today': chg_today, 'vol_ratio': vol_ratio,
        'rsi_14': rsi_14, 'up_streak': up_streak, 'down_streak': down_streak,
    }


# ═══════════════════════════════════════════════════════════
# 个股自适应权重管理器
# ═══════════════════════════════════════════════════════════

class StockAdaptiveWeights:
    """个股自适应权重管理器。

    根据每只股票的历史预测表现，动态调整各因子权重。
    核心思路：
    - 跟踪每个因子的历史命中率
    - 命中率高的因子增加权重，低的减少
    - 概念信号和技术信号分别跟踪
    - 使用滚动窗口避免过拟合
    """

    def __init__(self, window_size: int = 30):
        self.window_size = window_size
        self.history = []  # [{factor_signals, concept_signal, actual_dir, ...}]

        # 默认权重
        self.tech_weight = 0.40
        self.concept_weight = 0.35
        self.trend_weight = 0.15
        self.adaptive_weight = 0.10

        # 因子级别权重
        self.factor_weights = {
            'reversion': 1.2, 'rsi': 1.0, 'kdj': 0.3, 'macd': 0.3,
            'boll': 0.5, 'vp': 0.8, 'market': 1.2, 'streak': 0.8,
            'vol_regime': 0.8, 'gap_signal': 0.8, 'intraday_pos': 0.6,
            'db_fund': 0.5, 'ma_cross': 0.5,
        }

        # 概念子信号权重
        self.concept_sub_weights = {
            'consensus': 2.0,
            'board_strength': 1.5,
            'stock_strength': 1.0,
            'momentum': 0.8,
            'board_excess': 1.0,
            'stock_excess': 0.8,
            'consistency': 0.5,
        }

    def record(self, tech_factors: dict, concept_signal: float,
               concept_sub_signals: dict, pred_dir: str, actual_chg: float):
        """记录一次预测结果。"""
        actual_up = actual_chg >= 0
        pred_up = pred_dir == '上涨'
        loose_ok = (pred_up and actual_up) or (not pred_up and not actual_up)

        # 记录各因子方向是否与实际一致
        factor_hits = {}
        for fname, fval in tech_factors.items():
            if fname in self.factor_weights and fval != 0:
                factor_up = fval > 0
                factor_hits[fname] = (factor_up == actual_up)

        concept_hit = False
        if concept_signal != 0:
            concept_up = concept_signal > 0
            concept_hit = (concept_up == actual_up)

        self.history.append({
            'loose_ok': loose_ok,
            'factor_hits': factor_hits,
            'concept_hit': concept_hit,
            'concept_sub_signals': concept_sub_signals,
            'actual_up': actual_up,
        })

        # 保持窗口大小
        if len(self.history) > self.window_size:
            self.history = self.history[-self.window_size:]

        # 更新权重
        self._update_weights()

    def _update_weights(self):
        """根据历史表现更新权重。"""
        if len(self.history) < 8:
            return

        # 计算整体准确率
        loose_acc = sum(1 for h in self.history if h['loose_ok']) / len(self.history)

        # 计算概念信号命中率
        concept_records = [h for h in self.history if h.get('concept_hit') is not None]
        if concept_records:
            concept_hit_rate = sum(1 for h in concept_records if h['concept_hit']) / len(concept_records)
        else:
            concept_hit_rate = 0.5

        # 动态调整概念 vs 技术权重
        if concept_hit_rate > 0.60:
            self.concept_weight = min(0.50, 0.35 + (concept_hit_rate - 0.60) * 0.5)
            self.tech_weight = max(0.30, 0.40 - (concept_hit_rate - 0.60) * 0.3)
        elif concept_hit_rate < 0.45:
            self.concept_weight = max(0.15, 0.35 - (0.45 - concept_hit_rate) * 0.5)
            self.tech_weight = min(0.55, 0.40 + (0.45 - concept_hit_rate) * 0.3)

        # 更新因子级别权重
        for fname in self.factor_weights:
            hits = [h['factor_hits'].get(fname) for h in self.history
                    if fname in h.get('factor_hits', {})]
            if len(hits) >= 5:
                hit_rate = sum(1 for h in hits if h) / len(hits)
                # 有效因子增强，无效因子减弱
                if hit_rate > 0.58:
                    self.factor_weights[fname] = min(2.0, self.factor_weights[fname] * 1.05)
                elif hit_rate < 0.42:
                    # 反转使用
                    self.factor_weights[fname] = max(-1.5, self.factor_weights[fname] * 0.9 - 0.1)
                elif 0.48 <= hit_rate <= 0.52:
                    # 噪声因子，权重趋零
                    self.factor_weights[fname] *= 0.95

    def get_weights(self) -> dict:
        """获取当前权重配置。"""
        return {
            'tech_weight': self.tech_weight,
            'concept_weight': self.concept_weight,
            'trend_weight': self.trend_weight,
            'adaptive_weight': self.adaptive_weight,
            'factor_weights': dict(self.factor_weights),
        }

    def get_accuracy(self) -> float:
        if not self.history:
            return 0.5
        return sum(1 for h in self.history if h['loose_ok']) / len(self.history)


# ═══════════════════════════════════════════════════════════
# 概念板块增强方向决策（核心算法 v2）
# ═══════════════════════════════════════════════════════════

def _decide_direction_v2(tech_factors: dict, concept_signals: dict,
                         klines: list[dict], end_idx: int,
                         score_date: str,
                         adaptive: StockAdaptiveWeights) -> dict:
    """融合技术因子 + 概念板块信号的方向决策 v2。

    核心改进：
    1. 概念板块信号更细粒度（板块强弱、个股强弱、一致性）
    2. 个股自适应权重
    3. 动态阈值
    4. 多层修正机制
    """
    weights = adaptive.get_weights()
    fw = weights['factor_weights']

    # ── 技术信号加权 ──
    tech_signal = sum(tech_factors.get(k, 0) * fw.get(k, 0.5) for k in fw)

    # ── 概念板块信号（深度版） ──
    concept_signal = 0.0
    concept_confidence = 0.0
    concept_sub = {}
    has_concept = concept_signals.get('has_concept', False)

    if has_concept:
        consensus = concept_signals['consensus']
        board_strength = concept_signals['avg_board_strength']
        stock_strength = concept_signals['avg_stock_strength']
        momentum = concept_signals['avg_momentum']
        strong_board_pct = concept_signals['strong_board_pct']
        weak_board_pct = concept_signals['weak_board_pct']
        n_boards = concept_signals['n_boards']
        board_excess_consistency = concept_signals['board_excess_consistency']
        stock_excess_consensus = concept_signals['stock_excess_consensus']
        momentum_consensus = concept_signals['momentum_consensus']
        avg_board_excess_5d = concept_signals['avg_board_excess_5d']
        avg_stock_excess_5d = concept_signals['avg_stock_excess_5d']
        avg_board_win_rate = concept_signals['avg_board_win_rate']
        avg_stock_win_rate = concept_signals['avg_stock_win_rate']

        csw = adaptive.concept_sub_weights

        # 1. 概念板块共识度信号（最重要）
        consensus_sig = 0.0
        if consensus > 0.75:
            consensus_sig = 3.0
        elif consensus > 0.65:
            consensus_sig = 2.0
        elif consensus > 0.55:
            consensus_sig = 0.8
        elif consensus < 0.25:
            consensus_sig = -3.0
        elif consensus < 0.35:
            consensus_sig = -2.0
        elif consensus < 0.45:
            consensus_sig = -0.8
        concept_signal += consensus_sig * csw['consensus']
        concept_sub['consensus'] = consensus_sig

        # 2. 板块整体强弱信号
        board_str_sig = 0.0
        if board_strength > 70:
            board_str_sig = 2.0
        elif board_strength > 60:
            board_str_sig = 1.0
        elif board_strength > 55:
            board_str_sig = 0.3
        elif board_strength < 30:
            board_str_sig = -2.0
        elif board_strength < 40:
            board_str_sig = -1.0
        elif board_strength < 45:
            board_str_sig = -0.3
        concept_signal += board_str_sig * csw['board_strength']
        concept_sub['board_strength'] = board_str_sig

        # 3. 个股在板块中的强弱信号
        stock_str_sig = 0.0
        if stock_strength > 75:
            stock_str_sig = 1.5
        elif stock_strength > 60:
            stock_str_sig = 0.5
        elif stock_strength < 25:
            stock_str_sig = -1.5
        elif stock_strength < 40:
            stock_str_sig = -0.5
        concept_signal += stock_str_sig * csw['stock_strength']
        concept_sub['stock_strength'] = stock_str_sig

        # 4. 板块动量信号
        mom_sig = 0.0
        if momentum > 0.8:
            mom_sig = 1.5
        elif momentum > 0.3:
            mom_sig = 0.5
        elif momentum < -0.8:
            mom_sig = -1.5
        elif momentum < -0.3:
            mom_sig = -0.5
        concept_signal += mom_sig * csw['momentum']
        concept_sub['momentum'] = mom_sig

        # 5. 板块超额收益信号
        board_excess_sig = 0.0
        if avg_board_excess_5d > 2.0:
            board_excess_sig = 1.5
        elif avg_board_excess_5d > 0.5:
            board_excess_sig = 0.5
        elif avg_board_excess_5d < -2.0:
            board_excess_sig = -1.5
        elif avg_board_excess_5d < -0.5:
            board_excess_sig = -0.5
        concept_signal += board_excess_sig * csw['board_excess']
        concept_sub['board_excess'] = board_excess_sig

        # 6. 个股超额收益信号
        stock_excess_sig = 0.0
        if avg_stock_excess_5d > 2.0:
            stock_excess_sig = 1.0
        elif avg_stock_excess_5d > 0.5:
            stock_excess_sig = 0.3
        elif avg_stock_excess_5d < -2.0:
            stock_excess_sig = -1.0
        elif avg_stock_excess_5d < -0.5:
            stock_excess_sig = -0.3
        concept_signal += stock_excess_sig * csw['stock_excess']
        concept_sub['stock_excess'] = stock_excess_sig

        # 7. 一致性增强
        consistency_sig = 0.0
        if board_excess_consistency > 0.7 and momentum_consensus > 0.7:
            consistency_sig = 1.0 if consensus > 0.5 else -1.0
        elif board_excess_consistency > 0.6 and momentum_consensus > 0.6:
            consistency_sig = 0.5 if consensus > 0.5 else -0.5
        concept_signal += consistency_sig * csw['consistency']
        concept_sub['consistency'] = consistency_sig

        # 8. 强弱板块占比差异
        if strong_board_pct > 0.6 and weak_board_pct < 0.2:
            concept_signal += 1.0
        elif weak_board_pct > 0.6 and strong_board_pct < 0.2:
            concept_signal -= 1.0

        # 概念信号置信度
        concept_confidence = min(1.0, n_boards / 6) * (0.5 + 0.5 * board_excess_consistency)

    # ── 趋势自适应信号 ──
    trend_signal = tech_factors.get('trend_adaptive', 0)
    ma_cross = tech_factors.get('ma_cross', 0)
    trend_combined = trend_signal * 0.6 + ma_cross * 0.4

    # ── 自适应历史偏向 ──
    # 如果近期预测准确率高，维持当前策略；低则增加均值回归权重
    hist_acc = adaptive.get_accuracy()
    adaptive_bias = 0.0
    if hist_acc < 0.50:
        # 准确率低，增强均值回归
        adaptive_bias = tech_factors.get('reversion', 0) * 0.3
    elif hist_acc > 0.70:
        # 准确率高，维持当前策略方向
        adaptive_bias = 0.0

    # ── 融合信号 ──
    z_today = tech_factors.get('z_today', 0)
    vol_regime = tech_factors.get('vol_regime', 0)

    # 波动率自适应阈值
    vol_std = tech_factors.get('vol_std', 2.0)
    dynamic_threshold = max(0.15, min(0.5, 0.3 * (vol_std / 2.0)))

    tw = weights['tech_weight']
    cw = weights['concept_weight']
    trw = weights['trend_weight']
    aw = weights['adaptive_weight']

    combined = (
        tech_signal * tw +
        concept_signal * cw * concept_confidence +
        trend_combined * trw +
        adaptive_bias * aw +
        z_today * (-0.08)
    )

    # ── 方向决策 ──
    abs_combined = abs(combined)
    confidence = 'high' if abs_combined > 2.5 else ('medium' if abs_combined > 1.0 else 'low')

    # 主决策
    if combined > dynamic_threshold:
        direction = '上涨'
    elif combined < -dynamic_threshold:
        direction = '下跌'
    else:
        # 模糊区决策优先级：概念信号 > 均值回归 > 市场偏向
        if has_concept and abs(concept_signal) > 1.5:
            direction = '上涨' if concept_signal > 0 else '下跌'
        elif abs(tech_factors.get('reversion', 0)) > 1.0:
            direction = '上涨' if tech_factors['reversion'] > 0 else '下跌'
        else:
            # 兜底：市场微涨偏向（A股统计上涨天数略多于下跌）
            direction = '上涨'

    # ── 修正层1: 概念极端信号修正 ──
    if has_concept and confidence != 'high':
        if concept_signal > 4.0 and direction == '下跌':
            direction = '上涨'
        elif concept_signal < -4.0 and direction == '上涨':
            direction = '下跌'

    # ── 修正层2: 连续涨跌极端修正 ──
    up_streak = tech_factors.get('up_streak', 0)
    down_streak = tech_factors.get('down_streak', 0)
    if (up_streak >= 4 or z_today > 2.5) and confidence != 'high':
        direction = '下跌'
    elif (down_streak >= 4 or z_today < -2.5) and confidence != 'high':
        direction = '上涨'

    # ── 修正层3: 概念板块+技术双重确认 ──
    if has_concept and confidence == 'medium':
        tech_dir = '上涨' if tech_signal > 0 else '下跌'
        concept_dir = '上涨' if concept_signal > 0 else '下跌'
        if tech_dir == concept_dir and tech_dir != direction:
            # 技术和概念一致但与当前决策不同，修正
            direction = tech_dir

    # ── 修正层4: 星期效应（仅最强的） ──
    try:
        wd = datetime.strptime(score_date, '%Y-%m-%d').weekday()
        if wd == 4 and confidence == 'low':
            direction = '下跌'
        elif wd == 1 and confidence == 'low' and concept_signal < 0:
            direction = '下跌'
    except ValueError:
        pass

    return {
        '方向': direction,
        '融合信号': round(combined, 3),
        '技术信号': round(tech_signal, 3),
        '概念信号': round(concept_signal, 3),
        '趋势信号': round(trend_combined, 2),
        '概念置信度': round(concept_confidence, 2),
        '置信度': confidence,
        'z_today': round(z_today, 2),
        '技术权重': round(tw, 3),
        '概念权重': round(cw, 3),
        '动态阈值': round(dynamic_threshold, 3),
        '历史准确率': round(hist_acc, 3),
    }


# ═══════════════════════════════════════════════════════════
# 主回测函数
# ═══════════════════════════════════════════════════════════

def run_concept_adaptive_backtest(
    stock_codes: list[str],
    start_date: str = '2025-12-10',
    end_date: str = '2026-03-10',
    min_kline_days: int = 100,
    preloaded_data: dict = None,
) -> dict:
    """概念板块自适应日预测回测 v2。

    Args:
        stock_codes: 股票代码列表（至少60只，覆盖不同概念板块）
        start_date: 回测起始日期
        end_date: 回测截止日期
        min_kline_days: 最少K线天数
        preloaded_data: 预加载的数据（可选，用于模拟模式或外部数据注入）

    Returns:
        回测结果汇总
    """
    t_start = datetime.now()
    logger.info("开始概念板块自适应日预测回测 v2: %d只股票, %s ~ %s",
                len(stock_codes), start_date, end_date)

    # ── 1. 预加载所有数据 ──
    if preloaded_data:
        data = preloaded_data
        logger.info("[数据] 使用预加载数据")
    else:
        data = _preload_all_data(stock_codes, start_date, end_date)
    stock_klines = data['stock_klines']
    stock_boards = data['stock_boards']
    board_kline_map = data['board_kline_map']
    market_klines = data['market_klines']
    fund_flow_map = data['fund_flow_map']
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

        # 个股自适应权重管理器
        adaptive = StockAdaptiveWeights(window_size=30)

        day_results = []
        for i in range(start_idx, len(klines) - 1):
            score_date_val = klines[i]['date']
            if score_date_val > end_date:
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
                market_klines, fund_flow, score_date_val,
            )

            # 概念板块信号（深度版）
            concept_signals = _compute_concept_signals_deep(
                code, score_date_val, klines[start_lb:i + 1],
                boards, board_kline_map, market_klines,
                stock_strength_data=strength_data,
            )

            # 方向决策 v2
            decision = _decide_direction_v2(
                tech_factors, concept_signals,
                klines, i, score_date_val, adaptive,
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

            # 更新自适应权重
            concept_sub = {}
            if concept_signals.get('has_concept'):
                concept_sub = {
                    'consensus': concept_signals.get('consensus', 0.5),
                    'board_strength': concept_signals.get('avg_board_strength', 50),
                    'stock_strength': concept_signals.get('avg_stock_strength', 50),
                }
            adaptive.record(
                tech_factors, decision.get('概念信号', 0),
                concept_sub, pred_direction, actual_chg,
            )

            # 记录主要概念板块名称
            main_boards = [b['board_name'] for b in boards[:3]] if boards else []

            day_results.append({
                'stock_code': code,
                'score_date': score_date_val,
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
                '概念板块数': len(boards),
                '回测天数': n_days,
                '准确率(宽松)': f'{l_ok}/{n_days} ({round(l_ok / n_days * 100, 1)}%)',
                '准确率(严格)': f'{d_ok}/{n_days} ({round(d_ok / n_days * 100, 1)}%)',
                '自适应准确率': f'{round(adaptive.get_accuracy() * 100, 1)}%',
            })
            logger.info("%s [%s] %d天 宽松%.1f%% 严格%.1f%% 自适应%.1f%%",
                        code, ', '.join([b['board_name'] for b in boards[:2]]),
                        n_days, l_ok / n_days * 100, d_ok / n_days * 100,
                        adaptive.get_accuracy() * 100)

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

    # 达标统计
    stocks_above_65 = sum(
        1 for s in stock_summaries
        if float(s['准确率(宽松)'].split('(')[1].replace('%)', '')) >= 65
    )
    stocks_above_60 = sum(
        1 for s in stock_summaries
        if float(s['准确率(宽松)'].split('(')[1].replace('%)', '')) >= 60
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
        '回测类型': '概念板块自适应日预测回测 v2',
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
        '达标统计': {
            '≥65%股票数': stocks_above_65,
            '≥60%股票数': stocks_above_60,
            '总有效股票数': len(stock_summaries),
        },
        '按预测方向统计': pred_dir_summary,
        '概念板块效果对比': concept_comparison,
        '置信度分析': confidence_summary,
        '按概念板块统计(Top20)': board_summary,
        '各股票汇总(按准确率排序)': stock_summaries_sorted,
        '逐日详情(前200条)': detail_list,
    }
