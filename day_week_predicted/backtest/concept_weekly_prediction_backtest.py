#!/usr/bin/env python3
"""
概念板块增强周预测回测引擎

核心策略：
1. 策略C（前3天涨跌方向）作为主信号
2. 概念板块整体走势强弱（板块 vs 大盘超额收益）增强模糊区判断
3. 个股在概念板块中的相对强弱势作为辅助信号
4. 个股自适应阈值：根据历史波动率和概念信号有效率动态调整

数据源：全部从数据库获取
- stock_kline: 个股日K线
- concept_board_kline: 概念板块日K线
- stock_concept_board_stock: 个股-概念板块映射
- stock_fund_flow: 资金流向

评估方法：
- 全样本准确率
- LOWO交叉验证（Leave-One-Week-Out，无泄露）
- 按概念板块维度分析
- 模糊区修正效果分析

目标：周预测准确率 ≥ 80%
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


def _compound_return(pcts):
    p = 1.0
    for r in pcts:
        p *= (1 + r / 100)
    return (p - 1) * 100


def _rate_str(ok, n):
    return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'


# ═══════════════════════════════════════════════════════════
# 数据预加载（全部从DB）
# ═══════════════════════════════════════════════════════════

def _preload_weekly_data(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """一次性从DB预加载所有需要的数据。"""
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
    logger.info("[周预测数据] %d只股票K线, %d只有概念板块, %d/%d板块有K线, "
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
# 概念板块信号计算
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

    s_short = _sigmoid(excess_5d, center=0, scale=2) * 40
    s_mid = _sigmoid(excess_20d, center=0, scale=5) * 35
    s_wr = max(0, min(25, (win_rate - 0.3) / 0.4 * 25))
    score = round(max(0, min(100, s_short + s_mid + s_wr)), 1)

    return {
        'strength_score': score,
        'excess_5d': round(excess_5d, 3),
        'excess_20d': round(excess_20d, 3),
        'win_rate': round(win_rate, 4),
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


def compute_weekly_concept_signal(stock_code, score_date, data):
    """计算个股在某日期的概念板块综合信号。

    Returns:
        {
            board_market_score,        # 板块整体强弱均分(0-100)
            board_market_strong_pct,   # 强势板块占比
            stock_board_score,         # 个股板块内强弱均分(0-100)
            stock_board_strong_pct,    # 个股在强势位的板块占比
            board_momentum_5d,         # 板块5日动量均值
            stock_excess_5d,           # 个股5日超额收益均值
            concept_momentum,          # 概念板块平均动量
            concept_consensus,         # 看涨概念板块占比
            fund_flow_signal,          # 资金流信号
            composite_score,           # 综合评分(-5 ~ +5)
            n_boards,
        } 或 None
    """
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

    for board in boards:
        bc = board['board_code']
        bk = board_kline_map.get(bc, [])
        if not bk:
            continue

        # 板块 vs 大盘强弱
        bs = _compute_board_vs_market_strength(bk, market_klines, score_date)
        if bs:
            board_scores.append(bs['score'])
            board_momentums_5d.append(bs['momentum'])
            if bs['score'] >= 55:
                strong_boards += 1
            valid_boards += 1

        # 概念板块动量（近5日平均涨跌）
        valid_klines = [k for k in bk if k['date'] <= score_date]
        if len(valid_klines) >= 3:
            recent_5 = valid_klines[-5:]
            avg_chg = _mean([k['change_percent'] for k in recent_5])
            if avg_chg > 0:
                boards_up += 1

        # 个股 vs 板块强弱
        if stock_kl:
            ss = _compute_stock_vs_board_strength(stock_kl, bk, score_date)
            if ss:
                stock_in_board_scores.append(ss['strength_score'])
                stock_excess_5d_list.append(ss['excess_5d'])
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

    # 资金流信号
    fund_flows = data['fund_flow_map'].get(stock_code, [])
    ff_signal = _compute_fund_flow_signal(fund_flows, score_date)

    # ── 综合评分 (-5 ~ +5) ──
    cs = 0.0

    # 维度1: 板块整体强弱 (权重 1.5)
    if board_market_score >= 62:
        cs += 1.5
    elif board_market_score >= 55:
        cs += 0.8
    elif board_market_score <= 38:
        cs -= 1.5
    elif board_market_score <= 45:
        cs -= 0.8

    # 维度2: 强势板块占比 (权重 0.8)
    if board_market_strong_pct >= 0.65:
        cs += 0.8
    elif board_market_strong_pct >= 0.5:
        cs += 0.3
    elif board_market_strong_pct <= 0.25:
        cs -= 0.8
    elif board_market_strong_pct <= 0.4:
        cs -= 0.3

    # 维度3: 个股板块内强弱 (权重 1.2)
    if stock_board_score >= 62:
        cs += 1.2
    elif stock_board_score >= 55:
        cs += 0.5
    elif stock_board_score <= 38:
        cs -= 1.2
    elif stock_board_score <= 45:
        cs -= 0.5

    # 维度4: 个股强势板块占比 (权重 0.5)
    if stock_board_strong_pct >= 0.6:
        cs += 0.5
    elif stock_board_strong_pct <= 0.3:
        cs -= 0.5

    # 维度5: 板块5日动量 (权重 0.5)
    if avg_board_momentum_5d > 0.5:
        cs += 0.5
    elif avg_board_momentum_5d > 0.2:
        cs += 0.2
    elif avg_board_momentum_5d < -0.5:
        cs -= 0.5
    elif avg_board_momentum_5d < -0.2:
        cs -= 0.2

    # 维度6: 个股5日超额收益 (权重 0.5)
    if avg_stock_excess_5d > 2:
        cs += 0.5
    elif avg_stock_excess_5d > 0.5:
        cs += 0.2
    elif avg_stock_excess_5d < -2:
        cs -= 0.5
    elif avg_stock_excess_5d < -0.5:
        cs -= 0.2

    # 维度7: 概念共识度 (权重 0.5)
    if concept_consensus > 0.65:
        cs += 0.5
    elif concept_consensus < 0.35:
        cs -= 0.5

    # 维度8: 资金流 (权重 0.3)
    cs += ff_signal * 0.3

    # 信号可靠度（板块数越多越可靠）
    reliability = min(1.0, valid_boards / 5)
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
        'composite_score': round(weighted_cs, 2),
        'n_boards': valid_boards,
    }


# ═══════════════════════════════════════════════════════════
# 周预测策略
# ═══════════════════════════════════════════════════════════

def predict_weekly_direction(d3_chg, sig, stock_stats=None):
    """周预测核心策略：前3天涨跌方向 + 概念板块多维信号 + 个股自适应。

    Args:
        d3_chg: 前3天（周一~周三）累计涨跌幅(%)
        sig: 概念板块综合信号（compute_weekly_concept_signal返回值）
        stock_stats: 个股历史统计（可选，用于自适应调整）

    Returns:
        (pred_up: bool, reason: str, confidence: str)
    """
    if sig is None:
        # 无概念信号，纯跟随前3天方向
        if abs(d3_chg) > 0.3:
            return d3_chg > 0, f'无概念:前3天{d3_chg:+.2f}%', 'medium'
        return d3_chg > 0, f'无概念:前3天{d3_chg:+.2f}%(弱)', 'low'

    cs = sig['composite_score']

    # ── 个股自适应调整 ──
    vol_threshold_strong = 2.0  # 强信号区阈值
    vol_threshold_mid = 0.8     # 中等信号区阈值
    concept_flip_threshold = 2.5  # 概念反转阈值

    if stock_stats:
        vol = stock_stats.get('weekly_volatility', 2.0)
        concept_eff = stock_stats.get('concept_effectiveness', 0.5)
        # 高波动股票：扩大强信号区
        if vol > 4.0:
            vol_threshold_strong = 3.0
            vol_threshold_mid = 1.2
        elif vol > 3.0:
            vol_threshold_strong = 2.5
            vol_threshold_mid = 1.0
        # 概念信号历史有效率高：降低反转阈值
        if concept_eff > 0.65:
            concept_flip_threshold = 2.0

    # ── 强信号区 (|d3_chg| > strong_threshold) ──
    if abs(d3_chg) > vol_threshold_strong:
        pred = d3_chg > 0
        # 仅在概念极端反向时修正
        if pred and cs <= -concept_flip_threshold - 1.0:
            return False, f'前3天涨{d3_chg:+.2f}%但概念极弱({cs:.1f})→反转', 'medium'
        if not pred and cs >= concept_flip_threshold + 1.0:
            return True, f'前3天跌{d3_chg:+.2f}%但概念极强({cs:.1f})→反弹', 'medium'
        return pred, f'前3天{d3_chg:+.2f}%(强信号)', 'high'

    # ── 中等信号区 (mid_threshold < |d3_chg| <= strong_threshold) ──
    if abs(d3_chg) > vol_threshold_mid:
        pred = d3_chg > 0
        # 概念信号可以修正
        if pred and cs <= -concept_flip_threshold:
            return False, f'前3天涨{d3_chg:+.2f}%但概念弱({cs:.1f})→反转', 'medium'
        if not pred and cs >= concept_flip_threshold:
            return True, f'前3天跌{d3_chg:+.2f}%但概念强({cs:.1f})→反弹', 'medium'
        return pred, f'前3天{d3_chg:+.2f}%(中等信号)', 'medium'

    # ── 模糊区 (|d3_chg| <= mid_threshold) ──
    # 概念信号主导
    if cs > 1.5:
        return True, f'模糊区+概念看涨({cs:.1f})', 'medium'
    if cs < -1.5:
        return False, f'模糊区+概念看跌({cs:.1f})', 'medium'

    # 板块强弱 + 个股强弱综合
    board_bias = sig['board_market_strong_pct'] - 0.5
    stock_bias = (sig['stock_board_score'] - 50) / 50
    momentum_bias = (1 if sig.get('board_momentum_5d', 0) > 0.1
                     else (-1 if sig.get('board_momentum_5d', 0) < -0.1 else 0))
    combined_bias = board_bias * 0.4 + stock_bias * 0.3 + momentum_bias * 0.3

    if combined_bias > 0.08:
        return True, f'模糊区+综合偏涨({combined_bias:.2f})', 'low'
    if combined_bias < -0.08:
        return False, f'模糊区+综合偏跌({combined_bias:.2f})', 'low'

    # 兜底：前3天方向
    if abs(d3_chg) > 0.05:
        return d3_chg > 0, f'模糊区兜底前3天{d3_chg:+.2f}%', 'low'

    # 极端模糊：用概念共识度
    return sig['concept_consensus'] > 0.5, f'极模糊:共识度{sig["concept_consensus"]:.0%}', 'low'


# ═══════════════════════════════════════════════════════════
# 构建周数据
# ═══════════════════════════════════════════════════════════

def _build_weekly_records(stock_codes, data, start_date, end_date):
    """从日K线构建周数据记录。

    Returns:
        weekly_records: [{
            code, iso_week, week_dates, n_days,
            daily_changes, d3_chg, weekly_change, weekly_up,
            wed_date, concept_signal, concept_boards,
        }, ...]
    """
    weekly = []

    for code in stock_codes:
        klines = data['stock_klines'].get(code, [])
        if not klines:
            continue

        # 过滤到回测区间
        bt_klines = [k for k in klines if start_date <= k['date'] <= end_date]
        if len(bt_klines) < 5:
            continue

        # 按ISO周分组
        week_groups = defaultdict(list)
        for k in bt_klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iso_week = dt.isocalendar()[:2]
            week_groups[iso_week].append(k)

        # 获取该股票的概念板块名称
        boards = data['stock_boards'].get(code, [])
        board_names = [b['board_name'] for b in boards[:5]]

        for iso_week, days in week_groups.items():
            days.sort(key=lambda x: x['date'])
            if len(days) < 3:
                continue

            # 周涨跌幅（复合收益）
            daily_pcts = [d['change_percent'] for d in days]
            weekly_chg = _compound_return(daily_pcts)
            weekly_up = weekly_chg >= 0

            # 前3天累计涨跌
            d3_pcts = [d['change_percent'] for d in days[:3]]
            d3_chg = _compound_return(d3_pcts)

            # 周三日期（用于计算概念信号）
            wed_date = days[2]['date']

            # 计算周三收盘时的概念信号
            sig = compute_weekly_concept_signal(code, wed_date, data)

            weekly.append({
                'code': code,
                'iso_week': iso_week,
                'week_dates': [d['date'] for d in days],
                'n_days': len(days),
                'daily_changes': daily_pcts,
                'd3_chg': round(d3_chg, 4),
                'weekly_change': round(weekly_chg, 4),
                'weekly_up': weekly_up,
                'wed_date': wed_date,
                'concept_signal': sig,
                'concept_boards': board_names,
            })

    return weekly


# ═══════════════════════════════════════════════════════════
# 个股自适应统计
# ═══════════════════════════════════════════════════════════

def _compute_stock_stats(weekly_records, exclude_week=None):
    """计算每只股票的历史统计（用于自适应调整）。

    Args:
        weekly_records: 周数据列表
        exclude_week: 排除的周（LOWO用）

    Returns:
        {stock_code: {weekly_volatility, concept_effectiveness, ...}}
    """
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
        vol = (sum((c - _mean(weekly_chgs)) ** 2 for c in weekly_chgs)
               / len(weekly_chgs)) ** 0.5

        # 概念信号有效率
        concept_correct = 0
        concept_total = 0
        for w in weeks:
            sig = w['concept_signal']
            if sig and abs(sig['composite_score']) > 1.0:
                concept_total += 1
                pred_up = sig['composite_score'] > 0
                if pred_up == w['weekly_up']:
                    concept_correct += 1

        concept_eff = concept_correct / concept_total if concept_total >= 3 else 0.5

        stats[code] = {
            'weekly_volatility': round(vol, 3),
            'concept_effectiveness': round(concept_eff, 3),
            'n_weeks': len(weeks),
        }

    return stats


# ═══════════════════════════════════════════════════════════
# 回测主函数
# ═══════════════════════════════════════════════════════════

def run_concept_weekly_backtest(
    stock_codes: list[str],
    start_date: str = '2025-12-10',
    end_date: str = '2026-03-10',
    concept_board_map: dict = None,
) -> dict:
    """运行概念板块增强周预测回测。

    Args:
        stock_codes: 股票代码列表（带后缀，如 '002230.SZ'）
        start_date: 回测起始日期
        end_date: 回测截止日期
        concept_board_map: {stock_code: board_name} 概念板块归属（用于分组统计）

    Returns:
        回测结果汇总
    """
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  概念板块增强周预测回测")
    logger.info("  股票: %d只, 区间: %s ~ %s", len(stock_codes), start_date, end_date)
    logger.info("=" * 70)

    # 1. 预加载数据
    logger.info("[1/5] 预加载数据...")
    data = _preload_weekly_data(stock_codes, start_date, end_date)

    # 2. 构建周数据
    logger.info("[2/5] 构建周数据...")
    weekly = _build_weekly_records(stock_codes, data, start_date, end_date)
    logger.info("  周样本总数: %d", len(weekly))

    if not weekly:
        return {'error': '无有效周数据', 'weekly_count': 0}

    # 统计
    n_with_sig = sum(1 for w in weekly if w['concept_signal'] is not None)
    logger.info("  有概念信号: %d/%d (%.1f%%)",
                n_with_sig, len(weekly), n_with_sig / len(weekly) * 100)

    all_weeks = sorted(set(w['iso_week'] for w in weekly))
    all_stocks = sorted(set(w['code'] for w in weekly))
    logger.info("  覆盖周数: %d, 覆盖股票: %d", len(all_weeks), len(all_stocks))

    # 3. 全样本评估
    logger.info("[3/5] 全样本评估...")
    stock_stats = _compute_stock_stats(weekly)
    full_results = _evaluate_predictions(weekly, stock_stats)

    # 4. LOWO交叉验证
    logger.info("[4/5] LOWO交叉验证（Leave-One-Week-Out）...")
    lowo_results = _run_lowo_cv(weekly, all_weeks)

    # 5. 按概念板块分析
    logger.info("[5/5] 按概念板块分析...")
    board_results = _analyze_by_concept_board(weekly, stock_stats, concept_board_map)

    elapsed = (datetime.now() - t_start).total_seconds()
    logger.info("回测完成，耗时 %.1f 秒", elapsed)

    return {
        'summary': {
            'stock_count': len(all_stocks),
            'week_count': len(all_weeks),
            'weekly_sample_count': len(weekly),
            'concept_signal_coverage': round(n_with_sig / len(weekly) * 100, 1),
            'backtest_period': f'{start_date} ~ {end_date}',
            'elapsed_seconds': round(elapsed, 1),
        },
        'full_sample': full_results,
        'lowo_cv': lowo_results,
        'by_concept_board': board_results,
    }


def _evaluate_predictions(weekly, stock_stats, exclude_week=None):
    """评估预测准确率。"""
    correct = 0
    total = 0
    correct_with_sig = 0
    total_with_sig = 0
    correct_no_sig = 0
    total_no_sig = 0

    # 按置信度分层
    conf_stats = {'high': [0, 0], 'medium': [0, 0], 'low': [0, 0]}

    # 模糊区统计
    fuzzy_correct = 0
    fuzzy_total = 0

    details = []

    for w in weekly:
        if exclude_week and w['iso_week'] == exclude_week:
            continue

        sig = w['concept_signal']
        ss = stock_stats.get(w['code']) if stock_stats else None

        pred_up, reason, conf = predict_weekly_direction(w['d3_chg'], sig, ss)
        actual_up = w['weekly_up']
        is_correct = pred_up == actual_up

        total += 1
        if is_correct:
            correct += 1

        if sig is not None:
            total_with_sig += 1
            if is_correct:
                correct_with_sig += 1
        else:
            total_no_sig += 1
            if is_correct:
                correct_no_sig += 1

        conf_stats[conf][1] += 1
        if is_correct:
            conf_stats[conf][0] += 1

        # 模糊区
        if abs(w['d3_chg']) <= 0.8:
            fuzzy_total += 1
            if is_correct:
                fuzzy_correct += 1

        details.append({
            'code': w['code'],
            'iso_week': w['iso_week'],
            'd3_chg': w['d3_chg'],
            'weekly_change': w['weekly_change'],
            'pred_up': pred_up,
            'actual_up': actual_up,
            'correct': is_correct,
            'reason': reason,
            'confidence': conf,
            'concept_boards': w['concept_boards'],
        })

    accuracy = correct / total * 100 if total > 0 else 0

    return {
        'accuracy': round(accuracy, 1),
        'correct': correct,
        'total': total,
        'with_concept_signal': {
            'accuracy': round(correct_with_sig / total_with_sig * 100, 1) if total_with_sig > 0 else 0,
            'count': total_with_sig,
        },
        'without_concept_signal': {
            'accuracy': round(correct_no_sig / total_no_sig * 100, 1) if total_no_sig > 0 else 0,
            'count': total_no_sig,
        },
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


def _run_lowo_cv(weekly, all_weeks):
    """Leave-One-Week-Out 交叉验证（无泄露）。"""
    week_accuracies = []
    total_correct = 0
    total_count = 0

    for held_out_week in all_weeks:
        # 用其他周的数据计算stock_stats
        train_stats = _compute_stock_stats(weekly, exclude_week=held_out_week)

        # 评估held_out_week
        test_records = [w for w in weekly if w['iso_week'] == held_out_week]
        if not test_records:
            continue

        correct = 0
        for w in test_records:
            sig = w['concept_signal']
            ss = train_stats.get(w['code'])
            pred_up, _, _ = predict_weekly_direction(w['d3_chg'], sig, ss)
            if pred_up == w['weekly_up']:
                correct += 1

        acc = correct / len(test_records) * 100
        week_accuracies.append(acc)
        total_correct += correct
        total_count += len(test_records)

        logger.info("  LOWO 周%s: %s (样本%d)",
                     held_out_week, _rate_str(correct, len(test_records)),
                     len(test_records))

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


def _analyze_by_concept_board(weekly, stock_stats, concept_board_map=None):
    """按概念板块分组分析准确率。"""
    board_stats = defaultdict(lambda: {'correct': 0, 'total': 0, 'stocks': set()})

    for w in weekly:
        sig = w['concept_signal']
        ss = stock_stats.get(w['code']) if stock_stats else None
        pred_up, _, _ = predict_weekly_direction(w['d3_chg'], sig, ss)
        is_correct = pred_up == w['weekly_up']

        # 使用传入的概念板块映射
        board_name = '未分类'
        if concept_board_map and w['code'] in concept_board_map:
            board_name = concept_board_map[w['code']]
        elif w['concept_boards']:
            board_name = w['concept_boards'][0]

        board_stats[board_name]['total'] += 1
        board_stats[board_name]['stocks'].add(w['code'])
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
# 本地模拟数据模式（DB不可达时使用）
# ═══════════════════════════════════════════════════════════

import random
import hashlib


def _generate_realistic_klines(stock_code, start_date, end_date,
                                base_price=50.0, volatility=2.0, trend=0.0):
    """生成逼真的个股日K线数据。

    使用stock_code作为随机种子，确保可复现。
    trend: 正值偏涨，负值偏跌，0中性。
    """
    seed = int(hashlib.md5(stock_code.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    dt = datetime.strptime(start_date, '%Y-%m-%d')
    dt_end = datetime.strptime(end_date, '%Y-%m-%d')

    klines = []
    price = base_price
    while dt <= dt_end:
        # 跳过周末
        if dt.weekday() >= 5:
            dt += timedelta(days=1)
            continue

        # 日涨跌幅：正态分布 + 趋势偏移
        daily_chg = rng.gauss(trend / 250, volatility / 100)
        # 限制涨跌停
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

        price = new_price
        dt += timedelta(days=1)

    return klines


def _generate_board_klines(board_code, start_date, end_date,
                            member_klines_list, noise=0.3):
    """根据成分股K线生成板块K线（加权平均 + 噪声）。"""
    seed = int(hashlib.md5(board_code.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    # 收集所有日期
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
    """生成模拟数据（DB不可达时使用）。

    Args:
        stock_codes: 股票代码列表
        start_date: 起始日期
        end_date: 截止日期
        concept_board_stocks: {board_name: [stock_codes]} 概念板块映射

    Returns:
        与 _preload_weekly_data 相同格式的数据字典
    """
    dt = datetime.strptime(start_date, '%Y-%m-%d')
    ext_start = (dt - timedelta(days=180)).strftime('%Y-%m-%d')

    # 为每个板块设定趋势特征
    board_trends = {}
    board_vols = {}
    for i, board_name in enumerate(concept_board_stocks.keys()):
        seed = int(hashlib.md5(board_name.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)
        board_trends[board_name] = rng.uniform(-15, 15)  # 年化趋势
        board_vols[board_name] = rng.uniform(1.5, 3.5)   # 日波动率

    # 生成个股K线
    stock_klines = {}
    stock_boards = defaultdict(list)
    code_to_board = {}

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
                # 已生成（跨板块股票），只添加板块映射
                stock_boards[code].append({
                    'board_code': board_code,
                    'board_name': board_name,
                })
                continue

            # 个股趋势 = 板块趋势 + 个股alpha
            code_seed = int(hashlib.md5(code.encode()).hexdigest()[:8], 16)
            code_rng = random.Random(code_seed)
            stock_trend = trend + code_rng.uniform(-10, 10)
            stock_vol = vol * code_rng.uniform(0.8, 1.3)
            base_price = code_rng.uniform(10, 200)

            klines = _generate_realistic_klines(
                code, ext_start, end_date,
                base_price=base_price, volatility=stock_vol, trend=stock_trend
            )
            stock_klines[code] = klines
            stock_boards[code].append({
                'board_code': board_code,
                'board_name': board_name,
            })
            code_to_board[code] = board_name

    # 生成板块K线
    board_kline_map = {}
    for board_name, codes in concept_board_stocks.items():
        board_code = board_code_map[board_name]
        member_klines = [stock_klines[c] for c in codes if c in stock_klines]
        if member_klines:
            board_kline_map[board_code] = _generate_board_klines(
                board_code, ext_start, end_date, member_klines
            )

    # 生成大盘K线（所有股票的加权平均）
    all_member_klines = list(stock_klines.values())
    market_klines = _generate_board_klines(
        'market_index', ext_start, end_date, all_member_klines, noise=0.1
    )

    # 生成资金流数据
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
        fund_flow_map[code] = list(reversed(flows))  # 按日期倒序

    logger.info("[模拟数据] %d只股票K线, %d只有概念板块, %d个板块有K线, "
                "大盘%d天, 资金流%d只",
                len(stock_klines), len(stock_boards),
                len(board_kline_map), len(market_klines), len(fund_flow_map))

    return {
        'stock_klines': stock_klines,
        'stock_boards': dict(stock_boards),
        'board_kline_map': board_kline_map,
        'market_klines': market_klines,
        'fund_flow_map': fund_flow_map,
    }


def run_concept_weekly_backtest_simulated(
    stock_codes: list[str],
    concept_board_stocks: dict,
    start_date: str = '2025-12-01',
    end_date: str = '2026-03-10',
    concept_board_map: dict = None,
) -> dict:
    """使用模拟数据运行概念板块增强周预测回测。

    当DB不可达时使用此函数。数据特征：
    - 每个板块有独立的趋势和波动率
    - 个股在板块趋势基础上有个体alpha
    - 板块K线是成分股的加权平均+噪声
    - 大盘K线是所有股票的加权平均

    Args:
        stock_codes: 股票代码列表
        concept_board_stocks: {board_name: [stock_codes]} 概念板块映射
        start_date: 回测起始日期
        end_date: 回测截止日期
        concept_board_map: {stock_code: board_name}

    Returns:
        回测结果汇总（与 run_concept_weekly_backtest 格式相同）
    """
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  概念板块增强周预测回测（模拟数据模式）")
    logger.info("  股票: %d只, 区间: %s ~ %s", len(stock_codes), start_date, end_date)
    logger.info("=" * 70)

    # 1. 生成模拟数据
    logger.info("[1/5] 生成模拟数据...")
    data = _preload_simulated_data(stock_codes, start_date, end_date,
                                    concept_board_stocks)

    # 2. 构建周数据
    logger.info("[2/5] 构建周数据...")
    weekly = _build_weekly_records(stock_codes, data, start_date, end_date)
    logger.info("  周样本总数: %d", len(weekly))

    if not weekly:
        return {'error': '无有效周数据', 'weekly_count': 0}

    n_with_sig = sum(1 for w in weekly if w['concept_signal'] is not None)
    logger.info("  有概念信号: %d/%d (%.1f%%)",
                n_with_sig, len(weekly), n_with_sig / len(weekly) * 100)

    all_weeks = sorted(set(w['iso_week'] for w in weekly))
    all_stocks = sorted(set(w['code'] for w in weekly))
    logger.info("  覆盖周数: %d, 覆盖股票: %d", len(all_weeks), len(all_stocks))

    # 3. 全样本评估
    logger.info("[3/5] 全样本评估...")
    stock_stats = _compute_stock_stats(weekly)
    full_results = _evaluate_predictions(weekly, stock_stats)

    # 4. LOWO交叉验证
    logger.info("[4/5] LOWO交叉验证...")
    lowo_results = _run_lowo_cv(weekly, all_weeks)

    # 5. 按概念板块分析
    logger.info("[5/5] 按概念板块分析...")
    board_results = _analyze_by_concept_board(weekly, stock_stats, concept_board_map)

    elapsed = (datetime.now() - t_start).total_seconds()
    logger.info("回测完成，耗时 %.1f 秒", elapsed)

    return {
        'summary': {
            'stock_count': len(all_stocks),
            'week_count': len(all_weeks),
            'weekly_sample_count': len(weekly),
            'concept_signal_coverage': round(n_with_sig / len(weekly) * 100, 1),
            'backtest_period': f'{start_date} ~ {end_date}',
            'elapsed_seconds': round(elapsed, 1),
            'data_mode': 'simulated',
        },
        'full_sample': full_results,
        'lowo_cv': lowo_results,
        'by_concept_board': board_results,
    }
