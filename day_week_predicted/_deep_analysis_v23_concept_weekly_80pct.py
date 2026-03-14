#!/usr/bin/env python3
"""
v23 深度分析：概念板块整体强弱 + 个股板块内强弱 → 周预测准确率 ≥ 80%

核心策略：
1. 基础策略C（前3天涨跌方向）作为主信号 — 已有~82%准确率
2. 概念板块整体强弱势（板块 vs 大盘）增强模糊区判断
3. 个股在概念板块中的相对强弱势作为辅助信号
4. 个股自适应阈值：根据每只股票的历史概念板块特征动态调整
5. 多维度信号融合 + 置信度分层决策

评估方法：
- 全样本准确率
- LOWO交叉验证（Leave-One-Week-Out，无泄露）
- 前半→后半泛化测试
- 按板块/概念板块维度分析
- 模糊区修正效果分析

数据源：DB（concept_board_kline, stock_concept_strength, stock_kline）
         + 50只股票日频回测结果
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import math
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from decimal import Decimal

from dao import get_connection
from service.analysis.concept_weekly_signal import (
    batch_preload_concept_data,
    compute_concept_signal_for_date,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

P = print


# ══════════════════════════════════════════════════════════════
# 工具函数
# ══════════════════════════════════════════════════════════════

def _to_float(v) -> float:
    if v is None: return 0.0
    if isinstance(v, Decimal): return float(v)
    return float(v)

def mean(lst):
    return sum(lst) / len(lst) if lst else 0.0

def median(lst):
    if not lst: return 0.0
    s = sorted(lst)
    n = len(s)
    return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2

def corrcoef(xs, ys):
    n = len(xs)
    if n < 3: return 0.0
    mx, my = mean(xs), mean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0: return 0.0
    return cov / (sx * sy)

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

def compound_return(daily_pcts):
    product = 1.0
    for p in daily_pcts:
        product *= (1 + p / 100)
    return (product - 1) * 100

def sigmoid_score(x, center=0, scale=1):
    try:
        return 1.0 / (1.0 + math.exp(-(x - center) / scale))
    except OverflowError:
        return 0.0 if x < center else 1.0

def _rate(ok, n):
    return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'


# ══════════════════════════════════════════════════════════════
# DB查询：板块整体强弱 + 个股板块内强弱
# ══════════════════════════════════════════════════════════════

def compute_board_strength_for_date(board_code, board_klines, market_klines,
                                     score_date, lookback=20):
    """计算某板块在某日期的整体强弱势评分（0-100）。"""
    bk = [k for k in board_klines if k['date'] <= score_date]
    mk_map = {k['date']: _to_float(k['change_percent']) for k in market_klines
              if k['date'] <= score_date}
    if len(bk) < 5:
        return None
    recent = bk[-lookback:]
    aligned = [(k, mk_map[k['date']]) for k in recent if k['date'] in mk_map]
    if len(aligned) < 5:
        return None

    daily_excess = [_to_float(bk_['change_percent']) - mk_ for bk_, mk_ in aligned]
    board_rets = [_to_float(bk_['change_percent']) for bk_, _ in aligned]
    market_rets = [mk_ for _, mk_ in aligned]
    n = len(aligned)

    excess_total = compound_return(board_rets) - compound_return(market_rets)
    recent_5 = daily_excess[-min(5, n):]
    excess_5d = sum(recent_5)
    win_days = sum(1 for e in daily_excess if e > 0)
    win_rate = win_days / n
    momentum = mean([_to_float(bk_['change_percent']) for bk_, _ in aligned[-5:]])

    s1 = sigmoid_score(excess_total, center=0, scale=8) * 30
    s2 = sigmoid_score(excess_5d, center=0, scale=2) * 25
    recent_20 = daily_excess[-min(20, n):]
    s3 = sigmoid_score(sum(recent_20), center=0, scale=4) * 20
    s4 = max(0, min(15, (win_rate - 0.3) / 0.4 * 15))
    score = round(max(0, min(100, s1 + s2 + s3 + s4)), 1)

    return {
        'score': score,
        'excess_return': round(excess_total, 3),
        'excess_5d': round(excess_5d, 3),
        'win_rate': round(win_rate, 4),
        'momentum': round(momentum, 4),
    }


def compute_stock_in_board_strength(stock_code, board_code, stock_klines,
                                     board_klines, score_date, lookback=20):
    """计算个股在某概念板块中的相对强弱势。"""
    sk_map = {k['date']: _to_float(k['change_percent']) for k in stock_klines
              if k['date'] <= score_date}
    bk = [k for k in board_klines if k['date'] <= score_date]
    if len(bk) < 5:
        return None
    recent = bk[-lookback:]
    aligned = []
    for k in recent:
        d = k['date']
        if d in sk_map:
            aligned.append((sk_map[d], _to_float(k['change_percent'])))
    if len(aligned) < 5:
        return None

    daily_excess = [s - b for s, b in aligned]
    n = len(aligned)
    excess_5d = sum(daily_excess[-min(5, n):])
    excess_20d = sum(daily_excess[-min(20, n):])
    win_rate = sum(1 for e in daily_excess if e > 0) / n

    s_short = sigmoid_score(excess_5d, center=0, scale=2) * 40
    s_mid = sigmoid_score(excess_20d, center=0, scale=5) * 35
    s_wr = max(0, min(25, (win_rate - 0.3) / 0.4 * 25))
    score = round(max(0, min(100, s_short + s_mid + s_wr)), 1)

    return {
        'excess_5d': round(excess_5d, 3),
        'excess_20d': round(excess_20d, 3),
        'win_rate': round(win_rate, 4),
        'strength_score': score,
    }


def load_market_klines(start_date, end_date):
    """从DB加载大盘K线（上证指数）"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT `date`, change_percent, close_price FROM stock_kline "
            "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
            "ORDER BY `date` ASC",
            (start_date, end_date),
        )
        return [{'date': r['date'],
                 'change_percent': _to_float(r['change_percent']),
                 'close_price': _to_float(r['close_price'])}
                for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def load_stock_klines_batch(stock_codes, start_date, end_date):
    """批量加载个股K线"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = {}
    try:
        if not stock_codes:
            return result
        placeholders = ','.join(['%s'] * len(stock_codes))
        cur.execute(
            f"SELECT stock_code, `date`, change_percent, close_price "
            f"FROM stock_kline "
            f"WHERE stock_code IN ({placeholders}) "
            f"AND `date` >= %s AND `date` <= %s "
            f"ORDER BY stock_code, `date` ASC",
            (*stock_codes, start_date, end_date),
        )
        for r in cur.fetchall():
            code = r['stock_code']
            if code not in result:
                result[code] = []
            result[code].append({
                'date': r['date'],
                'change_percent': _to_float(r['change_percent']),
                'close_price': _to_float(r['close_price']),
            })
    finally:
        cur.close()
        conn.close()
    return result


# ══════════════════════════════════════════════════════════════
# v23 增强概念信号计算
# ══════════════════════════════════════════════════════════════

def compute_v23_concept_signal(stock_code, score_date, concept_data,
                                board_kline_map, market_klines,
                                stock_klines_map):
    """计算v23增强概念信号：板块整体强弱 + 个股板块内强弱 + 原有信号。

    Returns:
        {
            concept_momentum,          # 概念板块平均动量
            concept_consensus,         # 看涨概念板块占比
            concept_strength,          # 概念板块平均强度
            board_market_score,        # 板块整体强弱均分(0-100)
            board_market_strong_pct,   # 强势板块占比
            stock_board_score,         # 个股板块内强弱均分(0-100)
            stock_board_strong_pct,    # 个股在强势位的板块占比
            board_momentum_5d,         # 板块5日动量均值
            stock_excess_5d,           # 个股5日超额收益均值
            composite_score,           # 综合评分(-5 ~ +5)
            n_boards,
        } 或 None
    """
    stock_boards = concept_data.get('stock_boards', {}).get(stock_code, [])
    if not stock_boards:
        return None

    # 原有概念信号
    orig_sig = compute_concept_signal_for_date(
        stock_code, score_date, board_kline_map, stock_boards, lookback=5
    )

    # 板块整体强弱
    board_scores = []
    strong_boards = 0
    stock_in_board_scores = []
    stock_strong_count = 0
    valid_boards = 0
    board_momentums_5d = []
    stock_excess_5d_list = []

    stock_kl = stock_klines_map.get(stock_code, [])

    for board in stock_boards:
        bc = board['board_code']
        bk = board_kline_map.get(bc, [])
        if not bk:
            continue

        # 板块 vs 大盘强弱
        bs = compute_board_strength_for_date(bc, bk, market_klines, score_date)
        if bs:
            board_scores.append(bs['score'])
            board_momentums_5d.append(bs['momentum'])
            if bs['score'] >= 55:
                strong_boards += 1
            valid_boards += 1

        # 个股 vs 板块强弱
        if stock_kl:
            ss = compute_stock_in_board_strength(
                stock_code, bc, stock_kl, bk, score_date
            )
            if ss:
                stock_in_board_scores.append(ss['strength_score'])
                stock_excess_5d_list.append(ss['excess_5d'])
                if ss['strength_score'] >= 55:
                    stock_strong_count += 1

    if valid_boards == 0:
        return orig_sig

    board_market_score = mean(board_scores)
    board_market_strong_pct = strong_boards / valid_boards
    stock_board_score = mean(stock_in_board_scores) if stock_in_board_scores else 50
    stock_board_strong_pct = (stock_strong_count / len(stock_in_board_scores)
                              if stock_in_board_scores else 0.5)
    avg_board_momentum_5d = mean(board_momentums_5d)
    avg_stock_excess_5d = mean(stock_excess_5d_list) if stock_excess_5d_list else 0

    # ── 综合评分 (-5 ~ +5) ──
    cs = 0.0

    # 维度1: 板块整体强弱 (权重 1.5)
    if board_market_score >= 62: cs += 1.5
    elif board_market_score >= 55: cs += 0.8
    elif board_market_score <= 38: cs -= 1.5
    elif board_market_score <= 45: cs -= 0.8

    # 维度2: 强势板块占比 (权重 0.8)
    if board_market_strong_pct >= 0.65: cs += 0.8
    elif board_market_strong_pct >= 0.5: cs += 0.3
    elif board_market_strong_pct <= 0.25: cs -= 0.8
    elif board_market_strong_pct <= 0.4: cs -= 0.3

    # 维度3: 个股板块内强弱 (权重 1.2)
    if stock_board_score >= 62: cs += 1.2
    elif stock_board_score >= 55: cs += 0.5
    elif stock_board_score <= 38: cs -= 1.2
    elif stock_board_score <= 45: cs -= 0.5

    # 维度4: 个股强势板块占比 (权重 0.5)
    if stock_board_strong_pct >= 0.6: cs += 0.5
    elif stock_board_strong_pct <= 0.3: cs -= 0.5

    # 维度5: 板块5日动量 (权重 0.5)
    if avg_board_momentum_5d > 0.5: cs += 0.5
    elif avg_board_momentum_5d > 0.2: cs += 0.2
    elif avg_board_momentum_5d < -0.5: cs -= 0.5
    elif avg_board_momentum_5d < -0.2: cs -= 0.2

    # 维度6: 个股5日超额收益 (权重 0.5)
    if avg_stock_excess_5d > 2: cs += 0.5
    elif avg_stock_excess_5d > 0.5: cs += 0.2
    elif avg_stock_excess_5d < -2: cs -= 0.5
    elif avg_stock_excess_5d < -0.5: cs -= 0.2

    result = {
        'board_market_score': round(board_market_score, 1),
        'board_market_strong_pct': round(board_market_strong_pct, 3),
        'stock_board_score': round(stock_board_score, 1),
        'stock_board_strong_pct': round(stock_board_strong_pct, 3),
        'board_momentum_5d': round(avg_board_momentum_5d, 4),
        'stock_excess_5d': round(avg_stock_excess_5d, 3),
        'composite_score': round(cs, 2),
        'n_boards': valid_boards,
    }

    # 合并原有信号
    if orig_sig:
        result['concept_momentum'] = orig_sig['concept_momentum']
        result['concept_consensus'] = orig_sig['concept_consensus']
        result['concept_strength'] = orig_sig['concept_strength']
    else:
        result['concept_momentum'] = 0
        result['concept_consensus'] = 0.5
        result['concept_strength'] = 0

    return result


# ══════════════════════════════════════════════════════════════
# v23 周预测策略
# ══════════════════════════════════════════════════════════════

def predict_B_v23(mon_actual, sector_up_rate, sig, stock_history=None):
    """v23策略B：周一收盘 + 概念板块多维信号 + 个股自适应。

    改进点：
    1. 更精细的信号区间划分
    2. 概念信号权重根据板块数量自适应
    3. 个股历史准确率反馈调整
    """
    if sig is None:
        if mon_actual > 0.5: return True, 'B:周一涨', 'high'
        if mon_actual < -0.5: return False, 'B:周一跌', 'high'
        return sector_up_rate > 0.5, 'B:板块基准率', 'low'

    cs = sig['composite_score']
    # 原有概念信号评分
    orig_cs = 0.0
    if sig['concept_consensus'] > 0.65: orig_cs += 1.0
    elif sig['concept_consensus'] < 0.35: orig_cs -= 1.0
    if sig['concept_momentum'] > 0.3: orig_cs += 0.8
    elif sig['concept_momentum'] < -0.3: orig_cs -= 0.8
    if sig['concept_strength'] > 1.5: orig_cs += 0.5
    elif sig['concept_strength'] < -1.5: orig_cs -= 0.5

    # 融合评分
    total_cs = cs + orig_cs

    # 信号可靠度（板块数越多越可靠）
    n_boards = sig.get('n_boards', 0)
    reliability = min(1.0, n_boards / 5)  # 5个板块以上满分

    # 加权融合评分
    weighted_cs = total_cs * reliability

    # ── 强看涨区 (>1.0%) ──
    if mon_actual > 1.0:
        if weighted_cs <= -3.0:
            return False, f'B:周一涨{mon_actual:+.2f}%但概念极弱({weighted_cs:.1f})→反转', 'medium'
        return True, f'B:周一涨{mon_actual:+.2f}%→看涨', 'high'

    # ── 偏涨区 (0.5~1.0%) ──
    if mon_actual > 0.5:
        if weighted_cs <= -2.0:
            return False, f'B:周一涨{mon_actual:+.2f}%但概念弱({weighted_cs:.1f})→反转', 'medium'
        return True, f'B:周一涨{mon_actual:+.2f}%→看涨', 'high'

    # ── 强看跌区 (<-1.0%) ──
    if mon_actual < -1.0:
        if weighted_cs >= 3.0:
            return True, f'B:周一跌{mon_actual:+.2f}%但概念极强({weighted_cs:.1f})→反弹', 'medium'
        return False, f'B:周一跌{mon_actual:+.2f}%→看跌', 'high'

    # ── 偏跌区 (-1.0~-0.5%) ──
    if mon_actual < -0.5:
        if weighted_cs >= 2.0:
            return True, f'B:周一跌{mon_actual:+.2f}%但概念强({weighted_cs:.1f})→反弹', 'medium'
        return False, f'B:周一跌{mon_actual:+.2f}%→看跌', 'high'

    # ── 模糊区 (-0.5~0.5%) ──
    if weighted_cs > 1.0:
        return True, f'B:模糊区+概念看涨({weighted_cs:.1f})', 'medium'
    if weighted_cs < -1.0:
        return False, f'B:模糊区+概念看跌({weighted_cs:.1f})', 'medium'

    # 弱信号：板块强弱占比 + 个股板块内强弱
    board_bias = sig['board_market_strong_pct'] - 0.5
    stock_bias = (sig['stock_board_score'] - 50) / 50
    combined_bias = board_bias * 0.6 + stock_bias * 0.4

    if combined_bias > 0.1:
        return True, f'B:模糊区+板块偏强({combined_bias:.2f})', 'low'
    if combined_bias < -0.1:
        return False, f'B:模糊区+板块偏弱({combined_bias:.2f})', 'low'

    return sector_up_rate > 0.5, f'B:兜底基准率({sector_up_rate:.0%})', 'low'


def predict_C_v23(d3_chg, sig, mon_actual=None, stock_history=None):
    """v23策略C：周三收盘 + 概念板块多维信号 + 自适应阈值。

    核心改进：
    1. 前3天涨跌方向作为主信号（基础准确率~82%）
    2. 概念信号仅在模糊区（|d3_chg| < 1.5%）发挥修正作用
    3. 板块整体强弱 + 个股板块内强弱双重确认
    4. 动态阈值：根据概念信号强度调整模糊区边界
    """
    if sig is None:
        return d3_chg > 0, f'C:前3天{d3_chg:+.2f}%', 'medium'

    cs = sig['composite_score']
    orig_cs = 0.0
    if sig.get('concept_consensus', 0.5) > 0.6: orig_cs += 0.8
    elif sig.get('concept_consensus', 0.5) < 0.4: orig_cs -= 0.8
    if sig.get('concept_momentum', 0) > 0.2: orig_cs += 0.6
    elif sig.get('concept_momentum', 0) < -0.2: orig_cs -= 0.6

    total_cs = cs + orig_cs
    n_boards = sig.get('n_boards', 0)
    reliability = min(1.0, n_boards / 5)
    weighted_cs = total_cs * reliability

    # ── 强信号区 (|d3_chg| > 2.0%) ──
    if abs(d3_chg) > 2.0:
        return d3_chg > 0, f'C:前3天{d3_chg:+.2f}%(强信号)', 'high'

    # ── 中强信号区 (1.5~2.0%) ──
    if abs(d3_chg) > 1.5:
        pred = d3_chg > 0
        # 仅在概念极端反向时修正
        if pred and weighted_cs <= -3.5:
            return False, f'C:前3天涨{d3_chg:+.2f}%但概念极弱→反转', 'medium'
        if not pred and weighted_cs >= 3.5:
            return True, f'C:前3天跌{d3_chg:+.2f}%但概念极强→反弹', 'medium'
        return pred, f'C:前3天{d3_chg:+.2f}%→{"涨" if pred else "跌"}', 'high'

    # ── 中等信号区 (0.5~1.5%) ──
    if abs(d3_chg) > 0.5:
        pred = d3_chg > 0
        # 概念信号可以修正
        if pred and weighted_cs <= -2.5:
            return False, f'C:前3天涨{d3_chg:+.2f}%但概念弱({weighted_cs:.1f})→反转', 'medium'
        if not pred and weighted_cs >= 2.5:
            return True, f'C:前3天跌{d3_chg:+.2f}%但概念强({weighted_cs:.1f})→反弹', 'medium'
        return pred, f'C:前3天{d3_chg:+.2f}%→{"涨" if pred else "跌"}', 'medium'

    # ── 模糊区 (|d3_chg| <= 0.5%) ──
    # 概念信号主导
    if weighted_cs > 1.5:
        return True, f'C:模糊区+概念看涨({weighted_cs:.1f})', 'medium'
    if weighted_cs < -1.5:
        return False, f'C:模糊区+概念看跌({weighted_cs:.1f})', 'medium'

    # 板块强弱 + 个股强弱综合
    board_bias = sig['board_market_strong_pct'] - 0.5
    stock_bias = (sig['stock_board_score'] - 50) / 50
    momentum_bias = 1 if sig.get('board_momentum_5d', 0) > 0.1 else (-1 if sig.get('board_momentum_5d', 0) < -0.1 else 0)

    combined_bias = board_bias * 0.4 + stock_bias * 0.3 + momentum_bias * 0.3

    if combined_bias > 0.05:
        return True, f'C:模糊区+综合偏涨({combined_bias:.2f})', 'low'
    if combined_bias < -0.05:
        return False, f'C:模糊区+综合偏跌({combined_bias:.2f})', 'low'

    # 兜底：前3天方向
    return d3_chg > 0, f'C:模糊区兜底前3天{d3_chg:+.2f}%', 'low'


def predict_C_v23_adaptive(d3_chg, sig, stock_code, stock_stats):
    """v23策略C自适应版：根据个股历史特征动态调整阈值。

    stock_stats: 该股票的历史统计 {
        'avg_weekly_chg': 平均周涨跌幅,
        'weekly_volatility': 周涨跌波动率,
        'concept_effectiveness': 概念信号历史有效率,
        'fuzzy_zone_ratio': 模糊区占比,
    }
    """
    # 基础预测
    pred, reason, conf = predict_C_v23(d3_chg, sig)

    if stock_stats is None:
        return pred, reason, conf

    # 自适应调整：根据个股波动率调整模糊区边界
    vol = stock_stats.get('weekly_volatility', 2.0)
    concept_eff = stock_stats.get('concept_effectiveness', 0.5)

    # 高波动股票：扩大强信号区（减少模糊区修正）
    if vol > 3.0 and abs(d3_chg) > 0.3:
        return d3_chg > 0, f'C_adapt:高波动({vol:.1f}%)前3天{d3_chg:+.2f}%', 'medium'

    # 概念信号历史有效率高的股票：增加概念信号权重
    if concept_eff > 0.65 and sig is not None:
        cs = sig['composite_score']
        if abs(d3_chg) <= 1.0 and abs(cs) > 1.5:
            pred_concept = cs > 0
            return pred_concept, f'C_adapt:概念有效({concept_eff:.0%})cs={cs:.1f}', 'medium'

    return pred, reason, conf
