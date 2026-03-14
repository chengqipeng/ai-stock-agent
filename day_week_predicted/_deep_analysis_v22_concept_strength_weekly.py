#!/usr/bin/env python3
"""
v22 深度分析：概念板块整体走势强弱 + 个股板块内强弱 → 周预测准确率评估

在v20d/v21基础上，新增两个维度：
1. 概念板块整体走势强弱势（板块 vs 大盘）：
   - 从 stock_concept_board 表获取板块信息
   - 从 concept_board_kline 表获取板块K线
   - 从 stock_kline 表获取大盘K线（000001.SH）
   - 计算板块超额收益、短期动量、胜率等综合评分

2. 个股在概念板块中的强弱势：
   - 从 stock_concept_strength 表获取个股板块内排名/评分
   - 或实时计算：个股涨跌 vs 板块涨跌的超额收益

3. 综合信号融合：
   - concept_board_strength: 板块整体强弱（0-100分）
   - stock_in_board_strength: 个股在板块内的相对强弱
   - 与原有 concept_momentum/consensus/strength 信号叠加

评估方法：
- 全样本准确率对比
- LOWO交叉验证（无泄露）
- 前半→后半泛化测试
- 模糊区修正效果分析
- 按板块/概念板块维度分析

数据源：DB（concept_board_kline, stock_concept_strength, stock_kline）
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
    predict_weekly_B_with_concept,
    predict_weekly_C_with_concept,
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


# ══════════════════════════════════════════════════════════════
# DB查询：板块整体强弱 + 个股板块内强弱
# ══════════════════════════════════════════════════════════════

def compute_board_strength_for_date(board_code, board_klines, market_klines,
                                     score_date, lookback=20):
    """计算某板块在某日期的整体强弱势评分（0-100）。

    基于板块K线 vs 大盘K线的超额收益，不依赖DB实时查询。

    Args:
        board_code: 板块代码
        board_klines: 板块K线列表 [{date, change_percent, ...}]
        market_klines: 大盘K线列表 [{date, change_percent, ...}]
        score_date: 评分日期
        lookback: 回看天数

    Returns:
        {score, excess_return, excess_5d, win_rate, momentum} 或 None
    """
    # 过滤到score_date及之前
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

    # 超额收益
    excess_total = compound_return(board_rets) - compound_return(market_rets)

    # 近5日超额
    recent_5 = daily_excess[-min(5, n):]
    excess_5d = sum(recent_5)

    # 胜率
    win_days = sum(1 for e in daily_excess if e > 0)
    win_rate = win_days / n

    # 动量（近5日均涨跌）
    momentum = mean([_to_float(bk_['change_percent']) for bk_, _ in aligned[-5:]])

    # 综合评分（简化版，与concept_board_market_strength一致）
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
    """计算个股在某概念板块中的相对强弱势。

    Args:
        stock_code: 股票代码
        board_code: 板块代码
        stock_klines: 个股K线 [{date, change_percent, ...}]
        board_klines: 板块K线 [{date, change_percent, ...}]
        score_date: 评分日期
        lookback: 回看天数

    Returns:
        {excess_5d, excess_20d, win_rate, strength_score} 或 None
    """
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

    # 综合评分：短期40% + 中期35% + 胜率25%
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


def load_stock_concept_strength_from_db(stock_codes):
    """从DB加载个股概念板块强弱势评分（stock_concept_strength表）"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    try:
        codes_6 = [c.split('.')[0] if '.' in c else c for c in stock_codes]
        if not codes_6:
            return result
        placeholders = ','.join(['%s'] * len(codes_6))
        cur.execute(
            f"SELECT stock_code, board_code, board_name, strength_score, "
            f"strength_level, excess_5d, excess_20d, win_rate, rank_in_board, "
            f"board_total_stocks "
            f"FROM stock_concept_strength "
            f"WHERE stock_code IN ({placeholders})",
            tuple(codes_6),
        )
        for r in cur.fetchall():
            result[r['stock_code']].append({
                'board_code': r['board_code'],
                'board_name': r['board_name'],
                'strength_score': _to_float(r['strength_score']),
                'strength_level': r['strength_level'],
                'excess_5d': _to_float(r.get('excess_5d')),
                'excess_20d': _to_float(r.get('excess_20d')),
                'win_rate': _to_float(r.get('win_rate')),
                'rank_in_board': r.get('rank_in_board'),
                'board_total_stocks': r.get('board_total_stocks'),
            })
    except Exception as e:
        logger.warning("加载stock_concept_strength失败: %s", e)
    finally:
        cur.close()
        conn.close()
    return dict(result)


# ══════════════════════════════════════════════════════════════
# v22 增强预测策略
# ══════════════════════════════════════════════════════════════

def compute_v22_concept_signal(stock_code, score_date, concept_data,
                                board_kline_map, market_klines,
                                stock_klines_map, db_strength_map):
    """计算v22增强概念信号：板块整体强弱 + 个股板块内强弱 + 原有信号。

    Returns:
        {
            concept_momentum, concept_consensus, concept_strength,  # 原有
            board_market_score,     # 板块整体强弱均分(0-100)
            board_market_strong_pct,# 强势板块占比
            stock_board_score,      # 个股板块内强弱均分(0-100)
            stock_board_strong_pct, # 个股在强势位的板块占比
            composite_score,        # 综合评分(-3 ~ +3)
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

    code_6 = stock_code.split('.')[0] if '.' in stock_code else stock_code
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
                if ss['strength_score'] >= 55:
                    stock_strong_count += 1

    if valid_boards == 0:
        return orig_sig  # 回退到原有信号

    board_market_score = mean(board_scores) if board_scores else 50
    board_market_strong_pct = strong_boards / valid_boards
    stock_board_score = mean(stock_in_board_scores) if stock_in_board_scores else 50
    stock_board_strong_pct = (stock_strong_count / len(stock_in_board_scores)
                              if stock_in_board_scores else 0.5)

    # 综合评分 (-3 ~ +3)
    cs = 0
    # 板块整体强弱
    if board_market_score >= 60: cs += 1
    elif board_market_score <= 40: cs -= 1
    if board_market_strong_pct >= 0.6: cs += 0.5
    elif board_market_strong_pct <= 0.3: cs -= 0.5

    # 个股板块内强弱
    if stock_board_score >= 60: cs += 1
    elif stock_board_score <= 40: cs -= 1
    if stock_board_strong_pct >= 0.6: cs += 0.5
    elif stock_board_strong_pct <= 0.3: cs -= 0.5

    result = {
        'board_market_score': round(board_market_score, 1),
        'board_market_strong_pct': round(board_market_strong_pct, 3),
        'stock_board_score': round(stock_board_score, 1),
        'stock_board_strong_pct': round(stock_board_strong_pct, 3),
        'composite_score': round(cs, 1),
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


def predict_B_v22(mon_actual, sector_up_rate, sig):
    """v22策略B：周一收盘 + 板块整体强弱 + 个股板块内强弱。"""
    if sig is None:
        # 无概念数据，回退原始B
        if mon_actual > 0.5: return True, 'B原始:周一涨'
        if mon_actual < -0.5: return False, 'B原始:周一跌'
        return sector_up_rate > 0.5, 'B原始:板块基准率'

    cs = sig['composite_score']
    # 原有概念信号评分
    orig_cs = 0
    if sig['concept_consensus'] > 0.65: orig_cs += 1
    elif sig['concept_consensus'] < 0.35: orig_cs -= 1
    if sig['concept_momentum'] > 0.3: orig_cs += 1
    elif sig['concept_momentum'] < -0.3: orig_cs -= 1

    # 融合评分 = 板块强弱 + 原有概念 + 个股强弱
    total_cs = cs + orig_cs

    # 强信号区
    if mon_actual > 0.5:
        if total_cs <= -2.5:
            return False, f'B_v22:周一涨{mon_actual:+.2f}%但概念极弱({total_cs:.1f})→反转'
        return True, f'B_v22:周一涨{mon_actual:+.2f}%→看涨'

    if mon_actual < -0.5:
        if total_cs >= 2.5:
            return True, f'B_v22:周一跌{mon_actual:+.2f}%但概念极强({total_cs:.1f})→反弹'
        return False, f'B_v22:周一跌{mon_actual:+.2f}%→看跌'

    # 模糊区：综合评分决定
    if total_cs > 0.5:
        return True, f'B_v22:模糊区+概念看涨({total_cs:.1f})'
    if total_cs < -0.5:
        return False, f'B_v22:模糊区+概念看跌({total_cs:.1f})'

    # 弱信号：用板块强弱占比
    if sig['board_market_strong_pct'] > 0.55:
        return True, f'B_v22:模糊区+板块偏强({sig["board_market_strong_pct"]:.0%})'
    if sig['board_market_strong_pct'] < 0.35:
        return False, f'B_v22:模糊区+板块偏弱({sig["board_market_strong_pct"]:.0%})'

    return sector_up_rate > 0.5, f'B_v22:兜底板块基准率({sector_up_rate:.0%})'


def predict_C_v22(d3_chg, sig):
    """v22策略C：周三收盘 + 板块整体强弱 + 个股板块内强弱。"""
    if sig is None:
        return d3_chg > 0, f'C原始:前3天{d3_chg:+.2f}%'

    cs = sig['composite_score']
    orig_cs = 0
    if sig.get('concept_consensus', 0.5) > 0.6: orig_cs += 1
    elif sig.get('concept_consensus', 0.5) < 0.4: orig_cs -= 1
    if sig.get('concept_momentum', 0) > 0.2: orig_cs += 1
    elif sig.get('concept_momentum', 0) < -0.2: orig_cs -= 1

    total_cs = cs + orig_cs

    # 强信号区
    if abs(d3_chg) > 1.5:
        return d3_chg > 0, f'C_v22:前3天{d3_chg:+.2f}%(强信号)'

    # 中等信号区
    if abs(d3_chg) > 0.5:
        pred = d3_chg > 0
        if pred and total_cs <= -2.5:
            return False, f'C_v22:前3天涨{d3_chg:+.2f}%但概念极弱→反转'
        if not pred and total_cs >= 2.5:
            return True, f'C_v22:前3天跌{d3_chg:+.2f}%但概念极强→反弹'
        return pred, f'C_v22:前3天{d3_chg:+.2f}%→{"涨" if pred else "跌"}'

    # 模糊区
    if total_cs > 0.5:
        return True, f'C_v22:模糊区+概念看涨({total_cs:.1f})'
    if total_cs < -0.5:
        return False, f'C_v22:模糊区+概念看跌({total_cs:.1f})'

    return d3_chg > 0, f'C_v22:前3天{d3_chg:+.2f}%→{"涨" if d3_chg > 0 else "跌"}'

