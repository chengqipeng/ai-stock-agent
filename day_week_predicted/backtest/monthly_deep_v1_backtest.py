#!/usr/bin/env python3
"""
月度深度预测模型 V1 — 多因子融合 + 自适应阈值
==============================================
目标：预测下月涨跌方向，准确率 ≥ 65%，回测 ≥ 50 只股票

核心策略（5大维度融合）：
  1. 价格动量与均值回归（月度涨跌幅、位置、连续涨跌）
  2. 量价关系（缩量/放量 + 价格方向组合）
  3. 大盘环境（指数月度涨跌 + 个股与大盘相关性）
  4. 资金流向（主力净流入趋势、大单占比）
  5. 概念板块强弱（板块 vs 大盘超额收益、个股 vs 板块强弱）

评分体系：
  - 每个维度输出 [-3, +3] 的信号分
  - 加权融合后通过自适应阈值决策
  - 仅在信号强度足够时出手（提高精度、牺牲覆盖率）

回测方法：
  - 滚动窗口（逐月前推）
  - 全样本 + 按市场 + 按月份 + 按信号强度分层
  - 稳定性检验（月间准确率标准差）

用法：
    python -m day_week_predicted.backtest.monthly_deep_v1_backtest
"""
import sys
import math
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.insert(0, '.')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float,
    _compound_return, _get_stock_index,
)

N_MONTHS = 8  # 回测月数
MIN_SIGNAL_THRESHOLD = 0.8  # 最低出手阈值（|score| >= 此值才预测）


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0


def _std(lst):
    if len(lst) < 2:
        return 0.0
    m = _mean(lst)
    return (sum((x - m) ** 2 for x in lst) / (len(lst) - 1)) ** 0.5


def _sigmoid(x, center=0, scale=1):
    try:
        return 1.0 / (1.0 + math.exp(-(x - center) / scale))
    except OverflowError:
        return 0.0 if x < center else 1.0


def _safe_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _group_by_month(klines):
    """将日K线按自然月分组。返回 {(year, month): [kline_dicts]}"""
    groups = defaultdict(list)
    for k in klines:
        d = k['date']
        if isinstance(d, str):
            dt = datetime.strptime(d, '%Y-%m-%d')
        else:
            dt = d
        groups[(dt.year, dt.month)].append(k)
    for key in groups:
        groups[key].sort(key=lambda x: x['date'])
    return groups



# ═══════════════════════════════════════════════════════════
# 维度1: 价格动量与均值回归
# ═══════════════════════════════════════════════════════════

def _score_price_momentum(feat: dict) -> float:
    """
    价格动量评分 [-3, +3]

    V2核心发现：
    - A股月度最可靠信号是"超跌反弹"（均值回归）
    - 跌信号（高位回调）不可靠，需要极端条件才出手
    - 位置是最重要的辅助因子
    """
    score = 0.0
    this_chg = feat['this_chg']
    pos60 = feat['pos60']
    prev_chg = feat['prev_chg']
    last_week_chg = feat['last_week_chg']

    # ── 超跌反弹信号（最核心，高权重）──
    if this_chg < -15:
        score += 2.0
        if pos60 is not None and pos60 < 0.2:
            score += 0.8
    elif this_chg < -10:
        score += 1.3
        if pos60 is not None and pos60 < 0.25:
            score += 0.5
    elif this_chg < -7:
        score += 0.8
        if pos60 is not None and pos60 < 0.3:
            score += 0.3
    elif this_chg < -5:
        score += 0.4
        if pos60 is not None and pos60 < 0.3:
            score += 0.2

    # ── 过热回调信号（仅极端情况，且需要多维度确认）──
    if this_chg > 30 and pos60 is not None and pos60 > 0.9:
        score -= 2.0
    elif this_chg > 25 and pos60 is not None and pos60 > 0.85:
        score -= 1.5
    elif this_chg > 20 and pos60 is not None and pos60 > 0.85:
        score -= 1.0

    # ── 连续月份方向 ──
    if prev_chg is not None:
        if this_chg < -5 and prev_chg < -5:
            score += 0.8  # 连续两月大跌 → 反弹
        elif this_chg < -3 and prev_chg < -5:
            score += 0.4
        elif this_chg > 15 and prev_chg > 15:
            score -= 0.8  # 连续两月暴涨 → 回调（收紧阈值）
        elif this_chg > 10 and prev_chg > 10:
            score -= 0.5

    # ── 尾部走势 ──
    if this_chg < -5 and last_week_chg > 2:
        score += 0.4  # 月跌但尾部反弹 → 底部企稳
    elif this_chg > 10 and last_week_chg < -3:
        score -= 0.3  # 月涨但尾部回落

    # ── 低位加成 ──
    if pos60 is not None:
        if pos60 < 0.15:
            score += 0.5
        elif pos60 < 0.25:
            score += 0.3

    return max(-3.0, min(3.0, score))


# ═══════════════════════════════════════════════════════════
# 维度2: 量价关系
# ═══════════════════════════════════════════════════════════

def _score_volume_price(feat: dict) -> float:
    """
    量价关系评分 [-3, +3]

    V2优化：聚焦缩量下跌（最可靠的看涨信号）
    """
    score = 0.0
    this_chg = feat['this_chg']
    vol_ratio = feat['vol_ratio']
    pos60 = feat['pos60']

    if vol_ratio is None:
        return 0.0

    # ── 缩量下跌 → 看涨（恐慌出尽，最可靠）──
    if this_chg < -8 and vol_ratio < 0.5:
        score += 1.8
    elif this_chg < -5 and vol_ratio < 0.6:
        score += 1.2
        if pos60 is not None and pos60 < 0.3:
            score += 0.3
    elif this_chg < -3 and vol_ratio < 0.7:
        score += 0.6

    # ── 放量大涨 + 极高位 → 看跌（仅极端情况）──
    if this_chg > 20 and vol_ratio > 2.0 and pos60 is not None and pos60 > 0.9:
        score -= 1.0
    elif this_chg > 15 and vol_ratio > 2.5 and pos60 is not None and pos60 > 0.85:
        score -= 0.6

    # ── 放量下跌 + 极低位 → 可能见底 ──
    if this_chg < -10 and vol_ratio > 1.5 and pos60 is not None and pos60 < 0.2:
        score += 0.5  # 恐慌性抛售可能是最后一跌

    return max(-3.0, min(3.0, score))


# ═══════════════════════════════════════════════════════════
# 维度3: 大盘环境
# ═══════════════════════════════════════════════════════════

def _score_market_env(feat: dict) -> float:
    """
    大盘环境评分 [-3, +3]

    V3优化：增加大盘趋势判断，避免在趋势性下跌中抄底
    """
    score = 0.0
    this_chg = feat['this_chg']
    mkt_chg = feat['mkt_chg']
    prev_chg = feat.get('prev_chg')
    mkt_prev_chg = feat.get('mkt_prev_chg')

    # ── 大盘趋势判断 ──
    mkt_trend_down = False
    if mkt_prev_chg is not None and mkt_prev_chg < -2 and mkt_chg < -2:
        mkt_trend_down = True  # 大盘连续两月下跌

    # ── 大盘深跌 + 个股跌 ──
    if mkt_chg < -5:
        if mkt_trend_down:
            # 大盘趋势性下跌，不要轻易抄底
            if this_chg < -15:
                score += 0.8  # 只有极端超跌才看涨
            elif this_chg < -8:
                score += 0.3
            # 否则不给信号
        else:
            # 大盘单月深跌（非趋势），反弹概率高
            if this_chg < -8:
                score += 1.5
            elif this_chg < -3:
                score += 1.0

    elif mkt_chg < -2:
        if mkt_trend_down:
            if this_chg < -10:
                score += 0.5
        else:
            if this_chg < -8:
                score += 1.0
            elif this_chg < -3:
                score += 0.5
            elif this_chg > 8:
                score -= 0.5

    # ── 大盘涨 + 个股跌 → 需要区分补涨 vs 弱势 ──
    elif mkt_chg > 3:
        if mkt_chg > 10 and this_chg < -3:
            score -= 0.8
        elif mkt_chg > 5 and this_chg < -5:
            score -= 0.3
        elif this_chg < -8:
            score += 0.4

    # ── 个股相对大盘超额收益极端值 ──
    excess = this_chg - mkt_chg
    if excess < -15:
        if mkt_chg > 5:
            score -= 0.5
        else:
            score += 0.4
    elif excess > 20:
        score -= 0.3

    return max(-3.0, min(3.0, score))



# ═══════════════════════════════════════════════════════════
# 维度4: 资金流向
# ═══════════════════════════════════════════════════════════

def _score_fund_flow(feat: dict) -> float:
    """
    资金流向评分 [-3, +3]

    核心逻辑：
    - 月末主力持续净流入 → 看涨
    - 月末主力持续净流出 → 看跌
    - 大单占比趋势变化
    """
    score = 0.0
    ff_data = feat.get('fund_flow_data', [])
    if not ff_data:
        return 0.0

    # 取本月最后10个交易日的资金流数据
    recent = ff_data[:10]  # 已按日期降序
    if len(recent) < 3:
        return 0.0

    # 大单净占比均值
    big_net_pcts = [_safe_float(f.get('big_net_pct', 0)) for f in recent]
    avg_big_pct = _mean(big_net_pcts)

    # 净流入趋势（最近5天 vs 前5天）
    if len(recent) >= 6:
        recent_5 = big_net_pcts[:5]
        prev_5 = big_net_pcts[5:10] if len(big_net_pcts) >= 10 else big_net_pcts[5:]
        if prev_5:
            trend = _mean(recent_5) - _mean(prev_5)
        else:
            trend = 0
    else:
        trend = 0

    # 主力5日净额
    main_5d = [_safe_float(f.get('main_net_5day', 0)) for f in recent[:5]]
    avg_main_5d = _mean(main_5d)

    # ── 大单净占比信号 ──
    if avg_big_pct > 5:
        score += 1.5
    elif avg_big_pct > 3:
        score += 1.0
    elif avg_big_pct > 1:
        score += 0.5
    elif avg_big_pct < -5:
        score -= 1.5
    elif avg_big_pct < -3:
        score -= 1.0
    elif avg_big_pct < -1:
        score -= 0.5

    # ── 资金流趋势 ──
    if trend > 3:
        score += 0.8  # 资金加速流入
    elif trend > 1:
        score += 0.3
    elif trend < -3:
        score -= 0.8  # 资金加速流出
    elif trend < -1:
        score -= 0.3

    # ── 主力5日净额方向 ──
    if avg_main_5d > 0:
        score += 0.3
    elif avg_main_5d < 0:
        score -= 0.3

    return max(-3.0, min(3.0, score))


# ═══════════════════════════════════════════════════════════
# 维度5: 概念板块强弱
# ═══════════════════════════════════════════════════════════

def _compute_board_strength_for_month(board_klines, market_klines,
                                       month_end_date, lookback=20):
    """计算概念板块相对大盘的强弱势评分（0-100）。"""
    bk = [k for k in board_klines if k['date'] <= month_end_date]
    mk_map = {k['date']: k['change_percent'] for k in market_klines
              if k['date'] <= month_end_date}
    if len(bk) < 5:
        return None

    recent = bk[-lookback:]
    aligned = [(k, mk_map[k['date']]) for k in recent if k['date'] in mk_map]
    if len(aligned) < 5:
        return None

    board_rets = [k['change_percent'] for k, _ in aligned]
    market_rets = [mk for _, mk in aligned]
    daily_excess = [b - m for b, m in zip(board_rets, market_rets)]
    n = len(aligned)

    excess_total = _compound_return(board_rets) - _compound_return(market_rets)
    excess_5d = sum(daily_excess[-min(5, n):])
    win_rate = sum(1 for e in daily_excess if e > 0) / n
    momentum = _mean(board_rets[-5:])

    s1 = _sigmoid(excess_total, center=0, scale=8) * 30
    s2 = _sigmoid(excess_5d, center=0, scale=2) * 25
    s3 = _sigmoid(sum(daily_excess[-min(20, n):]), center=0, scale=4) * 20
    s4 = max(0, min(15, (win_rate - 0.3) / 0.4 * 15))
    s5 = min(10, abs(momentum) * 5) if momentum > 0 else 0
    score = round(max(0, min(100, s1 + s2 + s3 + s4 + s5)), 1)

    return {
        'score': score,
        'excess_total': round(excess_total, 3),
        'momentum': round(momentum, 4),
        'win_rate': round(win_rate, 4),
    }


def _compute_stock_board_strength(stock_klines, board_klines,
                                   month_end_date, lookback=20):
    """计算个股相对概念板块的强弱势评分（0-100）。"""
    sk_map = {k['date']: k['change_percent'] for k in stock_klines
              if k['date'] <= month_end_date}
    bk = [k for k in board_klines if k['date'] <= month_end_date]
    if len(bk) < 5:
        return None

    recent = bk[-lookback:]
    aligned = [(sk_map[k['date']], k['change_percent'])
               for k in recent if k['date'] in sk_map]
    if len(aligned) < 5:
        return None

    daily_excess = [s - b for s, b in aligned]
    n = len(aligned)
    excess_5d = sum(daily_excess[-min(5, n):])
    excess_20d = sum(daily_excess[-min(20, n):])
    win_rate = sum(1 for e in daily_excess if e > 0) / n

    s_short = _sigmoid(excess_5d, center=0, scale=2) * 35
    s_mid = _sigmoid(excess_20d, center=0, scale=5) * 30
    s_wr = max(0, min(20, (win_rate - 0.3) / 0.4 * 20))
    s_extra = 15 if excess_5d > 0 and excess_20d > 0 else 0
    score = round(max(0, min(100, s_short + s_mid + s_wr + s_extra)), 1)

    return {
        'strength_score': score,
        'excess_5d': round(excess_5d, 3),
        'excess_20d': round(excess_20d, 3),
        'win_rate': round(win_rate, 4),
    }


def _score_concept_board(feat: dict) -> float:
    """
    概念板块强弱评分 [-3, +3]

    V2关键发现：概念板块弱势时个股下月反弹概率更高（均值回归）
    因此反转信号方向：板块弱 → 看涨，板块强 → 看跌
    """
    board_signals = feat.get('board_signals', [])
    stock_board_signals = feat.get('stock_board_signals', [])

    if not board_signals:
        return 0.0

    board_scores = [s['score'] for s in board_signals if s is not None]
    if not board_scores:
        return 0.0

    avg_board = _mean(board_scores)
    stock_scores = [s['strength_score'] for s in stock_board_signals
                    if s is not None]
    avg_stock = _mean(stock_scores) if stock_scores else 50

    score = 0.0

    # ── 板块弱势 → 看涨（均值回归）──
    if avg_board <= 35:
        score += 1.0
    elif avg_board <= 42:
        score += 0.5
    # ── 板块强势 → 看跌（过热回调）──
    elif avg_board >= 65:
        score -= 0.8
    elif avg_board >= 58:
        score -= 0.3

    # ── 个股在板块内弱势 → 看涨 ──
    if avg_stock <= 35:
        score += 0.6
    elif avg_stock <= 42:
        score += 0.3
    elif avg_stock >= 65:
        score -= 0.5
    elif avg_stock >= 58:
        score -= 0.2

    # ── 板块动量反转 ──
    momentums = [s['momentum'] for s in board_signals
                 if s is not None and 'momentum' in s]
    if momentums:
        avg_mom = _mean(momentums)
        if avg_mom < -0.5:
            score += 0.4  # 板块动量向下 → 反弹
        elif avg_mom > 0.5:
            score -= 0.3  # 板块动量向上 → 可能过热

    return max(-3.0, min(3.0, score))


# ═══════════════════════════════════════════════════════════
# 综合决策引擎
# ═══════════════════════════════════════════════════════════

# 维度权重
WEIGHTS = {
    'price_momentum': 0.8,    # 价格动量
    'volume_price': 0.7,      # 量价关系
    'market_env': 0.8,        # 大盘环境
    'fund_flow': 0.7,         # 资金流向（提高，区分度高）
    'concept_board': 0.9,     # 概念板块
}


def predict_monthly_direction(feat: dict) -> dict:
    """
    月度方向预测综合决策。

    Returns:
        {
            'pred_up': bool or None,  # None表示信号不足不出手
            'score': float,           # 综合评分
            'confidence': str,        # high/medium/low
            'reason': str,            # 决策理由
            'dim_scores': dict,       # 各维度评分
        }
    """
    # 计算各维度评分
    s_price = _score_price_momentum(feat)
    s_volume = _score_volume_price(feat)
    s_market = _score_market_env(feat)
    s_fund = _score_fund_flow(feat)
    s_concept = _score_concept_board(feat)

    dim_scores = {
        'price_momentum': round(s_price, 2),
        'volume_price': round(s_volume, 2),
        'market_env': round(s_market, 2),
        'fund_flow': round(s_fund, 2),
        'concept_board': round(s_concept, 2),
    }

    # 加权融合
    total_score = (
        s_price * WEIGHTS['price_momentum']
        + s_volume * WEIGHTS['volume_price']
        + s_market * WEIGHTS['market_env']
        + s_fund * WEIGHTS['fund_flow']
        + s_concept * WEIGHTS['concept_board']
    )

    # 归一化（除以权重总和，使范围约为 [-3, +3]）
    w_sum = sum(WEIGHTS.values())
    norm_score = total_score / w_sum

    # ── 一致性加成 ──
    # 多维度方向一致时增强信号
    dims = [s_price, s_volume, s_market, s_fund, s_concept]
    non_zero = [d for d in dims if abs(d) > 0.2]
    if len(non_zero) >= 3:
        pos_count = sum(1 for d in non_zero if d > 0)
        neg_count = sum(1 for d in non_zero if d < 0)
        consistency = max(pos_count, neg_count) / len(non_zero)
        if consistency >= 0.8:
            norm_score *= 1.15  # 高一致性加成15%

    # ── 月度波动率修正 ──
    # 高波动月份信号更可靠（极端行情后反转概率更高）
    this_chg = feat.get('this_chg', 0)
    if abs(this_chg) > 12:
        norm_score *= 1.1  # 大波动月份加成

    # ── 决策 ──
    abs_score = abs(norm_score)

    if abs_score < MIN_SIGNAL_THRESHOLD:
        return {
            'pred_up': None,
            'score': round(norm_score, 3),
            'confidence': 'skip',
            'reason': f'信号不足({norm_score:+.2f})',
            'dim_scores': dim_scores,
        }

    pred_up = norm_score > 0

    # ── 策略核心：只做涨信号（A股月度跌信号不可靠）──
    if not pred_up:
        return {
            'pred_up': None,
            'score': round(norm_score, 3),
            'confidence': 'skip_down',
            'reason': f'跳过跌信号({norm_score:+.2f})',
            'dim_scores': dim_scores,
        }

    # ── 弱势股软过滤：仅过滤极端弱势（大盘大涨但个股大跌）──
    mkt_chg = feat.get('mkt_chg', 0)
    this_chg = feat.get('this_chg', 0)
    pos60 = feat.get('pos60')

    # 大盘暴涨但个股跌 = 结构性弱势，不预测反弹
    if mkt_chg > 8 and this_chg < -3:
        return {
            'pred_up': None,
            'score': round(norm_score, 3),
            'confidence': 'skip_weak',
            'reason': f'极端弱势股(大盘{mkt_chg:+.1f}%个股{this_chg:+.1f}%)',
            'dim_scores': dim_scores,
        }

    # 弱势股过滤：个股大幅跑输大盘 = 结构性弱势
    # 当大盘没有大跌(>-5%)时，个股跑输大盘12%+说明是个股自身问题
    excess = this_chg - mkt_chg
    if mkt_chg > -5 and excess < -12:
        if excess < -20:
            penalty = 0.25
        elif excess < -15:
            penalty = 0.4
        else:
            penalty = 0.6
        norm_score *= penalty
        abs_score = abs(norm_score)
        if abs_score < MIN_SIGNAL_THRESHOLD:
            return {
                'pred_up': None,
                'score': round(norm_score, 3),
                'confidence': 'skip_weak',
                'reason': f'弱势股过滤(超额{excess:+.1f}%)',
                'dim_scores': dim_scores,
            }

    # ── 资金流出 + 弱信号 → 不抄底 ──
    # 资金流向是区分度最高的维度，当资金流出时降低信号
    if s_fund < -0.5 and abs_score < 1.3:
        norm_score *= 0.8
        abs_score = abs(norm_score)
        if abs_score < MIN_SIGNAL_THRESHOLD:
            return {
                'pred_up': None,
                'score': round(norm_score, 3),
                'confidence': 'skip_outflow',
                'reason': f'资金流出(fund_flow={s_fund:+.1f},score={norm_score:+.2f})',
                'dim_scores': dim_scores,
            }

    # ── 连续下跌趋势过滤 ──
    # 前月也跌 + 本月继续跌 + 信号不强 → 下跌趋势中，不抄底
    prev_chg = feat.get('prev_chg')
    if prev_chg is not None and prev_chg < -5 and this_chg < -5 and abs_score < 1.2:
        return {
            'pred_up': None,
            'score': round(norm_score, 3),
            'confidence': 'skip_trend',
            'reason': f'连续下跌趋势(前月{prev_chg:+.1f}%本月{this_chg:+.1f}%)',
            'dim_scores': dim_scores,
        }

    # ── 高位弱信号过滤：高位+弱信号不出手 ──
    if pos60 is not None and pos60 > 0.7 and abs_score < 1.2:
        return {
            'pred_up': None,
            'score': round(norm_score, 3),
            'confidence': 'skip_highpos',
            'reason': f'高位弱信号(pos60={pos60:.2f},score={norm_score:+.2f})',
            'dim_scores': dim_scores,
        }

    if abs_score >= 1.8:
        confidence = 'high'
    elif abs_score >= 0.8:
        confidence = 'medium'
    else:
        confidence = 'low'

    # 构建理由
    top_dims = sorted(dim_scores.items(), key=lambda x: abs(x[1]), reverse=True)
    top2 = [f"{k}={v:+.1f}" for k, v in top_dims[:2] if abs(v) > 0.3]
    direction = "涨" if pred_up else "跌"
    reason = f"预测{direction}({norm_score:+.2f}) {', '.join(top2)}"

    return {
        'pred_up': pred_up,
        'score': round(norm_score, 3),
        'confidence': confidence,
        'reason': reason,
        'dim_scores': dim_scores,
    }



# ═══════════════════════════════════════════════════════════
# 回测主函数
# ═══════════════════════════════════════════════════════════

def run_backtest(n_months=N_MONTHS, sample_limit=0):
    t0 = datetime.now()
    logger.info("=" * 80)
    logger.info("  月度深度预测模型 V1 — 多因子融合回测")
    logger.info("  回测月数: %d, 出手阈值: %.1f", n_months, MIN_SIGNAL_THRESHOLD)
    logger.info("=" * 80)

    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_months + 3) * 31 + 240)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=n_months * 31 + 31)

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("股票数: %d", len(all_codes))

    # ── 加载数据 ──
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 1. 个股K线
    logger.info("加载个股K线...")
    stock_klines = defaultdict(list)
    bs = 200
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,close_price,change_percent,"
            f"trading_volume,high_price,low_price "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'],
                'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
                'high': _to_float(r['high_price']),
                'low': _to_float(r['low_price']),
            })

    # 2. 指数K线
    logger.info("加载指数K线...")
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

    # 3. 资金流向
    logger.info("加载资金流向...")
    fund_flow_map = defaultdict(list)
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,big_net,big_net_pct,main_net_5day,net_flow "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date` DESC",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            fund_flow_map[r['stock_code']].append({
                'date': r['date'],
                'big_net': _safe_float(r['big_net']),
                'big_net_pct': _safe_float(r['big_net_pct']),
                'main_net_5day': _safe_float(r['main_net_5day']),
                'net_flow': _safe_float(r['net_flow']),
            })

    # 4. 概念板块映射
    logger.info("加载概念板块映射...")
    stock_boards = defaultdict(list)
    all_board_codes = set()
    codes_6 = [c.split('.')[0] for c in all_codes]
    full_map = {}
    for c in all_codes:
        c6 = c.split('.')[0]
        full_map[c6] = c

    for i in range(0, len(codes_6), bs):
        batch = codes_6[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_code, board_name "
            f"FROM stock_concept_board_stock "
            f"WHERE stock_code IN ({ph})",
            batch)
        for r in cur.fetchall():
            full = full_map.get(r['stock_code'], r['stock_code'])
            stock_boards[full].append({
                'board_code': r['board_code'],
                'board_name': r['board_name'],
            })
            all_board_codes.add(r['board_code'])

    # 5. 概念板块K线
    logger.info("加载概念板块K线 (%d个板块)...", len(all_board_codes))
    board_kline_map = defaultdict(list)
    bc_list = list(all_board_codes)
    for i in range(0, len(bc_list), bs):
        batch = bc_list[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT board_code,`date`,change_percent,close_price "
            f"FROM concept_board_kline "
            f"WHERE board_code IN ({ph}) AND `date`>=%s AND `date`<=%s "
            f"ORDER BY board_code,`date` ASC",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            board_kline_map[r['board_code']].append({
                'date': r['date'],
                'change_percent': _to_float(r['change_percent']),
                'close_price': _to_float(r['close_price']),
            })

    # 6. 大盘K线（上证指数，用于板块对比）
    market_klines_for_board = mkt_kl.get('000001.SH', [])

    conn.close()

    # 指数按月分组
    mkt_by_month = {}
    for ic, kl in mkt_kl.items():
        mkt_by_month[ic] = _group_by_month(kl)

    logger.info("数据加载完成, 开始月度回测...")
    logger.info("  个股K线: %d只, 资金流: %d只, 概念板块: %d个",
                len(stock_klines), len(fund_flow_map), len(board_kline_map))

    # ── 统计变量 ──
    all_month_samples = 0
    total_pred = 0
    total_correct = 0
    by_confidence = defaultdict(lambda: {'pred': 0, 'correct': 0})
    by_suffix = defaultdict(lambda: {'pred': 0, 'correct': 0, 'total': 0})
    by_ym = defaultdict(lambda: {'pred': 0, 'correct': 0, 'total': 0})
    by_direction = defaultdict(lambda: {'pred': 0, 'correct': 0})
    all_details = []
    # 记录参与预测的股票
    predicted_stocks = set()

    processed = 0
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 80:
            continue

        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
        idx_months = mkt_by_month.get(stock_idx, {})

        month_groups = _group_by_month(klines)
        sorted_months = sorted(month_groups.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])

        # 个股资金流
        ff_data = fund_flow_map.get(code, [])

        # 个股概念板块
        boards = stock_boards.get(code, [])

        for i in range(len(sorted_months) - 1):
            ym_this = sorted_months[i]
            ym_next = sorted_months[i + 1]
            this_days = month_groups[ym_this]
            next_days = month_groups[ym_next]

            if len(this_days) < 10 or len(next_days) < 10:
                continue

            dt_first = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_first < dt_cutoff:
                continue

            # ── 基础特征 ──
            this_pcts = [d['change_percent'] for d in this_days]
            this_chg = _compound_return(this_pcts)
            next_chg = _compound_return(
                [d['change_percent'] for d in next_days])
            actual_up = next_chg >= 0

            # 大盘本月涨跌
            mkt_days = idx_months.get(ym_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(
                    mkt_days, key=lambda x: x['date'])]
            ) if len(mkt_days) >= 10 else 0.0

            # 60日位置
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days
                               if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            # 前月涨跌
            prev_chg = None
            if i > 0:
                prev_ym = sorted_months[i - 1]
                prev_days = month_groups[prev_ym]
                if len(prev_days) >= 10:
                    prev_chg = _compound_return(
                        [k['change_percent'] for k in prev_days])

            # 量比
            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = sum(tv) / len(tv)
                ah = sum(hv) / len(hv)
                if ah > 0:
                    vol_ratio = at / ah

            # 尾周涨跌
            last_week_days = this_days[-5:]
            last_week_chg = _compound_return(
                [d['change_percent'] for d in last_week_days]
            ) if len(last_week_days) >= 3 else 0.0

            # 本月末日期（用于概念板块查询）
            month_end_date = this_days[-1]['date']

            # 资金流数据（本月内）
            month_ff = [f for f in ff_data
                        if f['date'] >= first_date and f['date'] <= month_end_date]
            # 按日期降序
            month_ff.sort(key=lambda x: x['date'], reverse=True)

            # 概念板块信号
            board_signals = []
            stock_board_signals = []
            for board in boards:
                bc = board['board_code']
                bk = board_kline_map.get(bc, [])
                if not bk:
                    continue
                bs_sig = _compute_board_strength_for_month(
                    bk, market_klines_for_board, month_end_date)
                if bs_sig:
                    board_signals.append(bs_sig)
                sb_sig = _compute_stock_board_strength(
                    klines, bk, month_end_date)
                if sb_sig:
                    stock_board_signals.append(sb_sig)

            all_month_samples += 1
            by_suffix[suffix]['total'] += 1
            ym_str = f"{ym_this[0]}-{ym_this[1]:02d}"
            by_ym[ym_str]['total'] += 1

            # ── 构建特征 ──
            # 大盘前月涨跌
            mkt_prev_chg = None
            if i > 0:
                prev_ym = sorted_months[i - 1]
                mkt_prev_days = idx_months.get(prev_ym, [])
                if len(mkt_prev_days) >= 10:
                    mkt_prev_chg = _compound_return(
                        [k['change_percent'] for k in sorted(
                            mkt_prev_days, key=lambda x: x['date'])])

            feat = {
                'this_chg': this_chg,
                'mkt_chg': mkt_chg,
                'mkt_prev_chg': mkt_prev_chg,
                'pos60': pos60,
                'prev_chg': prev_chg,
                'vol_ratio': vol_ratio,
                'suffix': suffix,
                'last_week_chg': last_week_chg,
                'fund_flow_data': month_ff,
                'board_signals': board_signals,
                'stock_board_signals': stock_board_signals,
            }

            # ── 预测 ──
            result = predict_monthly_direction(feat)

            if result['pred_up'] is not None:
                is_correct = result['pred_up'] == actual_up
                total_pred += 1
                if is_correct:
                    total_correct += 1
                predicted_stocks.add(code)

                by_confidence[result['confidence']]['pred'] += 1
                if is_correct:
                    by_confidence[result['confidence']]['correct'] += 1

                by_suffix[suffix]['pred'] += 1
                if is_correct:
                    by_suffix[suffix]['correct'] += 1

                by_ym[ym_str]['pred'] += 1
                if is_correct:
                    by_ym[ym_str]['correct'] += 1

                direction = 'up' if result['pred_up'] else 'down'
                by_direction[direction]['pred'] += 1
                if is_correct:
                    by_direction[direction]['correct'] += 1

                all_details.append({
                    'code': code, 'ym': ym_str,
                    'this_chg': round(this_chg, 2),
                    'next_chg': round(next_chg, 2),
                    'mkt_chg': round(mkt_chg, 2),
                    'pred_up': result['pred_up'],
                    'actual_up': actual_up,
                    'correct': is_correct,
                    'score': result['score'],
                    'confidence': result['confidence'],
                    'reason': result['reason'],
                    'dim_scores': result['dim_scores'],
                })

        processed += 1
        if processed % 1000 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    # ── 输出结果 ──
    elapsed = (datetime.now() - t0).total_seconds()
    _p = lambda c, t: f"{c / t * 100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  月度深度预测模型 V1 回测结果")
    logger.info("=" * 80)

    logger.info("  总可评估月样本: %d", all_month_samples)
    logger.info("  参与预测股票数: %d", len(predicted_stocks))
    logger.info("  预测命中: %s (%d/%d) 覆盖率%s",
                _p(total_correct, total_pred), total_correct, total_pred,
                _p(total_pred, all_month_samples))

    logger.info("")
    logger.info("  ── 按置信度 ──")
    for conf in ['high', 'medium', 'low']:
        s = by_confidence[conf]
        if s['pred'] > 0:
            logger.info("    %s: %s (%d/%d)", conf,
                         _p(s['correct'], s['pred']),
                         s['correct'], s['pred'])

    logger.info("")
    logger.info("  ── 按方向 ──")
    for d in ['up', 'down']:
        s = by_direction[d]
        if s['pred'] > 0:
            logger.info("    预测%s: %s (%d/%d)",
                         "涨" if d == 'up' else "跌",
                         _p(s['correct'], s['pred']),
                         s['correct'], s['pred'])

    logger.info("")
    logger.info("  ── 按市场 ──")
    for sfx in sorted(by_suffix.keys()):
        s = by_suffix[sfx]
        if s['pred'] > 0:
            logger.info("    %s: 预测%s (%d/%d) 覆盖%s",
                         sfx, _p(s['correct'], s['pred']),
                         s['correct'], s['pred'],
                         _p(s['pred'], s['total']))

    logger.info("")
    logger.info("  ── 按月份(滚动窗口) ──")
    rolling_results = []
    all_yms = sorted(by_ym.keys())
    for ym in all_yms:
        s = by_ym[ym]
        if s['pred'] > 0:
            acc = round(s['correct'] / s['pred'] * 100, 1)
            cov = round(s['pred'] / s['total'] * 100, 1) if s['total'] > 0 else 0
            rolling_results.append({
                'month': ym, 'accuracy': acc,
                'correct': s['correct'], 'total': s['pred'],
                'all_samples': s['total'], 'coverage': cov,
            })
            logger.info("    %s: 准确率%s (%d/%d) 覆盖%s (总样本%d)",
                         ym, f"{acc:.1f}%",
                         s['correct'], s['pred'],
                         f"{cov:.1f}%", s['total'])

    # 稳定性检验
    if rolling_results:
        accs = [r['accuracy'] for r in rolling_results if r['total'] >= 10]
        if len(accs) >= 2:
            avg_acc = sum(accs) / len(accs)
            std_acc = (sum((a - avg_acc) ** 2 for a in accs) / len(accs)) ** 0.5
            logger.info("")
            logger.info("  ── 稳定性检验 ──")
            logger.info("    月均准确率: %.1f%%", avg_acc)
            logger.info("    准确率标准差: %.1f%%", std_acc)
            logger.info("    最高月: %.1f%%  最低月: %.1f%%",
                         max(accs), min(accs))
            logger.info("    稳定性评级: %s",
                         "优" if std_acc < 5 else
                         "良" if std_acc < 10 else
                         "中" if std_acc < 15 else "差")

    # 维度贡献分析
    if all_details:
        logger.info("")
        logger.info("  ── 维度贡献分析 ──")
        for dim in WEIGHTS:
            correct_scores = [d['dim_scores'][dim] for d in all_details
                              if d['correct']]
            wrong_scores = [d['dim_scores'][dim] for d in all_details
                            if not d['correct']]
            if correct_scores and wrong_scores:
                logger.info("    %s: 正确预测均值=%.2f, 错误预测均值=%.2f, "
                             "区分度=%.2f",
                             dim, _mean(correct_scores), _mean(wrong_scores),
                             abs(_mean(correct_scores) - _mean(wrong_scores)))

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)

    return {
        'total_samples': all_month_samples,
        'total_pred': total_pred,
        'total_correct': total_correct,
        'accuracy': round(total_correct / total_pred * 100, 1)
                    if total_pred > 0 else 0,
        'coverage': round(total_pred / all_month_samples * 100, 1)
                    if all_month_samples > 0 else 0,
        'predicted_stocks': len(predicted_stocks),
        'by_confidence': dict(by_confidence),
        'by_month': rolling_results,
        'details': all_details,
    }


if __name__ == '__main__':
    run_backtest(n_months=N_MONTHS, sample_limit=0)
