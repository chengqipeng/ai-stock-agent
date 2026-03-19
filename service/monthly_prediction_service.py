"""
月度预测服务 — 多因子融合月度方向预测

从 monthly_deep_v1_backtest.py 提取核心算法，
封装为生产服务，结果写入 stock_weekly_prediction 表的 nm_* 列。

核心策略（5大维度融合）：
  1. 价格动量与均值回归
  2. 量价关系
  3. 大盘环境
  4. 资金流向
  5. 概念板块强弱
"""
import json
import math
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from calendar import monthrange

from dao import get_connection
from dao.stock_weekly_prediction_dao import (
    ensure_tables, batch_upsert_latest_predictions,
)
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float,
    _compound_return, _get_stock_index,
)

logger = logging.getLogger(__name__)

MIN_SIGNAL_THRESHOLD = 0.8

WEIGHTS = {
    'price_momentum': 0.8,
    'volume_price': 0.7,
    'market_env': 0.8,
    'fund_flow': 0.7,
    'concept_board': 0.9,
}


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
    score = 0.0
    this_chg = feat['this_chg']
    pos60 = feat['pos60']
    prev_chg = feat['prev_chg']
    last_week_chg = feat['last_week_chg']

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

    if this_chg > 30 and pos60 is not None and pos60 > 0.9:
        score -= 2.0
    elif this_chg > 25 and pos60 is not None and pos60 > 0.85:
        score -= 1.5
    elif this_chg > 20 and pos60 is not None and pos60 > 0.85:
        score -= 1.0

    if prev_chg is not None:
        if this_chg < -5 and prev_chg < -5:
            score += 0.8
        elif this_chg < -3 and prev_chg < -5:
            score += 0.4
        elif this_chg > 15 and prev_chg > 15:
            score -= 0.8
        elif this_chg > 10 and prev_chg > 10:
            score -= 0.5

    if this_chg < -5 and last_week_chg > 2:
        score += 0.4
    elif this_chg > 10 and last_week_chg < -3:
        score -= 0.3

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
    score = 0.0
    this_chg = feat['this_chg']
    vol_ratio = feat['vol_ratio']
    pos60 = feat['pos60']
    if vol_ratio is None:
        return 0.0

    if this_chg < -8 and vol_ratio < 0.5:
        score += 1.8
    elif this_chg < -5 and vol_ratio < 0.6:
        score += 1.2
        if pos60 is not None and pos60 < 0.3:
            score += 0.3
    elif this_chg < -3 and vol_ratio < 0.7:
        score += 0.6

    if this_chg > 20 and vol_ratio > 2.0 and pos60 is not None and pos60 > 0.9:
        score -= 1.0
    elif this_chg > 15 and vol_ratio > 2.5 and pos60 is not None and pos60 > 0.85:
        score -= 0.6

    if this_chg < -10 and vol_ratio > 1.5 and pos60 is not None and pos60 < 0.2:
        score += 0.5

    return max(-3.0, min(3.0, score))


# ═══════════════════════════════════════════════════════════
# 维度3: 大盘环境
# ═══════════════════════════════════════════════════════════

def _score_market_env(feat: dict) -> float:
    score = 0.0
    this_chg = feat['this_chg']
    mkt_chg = feat['mkt_chg']
    mkt_prev_chg = feat.get('mkt_prev_chg')

    mkt_trend_down = False
    if mkt_prev_chg is not None and mkt_prev_chg < -2 and mkt_chg < -2:
        mkt_trend_down = True

    if mkt_chg < -5:
        if mkt_trend_down:
            if this_chg < -15:
                score += 0.8
            elif this_chg < -8:
                score += 0.3
        else:
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
    elif mkt_chg > 3:
        if mkt_chg > 10 and this_chg < -3:
            score -= 0.8
        elif mkt_chg > 5 and this_chg < -5:
            score -= 0.3
        elif this_chg < -8:
            score += 0.4

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
    score = 0.0
    ff_data = feat.get('fund_flow_data', [])
    if not ff_data:
        return 0.0

    recent = ff_data[:10]
    if len(recent) < 3:
        return 0.0

    big_net_pcts = [_safe_float(f.get('big_net_pct', 0)) for f in recent]
    avg_big_pct = _mean(big_net_pcts)

    if len(recent) >= 6:
        recent_5 = big_net_pcts[:5]
        prev_5 = big_net_pcts[5:10] if len(big_net_pcts) >= 10 else big_net_pcts[5:]
        trend = _mean(recent_5) - _mean(prev_5) if prev_5 else 0
    else:
        trend = 0

    main_5d = [_safe_float(f.get('main_net_5day', 0)) for f in recent[:5]]
    avg_main_5d = _mean(main_5d)

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

    if trend > 3:
        score += 0.8
    elif trend > 1:
        score += 0.3
    elif trend < -3:
        score -= 0.8
    elif trend < -1:
        score -= 0.3

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
    if avg_board <= 35:
        score += 1.0
    elif avg_board <= 42:
        score += 0.5
    elif avg_board >= 65:
        score -= 0.8
    elif avg_board >= 58:
        score -= 0.3

    if avg_stock <= 35:
        score += 0.6
    elif avg_stock <= 42:
        score += 0.3
    elif avg_stock >= 65:
        score -= 0.5
    elif avg_stock >= 58:
        score -= 0.2

    momentums = [s['momentum'] for s in board_signals
                 if s is not None and 'momentum' in s]
    if momentums:
        avg_mom = _mean(momentums)
        if avg_mom < -0.5:
            score += 0.4
        elif avg_mom > 0.5:
            score -= 0.3

    return max(-3.0, min(3.0, score))


# ═══════════════════════════════════════════════════════════
# 综合决策引擎
# ═══════════════════════════════════════════════════════════

def predict_monthly_direction(feat: dict) -> dict:
    """月度方向预测综合决策。"""
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

    total_score = (
        s_price * WEIGHTS['price_momentum']
        + s_volume * WEIGHTS['volume_price']
        + s_market * WEIGHTS['market_env']
        + s_fund * WEIGHTS['fund_flow']
        + s_concept * WEIGHTS['concept_board']
    )
    w_sum = sum(WEIGHTS.values())
    norm_score = total_score / w_sum

    # 一致性加成
    dims = [s_price, s_volume, s_market, s_fund, s_concept]
    non_zero = [d for d in dims if abs(d) > 0.2]
    if len(non_zero) >= 3:
        pos_count = sum(1 for d in non_zero if d > 0)
        neg_count = sum(1 for d in non_zero if d < 0)
        consistency = max(pos_count, neg_count) / len(non_zero)
        if consistency >= 0.8:
            norm_score *= 1.15

    # 月度波动率修正
    this_chg = feat.get('this_chg', 0)
    if abs(this_chg) > 12:
        norm_score *= 1.1

    abs_score = abs(norm_score)

    if abs_score < MIN_SIGNAL_THRESHOLD:
        return {'pred_up': None, 'score': round(norm_score, 3),
                'confidence': 'skip', 'reason': f'信号不足({norm_score:+.2f})',
                'dim_scores': dim_scores}

    pred_up = norm_score > 0

    # 只做涨信号
    if not pred_up:
        return {'pred_up': None, 'score': round(norm_score, 3),
                'confidence': 'skip_down', 'reason': f'跳过跌信号({norm_score:+.2f})',
                'dim_scores': dim_scores}

    mkt_chg = feat.get('mkt_chg', 0)
    pos60 = feat.get('pos60')

    # 弱势股过滤
    if mkt_chg > 8 and this_chg < -3:
        return {'pred_up': None, 'score': round(norm_score, 3),
                'confidence': 'skip_weak',
                'reason': f'极端弱势股(大盘{mkt_chg:+.1f}%个股{this_chg:+.1f}%)',
                'dim_scores': dim_scores}

    excess = this_chg - mkt_chg
    if mkt_chg > -5 and excess < -12:
        penalty = 0.25 if excess < -20 else (0.4 if excess < -15 else 0.6)
        norm_score *= penalty
        abs_score = abs(norm_score)
        if abs_score < MIN_SIGNAL_THRESHOLD:
            return {'pred_up': None, 'score': round(norm_score, 3),
                    'confidence': 'skip_weak',
                    'reason': f'弱势股过滤(超额{excess:+.1f}%)',
                    'dim_scores': dim_scores}

    # 资金流出 + 弱信号
    if s_fund < -0.5 and abs_score < 1.3:
        norm_score *= 0.8
        abs_score = abs(norm_score)
        if abs_score < MIN_SIGNAL_THRESHOLD:
            return {'pred_up': None, 'score': round(norm_score, 3),
                    'confidence': 'skip_outflow',
                    'reason': f'资金流出(fund={s_fund:+.1f})',
                    'dim_scores': dim_scores}

    # 连续下跌趋势过滤
    prev_chg = feat.get('prev_chg')
    if prev_chg is not None and prev_chg < -5 and this_chg < -5 and abs_score < 1.2:
        return {'pred_up': None, 'score': round(norm_score, 3),
                'confidence': 'skip_trend',
                'reason': f'连续下跌(前月{prev_chg:+.1f}%本月{this_chg:+.1f}%)',
                'dim_scores': dim_scores}

    # 高位弱信号过滤
    if pos60 is not None and pos60 > 0.7 and abs_score < 1.2:
        return {'pred_up': None, 'score': round(norm_score, 3),
                'confidence': 'skip_highpos',
                'reason': f'高位弱信号(pos60={pos60:.2f})',
                'dim_scores': dim_scores}

    if abs_score >= 1.8:
        confidence = 'high'
    elif abs_score >= 0.8:
        confidence = 'medium'
    else:
        confidence = 'low'

    top_dims = sorted(dim_scores.items(), key=lambda x: abs(x[1]), reverse=True)
    top2 = [f"{k}={v:+.1f}" for k, v in top_dims[:2] if abs(v) > 0.3]
    reason = f"预测涨({norm_score:+.2f}) {', '.join(top2)}"

    return {
        'pred_up': pred_up,
        'score': round(norm_score, 3),
        'confidence': confidence,
        'reason': reason,
        'dim_scores': dim_scores,
    }


# ═══════════════════════════════════════════════════════════
# 批量月度预测主函数
# ═══════════════════════════════════════════════════════════

def _get_next_month(year, month):
    """返回下一个月的 (year, month)"""
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _get_month_date_range(year, month):
    """返回某月的日期范围字符串，如 '2026-04-01~2026-04-30'"""
    _, last_day = monthrange(year, month)
    return f"{year}-{month:02d}-01~{year}-{month:02d}-{last_day:02d}"


def run_batch_monthly_prediction(progress_callback=None):
    """批量月度预测主函数。

    流程：
    1. 加载全部股票K线、资金流、概念板块数据
    2. 按月分组，计算当前月特征
    3. 对每只股票进行月度方向预测
    4. 同时进行滚动回测计算准确率
    5. 将结果写入 stock_weekly_prediction 表的 nm_* 列

    Args:
        progress_callback: 进度回调 (total, done, up_count)
    """
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  批量月度预测服务启动")
    logger.info("=" * 70)

    ensure_tables()

    all_codes = _get_all_stock_codes()
    if not all_codes:
        logger.error("无股票数据")
        return None

    latest_date = _get_latest_trade_date()
    if not latest_date:
        logger.error("无法获取最新交易日")
        return None
    logger.info("最新交易日: %s, 股票数: %d", latest_date, len(all_codes))

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    # 加载足够历史数据（回测8个月 + 240天历史）
    n_backtest_months = 8
    dt_start = dt_end - timedelta(days=(n_backtest_months + 3) * 31 + 240)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=n_backtest_months * 31 + 31)

    # ── 加载数据 ──
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    bs = 200

    # 1. 个股K线
    logger.info("[1/6] 加载个股K线...")
    stock_klines = defaultdict(list)
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
                'date': r['date'] if isinstance(r['date'], str) else r['date'].strftime('%Y-%m-%d') if hasattr(r['date'], 'strftime') else str(r['date']),
                'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
                'high': _to_float(r['high_price']),
                'low': _to_float(r['low_price']),
            })

    # 2. 指数K线
    logger.info("[2/6] 加载指数K线...")
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
        d = r['date'] if isinstance(r['date'], str) else r['date'].strftime('%Y-%m-%d') if hasattr(r['date'], 'strftime') else str(r['date'])
        mkt_kl[r['stock_code']].append({
            'date': d,
            'change_percent': _to_float(r['change_percent']),
        })

    # 3. 资金流向
    logger.info("[3/6] 加载资金流向...")
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
            d = r['date'] if isinstance(r['date'], str) else r['date'].strftime('%Y-%m-%d') if hasattr(r['date'], 'strftime') else str(r['date'])
            fund_flow_map[r['stock_code']].append({
                'date': d,
                'big_net': _safe_float(r['big_net']),
                'big_net_pct': _safe_float(r['big_net_pct']),
                'main_net_5day': _safe_float(r['main_net_5day']),
                'net_flow': _safe_float(r['net_flow']),
            })

    # 4. 概念板块映射
    logger.info("[4/6] 加载概念板块映射...")
    stock_boards = defaultdict(list)
    all_board_codes = set()
    codes_6 = [c.split('.')[0] for c in all_codes]
    full_map = {c.split('.')[0]: c for c in all_codes}

    for i in range(0, len(codes_6), bs):
        batch = codes_6[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_code, board_name "
            f"FROM stock_concept_board_stock WHERE stock_code IN ({ph})", batch)
        for r in cur.fetchall():
            full = full_map.get(r['stock_code'], r['stock_code'])
            stock_boards[full].append({
                'board_code': r['board_code'],
                'board_name': r['board_name'],
            })
            all_board_codes.add(r['board_code'])

    # 5. 概念板块K线
    logger.info("[5/6] 加载概念板块K线 (%d个板块)...", len(all_board_codes))
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
            d = r['date'] if isinstance(r['date'], str) else r['date'].strftime('%Y-%m-%d') if hasattr(r['date'], 'strftime') else str(r['date'])
            board_kline_map[r['board_code']].append({
                'date': d,
                'change_percent': _to_float(r['change_percent']),
                'close_price': _to_float(r['close_price']),
            })

    market_klines_for_board = mkt_kl.get('000001.SH', [])
    conn.close()

    # 指数按月分组
    mkt_by_month = {}
    for ic, kl in mkt_kl.items():
        mkt_by_month[ic] = _group_by_month(kl)

    logger.info("[6/6] 数据加载完成, 开始月度预测+回测...")
    logger.info("  个股K线: %d只, 资金流: %d只, 概念板块: %d个",
                len(stock_klines), len(fund_flow_map), len(board_kline_map))

    # ── 当前月份信息 ──
    current_year, current_month = dt_end.year, dt_end.month
    next_year, next_month = _get_next_month(current_year, current_month)
    next_month_range = _get_month_date_range(next_year, next_month)
    logger.info("  当前月: %d-%02d, 预测目标月: %d-%02d (%s)",
                current_year, current_month, next_year, next_month, next_month_range)

    # ── 回测统计 + 当前月预测 ──
    bt_total_pred = 0
    bt_total_correct = 0
    bt_by_ym = defaultdict(lambda: {'pred': 0, 'correct': 0})

    predictions = []  # 当前月预测结果
    processed = 0

    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 80:
            continue

        stock_idx = _get_stock_index(code)
        idx_months = mkt_by_month.get(stock_idx, {})
        month_groups = _group_by_month(klines)
        sorted_months = sorted(month_groups.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])
        ff_data = fund_flow_map.get(code, [])
        boards = stock_boards.get(code, [])

        for i in range(len(sorted_months) - 1):
            ym_this = sorted_months[i]
            ym_next = sorted_months[i + 1]
            this_days = month_groups[ym_this]
            next_days = month_groups[ym_next]

            if len(this_days) < 10 or len(next_days) < 10:
                continue

            first_date_str = this_days[0]['date']
            dt_first = datetime.strptime(first_date_str, '%Y-%m-%d')
            if dt_first < dt_cutoff:
                continue

            # 基础特征
            this_pcts = [d['change_percent'] for d in this_days]
            this_chg = _compound_return(this_pcts)
            next_chg = _compound_return([d['change_percent'] for d in next_days])
            actual_up = next_chg >= 0

            mkt_days = idx_months.get(ym_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mkt_days, key=lambda x: x['date'])]
            ) if len(mkt_days) >= 10 else 0.0

            # 60日位置
            hist = [k for k in sorted_all if k['date'] < first_date_str]
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
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
                    prev_chg = _compound_return([k['change_percent'] for k in prev_days])

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

            month_end_date = this_days[-1]['date']

            # 资金流数据
            month_ff = [f for f in ff_data
                        if f['date'] >= first_date_str and f['date'] <= month_end_date]
            month_ff.sort(key=lambda x: x['date'], reverse=True)

            # 概念板块信号
            board_signals = []
            stock_board_signals = []
            for board in boards:
                bc = board['board_code']
                bk = board_kline_map.get(bc, [])
                if not bk:
                    continue
                bs_sig = _compute_board_strength_for_month(bk, market_klines_for_board, month_end_date)
                if bs_sig:
                    board_signals.append(bs_sig)
                sb_sig = _compute_stock_board_strength(klines, bk, month_end_date)
                if sb_sig:
                    stock_board_signals.append(sb_sig)

            # 大盘前月涨跌
            mkt_prev_chg = None
            if i > 0:
                prev_ym = sorted_months[i - 1]
                mkt_prev_days = idx_months.get(prev_ym, [])
                if len(mkt_prev_days) >= 10:
                    mkt_prev_chg = _compound_return(
                        [k['change_percent'] for k in sorted(mkt_prev_days, key=lambda x: x['date'])])

            feat = {
                'this_chg': this_chg, 'mkt_chg': mkt_chg, 'mkt_prev_chg': mkt_prev_chg,
                'pos60': pos60, 'prev_chg': prev_chg, 'vol_ratio': vol_ratio,
                'last_week_chg': last_week_chg, 'fund_flow_data': month_ff,
                'board_signals': board_signals, 'stock_board_signals': stock_board_signals,
            }

            result = predict_monthly_direction(feat)

            # 回测统计（历史月份）
            is_current_month = (ym_this == (current_year, current_month))
            if not is_current_month and result['pred_up'] is not None:
                is_correct = result['pred_up'] == actual_up
                bt_total_pred += 1
                if is_correct:
                    bt_total_correct += 1
                ym_str = f"{ym_this[0]}-{ym_this[1]:02d}"
                bt_by_ym[ym_str]['pred'] += 1
                if is_correct:
                    bt_by_ym[ym_str]['correct'] += 1

            # 当前月预测（最后一个月）— 注意：此处不会触发，因为当前月是 sorted_months 最后一个元素
            if is_current_month and result['pred_up'] is not None:
                predictions.append({
                    'stock_code': code,
                    'nm_pred_direction': 'UP',
                    'nm_confidence': result['confidence'],
                    'nm_strategy': 'monthly_deep_v1',
                    'nm_reason': result['reason'][:200] if result['reason'] else None,
                    'nm_composite_score': result['score'],
                    'nm_this_month_chg': round(this_chg, 2),
                    'nm_target_year': next_year,
                    'nm_target_month': next_month,
                    'nm_date_range': next_month_range,
                    'nm_dim_scores': json.dumps(result['dim_scores'], ensure_ascii=False),
                })

        # ── 当前月单独处理（循环不会覆盖最后一个月） ──
        ym_cur = (current_year, current_month)
        if ym_cur in month_groups:
            cur_days = month_groups[ym_cur]
            if len(cur_days) >= 10:
                first_date_str = cur_days[0]['date']
                month_end_date = cur_days[-1]['date']

                # 基础特征
                cur_pcts = [d['change_percent'] for d in cur_days]
                cur_chg = _compound_return(cur_pcts)

                mkt_days_cur = idx_months.get(ym_cur, [])
                mkt_chg_cur = _compound_return(
                    [k['change_percent'] for k in sorted(mkt_days_cur, key=lambda x: x['date'])]
                ) if len(mkt_days_cur) >= 10 else 0.0

                # 60日位置
                hist = [k for k in sorted_all if k['date'] < first_date_str]
                pos60_cur = None
                if len(hist) >= 20:
                    hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                    if hc:
                        ac = hc + [k['close'] for k in cur_days if k['close'] > 0]
                        mn, mx = min(ac), max(ac)
                        lc = cur_days[-1]['close']
                        if mx > mn and lc > 0:
                            pos60_cur = (lc - mn) / (mx - mn)

                # 前月涨跌
                prev_chg_cur = None
                idx_cur = sorted_months.index(ym_cur) if ym_cur in sorted_months else -1
                if idx_cur > 0:
                    prev_ym = sorted_months[idx_cur - 1]
                    prev_days = month_groups[prev_ym]
                    if len(prev_days) >= 10:
                        prev_chg_cur = _compound_return([k['change_percent'] for k in prev_days])

                # 量比
                vol_ratio_cur = None
                tv = [d['volume'] for d in cur_days if d['volume'] > 0]
                hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
                if tv and hv:
                    at = sum(tv) / len(tv)
                    ah = sum(hv) / len(hv)
                    if ah > 0:
                        vol_ratio_cur = at / ah

                # 尾周涨跌
                last_week_days = cur_days[-5:]
                last_week_chg_cur = _compound_return(
                    [d['change_percent'] for d in last_week_days]
                ) if len(last_week_days) >= 3 else 0.0

                # 资金流数据
                month_ff = [f for f in ff_data
                            if f['date'] >= first_date_str and f['date'] <= month_end_date]
                month_ff.sort(key=lambda x: x['date'], reverse=True)

                # 概念板块信号
                board_signals_cur = []
                stock_board_signals_cur = []
                for board in boards:
                    bc = board['board_code']
                    bk = board_kline_map.get(bc, [])
                    if not bk:
                        continue
                    bs_sig = _compute_board_strength_for_month(bk, market_klines_for_board, month_end_date)
                    if bs_sig:
                        board_signals_cur.append(bs_sig)
                    sb_sig = _compute_stock_board_strength(klines, bk, month_end_date)
                    if sb_sig:
                        stock_board_signals_cur.append(sb_sig)

                # 大盘前月涨跌
                mkt_prev_chg_cur = None
                if idx_cur > 0:
                    prev_ym = sorted_months[idx_cur - 1]
                    mkt_prev_days = idx_months.get(prev_ym, [])
                    if len(mkt_prev_days) >= 10:
                        mkt_prev_chg_cur = _compound_return(
                            [k['change_percent'] for k in sorted(mkt_prev_days, key=lambda x: x['date'])])

                feat_cur = {
                    'this_chg': cur_chg, 'mkt_chg': mkt_chg_cur, 'mkt_prev_chg': mkt_prev_chg_cur,
                    'pos60': pos60_cur, 'prev_chg': prev_chg_cur, 'vol_ratio': vol_ratio_cur,
                    'last_week_chg': last_week_chg_cur, 'fund_flow_data': month_ff,
                    'board_signals': board_signals_cur, 'stock_board_signals': stock_board_signals_cur,
                }

                result_cur = predict_monthly_direction(feat_cur)
                if result_cur['pred_up'] is not None:
                    predictions.append({
                        'stock_code': code,
                        'nm_pred_direction': 'UP',
                        'nm_confidence': result_cur['confidence'],
                        'nm_strategy': 'monthly_deep_v1',
                        'nm_reason': result_cur['reason'][:200] if result_cur['reason'] else None,
                        'nm_composite_score': result_cur['score'],
                        'nm_this_month_chg': round(cur_chg, 2),
                        'nm_target_year': next_year,
                        'nm_target_month': next_month,
                        'nm_date_range': next_month_range,
                        'nm_dim_scores': json.dumps(result_cur['dim_scores'], ensure_ascii=False),
                    })

        processed += 1
        if progress_callback and (processed % 500 == 0 or processed == len(all_codes)):
            progress_callback(len(all_codes), processed, len(predictions))
        if processed % 1000 == 0:
            logger.info("  已处理 %d/%d, 当前预测 %d 只...", processed, len(all_codes), len(predictions))

    # ── 回测准确率 ──
    bt_accuracy = round(bt_total_correct / bt_total_pred * 100, 1) if bt_total_pred > 0 else 0
    logger.info("  回测准确率: %.1f%% (%d/%d)", bt_accuracy, bt_total_correct, bt_total_pred)

    for ym in sorted(bt_by_ym.keys()):
        s = bt_by_ym[ym]
        if s['pred'] > 0:
            acc = round(s['correct'] / s['pred'] * 100, 1)
            logger.info("    %s: %.1f%% (%d/%d)", ym, acc, s['correct'], s['pred'])

    # ── 填充回测准确率到预测结果 ──
    for p in predictions:
        p['nm_backtest_accuracy'] = bt_accuracy
        p['nm_backtest_samples'] = bt_total_pred

    # ── 写入数据库 ──
    if predictions:
        logger.info("  写入数据库: %d 条月度预测...", len(predictions))
        batch_size = 500
        for i in range(0, len(predictions), batch_size):
            batch = predictions[i:i + batch_size]
            # 只更新 nm_* 列，不覆盖其他列
            _batch_update_monthly_predictions(batch)
    else:
        logger.warning("  无有效月度预测结果")

    elapsed = (datetime.now() - t_start).total_seconds()

    logger.info("=" * 70)
    logger.info("  批量月度预测完成")
    logger.info("  预测目标: %d-%02d (%s)", next_year, next_month, next_month_range)
    logger.info("  预测涨: %d 只", len(predictions))
    logger.info("  回测准确率: %.1f%% (%d/%d, %d个月)",
                bt_accuracy, bt_total_correct, bt_total_pred, len(bt_by_ym))
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 70)

    return {
        'target_year': next_year,
        'target_month': next_month,
        'date_range': next_month_range,
        'total_predicted': len(predictions),
        'backtest_accuracy': bt_accuracy,
        'backtest_samples': bt_total_pred,
        'backtest_months': len(bt_by_ym),
        'elapsed': round(elapsed, 1),
    }


def _batch_update_monthly_predictions(predictions: list[dict]):
    """批量更新 nm_* 列（仅更新月度预测字段，不影响其他列）。"""
    if not predictions:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        sql = """
            UPDATE stock_weekly_prediction SET
                nm_pred_direction = %(nm_pred_direction)s,
                nm_confidence = %(nm_confidence)s,
                nm_strategy = %(nm_strategy)s,
                nm_reason = %(nm_reason)s,
                nm_composite_score = %(nm_composite_score)s,
                nm_this_month_chg = %(nm_this_month_chg)s,
                nm_target_year = %(nm_target_year)s,
                nm_target_month = %(nm_target_month)s,
                nm_date_range = %(nm_date_range)s,
                nm_backtest_accuracy = %(nm_backtest_accuracy)s,
                nm_backtest_samples = %(nm_backtest_samples)s,
                nm_dim_scores = %(nm_dim_scores)s
            WHERE stock_code = %(stock_code)s
        """
        cur.executemany(sql, predictions)
        conn.commit()
        logger.info("  批量更新月度预测: %d 条", len(predictions))
    finally:
        cur.close()
        conn.close()
