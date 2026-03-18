#!/usr/bin/env python3
"""
技术指标增强回测 — MACD/KDJ/BOLL 对本周+下周预测的增益验证
==========================================================
目标：
  验证在现有 d3/d4 + 规则引擎基础上，加入 MACD/KDJ/BOLL 技术指标
  能否提高本周预测和下周预测的准确率。

方案设计：
  A组(baseline): 现有策略（d3/d4方向 + 规则引擎）
  B组(enhanced): 现有策略 + 技术指标信号修正

技术指标信号提取（基于周五收盘时的日K线）：
  1. MACD: DIF/DEA金叉死叉、MACD柱方向、零轴位置
  2. KDJ:  K/D/J值超买超卖、金叉死叉
  3. BOLL: 价格相对布林带位置（上轨/中轨/下轨）

增强策略：
  - 技术指标作为"置信度修正因子"，不改变原有方向预测
  - 当技术指标与原预测方向一致时，提升置信度
  - 当技术指标与原预测方向矛盾时，降低置信度或翻转弱信号

用法：
    python -m day_week_predicted.backtest.tech_indicator_enhancement_backtest
"""
import math
import sys
import logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')

from dao import get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

from service.weekly_prediction_service import (
    _to_float,
    _compound_return,
    _mean,
    _std,
    _classify_stock_behavior,
    _get_stock_strategy_profile,
    _predict_with_profile,
    _get_all_stock_codes,
    _get_latest_trade_date,
    _nw_extract_features,
    _nw_match_rule,
    _get_stock_index,
    _INDEX_MKT_THRESHOLD,
)


# ═══════════════════════════════════════════════════════════
# 技术指标计算（复用 technical_backtest 的纯函数实现）
# ═══════════════════════════════════════════════════════════

def _ema(data: list[float], period: int) -> list[float]:
    result = [0.0] * len(data)
    if not data:
        return result
    result[0] = data[0]
    k = 2 / (period + 1)
    for i in range(1, len(data)):
        result[i] = data[i] * k + result[i - 1] * (1 - k)
    return result


def _calc_macd(closes: list[float], fast=12, slow=26, signal=9) -> list[dict | None]:
    """计算MACD指标，返回与closes等长的列表"""
    if len(closes) < slow + signal:
        return [None] * len(closes)
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    dif = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]
    dea = _ema(dif, signal)
    result = []
    for i in range(len(closes)):
        bar = 2 * (dif[i] - dea[i])
        result.append({
            'DIF': round(dif[i], 4),
            'DEA': round(dea[i], 4),
            'MACD_BAR': round(bar, 4),
        })
    return result


def _calc_kdj(highs, lows, closes, n=9, m1=3, m2=3) -> list[dict | None]:
    """计算KDJ指标，返回与closes等长的列表"""
    result = [None] * len(closes)
    if len(closes) < n:
        return result
    k_prev, d_prev = 50.0, 50.0
    for i in range(n - 1, len(closes)):
        h_n = max(highs[i - n + 1:i + 1])
        l_n = min(lows[i - n + 1:i + 1])
        rsv = (closes[i] - l_n) / (h_n - l_n) * 100 if h_n != l_n else 50
        k = (m1 - 1) / m1 * k_prev + 1 / m1 * rsv
        d = (m2 - 1) / m2 * d_prev + 1 / m2 * k
        j = 3 * k - 2 * d
        result[i] = {'K': round(k, 2), 'D': round(d, 2), 'J': round(j, 2)}
        k_prev, d_prev = k, d
    return result


def _calc_boll(closes, period=20, mult=2) -> list[dict | None]:
    """计算布林带指标，返回与closes等长的列表"""
    result = [None] * len(closes)
    if len(closes) < period:
        return result
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1:i + 1]
        mid = sum(window) / period
        std = math.sqrt(sum((x - mid) ** 2 for x in window) / period)
        result[i] = {
            'MID': round(mid, 2),
            'UPPER': round(mid + mult * std, 2),
            'LOWER': round(mid - mult * std, 2),
        }
    return result


# ═══════════════════════════════════════════════════════════
# 技术指标信号提取
# ═══════════════════════════════════════════════════════════

def extract_tech_signals(closes, highs, lows, end_idx) -> dict:
    """从OHLC数据中提取技术指标信号（截至end_idx位置）。

    Args:
        closes: 全部收盘价序列（升序）
        highs: 全部最高价序列
        lows: 全部最低价序列
        end_idx: 截止位置索引（包含）

    Returns:
        dict with signal values:
        - macd_signal: -1(死叉/空头) ~ +1(金叉/多头)
        - kdj_signal: -1(超买/死叉) ~ +1(超卖/金叉)
        - boll_signal: -1(触上轨) ~ +1(触下轨，反弹预期)
        - tech_composite: 综合信号 (-1 ~ +1)
    """
    c = closes[:end_idx + 1]
    h = highs[:end_idx + 1]
    l = lows[:end_idx + 1]

    if len(c) < 35:  # MACD需要至少35根K线
        return {'macd_signal': 0, 'kdj_signal': 0, 'boll_signal': 0,
                'tech_composite': 0, 'valid': False}

    # ── MACD信号 ──
    macd_list = _calc_macd(c)
    macd_signal = 0.0
    if macd_list and macd_list[-1] is not None:
        cur = macd_list[-1]
        prev = macd_list[-2] if len(macd_list) >= 2 and macd_list[-2] else cur

        # 1. MACD柱方向（正=多头动能，负=空头动能）
        bar_direction = 1 if cur['MACD_BAR'] > 0 else -1

        # 2. MACD柱变化趋势（柱子变长=动能增强）
        bar_trend = 0
        if prev:
            if cur['MACD_BAR'] > prev['MACD_BAR']:
                bar_trend = 0.3  # 柱子变长（多头增强或空头减弱）
            elif cur['MACD_BAR'] < prev['MACD_BAR']:
                bar_trend = -0.3

        # 3. DIF/DEA金叉死叉
        cross = 0
        if prev:
            if prev['DIF'] <= prev['DEA'] and cur['DIF'] > cur['DEA']:
                cross = 0.5  # 金叉
            elif prev['DIF'] >= prev['DEA'] and cur['DIF'] < cur['DEA']:
                cross = -0.5  # 死叉

        # 4. 零轴位置
        zero_pos = 0.2 if cur['DIF'] > 0 else -0.2

        macd_signal = max(-1, min(1, bar_direction * 0.3 + bar_trend + cross + zero_pos))

    # ── KDJ信号 ──
    kdj_list = _calc_kdj(h, l, c)
    kdj_signal = 0.0
    if kdj_list and kdj_list[-1] is not None:
        cur_k = kdj_list[-1]
        prev_k = kdj_list[-2] if len(kdj_list) >= 2 and kdj_list[-2] else None

        # 1. 超买超卖
        if cur_k['J'] > 100:
            kdj_signal -= 0.4  # 超买 → 看跌
        elif cur_k['J'] < 0:
            kdj_signal += 0.4  # 超卖 → 看涨

        # 2. K/D金叉死叉
        if prev_k:
            if prev_k['K'] <= prev_k['D'] and cur_k['K'] > cur_k['D']:
                kdj_signal += 0.4  # 金叉
            elif prev_k['K'] >= prev_k['D'] and cur_k['K'] < cur_k['D']:
                kdj_signal -= 0.4  # 死叉

        # 3. K值位置
        if cur_k['K'] < 20:
            kdj_signal += 0.2  # 低位
        elif cur_k['K'] > 80:
            kdj_signal -= 0.2  # 高位

        kdj_signal = max(-1, min(1, kdj_signal))

    # ── BOLL信号 ──
    boll_list = _calc_boll(c)
    boll_signal = 0.0
    if boll_list and boll_list[-1] is not None:
        cur_b = boll_list[-1]
        price = c[-1]
        upper = cur_b['UPPER']
        lower = cur_b['LOWER']
        mid = cur_b['MID']

        if upper > lower:
            # 价格在布林带中的相对位置 (0=下轨, 1=上轨)
            boll_pos = (price - lower) / (upper - lower)

            # 触下轨 → 反弹预期(看涨)，触上轨 → 回调预期(看跌)
            if boll_pos <= 0.1:
                boll_signal = 0.8  # 触下轨，强反弹信号
            elif boll_pos <= 0.3:
                boll_signal = 0.4  # 接近下轨
            elif boll_pos >= 0.9:
                boll_signal = -0.8  # 触上轨，强回调信号
            elif boll_pos >= 0.7:
                boll_signal = -0.4  # 接近上轨
            else:
                # 中间区域，用偏离中轨的方向
                boll_signal = -0.2 * (boll_pos - 0.5) / 0.5

    # ── 综合信号 ──
    # 权重: MACD 0.35, KDJ 0.30, BOLL 0.35
    tech_composite = macd_signal * 0.35 + kdj_signal * 0.30 + boll_signal * 0.35
    tech_composite = max(-1, min(1, tech_composite))

    return {
        'macd_signal': round(macd_signal, 4),
        'kdj_signal': round(kdj_signal, 4),
        'boll_signal': round(boll_signal, 4),
        'tech_composite': round(tech_composite, 4),
        'valid': True,
    }


# ═══════════════════════════════════════════════════════════
# 增强预测策略
# ═══════════════════════════════════════════════════════════

def enhanced_this_week_predict(d4_chg, d3_chg, is_suspended, n_days,
                               daily_pcts, profile, tech_signals) -> tuple:
    """增强版本周预测：原始d3/d4策略 + 技术指标修正。

    修正规则：
    1. 原策略为弱信号(low confidence) + 技术指标强烈反向 → 翻转方向
    2. 原策略为中信号 + 技术指标同向 → 提升为高置信
    3. 原策略为中信号 + 技术指标反向 → 降为低置信
    4. 原策略为高信号 → 不修正（d3/d4强信号已经很准）

    Returns:
        (pred_up, confidence, strategy, reason)
    """
    # 先用原始策略预测
    pred_up, conf, strat, reason = _predict_with_profile(
        d4_chg, d3_chg, is_suspended, n_days, daily_pcts, profile)

    if not tech_signals.get('valid'):
        return pred_up, conf, strat, reason

    tc = tech_signals['tech_composite']
    tech_agrees = (tc > 0 and pred_up) or (tc < 0 and not pred_up)
    tech_strong = abs(tc) > 0.4

    # 高置信不修正
    if conf == 'high':
        suffix = f'+技术{"确认" if tech_agrees else "矛盾"}({tc:+.2f})'
        return pred_up, conf, strat + '_tech', reason + suffix

    # 中置信 + 技术同向 → 提升
    if conf == 'medium' and tech_agrees and abs(tc) > 0.3:
        return pred_up, 'high', strat + '_tech_boost', \
            reason + f'+技术确认({tc:+.2f})→高置信'

    # 中置信 + 技术反向 → 降级
    if conf == 'medium' and not tech_agrees and abs(tc) > 0.3:
        return pred_up, 'low', strat + '_tech_contra', \
            reason + f'+技术矛盾({tc:+.2f})→低置信'

    # 低置信 + 技术强烈反向 → 翻转
    if conf == 'low' and not tech_agrees and abs(tc) > 0.4:
        return not pred_up, 'low', strat + '_tech_flip', \
            reason + f'+技术翻转({tc:+.2f})'

    # 低置信 + 技术同向 → 提升到中
    if conf == 'low' and tech_agrees and abs(tc) > 0.3:
        return pred_up, 'medium', strat + '_tech_lift', \
            reason + f'+技术确认({tc:+.2f})→中置信'

    return pred_up, conf, strat, reason


def enhanced_next_week_predict(feat, tech_signals, rule_result) -> dict | None:
    """增强版下周预测：原始规则引擎 + 技术指标修正。

    修正规则（下周预测更保守）：
    1. Tier1规则 + 技术同向 → 保持高置信
    2. Tier1规则 + 技术强烈反向 → 降为参考级
    3. 无规则命中 + 技术强信号 → 新增Tier3预测
    4. Tier2规则 + 技术同向 → 提升为Tier1

    Returns:
        dict with 'pred_up', 'confidence', 'strategy', 'reason' or None
    """
    if not tech_signals.get('valid'):
        return rule_result

    tc = tech_signals['tech_composite']

    if rule_result is None:
        # 原规则未命中 → 技术指标独立预测（Tier 3）
        # 仅在信号极强时才触发（保守策略）
        if abs(tc) > 0.6:
            pred_up = tc > 0
            return {
                'pred_up': pred_up,
                'tier': 3,
                'confidence': 'reference',
                'strategy': 'tech_only',
                'reason': f'技术指标信号({tc:+.2f}): '
                          f'MACD={tech_signals["macd_signal"]:+.2f}, '
                          f'KDJ={tech_signals["kdj_signal"]:+.2f}, '
                          f'BOLL={tech_signals["boll_signal"]:+.2f}',
            }
        return None

    pred_up = rule_result['pred_up']
    tier = rule_result['tier']
    tech_agrees = (tc > 0 and pred_up) or (tc < 0 and not pred_up)

    result = dict(rule_result)

    if tier == 1:
        if tech_agrees:
            result['confidence'] = 'high'
            result['reason'] = rule_result['name'] + f'+技术确认({tc:+.2f})'
        elif abs(tc) > 0.4:
            result['confidence'] = 'reference'
            result['reason'] = rule_result['name'] + f'+技术矛盾({tc:+.2f})→参考'
        else:
            result['confidence'] = 'high'
    elif tier >= 2:
        if tech_agrees and abs(tc) > 0.3:
            result['tier'] = 1
            result['confidence'] = 'high'
            result['reason'] = rule_result['name'] + f'+技术确认({tc:+.2f})→高置信'
        elif not tech_agrees and abs(tc) > 0.3:
            result['confidence'] = 'low'
            result['reason'] = rule_result['name'] + f'+技术矛盾({tc:+.2f})→低置信'

    result['strategy'] = f'nw_rule_t{result.get("tier", tier)}_tech'
    return result


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def _load_backtest_data(stock_codes, start_date, end_date):
    """加载回测数据（含OHLC用于技术指标计算）"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 技术指标需要更长的历史数据（MACD需要~35天预热）
    dt_start = datetime.strptime(start_date, '%Y-%m-%d')
    lookback_start = (dt_start - timedelta(days=180)).strftime('%Y-%m-%d')

    # 1. 个股K线（含OHLC）
    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, open_price, close_price, "
            f"high_price, low_price, change_percent, trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [lookback_start, end_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'open': _to_float(row['open_price']),
                'close': _to_float(row['close_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'change_percent': _to_float(row['change_percent']),
                'volume': _to_float(row['trading_volume']),
            })

    # 2. 大盘K线（三大指数）
    market_klines_by_index = defaultdict(list)
    for idx_code in ('000001.SH', '399001.SZ', '899050.SZ'):
        cur.execute(
            "SELECT `date`, change_percent FROM stock_kline "
            "WHERE stock_code = %s AND `date` >= %s AND `date` <= %s "
            "ORDER BY `date`", (idx_code, lookback_start, end_date))
        for r in cur.fetchall():
            market_klines_by_index[idx_code].append({
                'date': r['date'],
                'change_percent': _to_float(r['change_percent']),
            })

    conn.close()

    # 3. 行业映射
    stock_sectors = {}
    try:
        from common.utils.sector_mapping_utils import parse_industry_list_md
        sector_mapping = parse_industry_list_md()
        for code in stock_codes:
            if code in sector_mapping:
                stock_sectors[code] = sector_mapping[code]
    except Exception:
        pass

    logger.info("[数据加载] %d只股票K线(含OHLC), 大盘%d个指数, 行业映射%d只",
                len(stock_klines), len(market_klines_by_index), len(stock_sectors))

    return {
        'stock_klines': dict(stock_klines),
        'market_klines_by_index': dict(market_klines_by_index),
        'stock_sectors': stock_sectors,
    }


def _get_market_chg_for_week(market_klines, iso_year, iso_week):
    """计算大盘指数某周的涨跌幅"""
    week_pcts = []
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        ical = dt.isocalendar()
        if ical[0] == iso_year and ical[1] == iso_week:
            week_pcts.append(k['change_percent'])
    return _compound_return(week_pcts) if len(week_pcts) >= 3 else 0.0


def _build_tech_signals_for_date(klines_asc, target_date):
    """为指定日期构建技术指标信号。

    找到target_date在klines_asc中的位置，用该位置之前的数据计算指标。
    """
    end_idx = None
    for i, k in enumerate(klines_asc):
        if k['date'] == target_date:
            end_idx = i
            break
        elif k['date'] > target_date:
            end_idx = i - 1 if i > 0 else None
            break

    if end_idx is None or end_idx < 35:
        return {'macd_signal': 0, 'kdj_signal': 0, 'boll_signal': 0,
                'tech_composite': 0, 'valid': False}

    closes = [k['close'] for k in klines_asc[:end_idx + 1]]
    highs = [k['high'] for k in klines_asc[:end_idx + 1]]
    lows = [k['low'] for k in klines_asc[:end_idx + 1]]

    # 过滤无效数据
    if any(c <= 0 for c in closes[-35:]):
        return {'macd_signal': 0, 'kdj_signal': 0, 'boll_signal': 0,
                'tech_composite': 0, 'valid': False}

    return extract_tech_signals(closes, highs, lows, len(closes) - 1)


# ═══════════════════════════════════════════════════════════
# A/B 回测主逻辑
# ═══════════════════════════════════════════════════════════

def _rate_str(ok, total):
    pct = ok / total * 100 if total > 0 else 0
    return f'{ok}/{total} = {pct:.1f}%'


def run_backtest(n_weeks=29, sample_limit=0):
    """运行A/B对比回测。

    Args:
        n_weeks: 回测周数
        sample_limit: 股票数量限制（0=全部）
    """
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  技术指标增强回测 (MACD/KDJ/BOLL)")
    logger.info("  A组=baseline(现有策略), B组=enhanced(+技术指标)")
    logger.info("=" * 70)

    # 1. 获取最新交易日和股票列表
    latest_date = _get_latest_trade_date()
    if not latest_date:
        logger.error("无法获取最新交易日")
        return
    logger.info("最新交易日: %s", latest_date)

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        import random
        random.seed(42)
        all_codes = random.sample(all_codes, min(sample_limit, len(all_codes)))
    logger.info("回测股票数: %d", len(all_codes))

    # 2. 计算回测时间范围
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=n_weeks * 7 + 7)
    start_date = dt_start.strftime('%Y-%m-%d')
    logger.info("回测区间: %s ~ %s (%d周)", start_date, latest_date, n_weeks)

    # 3. 加载数据
    data = _load_backtest_data(all_codes, start_date, latest_date)

    # 4. 逐股票逐周回测
    logger.info("[回测] 开始逐股票逐周A/B对比...")

    # ── 本周预测统计 ──
    tw_stats = {
        'A': {'correct': 0, 'total': 0,
               'by_conf': defaultdict(lambda: [0, 0]),
               'by_strat': defaultdict(lambda: [0, 0])},
        'B': {'correct': 0, 'total': 0,
               'by_conf': defaultdict(lambda: [0, 0]),
               'by_strat': defaultdict(lambda: [0, 0])},
    }

    # ── 下周预测统计 ──
    nw_stats = {
        'A': {'correct': 0, 'total': 0, 'covered': 0,
               'by_conf': defaultdict(lambda: [0, 0]),
               'by_tier': defaultdict(lambda: [0, 0])},
        'B': {'correct': 0, 'total': 0, 'covered': 0,
               'by_conf': defaultdict(lambda: [0, 0]),
               'by_tier': defaultdict(lambda: [0, 0])},
    }

    # ── 技术指标单独统计 ──
    tech_signal_stats = {
        'macd_agree_correct': 0, 'macd_agree_total': 0,
        'macd_disagree_correct': 0, 'macd_disagree_total': 0,
        'kdj_agree_correct': 0, 'kdj_agree_total': 0,
        'kdj_disagree_correct': 0, 'kdj_disagree_total': 0,
        'boll_agree_correct': 0, 'boll_agree_total': 0,
        'boll_disagree_correct': 0, 'boll_disagree_total': 0,
    }

    # ── 按周统计（用于LOWO） ──
    tw_week_stats = {'A': defaultdict(lambda: [0, 0]),
                     'B': defaultdict(lambda: [0, 0])}
    nw_week_stats = {'A': defaultdict(lambda: [0, 0]),
                     'B': defaultdict(lambda: [0, 0])}

    # ── 翻转/提升/降级统计 ──
    modification_stats = {
        'tw_flip': 0, 'tw_flip_correct': 0,
        'tw_boost': 0, 'tw_boost_correct': 0,
        'tw_degrade': 0, 'tw_degrade_correct': 0,
        'nw_new_coverage': 0, 'nw_new_correct': 0,
        'nw_tier_upgrade': 0, 'nw_tier_upgrade_correct': 0,
    }

    stocks_processed = 0
    for code in all_codes:
        klines = data['stock_klines'].get(code, [])
        if not klines or len(klines) < 60:
            continue

        klines.sort(key=lambda x: x['date'])

        # 行为分析
        behavior_klines = [k for k in klines if k['date'] < start_date]
        if len(behavior_klines) < 20:
            behavior_klines = klines[:20]
        behavior = _classify_stock_behavior(behavior_klines)

        sector = data['stock_sectors'].get(code, '')
        profile = _get_stock_strategy_profile(code, sector, behavior)

        # 大盘指数
        idx_code = _get_stock_index(code)
        market_klines = data['market_klines_by_index'].get(idx_code, [])

        # 按ISO周分组
        wg = defaultdict(list)
        for k in klines:
            if k['date'] < start_date or k['date'] > latest_date:
                continue
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())

        for wi, iw in enumerate(sorted_weeks):
            days = sorted(wg[iw], key=lambda x: x['date'])
            if len(days) < 3:
                continue

            pcts = [d['change_percent'] for d in days]
            weekly_chg = _compound_return(pcts)
            actual_up = weekly_chg >= 0

            d3 = _compound_return(pcts[:3])
            d4 = _compound_return(pcts[:4]) if len(days) >= 4 else None
            is_susp = all(p == 0 for p in pcts[:3])

            # 技术指标信号（基于本周最后一个交易日的数据）
            last_date = days[-1]['date']
            tech = _build_tech_signals_for_date(klines, last_date)

            # ════════════════════════════════════
            # 本周预测 A/B 对比
            # ════════════════════════════════════

            # A组: baseline
            a_up, a_conf, a_strat, a_reason = _predict_with_profile(
                d4, d3, is_susp, len(days), pcts, profile)
            a_correct = a_up == actual_up

            tw_stats['A']['total'] += 1
            tw_stats['A']['by_conf'][a_conf][1] += 1
            tw_stats['A']['by_strat'][a_strat][1] += 1
            tw_week_stats['A'][iw][1] += 1
            if a_correct:
                tw_stats['A']['correct'] += 1
                tw_stats['A']['by_conf'][a_conf][0] += 1
                tw_stats['A']['by_strat'][a_strat][0] += 1
                tw_week_stats['A'][iw][0] += 1

            # B组: enhanced
            b_up, b_conf, b_strat, b_reason = enhanced_this_week_predict(
                d4, d3, is_susp, len(days), pcts, profile, tech)
            b_correct = b_up == actual_up

            tw_stats['B']['total'] += 1
            tw_stats['B']['by_conf'][b_conf][1] += 1
            tw_stats['B']['by_strat'][b_strat][1] += 1
            tw_week_stats['B'][iw][1] += 1
            if b_correct:
                tw_stats['B']['correct'] += 1
                tw_stats['B']['by_conf'][b_conf][0] += 1
                tw_stats['B']['by_strat'][b_strat][0] += 1
                tw_week_stats['B'][iw][0] += 1

            # 修正统计
            if a_up != b_up:
                modification_stats['tw_flip'] += 1
                if b_correct:
                    modification_stats['tw_flip_correct'] += 1
            elif a_conf != b_conf:
                if b_conf == 'high' and a_conf in ('medium', 'low'):
                    modification_stats['tw_boost'] += 1
                    if b_correct:
                        modification_stats['tw_boost_correct'] += 1
                elif b_conf == 'low' and a_conf in ('medium', 'high'):
                    modification_stats['tw_degrade'] += 1
                    if b_correct:
                        modification_stats['tw_degrade_correct'] += 1

            # 技术指标单独有效性统计
            if tech.get('valid'):
                for ind_name, ind_val in [('macd', tech['macd_signal']),
                                           ('kdj', tech['kdj_signal']),
                                           ('boll', tech['boll_signal'])]:
                    if ind_val == 0:
                        continue
                    ind_up = ind_val > 0
                    agrees = ind_up == a_up
                    key_prefix = f'{ind_name}_agree' if agrees else f'{ind_name}_disagree'
                    tech_signal_stats[f'{key_prefix}_total'] += 1
                    if (agrees and a_correct) or (not agrees and not a_correct):
                        tech_signal_stats[f'{key_prefix}_correct'] += 1

            # ════════════════════════════════════
            # 下周预测 A/B 对比
            # ════════════════════════════════════
            # 需要下一周的实际数据来验证
            if wi + 1 >= len(sorted_weeks):
                continue
            next_iw = sorted_weeks[wi + 1]
            next_days = sorted(wg[next_iw], key=lambda x: x['date'])
            if len(next_days) < 3:
                continue

            next_pcts = [d['change_percent'] for d in next_days]
            next_weekly_chg = _compound_return(next_pcts)
            next_actual_up = next_weekly_chg >= 0

            # 大盘本周涨跌
            market_chg = _get_market_chg_for_week(market_klines, iw[0], iw[1])

            # 特征提取
            feat = _nw_extract_features(pcts, market_chg, market_index=idx_code)

            # A组: baseline规则引擎
            a_rule = _nw_match_rule(feat)
            nw_stats['A']['total'] += 1
            nw_week_stats['A'][iw][1] += 1
            if a_rule is not None:
                nw_stats['A']['covered'] += 1
                a_nw_correct = a_rule['pred_up'] == next_actual_up
                a_tier = a_rule['tier']
                a_nw_conf = 'high' if a_tier == 1 else 'reference'
                nw_stats['A']['by_tier'][a_tier][1] += 1
                nw_stats['A']['by_conf'][a_nw_conf][1] += 1
                if a_nw_correct:
                    nw_stats['A']['correct'] += 1
                    nw_stats['A']['by_tier'][a_tier][0] += 1
                    nw_stats['A']['by_conf'][a_nw_conf][0] += 1
                    nw_week_stats['A'][iw][0] += 1

            # B组: enhanced规则引擎+技术指标
            b_rule_result = enhanced_next_week_predict(feat, tech, a_rule)
            nw_stats['B']['total'] += 1
            nw_week_stats['B'][iw][1] += 1
            if b_rule_result is not None:
                nw_stats['B']['covered'] += 1
                b_nw_correct = b_rule_result['pred_up'] == next_actual_up
                b_tier = b_rule_result.get('tier', 3)
                b_nw_conf = b_rule_result.get('confidence', 'reference')
                nw_stats['B']['by_tier'][b_tier][1] += 1
                nw_stats['B']['by_conf'][b_nw_conf][1] += 1
                if b_nw_correct:
                    nw_stats['B']['correct'] += 1
                    nw_stats['B']['by_tier'][b_tier][0] += 1
                    nw_stats['B']['by_conf'][b_nw_conf][0] += 1
                    nw_week_stats['B'][iw][0] += 1

                # 新增覆盖统计
                if a_rule is None and b_rule_result is not None:
                    modification_stats['nw_new_coverage'] += 1
                    if b_nw_correct:
                        modification_stats['nw_new_correct'] += 1
                # Tier升级统计
                if a_rule is not None and b_tier < a_rule['tier']:
                    modification_stats['nw_tier_upgrade'] += 1
                    if b_nw_correct:
                        modification_stats['nw_tier_upgrade_correct'] += 1

        stocks_processed += 1
        if stocks_processed % 500 == 0:
            logger.info("  已处理 %d/%d 只股票...", stocks_processed, len(all_codes))

    logger.info("  回测完成: %d只股票", stocks_processed)

    # ═══════════════════════════════════════════════════════════
    # 5. 输出结果
    # ═══════════════════════════════════════════════════════════
    _print_results(tw_stats, nw_stats, tw_week_stats, nw_week_stats,
                   tech_signal_stats, modification_stats, t_start)


def _print_results(tw_stats, nw_stats, tw_week_stats, nw_week_stats,
                   tech_signal_stats, mod_stats, t_start):
    """输出A/B对比结果"""

    logger.info("")
    logger.info("=" * 70)
    logger.info("  回测结果: A/B 对比")
    logger.info("=" * 70)

    # ── 本周预测 ──
    logger.info("")
    logger.info("【1. 本周预测准确率】")
    for group in ['A', 'B']:
        s = tw_stats[group]
        acc = s['correct'] / s['total'] * 100 if s['total'] > 0 else 0
        label = 'baseline' if group == 'A' else 'enhanced'
        logger.info("  %s组(%s): %s", group, label, _rate_str(s['correct'], s['total']))

    # LOWO
    for group in ['A', 'B']:
        week_accs = []
        for iw, (ok, n) in tw_week_stats[group].items():
            if n > 0:
                week_accs.append(ok / n * 100)
        lowo = _mean(week_accs) if week_accs else 0
        label = 'baseline' if group == 'A' else 'enhanced'
        logger.info("  %s组(%s) LOWO: %.1f%% (%d周)", group, label, lowo, len(week_accs))

    # 按置信度
    logger.info("")
    logger.info("  按置信度:")
    for conf in ['high', 'medium', 'low']:
        a_ok, a_n = tw_stats['A']['by_conf'].get(conf, [0, 0])
        b_ok, b_n = tw_stats['B']['by_conf'].get(conf, [0, 0])
        if a_n > 0 or b_n > 0:
            a_str = _rate_str(a_ok, a_n) if a_n > 0 else 'N/A'
            b_str = _rate_str(b_ok, b_n) if b_n > 0 else 'N/A'
            diff = ''
            if a_n > 0 and b_n > 0:
                a_pct = a_ok / a_n * 100
                b_pct = b_ok / b_n * 100
                diff = f'  Δ={b_pct - a_pct:+.1f}pp'
            logger.info("    %-8s A=%s  B=%s%s", conf, a_str, b_str, diff)

    # ── 下周预测 ──
    logger.info("")
    logger.info("【2. 下周预测准确率】")
    for group in ['A', 'B']:
        s = nw_stats[group]
        covered = s['covered']
        total = s['total']
        correct = s['correct']
        coverage = covered / total * 100 if total > 0 else 0
        acc = correct / covered * 100 if covered > 0 else 0
        label = 'baseline' if group == 'A' else 'enhanced'
        logger.info("  %s组(%s): 覆盖%d/%d(%.1f%%), 准确%s",
                    group, label, covered, total, coverage,
                    _rate_str(correct, covered))

    # 按Tier
    logger.info("")
    logger.info("  按Tier:")
    for tier in [1, 2, 3]:
        a_ok, a_n = nw_stats['A']['by_tier'].get(tier, [0, 0])
        b_ok, b_n = nw_stats['B']['by_tier'].get(tier, [0, 0])
        if a_n > 0 or b_n > 0:
            a_str = _rate_str(a_ok, a_n) if a_n > 0 else 'N/A'
            b_str = _rate_str(b_ok, b_n) if b_n > 0 else 'N/A'
            logger.info("    Tier%d  A=%s  B=%s", tier, a_str, b_str)

    # ── 技术指标有效性 ──
    logger.info("")
    logger.info("【3. 技术指标单独有效性】")
    for ind in ['macd', 'kdj', 'boll']:
        agree_ok = tech_signal_stats[f'{ind}_agree_correct']
        agree_n = tech_signal_stats[f'{ind}_agree_total']
        dis_ok = tech_signal_stats[f'{ind}_disagree_correct']
        dis_n = tech_signal_stats[f'{ind}_disagree_total']
        logger.info("  %s: 同向时baseline正确=%s, 反向时baseline错误=%s",
                    ind.upper(),
                    _rate_str(agree_ok, agree_n),
                    _rate_str(dis_ok, dis_n))

    # ── 修正效果 ──
    logger.info("")
    logger.info("【4. 技术指标修正效果】")
    logger.info("  本周预测:")
    logger.info("    翻转: %s", _rate_str(mod_stats['tw_flip_correct'],
                                          mod_stats['tw_flip']))
    logger.info("    提升: %s", _rate_str(mod_stats['tw_boost_correct'],
                                          mod_stats['tw_boost']))
    logger.info("    降级: %s", _rate_str(mod_stats['tw_degrade_correct'],
                                          mod_stats['tw_degrade']))
    logger.info("  下周预测:")
    logger.info("    新增覆盖: %s", _rate_str(mod_stats['nw_new_correct'],
                                              mod_stats['nw_new_coverage']))
    logger.info("    Tier升级: %s", _rate_str(mod_stats['nw_tier_upgrade_correct'],
                                              mod_stats['nw_tier_upgrade']))

    # ── 综合判定 ──
    logger.info("")
    logger.info("=" * 70)
    logger.info("  综合判定")
    logger.info("=" * 70)

    a_tw_acc = tw_stats['A']['correct'] / tw_stats['A']['total'] * 100 if tw_stats['A']['total'] > 0 else 0
    b_tw_acc = tw_stats['B']['correct'] / tw_stats['B']['total'] * 100 if tw_stats['B']['total'] > 0 else 0
    tw_delta = b_tw_acc - a_tw_acc

    a_nw_acc = nw_stats['A']['correct'] / nw_stats['A']['covered'] * 100 if nw_stats['A']['covered'] > 0 else 0
    b_nw_acc = nw_stats['B']['correct'] / nw_stats['B']['covered'] * 100 if nw_stats['B']['covered'] > 0 else 0
    nw_delta = b_nw_acc - a_nw_acc

    a_nw_cov = nw_stats['A']['covered'] / nw_stats['A']['total'] * 100 if nw_stats['A']['total'] > 0 else 0
    b_nw_cov = nw_stats['B']['covered'] / nw_stats['B']['total'] * 100 if nw_stats['B']['total'] > 0 else 0
    cov_delta = b_nw_cov - a_nw_cov

    logger.info("  本周预测: A=%.1f%% → B=%.1f%% (Δ=%+.1f%%)", a_tw_acc, b_tw_acc, tw_delta)
    logger.info("  下周准确: A=%.1f%% → B=%.1f%% (Δ=%+.1f%%)", a_nw_acc, b_nw_acc, nw_delta)
    logger.info("  下周覆盖: A=%.1f%% → B=%.1f%% (Δ=%+.1f%%)", a_nw_cov, b_nw_cov, cov_delta)

    if tw_delta > 0.5 or nw_delta > 1.0 or cov_delta > 3.0:
        logger.info("  ✅ 技术指标增强有正向效果，建议整合到生产代码")
    elif tw_delta > -0.5 and nw_delta > -1.0:
        logger.info("  ⚠️  效果中性，可选择性整合（如仅用于下周预测覆盖率提升）")
    else:
        logger.info("  ❌ 技术指标增强无正向效果，不建议整合")

    elapsed = (datetime.now() - t_start).total_seconds()
    logger.info("  回测耗时: %.1fs", elapsed)
    logger.info("=" * 70)

    return {
        'tw_baseline_acc': round(a_tw_acc, 1),
        'tw_enhanced_acc': round(b_tw_acc, 1),
        'tw_delta': round(tw_delta, 1),
        'nw_baseline_acc': round(a_nw_acc, 1),
        'nw_enhanced_acc': round(b_nw_acc, 1),
        'nw_delta': round(nw_delta, 1),
        'nw_coverage_delta': round(cov_delta, 1),
    }


if __name__ == '__main__':
    # 默认: 全量股票, 29周回测
    # 快速测试: python -m ... 100  (只取100只股票)
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    weeks = int(sys.argv[2]) if len(sys.argv) > 2 else 29
    run_backtest(n_weeks=weeks, sample_limit=limit)
