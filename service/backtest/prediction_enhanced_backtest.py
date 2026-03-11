#!/usr/bin/env python3
"""
增强预测回测 v7：多因子综合分析 + 板块个性化预测逻辑 + 美股隔夜信号

在 v4(technical_backtest) 和 v5(sector_peer_backtest) 基础上重构：
1. MACD/KDJ/BOLL/量价/RS/板块同行 6大数据源综合分析
2. 板块分类从 stock_industry_list.md 获取（不调用API）
3. 板块个性化：因子权重、方向决策阈值、同行联动强度均按板块定制
4. 改进方向决策：基于板块特性的自适应阈值 + 多信号融合
5. v7新增：美股隔夜信号因子（第11因子），从数据库查询美股三大指数K线

核心改进点（相比v4/v5/v6）：
- v4问题：统一因子权重，不区分板块特性
- v5问题：板块同行信号固定0.15权重，反而拉低准确率
- v6方案：每个板块独立的因子权重矩阵 + 自适应同行联动强度
- v6b方案：基于因子有效性分析校准权重 + 反转无效因子
- v7方案：新增美股隔夜信号，按板块敏感度差异化配置（科技1.5/医药0.1）
"""

import asyncio
import logging
import math
from collections import defaultdict
from datetime import datetime
from typing import Optional

from dao.stock_kline_dao import get_kline_data
from common.utils.sector_mapping_utils import parse_industry_list_md, get_sector_peers
from service.eastmoney.indices.us_market_db_query import (
    preload_us_kline_map,
    get_us_overnight_signal_fast,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 板块个性化配置（基于 sector_scoring_analysis.json 回测数据）
# ═══════════════════════════════════════════════════════════

# 板块同行联动强度（基于v6回测因子有效性分析校准）
# 关键发现：科技/新能源/制造/化工的同行信号是反向指标（一致时准确率<矛盾时）
# 有色金属/医药的同行信号是正向指标（一致时准确率>矛盾时）
# 策略：正向板块用正权重，反向板块用负权重（反转同行信号）
_SECTOR_PEER_WEIGHT = {
    "科技": -0.15,      # 反向：一致35.8% vs 矛盾54.8%，反转使用
    "有色金属": 0.15,   # 正向：一致60.3% vs 矛盾50.6%
    "新能源": -0.10,    # 反向：一致39.7% vs 矛盾52.1%
    "汽车": 0.0,        # 中性：一致56.8% ≈ 矛盾56.2%，无信息量
    "化工": -0.12,      # 反向：一致45.0% vs 矛盾59.4%
    "医药": 0.12,       # 正向：一致56.1% vs 矛盾36.8%
    "制造": -0.10,      # 反向：一致33.3% vs 矛盾52.9%
}

# 板块个性化10因子权重（基于v6回测因子有效性分析校准）
# 权重规则：有效因子(>52%)给高权重，无效因子(<48%)给负权重或0，中性因子给低权重
# 负权重 = 反转该因子信号（因子看涨时实际看跌概率更高）
_SECTOR_FACTOR_WEIGHTS = {
    # 科技：reversion 57.5%, rsi 61.8%, boll 57.1%, market 60.4%, streak 63.8% 有效
    #       macd 46.0%, vp 35.7%, fund 41.2%, trend_bias 40.2% 无效（反转）
    #       美股隔夜：avg_corr=+0.165，但v7回测39.5%方向一致率→反向指标
    "科技": {
        "reversion": 1.3, "rsi": 1.2, "kdj": 0.1, "macd": -0.3,
        "boll": 1.0, "vp": -0.3, "fund": -0.5, "market": 1.0,
        "streak": 1.2, "trend_bias": -0.4, "us_overnight": -0.8,
    },
    # 有色金属：fund 55.4% 唯一有效
    #           reversion 43.8%, rsi 44.2%, kdj 46.7%, boll 44.8%, vp 23.1%,
    #           market 45.7%, streak 46.7% 全部无效（反转）
    #           美股隔夜：avg_corr=+0.093，紫金矿业r=0.30最强
    "有色金属": {
        "reversion": -0.4, "rsi": -0.3, "kdj": -0.2, "macd": 0.1,
        "boll": -0.3, "vp": -0.5, "fund": 0.8, "market": -0.2,
        "streak": -0.2, "trend_bias": 0.1, "us_overnight": 0.8,
    },
    # 汽车：reversion 59.7%, kdj 63.3%, boll 61.1%, market 66.7%, streak 65.2% 有效
    #       macd 41.5%, fund 40.8%, trend_bias 43.2% 无效（反转）
    #       美股隔夜：avg_corr=+0.029，v7回测38.7%→反向指标
    "汽车": {
        "reversion": 1.2, "rsi": 0.3, "kdj": 1.2, "macd": -0.4,
        "boll": 1.1, "vp": 0.1, "fund": -0.4, "market": 1.3,
        "streak": 1.2, "trend_bias": -0.3, "us_overnight": -0.5,
    },
    # 新能源：rsi 57.1%, kdj 54.5%, macd 53.3%, vp 80.0%, fund 53.0%,
    #         market 69.4%, streak 54.5%, trend_bias 52.4% 几乎全有效
    #         美股隔夜：avg_corr=+0.096，v7回测48.2%→中性，降低权重
    "新能源": {
        "reversion": 0.1, "rsi": 1.0, "kdj": 0.8, "macd": 0.6,
        "boll": 0.1, "vp": 1.5, "fund": 0.6, "market": 1.3,
        "streak": 0.8, "trend_bias": 0.4, "us_overnight": 0.1,
    },
    # 医药：rsi 56.6%, macd 61.4%, fund 59.8% 有效
    #       reversion 38.6%, kdj 41.5%, market 44.4%, streak 40.7% 无效（反转）
    #       美股隔夜：avg_corr=+0.013，几乎无相关
    "医药": {
        "reversion": -0.5, "rsi": 1.0, "kdj": -0.4, "macd": 1.2,
        "boll": 0.1, "vp": 0.1, "fund": 1.0, "market": -0.3,
        "streak": -0.4, "trend_bias": 0.1, "us_overnight": 0.1,
    },
    # 化工：reversion 53.0%, market 55.6%, trend_bias 54.2% 有效
    #       fund 38.5% 无效（反转）
    #       美股隔夜：avg_corr=+0.097，v7回测数据不足，保守处理
    "化工": {
        "reversion": 0.8, "rsi": 0.1, "kdj": 0.1, "macd": 0.1,
        "boll": 0.1, "vp": 0.1, "fund": -0.5, "market": 0.8,
        "streak": 0.1, "trend_bias": 0.8, "us_overnight": 0.3,
    },
    # 制造：reversion 56.7%, rsi 62.9%, macd 53.0%, fund 58.2%, market 62.5% 有效
    #       boll 42.4%, streak 45.8% 无效（反转）
    #       美股隔夜：avg_corr=+0.121
    "制造": {
        "reversion": 1.0, "rsi": 1.2, "kdj": 0.2, "macd": 0.6,
        "boll": -0.3, "vp": 0.1, "fund": 1.0, "market": 1.2,
        "streak": -0.3, "trend_bias": 0.1, "us_overnight": 0.8,
    },
}

# 默认因子权重（未匹配板块时使用，与v4一致）
_DEFAULT_FACTOR_WEIGHTS = {
    "reversion": 1.0, "rsi": 0.7, "kdj": 0.5, "macd": 0.6,
    "boll": 0.6, "vp": 0.5, "fund": 0.7, "market": 0.4,
    "streak": 0.6, "trend_bias": 0.3, "us_overnight": 0.5,
}

# 板块个性化方向决策阈值（基于v6回测校准）
# 关键发现：v6预测上涨736次(49.7%准确) vs 下跌373次(47.2%准确)
# 上涨预测过多但准确率不高，需要提高看涨阈值减少无效上涨预测
# 有色金属55.1%准确率最高，可以保持激进；科技45.5%最低，需要保守
_SECTOR_DIRECTION_THRESHOLDS = {
    "科技": {"bullish": 2.0, "bearish": -1.5, "z_revert": 1.0, "default_up": False},
    "有色金属": {"bullish": 0.5, "bearish": -1.0, "z_revert": 2.0, "default_up": True},
    "汽车": {"bullish": 1.0, "bearish": -1.5, "z_revert": 1.2, "default_up": True},
    "新能源": {"bullish": 1.5, "bearish": -1.5, "z_revert": 1.2, "default_up": False},
    "医药": {"bullish": 2.5, "bearish": -1.0, "z_revert": 0.8, "default_up": False},
    "化工": {"bullish": 1.0, "bearish": -1.5, "z_revert": 1.3, "default_up": True},
    "制造": {"bullish": 2.0, "bearish": -1.0, "z_revert": 1.0, "default_up": False},
}

_DEFAULT_DIRECTION_THRESHOLDS = {
    "bullish": 1.5, "bearish": -1.5, "z_revert": 1.2, "default_up": False,
}


def _get_factor_weights(sector: str | None) -> dict:
    if sector and sector in _SECTOR_FACTOR_WEIGHTS:
        return _SECTOR_FACTOR_WEIGHTS[sector]
    return dict(_DEFAULT_FACTOR_WEIGHTS)


def _get_direction_thresholds(sector: str | None) -> dict:
    if sector and sector in _SECTOR_DIRECTION_THRESHOLDS:
        return _SECTOR_DIRECTION_THRESHOLDS[sector]
    return dict(_DEFAULT_DIRECTION_THRESHOLDS)


def _get_peer_weight(sector: str | None) -> float:
    if sector and sector in _SECTOR_PEER_WEIGHT:
        return _SECTOR_PEER_WEIGHT[sector]
    return 0.10


# ═══════════════════════════════════════════════════════════
# 技术指标计算（复用 technical_backtest 中的实现）
# ═══════════════════════════════════════════════════════════

def _ema(data: list[float], period: int) -> list[float]:
    if not data:
        return []
    result = [0.0] * len(data)
    k = 2 / (period + 1)
    result[0] = data[0]
    for i in range(1, len(data)):
        result[i] = data[i] * k + result[i - 1] * (1 - k)
    return result


def _sma(data: list[float], period: int) -> list[float]:
    result = [0.0] * len(data)
    if len(data) < period:
        return result
    s = sum(data[:period])
    result[period - 1] = s / period
    for i in range(period, len(data)):
        s += data[i] - data[i - period]
        result[i] = s / period
    return result


def _calc_macd(closes: list[float], fast=12, slow=26, signal=9) -> list[dict]:
    if len(closes) < slow + signal:
        return []
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    dif = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]
    dea = _ema(dif, signal)
    result = []
    for i in range(len(closes)):
        bar = 2 * (dif[i] - dea[i])
        result.append({'DIF': dif[i], 'DEA': dea[i], 'MACD柱': bar})
    return result


def _calc_kdj(highs, lows, closes, n=9, m1=3, m2=3) -> list[dict]:
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


def _calc_boll(closes, period=20, mult=2) -> list[dict | None]:
    result = [None] * len(closes)
    if len(closes) < period:
        return result
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1:i + 1]
        mid = sum(window) / period
        std = math.sqrt(sum((x - mid) ** 2 for x in window) / period)
        result[i] = {
            '上轨': mid + mult * std,
            '中轨': mid,
            '下轨': mid - mult * std,
        }
    return result


# ═══════════════════════════════════════════════════════════
# 板块同行走势计算（改进版：按板块联动强度加权）
# ═══════════════════════════════════════════════════════════

def _compute_peer_trend(peer_klines: dict[str, list[dict]],
                        score_date: str, lookback: int = 5) -> dict:
    """计算板块同行走势信号。"""
    if not peer_klines:
        return {'信号分': 0.0, '有效同行数': 0}

    peer_changes_today = []
    peer_changes_nd = []
    up_count = 0
    down_count = 0
    valid = 0

    for code, klines in peer_klines.items():
        date_idx = None
        for i, k in enumerate(klines):
            if k['date'] == score_date:
                date_idx = i
                break
            elif k['date'] > score_date:
                date_idx = i - 1 if i > 0 else None
                break
        if date_idx is None or date_idx < 1:
            continue

        valid += 1
        c_today = klines[date_idx]['close_price']
        c_yest = klines[date_idx - 1]['close_price']
        if c_yest > 0:
            chg = (c_today - c_yest) / c_yest * 100
            peer_changes_today.append(chg)
            if chg > 0.3:
                up_count += 1
            elif chg < -0.3:
                down_count += 1

        lb_idx = max(0, date_idx - lookback)
        c_lb = klines[lb_idx]['close_price']
        if c_lb > 0:
            peer_changes_nd.append((c_today - c_lb) / c_lb * 100)

    if not peer_changes_today:
        return {'信号分': 0.0, '有效同行数': 0}

    avg_today = sum(peer_changes_today) / len(peer_changes_today)
    avg_nd = sum(peer_changes_nd) / len(peer_changes_nd) if peer_changes_nd else 0
    up_ratio = up_count / valid if valid > 0 else 0.5

    signal = 0.0
    if avg_today > 1.0:
        signal += 1.5
    elif avg_today > 0.3:
        signal += 0.5
    elif avg_today < -1.0:
        signal -= 1.5
    elif avg_today < -0.3:
        signal -= 0.5

    if up_ratio > 0.7:
        signal += 1.0
    elif up_ratio > 0.6:
        signal += 0.3
    elif up_ratio < 0.3:
        signal -= 1.0
    elif up_ratio < 0.4:
        signal -= 0.3

    if avg_nd > 3.0:
        signal += 0.5
    elif avg_nd < -3.0:
        signal -= 0.5

    signal = max(-3.0, min(3.0, signal))

    return {
        '信号分': round(signal, 2),
        '有效同行数': valid,
        '当日平均涨跌(%)': round(avg_today, 2),
        f'近{lookback}日平均涨跌(%)': round(avg_nd, 2),
        '上涨占比': round(up_ratio, 2),
    }


# ═══════════════════════════════════════════════════════════
# RS相对强度计算
# ═══════════════════════════════════════════════════════════

def _compute_rs(stock_klines: list[dict], index_klines: list[dict],
                end_idx: int, score_date: str) -> dict:
    """计算个股相对大盘的RS强度。"""
    idx_filtered = [k for k in index_klines if k['date'] <= score_date]
    if len(idx_filtered) < 20 or end_idx < 19:
        return {'5日超额': 0, '20日超额': 0}

    stock_now = stock_klines[end_idx]['close_price']
    stock_5d = stock_klines[max(0, end_idx - 5)]['close_price']
    stock_20d = stock_klines[max(0, end_idx - 20)]['close_price']

    idx_now = idx_filtered[-1]['close_price']
    idx_5d = idx_filtered[max(0, len(idx_filtered) - 6)]['close_price']
    idx_20d = idx_filtered[max(0, len(idx_filtered) - 21)]['close_price']

    s5 = (stock_now - stock_5d) / stock_5d * 100 if stock_5d > 0 else 0
    i5 = (idx_now - idx_5d) / idx_5d * 100 if idx_5d > 0 else 0
    s20 = (stock_now - stock_20d) / stock_20d * 100 if stock_20d > 0 else 0
    i20 = (idx_now - idx_20d) / idx_20d * 100 if idx_20d > 0 else 0

    return {
        '5日超额': round(s5 - i5, 2),
        '20日超额': round(s20 - i20, 2),
    }


# ═══════════════════════════════════════════════════════════
# 核心：板块个性化多因子信号计算
# ═══════════════════════════════════════════════════════════

def _compute_factors(klines_asc: list[dict], end_idx: int,
                     closes: list[float], highs: list[float], lows: list[float],
                     macd_list: list[dict], kdj_list: list[dict],
                     boll_list: list, n: int,
                     fund_flow_for_date: list[dict] | None,
                     index_klines: list[dict] | None,
                     peer_trend: dict,
                     sector: str | None,
                     us_overnight: dict | None = None) -> dict:
    """计算所有因子的原始信号值，返回 {因子名: 信号值}。

    信号值为正表示看涨，为负表示看跌。
    """
    k_today = klines_asc[end_idx]
    c_today = k_today['close_price']
    c_yest = klines_asc[end_idx - 1]['close_price'] if end_idx > 0 else c_today
    vol_today = k_today.get('trading_volume', 0) or 0
    chg_today = (c_today - c_yest) / c_yest * 100 if c_yest > 0 else 0

    # 近20日收益率序列
    daily_returns = []
    for j in range(min(20, end_idx)):
        c_j = klines_asc[end_idx - j]['close_price']
        c_j_prev = klines_asc[end_idx - j - 1]['close_price']
        if c_j_prev > 0:
            daily_returns.append((c_j - c_j_prev) / c_j_prev * 100)

    # 波动率
    if len(daily_returns) >= 10:
        avg_ret = sum(daily_returns) / len(daily_returns)
        vol_std = max(0.5, (sum((r - avg_ret) ** 2 for r in daily_returns) / len(daily_returns)) ** 0.5)
    else:
        vol_std = 2.0

    z_today = chg_today / vol_std

    # ── 因子1：均值回归 ──
    reversion = 0.0
    if z_today > 2.0:
        reversion = -3.0
    elif z_today > 1.2:
        reversion = -1.5
    elif z_today > 0.8:
        reversion = -0.5
    elif z_today < -2.0:
        reversion = 3.0
    elif z_today < -1.2:
        reversion = 1.5
    elif z_today < -0.8:
        reversion = 0.5

    # 多日累积
    if end_idx >= 2:
        c_2d = klines_asc[end_idx - 2]['close_price']
        chg_2d = (c_today - c_2d) / c_2d * 100 if c_2d > 0 else 0
        z_2d = chg_2d / (vol_std * 1.41)
        if z_2d > 1.8:
            reversion -= 2.0
        elif z_2d > 1.0:
            reversion -= 0.8
        elif z_2d < -1.8:
            reversion += 2.0
        elif z_2d < -1.0:
            reversion += 0.8

    if end_idx >= 5:
        c_5d = klines_asc[end_idx - 5]['close_price']
        chg_5d = (c_today - c_5d) / c_5d * 100 if c_5d > 0 else 0
        z_5d = chg_5d / (vol_std * 2.24)
        if z_5d > 1.5:
            reversion -= 1.0
        elif z_5d < -1.5:
            reversion += 1.0

    # ── 因子2：RSI(14) ──
    rsi_score = 0.0
    gains, losses = [], []
    for j in range(min(14, len(daily_returns))):
        r = daily_returns[j]
        gains.append(max(r, 0))
        losses.append(max(-r, 0))
    avg_gain = sum(gains) / 14 if gains else 0
    avg_loss = sum(losses) / 14 if losses else 0.001
    rsi_14 = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 50
    if rsi_14 > 80:
        rsi_score = -2.5
    elif rsi_14 > 70:
        rsi_score = -1.5
    elif rsi_14 > 65:
        rsi_score = -0.5
    elif rsi_14 < 20:
        rsi_score = 2.5
    elif rsi_14 < 30:
        rsi_score = 1.5
    elif rsi_14 < 35:
        rsi_score = 0.5

    # ── 因子3：KDJ ──
    kdj_score = 0.0
    if kdj_list and len(kdj_list) >= n and n >= 2:
        k_val = kdj_list[n - 1]['K']
        j_val = kdj_list[n - 1]['J']
        k_prev = kdj_list[n - 2]['K']
        d_prev = kdj_list[n - 2]['D']
        d_val = kdj_list[n - 1]['D']

        if j_val > 100 and k_val > 80:
            kdj_score = -2.0
        elif j_val > 90 and k_val > 75:
            kdj_score = -1.0
        elif j_val < 0 and k_val < 20:
            kdj_score = 2.0
        elif j_val < 10 and k_val < 25:
            kdj_score = 1.0

        # 金叉/死叉
        if k_prev < d_prev and k_val > d_val and k_val < 30:
            kdj_score += 1.0
        elif k_prev > d_prev and k_val < d_val and k_val > 70:
            kdj_score -= 1.0

    # ── 因子4：MACD动量 ──
    macd_score = 0.0
    if macd_list and len(macd_list) >= n and n >= 3:
        bar_today = macd_list[n - 1]['MACD柱']
        bar_yest = macd_list[n - 2]['MACD柱']
        bar_2d = macd_list[n - 3]['MACD柱']

        if bar_yest < 0 and bar_today > 0:
            macd_score += 1.5
        elif bar_yest > 0 and bar_today < 0:
            macd_score -= 1.5

        if bar_today > 0 and bar_today > bar_yest and bar_yest > bar_2d:
            macd_score += 0.5
        elif bar_today < 0 and bar_today < bar_yest and bar_yest < bar_2d:
            macd_score -= 0.5
        elif bar_today > 0 and bar_today < bar_yest:
            macd_score -= 0.3
        elif bar_today < 0 and bar_today > bar_yest:
            macd_score += 0.3

    # ── 因子5：BOLL位置 ──
    boll_score = 0.0
    if boll_list and len(boll_list) >= n and boll_list[n - 1] is not None:
        upper = boll_list[n - 1]['上轨']
        lower = boll_list[n - 1]['下轨']
        mid = boll_list[n - 1]['中轨']
        bw = upper - lower
        if bw > 0:
            pct = (c_today - lower) / bw
            if pct > 0.95:
                boll_score = -2.0
            elif pct > 0.85:
                boll_score = -1.0
            elif pct < 0.05:
                boll_score = 2.0
            elif pct < 0.15:
                boll_score = 1.0
            # 突破中轨
            if c_yest < mid and c_today > mid:
                boll_score += 0.5
            elif c_yest > mid and c_today < mid:
                boll_score -= 0.5

    # ── 因子6：量价背离 ──
    vp_score = 0.0
    vols_5 = [klines_asc[end_idx - j].get('trading_volume', 0) or 0
              for j in range(min(5, end_idx + 1))]
    avg_vol_5 = sum(vols_5) / len(vols_5) if vols_5 else 1
    vol_ratio = vol_today / avg_vol_5 if avg_vol_5 > 0 else 1.0

    if chg_today > 1.0 and vol_ratio < 0.7:
        vp_score = -1.0  # 价涨量缩
    elif chg_today < -1.0 and vol_ratio < 0.7:
        vp_score = 1.0   # 价跌量缩
    elif chg_today > 1.5 and vol_ratio > 1.8:
        vp_score = 0.5   # 放量上涨
    elif chg_today < -1.5 and vol_ratio > 1.8:
        vp_score = -0.5  # 放量下跌

    # ── 因子7：资金流 ──
    fund_score = 0.0
    if fund_flow_for_date:
        score_date_str = klines_asc[end_idx]['date']
        recent_flows = [r for r in fund_flow_for_date if r.get('date', '') <= score_date_str][:3]
        if recent_flows:
            weights_ff = [0.6, 0.25, 0.15]
            w_net = 0
            tw = 0
            for fi, row in enumerate(recent_flows):
                w = weights_ff[fi] if fi < len(weights_ff) else 0
                w_net += (row.get('big_net', 0) or 0) * w
                tw += w
            if tw > 0:
                w_net /= tw
            if w_net > 5000:
                fund_score = 2.0
            elif w_net > 2000:
                fund_score = 1.0
            elif w_net > 500:
                fund_score = 0.3
            elif w_net < -5000:
                fund_score = -2.0
            elif w_net < -2000:
                fund_score = -1.0
            elif w_net < -500:
                fund_score = -0.3

    # ── 因子8：大盘环境 ──
    market_score = 0.0
    if index_klines and end_idx >= 5:
        idx_date = klines_asc[end_idx]['date']
        idx_filtered = [k for k in index_klines if k['date'] <= idx_date]
        if len(idx_filtered) >= 2:
            idx_c = idx_filtered[-1]['close_price']
            idx_c_prev = idx_filtered[-2]['close_price']
            idx_chg = (idx_c - idx_c_prev) / idx_c_prev * 100 if idx_c_prev > 0 else 0
            if idx_chg < -1.5:
                market_score = 1.0
            elif idx_chg < -0.8:
                market_score = 0.3
            elif idx_chg > 1.5:
                market_score = -0.5
            if len(idx_filtered) >= 6:
                idx_c5 = idx_filtered[-6]['close_price']
                idx_chg5 = (idx_c - idx_c5) / idx_c5 * 100 if idx_c5 > 0 else 0
                if idx_chg5 > 3:
                    market_score -= 0.5
                elif idx_chg5 < -3:
                    market_score += 0.5

    # ── 因子9：连续涨跌 ──
    streak_score = 0.0
    up_streak = 0
    down_streak = 0
    for j in range(min(10, end_idx)):
        idx_j = end_idx - j
        if idx_j <= 0:
            break
        if klines_asc[idx_j]['close_price'] > klines_asc[idx_j - 1]['close_price']:
            if down_streak > 0:
                break
            up_streak += 1
        elif klines_asc[idx_j]['close_price'] < klines_asc[idx_j - 1]['close_price']:
            if up_streak > 0:
                break
            down_streak += 1
        else:
            break
    if up_streak >= 5:
        streak_score = -2.5
    elif up_streak >= 4:
        streak_score = -1.5
    elif up_streak >= 3:
        streak_score = -0.8
    elif down_streak >= 5:
        streak_score = 2.5
    elif down_streak >= 4:
        streak_score = 1.5
    elif down_streak >= 3:
        streak_score = 0.8

    # ── 因子10：MA趋势偏向 ──
    trend_bias = 0.0
    if n >= 20:
        ma5 = sum(closes[-5:]) / 5
        ma10 = sum(closes[-10:]) / 10
        ma20 = sum(closes[-20:]) / 20
        if ma5 > ma10 > ma20:
            trend_bias = 2.0
        elif ma5 > ma10:
            trend_bias = 1.0
        elif ma5 < ma10 < ma20:
            trend_bias = -2.0
        elif ma5 < ma10:
            trend_bias = -1.0

    # ── 因子11：美股隔夜信号 ──
    us_overnight_score = 0.0
    if us_overnight and us_overnight.get("有效"):
        us_overnight_score = us_overnight.get("信号分", 0.0)

    # ── 因子12(v9新增)：波动率状态 ──
    # 高波动率环境下均值回归更强，低波动率环境下趋势延续更强
    vol_regime = 0.0
    if len(daily_returns) >= 10:
        # 近5日波动率 vs 近20日波动率
        recent_5d = daily_returns[:min(5, len(daily_returns))]
        avg_5d = sum(recent_5d) / len(recent_5d)
        vol_5d = max(0.3, (sum((r - avg_5d) ** 2 for r in recent_5d) / len(recent_5d)) ** 0.5)
        vol_ratio_regime = vol_5d / vol_std if vol_std > 0.3 else 1.0
        if vol_ratio_regime > 1.5:
            vol_regime = 1.0   # 波动率扩张→均值回归概率高
        elif vol_ratio_regime < 0.6:
            vol_regime = -1.0  # 波动率收缩→趋势延续概率高

    # ── 因子13(v9新增)：短期动量持续性 ──
    # 1-3日收益率自相关：正自相关=动量延续，负自相关=反转
    momentum_persist = 0.0
    if len(daily_returns) >= 5:
        # 近3日方向一致性
        r1 = daily_returns[0] if len(daily_returns) > 0 else 0  # 今日
        r2 = daily_returns[1] if len(daily_returns) > 1 else 0  # 昨日
        r3 = daily_returns[2] if len(daily_returns) > 2 else 0  # 前日
        same_dir_count = 0
        if r1 * r2 > 0:
            same_dir_count += 1
        if r2 * r3 > 0:
            same_dir_count += 1
        if same_dir_count == 2 and abs(r1) > 0.5:
            # 3日同向且幅度不小→动量延续
            momentum_persist = 1.5 if r1 > 0 else -1.5
        elif same_dir_count == 0 and abs(r1) > 0.5:
            # 连续反转→反转模式
            momentum_persist = -1.0 if r1 > 0 else 1.0

    return {
        "reversion": reversion,
        "rsi": rsi_score,
        "kdj": kdj_score,
        "macd": macd_score,
        "boll": boll_score,
        "vp": vp_score,
        "fund": fund_score,
        "market": market_score,
        "streak": streak_score,
        "trend_bias": trend_bias,
        "us_overnight": us_overnight_score,
        "vol_regime": vol_regime,
        "momentum_persist": momentum_persist,
        # 辅助数据
        "_z_today": z_today,
        "_vol_std": vol_std,
        "_chg_today": chg_today,
        "_vol_ratio": vol_ratio,
        "_rsi": rsi_14,
    }


# ═══════════════════════════════════════════════════════════
# 板块个性化方向决策
# ═══════════════════════════════════════════════════════════


def _decide_direction(factors: dict, peer_trend: dict, rs_data: dict,
                      klines_asc: list[dict], end_idx: int,
                      sector: str | None,
                      total_score: int = 50) -> dict:
    """v8b方向决策：评分×融合信号二维决策矩阵 + 置信度过滤 + 低置信度校准。

    v8分析发现：
    - high置信度: 63.7%（240样本）✅
      - 评分<45+预测跌: 76.9% ✅
      - 评分>=55+预测涨: 45.2%（反向！需要反转）
      - 评分>=55+预测跌: 50.0%（不可靠）
    - medium置信度: 56.8%（488样本）
      - 评分45-54+预测跌: 63.3% ✅
      - 评分>=55+预测涨: 66.7%（21样本，保持）
      - 评分<45+预测涨: 58.1% ✅
    - low置信度: 48.0%（381样本）❌
      - 几乎全部预测下跌(357/381)，但准确率仅47.3%
      - 全部预测上涨反而53.5%
      - 评分55-64: 85样本仅45.9%（全预测跌但实际涨多）

    v8b校准：
    1. 低置信度：不再一刀切看跌，用up_ratio_10d趋势+z_today均值回归决策
    2. 高置信度：评分>=55时强制看跌（看涨45.2%太低）
    3. 中等置信度：评分>=55+看跌保持，微调阈值
    """
    fw = _get_factor_weights(sector)
    thresholds = _get_direction_thresholds(sector)
    peer_w = _get_peer_weight(sector)

    # 加权汇总11因子
    tech_signal = sum(factors[k] * fw[k] for k in fw if k in factors)

    # 板块同行信号
    peer_signal = peer_trend.get('信号分', 0.0)

    # RS相对强度信号
    rs_signal = 0.0
    excess_5d = rs_data.get('5日超额', 0)
    if excess_5d > 3:
        rs_signal += 1.0
    elif excess_5d > 1:
        rs_signal += 0.3
    elif excess_5d < -3:
        rs_signal -= 1.0
    elif excess_5d < -1:
        rs_signal -= 0.3

    # 近10日涨跌比
    rolling_window = 10
    recent_up = 0
    recent_down = 0
    for j in range(1, min(rolling_window + 1, end_idx + 1)):
        c_j = klines_asc[end_idx - j + 1]['close_price']
        c_j_prev = klines_asc[end_idx - j]['close_price']
        if c_j_prev > 0:
            r = (c_j - c_j_prev) / c_j_prev * 100
            if r > 0.3:
                recent_up += 1
            elif r < -0.3:
                recent_down += 1
    total_recent = recent_up + recent_down
    up_ratio_10d = recent_up / total_recent if total_recent > 0 else 0.5

    # 趋势自适应分
    trend_adaptive = 0.0
    if up_ratio_10d >= 0.7:
        trend_adaptive = 2.0
    elif up_ratio_10d >= 0.6:
        trend_adaptive = 1.0
    elif up_ratio_10d <= 0.3:
        trend_adaptive = -2.0
    elif up_ratio_10d <= 0.4:
        trend_adaptive = -1.0

    z_today = factors.get('_z_today', 0)
    effective_peer = peer_signal * peer_w

    # 美股大幅波动额外贡献
    us_signal = factors.get('us_overnight', 0)
    us_extra = 0.0
    if abs(us_signal) >= 1.5:
        us_extra = us_signal * 0.10

    combined = (
        tech_signal * 0.40 +
        effective_peer +
        trend_adaptive * 0.25 +
        rs_signal * 0.10 +
        z_today * (-0.15) +
        us_extra
    )

    # ═══════════════════════════════════════════════════════
    # v8b核心：校准后的二维决策矩阵
    # ═══════════════════════════════════════════════════════

    abs_combined = abs(combined)
    confidence = 'high' if abs_combined > 1.5 else ('medium' if abs_combined > 0.5 else 'low')

    # ── 第一层：低置信度(|combined|<0.5) ──
    # v8问题：357/381预测跌，准确率47.3%。全部预测涨反而53.5%
    # v8b策略：用趋势(up_ratio_10d)+均值回归(z_today)做决策，不再默认看跌
    if confidence == 'low':
        if total_score < 35:
            # 极低评分=强看跌（v7b验证65.3%），升级置信度
            direction = '下跌'
            confidence = 'medium'
        elif total_score > 65:
            direction = '上涨'
            confidence = 'medium'
        else:
            # v8b核心修正：弱信号区域按板块默认方向决策
            # 分析发现：低置信度全涨53.5% > 全跌47.0%
            # 按板块：有色金属全涨61.4%，新能源59.0%，医药全跌54.2%
            # 策略：按板块特性设定默认方向
            _low_conf_sector_up = {'有色金属', '新能源', '制造', '科技'}
            _low_conf_sector_down = {'医药', '汽车'}
            # 化工50/50，用combined微弱方向

            if sector in _low_conf_sector_up:
                direction = '上涨'
            elif sector in _low_conf_sector_down:
                direction = '下跌'
            else:
                # 化工或未分类：用combined微弱方向，偏向上涨
                if combined >= -0.1:
                    direction = '上涨'
                else:
                    direction = '下跌'

    # ── 第二层：中等信号(0.5~1.5) ──
    # v8b表现59.2%，评分45-54+预测跌63.3%最佳
    elif confidence == 'medium':
        # v8b板块特异性修正
        if sector == '有色金属' and combined < -0.5:
            # 有色金属+medium+下跌: 48%，实际涨52%→反转为上涨
            direction = '上涨'
        elif sector == '汽车' and combined > 0.5 and total_score >= 45:
            # 汽车+medium+上涨: 46.4%→反转为下跌
            direction = '下跌'
        elif sector == '医药':
            # 医药+medium: 全部预测涨+1(60.0%)，评分<45预测涨+2
            # 医药在medium区域实际涨多于跌
            if combined < -0.5 and total_score >= 45 and total_score < 55:
                direction = '下跌'  # 中性评分+看跌信号保持
            else:
                direction = '上涨'
        elif total_score < 45 and combined < -0.5:
            # 评分低+融合看跌 → 强组合（v7b 66.7%）
            direction = '下跌'
        elif total_score < 45 and combined > 0.5:
            # 评分低+融合看涨 → 反转看涨有效（v8 58.1%）
            direction = '上涨'
        elif total_score > 55 and combined > 0.5:
            # 中等信号+高评分看涨（v8b 65.2%）
            direction = '上涨'
        elif total_score > 55 and combined < -0.5:
            # 评分高+融合看跌（v8b 64.9%）
            direction = '下跌'
        elif combined > 0.5:
            # 中性评分+融合看涨
            if z_today > 1.5:
                direction = '下跌'  # 大涨后反转
            else:
                direction = '上涨'
        elif combined < -0.5:
            # 中性评分+融合看跌（v8b: 评分45-54+预测跌=63.3%）
            if z_today < -1.5:
                direction = '上涨'  # 大跌后反弹
            else:
                direction = '下跌'
        else:
            direction = '下跌'

    # ── 第三层：强信号(>1.5) ──
    # v8b表现65.0%，评分<45+预测跌76.9%极佳
    # 评分>=55+预测跌52.0%（反转后也一般）
    else:
        # v8b板块特异性修正
        if sector == '化工' and combined < 0:
            # 化工+high+下跌: 25%，实际涨75%→强制反转为上涨
            direction = '上涨'
        elif combined > 0:
            # 强看涨信号
            if total_score >= 55:
                # v8b关键修正：高评分+强看涨=45.2%，必须反转
                direction = '下跌'
            elif total_score < 40 and z_today > 2.0:
                direction = '下跌'  # 极端：低分+大涨+强信号=过热
            else:
                direction = '上涨'
        else:
            # 强看跌信号
            if total_score > 60 and z_today < -2.0:
                direction = '上涨'  # 极端：高分+大跌=超卖反弹
            else:
                direction = '下跌'

    return {
        '方向': direction,
        '融合信号': round(combined, 3),
        '技术信号': round(tech_signal, 3),
        '同行信号': round(peer_signal, 2),
        'RS信号': round(rs_signal, 2),
        '趋势自适应': round(trend_adaptive, 2),
        '近10日涨占比': round(up_ratio_10d, 2),
        'z_today': round(z_today, 2),
        '美股隔夜': round(us_signal, 2),
        '置信度': confidence,
        '评分': total_score,
    }



# ═══════════════════════════════════════════════════════════
# 同时调用 _compute_comprehensive_score 获取7维度评分
# ═══════════════════════════════════════════════════════════

def _score_comprehensive(klines_asc: list[dict], end_idx: int,
                         fund_flow_for_date: list[dict] | None,
                         prev_sentiment: int | None,
                         index_klines: list[dict] | None,
                         prev_total: int | None,
                         sector: str | None) -> dict | None:
    """复用 technical_backtest 的完整7维度评分。"""
    from service.backtest.technical_backtest import _score_full_technical
    return _score_full_technical(
        klines_asc, end_idx, fund_flow_for_date,
        prev_sentiment, index_klines, prev_total, sector
    )


# ═══════════════════════════════════════════════════════════
# 主回测函数
# ═══════════════════════════════════════════════════════════

async def run_prediction_enhanced_backtest(
    stock_codes: list[str],
    start_date: str = '2025-12-10',
    end_date: str = '2026-03-10',
    max_peers: int = 8,
) -> dict:
    """增强预测回测 v6：多因子综合 + 板块个性化。

    Args:
        stock_codes: 股票代码列表
        start_date: 回测起始日期
        end_date: 回测截止日期
        max_peers: 每只股票最多取多少只同行

    Returns:
        回测结果汇总
    """
    from common.utils.stock_info_utils import get_stock_info_by_code
    from service.jqka10.stock_history_fund_flow_10jqka import get_fund_flow_history

    t_start = datetime.now()

    # ── 1. 板块映射（从 stock_industry_list.md） ──
    sector_mapping = parse_industry_list_md()
    logger.info("板块映射: %d 只股票", len(sector_mapping))

    # ── 2. 预加载同板块个股K线 ──
    peer_codes_needed = set()
    stock_sector_map = {}
    for code in stock_codes:
        sector = sector_mapping.get(code)
        stock_sector_map[code] = sector
        if sector:
            peers = get_sector_peers(sector_mapping, code, max_peers)
            peer_codes_needed.update(peers)
    peer_codes_needed -= set(stock_codes)

    peer_kline_cache = {}
    for pc in peer_codes_needed:
        kl = get_kline_data(pc, start_date='2025-06-01', end_date=end_date)
        kl = [k for k in kl if (k.get('trading_volume') or 0) > 0]
        if len(kl) >= 60:
            peer_kline_cache[pc] = kl
    logger.info("同行K线: %d 只", len(peer_kline_cache))

    # ── 3. 大盘指数K线 ──
    index_klines = get_kline_data('000001.SH', start_date='2025-06-01', end_date=end_date)
    index_klines = [k for k in index_klines if (k.get('trading_volume') or 0) > 0]

    # ── 3b. 预加载美股指数K线（用于隔夜信号） ──
    us_kline_map = {}
    try:
        us_kline_map = preload_us_kline_map(
            start_date='2025-06-01',
            end_date=end_date,
            index_codes=['NDX', 'SPX', 'DJIA'],
        )
        logger.info("美股K线预加载完成: %s",
                     {k: len(v) for k, v in us_kline_map.items()})
    except Exception as e:
        logger.warning("美股K线预加载失败(回测将不使用美股因子): %s", e)

    stock_kline_cache = {}
    all_day_results = []
    stock_summaries = []
    sector_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0, 'stocks': set()})

    # ── 4. 逐股票回测 ──
    for code in stock_codes:
        logger.info("回测 %s ...", code)

        all_kline = get_kline_data(code, start_date='2025-06-01', end_date=end_date)
        all_kline = [k for k in all_kline if (k.get('trading_volume') or 0) > 0]
        if len(all_kline) < 150:
            logger.warning("%s K线不足(%d)，跳过", code, len(all_kline))
            continue

        stock_kline_cache[code] = all_kline

        start_idx = None
        for i, k in enumerate(all_kline):
            if k['date'] >= start_date:
                start_idx = i
                break
        if start_idx is None or start_idx < 120:
            logger.warning("%s 起始日期前数据不足，跳过", code)
            continue

        stock_info = get_stock_info_by_code(code)
        fund_flow_all = []
        stock_name = code
        if stock_info:
            stock_name = stock_info.stock_name
            try:
                fund_flow_all = await get_fund_flow_history(stock_info)
            except Exception as e:
                logger.warning("%s 资金流获取失败: %s", stock_name, e)

        stock_sector = stock_sector_map.get(code)
        if stock_sector:
            logger.info("%s → [%s]", stock_name, stock_sector)

        # 同板块个股K线
        peer_klines_for_stock = {}
        if stock_sector:
            peers = get_sector_peers(sector_mapping, code, max_peers)
            for pc in peers:
                if pc in peer_kline_cache:
                    peer_klines_for_stock[pc] = peer_kline_cache[pc]
                elif pc in stock_kline_cache:
                    peer_klines_for_stock[pc] = stock_kline_cache[pc]

        day_results = []
        prev_sentiment = None
        prev_total_score = None

        for i in range(start_idx, len(all_kline) - 1):
            score_date = all_kline[i]['date']
            if score_date > end_date:
                break

            # 资金流过滤（防止未来数据泄露）
            fund_flow_for_date = [
                r for r in fund_flow_all if r.get('date', '') <= score_date
            ] if fund_flow_all else None

            # 7维度综合评分
            score_result = _score_comprehensive(
                all_kline, i, fund_flow_for_date, prev_sentiment,
                index_klines, prev_total_score, stock_sector
            )
            if not score_result:
                continue

            sent_str = score_result['各维度得分'].get('短线情绪', '7/15')
            prev_sentiment = int(sent_str.split('/')[0])
            total = score_result['总分']
            prev_total_score = total

            # 技术指标数据准备
            lookback = 120
            start_lb = max(0, i - lookback + 1)
            closes = [k['close_price'] for k in all_kline[start_lb:i + 1]]
            highs = [k['high_price'] for k in all_kline[start_lb:i + 1]]
            lows = [k['low_price'] for k in all_kline[start_lb:i + 1]]
            n = len(closes)

            macd_list = _calc_macd(closes)
            kdj_list = _calc_kdj(highs, lows, closes)
            boll_list = _calc_boll(closes)

            if not macd_list or len(macd_list) < n:
                continue

            # 板块同行走势
            peer_trend = _compute_peer_trend(peer_klines_for_stock, score_date)

            # RS相对强度
            rs_data = _compute_rs(all_kline, index_klines, i, score_date) if index_klines else {}

            # 美股隔夜信号
            us_overnight = {}
            if us_kline_map:
                us_overnight = get_us_overnight_signal_fast(
                    score_date, stock_sector or '', us_kline_map
                )

            # 板块个性化多因子信号
            factors = _compute_factors(
                all_kline, i, closes, highs, lows,
                macd_list, kdj_list, boll_list, n,
                fund_flow_for_date, index_klines,
                peer_trend, stock_sector,
                us_overnight=us_overnight,
            )

            # 板块个性化方向决策
            decision = _decide_direction(
                factors, peer_trend, rs_data,
                all_kline, i, stock_sector,
                total_score=total,
            )
            final_direction = decision['方向']

            # T+1 实际涨跌
            base_close = all_kline[i]['close_price']
            next_day = all_kline[i + 1]
            if base_close <= 0:
                continue

            actual_chg = round((next_day['close_price'] - base_close) / base_close * 100, 2)
            if actual_chg > 0.3:
                actual_dir = '上涨'
            elif actual_chg < -0.3:
                actual_dir = '下跌'
            else:
                actual_dir = '横盘震荡'

            dir_ok = (final_direction == actual_dir)
            loose_ok = dir_ok
            if not dir_ok:
                if final_direction == '上涨' and actual_chg >= 0:
                    loose_ok = True
                elif final_direction == '下跌' and actual_chg <= 0:
                    loose_ok = True

            day_results.append({
                'stock_code': code,
                'stock_name': stock_name,
                'sector': stock_sector or '未分类',
                'score_date': score_date,
                'next_date': next_day['date'],
                'total_score': total,
                'grade': score_result['评级'],
                'pred_direction': final_direction,
                'actual_change_pct': actual_chg,
                'actual_direction': actual_dir,
                'direction_correct': dir_ok,
                'direction_loose_correct': loose_ok,
                'dimensions': score_result['各维度得分'],
                'decision': decision,
                'factors': {k: round(v, 3) for k, v in factors.items() if not k.startswith('_')},
                'peer_trend': peer_trend,
                'rs': rs_data,
                'us_overnight': us_overnight,
            })

        all_day_results.extend(day_results)

        if day_results:
            n_days = len(day_results)
            d_ok = sum(1 for r in day_results if r['direction_correct'])
            l_ok = sum(1 for r in day_results if r['direction_loose_correct'])
            avg_score = round(sum(r['total_score'] for r in day_results) / n_days, 1)
            avg_chg = round(sum(r['actual_change_pct'] for r in day_results) / n_days, 2)

            stock_summaries.append({
                '股票代码': code,
                '股票名称': stock_name,
                '板块': stock_sector or '未分类',
                '回测天数': n_days,
                '平均评分': avg_score,
                '准确率(宽松)': f'{l_ok}/{n_days} ({round(l_ok / n_days * 100, 1)}%)',
                '准确率(严格)': f'{d_ok}/{n_days} ({round(d_ok / n_days * 100, 1)}%)',
                '平均实际涨跌': f'{avg_chg:+.2f}%',
            })

            sec = stock_sector or '未分类'
            sector_stats[sec]['n'] += n_days
            sector_stats[sec]['ok'] += d_ok
            sector_stats[sec]['loose_ok'] += l_ok
            sector_stats[sec]['stocks'].add(stock_name)

            logger.info("%s(%s)[%s] %d天 宽松%.1f%% 严格%.1f%%",
                        stock_name, code, stock_sector or '-', n_days,
                        l_ok / n_days * 100, d_ok / n_days * 100)

    elapsed = (datetime.now() - t_start).total_seconds()

    if not all_day_results:
        return {'状态': '无有效回测数据', '耗时(秒)': round(elapsed, 1)}

    # ── 5. 汇总统计 ──
    return _build_summary(all_day_results, stock_summaries, sector_stats,
                          stock_codes, peer_kline_cache, elapsed, start_date, end_date)


def _build_summary(all_day_results, stock_summaries, sector_stats,
                   stock_codes, peer_kline_cache, elapsed,
                   start_date, end_date) -> dict:
    """构建回测结果汇总。"""
    total_n = len(all_day_results)
    total_ok = sum(1 for r in all_day_results if r['direction_correct'])
    total_loose = sum(1 for r in all_day_results if r['direction_loose_correct'])

    def _rate(ok, n):
        return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'

    # 按预测方向
    pred_dir_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0})
    for r in all_day_results:
        pd = r['pred_direction']
        pred_dir_stats[pd]['n'] += 1
        if r['direction_correct']:
            pred_dir_stats[pd]['ok'] += 1
        if r['direction_loose_correct']:
            pred_dir_stats[pd]['loose_ok'] += 1

    pred_dir_summary = {}
    for pd in ['上涨', '下跌', '横盘震荡']:
        d = pred_dir_stats.get(pd, {'ok': 0, 'n': 0, 'loose_ok': 0})
        pred_dir_summary[pd] = {
            '样本数': d['n'],
            '准确率(宽松)': _rate(d['loose_ok'], d['n']),
            '准确率(严格)': _rate(d['ok'], d['n']),
        }

    # 按评分区间
    bucket_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0})
    for r in all_day_results:
        s = r['total_score']
        if s >= 55:
            b = '≥55(看涨)'
        elif s >= 48:
            b = '48-54(偏中性)'
        else:
            b = '<48(看跌)'
        bucket_stats[b]['n'] += 1
        if r['direction_correct']:
            bucket_stats[b]['ok'] += 1
        if r['direction_loose_correct']:
            bucket_stats[b]['loose_ok'] += 1

    bucket_summary = {}
    for b in ['≥55(看涨)', '48-54(偏中性)', '<48(看跌)']:
        d = bucket_stats.get(b, {'ok': 0, 'n': 0, 'loose_ok': 0})
        bucket_summary[b] = {
            '样本数': d['n'],
            '准确率(宽松)': _rate(d['loose_ok'], d['n']),
            '准确率(严格)': _rate(d['ok'], d['n']),
        }

    # 按板块
    sector_summary = {}
    for sec, stats in sorted(sector_stats.items()):
        sector_summary[sec] = {
            '股票数': len(stats['stocks']),
            '样本数': stats['n'],
            '准确率(宽松)': _rate(stats['loose_ok'], stats['n']),
            '准确率(严格)': _rate(stats['ok'], stats['n']),
            '股票列表': sorted(stats['stocks']),
        }

    # 板块同行信号有效性
    peer_analysis = _analyze_peer_effectiveness(all_day_results)

    # 置信度分析
    confidence_analysis = _analyze_confidence(all_day_results)

    # 因子有效性分析（按板块）
    factor_analysis = _analyze_factor_effectiveness(all_day_results)

    # 板块个性化效果对比
    sector_config_summary = {}
    for sec in sector_summary:
        if sec == '未分类':
            continue
        sector_config_summary[sec] = {
            '因子权重': _get_factor_weights(sec),
            '方向阈值': _get_direction_thresholds(sec),
            '同行联动权重': _get_peer_weight(sec),
        }

    # 逐日详情（精简版）
    detail_list = []
    for r in sorted(all_day_results, key=lambda x: (x['stock_code'], x['score_date'])):
        detail_list.append({
            '代码': r['stock_code'],
            '名称': r['stock_name'],
            '板块': r['sector'],
            '评分日': r['score_date'],
            '预测日': r['next_date'],
            '评分': r['total_score'],
            '评级': r['grade'],
            '预测方向': r['pred_direction'],
            '实际涨跌': f"{r['actual_change_pct']:+.2f}%",
            '实际方向': r['actual_direction'],
            '宽松正确': '✓' if r['direction_loose_correct'] else '✗',
            '严格正确': '✓' if r['direction_correct'] else '✗',
            '融合信号': r['decision']['融合信号'],
            '技术信号': r['decision']['技术信号'],
            '同行信号': r['decision']['同行信号'],
            'RS信号': r['decision']['RS信号'],
            '美股隔夜': r['decision'].get('美股隔夜', 0),
            '美股涨跌(%)': (r.get('us_overnight') or {}).get('隔夜涨跌(%)', None),
            '置信度': r['decision'].get('置信度', ''),
        })

    return {
        '回测类型': '增强预测回测 v8b（二维决策矩阵+置信度校准+低置信度趋势决策）',
        '回测时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        '耗时(秒)': round(elapsed, 1),
        '回测区间': f'{start_date} ~ {end_date}',
        '评判模式': '宽松模式（预测上涨→实际≥0%即正确，预测下跌→实际≤0%即正确）',
        '股票数': len(stock_codes),
        '同行K线加载数': len(peer_kline_cache),
        '总样本数': total_n,
        '总体准确率(宽松)': _rate(total_loose, total_n),
        '总体准确率(严格)': _rate(total_ok, total_n),
        '按预测方向统计': pred_dir_summary,
        '按评分区间': bucket_summary,
        '按板块统计': sector_summary,
        '板块同行信号分析': peer_analysis,
        '置信度分析': confidence_analysis,
        '因子有效性分析(按板块)': factor_analysis,
        '板块个性化配置': sector_config_summary,
        '各股票汇总': stock_summaries,
        '逐日详情': detail_list,
        '说明': (
            'v8b模型：在v8基础上校准低置信度决策+高置信度高评分反转。'
            '核心改进：(1) 低置信度不再一刀切看跌，改用趋势(up_ratio_10d)+均值回归(z_today)决策；'
            '(2) 高置信度+评分>=55+看涨强制反转为看跌(v8仅45.2%)；'
            '(3) 中等置信度+评分>=55+看涨保持(v8为66.7%)；'
            '(4) 综合MACD/KDJ/BOLL/量价/RS/板块同行/美股隔夜7大数据源+11因子。'
        ),
    }


def _analyze_peer_effectiveness(all_day_results: list[dict]) -> dict:
    """分析板块同行信号有效性。"""
    peer_bullish = []
    peer_bearish = []
    peer_neutral = []

    for r in all_day_results:
        ps = r.get('peer_trend', {}).get('信号分', 0)
        if ps > 0.5:
            peer_bullish.append(r)
        elif ps < -0.5:
            peer_bearish.append(r)
        else:
            peer_neutral.append(r)

    def _group_rate(group):
        if not group:
            return {'样本数': 0, '宽松准确率': '无数据'}
        n = len(group)
        l = sum(1 for r in group if r['direction_loose_correct'])
        s = sum(1 for r in group if r['direction_correct'])
        return {
            '样本数': n,
            '宽松准确率': f'{l}/{n} ({round(l / n * 100, 1)}%)',
            '严格准确率': f'{s}/{n} ({round(s / n * 100, 1)}%)',
        }

    # 按板块分组的同行信号有效性
    sector_peer = defaultdict(lambda: {'aligned_ok': 0, 'aligned_n': 0,
                                        'misaligned_ok': 0, 'misaligned_n': 0})
    for r in all_day_results:
        sec = r.get('sector', '未分类')
        ps = r.get('peer_trend', {}).get('信号分', 0)
        pred = r['pred_direction']
        if (ps > 0.5 and pred == '上涨') or (ps < -0.5 and pred == '下跌'):
            sector_peer[sec]['aligned_n'] += 1
            if r['direction_loose_correct']:
                sector_peer[sec]['aligned_ok'] += 1
        elif (ps > 0.5 and pred == '下跌') or (ps < -0.5 and pred == '上涨'):
            sector_peer[sec]['misaligned_n'] += 1
            if r['direction_loose_correct']:
                sector_peer[sec]['misaligned_ok'] += 1

    def _rate(ok, n):
        return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'

    sector_peer_summary = {}
    for sec, d in sorted(sector_peer.items()):
        sector_peer_summary[sec] = {
            '信号一致时': _rate(d['aligned_ok'], d['aligned_n']),
            '信号矛盾时': _rate(d['misaligned_ok'], d['misaligned_n']),
        }

    return {
        '同行看涨时': _group_rate(peer_bullish),
        '同行看跌时': _group_rate(peer_bearish),
        '同行中性时': _group_rate(peer_neutral),
        '按板块同行信号': sector_peer_summary,
    }

def _analyze_confidence(all_day_results: list[dict]) -> dict:
    """按置信度等级分析预测准确率。"""
    tiers = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0})
    for r in all_day_results:
        conf = r.get('decision', {}).get('置信度', 'unknown')
        tiers[conf]['n'] += 1
        if r['direction_correct']:
            tiers[conf]['ok'] += 1
        if r['direction_loose_correct']:
            tiers[conf]['loose_ok'] += 1

    def _rate(ok, n):
        return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'

    result = {}
    for tier in ['high', 'medium', 'low', 'unknown']:
        d = tiers.get(tier, {'ok': 0, 'n': 0, 'loose_ok': 0})
        if d['n'] > 0:
            result[tier] = {
                '样本数': d['n'],
                '占比': f"{round(d['n'] / len(all_day_results) * 100, 1)}%",
                '准确率(宽松)': _rate(d['loose_ok'], d['n']),
                '准确率(严格)': _rate(d['ok'], d['n']),
            }

    # 排除低置信度后的整体准确率
    high_medium = [r for r in all_day_results
                   if r.get('decision', {}).get('置信度') in ('high', 'medium')]
    if high_medium:
        hm_n = len(high_medium)
        hm_loose = sum(1 for r in high_medium if r['direction_loose_correct'])
        hm_strict = sum(1 for r in high_medium if r['direction_correct'])
        result['排除低置信度后'] = {
            '样本数': hm_n,
            '占比': f"{round(hm_n / len(all_day_results) * 100, 1)}%",
            '准确率(宽松)': _rate(hm_loose, hm_n),
            '准确率(严格)': _rate(hm_strict, hm_n),
        }

    return result



def _analyze_factor_effectiveness(all_day_results: list[dict]) -> dict:
    """按板块分析各因子的有效性（信号方向与实际方向的一致率）。"""
    sector_factor_stats = defaultdict(lambda: defaultdict(lambda: {'aligned': 0, 'total': 0}))

    factor_names = ['reversion', 'rsi', 'kdj', 'macd', 'boll', 'vp', 'fund', 'market', 'streak', 'trend_bias', 'us_overnight']

    for r in all_day_results:
        sec = r.get('sector', '未分类')
        factors = r.get('factors', {})
        actual_chg = r.get('actual_change_pct', 0)

        for fname in factor_names:
            fval = factors.get(fname, 0)
            if abs(fval) < 0.1:
                continue  # 信号太弱，跳过
            sector_factor_stats[sec][fname]['total'] += 1
            # 因子信号方向与实际方向一致
            if (fval > 0 and actual_chg > 0) or (fval < 0 and actual_chg < 0):
                sector_factor_stats[sec][fname]['aligned'] += 1

    result = {}
    for sec in sorted(sector_factor_stats):
        sec_result = {}
        for fname in factor_names:
            d = sector_factor_stats[sec][fname]
            if d['total'] >= 10:
                rate = round(d['aligned'] / d['total'] * 100, 1)
                sec_result[fname] = {
                    '样本数': d['total'],
                    '方向一致率': f"{rate}%",
                    '有效性': '有效' if rate > 52 else ('无效' if rate < 48 else '中性'),
                }
        result[sec] = sec_result

    return result


# ═══════════════════════════════════════════════════════════
# CLI 入口
# ═══════════════════════════════════════════════════════════

if __name__ == '__main__':
    import json
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    async def _main():
        # 20只股票，覆盖7个板块
        stock_codes = [
            # 科技(4)
            '002371.SZ', '300308.SZ', '002916.SZ', '603986.SH',
            # 有色金属(3)
            '002155.SZ', '601899.SH', '600549.SH',
            # 汽车(2)
            '002594.SZ', '600066.SH',
            # 新能源(3)
            '300750.SZ', '300763.SZ', '002709.SZ',
            # 医药(3)
            '600276.SH', '600436.SH', '603259.SH',
            # 化工(3)
            '600309.SH', '002440.SZ', '002497.SZ',
            # 制造(2)
            '600031.SH', '300124.SZ',
        ]

        result = await run_prediction_enhanced_backtest(
            stock_codes=stock_codes,
            start_date='2025-12-10',
            end_date='2026-03-10',
            max_peers=8,
        )

        output_path = 'data_results/backtest_prediction_enhanced_result.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        # 打印汇总
        print(json.dumps({k: v for k, v in result.items()
                          if k not in ('逐日详情', '各股票汇总')},
                         ensure_ascii=False, indent=2))

        print(f"\n各股票汇总:")
        for s in result.get('各股票汇总', []):
            print(f"  {s['股票名称']}({s['股票代码']})[{s['板块']}]: "
                  f"{s['回测天数']}天 宽松{s['准确率(宽松)']} 严格{s['准确率(严格)']} "
                  f"均涨跌{s['平均实际涨跌']}")

        print(f"\n结果已保存到: {output_path}")

    asyncio.run(_main())
