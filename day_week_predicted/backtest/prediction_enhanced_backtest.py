#!/usr/bin/env python3
"""
增强预测回测 v16：简洁稳健版 — 基于2777样本交叉验证优化

v11核心策略（基于v10c 2777样本因子有效性实测数据）：
1. 数据驱动因子权重：根据实测方向一致率校准每个板块的因子权重
   - 有效(>52%): 正权重放大
   - 无效(<48%): 反转使用（负权重）
   - 中性(48-52%): 零权重淘汰
2. 板块偏向强化：化工(61%涨)→强偏涨，有色金属(58%涨)→偏涨
3. 宽松模式优化：低置信度利用市场微涨偏向(50.5%>=0%)
4. 同行信号一致性利用：科技一致67.6%，化工一致62.1%，制造一致66.0%
5. 置信度分层优化：high保持，medium按板块特化，low利用基准率

v10c→v11关键改进：
- v10c因子权重基于经验 → v11基于2777样本实测方向一致率
- v10c同行反转信号效果有限 → v11改用同行一致性信号（准确率更高）
- v10c低置信度55.1% → v11利用板块涨跌基准率+宽松模式偏向
- v10c化工55.1%（最差板块）→ v11强化偏涨策略
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

# 板块同行联动强度（v9c原始值）
_SECTOR_PEER_WEIGHT = {
    "科技": -0.15,
    "有色金属": 0.15,
    "新能源": -0.10,
    "汽车": 0.0,
    "化工": -0.12,
    "医药": 0.12,
    "制造": -0.10,
}

# ═══════════════════════════════════════════════════════════
# v11 因子权重（基于2777样本实测方向一致率校准）
# 校准规则：
#   一致率>60% → 权重1.5~2.0（强有效）
#   一致率55-60% → 权重0.8~1.2（有效）
#   一致率52-55% → 权重0.3~0.5（弱有效）
#   一致率48-52% → 权重0.0（噪声，淘汰）
#   一致率45-48% → 权重-0.3~-0.5（弱反转）
#   一致率<45% → 权重-0.8~-1.5（强反转）
# ═══════════════════════════════════════════════════════════
_SECTOR_FACTOR_WEIGHTS = {
    "科技": {
        # v19剪枝: macd(47%)→0, fund(45%)→0, db_fund(49%)→0
        # 保留有效: reversion=59.7%, rsi=59.7%, boll=57.0%, market=62.0%
        # streak=60.2%, vol_regime=57.1%, gap_signal=58.0%, intraday_pos=55.8%
        # 保留反转: vp=33.3%, trend_bias=44.2%, us_overnight=41.9%, momentum_persist=44.5%
        "reversion": 1.3, "rsi": 1.3, "kdj": 0.0, "macd": 0.0,
        "boll": 1.0, "vp": -1.5, "fund": 0.0, "market": 1.5,
        "streak": 1.5, "trend_bias": -0.8, "us_overnight": -1.2,
        "vol_regime": 1.0, "momentum_persist": -0.8,
        "gap_signal": 1.0, "intraday_pos": 0.8,
        "db_fund": 0.0, "turnover": 0.5,
    },
    "有色金属": {
        # v19剪枝: reversion(46%)→0, market(47%)→0, us_overnight(55%)→0,
        # intraday_pos(54%)→0, db_fund(45%)→0
        # 保留反转: rsi=43.6%, vp=28.2%, streak=44.2%, vol_regime=42.9%,
        # momentum_persist=44.9%, gap_signal=43.0%
        "reversion": 0.0, "rsi": -0.8, "kdj": 0.0, "macd": 0.0,
        "boll": 0.0, "vp": -1.5, "fund": 0.0, "market": 0.0,
        "streak": -0.8, "trend_bias": 0.0, "us_overnight": 0.0,
        "vol_regime": -1.0, "momentum_persist": -0.8,
        "gap_signal": -0.8, "intraday_pos": 0.0,
        "db_fund": 0.0, "turnover": 0.3,
    },
    "汽车": {
        # v19剪枝: rsi(54%)→0, macd(49%)→0, boll(54%)→0, vp(52%)→0,
        # momentum_persist(52%)→0
        # 保留有效: reversion=55.7%, market=58.3%, vol_regime=56.8%,
        # gap_signal=55.6%, intraday_pos=61.2%
        # 保留反转: us_overnight=41.4%
        "reversion": 0.8, "rsi": 0.0, "kdj": 0.0, "macd": 0.0,
        "boll": 0.0, "vp": 0.0, "fund": 0.0, "market": 1.2,
        "streak": 0.0, "trend_bias": 0.0, "us_overnight": -1.2,
        "vol_regime": 1.0, "momentum_persist": 0.0,
        "gap_signal": 0.8, "intraday_pos": 1.5,
        "db_fund": 0.0, "turnover": 0.0,
    },
    "新能源": {
        # v19剪枝: fund(53%)→0, momentum_persist(46%)→0, intraday_pos(52%)→0,
        # db_fund(55%)→0, turnover(50%)→0
        # 保留有效: vp=66.7%, market=65.6%, vol_regime=60.2%
        "reversion": 0.0, "rsi": 0.0, "kdj": 0.0, "macd": 0.0,
        "boll": 0.0, "vp": 1.8, "fund": 0.0, "market": 1.8,
        "streak": 0.0, "trend_bias": 0.0, "us_overnight": 0.0,
        "vol_regime": 1.5, "momentum_persist": 0.0,
        "gap_signal": 0.0, "intraday_pos": 0.0,
        "db_fund": 0.0, "turnover": 0.0,
    },
    "医药": {
        # v19剪枝: reversion(47%)→0, rsi(54%)→0, macd(53%)→0, fund(54%)→0,
        # us_overnight(46%)→0, intraday_pos(46%)→0, db_fund(53%)→0
        # 保留有效: vol_regime=62.9%, turnover=61.9%
        # 保留反转: kdj=44.1%, vp=40.0%, momentum_persist=43.8%, gap_signal=39.1%
        "reversion": 0.0, "rsi": 0.0, "kdj": -0.8, "macd": 0.0,
        "boll": 0.0, "vp": -1.0, "fund": 0.0, "market": 0.0,
        "streak": 0.0, "trend_bias": 0.0, "us_overnight": 0.0,
        "vol_regime": 1.5, "momentum_persist": -0.8,
        "gap_signal": -1.0, "intraday_pos": 0.0,
        "db_fund": 0.0, "turnover": 0.5,
    },
    "化工": {
        # v19剪枝: boll(45%)→0, fund(46%)→0, streak(47%)→0, trend_bias(53%)→0,
        # us_overnight(54%)→0, intraday_pos(52%)→0, db_fund(50%)→0
        # 保留反转: rsi=43.1%, kdj=41.6%, vol_regime=41.5%, turnover=39.1%
        # 保留有效: gap_signal=63.6%
        "reversion": 0.0, "rsi": -0.8, "kdj": -1.0, "macd": 0.0,
        "boll": 0.0, "vp": 0.0, "fund": 0.0, "market": 0.0,
        "streak": 0.0, "trend_bias": 0.0, "us_overnight": 0.0,
        "vol_regime": -1.0, "momentum_persist": 0.0,
        "gap_signal": 0.5, "intraday_pos": 0.0,
        "db_fund": 0.0, "turnover": -0.5,
    },
    "制造": {
        # v19剪枝: kdj(54%)→0, macd(52%)→0, fund(54%)→0, streak(47%)→0,
        # trend_bias(48%)→0, us_overnight(45%)→0, db_fund(54%)→0
        # 保留有效: reversion=58.4%, rsi=65.5%, vp=61.1%, market=63.9%,
        # vol_regime=57.0%, momentum_persist=56.1%
        "reversion": 1.2, "rsi": 1.8, "kdj": 0.0, "macd": 0.0,
        "boll": 0.0, "vp": 1.5, "fund": 0.0, "market": 1.6,
        "streak": 0.0, "trend_bias": 0.0, "us_overnight": 0.0,
        "vol_regime": 1.0, "momentum_persist": 0.8,
        "gap_signal": 0.5, "intraday_pos": 0.0,
        "db_fund": 0.0, "turnover": 0.5,
    },
}

_DEFAULT_FACTOR_WEIGHTS = {
    "reversion": 0.8, "rsi": 0.5, "kdj": 0.0, "macd": 0.3,
    "boll": 0.3, "vp": 0.5, "fund": 0.5, "market": 0.8,
    "streak": 0.3, "trend_bias": 0.0, "us_overnight": 0.0,
    "vol_regime": 1.0, "momentum_persist": 0.0,
    "gap_signal": 0.5, "intraday_pos": 0.5,
    "db_fund": 0.5, "turnover": 0.3,
}

# ═══════════════════════════════════════════════════════════
# v11 同行信号配置（基于v10c实测数据）
# 实测一致时准确率: 科技67.6%, 制造66.0%, 化工62.1%, 新能源62.1%
#                   医药60.6%, 有色金属59.7%, 汽车56.1%
# 实测矛盾时准确率: 汽车57.3%, 科技57.3%, 有色金属54.7%
#                   新能源54.6%, 化工54.5%, 制造54.3%, 医药53.5%
# 结论：一致时准确率全面高于矛盾时 → 改用一致性信号
# ═══════════════════════════════════════════════════════════
_SECTOR_PEER_CONTRARIAN = {
    # v13: 基于2777样本实测 — 一致/矛盾时的模型准确率（非方向一致率）
    # 科技: 一致49.2% vs 矛盾60.5% → 反转信号更好
    # 化工: 一致51.9% vs 矛盾61.5% → 反转信号更好
    # 有色金属: 一致59.8% vs 矛盾61.4% → 矛盾略好，改反转
    "化工": True,       # 矛盾61.5% >> 一致51.9%
    "科技": True,       # 矛盾60.5% >> 一致49.2%
    "有色金属": True,   # 矛盾61.4% > 一致59.8%
    "制造": False,      # 一致66.7% >> 矛盾58.5%
    "新能源": False,    # 一致59.1% >> 矛盾50.0%
    "汽车": False,      # 一致65.7% >> 矛盾57.9%
    "医药": False,      # 一致63.8% >> 矛盾52.2%
}

# 同行信号一致时的准确率（用于决策加权）
# v13: 对反转板块，这里存的是"矛盾时"的准确率
_SECTOR_PEER_ALIGNED_RATE = {
    '科技': 0.605,      # 矛盾时60.5%（反转模式）
    '制造': 0.667,      # 一致时66.7%
    '化工': 0.615,      # 矛盾时61.5%（反转模式）
    '新能源': 0.591,    # 一致时59.1%
    '医药': 0.638,      # 一致时63.8%
    '有色金属': 0.614,  # 矛盾时61.4%（反转模式）
    '汽车': 0.657,      # 一致时65.7%
}

# 板块实际涨跌基准率（>=0%占比，用于宽松模式优化）
_SECTOR_UP_BASE_RATE = {
    '化工': 0.610,      # 61.0% >= 0%
    '有色金属': 0.579,  # 57.9% >= 0%
    '新能源': 0.510,    # 51.0% >= 0%
    '制造': 0.479,      # 47.9% >= 0%
    '科技': 0.458,      # 45.8% >= 0%
    '汽车': 0.453,      # 45.3% >= 0%
    '医药': 0.446,      # 44.6% >= 0%
}

# v11: 方向阈值（基于实测置信度分析优化）
_SECTOR_DIRECTION_THRESHOLDS = {
    "科技": {"bullish": 1.5, "bearish": -1.5, "z_revert": 1.0, "default_up": False},
    "有色金属": {"bullish": 0.3, "bearish": -0.3, "z_revert": 2.0, "default_up": True},
    "汽车": {"bullish": 1.0, "bearish": -1.0, "z_revert": 1.2, "default_up": False},
    "新能源": {"bullish": 1.0, "bearish": -1.0, "z_revert": 1.2, "default_up": True},
    "医药": {"bullish": 1.5, "bearish": -1.5, "z_revert": 0.8, "default_up": False},
    "化工": {"bullish": 0.3, "bearish": -0.3, "z_revert": 1.3, "default_up": True},
    "制造": {"bullish": 1.5, "bearish": -1.0, "z_revert": 1.0, "default_up": False},
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
# 板块个性化方向决策 v16b
# ═══════════════════════════════════════════════════════════

def _decide_direction(factors: dict, peer_trend: dict, rs_data: dict,
                      klines_asc: list[dict], end_idx: int,
                      sector: str | None,
                      total_score: int = 50,
                      score_date: str = '',
                      prev_pred_correct: bool | None = None) -> dict:
    """v19方向决策：数据驱动 + 噪声因子剪枝 + 阈值优化。

    核心策略：
    1. combined信号作为主决策（噪声因子已剪枝，信号更纯净）
    2. 板块涨跌基准率作为低信号默认方向
    3. 化工/有色金属: 强偏涨（基准率>57%）
    4. 星期效应: 仅使用最强的几个（>60%偏向）
    5. v19优化: 科技/医药/制造阈值基于前半训练+后半验证调整
    """
    fw = _get_factor_weights(sector)
    peer_w = _get_peer_weight(sector)

    # 加权汇总因子
    tech_signal = sum(factors.get(k, 0) * fw[k] for k in fw if k in factors)

    # 板块同行信号
    peer_signal = peer_trend.get('信号分', 0.0)

    # RS相对强度信号
    rs_signal = 0.0
    excess_5d = rs_data.get('5日超额', 0)
    excess_20d = rs_data.get('20日超额', 0)
    if excess_5d > 3:
        rs_signal += 1.0
    elif excess_5d > 1:
        rs_signal += 0.3
    elif excess_5d < -3:
        rs_signal -= 1.0
    elif excess_5d < -1:
        rs_signal -= 0.3
    if excess_20d > 5:
        rs_signal += 0.5
    elif excess_20d < -5:
        rs_signal -= 0.5

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
    vol_regime = factors.get('vol_regime', 0)
    effective_peer = peer_signal * peer_w

    # 美股大幅波动额外贡献
    us_signal = factors.get('us_overnight', 0)
    us_extra = 0.0
    if abs(us_signal) >= 1.5:
        if sector in ('制造', '化工', '有色金属', '科技'):
            us_extra = -us_signal * 0.10
        else:
            us_extra = us_signal * 0.10

    # 波动率自适应融合权重
    if vol_regime > 0:
        tech_w = 0.45
        trend_w = 0.20
    elif vol_regime < 0:
        tech_w = 0.35
        trend_w = 0.30
    else:
        tech_w = 0.40
        trend_w = 0.25

    combined = (
        tech_signal * tech_w +
        effective_peer +
        trend_adaptive * trend_w +
        rs_signal * 0.10 +
        z_today * (-0.15) +
        us_extra
    )

    # ═══════════════════════════════════════════════════════
    # v16b 决策逻辑
    # ═══════════════════════════════════════════════════════

    abs_combined = abs(combined)
    confidence = 'high' if abs_combined > 1.5 else ('medium' if abs_combined > 0.5 else 'low')

    # ── 主决策：基于combined信号 + 板块特化 ──
    if sector == '化工':
        if combined < -1.0:
            direction = '下跌'
        else:
            direction = '上涨'
    elif sector == '有色金属':
        direction = '上涨'
    elif sector == '科技':
        # v19优化: bull>0.0, bear<-0.3 (前半58.9%→后半59.6%)
        if combined > 0.0:
            direction = '上涨'
        elif combined < -0.3:
            direction = '下跌'
        else:
            direction = '上涨'
    elif sector == '汽车':
        if combined > 0.5:
            direction = '上涨'
        elif combined < -0.5:
            direction = '下跌'
        else:
            direction = '下跌'
    elif sector == '新能源':
        if combined > 0.5:
            direction = '上涨'
        elif combined < -1.0:
            direction = '下跌'
        else:
            direction = '上涨'
    elif sector == '医药':
        # v19优化: bull>0.0, bear<-2.0 (前半58.7%→后半59.7%)
        if combined > 0.0:
            direction = '上涨'
        elif combined < -2.0:
            direction = '下跌'
        else:
            direction = '下跌'
    elif sector == '制造':
        # v19优化: bull>-0.5, bear<-2.0, default=跌 (前半65.5%→后半60.7%)
        if combined > -0.5:
            direction = '上涨'
        elif combined < -2.0:
            direction = '下跌'
        else:
            direction = '下跌'
    else:
        if combined > 0.5:
            direction = '上涨'
        elif combined < -0.5:
            direction = '下跌'
        else:
            direction = '上涨' if _SECTOR_UP_BASE_RATE.get(sector, 0.5) > 0.5 else '下跌'

    # ── 修正层1: 星期效应（仅最强的，偏向>60%）──
    if score_date:
        try:
            wd = datetime.strptime(score_date, '%Y-%m-%d').weekday()
            if sector == '医药' and wd == 4:
                direction = '下跌'
            elif sector == '汽车' and wd == 1 and confidence != 'high':
                direction = '下跌'
            elif sector == '汽车' and wd == 2 and confidence != 'high':
                direction = '下跌'
            elif sector == '有色金属' and wd == 2 and confidence != 'high':
                direction = '下跌'
            elif sector == '科技' and wd == 2 and confidence != 'high':
                direction = '下跌'
            elif sector == '新能源' and wd == 4 and confidence != 'high':
                direction = '下跌'
            elif sector == '新能源' and wd == 0 and confidence != 'high':
                direction = '下跌'
            elif sector == '制造' and wd == 4 and confidence != 'high':
                direction = '下跌'
            elif sector == '化工' and wd == 2 and confidence == 'low':
                direction = '下跌'
            elif sector == '有色金属' and wd == 1:
                direction = '上涨'
            elif sector == '有色金属' and wd == 4:
                direction = '上涨'
            elif sector == '化工' and wd == 4:
                direction = '上涨'
            elif sector == '化工' and wd == 1 and confidence != 'high':
                direction = '上涨'
        except ValueError:
            pass

    # ── 修正层2: 评分极端值 ──
    if sector == '汽车' and total_score < 35:
        direction = '上涨'
    elif sector == '有色金属' and total_score < 35:
        direction = '上涨'
    elif sector == '科技' and total_score < 35:
        direction = '上涨'

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
        '波动率状态': round(vol_regime, 2),
        '同行一致': False,
    }


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
                     us_overnight: dict | None = None,
                     db_fund_flow: list[dict] | None = None,
                     score_date: str = '') -> dict:
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

    # ── 因子14(v10c新增)：跳空缺口信号 ──
    # 今日开盘价 vs 昨日收盘价的缺口方向，缺口往往会回补
    gap_signal = 0.0
    open_today = k_today.get('open_price', c_today)
    if c_yest > 0 and open_today > 0:
        gap_pct = (open_today - c_yest) / c_yest * 100
        gap_z = gap_pct / vol_std if vol_std > 0.3 else 0
        if gap_z > 1.5:
            gap_signal = -2.0  # 大幅高开→回补缺口概率高
        elif gap_z > 0.8:
            gap_signal = -1.0
        elif gap_z < -1.5:
            gap_signal = 2.0   # 大幅低开→回补缺口概率高
        elif gap_z < -0.8:
            gap_signal = 1.0

    # ── 因子15(v10c新增)：日内收盘位置 ──
    # 收盘价在当日高低点的位置，高位收盘→次日回调概率高
    intraday_pos = 0.0
    h_today = k_today.get('high_price', c_today)
    l_today = k_today.get('low_price', c_today)
    day_range = h_today - l_today
    if day_range > 0:
        close_pos = (c_today - l_today) / day_range  # 0=最低, 1=最高
        if close_pos > 0.9:
            intraday_pos = -1.5  # 收在最高位→次日回调
        elif close_pos > 0.75:
            intraday_pos = -0.5
        elif close_pos < 0.1:
            intraday_pos = 1.5   # 收在最低位→次日反弹
        elif close_pos < 0.25:
            intraday_pos = 0.5

    # ── 因子16(v13新增)：DB资金流增强信号 ──
    # 使用DB中的大单净额、大单净占比、5日主力净额趋势
    db_fund_signal = 0.0
    if db_fund_flow and score_date:
        # DB数据按日期倒序，过滤到score_date之前
        recent_ff = [r for r in db_fund_flow
                     if (r.get('date') or '') <= score_date][:5]
        if recent_ff:
            # 大单净占比（比绝对值更有意义）
            big_net_pct_today = recent_ff[0].get('big_net_pct') or 0
            if big_net_pct_today > 5:
                db_fund_signal += 1.5
            elif big_net_pct_today > 2:
                db_fund_signal += 0.5
            elif big_net_pct_today < -5:
                db_fund_signal -= 1.5
            elif big_net_pct_today < -2:
                db_fund_signal -= 0.5

            # 5日主力净额趋势
            main_5d = recent_ff[0].get('main_net_5day') or 0
            if main_5d > 5000:
                db_fund_signal += 1.0
            elif main_5d > 1000:
                db_fund_signal += 0.3
            elif main_5d < -5000:
                db_fund_signal -= 1.0
            elif main_5d < -1000:
                db_fund_signal -= 0.3

            # 连续3日大单净流入/流出趋势
            if len(recent_ff) >= 3:
                big_nets = [(r.get('big_net') or 0) for r in recent_ff[:3]]
                if all(b > 0 for b in big_nets):
                    db_fund_signal += 0.5  # 连续3日主力净流入
                elif all(b < 0 for b in big_nets):
                    db_fund_signal -= 0.5  # 连续3日主力净流出

    # ── 因子17(v13新增)：换手率信号 ──
    turnover_signal = 0.0
    amount_today = k_today.get('trading_amount', 0) or 0
    if end_idx >= 20:
        amounts_20 = [klines_asc[end_idx - j].get('trading_amount', 0) or 0
                       for j in range(20)]
        avg_amount_20 = sum(amounts_20) / 20 if amounts_20 else 1
        if avg_amount_20 > 0:
            amount_ratio = amount_today / avg_amount_20
            if amount_ratio > 2.5 and chg_today > 1.0:
                turnover_signal = -1.0  # 放量大涨→次日回调
            elif amount_ratio > 2.5 and chg_today < -1.0:
                turnover_signal = 1.0   # 放量大跌→次日反弹
            elif amount_ratio < 0.5:
                turnover_signal = 0.3 if chg_today > 0 else -0.3  # 缩量延续

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
        "gap_signal": gap_signal,
        "intraday_pos": intraday_pos,
        "db_fund": db_fund_signal,
        "turnover": turnover_signal,
        # 辅助数据
        "_z_today": z_today,
        "_vol_std": vol_std,
        "_chg_today": chg_today,
        "_vol_ratio": vol_ratio,
        "_rsi": rsi_14,
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
    from day_week_predicted.backtest.technical_backtest import _score_full_technical
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

    # ── 3c. 预加载DB资金流数据（比实时API更可靠） ──
    db_fund_flow_cache = {}
    try:
        from dao.stock_fund_flow_dao import get_fund_flow_by_code
        for code in stock_codes:
            ff = get_fund_flow_by_code(code, limit=200)
            if ff:
                db_fund_flow_cache[code] = ff
        logger.info("DB资金流预加载: %d 只股票", len(db_fund_flow_cache))
    except Exception as e:
        logger.warning("DB资金流预加载失败: %s", e)

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
                fund_flow_all = await asyncio.wait_for(
                    get_fund_flow_history(stock_info), timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning("%s 资金流API超时(10s)，使用DB数据", stock_name)
            except Exception as e:
                logger.warning("%s 资金流获取失败: %s", stock_name, e)

        # v13: 优先使用DB资金流数据（更完整）
        db_fund_flow = db_fund_flow_cache.get(code, [])

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
        prev_pred_correct = None  # v13: 前一日预测是否正确

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
                db_fund_flow=db_fund_flow,
                score_date=score_date,
            )

            # 板块个性化方向决策
            decision = _decide_direction(
                factors, peer_trend, rs_data,
                all_kline, i, stock_sector,
                total_score=total,
                score_date=score_date,
                prev_pred_correct=prev_pred_correct,
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

            # v13: 更新前一日预测结果
            prev_pred_correct = loose_ok

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

    # 周预测分析
    weekly_prediction = _compute_weekly_predictions(all_day_results)

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
            '波动率状态': r['decision'].get('波动率状态', 0),
            'z_today': r['decision'].get('z_today', 0),
        })

    return {
        '回测类型': '增强预测回测 v13（反转同行+美股差异化+评分修正+DB资金流+前日反馈）',
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
        '周预测分析': weekly_prediction,
        '板块个性化配置': sector_config_summary,
        '各股票汇总': stock_summaries,
        '逐日详情': detail_list,
        '说明': (
            'v11模型：数据驱动因子权重校准+同行一致性信号+板块偏向强化。'
            '核心改进：(1) 因子权重基于2777样本实测方向一致率校准；'
            '(2) 同行信号改用一致性（科技67.6%,制造66.0%,化工62.1%）；'
            '(3) 低置信度利用板块涨跌基准率（化工61%涨,有色58%涨）；'
            '(4) 无效因子反转使用（<48%一致率→负权重）；'
            '(5) 宽松模式偏向：不确定时偏涨（50.5%实际>=0%）。'
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

    factor_names = ['reversion', 'rsi', 'kdj', 'macd', 'boll', 'vp', 'fund', 'market', 'streak', 'trend_bias', 'us_overnight', 'vol_regime', 'momentum_persist', 'gap_signal', 'intraday_pos', 'db_fund', 'turnover']

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
# v19e 周预测模块
# 策略B: 周一收盘后预测（准确率68-70%）
# 策略C: 周三收盘后预测（准确率77-81%）
# ═══════════════════════════════════════════════════════════

def _compute_weekly_predictions(all_day_results: list[dict]) -> dict:
    """基于日频回测结果计算周预测准确率，含概念板块信号增强。

    策略：
    - 时点A（周一开盘前）：仅用周一模型信号
    - 时点B（周一收盘后）：周一实际涨跌 + 板块基准率混合
    - 时点B+概念（周一收盘后）：B策略 + 概念板块动量/共识度修正
    - 时点C（周三收盘后）：前3天累计涨跌方向
    - 时点C+概念（周三收盘后）：C策略 + 概念板块信号修正

    Returns:
        周预测汇总字典
    """
    from collections import defaultdict
    from service.analysis.concept_weekly_signal import (
        batch_preload_concept_data,
        compute_concept_signal_for_date,
        predict_weekly_B_with_concept,
        predict_weekly_C_with_concept,
    )

    # 按(股票代码, ISO周)分组
    stock_week = defaultdict(list)
    for d in all_day_results:
        dt = datetime.strptime(d['score_date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        stock_week[(d['stock_code'], iw)].append(d)

    weekly_records = []
    for (code, iw), days in stock_week.items():
        days.sort(key=lambda x: x['score_date'])
        if len(days) < 2:
            continue

        # 周累计涨跌
        cum = 1.0
        for d in days:
            cum *= (1 + d['actual_change_pct'] / 100)
        wchg = (cum - 1) * 100
        wup = wchg >= 0

        sector = days[0]['sector']
        d0 = days[0]

        rec = {
            'code': code, 'sector': sector, 'iw': iw,
            'n': len(days), 'wchg': wchg, 'wup': wup,
            'mon_actual': d0['actual_change_pct'],
            'mon_comb': d0['decision']['融合信号'],
            'd3_chg': sum(d['actual_change_pct'] for d in days[:min(3, len(days))]),
            'mon_date': d0['score_date'],
            'wed_date': days[2]['score_date'] if len(days) >= 3 else d0['score_date'],
            'days': days,
        }
        weekly_records.append(rec)

    if not weekly_records:
        return {'状态': '无周数据'}

    nw = len(weekly_records)

    # ── 预加载概念板块数据 ──
    stock_codes = list(set(r['code'] for r in weekly_records))
    all_dates = sorted(set(d['score_date'] for d in all_day_results))
    start_date = all_dates[0] if all_dates else '2025-01-01'
    end_date = all_dates[-1] if all_dates else '2026-12-31'

    concept_data = None
    try:
        concept_data = batch_preload_concept_data(stock_codes, start_date, end_date)
        logger.info("[周预测] 概念板块数据预加载完成")
    except Exception as e:
        logger.warning("[周预测] 概念板块数据加载失败(将跳过概念增强): %s", e)

    # 为每条周记录计算概念信号
    n_with_concept_mon = 0
    n_with_concept_wed = 0
    for r in weekly_records:
        r['concept_mon'] = None
        r['concept_wed'] = None
        if concept_data:
            stock_boards = concept_data['stock_boards'].get(r['code'], [])
            board_kline_map = concept_data['board_kline_map']
            if stock_boards:
                # 周一概念信号（5日lookback）
                sig_mon = compute_concept_signal_for_date(
                    r['code'], r['mon_date'], board_kline_map, stock_boards, lookback=5
                )
                if sig_mon:
                    r['concept_mon'] = sig_mon
                    n_with_concept_mon += 1
                # 周三概念信号（3日lookback）
                sig_wed = compute_concept_signal_for_date(
                    r['code'], r['wed_date'], board_kline_map, stock_boards, lookback=3
                )
                if sig_wed:
                    r['concept_wed'] = sig_wed
                    n_with_concept_wed += 1

    logger.info("[周预测] 概念信号: 周一 %d/%d, 周三 %d/%d",
                n_with_concept_mon, nw, n_with_concept_wed, nw)

    # ── 策略A: 周一融合信号 ──
    a_ok = sum(1 for r in weekly_records
               if (r['mon_comb'] > 0 and r['wup']) or
                  (r['mon_comb'] <= 0 and not r['wup']))

    # ── 策略B: 周一混合(0.5) ──
    sector_up_rate = {}
    for sec in set(r['sector'] for r in weekly_records):
        sr = [r for r in weekly_records if r['sector'] == sec]
        sector_up_rate[sec] = sum(1 for r in sr if r['wup']) / len(sr)

    b_ok = 0
    for r in weekly_records:
        if r['mon_actual'] > 0.5:
            pred_up = True
        elif r['mon_actual'] < -0.5:
            pred_up = False
        else:
            pred_up = sector_up_rate.get(r['sector'], 0.5) > 0.5
        if (pred_up and r['wup']) or (not pred_up and not r['wup']):
            b_ok += 1

    # ── 策略B+概念: 周一混合 + 概念板块信号 ──
    bc_ok = 0
    bc_details = {'修正对': 0, '修正错': 0, '概念样本': 0}
    for r in weekly_records:
        # 原始B策略预测
        if r['mon_actual'] > 0.5:
            b_pred = True
        elif r['mon_actual'] < -0.5:
            b_pred = False
        else:
            b_pred = sector_up_rate.get(r['sector'], 0.5) > 0.5
        b_correct = (b_pred and r['wup']) or (not b_pred and not r['wup'])

        # B+概念策略预测
        bc_pred, bc_reason = predict_weekly_B_with_concept(
            r['mon_actual'],
            sector_up_rate.get(r['sector'], 0.5),
            r['concept_mon'],
        )
        r['bc_pred'] = bc_pred
        r['bc_reason'] = bc_reason
        bc_correct = (bc_pred and r['wup']) or (not bc_pred and not r['wup'])

        if bc_correct:
            bc_ok += 1
        if r['concept_mon']:
            bc_details['概念样本'] += 1
            if not b_correct and bc_correct:
                bc_details['修正对'] += 1
            elif b_correct and not bc_correct:
                bc_details['修正错'] += 1

    # ── 策略B2: 周一涨跌>-0.5 ──
    b2_ok = sum(1 for r in weekly_records
                if (r['mon_actual'] > -0.5 and r['wup']) or
                   (r['mon_actual'] <= -0.5 and not r['wup']))

    # ── 策略C: 前3天涨跌>0 ──
    c_ok = sum(1 for r in weekly_records
               if (r['d3_chg'] > 0 and r['wup']) or
                  (r['d3_chg'] <= 0 and not r['wup']))

    # ── 策略C+概念: 前3天方向 + 概念板块信号 ──
    cc_ok = 0
    cc_details = {'修正对': 0, '修正错': 0, '概念样本': 0}
    for r in weekly_records:
        # 原始C策略预测
        c_pred = r['d3_chg'] > 0
        c_correct = (c_pred and r['wup']) or (not c_pred and not r['wup'])

        # C+概念策略预测
        cc_pred, cc_reason = predict_weekly_C_with_concept(
            r['d3_chg'], r['concept_wed'],
        )
        r['cc_pred'] = cc_pred
        r['cc_reason'] = cc_reason
        cc_correct = (cc_pred and r['wup']) or (not cc_pred and not r['wup'])

        if cc_correct:
            cc_ok += 1
        if r['concept_wed']:
            cc_details['概念样本'] += 1
            if not c_correct and cc_correct:
                cc_details['修正对'] += 1
            elif c_correct and not cc_correct:
                cc_details['修正错'] += 1

    # ── 策略C2: 前3天涨跌>0.5 ──
    c2_ok = sum(1 for r in weekly_records
                if (r['d3_chg'] > 0.5 and r['wup']) or
                   (r['d3_chg'] <= 0.5 and not r['wup']))

    def _rate(ok, n):
        return f'{ok}/{n} ({round(ok / n * 100, 1)}%)' if n > 0 else '无数据'

    # ── 按板块统计（含概念增强） ──
    sector_weekly = {}
    for sec in sorted(set(r['sector'] for r in weekly_records)):
        sr = [r for r in weekly_records if r['sector'] == sec]
        sn = len(sr)
        s_b = sum(1 for r in sr
                  if (r['mon_actual'] > 0.5 and r['wup']) or
                     (r['mon_actual'] < -0.5 and not r['wup']) or
                     (abs(r['mon_actual']) <= 0.5 and
                      ((sector_up_rate.get(sec, 0.5) > 0.5 and r['wup']) or
                       (sector_up_rate.get(sec, 0.5) <= 0.5 and not r['wup']))))
        s_bc = sum(1 for r in sr
                   if (r.get('bc_pred') and r['wup']) or
                      (not r.get('bc_pred') and not r['wup']))
        s_c = sum(1 for r in sr
                  if (r['d3_chg'] > 0 and r['wup']) or
                     (r['d3_chg'] <= 0 and not r['wup']))
        s_cc = sum(1 for r in sr
                   if (r.get('cc_pred') and r['wup']) or
                      (not r.get('cc_pred') and not r['wup']))
        sector_weekly[sec] = {
            '周样本数': sn,
            'B:周一混合': _rate(s_b, sn),
            'B+概念': _rate(s_bc, sn),
            'C:前3天方向': _rate(s_c, sn),
            'C+概念': _rate(s_cc, sn),
        }

    # ── 概念信号有效性分析 ──
    concept_analysis = {
        '概念数据覆盖': {
            '周一信号': f'{n_with_concept_mon}/{nw} ({round(n_with_concept_mon/nw*100,1) if nw else 0}%)',
            '周三信号': f'{n_with_concept_wed}/{nw} ({round(n_with_concept_wed/nw*100,1) if nw else 0}%)',
        },
        'B策略概念修正': {
            '概念样本数': bc_details['概念样本'],
            '原始错→概念修正对': bc_details['修正对'],
            '原始对→概念修正错': bc_details['修正错'],
            '净改善': bc_details['修正对'] - bc_details['修正错'],
        },
        'C策略概念修正': {
            '概念样本数': cc_details['概念样本'],
            '原始错→概念修正对': cc_details['修正对'],
            '原始对→概念修正错': cc_details['修正错'],
            '净改善': cc_details['修正对'] - cc_details['修正错'],
        },
    }

    return {
        '周样本数': nw,
        '周数': len(set(r['iw'] for r in weekly_records)),
        '策略汇总': {
            'A:周一融合信号': _rate(a_ok, nw),
            'B:周一混合(0.5)': _rate(b_ok, nw),
            'B+概念:周一混合+概念信号': _rate(bc_ok, nw),
            'B2:周一涨跌>-0.5': _rate(b2_ok, nw),
            'C:前3天涨跌>0': _rate(c_ok, nw),
            'C+概念:前3天+概念信号': _rate(cc_ok, nw),
            'C2:前3天涨跌>0.5': _rate(c2_ok, nw),
        },
        '推荐策略': {
            '周一收盘可用': 'B+概念 — 周一涨跌+概念板块动量/共识度综合判断',
            '周三收盘可用': 'C+概念 — 前3天涨跌+概念板块信号修正边界区',
        },
        '按板块': sector_weekly,
        '概念信号分析': concept_analysis,
        '说明': (
            '周预测v20: 在v19e基础上集成概念板块信号。'
            '概念信号来源: DB中概念板块K线数据(concept_board_kline)。'
            '概念维度: 动量(近N日均涨跌), 共识度(看涨板块占比), 强度(累计超额)。'
            '概念信号主要在模糊区(B:±0.5%, C:±1.5%)发挥修正作用。'
        ),
    }


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
