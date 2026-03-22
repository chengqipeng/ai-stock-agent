#!/usr/bin/env python3
"""
因子计算基础算子库
==================
来源：国泰君安191因子体系 + S&C经典指标 + SSRN学术因子

设计原则：
  1. 保留因子原味 — 算子只做数学变换，不做截断/winsorize
  2. 最少参数 — 每个算子只接受必要参数，避免过度加工
  3. 向量化 — 基于 list[float] 操作，可直接对接K线数据
"""
import math
from typing import Optional


# ═══════════════════════════════════════════════════════════
# 基础数学算子（国君191体系核心）
# ═══════════════════════════════════════════════════════════

def ts_mean(x: list[float], d: int) -> list[float]:
    """过去d天滚动均值"""
    out = [None] * len(x)
    for i in range(d - 1, len(x)):
        out[i] = sum(x[i - d + 1:i + 1]) / d
    return out


def ts_sum(x: list[float], d: int) -> list[float]:
    """过去d天滚动求和"""
    out = [None] * len(x)
    for i in range(d - 1, len(x)):
        out[i] = sum(x[i - d + 1:i + 1])
    return out


def ts_std(x: list[float], d: int) -> list[float]:
    """过去d天滚动标准差"""
    out = [None] * len(x)
    for i in range(d - 1, len(x)):
        window = x[i - d + 1:i + 1]
        m = sum(window) / d
        var = sum((v - m) ** 2 for v in window) / (d - 1) if d > 1 else 0
        out[i] = var ** 0.5
    return out


def ts_max(x: list[float], d: int) -> list[float]:
    """过去d天滚动最大值"""
    out = [None] * len(x)
    for i in range(d - 1, len(x)):
        out[i] = max(x[i - d + 1:i + 1])
    return out


def ts_min(x: list[float], d: int) -> list[float]:
    """过去d天滚动最小值"""
    out = [None] * len(x)
    for i in range(d - 1, len(x)):
        out[i] = min(x[i - d + 1:i + 1])
    return out


def ts_rank(x: list[float], d: int) -> list[float]:
    """过去d天时序排名百分位（当前值在窗口中的排名/窗口长度）"""
    out = [None] * len(x)
    for i in range(d - 1, len(x)):
        window = x[i - d + 1:i + 1]
        cur = x[i]
        rank = sum(1 for v in window if v <= cur)
        out[i] = rank / d
    return out


def ts_argmax(x: list[float], d: int) -> list[float]:
    """过去d天最大值出现的位置（0=最远，d-1=最近）"""
    out = [None] * len(x)
    for i in range(d - 1, len(x)):
        window = x[i - d + 1:i + 1]
        out[i] = max(range(d), key=lambda j: window[j])
    return out


def ts_argmin(x: list[float], d: int) -> list[float]:
    """过去d天最小值出现的位置"""
    out = [None] * len(x)
    for i in range(d - 1, len(x)):
        window = x[i - d + 1:i + 1]
        out[i] = min(range(d), key=lambda j: window[j])
    return out


def delta(x: list[float], d: int) -> list[float]:
    """x[t] - x[t-d]"""
    out = [None] * len(x)
    for i in range(d, len(x)):
        if x[i] is not None and x[i - d] is not None:
            out[i] = x[i] - x[i - d]
    return out


def delay(x: list[float], d: int) -> list[float]:
    """x[t-d]"""
    out = [None] * len(x)
    for i in range(d, len(x)):
        out[i] = x[i - d]
    return out


def correlation(x: list[float], y: list[float], d: int) -> list[float]:
    """过去d天滚动相关系数"""
    n = min(len(x), len(y))
    out = [None] * n
    for i in range(d - 1, n):
        wx = x[i - d + 1:i + 1]
        wy = y[i - d + 1:i + 1]
        if None in wx or None in wy:
            continue
        mx = sum(wx) / d
        my = sum(wy) / d
        cov = sum((a - mx) * (b - my) for a, b in zip(wx, wy))
        sx = (sum((a - mx) ** 2 for a in wx)) ** 0.5
        sy = (sum((b - my) ** 2 for b in wy)) ** 0.5
        out[i] = cov / (sx * sy) if sx > 0 and sy > 0 else 0
    return out


def decay_linear(x: list[float], d: int) -> list[float]:
    """线性衰减加权均值：最近的权重最大"""
    weights = list(range(1, d + 1))
    w_sum = sum(weights)
    out = [None] * len(x)
    for i in range(d - 1, len(x)):
        window = x[i - d + 1:i + 1]
        if None in window:
            continue
        out[i] = sum(w * v for w, v in zip(weights, window)) / w_sum
    return out


def returns(close: list[float]) -> list[float]:
    """日收益率序列"""
    out = [None] * len(close)
    for i in range(1, len(close)):
        if close[i - 1] and close[i - 1] > 0:
            out[i] = (close[i] - close[i - 1]) / close[i - 1]
    return out


# ═══════════════════════════════════════════════════════════
# S&C 经典技术指标（保留原味，最少参数）
# ═══════════════════════════════════════════════════════════

def rsi(close: list[float], period: int = 14) -> list[float]:
    """Relative Strength Index — Welles Wilder原版"""
    ret = returns(close)
    out = [None] * len(close)
    if len(close) < period + 1:
        return out
    gains = [max(0, r) if r is not None else 0 for r in ret]
    losses = [max(0, -r) if r is not None else 0 for r in ret]
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period
    for i in range(period, len(close)):
        if i > period:
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            out[i] = 100 - 100 / (1 + rs)
        else:
            out[i] = 100
    return out


def atr(high: list[float], low: list[float], close: list[float],
        period: int = 14) -> list[float]:
    """Average True Range — S&C经典波动率指标"""
    n = len(close)
    tr = [None] * n
    tr[0] = high[0] - low[0] if high[0] and low[0] else None
    for i in range(1, n):
        if None in (high[i], low[i], close[i - 1]):
            continue
        tr[i] = max(high[i] - low[i],
                     abs(high[i] - close[i - 1]),
                     abs(low[i] - close[i - 1]))
    out = [None] * n
    valid_tr = [v for v in tr[1:period + 1] if v is not None]
    if len(valid_tr) >= period:
        out[period] = sum(valid_tr) / period
        for i in range(period + 1, n):
            if tr[i] is not None and out[i - 1] is not None:
                out[i] = (out[i - 1] * (period - 1) + tr[i]) / period
    return out


def chaikin_money_flow(high: list[float], low: list[float],
                       close: list[float], volume: list[float],
                       period: int = 20) -> list[float]:
    """Chaikin Money Flow — S&C资金流指标，无额外参数"""
    n = len(close)
    mfv = [None] * n
    for i in range(n):
        if None in (high[i], low[i], close[i], volume[i]):
            continue
        hl = high[i] - low[i]
        if hl > 0:
            mfv[i] = ((close[i] - low[i]) - (high[i] - close[i])) / hl * volume[i]
        else:
            mfv[i] = 0
    out = [None] * n
    for i in range(period - 1, n):
        window_mfv = mfv[i - period + 1:i + 1]
        window_vol = volume[i - period + 1:i + 1]
        if None in window_mfv or None in window_vol:
            continue
        vol_sum = sum(window_vol)
        out[i] = sum(window_mfv) / vol_sum if vol_sum > 0 else 0
    return out


def force_index(close: list[float], volume: list[float],
                period: int = 13) -> list[float]:
    """Elder's Force Index — S&C力量指数"""
    n = len(close)
    raw = [None] * n
    for i in range(1, n):
        if None not in (close[i], close[i - 1], volume[i]):
            raw[i] = (close[i] - close[i - 1]) * volume[i]
    # EMA平滑
    out = [None] * n
    k = 2 / (period + 1)
    for i in range(1, n):
        if raw[i] is None:
            continue
        if out[i - 1] is None:
            out[i] = raw[i]
        else:
            out[i] = raw[i] * k + out[i - 1] * (1 - k)
    return out
