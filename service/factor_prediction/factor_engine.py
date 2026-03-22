#!/usr/bin/env python3
"""
多源因子计算引擎
================
理论来源：
  - 国泰君安191因子体系（量价因子）
  - S&C杂志（技术指标因子）
  - SSRN学术论文（异象因子）
  - Quantocracy/Alpha Architect（实证因子）

设计原则：
  1. 保留因子原味 — 直接输出原始因子值，不做过度变换
  2. 最少参数 — 每个因子只用1-2个窗口参数，均为学术文献推荐值
  3. 泛化优先 — 只选有经济学逻辑支撑的因子，拒绝纯数据挖掘
  4. 避免过拟合 — 不做因子阈值优化，用截面rank标准化

因子分类：
  A. 量价因子（来源：国君191 + S&C）
  B. 基本面因子（来源：SSRN + CAN SLIM）
  C. 另类因子（来源：Quantocracy + 本土特色）
"""
import logging
import math
from typing import Optional

from service.factor_prediction.alpha_operators import (
    ts_mean, ts_sum, ts_std, ts_max, ts_min, ts_rank,
    ts_argmax, ts_argmin, delta, delay, correlation,
    decay_linear, returns, rsi, atr, chaikin_money_flow, force_index,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# A. 量价因子（Price-Volume Factors）
# ═══════════════════════════════════════════════════════════

def compute_price_volume_factors(klines: list[dict]) -> dict:
    """
    从K线数据计算量价因子。

    Args:
        klines: 按日期升序排列的K线列表，每条包含:
                close, open, high, low, volume, change_percent, turnover

    Returns:
        dict: 因子名→因子值（最新一天的值）

    因子列表及理论来源：
      momentum_20d    — 20日动量（Jegadeesh & Titman 1993, SSRN经典）
      momentum_60d    — 60日动量（中期趋势，S&C常用周期）
      reversal_5d     — 5日反转（短期均值回归，国君191 Alpha#12变体）
      volatility_20d  — 20日已实现波动率（Low-Vol Anomaly, SSRN）
      volume_ratio    — 5日/20日量比（国君191量价因子）
      turnover_20d    — 20日平均换手率（流动性因子，SSRN）
      amihud_20d      — Amihud非流动性（Amihud 2002, SSRN经典）
      rsi_14          — 14日RSI（Wilder, S&C原版）
      cmf_20          — 20日Chaikin资金流（S&C）
      force_13        — 13日Force Index（Elder, S&C）
      atr_14          — 14日ATR（S&C波动率）
      price_pos_60    — 60日价格位置（0~1，国君191变体）
      vol_price_corr  — 20日量价相关系数（国君191 Alpha#6变体）
      consec_down     — 连跌天数（行为金融，过度反应因子）
      consec_up       — 连涨天数
      upper_shadow    — 上影线比例（K线形态因子，S&C）
      gap_ratio       — 跳空缺口比例（S&C技术形态）
    """
    if len(klines) < 60:
        return {}

    close = [k.get('close', 0) or 0 for k in klines]
    open_ = [k.get('open', 0) or 0 for k in klines]
    high = [k.get('high', 0) or 0 for k in klines]
    low = [k.get('low', 0) or 0 for k in klines]
    volume = [k.get('volume', 0) or 0 for k in klines]
    pct = [k.get('change_percent', 0) or 0 for k in klines]
    turnover = [k.get('turnover', 0) or 0 for k in klines]

    n = len(klines)
    factors = {}

    # ── 动量因子（SSRN: Jegadeesh & Titman 1993）──
    if close[-1] > 0 and close[-21] > 0:
        factors['momentum_20d'] = (close[-1] / close[-21] - 1) * 100
    if close[-1] > 0 and n >= 61 and close[-61] > 0:
        factors['momentum_60d'] = (close[-1] / close[-61] - 1) * 100

    # ── 短期反转（国君191 Alpha#12变体 + SSRN短期反转异象）──
    if n >= 6 and close[-1] > 0 and close[-6] > 0:
        factors['reversal_5d'] = (close[-1] / close[-6] - 1) * 100

    # ── 波动率因子（SSRN: Low Volatility Anomaly）──
    if len(pct) >= 20:
        recent_pct = pct[-20:]
        m = sum(recent_pct) / 20
        factors['volatility_20d'] = (sum((p - m) ** 2 for p in recent_pct) / 19) ** 0.5

    # ── 量比因子（国君191量价因子）──
    vol_5 = sum(volume[-5:]) / 5 if len(volume) >= 5 else 0
    vol_20 = sum(volume[-20:]) / 20 if len(volume) >= 20 else 0
    if vol_20 > 0:
        factors['volume_ratio'] = vol_5 / vol_20

    # ── 换手率因子（SSRN流动性溢价）──
    if len(turnover) >= 20:
        factors['turnover_20d'] = sum(turnover[-20:]) / 20

    # ── Amihud非流动性（Amihud 2002, SSRN经典因子）──
    amihud_vals = []
    for i in range(-20, 0):
        if abs(i) <= n and volume[i] > 0:
            amihud_vals.append(abs(pct[i]) / (volume[i] * close[i] / 1e8 + 1e-10))
    if amihud_vals:
        factors['amihud_20d'] = sum(amihud_vals) / len(amihud_vals)

    # ── RSI（Wilder, S&C原版14日）──
    rsi_vals = rsi(close, 14)
    if rsi_vals[-1] is not None:
        factors['rsi_14'] = rsi_vals[-1]

    # ── Chaikin Money Flow（S&C, 20日）──
    cmf_vals = chaikin_money_flow(high, low, close, volume, 20)
    if cmf_vals[-1] is not None:
        factors['cmf_20'] = cmf_vals[-1]

    # ── Force Index（Elder, S&C, 13日EMA）──
    fi_vals = force_index(close, volume, 13)
    if fi_vals[-1] is not None:
        factors['force_13'] = fi_vals[-1]

    # ── ATR（S&C, 14日）──
    atr_vals = atr(high, low, close, 14)
    if atr_vals[-1] is not None and close[-1] > 0:
        factors['atr_14'] = atr_vals[-1] / close[-1]  # 归一化为ATR%

    # ── 价格位置（国君191变体）──
    if n >= 60:
        h60 = max(close[-60:])
        l60 = min(c for c in close[-60:] if c > 0) if any(c > 0 for c in close[-60:]) else 0
        if h60 > l60:
            factors['price_pos_60'] = (close[-1] - l60) / (h60 - l60)

    # ── 量价相关系数（国君191 Alpha#6: -corr(open, volume, 10)）──
    corr_vals = correlation(close[-21:], volume[-21:], 20)
    if corr_vals and corr_vals[-1] is not None:
        factors['vol_price_corr'] = corr_vals[-1]

    # ── 连涨/连跌天数（行为金融：过度反应因子）──
    cd, cu = 0, 0
    for p in reversed(pct):
        if p < 0:
            cd += 1
        else:
            break
    for p in reversed(pct):
        if p > 0:
            cu += 1
        else:
            break
    factors['consec_down'] = cd
    factors['consec_up'] = cu

    # ── 上影线比例（S&C K线形态）──
    if high[-1] > low[-1] and high[-1] > 0:
        factors['upper_shadow'] = (high[-1] - max(close[-1], open_[-1])) / (high[-1] - low[-1])

    # ── 跳空缺口（S&C技术形态）──
    if n >= 2 and close[-2] > 0:
        factors['gap_ratio'] = (open_[-1] - close[-2]) / close[-2] * 100

    # ── A股特色量价因子（基于实证研究）──

    # 异常换手率（A股散户行为因子：异常高换手后反转）
    if len(turnover) >= 20:
        avg_turn_20 = sum(turnover[-20:]) / 20
        if avg_turn_20 > 0:
            factors['abnormal_turnover'] = turnover[-1] / avg_turn_20

    # 振幅因子（A股涨跌停制度下的波动特征）
    if n >= 5:
        amplitudes = [(high[i] - low[i]) / close[i] * 100 for i in range(-5, 0) if close[i] > 0]
        if amplitudes:
            factors['amplitude_5d'] = sum(amplitudes) / len(amplitudes)

    # 量价背离强度（A股经典技术分析因子）
    if n >= 10:
        price_chg_10 = (close[-1] / close[-11] - 1) if close[-11] > 0 else 0
        vol_chg_10 = (sum(volume[-5:]) / max(sum(volume[-10:-5]), 1)) - 1
        factors['vol_price_diverge'] = price_chg_10 * (-1 if vol_chg_10 < -0.2 else 1)

    # 尾盘效应因子（A股T+1制度下尾盘信息含量高）
    if n >= 1 and high[-1] > low[-1]:
        factors['close_position'] = (close[-1] - low[-1]) / (high[-1] - low[-1])

    # 20日收益偏度（A股彩票效应：高偏度股票未来收益低）
    if len(pct) >= 20:
        recent = pct[-20:]
        m = sum(recent) / 20
        s = (sum((p - m) ** 2 for p in recent) / 19) ** 0.5
        if s > 0:
            factors['skewness_20d'] = sum((p - m) ** 3 for p in recent) / (20 * s ** 3)

    return factors


# ═══════════════════════════════════════════════════════════
# B. 基本面因子（Fundamental Factors）
# ═══════════════════════════════════════════════════════════

def compute_fundamental_factors(finance_records: list[dict]) -> dict:
    """
    从财报数据计算基本面因子。

    理论来源：
      earnings_surprise  — SUE标准化预期外盈利（Ball & Brown 1968, SSRN）
      revenue_accel      — 营收加速度（连续两季增速之差，Quantocracy常见）
      profit_quality     — 盈利质量（经营现金流/净利润，Sloan 1996 SSRN）
      roe                — 净资产收益率（Fama-French质量因子）
      revenue_yoy        — 营收同比增长（CAN SLIM的C维度核心）
      profit_yoy         — 净利润同比增长（CAN SLIM的C维度）

    Args:
        finance_records: 按报告日期降序排列的财报记录（已解析的dict列表）
    """
    if not finance_records or len(finance_records) < 2:
        return {}

    factors = {}

    def _sf(v):
        if v is None:
            return 0.0
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

    latest = finance_records[0]
    prev = finance_records[1]

    # ── 营收同比增长（CAN SLIM C维度）──
    rev_yoy = _sf(latest.get('营业总收入同比增长(%)') or latest.get('单季营业收入同比增长(%)'))
    factors['revenue_yoy'] = rev_yoy

    # ── 净利润同比增长（CAN SLIM C维度）──
    profit_yoy = _sf(latest.get('扣非净利润同比增长(%)') or latest.get('单季度扣非净利润同比增长(%)'))
    factors['profit_yoy'] = profit_yoy

    # ── ROE（Fama-French质量因子）──
    roe = _sf(latest.get('净资产收益率(%)'))
    factors['roe'] = roe

    # ── 营收加速度（Quantocracy: revenue acceleration）──
    prev_rev_yoy = _sf(prev.get('营业总收入同比增长(%)') or prev.get('单季营业收入同比增长(%)'))
    factors['revenue_accel'] = rev_yoy - prev_rev_yoy

    # ── 盈利惊喜 SUE（SSRN: Ball & Brown 1968）──
    # 简化版：(本季EPS - 去年同季EPS) / std(过去4季EPS差)
    eps_list = []
    for rec in finance_records[:8]:
        eps_list.append(_sf(rec.get('基本每股收益(元)')))
    if len(eps_list) >= 5:
        diffs = [eps_list[i] - eps_list[i + 4] for i in range(min(4, len(eps_list) - 4))]
        if diffs:
            m = sum(diffs) / len(diffs)
            std = (sum((d - m) ** 2 for d in diffs) / max(len(diffs) - 1, 1)) ** 0.5
            if std > 0.001:
                factors['earnings_surprise'] = diffs[0] / std
            else:
                factors['earnings_surprise'] = diffs[0] * 100  # 标准差极小时直接用差值

    # ── 盈利质量（Sloan 1996 Accrual Anomaly）──
    cash_flow = _sf(latest.get('每股经营现金流(元)'))
    eps = _sf(latest.get('基本每股收益(元)'))
    if eps != 0:
        factors['profit_quality'] = cash_flow / eps
    elif cash_flow > 0:
        factors['profit_quality'] = 1.0
    else:
        factors['profit_quality'] = -1.0

    return factors


# ═══════════════════════════════════════════════════════════
# C. 另类因子（Alternative Factors）
# ═══════════════════════════════════════════════════════════

def compute_alternative_factors(fund_flow: list[dict],
                                concept_strength: list[dict] = None,
                                market_klines: list[dict] = None,
                                stock_klines: list[dict] = None) -> dict:
    """
    计算另类因子。

    理论来源：
      smart_money_5d     — 聪明钱因子（大单净流入占比，Quantocracy）
      fund_flow_momentum — 资金流动量（5日资金流趋势，本土特色因子）
      board_strength_max — 概念板块最大强度（板块动量，本土特色）
      board_consensus    — 板块一致性（多板块方向一致度）
      relative_strength  — 相对强弱（个股vs大盘，O'Neil RS, CAN SLIM L维度）
      excess_return_20d  — 20日超额收益（Alpha Architect常用）

    Args:
        fund_flow: 按日期降序的资金流向数据
        concept_strength: 个股概念板块强弱势评分列表
        market_klines: 大盘K线（升序）
        stock_klines: 个股K线（升序）
    """
    factors = {}

    def _sf(v):
        if v is None:
            return 0.0
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

    # ── 聪明钱因子（Quantocracy: Smart Money）──
    if fund_flow and len(fund_flow) >= 5:
        big_net_pcts = [_sf(f.get('big_net_pct')) for f in fund_flow[:5]]
        factors['smart_money_5d'] = sum(big_net_pcts) / 5

    # ── 资金流动量（本土特色）──
    if fund_flow and len(fund_flow) >= 10:
        recent_5 = sum(_sf(f.get('net_flow', 0)) for f in fund_flow[:5])
        prev_5 = sum(_sf(f.get('net_flow', 0)) for f in fund_flow[5:10])
        if abs(prev_5) > 0:
            factors['fund_flow_momentum'] = (recent_5 - prev_5) / (abs(prev_5) + 1e-10)
        else:
            factors['fund_flow_momentum'] = 1.0 if recent_5 > 0 else -1.0

    # ── 概念板块强度（本土特色因子）──
    if concept_strength:
        scores = [_sf(cs.get('strength_score', 0)) for cs in concept_strength]
        if scores:
            factors['board_strength_max'] = max(scores)
            factors['board_strength_avg'] = sum(scores) / len(scores)
            # 板块一致性：强势板块占比
            strong = sum(1 for s in scores if s >= 60)
            factors['board_consensus'] = strong / len(scores)

    # ── 相对强弱（O'Neil RS Rating, CAN SLIM L维度）──
    if market_klines and stock_klines and len(market_klines) >= 20 and len(stock_klines) >= 20:
        stock_ret_20 = sum(_sf(k.get('change_percent', 0)) for k in stock_klines[-20:])
        market_ret_20 = sum(_sf(k.get('change_percent', 0)) for k in market_klines[-20:])
        factors['relative_strength_20d'] = stock_ret_20 - market_ret_20

        if len(market_klines) >= 60 and len(stock_klines) >= 60:
            stock_ret_60 = sum(_sf(k.get('change_percent', 0)) for k in stock_klines[-60:])
            market_ret_60 = sum(_sf(k.get('change_percent', 0)) for k in market_klines[-60:])
            factors['excess_return_60d'] = stock_ret_60 - market_ret_60

    return factors


# ═══════════════════════════════════════════════════════════
# 因子合成：截面Rank标准化（避免过拟合的关键）
# ═══════════════════════════════════════════════════════════

def cross_sectional_rank(stock_factors: dict[str, dict], factor_name: str) -> dict[str, float]:
    """
    截面排序标准化：将因子值转为0~1的排名百分位。

    这是避免过拟合的核心手段（来源：SSRN多因子文献共识）：
    - 不需要设定因子阈值
    - 对异常值天然鲁棒
    - 不同因子可以直接相加

    Args:
        stock_factors: {stock_code: {factor_name: value, ...}, ...}
        factor_name: 要排序的因子名

    Returns:
        {stock_code: rank_percentile}  (0=最差, 1=最好)
    """
    vals = {}
    for code, fdict in stock_factors.items():
        v = fdict.get(factor_name)
        if v is not None and not math.isnan(v) and not math.isinf(v):
            vals[code] = v

    if not vals:
        return {}

    sorted_codes = sorted(vals.keys(), key=lambda c: vals[c])
    n = len(sorted_codes)
    return {code: (i / (n - 1) if n > 1 else 0.5) for i, code in enumerate(sorted_codes)}
