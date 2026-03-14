"""
基于历史K线数据的自动回测模块

不依赖 LLM 预测记录，直接从 stock_kline 表读取历史数据，
用 Python 端纯算技术指标 + 评分模型生成"模拟预测"，
然后与实际涨跌对比，验证评分模型的准确率。

可计算的维度（5/7，满分80分）：
- 趋势强度（20分）：MACD + 均线排列 + 周线趋势
- 动能与量价（20分）：KDJ + 量价形态
- 结构边界（15分）：BOLL + 乖离率
- 资金筹码近似（15分）：成交额趋势 + 换手率异常 + 量价背离
- 风险收益比（10分）：BOLL空间 + 距高低点

不可计算的维度（2/7，满分20分）：
- 短线情绪（15分）：需要分时/盘口/实时资金流
- 外部环境（5分）：需要大盘指数/消息面
"""
import logging
import math
import random
from collections import defaultdict
from datetime import datetime
from typing import Optional

from dao import get_connection
from dao.stock_kline_dao import get_kline_data

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 技术指标计算（纯 K线数据）
# ═══════════════════════════════════════════════════════════

def _ema(data: list[float], period: int) -> list[float]:
    """指数移动平均线，返回与 data 等长的列表"""
    if not data:
        return []
    result = [0.0] * len(data)
    k = 2 / (period + 1)
    result[0] = data[0]
    for i in range(1, len(data)):
        result[i] = data[i] * k + result[i - 1] * (1 - k)
    return result


def _sma(data: list[float], period: int) -> list[float]:
    """简单移动平均线"""
    result = [0.0] * len(data)
    for i in range(len(data)):
        if i < period - 1:
            result[i] = sum(data[:i + 1]) / (i + 1)
        else:
            result[i] = sum(data[i - period + 1:i + 1]) / period
    return result


def _calc_macd(closes: list[float], fast=12, slow=26, signal=9) -> list[dict]:
    """计算 MACD 指标，返回按日期升序的 [{DIF, DEA, MACD柱, 市场状态}, ...]"""
    if len(closes) < slow + signal:
        return []
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    dif = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]
    dea = _ema(dif, signal)
    macd_bar = [(dif[i] - dea[i]) * 2 for i in range(len(closes))]

    result = []
    for i in range(len(closes)):
        if dif[i] > 0 and dea[i] > 0:
            state = '强多头'
        elif dif[i] > 0:
            state = '弱多头'
        else:
            state = '空头'
        result.append({
            'DIF': round(dif[i], 4),
            'DEA': round(dea[i], 4),
            'MACD柱': round(macd_bar[i], 4),
            '市场状态': state,
        })
    return result


def _calc_kdj(highs: list[float], lows: list[float], closes: list[float],
              n=9, m1=3, m2=3) -> list[dict]:
    """计算 KDJ 指标"""
    if len(closes) < n:
        return []
    result = []
    k_prev, d_prev = 50.0, 50.0
    for i in range(len(closes)):
        if i < n - 1:
            result.append({'K': 50.0, 'D': 50.0, 'J': 50.0})
            continue
        low_n = min(lows[i - n + 1:i + 1])
        high_n = max(highs[i - n + 1:i + 1])
        rsv = (closes[i] - low_n) / (high_n - low_n) * 100 if high_n != low_n else 50
        k = (m1 - 1) / m1 * k_prev + 1 / m1 * rsv
        d = (m2 - 1) / m2 * d_prev + 1 / m2 * k
        j = 3 * k - 2 * d
        k_prev, d_prev = k, d
        result.append({'K': round(k, 2), 'D': round(d, 2), 'J': round(j, 2)})
    return result


def _calc_boll(closes: list[float], period=20, mult=2) -> list[dict]:
    """计算布林带"""
    if len(closes) < period:
        return []
    result = []
    for i in range(len(closes)):
        if i < period - 1:
            result.append({'上轨': None, '中轨': None, '下轨': None, '带宽': None})
            continue
        window = closes[i - period + 1:i + 1]
        mid = sum(window) / period
        std = math.sqrt(sum((x - mid) ** 2 for x in window) / period)
        upper = mid + mult * std
        lower = mid - mult * std
        bw = (upper - lower) / mid * 100 if mid > 0 else 0
        result.append({
            '上轨': round(upper, 3),
            '中轨': round(mid, 3),
            '下轨': round(lower, 3),
            '带宽': round(bw, 2),
        })
    return result


def _calc_ma(closes: list[float], periods=(5, 10, 20, 60)) -> dict[int, list[float]]:
    """计算多条均线"""
    return {p: _sma(closes, p) for p in periods}


def _calc_bias(closes: list[float], ma_values: list[float]) -> list[float]:
    """计算乖离率 BIAS = (close - MA) / MA * 100"""
    return [round((closes[i] - ma_values[i]) / ma_values[i] * 100, 2)
            if ma_values[i] > 0 else 0 for i in range(len(closes))]


# ═══════════════════════════════════════════════════════════
# 简化版评分（4个维度，满分65分）
# ═══════════════════════════════════════════════════════════

def _score_from_kline(klines_asc: list[dict], fund_flow_data: list[dict] = None) -> Optional[dict]:
    """从K线数据计算简化版综合评分

    Args:
        klines_asc: K线数据列表，按日期升序，需要至少120条
        fund_flow_data: 同花顺历史资金流数据（按日期倒序，万元单位），可选。
                        提供时用真实主力资金数据替代K线近似的资金筹码维度。

    Returns:
        评分结果 dict 或 None（数据不足）
    """
    if len(klines_asc) < 60:
        return None

    closes = [k['close_price'] for k in klines_asc]
    highs = [k['high_price'] for k in klines_asc]
    lows = [k['low_price'] for k in klines_asc]
    volumes = [k.get('trading_volume', 0) or 0 for k in klines_asc]
    changes = [k.get('change_percent', 0) or 0 for k in klines_asc]

    n = len(closes)
    idx = n - 1  # 最新一天的索引

    # 计算技术指标
    macd_list = _calc_macd(closes)
    kdj_list = _calc_kdj(highs, lows, closes)
    boll_list = _calc_boll(closes)
    ma_dict = _calc_ma(closes)

    if not macd_list or not kdj_list or not boll_list:
        return None

    macd = macd_list[idx]
    macd_prev = macd_list[idx - 1] if idx > 0 else macd
    kdj = kdj_list[idx]
    kdj_prev = kdj_list[idx - 1] if idx > 0 else kdj
    boll = boll_list[idx]
    boll_prev = boll_list[idx - 1] if idx > 0 else boll

    scores = {}
    details = {}

    # ════════════════════════════════════════
    # 维度1：趋势强度（满分20分）
    # ════════════════════════════════════════
    trend = 10
    trend_r = []

    # MACD 市场状态
    if macd['市场状态'] == '强多头':
        trend += 4; trend_r.append('MACD强多头+4')
    elif macd['市场状态'] == '弱多头':
        trend += 1; trend_r.append('MACD弱多头+1')
    else:
        trend -= 3; trend_r.append('MACD空头-3')

    # MACD柱趋势
    bar = macd['MACD柱']
    bar_prev = macd_prev['MACD柱']
    if bar > 0 and abs(bar) > abs(bar_prev) and bar_prev > 0:
        trend += 2; trend_r.append('红柱放大+2')
    elif bar > 0 and abs(bar) < abs(bar_prev) and bar_prev > 0:
        trend_r.append('红柱收窄+0')
    elif bar < 0 and abs(bar) < abs(bar_prev) and bar_prev < 0:
        trend += 1; trend_r.append('绿柱收窄+1')
    elif bar < 0 and abs(bar) > abs(bar_prev) and bar_prev < 0:
        trend -= 2; trend_r.append('绿柱放大-2')

    # 均线排列
    ma5 = ma_dict[5][idx]
    ma10 = ma_dict[10][idx]
    ma20 = ma_dict[20][idx]
    ma60 = ma_dict[60][idx] if idx >= 59 else ma20

    if ma5 > ma10 > ma20 > ma60:
        trend += 4; trend_r.append('多头排列+4')
        alignment = '多头排列'
    elif ma5 < ma10 < ma20 < ma60:
        trend -= 4; trend_r.append('空头排列-4')
        alignment = '空头排列'
    else:
        trend_r.append('均线纠缠+0')
        alignment = '纠缠'

    # 简化周线：用20日和60日均线模拟
    if idx >= 59:
        ma20_prev20 = ma_dict[20][idx - 20] if idx >= 20 else ma20
        ma60_prev20 = ma_dict[60][idx - 20] if idx >= 79 else ma60
        weekly_bull = ma20 > ma60 and ma20 > ma20_prev20
        weekly_bear = ma20 < ma60 and ma20 < ma20_prev20
        if weekly_bull and '多头' in alignment:
            trend += 4; trend_r.append('日周共振多头+4')
        elif weekly_bull:
            trend += 2; trend_r.append('周线多头+2')
        elif weekly_bear and '空头' in alignment:
            trend -= 4; trend_r.append('日周共振空头-4')
        elif weekly_bear:
            trend -= 2; trend_r.append('周线空头-2')

    trend = max(0, min(20, trend))
    scores['趋势强度'] = trend
    details['趋势强度'] = trend_r

    # ════════════════════════════════════════
    # 维度2：动能与量价（满分20分）
    # ════════════════════════════════════════
    momentum = 10
    mom_r = []

    k, j = kdj['K'], kdj['J']
    k_prev, j_prev = kdj_prev['K'], kdj_prev['J']
    k_change = k - k_prev
    j_change = j - j_prev

    # 近5日超买超卖
    was_oversold = any(kdj_list[i]['K'] < 20 and kdj_list[i]['J'] < 0
                       for i in range(max(0, idx - 4), idx + 1))
    was_overbought = any(kdj_list[i]['K'] > 80 and kdj_list[i]['J'] > 100
                         for i in range(max(0, idx - 4), idx + 1))

    if was_oversold and k > k_prev and j > j_prev and k_prev < 30:
        momentum += 6; mom_r.append('超卖区低位拐头+6')
    elif k > k_prev and j > j_prev and k_prev < 30:
        momentum += 3; mom_r.append('低位拐头向上+3')
    elif was_overbought and k < k_prev and j < j_prev and k_prev > 75:
        momentum -= 4; mom_r.append('超买区高位拐头-4')
    elif k < k_prev and j < j_prev and k_prev > 75:
        momentum -= 2; mom_r.append('高位拐头向下-2')
    elif k > 50 and k_change > 0:
        momentum += 1; mom_r.append('中高位上行+1')
    elif k < 50 and k_change < 0:
        momentum -= 1; mom_r.append('中低位下行-1')

    # 量价形态
    if idx >= 1:
        vol_now = volumes[idx]
        vol_prev = volumes[idx - 1]
        chg_now = changes[idx]
        chg_prev = changes[idx - 1]
        vol_change_pct = (vol_now - vol_prev) / vol_prev * 100 if vol_prev > 0 else 0

        if vol_change_pct > 20 and chg_now > 2:
            momentum += 4; mom_r.append('放量上攻+4')
        elif vol_change_pct > 20 and chg_prev > 0 and chg_now < 0:
            momentum -= 1; mom_r.append('放量冲高缩量回落-1')
        elif vol_change_pct > 20 and chg_now < -2:
            momentum -= 3; mom_r.append('放量杀跌-3')
        elif vol_change_pct < -20 and chg_now > 0:
            mom_r.append('缩量反弹+0')
        elif vol_change_pct < -20 and chg_now < 0:
            momentum -= 2; mom_r.append('缩量阴跌-2')

    # 量能趋势
    if idx >= 4:
        recent_vols = [volumes[idx - i] for i in range(5)]
        if all(recent_vols[i] >= recent_vols[i + 1] for i in range(4)):
            momentum += 2; mom_r.append('连续放量+2')
        elif all(recent_vols[i] <= recent_vols[i + 1] for i in range(4)):
            momentum -= 2; mom_r.append('连续缩量-2')

    momentum = max(0, min(20, momentum))
    scores['动能与量价'] = momentum
    details['动能与量价'] = mom_r

    # ════════════════════════════════════════
    # 维度3：结构边界（满分15分）
    # ════════════════════════════════════════
    structure = 7
    str_r = []

    if boll['中轨'] and boll['上轨']:
        close = closes[idx]
        mid = boll['中轨']
        upper = boll['上轨']
        lower = boll['下轨']
        mid_prev = boll_prev['中轨'] if boll_prev['中轨'] else mid

        above_mid = close > mid
        mid_rising = mid > mid_prev

        if above_mid and mid_rising:
            structure += 4; str_r.append('中轨上方+上倾+4')
        elif above_mid:
            structure += 1; str_r.append('中轨上方+下倾+1')
        elif not above_mid and not mid_rising:
            structure -= 3; str_r.append('中轨下方+下倾-3')
        elif not above_mid:
            structure -= 1; str_r.append('中轨下方-1')

        # 乖离率
        bias5 = _calc_bias(closes, ma_dict[5])
        if bias5[idx] > 7:
            structure -= 2; str_r.append(f'BIAS5={bias5[idx]}超买-2')
        elif bias5[idx] < -7:
            structure += 1; str_r.append(f'BIAS5={bias5[idx]}超卖+1')

        # 上方空间
        dist_upper_pct = round((upper - close) / close * 100, 2)
        if dist_upper_pct > 10:
            structure += 3; str_r.append(f'距上轨{dist_upper_pct}%+3')
        elif dist_upper_pct > 5:
            structure += 1; str_r.append(f'距上轨{dist_upper_pct}%+1')
        elif dist_upper_pct < 2:
            structure -= 1; str_r.append(f'距上轨{dist_upper_pct}%-1')

    structure = max(0, min(15, structure))
    scores['结构边界'] = structure
    details['结构边界'] = str_r

    # ════════════════════════════════════════
    # 维度4：风险收益比（满分10分）
    # ════════════════════════════════════════
    rr = 5
    rr_r = []

    if boll['中轨'] and boll['上轨']:
        close = closes[idx]
        upper = boll['上轨']
        mid = boll['中轨']
        dist_up = round((upper - close) / close * 100, 2)
        dist_mid = round((close - mid) / close * 100, 2) if close > mid else 0

        if dist_mid > 0:
            ratio = round(dist_up / dist_mid, 1)
            if ratio >= 3:
                rr += 4; rr_r.append(f'风险收益比{ratio}:1+4')
            elif ratio >= 2:
                rr += 2; rr_r.append(f'风险收益比{ratio}:1+2')
            elif ratio < 1:
                rr -= 2; rr_r.append(f'风险收益比{ratio}:1-2')
        else:
            rr -= 2; rr_r.append('中轨下方-2')

    # 距120日高点
    lookback = min(120, len(closes))
    high_120 = max(closes[idx - lookback + 1:idx + 1])
    dist_high_pct = round((closes[idx] - high_120) / high_120 * 100, 1)
    if dist_high_pct < -30:
        rr += 1; rr_r.append(f'距120日高点{dist_high_pct}%深度回调+1')
    elif dist_high_pct > -5:
        rr -= 1; rr_r.append(f'距120日高点{dist_high_pct}%接近前高-1')

    rr = max(0, min(10, rr))
    scores['风险收益比'] = rr
    details['风险收益比'] = rr_r

    # ════════════════════════════════════════
    # 维度5：资金筹码（满分15分）
    # 有真实资金流数据时用同花顺大单数据，否则用成交额/换手率近似
    # ════════════════════════════════════════
    capital = 7
    cap_r = []

    if fund_flow_data and len(fund_flow_data) >= 3:
        # ── 使用同花顺真实主力资金数据 ──
        cap_r.append('[数据源:同花顺真实资金流]')

        # --- 连续大单净流入/流出天数（+4/-4分）---
        consecutive_inflow = 0
        consecutive_outflow = 0
        for row in fund_flow_data:
            big_net = row.get('big_net', None)
            if big_net is None:
                continue
            if big_net > 0:
                if consecutive_outflow == 0:
                    consecutive_inflow += 1
                else:
                    break
            elif big_net < 0:
                if consecutive_inflow == 0:
                    consecutive_outflow += 1
                else:
                    break

        if consecutive_inflow >= 5:
            capital += 4; cap_r.append(f'主力连续{consecutive_inflow}日净流入+4')
        elif consecutive_inflow >= 3:
            capital += 3; cap_r.append(f'主力连续{consecutive_inflow}日净流入+3')
        elif consecutive_inflow >= 2:
            capital += 1; cap_r.append(f'主力连续{consecutive_inflow}日净流入+1')
        elif consecutive_outflow >= 5:
            capital -= 4; cap_r.append(f'主力连续{consecutive_outflow}日净流出-4')
        elif consecutive_outflow >= 3:
            capital -= 3; cap_r.append(f'主力连续{consecutive_outflow}日净流出-3')
        elif consecutive_outflow >= 2:
            capital -= 1; cap_r.append(f'主力连续{consecutive_outflow}日净流出-1')
        else:
            cap_r.append('主力资金方向不明+0')

        # --- 近5日大单净额累计方向（+2/-2分）---
        recent_5_ff = fund_flow_data[:5]
        big_net_5d = sum((r.get('big_net', 0) or 0) for r in recent_5_ff)
        if big_net_5d > 500:
            capital += 2; cap_r.append(f'近5日大单累计净流入{big_net_5d:.0f}万+2')
        elif big_net_5d > 0:
            capital += 1; cap_r.append(f'近5日大单累计净流入{big_net_5d:.0f}万+1')
        elif big_net_5d < -500:
            capital -= 2; cap_r.append(f'近5日大单累计净流出{big_net_5d:.0f}万-2')
        elif big_net_5d < 0:
            capital -= 1; cap_r.append(f'近5日大单累计净流出{big_net_5d:.0f}万-1')

        # --- 大单净占比趋势（+2/-2分）---
        recent_pcts = [r.get('big_net_pct', None) for r in fund_flow_data[:5]]
        valid_pcts = [p for p in recent_pcts if p is not None]
        if len(valid_pcts) >= 3:
            avg_pct = sum(valid_pcts) / len(valid_pcts)
            if avg_pct > 3:
                capital += 2; cap_r.append(f'近期大单净占比均值{avg_pct:.1f}%偏高+2')
            elif avg_pct > 0:
                capital += 1; cap_r.append(f'近期大单净占比均值{avg_pct:.1f}%+1')
            elif avg_pct < -3:
                capital -= 2; cap_r.append(f'近期大单净占比均值{avg_pct:.1f}%偏低-2')
            elif avg_pct < 0:
                capital -= 1; cap_r.append(f'近期大单净占比均值{avg_pct:.1f}%-1')

    else:
        # ── K线近似模式（无真实资金流数据）──
        cap_r.append('[数据源:K线成交额/换手率近似]')
        amounts = [k.get('trading_amount', 0) or 0 for k in klines_asc]
        hand_rates = [k.get('change_hand', 0) or 0 for k in klines_asc]

        # --- 成交额趋势模拟"主力资金方向"（-3~+3分）---
        if idx >= 4:
            inflow_days = 0
            outflow_days = 0
            for d in range(5):
                di = idx - d
                if di < 1:
                    break
                amt_chg = (amounts[di] - amounts[di - 1]) / amounts[di - 1] if amounts[di - 1] > 0 else 0
                if amt_chg > 0.1 and changes[di] > 0:
                    inflow_days += 1
                elif amt_chg > 0.1 and changes[di] < 0:
                    outflow_days += 1

            if inflow_days >= 3:
                capital += 3; cap_r.append(f'近5日{inflow_days}天放量上涨（类主力流入）+3')
            elif inflow_days >= 2:
                capital += 1; cap_r.append(f'近5日{inflow_days}天放量上涨+1')
            elif outflow_days >= 3:
                capital -= 3; cap_r.append(f'近5日{outflow_days}天放量下跌（类主力流出）-3')
            elif outflow_days >= 2:
                capital -= 1; cap_r.append(f'近5日{outflow_days}天放量下跌-1')
            else:
                cap_r.append('资金方向不明+0')

        # --- 换手率异常检测模拟"龙虎榜/大宗交易"（-2~+2分）---
        if idx >= 20:
            avg_hand_20 = sum(hand_rates[idx - 19:idx + 1]) / 20 if any(hand_rates[idx - 19:idx + 1]) else 0
            latest_hand = hand_rates[idx]
            if avg_hand_20 > 0 and latest_hand > avg_hand_20 * 3:
                if changes[idx] > 3:
                    capital += 2; cap_r.append(f'换手率异常高({latest_hand:.1f}%>{avg_hand_20:.1f}%×3)+大涨+2')
                elif changes[idx] < -3:
                    capital -= 2; cap_r.append(f'换手率异常高+大跌-2（疑似主力出货）')
                else:
                    cap_r.append(f'换手率偏高但涨跌幅不大+0')

        # --- 量价背离检测（-2~+2分）---
        if idx >= 4:
            amt_5 = amounts[idx - 4:idx + 1]
            chg_5 = changes[idx - 4:idx + 1]
            amt_expanding = all(amt_5[i] >= amt_5[i - 1] * 0.9 for i in range(1, 5)) and amt_5[4] > amt_5[0] * 1.3
            price_flat_or_down = sum(chg_5) < 0

            if amt_expanding and price_flat_or_down:
                capital -= 2; cap_r.append('量增价跌（顶部出货信号）-2')
            elif not amt_expanding and sum(chg_5) > 0:
                amt_shrinking = amt_5[4] < amt_5[0] * 0.7 if amt_5[0] > 0 else False
                if amt_shrinking:
                    capital += 1; cap_r.append('缩量上涨（惜售信号）+1')

    capital = max(0, min(15, capital))
    scores['资金筹码'] = capital
    details['资金筹码'] = cap_r

    # ════════════════════════════════════════
    # 汇总（5维度满分80分，映射到100分制）
    # ════════════════════════════════════════
    raw_total = sum(scores.values())
    mapped_total = round(raw_total / 80 * 100)
    mapped_total = max(0, min(100, mapped_total))

    # 方向判定：收窄横盘区间（A股大部分交易日涨跌>0.3%，横盘很少）
    # 同时参考近3日动量方向作为辅助
    recent_momentum = sum(changes[-3:]) if len(changes) >= 3 else 0

    if mapped_total >= 53:
        direction = '上涨'
    elif mapped_total <= 47:
        direction = '下跌'
    elif recent_momentum > 0:
        direction = '上涨'
    elif recent_momentum < 0:
        direction = '下跌'
    else:
        direction = '横盘震荡'

    return {
        '原始得分': raw_total,
        '满分': 80,
        '映射总分': mapped_total,
        '预测方向': direction,
        '各维度得分': scores,
        '各维度依据': details,
    }


# ═══════════════════════════════════════════════════════════
# 历史回测主逻辑
# ═══════════════════════════════════════════════════════════

def run_historical_backtest(
    stock_codes: list[str] = None,
    start_date: str = '2024-01-01',
    end_date: str = '2026-03-06',
    sample_interval: int = 5,
    max_stocks: int = 50,
    max_samples_per_stock: int = 30,
) -> dict:
    """基于历史K线数据的自动回测

    对每只股票，在 [start_date, end_date] 区间内每隔 sample_interval 个交易日
    取一个采样点，用该点之前120日K线计算评分，然后与之后1日/5日实际涨跌对比。

    Args:
        stock_codes: 指定股票列表；None则随机抽样
        start_date: 回测起始日期
        end_date: 回测截止日期
        sample_interval: 采样间隔（交易日）
        max_stocks: 最大回测股票数
        max_samples_per_stock: 每只股票最大采样点数

    Returns:
        回测结果汇总
    """
    t_start = datetime.now()

    # 获取股票列表
    if not stock_codes:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT stock_code, COUNT(*) cnt
                FROM stock_kline
                WHERE `date` >= %s AND `date` <= %s
                GROUP BY stock_code
                HAVING cnt >= 180
                ORDER BY RAND()
                LIMIT %s
            """, (start_date, end_date, max_stocks))
            stock_codes = [r['stock_code'] for r in cursor.fetchall()]
        finally:
            cursor.close()
            conn.close()

    if not stock_codes:
        return {'状态': '无可用股票数据', '总样本数': 0}

    day_results = []
    week_results = []
    score_buckets = defaultdict(lambda: {'day_ok': 0, 'day_n': 0, 'week_ok': 0, 'week_n': 0})
    processed_stocks = 0

    for code in stock_codes:
        # 获取该股票的全部K线（升序）
        all_kline = get_kline_data(code, start_date='2023-06-01', end_date=end_date)
        if len(all_kline) < 150:
            continue

        # 找到 start_date 在数据中的位置
        start_idx = None
        for i, k in enumerate(all_kline):
            if k['date'] >= start_date:
                start_idx = i
                break
        if start_idx is None or start_idx < 120:
            continue

        # 采样
        sample_count = 0
        i = start_idx
        while i < len(all_kline) - 6 and sample_count < max_samples_per_stock:
            # 用 i 之前120天的数据计算评分
            lookback_start = max(0, i - 120)
            klines_for_score = all_kline[lookback_start:i + 1]

            # 过滤停牌日
            klines_for_score = [k for k in klines_for_score if (k.get('trading_volume') or 0) > 0]
            if len(klines_for_score) < 60:
                i += sample_interval
                continue

            score_result = _score_from_kline(klines_for_score)
            if not score_result:
                i += sample_interval
                continue

            mapped_score = score_result['映射总分']
            pred_direction = score_result['预测方向']
            base_close = all_kline[i]['close_price']
            sample_date = all_kline[i]['date']

            # 评分区间
            if mapped_score >= 70:
                bucket = '70-100'
            elif mapped_score >= 55:
                bucket = '55-69'
            elif mapped_score >= 40:
                bucket = '40-54'
            else:
                bucket = '0-39'

            # ── 次日验证 ──
            if i + 1 < len(all_kline):
                next_day = all_kline[i + 1]
                if (next_day.get('trading_volume') or 0) > 0 and base_close > 0:
                    actual_chg = round((next_day['close_price'] - base_close) / base_close * 100, 2)
                    if actual_chg > 0.3:
                        actual_dir = '上涨'
                    elif actual_chg < -0.3:
                        actual_dir = '下跌'
                    else:
                        actual_dir = '横盘震荡'

                    dir_ok = (pred_direction == actual_dir)
                    # 宽松判定
                    loose_ok = dir_ok
                    if not dir_ok:
                        if '上涨' in pred_direction and actual_chg >= 0:
                            loose_ok = True
                        elif '下跌' in pred_direction and actual_chg <= 0:
                            loose_ok = True

                    day_results.append({
                        'stock_code': code,
                        'sample_date': sample_date,
                        'mapped_score': mapped_score,
                        'pred_direction': pred_direction,
                        'actual_change_pct': actual_chg,
                        'actual_direction': actual_dir,
                        'direction_correct': dir_ok,
                        'direction_loose_correct': loose_ok,
                    })
                    score_buckets[bucket]['day_n'] += 1
                    if dir_ok:
                        score_buckets[bucket]['day_ok'] += 1

            # ── 一周验证 ──
            if i + 5 < len(all_kline):
                week_end_idx = min(i + 5, len(all_kline) - 1)
                week_close = all_kline[week_end_idx]['close_price']
                if base_close > 0 and (all_kline[week_end_idx].get('trading_volume') or 0) > 0:
                    week_chg = round((week_close - base_close) / base_close * 100, 2)
                    if week_chg > 0.3:
                        week_dir = '上涨'
                    elif week_chg < -0.3:
                        week_dir = '下跌'
                    else:
                        week_dir = '横盘震荡'

                    w_ok = (pred_direction == week_dir)
                    w_loose = w_ok
                    if not w_ok:
                        if '上涨' in pred_direction and week_chg >= 0:
                            w_loose = True
                        elif '下跌' in pred_direction and week_chg <= 0:
                            w_loose = True

                    week_results.append({
                        'stock_code': code,
                        'sample_date': sample_date,
                        'mapped_score': mapped_score,
                        'pred_direction': pred_direction,
                        'actual_change_pct': week_chg,
                        'actual_direction': week_dir,
                        'direction_correct': w_ok,
                        'direction_loose_correct': w_loose,
                    })
                    score_buckets[bucket]['week_n'] += 1
                    if w_ok:
                        score_buckets[bucket]['week_ok'] += 1

            sample_count += 1
            i += sample_interval

        processed_stocks += 1

    elapsed = (datetime.now() - t_start).total_seconds()

    return _build_historical_summary(day_results, week_results, score_buckets,
                                     processed_stocks, len(stock_codes), elapsed)


def _build_historical_summary(
    day_results: list[dict],
    week_results: list[dict],
    score_buckets: dict,
    processed_stocks: int,
    total_stocks: int,
    elapsed: float,
) -> dict:
    """构建历史回测汇总"""

    def _rate(ok, n):
        if n == 0:
            return '无数据'
        return f'{ok}/{n}（{round(ok / n * 100, 1)}%）'

    def _agg(results):
        if not results:
            return {'总数': 0, '方向准确率': '无数据', '宽松准确率': '无数据'}
        n = len(results)
        d_ok = sum(1 for r in results if r['direction_correct'])
        l_ok = sum(1 for r in results if r['direction_loose_correct'])
        avg_chg = round(sum(r['actual_change_pct'] for r in results) / n, 2)
        return {
            '总数': n,
            '方向准确率': _rate(d_ok, n),
            '方向准确率_数值': round(d_ok / n * 100, 1),
            '宽松准确率': _rate(l_ok, n),
            '宽松准确率_数值': round(l_ok / n * 100, 1),
            '平均实际涨跌幅(%)': avg_chg,
        }

    # 按评分区间
    bucket_summary = {}
    for b in ['70-100', '55-69', '40-54', '0-39']:
        d = score_buckets.get(b, {})
        bucket_summary[b] = {
            '次日方向准确率': _rate(d.get('day_ok', 0), d.get('day_n', 0)),
            '一周方向准确率': _rate(d.get('week_ok', 0), d.get('week_n', 0)),
            '次日样本数': d.get('day_n', 0),
            '一周样本数': d.get('week_n', 0),
        }

    # 按评分偏离度构建校准数据
    deviation_buckets = defaultdict(lambda: {'ok': 0, 'n': 0})
    for r in day_results:
        dev = abs(r['mapped_score'] - 50)
        if dev >= 30:
            db = '偏离≥30'
        elif dev >= 20:
            db = '偏离20-29'
        elif dev >= 10:
            db = '偏离10-19'
        elif dev >= 5:
            db = '偏离5-9'
        else:
            db = '偏离<5'
        deviation_buckets[db]['n'] += 1
        if r['direction_correct']:
            deviation_buckets[db]['ok'] += 1

    calibration = {}
    default_probs = {'偏离≥30': 70, '偏离20-29': 65, '偏离10-19': 60, '偏离5-9': 55, '偏离<5': 50}
    for bucket_name in ['偏离≥30', '偏离20-29', '偏离10-19', '偏离5-9', '偏离<5']:
        d = deviation_buckets.get(bucket_name, {})
        n = d.get('n', 0)
        ok = d.get('ok', 0)
        actual = round(ok / n * 100, 1) if n > 0 else None
        est = default_probs[bucket_name]
        entry = {'样本数': n, '实际准确率': f'{actual}%' if actual else '无数据', '模型预估': f'{est}%'}
        if actual is not None:
            diff = round(actual - est, 1)
            entry['偏差'] = f'{diff:+.1f}%'
            entry['校准建议'] = f'模型{"偏保守" if diff > 0 else "偏乐观"}' if abs(diff) > 5 else '校准良好'
        calibration[bucket_name] = entry

    # 评分分布
    score_dist = defaultdict(int)
    for r in day_results:
        s = r['mapped_score']
        if s >= 70:
            score_dist['≥70'] += 1
        elif s >= 55:
            score_dist['55-69'] += 1
        elif s >= 40:
            score_dist['40-54'] += 1
        else:
            score_dist['<40'] += 1

    return {
        '回测类型': '历史K线自动回测（4维度简化版）',
        '回测时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        '耗时(秒)': round(elapsed, 1),
        '回测股票数': processed_stocks,
        '候选股票数': total_stocks,
        '次日预测回测': _agg(day_results),
        '一周预测回测': _agg(week_results),
        '按评分区间': bucket_summary,
        '评分分布': dict(score_dist),
        '校准数据': calibration,
        '说明': (
            '本回测使用K线可计算的5个维度（趋势强度20+动能量价20+结构边界15+资金筹码近似15+风险收益比10=80分），'
            '映射到100分制后与实际涨跌对比。资金筹码维度使用成交额/换手率变化近似模拟主力资金行为。'
            '缺少短线情绪和外部环境2个维度（共20分），实际完整评分的准确率应高于此简化版。'
        ),
    }


# ═══════════════════════════════════════════════════════════
# 获取校准参数（供 _compute_prediction_probability 使用）
# ═══════════════════════════════════════════════════════════

def get_historical_calibrated_params(
    stock_codes: list[str] = None,
    start_date: str = '2024-06-01',
    end_date: str = '2026-03-01',
    min_samples: int = 100,
) -> Optional[dict]:
    """基于历史回测结果返回校准后的概率参数

    Returns:
        校准后的基准概率映射 dict，或 None（样本不足）
    """
    result = run_historical_backtest(
        stock_codes=stock_codes,
        start_date=start_date,
        end_date=end_date,
        sample_interval=5,
        max_stocks=30,
        max_samples_per_stock=20,
    )
    calibration = result.get('校准数据', {})
    total_samples = sum(c.get('样本数', 0) for c in calibration.values())
    if total_samples < min_samples:
        logger.info("历史回测样本不足（%d < %d），无法校准", total_samples, min_samples)
        return None

    default_map = {
        '偏离≥30': 0.70, '偏离20-29': 0.65, '偏离10-19': 0.60,
        '偏离5-9': 0.55, '偏离<5': 0.50,
    }
    calibrated = {}
    for bucket, default_prob in default_map.items():
        entry = calibration.get(bucket, {})
        n = entry.get('样本数', 0)
        actual_str = entry.get('实际准确率', '')
        if n > 0 and actual_str and actual_str != '无数据':
            actual = float(actual_str.replace('%', '')) / 100
            weight = min(n / 50, 1.0)
            calibrated[bucket] = round(default_prob * (1 - weight) + actual * weight, 3)
        else:
            calibrated[bucket] = default_prob

    return calibrated

async def run_historical_backtest_with_fund_flow(
    stock_codes: list[str] = None,
    max_stocks: int = 20,
    sample_interval: int = 3,
    max_samples_per_stock: int = 8,
) -> dict:
    """基于历史K线 + 同花顺真实资金流数据的增强回测

    与 run_historical_backtest 的区别：
    - 资金筹码维度使用同花顺真实大单净额数据，而非K线成交额近似
    - 受限于同花顺只返回最近~30个交易日数据，采样窗口较小
    - 需要异步调用同花顺接口，因此本函数为 async

    Args:
        stock_codes: 指定股票列表；None则随机抽样
        max_stocks: 最大回测股票数
        sample_interval: 采样间隔（交易日），建议3-5
        max_samples_per_stock: 每只股票最大采样点数

    Returns:
        回测结果汇总（与 run_historical_backtest 格式一致）
    """
    import asyncio
    from service.jqka10.stock_history_fund_flow_10jqka import get_fund_flow_history
    from common.utils.stock_info_utils import get_stock_info_by_code

    t_start = datetime.now()

    # 获取股票列表（取最近有K线数据的股票）
    if not stock_codes:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT stock_code, COUNT(*) cnt
                FROM stock_kline
                WHERE `date` >= '2025-12-01'
                GROUP BY stock_code
                HAVING cnt >= 30
                ORDER BY RAND()
                LIMIT %s
            """, (max_stocks,))
            stock_codes = [r['stock_code'] for r in cursor.fetchall()]
        finally:
            cursor.close()
            conn.close()

    if not stock_codes:
        return {'状态': '无可用股票数据', '总样本数': 0}

    day_results = []
    week_results = []
    score_buckets = defaultdict(lambda: {'day_ok': 0, 'day_n': 0, 'week_ok': 0, 'week_n': 0})
    processed_stocks = 0
    fund_flow_stats = {'成功': 0, '失败': 0, '无数据': 0}

    for code in stock_codes:
        # 获取 StockInfo
        stock_info = get_stock_info_by_code(code)
        if not stock_info:
            logger.warning("[%s] 无法获取StockInfo，跳过", code)
            continue

        # 获取同花顺历史资金流（~30个交易日，按日期倒序）
        try:
            fund_flow_raw = await get_fund_flow_history(stock_info)
        except Exception as e:
            logger.warning("[%s] 获取同花顺资金流失败: %s", code, e)
            fund_flow_stats['失败'] += 1
            continue

        if not fund_flow_raw or len(fund_flow_raw) < 5:
            fund_flow_stats['无数据'] += 1
            continue

        fund_flow_stats['成功'] += 1

        # 资金流日期范围（倒序，第一条是最新）
        ff_latest_date = fund_flow_raw[0].get('date', '')
        ff_earliest_date = fund_flow_raw[-1].get('date', '')

        # 获取K线数据，覆盖资金流日期范围前120天
        all_kline = get_kline_data(code, start_date='2025-06-01', end_date=ff_latest_date)
        if len(all_kline) < 60:
            continue

        # 建立资金流日期索引（date -> row）
        ff_by_date = {r['date']: r for r in fund_flow_raw}

        # 在资金流覆盖的日期范围内采样
        # 找到K线中对应资金流最早日期的位置
        ff_start_idx = None
        ff_end_idx = None
        for ki, k in enumerate(all_kline):
            if ff_start_idx is None and k['date'] >= ff_earliest_date:
                ff_start_idx = ki
            if k['date'] <= ff_latest_date:
                ff_end_idx = ki

        if ff_start_idx is None or ff_end_idx is None or ff_start_idx < 60:
            continue

        # 采样：在资金流覆盖范围内，每隔 sample_interval 天取一个点
        # 但需要确保采样点之后还有至少6天K线用于验证
        sample_count = 0
        i = ff_start_idx
        while i <= ff_end_idx - 6 and sample_count < max_samples_per_stock:
            sample_date = all_kline[i]['date']

            # 检查该日期是否在资金流数据中
            if sample_date not in ff_by_date:
                i += 1
                continue

            # 用 i 之前120天的K线计算评分
            lookback_start = max(0, i - 120)
            klines_for_score = all_kline[lookback_start:i + 1]
            klines_for_score = [k for k in klines_for_score if (k.get('trading_volume') or 0) > 0]
            if len(klines_for_score) < 60:
                i += sample_interval
                continue

            # 构建该采样点对应的资金流数据（采样日及之前的数据，按日期倒序）
            ff_for_sample = [r for r in fund_flow_raw if r['date'] <= sample_date]

            score_result = _score_from_kline(klines_for_score, fund_flow_data=ff_for_sample)
            if not score_result:
                i += sample_interval
                continue

            mapped_score = score_result['映射总分']
            pred_direction = score_result['预测方向']
            base_close = all_kline[i]['close_price']

            # 评分区间
            if mapped_score >= 70:
                bucket = '70-100'
            elif mapped_score >= 55:
                bucket = '55-69'
            elif mapped_score >= 40:
                bucket = '40-54'
            else:
                bucket = '0-39'

            # ── 次日验证 ──
            if i + 1 < len(all_kline):
                next_day = all_kline[i + 1]
                if (next_day.get('trading_volume') or 0) > 0 and base_close > 0:
                    actual_chg = round((next_day['close_price'] - base_close) / base_close * 100, 2)
                    actual_dir = '上涨' if actual_chg > 0.3 else ('下跌' if actual_chg < -0.3 else '横盘震荡')
                    dir_ok = (pred_direction == actual_dir)
                    loose_ok = dir_ok
                    if not dir_ok:
                        if '上涨' in pred_direction and actual_chg >= 0:
                            loose_ok = True
                        elif '下跌' in pred_direction and actual_chg <= 0:
                            loose_ok = True

                    day_results.append({
                        'stock_code': code,
                        'sample_date': sample_date,
                        'mapped_score': mapped_score,
                        'pred_direction': pred_direction,
                        'actual_change_pct': actual_chg,
                        'actual_direction': actual_dir,
                        'direction_correct': dir_ok,
                        'direction_loose_correct': loose_ok,
                        'fund_flow_days': len(ff_for_sample),
                    })
                    score_buckets[bucket]['day_n'] += 1
                    if dir_ok:
                        score_buckets[bucket]['day_ok'] += 1

            # ── 一周验证 ──
            if i + 5 < len(all_kline):
                week_end_idx = min(i + 5, len(all_kline) - 1)
                week_close = all_kline[week_end_idx]['close_price']
                if base_close > 0 and (all_kline[week_end_idx].get('trading_volume') or 0) > 0:
                    week_chg = round((week_close - base_close) / base_close * 100, 2)
                    week_dir = '上涨' if week_chg > 0.3 else ('下跌' if week_chg < -0.3 else '横盘震荡')
                    w_ok = (pred_direction == week_dir)
                    w_loose = w_ok
                    if not w_ok:
                        if '上涨' in pred_direction and week_chg >= 0:
                            w_loose = True
                        elif '下跌' in pred_direction and week_chg <= 0:
                            w_loose = True

                    week_results.append({
                        'stock_code': code,
                        'sample_date': sample_date,
                        'mapped_score': mapped_score,
                        'pred_direction': pred_direction,
                        'actual_change_pct': week_chg,
                        'actual_direction': week_dir,
                        'direction_correct': w_ok,
                        'direction_loose_correct': w_loose,
                        'fund_flow_days': len(ff_for_sample),
                    })
                    score_buckets[bucket]['week_n'] += 1
                    if w_ok:
                        score_buckets[bucket]['week_ok'] += 1

            sample_count += 1
            i += sample_interval

        processed_stocks += 1

    elapsed = (datetime.now() - t_start).total_seconds()

    summary = _build_historical_summary(day_results, week_results, score_buckets,
                                        processed_stocks, len(stock_codes), elapsed)
    summary['回测类型'] = '历史K线+同花顺真实资金流增强回测'
    summary['资金流获取统计'] = fund_flow_stats
    summary['说明'] = (
        '本回测使用K线可计算的4个维度（趋势强度20+动能量价20+结构边界15+风险收益比10=65分）'
        '+ 同花顺真实主力资金流数据的资金筹码维度（15分），共80分映射到100分制。'
        '受限于同花顺仅返回最近~30个交易日数据，采样窗口较小。'
        '缺少短线情绪和外部环境2个维度（共20分）。'
    )
    return summary



# ═══════════════════════════════════════════════════════════
# CLI 入口
# ═══════════════════════════════════════════════════════════

if __name__ == '__main__':
    import json
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    result = run_historical_backtest(max_stocks=20, max_samples_per_stock=20)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
