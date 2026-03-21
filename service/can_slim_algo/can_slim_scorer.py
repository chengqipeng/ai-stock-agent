#!/usr/bin/env python3
"""
CAN SLIM 量化评分引擎 — 纯规则计算，无 LLM 依赖
==================================================
基于数据库中已有的 K线、财报、资金流数据，对每只股票的 7 个维度
（C/A/N/S/L/I/M）进行量化打分（0-100），并输出加权综合分。

评分标准严格对齐 common/constants/can_slim_final_outputs.py 中的公式。
"""
import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def _sf(v) -> float:
    """安全转 float"""
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _compound_return(pcts: list[float]) -> float:
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return round((r - 1) * 100, 4)


def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0


def _std(lst):
    if len(lst) < 2:
        return 0.0
    m = _mean(lst)
    return (sum((x - m) ** 2 for x in lst) / (len(lst) - 1)) ** 0.5


# ═══════════════════════════════════════════════════════════
# 维度权重（可调参）
# ═══════════════════════════════════════════════════════════

WEIGHTS = {
    'C': 0.20,  # 季度盈利
    'A': 0.15,  # 年度盈利
    'N': 0.10,  # 新产品/新高
    'S': 0.15,  # 供需
    'L': 0.15,  # 领军/落后
    'I': 0.10,  # 机构认同
    'M': 0.15,  # 大盘方向
}


# ═══════════════════════════════════════════════════════════
# C 维度 — 季度盈利 (0-100)
# ═══════════════════════════════════════════════════════════

def score_C(finance_records: list[dict]) -> dict:
    """
    基于财报 JSON 数据计算 C 维度得分。

    finance_records: 按报告日期降序排列的财报记录列表，
                     每条记录的 data_json 已解析为 dict。
    """
    if not finance_records or len(finance_records) < 2:
        return {'score': 0, 'detail': '财报数据不足'}

    # 提取季度数据（取最近 4 个季度）
    quarterly = []
    for rec in finance_records[:8]:
        data = rec if isinstance(rec, dict) else {}
        rev_yoy = _sf(data.get('营业总收入同比增长(%)') or data.get('单季营业收入同比增长(%)'))
        profit_yoy = _sf(data.get('单季度扣非净利润同比增长(%)') or data.get('扣非净利润同比增长(%)'))
        eps = _sf(data.get('基本每股收益(元)'))
        report_date = data.get('报告日期', '')
        quarterly.append({
            'report_date': report_date,
            'rev_yoy': rev_yoy,
            'profit_yoy': profit_yoy,
            'eps': eps,
        })

    latest = quarterly[0]

    # ── 指标1: 单季营收同比增长 (满分30) ──
    rev = latest['rev_yoy']
    if rev < 15:
        s1 = 0
    elif rev < 25:
        s1 = 10
    elif rev <= 50:
        s1 = 10 + (rev - 25) / 25 * 15
    else:
        s1 = 25
    # 持续性加分
    if len(quarterly) >= 2 and quarterly[0]['rev_yoy'] > 30 and quarterly[1]['rev_yoy'] > 30:
        s1 = min(30, s1 + 5)

    # ── 指标2: 业绩爆发力与趋势 (满分40) ──
    pf = latest['profit_yoy']
    # 绝对增速分 (20)
    if pf > 50:
        s2a = 20
    elif pf > 30:
        s2a = 10 + (pf - 30) / 20 * 10
    elif pf >= 15:
        s2a = 10
    else:
        s2a = 0

    # 趋势动能分 (20)
    if len(quarterly) >= 3:
        p0, p1, p2 = quarterly[0]['profit_yoy'], quarterly[1]['profit_yoy'], quarterly[2]['profit_yoy']
        if p0 > p1 > p2 and p0 > 30:
            s2b = 20  # 逐季加速
        elif min(p0, p1) > 40:
            s2b = 15  # 高位维持
        elif p0 > 25:
            s2b = 10  # 放缓但仍强劲
        else:
            s2b = 0
    else:
        s2b = 10 if pf > 25 else 0
    s2 = s2a + s2b

    # ── 指标3: 盈利质量 (满分30) ──
    # 利润扩张分 (15)
    if latest['profit_yoy'] >= latest['rev_yoy'] + 10:
        s3a = 15
    elif latest['profit_yoy'] >= latest['rev_yoy']:
        s3a = 10
    else:
        s3a = 0

    # EPS 新高分 (15)
    eps_list = [q['eps'] for q in quarterly[:4] if q['eps'] != 0]
    if eps_list and latest['eps'] >= max(eps_list):
        s3b = 15
    elif len(eps_list) >= 2 and latest['eps'] >= sorted(eps_list)[-2]:
        s3b = 10
    else:
        s3b = 0
    s3 = s3a + s3b

    total = round(s1 + s2 + s3)
    return {
        'score': min(100, max(0, total)),
        'detail': f'营收={rev:.1f}%({s1:.0f}), 利润={pf:.1f}%({s2:.0f}), 质量({s3:.0f})',
        'rev_yoy': rev,
        'profit_yoy': pf,
    }


# ═══════════════════════════════════════════════════════════
# A 维度 — 年度盈利 (0-100)
# ═══════════════════════════════════════════════════════════

def score_A(finance_records: list[dict]) -> dict:
    """
    基于年报数据计算 A 维度得分。
    需要至少 3 年的年报数据。
    """
    # 筛选年报（报告日期以 12-31 结尾）
    annual = []
    for rec in finance_records:
        data = rec if isinstance(rec, dict) else {}
        rd = data.get('报告日期', '')
        if '12-31' not in rd and '12-30' not in rd:
            continue
        eps_kc = _sf(data.get('扣非每股收益(元)') or data.get('基本每股收益(元)'))
        roe = _sf(data.get('净资产收益率(扣非/加权)(%)') or data.get('净资产收益率(%)'))
        cashflow = _sf(data.get('每股经营现金流(元)'))
        annual.append({'rd': rd, 'eps_kc': eps_kc, 'roe': roe, 'cashflow': cashflow})

    if len(annual) < 2:
        return {'score': 0, 'detail': '年报数据不足'}

    annual.sort(key=lambda x: x['rd'], reverse=True)
    annual = annual[:5]

    # ── 指标1: CAGR (满分40) ──
    eps_vals = [a['eps_kc'] for a in annual if a['eps_kc'] > 0]
    if len(eps_vals) >= 2:
        n_years = len(eps_vals) - 1
        cagr = ((eps_vals[0] / eps_vals[-1]) ** (1 / n_years) - 1) * 100 if eps_vals[-1] > 0 else 0
    else:
        cagr = 0

    if cagr < 15:
        s1 = 0
    elif cagr < 25:
        s1 = 10 + (cagr - 15) / 10 * 10
    elif cagr <= 50:
        s1 = 20 + (cagr - 25) / 25 * 20
    else:
        s1 = 40

    # ── 指标2: ROE (满分30) ──
    latest_roe = annual[0]['roe']
    if latest_roe < 10:
        s2 = 0
    elif latest_roe < 17:
        s2 = 10 + (latest_roe - 10) / 7 * 10
    elif latest_roe <= 25:
        s2 = 20 + (latest_roe - 17) / 8 * 10
    else:
        s2 = 30

    # ── 指标3: 现金流验证 (满分30) ──
    latest_cf = annual[0]['cashflow']
    latest_eps = annual[0]['eps_kc']
    cf_ratio = latest_cf / latest_eps if latest_eps > 0 else 0

    if cf_ratio < 0.5:
        s3 = max(0, cf_ratio * 20)
    elif cf_ratio < 0.8:
        s3 = 10 + (cf_ratio - 0.5) / 0.3 * 10
    elif cf_ratio <= 1.0:
        s3 = 20 + (cf_ratio - 0.8) / 0.2 * 10
    else:
        s3 = 30

    total = round(s1 + s2 + s3)
    return {
        'score': min(100, max(0, total)),
        'detail': f'CAGR={cagr:.1f}%({s1:.0f}), ROE={latest_roe:.1f}%({s2:.0f}), CF比={cf_ratio:.2f}({s3:.0f})',
        'cagr': cagr,
        'roe': latest_roe,
    }


# ═══════════════════════════════════════════════════════════
# 杯柄形态检测 (Cup with Handle)
# ═══════════════════════════════════════════════════════════
#
# 欧奈尔定义的杯柄形态要素：
#   杯身(Cup):
#     - 从前高点下跌形成左侧杯壁，跌幅通常 12%-35%（牛市），熊市可达 40%-50%
#     - 杯底呈圆弧形（U 形），非 V 形急跌急涨
#     - 杯身持续 7-65 周（约 35-325 个交易日）
#     - 右侧杯壁回升至接近前高点
#   杯柄(Handle):
#     - 出现在杯身右侧上方 1/3 区域
#     - 柄部回撤幅度通常 8%-12%，不超过杯身深度的 1/3-1/2
#     - 柄部持续 1-5 周（约 5-25 个交易日）
#     - 柄部成交量萎缩（洗盘特征）
#   突破(Pivot):
#     - 股价突破柄部高点（枢轴点）
#     - 突破日成交量放大至少 40%-50% 以上
#
# ═══════════════════════════════════════════════════════════

def detect_cup_with_handle(klines: list[dict],
                           min_cup_days: int = 35,
                           max_cup_days: int = 250,
                           min_cup_depth_pct: float = 15.0,
                           max_cup_depth_pct: float = 45.0,
                           max_handle_depth_ratio: float = 0.40,
                           min_handle_days: int = 5,
                           max_handle_days: int = 25) -> dict:
    """
    从 K 线数据中检测杯柄形态。

    Args:
        klines: 按日期升序排列的 K 线（建议 >= 150 天）
        min_cup_days: 杯身最短天数
        max_cup_days: 杯身最长天数
        min_cup_depth_pct: 杯身最小跌幅(%)
        max_cup_depth_pct: 杯身最大跌幅(%)
        max_handle_depth_ratio: 柄部回撤 / 杯身深度 的最大比值
        min_handle_days: 柄部最短天数
        max_handle_days: 柄部最长天数

    Returns:
        {
            'detected': bool,
            'score': 0-100,        # 形态质量评分
            'cup_depth': float,    # 杯身跌幅(%)
            'cup_days': int,       # 杯身天数
            'handle_depth': float, # 柄部回撤(%)
            'handle_days': int,    # 柄部天数
            'pivot_price': float,  # 枢轴点（突破价）
            'breakout': bool,      # 是否已突破
            'volume_confirm': bool,# 成交量确认
            'detail': str,
        }
    """
    result = {
        'detected': False, 'score': 0, 'cup_depth': 0, 'cup_days': 0,
        'handle_depth': 0, 'handle_days': 0, 'pivot_price': 0,
        'breakout': False, 'volume_confirm': False, 'detail': '',
    }

    if not klines or len(klines) < min_cup_days + min_handle_days + 10:
        result['detail'] = '数据不足'
        return result

    closes = [_sf(k.get('close_price') or k.get('close', 0)) for k in klines]
    highs = [_sf(k.get('high_price') or k.get('high', 0)) for k in klines]
    lows = [_sf(k.get('low_price') or k.get('low', 0)) for k in klines]
    volumes = [_sf(k.get('trading_volume') or k.get('volume', 0)) for k in klines]

    if not all(c > 0 for c in closes[-min_cup_days:]):
        result['detail'] = '收盘价数据异常'
        return result

    n = len(closes)
    best_score = 0
    best_pattern = None

    # 滑动窗口搜索：尝试不同的左侧高点位置
    # 从最近往前搜索，找到最佳的杯柄形态
    search_start = max(0, n - max_cup_days - max_handle_days - 10)
    search_end = n - min_cup_days - min_handle_days

    for left_peak_idx in range(search_end, search_start, -1):
        left_peak = highs[left_peak_idx]
        if left_peak <= 0:
            continue

        # ── 寻找杯底 ──
        cup_search_end = min(left_peak_idx + max_cup_days, n - min_handle_days)
        cup_search_start = left_peak_idx + min_cup_days // 3  # 杯底至少在左峰后 1/3 处

        if cup_search_start >= cup_search_end:
            continue

        # 找到区间内的最低点作为杯底
        cup_bottom_idx = cup_search_start
        cup_bottom_val = lows[cup_search_start]
        for i in range(cup_search_start, cup_search_end):
            if lows[i] < cup_bottom_val and lows[i] > 0:
                cup_bottom_val = lows[i]
                cup_bottom_idx = i

        # 检查杯身深度
        cup_depth_pct = (left_peak - cup_bottom_val) / left_peak * 100
        if cup_depth_pct < min_cup_depth_pct or cup_depth_pct > max_cup_depth_pct:
            continue

        # ── 寻找右侧杯壁高点（杯口） ──
        right_search_start = cup_bottom_idx + (cup_bottom_idx - left_peak_idx) // 3
        right_search_end = min(cup_bottom_idx + max_cup_days - (cup_bottom_idx - left_peak_idx), n)

        if right_search_start >= right_search_end or right_search_start >= n:
            continue

        right_peak_idx = right_search_start
        right_peak_val = highs[right_search_start]
        for i in range(right_search_start, right_search_end):
            if highs[i] > right_peak_val:
                right_peak_val = highs[i]
                right_peak_idx = i

        # 右侧杯壁必须回升到左侧高点的至少 85%
        if right_peak_val < left_peak * 0.85:
            continue

        cup_days = right_peak_idx - left_peak_idx
        if cup_days < min_cup_days or cup_days > max_cup_days:
            continue

        # ── 检查杯底形状：U 形 vs V 形 ──
        # U 形特征：杯底附近（中间 1/3）的价格波动较小，且杯底有一定持续时间
        cup_mid_start = left_peak_idx + cup_days // 3
        cup_mid_end = left_peak_idx + cup_days * 2 // 3
        is_u_shape = False
        if cup_mid_end > cup_mid_start + 4:
            mid_closes = closes[cup_mid_start:cup_mid_end]
            mid_range = (max(mid_closes) - min(mid_closes)) / min(mid_closes) * 100 if min(mid_closes) > 0 else 999
            # U 形要求中间段波动 < 杯深的 40%（更严格）
            # 同时要求杯底区域（低于杯深 80% 位置）至少持续 5 天
            bottom_threshold = cup_bottom_val + (left_peak - cup_bottom_val) * 0.2
            bottom_days = sum(1 for i in range(cup_mid_start, cup_mid_end) if closes[i] <= bottom_threshold)
            is_u_shape = mid_range < cup_depth_pct * 0.40 and bottom_days >= 5

        # ── 寻找柄部 ──
        handle_start_idx = right_peak_idx
        handle_search_end = min(right_peak_idx + max_handle_days, n)

        if handle_search_end <= handle_start_idx + min_handle_days:
            # 柄部空间不足，但如果杯身形态好，仍可作为"无柄杯"
            handle_days = 0
            handle_depth_pct = 0
            handle_low_val = right_peak_val
            has_handle = False
        else:
            # 找柄部最低点
            handle_lows = lows[handle_start_idx:handle_search_end]
            handle_low_val = min(handle_lows) if handle_lows else right_peak_val
            handle_low_idx = handle_start_idx + handle_lows.index(handle_low_val) if handle_lows else handle_start_idx

            handle_depth_pct = (right_peak_val - handle_low_val) / right_peak_val * 100 if right_peak_val > 0 else 0
            handle_days = min(handle_search_end, n) - handle_start_idx

            # 柄部回撤不能超过杯身深度的一定比例
            cup_depth_abs = left_peak - cup_bottom_val
            handle_depth_abs = right_peak_val - handle_low_val
            handle_ratio = handle_depth_abs / cup_depth_abs if cup_depth_abs > 0 else 999

            has_handle = (min_handle_days <= handle_days <= max_handle_days
                          and handle_ratio <= max_handle_depth_ratio
                          and handle_depth_pct <= 15)

            # 柄部必须在杯身上方 1/2 区域
            cup_midpoint = cup_bottom_val + cup_depth_abs * 0.5
            if handle_low_val < cup_midpoint:
                has_handle = False

        # ── 柄部成交量萎缩检查 ──
        vol_shrink = False
        if has_handle and handle_days >= 3:
            handle_avg_vol = _mean(volumes[handle_start_idx:handle_start_idx + handle_days])
            # 对比杯身右侧上升段的成交量
            rise_start = cup_bottom_idx
            rise_end = right_peak_idx
            if rise_end > rise_start + 3:
                rise_avg_vol = _mean(volumes[rise_start:rise_end])
                vol_shrink = handle_avg_vol < rise_avg_vol * 0.8  # 柄部量能萎缩 20%+

        # ── 枢轴点（Pivot Point）──
        pivot_price = right_peak_val * 1.001  # 突破右侧高点即为买入信号

        # ── 检查是否已突破 ──
        breakout = False
        volume_confirm = False
        latest_close = closes[-1]
        if latest_close > pivot_price:
            breakout = True
            # 检查突破日成交量
            for i in range(max(right_peak_idx, n - 10), n):
                if closes[i] > pivot_price and i > 0:
                    avg_vol_20 = _mean(volumes[max(0, i - 20):i]) if i >= 20 else _mean(volumes[:i])
                    if avg_vol_20 > 0 and volumes[i] > avg_vol_20 * 1.4:
                        volume_confirm = True
                    break

        # ── 综合评分 ──
        pattern_score = 0

        # 杯身形态 (0-30)：U 形是核心要求
        if is_u_shape:
            pattern_score += 28  # U 形杯底（核心加分）
        else:
            pattern_score += 5   # V 形杯底（大幅扣分，欧奈尔明确要求 U 形）

        # 杯身深度合理性 (0-15)
        if 18 <= cup_depth_pct <= 33:
            pattern_score += 15  # 理想深度
        elif 15 <= cup_depth_pct <= 40:
            pattern_score += 8
        else:
            pattern_score += 3

        # 杯口对称性：右侧高点接近左侧高点 (0-15)
        symmetry = right_peak_val / left_peak if left_peak > 0 else 0
        if symmetry >= 0.95:
            pattern_score += 15
        elif symmetry >= 0.90:
            pattern_score += 8
        elif symmetry >= 0.85:
            pattern_score += 3

        # 柄部质量 (0-25)：柄部是关键确认信号
        if has_handle:
            pattern_score += 15
            if vol_shrink:
                pattern_score += 10  # 量缩洗盘是经典信号
        else:
            pattern_score += 0  # 无柄不给分

        # 突破确认 (0-15)
        if breakout:
            pattern_score += 8
            if volume_confirm:
                pattern_score += 7  # 放量突破

        if pattern_score > best_score:
            best_score = pattern_score
            best_pattern = {
                'detected': pattern_score >= 50 and (has_handle or (is_u_shape and breakout)),
                'score': min(100, pattern_score),
                'cup_depth': round(cup_depth_pct, 1),
                'cup_days': cup_days,
                'handle_depth': round(handle_depth_pct, 1),
                'handle_days': handle_days if has_handle else 0,
                'pivot_price': round(pivot_price, 2),
                'breakout': breakout,
                'volume_confirm': volume_confirm,
                'is_u_shape': is_u_shape,
                'has_handle': has_handle,
                'vol_shrink': vol_shrink,
                'symmetry': round(symmetry, 3),
                'left_peak': round(left_peak, 2),
                'right_peak': round(right_peak_val, 2),
                'cup_bottom': round(cup_bottom_val, 2),
                'detail': (f'杯深={cup_depth_pct:.1f}%, 杯长={cup_days}天, '
                           f'{"U形" if is_u_shape else "V形"}, '
                           f'{"有柄" if has_handle else "无柄"}'
                           f'{",量缩" if vol_shrink else ""}'
                           f'{",已突破" if breakout else ""}'
                           f'{",放量确认" if volume_confirm else ""}'),
            }

    if best_pattern:
        return best_pattern

    result['detail'] = '未检测到杯柄形态'
    return result


# ═══════════════════════════════════════════════════════════
# N 维度 — 新产品/新高/杯柄形态 (0-100)
# ═══════════════════════════════════════════════════════════

def score_N(klines: list[dict], finance_records: list[dict] = None) -> dict:
    """
    基于 K 线数据评估 N 维度（新高/突破/杯柄形态）。
    klines: 按日期升序排列的 K 线数据（至少 120 天）。
    """
    if not klines or len(klines) < 60:
        return {'score': 0, 'detail': 'K线数据不足', 'cup_handle': None}

    closes = [_sf(k.get('close_price') or k.get('close', 0)) for k in klines]
    closes = [c for c in closes if c > 0]
    if len(closes) < 60:
        return {'score': 0, 'detail': '有效收盘价不足', 'cup_handle': None}

    latest_close = closes[-1]
    high_120 = max(closes[-120:]) if len(closes) >= 120 else max(closes)
    high_250 = max(closes[-250:]) if len(closes) >= 250 else max(closes)

    # ── 52周新高距离 (满分25) ──
    pct_from_high = (latest_close - high_250) / high_250 * 100 if high_250 > 0 else -100
    if pct_from_high >= -2:
        s1 = 25  # 接近或创新高
    elif pct_from_high >= -10:
        s1 = 15 + (pct_from_high + 10) / 8 * 10
    elif pct_from_high >= -25:
        s1 = 5 + (pct_from_high + 25) / 15 * 10
    else:
        s1 = 0

    # ── 突破形态 (满分20) ──
    if len(closes) >= 80:
        prev_high = max(closes[-80:-20])
        recent_high = max(closes[-20:])
        if recent_high > prev_high * 1.02:
            s2 = 20  # 有效突破
        elif recent_high > prev_high * 0.98:
            s2 = 12  # 接近突破
        else:
            s2 = max(0, 8 * (recent_high / prev_high - 0.85) / 0.13) if prev_high > 0 else 0
    else:
        s2 = 10

    # ── 上升趋势强度 (满分20) ──
    ma20 = _mean(closes[-20:])
    ma60 = _mean(closes[-60:])
    ma120 = _mean(closes[-120:]) if len(closes) >= 120 else _mean(closes)

    trend_score = 0
    if latest_close > ma20:
        trend_score += 7
    if ma20 > ma60:
        trend_score += 7
    if ma60 > ma120:
        trend_score += 6
    s3 = trend_score

    # ── 杯柄形态 (满分35) ──
    cup_handle = detect_cup_with_handle(klines)
    if cup_handle['detected']:
        ch_raw = cup_handle['score']  # 0-100 的形态质量分
        # 映射到 0-35 分
        s4 = round(ch_raw * 0.35)

        # 额外加分：已突破 + 放量确认 → 满分
        if cup_handle['breakout'] and cup_handle['volume_confirm']:
            s4 = 35
        elif cup_handle['breakout']:
            s4 = max(s4, 28)
    else:
        s4 = 0

    total = round(s1 + s2 + s3 + s4)
    ch_label = ''
    if cup_handle['detected']:
        ch_label = f', 杯柄={cup_handle["score"]}分({s4:.0f})'
        if cup_handle['breakout']:
            ch_label += '[已突破]'

    return {
        'score': min(100, max(0, total)),
        'detail': f'距高点={pct_from_high:.1f}%({s1:.0f}), 突破({s2:.0f}), 趋势({s3:.0f}){ch_label}',
        'pct_from_high': pct_from_high,
        'cup_handle': cup_handle if cup_handle['detected'] else None,
    }


# ═══════════════════════════════════════════════════════════
# S 维度 — 供需分析 (0-100)
# ═══════════════════════════════════════════════════════════

def score_S(klines: list[dict], fund_flow: list[dict] = None) -> dict:
    """
    基于成交量和资金流评估供需关系。
    klines: 按日期升序排列。
    fund_flow: 按日期降序排列的资金流数据。
    """
    if not klines or len(klines) < 20:
        return {'score': 0, 'detail': 'K线数据不足'}

    # ── 量价配合 (满分35) ──
    recent = klines[-20:]
    vols = [_sf(k.get('trading_volume') or k.get('volume', 0)) for k in recent]
    pcts = [_sf(k.get('change_percent', 0)) for k in recent]

    # 上涨日放量、下跌日缩量 → 健康
    up_vols = [v for v, p in zip(vols, pcts) if p > 0 and v > 0]
    down_vols = [v for v, p in zip(vols, pcts) if p < 0 and v > 0]
    avg_up_vol = _mean(up_vols) if up_vols else 0
    avg_down_vol = _mean(down_vols) if down_vols else 1

    if avg_down_vol > 0 and avg_up_vol > 0:
        vol_ratio = avg_up_vol / avg_down_vol
        if vol_ratio >= 1.5:
            s1 = 35
        elif vol_ratio >= 1.2:
            s1 = 25
        elif vol_ratio >= 1.0:
            s1 = 15
        else:
            s1 = 5
    else:
        s1 = 15

    # ── 量能趋势 (满分30) ──
    if len(klines) >= 40:
        vol_recent = _mean([_sf(k.get('trading_volume') or k.get('volume', 0)) for k in klines[-10:]])
        vol_prev = _mean([_sf(k.get('trading_volume') or k.get('volume', 0)) for k in klines[-40:-20]])
        if vol_prev > 0:
            vol_trend = vol_recent / vol_prev
            if vol_trend >= 1.3:
                s2 = 30  # 放量
            elif vol_trend >= 1.0:
                s2 = 20
            elif vol_trend >= 0.7:
                s2 = 10
            else:
                s2 = 0
        else:
            s2 = 15
    else:
        s2 = 15

    # ── 资金流向 (满分35) ──
    s3 = 15  # 默认中性
    if fund_flow and len(fund_flow) >= 5:
        recent_ff = fund_flow[:10]
        big_nets = [_sf(f.get('big_net', 0)) for f in recent_ff]
        net_flows = [_sf(f.get('net_flow', 0)) for f in recent_ff]
        avg_big = _mean(big_nets)
        avg_net = _mean(net_flows)

        if avg_big > 500 and avg_net > 0:
            s3 = 35  # 主力大幅流入
        elif avg_big > 0:
            s3 = 25
        elif avg_big > -200:
            s3 = 15
        elif avg_big > -500:
            s3 = 8
        else:
            s3 = 0

    total = round(s1 + s2 + s3)
    return {
        'score': min(100, max(0, total)),
        'detail': f'量价({s1:.0f}), 量能({s2:.0f}), 资金({s3:.0f})',
    }


# ═══════════════════════════════════════════════════════════
# L 维度 — 领军/落后 (0-100)
# ═══════════════════════════════════════════════════════════

def score_L(klines: list[dict], market_klines: list[dict]) -> dict:
    """
    计算相对强度（RS），评估是领军股还是落后股。
    klines, market_klines: 按日期升序排列。
    """
    if not klines or not market_klines or len(klines) < 60 or len(market_klines) < 60:
        return {'score': 0, 'detail': '数据不足'}

    def _period_return(data, n):
        if len(data) < n:
            return 0
        start = _sf(data[-n].get('close_price') or data[-n].get('close', 0))
        end = _sf(data[-1].get('close_price') or data[-1].get('close', 0))
        return (end - start) / start * 100 if start > 0 else 0

    # 多周期相对强度
    periods = [20, 60, 120]
    rs_scores = []
    for p in periods:
        if len(klines) >= p and len(market_klines) >= p:
            stock_ret = _period_return(klines, p)
            mkt_ret = _period_return(market_klines, p)
            excess = stock_ret - mkt_ret
            rs_scores.append(excess)

    if not rs_scores:
        return {'score': 0, 'detail': '无法计算RS'}

    # 加权 RS（近期权重更高）
    weights = [0.5, 0.3, 0.2][:len(rs_scores)]
    w_sum = sum(weights)
    weighted_rs = sum(r * w for r, w in zip(rs_scores, weights)) / w_sum

    # ── RS 评分 (满分60) ──
    if weighted_rs >= 20:
        s1 = 60
    elif weighted_rs >= 10:
        s1 = 40 + (weighted_rs - 10) / 10 * 20
    elif weighted_rs >= 0:
        s1 = 20 + weighted_rs / 10 * 20
    elif weighted_rs >= -10:
        s1 = 10 + (weighted_rs + 10) / 10 * 10
    else:
        s1 = 0

    # ── 趋势一致性 (满分40) ──
    # 检查多周期是否都跑赢大盘
    positive_count = sum(1 for r in rs_scores if r > 0)
    s2 = positive_count / len(rs_scores) * 40

    total = round(s1 + s2)
    return {
        'score': min(100, max(0, total)),
        'detail': f'加权RS={weighted_rs:.1f}%({s1:.0f}), 一致性({s2:.0f})',
        'weighted_rs': weighted_rs,
    }


# ═══════════════════════════════════════════════════════════
# I 维度 — 机构认同 (0-100)
# ═══════════════════════════════════════════════════════════

def score_I(fund_flow: list[dict], klines: list[dict] = None) -> dict:
    """
    基于资金流数据评估机构认同度。
    fund_flow: 按日期降序排列。
    """
    if not fund_flow or len(fund_flow) < 10:
        return {'score': 30, 'detail': '资金流数据不足，给予中性分'}

    # ── 主力资金趋势 (满分40) ──
    recent_10 = fund_flow[:10]
    prev_10 = fund_flow[10:20] if len(fund_flow) >= 20 else fund_flow[10:]

    recent_big = _mean([_sf(f.get('big_net', 0)) for f in recent_10])
    prev_big = _mean([_sf(f.get('big_net', 0)) for f in prev_10]) if prev_10 else 0

    if recent_big > 500:
        s1 = 40
    elif recent_big > 200:
        s1 = 30
    elif recent_big > 0:
        s1 = 20
    elif recent_big > -200:
        s1 = 10
    else:
        s1 = 0

    # 趋势改善加分
    if prev_big < 0 and recent_big > 0:
        s1 = min(40, s1 + 5)

    # ── 大单占比 (满分30) ──
    big_pcts = [_sf(f.get('big_net_pct', 0)) for f in recent_10]
    avg_pct = _mean(big_pcts)
    if avg_pct > 5:
        s2 = 30
    elif avg_pct > 2:
        s2 = 20
    elif avg_pct > 0:
        s2 = 15
    elif avg_pct > -3:
        s2 = 8
    else:
        s2 = 0

    # ── 5日主力净额趋势 (满分30) ──
    main_5d = [_sf(f.get('main_net_5day', 0)) for f in recent_10]
    if len(main_5d) >= 5:
        recent_avg = _mean(main_5d[:5])
        prev_avg = _mean(main_5d[5:]) if len(main_5d) >= 10 else 0
        if recent_avg > 500:
            s3 = 30
        elif recent_avg > 0:
            s3 = 20
        elif recent_avg > -300:
            s3 = 10
        else:
            s3 = 0
    else:
        s3 = 15

    total = round(s1 + s2 + s3)
    return {
        'score': min(100, max(0, total)),
        'detail': f'主力={recent_big:.0f}万({s1:.0f}), 大单占比={avg_pct:.1f}%({s2:.0f}), 5日({s3:.0f})',
    }


# ═══════════════════════════════════════════════════════════
# M 维度 — 大盘方向 (0-100)
# ═══════════════════════════════════════════════════════════

def score_M(market_klines: list[dict]) -> dict:
    """
    评估大盘环境。
    market_klines: 指数 K 线，按日期升序排列。
    """
    if not market_klines or len(market_klines) < 20:
        return {'score': 50, 'detail': '大盘数据不足'}

    pcts = [_sf(k.get('change_percent', 0)) for k in market_klines]

    # ── 短期动量 (满分30) ──
    d5 = _compound_return(pcts[-5:])
    d10 = _compound_return(pcts[-10:])
    if d5 > 2:
        s1 = 30
    elif d5 > 0:
        s1 = 20
    elif d5 > -2:
        s1 = 10
    else:
        s1 = 0

    # ── 中期趋势 (满分40) ──
    d20 = _compound_return(pcts[-20:])
    d60 = _compound_return(pcts[-60:]) if len(pcts) >= 60 else d20

    closes = [_sf(k.get('close_price') or k.get('close', 0)) for k in market_klines]
    closes = [c for c in closes if c > 0]
    ma20 = _mean(closes[-20:]) if len(closes) >= 20 else 0
    ma60 = _mean(closes[-60:]) if len(closes) >= 60 else 0
    latest = closes[-1] if closes else 0

    trend_pts = 0
    if latest > ma20:
        trend_pts += 15
    if ma20 > ma60:
        trend_pts += 15
    if d20 > 0:
        trend_pts += 10
    s2 = min(40, trend_pts)

    # ── 出货日计数 (满分30) ──
    # 出货日: 指数下跌且成交量放大
    dist_days = 0
    for i in range(-25, 0):
        if abs(i) > len(market_klines):
            continue
        k = market_klines[i]
        chg = _sf(k.get('change_percent', 0))
        vol = _sf(k.get('trading_volume') or k.get('volume', 0))
        if i > -len(market_klines) + 1:
            prev_vol = _sf(market_klines[i - 1].get('trading_volume') or market_klines[i - 1].get('volume', 0))
        else:
            prev_vol = vol
        if chg < -0.2 and vol > prev_vol * 1.05:
            dist_days += 1

    if dist_days <= 2:
        s3 = 30  # 健康
    elif dist_days <= 4:
        s3 = 20
    elif dist_days <= 6:
        s3 = 10
    else:
        s3 = 0  # 出货日过多

    total = round(s1 + s2 + s3)
    return {
        'score': min(100, max(0, total)),
        'detail': f'd5={d5:.1f}%({s1:.0f}), 趋势({s2:.0f}), 出货日={dist_days}({s3:.0f})',
        'd5': d5,
        'd20': d20,
    }


# ═══════════════════════════════════════════════════════════
# 综合评分
# ═══════════════════════════════════════════════════════════

def compute_canslim_composite(dim_scores: dict) -> dict:
    """
    计算 CAN SLIM 加权综合分。

    dim_scores: {'C': {'score': 80, ...}, 'A': {...}, ...}
    返回: {'composite': 75.2, 'grade': '强烈买入', 'dim_scores': {...}, 'cup_handle': {...}}
    """
    weighted_sum = 0
    weight_total = 0
    scores_map = {}

    for dim, weight in WEIGHTS.items():
        ds = dim_scores.get(dim, {})
        s = ds.get('score', 0)
        scores_map[dim] = s
        weighted_sum += s * weight
        weight_total += weight

    composite = round(weighted_sum / weight_total, 1) if weight_total > 0 else 0

    # M 维度一票否决
    m_score = dim_scores.get('M', {}).get('score', 50)
    if m_score < 30:
        composite = min(composite, 40)  # 大盘极差时压制综合分

    # 杯柄形态加成
    cup_handle = dim_scores.get('N', {}).get('cup_handle')
    cup_handle_bonus = 0
    if cup_handle and cup_handle.get('detected'):
        ch_score = cup_handle['score']
        # 基础加成：形态质量越高加成越大（最多 +8 分）
        cup_handle_bonus = round(ch_score * 0.08)
        # 突破确认额外加成
        if cup_handle.get('breakout'):
            cup_handle_bonus += 3
            if cup_handle.get('volume_confirm'):
                cup_handle_bonus += 2  # 放量突破再加
        # 大盘不好时削减加成
        if m_score < 40:
            cup_handle_bonus = cup_handle_bonus // 2
        composite = min(100, composite + cup_handle_bonus)

    # 评级
    if composite >= 75:
        grade = '强烈买入'
    elif composite >= 60:
        grade = '列入观察'
    elif composite >= 45:
        grade = '保持观望'
    else:
        grade = '坚决规避'

    result = {
        'composite': composite,
        'grade': grade,
        'dim_scores': scores_map,
        'dim_details': {d: dim_scores.get(d, {}).get('detail', '') for d in WEIGHTS},
    }

    if cup_handle and cup_handle.get('detected'):
        result['cup_handle'] = {
            'detected': True,
            'pattern_score': cup_handle['score'],
            'bonus': cup_handle_bonus,
            'cup_depth': cup_handle.get('cup_depth'),
            'cup_days': cup_handle.get('cup_days'),
            'handle_depth': cup_handle.get('handle_depth'),
            'handle_days': cup_handle.get('handle_days'),
            'pivot_price': cup_handle.get('pivot_price'),
            'breakout': cup_handle.get('breakout', False),
            'volume_confirm': cup_handle.get('volume_confirm', False),
            'detail': cup_handle.get('detail', ''),
        }

    return result


# ═══════════════════════════════════════════════════════════
# 单只股票完整评分（从原始数据）
# ═══════════════════════════════════════════════════════════

def score_stock(stock_code: str,
                klines: list[dict],
                market_klines: list[dict],
                finance_records: list[dict],
                fund_flow: list[dict]) -> dict:
    """
    对单只股票进行完整 CAN SLIM 评分。

    所有数据均为已从 DB 加载的原始列表。
    klines/market_klines: 按日期升序。
    finance_records: 按报告日期降序。
    fund_flow: 按日期降序。
    """
    c_result = score_C(finance_records)
    a_result = score_A(finance_records)
    n_result = score_N(klines, finance_records)
    s_result = score_S(klines, fund_flow)
    l_result = score_L(klines, market_klines)
    i_result = score_I(fund_flow, klines)
    m_result = score_M(market_klines)

    dim_scores = {
        'C': c_result,
        'A': a_result,
        'N': n_result,
        'S': s_result,
        'L': l_result,
        'I': i_result,
        'M': m_result,
    }

    result = compute_canslim_composite(dim_scores)
    result['stock_code'] = stock_code
    return result
