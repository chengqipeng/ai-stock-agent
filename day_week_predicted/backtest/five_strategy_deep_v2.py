#!/usr/bin/env python3
"""
5大选股策略 深度混合维度分析 v2
================================
在v1基础上进行深度调优：
  1. 放宽各策略参数，增加信号覆盖
  2. 引入试盘线VP量价过滤维度交叉增强
  3. 策略OR联合 + 技术指标过滤组合穷举
  4. 市场环境分层（牛/熊/震荡）分析
  5. 自动搜索最优参数组合

用法：
    python -m day_week_predicted.backtest.five_strategy_deep_v2
"""
import sys
import math
import logging
import json
from datetime import datetime
from collections import defaultdict
from itertools import combinations

sys.path.insert(0, '.')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

from dao.stock_kline_dao import get_kline_data, get_all_stock_codes
from day_week_predicted.backtest.shipanxian_enhanced_backtest import (
    calc_ma, calc_ema, calc_macd, calc_rsi, calc_kdj, calc_stats,
)

HOLD_DAYS = [3, 5, 7]


# ═══════════════════════════════════════════════════════════════════
#  指标预计算（扩展版，含布林/ATR）
# ═══════════════════════════════════════════════════════════════════

def _safe_div(a, b, default=0.0):
    return a / b if b and b != 0 else default


def precompute_all(klines: list[dict]) -> dict | None:
    n = len(klines)
    if n < 260:
        return None

    closes = [float(k['close_price']) for k in klines]
    opens = [float(k['open_price']) for k in klines]
    highs = [float(k['high_price']) for k in klines]
    lows = [float(k['low_price']) for k in klines]
    vols = [float(k['trading_volume']) for k in klines]
    ch = [float(k.get('change_hand', 0) or 0) for k in klines]

    ma5 = calc_ma(closes, 5)
    ma10 = calc_ma(closes, 10)
    ma20 = calc_ma(closes, 20)
    ma60 = calc_ma(closes, 60)
    ma250 = calc_ma(closes, 250)
    ema20 = calc_ema(closes, 20)
    ema40 = calc_ema(closes, 40)
    ema60 = calc_ema(closes, 60)

    vm5 = calc_ma(vols, 5)
    vm10 = calc_ma(vols, 10)
    vm20 = calc_ma(vols, 20)

    dif, dea, macd_bar = calc_macd(closes)
    rsi6 = calc_rsi(closes, 6)
    rsi14 = calc_rsi(closes, 14)
    k_vals, d_vals, j_vals = calc_kdj(highs, lows, closes)

    # 布林带 (20日)
    boll_mid = ma20[:]
    boll_up = [0.0] * n
    boll_dn = [0.0] * n
    for i in range(19, n):
        window = closes[i - 19:i + 1]
        avg = sum(window) / 20
        std = (sum((x - avg) ** 2 for x in window) / 20) ** 0.5
        boll_up[i] = avg + 2 * std
        boll_dn[i] = avg - 2 * std

    # ATR(14)
    atr14 = [0.0] * n
    trs = []
    for i in range(1, n):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i - 1]),
                 abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    if len(trs) >= 14:
        atr14[14] = sum(trs[:14]) / 14
        for i in range(15, n):
            atr14[i] = (atr14[i - 1] * 13 + trs[i - 1]) / 14

    return {
        'n': n, 'closes': closes, 'opens': opens, 'highs': highs,
        'lows': lows, 'vols': vols, 'change_hands': ch,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60, 'ma250': ma250,
        'ema20': ema20, 'ema40': ema40, 'ema60': ema60,
        'vm5': vm5, 'vm10': vm10, 'vm20': vm20,
        'dif': dif, 'dea': dea, 'macd_bar': macd_bar,
        'rsi6': rsi6, 'rsi14': rsi14,
        'k': k_vals, 'd': d_vals, 'j': j_vals,
        'boll_up': boll_up, 'boll_dn': boll_dn, 'boll_mid': boll_mid,
        'atr14': atr14,
    }


# ═══════════════════════════════════════════════════════════════════
#  5大策略（含宽松/严格变体）
# ═══════════════════════════════════════════════════════════════════

def _approx_winner(closes, i, period=60):
    if i < period:
        return 0.5
    window = closes[i - period:i + 1]
    return sum(1 for p in window if p <= closes[i]) / len(window)


def _approx_cost_ratio(closes, i, period=50):
    if i < period:
        return 1.0
    window = sorted(closes[i - period:i + 1])
    median = window[len(window) // 2]
    return _safe_div(closes[i], median, 1.0)


# --- S1 暴力洗盘 ---
def s1_strict(ind, i, code):
    """原版暴力洗盘"""
    if i < 60 or i < 1 or ind['closes'][i - 1] <= 0:
        return False
    c = ind['closes']
    drop = (c[i] - c[i - 1]) / c[i - 1] * 100
    if drop >= -4.5:
        return False
    has_zt = any(c[j] / c[j - 1] >= 0.099 for j in range(max(1, i - 8), i + 1) if c[j - 1] > 0)
    if not has_zt:
        return False
    if not (ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > ind['ma60'][i] > 0):
        return False
    w = _approx_winner(c, i)
    if not (0.65 <= w <= 0.90):
        return False
    cr = _approx_cost_ratio(c, i)
    return cr > 1.15


def s1_loose(ind, i, code):
    """放宽版：跌幅>3%，获利盘50-95%，去掉成本比率"""
    if i < 60 or i < 1 or ind['closes'][i - 1] <= 0:
        return False
    c = ind['closes']
    drop = (c[i] - c[i - 1]) / c[i - 1] * 100
    if drop >= -3.0:
        return False
    has_zt = any(c[j] / c[j - 1] >= 0.095 for j in range(max(1, i - 12), i + 1) if c[j - 1] > 0)
    if not has_zt:
        return False
    if not (ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0):
        return False
    w = _approx_winner(c, i)
    return 0.50 <= w <= 0.95


# --- S2 堆量挖坑 ---
def s2_strict(ind, i, code):
    """原版堆量挖坑"""
    if i < 60 or code.startswith('688'):
        return False
    c, l, v = ind['closes'], ind['lows'], ind['vols']
    vm5, vm10 = ind['vm5'], ind['vm10']
    if vm5[i] <= 0 or vm10[i] <= 0 or i < 1:
        return False
    cross = vm5[i] > vm10[i] and vm5[i - 1] <= vm10[i - 1]
    if not cross:
        return False
    if v[i] <= vm5[i] * 1.2:
        return False
    if i < 5 or c[i - 5] <= 0 or c[i] / c[i - 5] >= 1.00:
        return False
    ma10v = ind['ma10'][i]
    if ma10v <= 0 or l[i] > ma10v * 1.02 or c[i] < ma10v:
        return False
    ma60v = ind['ma60'][i]
    if ma60v <= 0 or c[i] <= ma60v or ind['ma60'][i] <= ind['ma60'][i - 1]:
        return False
    return True


def s2_loose(ind, i, code):
    """放宽版：不要求量能金叉，只要放量+回踩均线+趋势向上"""
    if i < 60 or code.startswith('688'):
        return False
    c, l, v = ind['closes'], ind['lows'], ind['vols']
    vm5 = ind['vm5']
    if vm5[i] <= 0 or v[i] <= vm5[i] * 1.1:
        return False
    # 近5日有回调
    if i < 5 or c[i - 5] <= 0:
        return False
    min_5d = min(c[j] for j in range(i - 4, i + 1))
    if min_5d / c[i - 5] >= 1.00:  # 近5日有低于5日前的价格
        pass  # OK
    else:
        return False
    # 收盘在MA10上方
    ma10v = ind['ma10'][i]
    if ma10v <= 0 or c[i] < ma10v * 0.98:
        return False
    # MA60向上
    ma60v = ind['ma60'][i]
    if ma60v <= 0 or c[i] <= ma60v * 0.95:
        return False
    return True


# --- S3 蜻蜓点水 ---
def s3_strict(ind, i, code):
    """原版蜻蜓点水"""
    if i < 60:
        return False
    c, o, h, l = ind['closes'][i], ind['opens'][i], ind['highs'][i], ind['lows'][i]
    v = ind['vols']
    ma20v, ma60v = ind['ma20'][i], ind['ma60'][i]
    if ma20v <= 0 or ma60v <= 0:
        return False
    hui_20 = abs(l - ma20v) / ma20v <= 0.02 and c > ma20v
    hui_60 = abs(l - ma60v) / ma60v <= 0.02 and c > ma60v
    if not (hui_20 or hui_60):
        return False
    amp = h - l
    if amp <= 0:
        return False
    if (min(o, c) - l) / amp <= 0.3:
        return False
    if (c - o) / max(o, 0.01) <= -0.01:
        return False
    if i < 1 or v[i - 1] <= 0:
        return False
    if not (v[i] > v[i - 1] * 1.1 and v[i] < v[i - 1] * 4):
        return False
    if max(c, o) <= 0 or (h - max(c, o)) / max(c, o) > 0.01:
        return False
    if l <= ind['lows'][i - 1] * 1.01:
        return False
    if i < 3:
        return False
    llv4 = min(ind['lows'][j] for j in range(i - 3, i + 1))
    if l > llv4 * 1.005:
        return False
    return True


def s3_loose(ind, i, code):
    """放宽版：回踩幅度5%，下影线>20%，允许微阴，去掉4日最低"""
    if i < 60:
        return False
    c, o, h, l = ind['closes'][i], ind['opens'][i], ind['highs'][i], ind['lows'][i]
    v = ind['vols']
    ma20v, ma60v = ind['ma20'][i], ind['ma60'][i]
    if ma20v <= 0 and ma60v <= 0:
        return False
    hui_20 = ma20v > 0 and abs(l - ma20v) / ma20v <= 0.05 and c > ma20v * 0.99
    hui_60 = ma60v > 0 and abs(l - ma60v) / ma60v <= 0.05 and c > ma60v * 0.99
    if not (hui_20 or hui_60):
        return False
    amp = h - l
    if amp <= 0:
        return False
    if (min(o, c) - l) / amp <= 0.20:
        return False
    if (c - o) / max(o, 0.01) <= -0.02:
        return False
    if i < 1 or v[i - 1] <= 0:
        return False
    if v[i] < v[i - 1] * 0.8:
        return False
    return True


# --- S4 出水芙蓉 ---
def s4_strict(ind, i, code):
    """原版出水芙蓉"""
    if i < 250:
        return False
    c, o, v = ind['closes'][i], ind['opens'][i], ind['vols'][i]
    e20, e40, e60 = ind['ema20'][i], ind['ema40'][i], ind['ema60'][i]
    ma250v, vm10 = ind['ma250'][i], ind['vm10'][i]
    if e60 <= 0 or e40 <= 0 or e20 <= 0 or ma250v <= 0 or vm10 <= 0:
        return False
    if o >= e60:
        return False
    max_e = max(e20, e40, e60)
    if c <= max_e:
        return False
    if v / vm10 <= 1.2:
        return False
    if i < 1 or ind['closes'][i - 1] <= 0 or c / ind['closes'][i - 1] <= 1.049:
        return False
    min_e = min(e20, e40, e60)
    if min_e <= 0 or max_e / min_e >= 1.1:
        return False
    if i < 21:
        return False
    hhv = max(ind['highs'][j] for j in range(i - 20, i))
    llv = min(ind['lows'][j] for j in range(i - 20, i))
    if llv <= 0 or hhv / llv >= 1.25:
        return False
    if c / ma250v >= 1.15:
        return False
    if ind['dif'][i] <= ind['dea'][i]:
        return False
    if ind['rsi6'][i] >= 80:
        return False
    return True


def s4_loose(ind, i, code):
    """放宽版：涨幅>3%，均线粘合<15%，去掉年线和RSI限制"""
    if i < 60:
        return False
    c, o, v = ind['closes'][i], ind['opens'][i], ind['vols'][i]
    e20, e40, e60 = ind['ema20'][i], ind['ema40'][i], ind['ema60'][i]
    vm10 = ind['vm10'][i]
    if e60 <= 0 or e20 <= 0 or vm10 <= 0:
        return False
    # 开盘在某条均线下
    if o >= max(e20, e40, e60):
        return False
    # 收盘突破至少2条均线
    above_count = sum(1 for e in [e20, e40, e60] if c > e)
    if above_count < 2:
        return False
    if v / vm10 <= 1.0:
        return False
    if i < 1 or ind['closes'][i - 1] <= 0 or c / ind['closes'][i - 1] <= 1.03:
        return False
    max_e = max(e20, e40, e60)
    min_e = min(e20, e40, e60)
    if min_e <= 0 or max_e / min_e >= 1.15:
        return False
    if ind['dif'][i] <= ind['dea'][i]:
        return False
    return True


# --- S5 店大欺客 ---
def s5_strict(ind, i, code):
    """原版店大欺客"""
    if i < 20 or code.startswith('688') or code.startswith('30'):
        return False
    c, o, v = ind['closes'], ind['opens'], ind['vols']
    if v[i] <= 1:
        return False
    zt_cnt = sum(1 for j in range(max(1, i - 4), i + 1) if c[j - 1] > 0 and c[j] / c[j - 1] >= 1.0987)
    if zt_cnt < 2:
        return False
    if c[i] >= o[i]:
        return False
    yang_cnt = sum(1 for j in range(max(0, i - 4), i + 1) if c[j] >= o[j])
    if yang_cnt < 3:
        return False
    vm20 = ind['vm20'][i]
    if vm20 <= 0 or v[i] / vm20 >= 2.5:
        return False
    if c[i] <= ind['ma5'][i] or c[i] <= ind['ma10'][i]:
        return False
    return True


def s5_loose(ind, i, code):
    """放宽版：1次涨停即可，允许创业板，量比<3.5"""
    if i < 20 or code.startswith('688'):
        return False
    c, o, v = ind['closes'], ind['opens'], ind['vols']
    if v[i] <= 1:
        return False
    zt_cnt = sum(1 for j in range(max(1, i - 6), i + 1) if c[j - 1] > 0 and c[j] / c[j - 1] >= 1.095)
    if zt_cnt < 1:
        return False
    if c[i] >= o[i]:
        return False
    yang_cnt = sum(1 for j in range(max(0, i - 4), i + 1) if c[j] >= o[j])
    if yang_cnt < 2:
        return False
    vm20 = ind['vm20'][i]
    if vm20 <= 0 or v[i] / vm20 >= 3.5:
        return False
    if c[i] <= ind['ma5'][i] * 0.97:
        return False
    return True


# --- 试盘线 ---
def s0_shipanxian(ind, i, code):
    if i < 50:
        return False
    c, o, h, l = ind['closes'][i], ind['opens'][i], ind['highs'][i], ind['lows'][i]
    v = ind['vols'][i]
    pc, pv = ind['closes'][i - 1], ind['vols'][i - 1]
    if c <= 0 or pc <= 0 or pv <= 0:
        return False
    if (h - max(o, c)) / c <= 0.025:
        return False
    if (h - pc) / pc <= 0.07:
        return False
    if (c - pc) / pc <= 0.03:
        return False
    llv50 = min(ind['lows'][j] for j in range(i - 49, i + 1))
    if llv50 <= 0 or c / llv50 >= 1.4:
        return False
    if c < max(ind['closes'][j] for j in range(i - 4, i + 1)):
        return False
    if v / pv <= 2:
        return False
    if v < max(ind['vols'][j] for j in range(i - 4, i + 1)):
        return False
    llv20 = min(ind['lows'][j] for j in range(i - 19, i + 1))
    if llv20 <= 0 or l / llv20 >= 1.2:
        return False
    hhv20 = max(ind['highs'][j] for j in range(i - 19, i + 1))
    if hhv20 <= 0 or l / hhv20 <= 0.9:
        return False
    return True


# ═══════════════════════════════════════════════════════════════════
#  VP量价过滤维度 + 技术指标过滤维度
# ═══════════════════════════════════════════════════════════════════

def f_vol_ratio_ok(ind, i):
    """量比2~5"""
    if ind['vm5'][i] <= 0:
        return False
    r = ind['vols'][i] / ind['vm5'][i]
    return 2.0 <= r <= 5.0


def f_vol_mild(ind, i):
    """温和放量: 5日均量/20日均量 1.2~3"""
    if ind['vm20'][i] <= 0:
        return False
    r = ind['vm5'][i] / ind['vm20'][i]
    return 1.2 <= r <= 3.0


def f_turnover_ok(ind, i):
    """换手率3~15%"""
    return 3.0 <= ind['change_hands'][i] <= 15.0


def f_solid_yang(ind, i):
    """实体阳线: body/amp > 0.5"""
    o, c, h, l = ind['opens'][i], ind['closes'][i], ind['highs'][i], ind['lows'][i]
    amp = h - l
    if amp <= 0:
        return False
    return abs(c - o) / amp > 0.5 and c > o


def f_ma_bull(ind, i):
    """均线多头: MA5>MA10>MA20"""
    return ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0


def f_ma_bull_60(ind, i):
    """大均线多头: MA20>MA60 且 C>MA60"""
    return ind['ma20'][i] > ind['ma60'][i] > 0 and ind['closes'][i] > ind['ma60'][i]


def f_macd_bull(ind, i):
    """MACD多头: DIF>DEA"""
    return ind['dif'][i] > ind['dea'][i]


def f_macd_golden(ind, i):
    """MACD金叉: DIF上穿DEA"""
    if i < 1:
        return False
    return ind['dif'][i] > ind['dea'][i] and ind['dif'][i - 1] <= ind['dea'][i - 1]


def f_kdj_bull(ind, i):
    """KDJ金叉区: K>D 且 K<80"""
    return ind['k'][i] > ind['d'][i] and ind['k'][i] < 80


def f_rsi_mid(ind, i):
    """RSI适中: 40~70"""
    return 40 <= ind['rsi14'][i] <= 70


def f_boll_lower(ind, i):
    """价格在布林下轨附近(下半区)"""
    mid = ind['boll_mid'][i]
    dn = ind['boll_dn'][i]
    if mid <= 0 or dn <= 0:
        return False
    return ind['closes'][i] <= (mid + dn) / 2


def f_boll_squeeze(ind, i):
    """布林收窄: (上轨-下轨)/中轨 < 0.1"""
    mid = ind['boll_mid'][i]
    if mid <= 0:
        return False
    width = (ind['boll_up'][i] - ind['boll_dn'][i]) / mid
    return width < 0.10


def f_close_upper_half(ind, i):
    """收盘在当日上半区"""
    h, l, c = ind['highs'][i], ind['lows'][i], ind['closes'][i]
    if h == l:
        return False
    return (c - l) / (h - l) > 0.5


def f_vol_shrink_then_expand(ind, i):
    """缩量后放量: 前3日均量<10日均量，当日>10日均量*1.5"""
    if i < 3 or ind['vm10'][i] <= 0:
        return False
    prev3 = sum(ind['vols'][i - 3:i]) / 3
    return prev3 < ind['vm10'][i] and ind['vols'][i] > ind['vm10'][i] * 1.5


def f_no_divergence(ind, i):
    """非量价背离: 涨>3%时量比>=1.2"""
    if i < 1 or ind['closes'][i - 1] <= 0 or ind['vm5'][i] <= 0:
        return True
    chg = (ind['closes'][i] - ind['closes'][i - 1]) / ind['closes'][i - 1]
    if chg > 0.03:
        return ind['vols'][i] / ind['vm5'][i] >= 1.2
    return True


def f_atr_moderate(ind, i):
    """ATR适中: ATR/C 在 1%~5%"""
    if ind['atr14'][i] <= 0 or ind['closes'][i] <= 0:
        return False
    r = ind['atr14'][i] / ind['closes'][i]
    return 0.01 <= r <= 0.05


# 过滤器注册表
FILTERS = {
    'F_量比适中': f_vol_ratio_ok,
    'F_温和放量': f_vol_mild,
    'F_换手率OK': f_turnover_ok,
    'F_实体阳线': f_solid_yang,
    'F_均线多头': f_ma_bull,
    'F_大趋势多头': f_ma_bull_60,
    'F_MACD多头': f_macd_bull,
    'F_MACD金叉': f_macd_golden,
    'F_KDJ金叉区': f_kdj_bull,
    'F_RSI适中': f_rsi_mid,
    'F_布林下轨': f_boll_lower,
    'F_布林收窄': f_boll_squeeze,
    'F_收盘上半区': f_close_upper_half,
    'F_缩放量': f_vol_shrink_then_expand,
    'F_非背离': f_no_divergence,
    'F_ATR适中': f_atr_moderate,
}


# ═══════════════════════════════════════════════════════════════════
#  策略注册表
# ═══════════════════════════════════════════════════════════════════

STRATEGY_FUNCS = {
    'S0_试盘线': s0_shipanxian,
    'S1_暴力洗盘': s1_strict,
    'S1L_暴力洗盘宽': s1_loose,
    'S2_堆量挖坑': s2_strict,
    'S2L_堆量挖坑宽': s2_loose,
    'S3_蜻蜓点水': s3_strict,
    'S3L_蜻蜓点水宽': s3_loose,
    'S4_出水芙蓉': s4_strict,
    'S4L_出水芙蓉宽': s4_loose,
    'S5_店大欺客': s5_strict,
    'S5L_店大欺客宽': s5_loose,
}


# ═══════════════════════════════════════════════════════════════════
#  市场环境判断
# ═══════════════════════════════════════════════════════════════════

def classify_market(ind, i):
    """根据MA60斜率和价格位置判断市场环境"""
    if i < 60 or ind['ma60'][i] <= 0:
        return 'unknown'
    c = ind['closes'][i]
    ma60_now = ind['ma60'][i]
    ma60_prev = ind['ma60'][max(0, i - 20)]
    slope = (ma60_now - ma60_prev) / ma60_prev if ma60_prev > 0 else 0

    if c > ma60_now and slope > 0.02:
        return 'bull'
    elif c < ma60_now and slope < -0.02:
        return 'bear'
    else:
        return 'range'


# ═══════════════════════════════════════════════════════════════════
#  单股回测
# ═══════════════════════════════════════════════════════════════════

def backtest_stock(stock_code: str) -> list[dict]:
    klines = get_kline_data(stock_code, limit=500)
    if len(klines) < 260:
        return []
    ind = precompute_all(klines)
    if ind is None:
        return []

    signals = []
    n = ind['n']

    for i in range(250, n):
        # 检测所有策略
        hits = {}
        for sname, sfunc in STRATEGY_FUNCS.items():
            try:
                hits[sname] = sfunc(ind, i, stock_code)
            except Exception:
                hits[sname] = False

        if not any(hits.values()):
            continue

        if i + 1 >= n:
            continue
        buy_price = float(klines[i + 1]['open_price'])
        if buy_price <= 0:
            continue

        sig = {
            'code': stock_code,
            'date': str(klines[i]['date']),
            'buy': buy_price,
            'market': classify_market(ind, i),
        }

        for hd in HOLD_DAYS:
            si = i + 1 + hd
            if si < n:
                sp = float(klines[si]['close_price'])
                sig[f'r{hd}'] = round((sp - buy_price) / buy_price * 100, 2)

        for sname, hit in hits.items():
            sig[f'h_{sname}'] = hit

        # 计算过滤器状态
        for fname, ffunc in FILTERS.items():
            try:
                sig[f'f_{fname}'] = ffunc(ind, i)
            except Exception:
                sig[f'f_{fname}'] = False

        # 投票
        core5 = ['S1_暴力洗盘', 'S2_堆量挖坑', 'S3_蜻蜓点水', 'S4_出水芙蓉', 'S5_店大欺客']
        sig['votes'] = sum(1 for s in core5 if hits.get(s, False))
        all11 = list(STRATEGY_FUNCS.keys())
        sig['votes_all'] = sum(1 for s in all11 if hits.get(s, False))

        signals.append(sig)

    return signals


# ═══════════════════════════════════════════════════════════════════
#  统计工具
# ═══════════════════════════════════════════════════════════════════

def quick_stats(sigs, hd=5):
    key = f'r{hd}'
    rets = [s[key] for s in sigs if key in s]
    if not rets:
        return {'n': 0}
    wins = sum(1 for r in rets if r > 0)
    total = len(rets)
    avg = sum(rets) / total
    sr = sorted(rets)
    return {
        'n': total,
        'wr': round(wins / total * 100, 1),
        'avg': round(avg, 2),
        'med': round(sr[total // 2], 2),
        'mx': round(max(rets), 2),
        'mn': round(min(rets), 2),
    }


def print_stats_line(label, stats, width=30):
    if stats['n'] == 0:
        logger.info("  %-*s %5d  (无数据)", width, label, 0)
    else:
        logger.info("  %-*s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                     width, label, stats['n'], stats['wr'], stats['avg'], stats['med'])


# ═══════════════════════════════════════════════════════════════════
#  主回测
# ═══════════════════════════════════════════════════════════════════

def run(sample_limit=1000):
    t0 = datetime.now()
    logger.info("=" * 85)
    logger.info("  5大策略深度混合分析 v2 (含宽松变体+VP过滤+市场分层)")
    logger.info("  样本上限: %d | 持有: %s", sample_limit, HOLD_DAYS)
    logger.info("=" * 85)

    codes = sorted(get_all_stock_codes())
    if sample_limit > 0:
        codes = codes[:sample_limit]
    logger.info("股票数: %d", len(codes))

    all_sigs = []
    for idx, c in enumerate(codes):
        if (idx + 1) % 100 == 0:
            logger.info("  进度: %d/%d (信号: %d)", idx + 1, len(codes), len(all_sigs))
        all_sigs.extend(backtest_stock(c))

    logger.info("  总信号: %d", len(all_sigs))
    if not all_sigs:
        return

    # ═══ PART 1: 所有策略变体独立表现 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [1] 所有策略变体独立表现")
    logger.info("=" * 85)
    logger.info("  %-22s %5s  %7s  %7s  %7s  │ %7s  %7s  %7s",
                "策略", "信号", "3d胜率", "3d均收", "3d中位", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 85)

    strat_perf = {}
    for sname in STRATEGY_FUNCS:
        filtered = [s for s in all_sigs if s.get(f'h_{sname}', False)]
        s3 = quick_stats(filtered, 3)
        s5 = quick_stats(filtered, 5)
        strat_perf[sname] = {'s3': s3, 's5': s5, 'sigs': filtered}
        if s5['n'] > 0:
            logger.info("  %-22s %5d  %6.1f%%  %6.2f%%  %6.2f%%  │ %6.1f%%  %6.2f%%  %6.2f%%",
                         sname, s5['n'],
                         s3['wr'] if s3['n'] > 0 else 0,
                         s3['avg'] if s3['n'] > 0 else 0,
                         s3['med'] if s3['n'] > 0 else 0,
                         s5['wr'], s5['avg'], s5['med'])
        else:
            logger.info("  %-22s %5d  (无信号)", sname, 0)

    # ═══ PART 2: 市场环境分层 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [2] 市场环境分层分析 (5日持有)")
    logger.info("=" * 85)

    for sname in STRATEGY_FUNCS:
        sigs = strat_perf[sname]['sigs']
        if len(sigs) < 10:
            continue
        logger.info("  ── %s ──", sname)
        for env in ['bull', 'range', 'bear']:
            env_sigs = [s for s in sigs if s.get('market') == env]
            st = quick_stats(env_sigs, 5)
            if st['n'] > 0:
                logger.info("    %-6s %4d信号  胜率%5.1f%%  均收%6.2f%%  中位%6.2f%%",
                             env, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 3: 策略OR联合 + 试盘线 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [3] 策略OR联合 (任一触发即入场)")
    logger.info("=" * 85)
    logger.info("  %-35s %5s  %7s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 70)

    # 核心5策略严格版OR
    core_strict = ['S1_暴力洗盘', 'S2_堆量挖坑', 'S3_蜻蜓点水', 'S4_出水芙蓉', 'S5_店大欺客']
    core_loose = ['S1L_暴力洗盘宽', 'S2L_堆量挖坑宽', 'S3L_蜻蜓点水宽', 'S4L_出水芙蓉宽', 'S5L_店大欺客宽']

    or_combos = [
        ('5策略严格OR', core_strict),
        ('5策略宽松OR', core_loose),
        ('5策略严格OR + 试盘线', core_strict + ['S0_试盘线']),
        ('5策略宽松OR + 试盘线', core_loose + ['S0_试盘线']),
        ('S2堆量 OR 试盘线', ['S2_堆量挖坑', 'S0_试盘线']),
        ('S2堆量宽 OR 试盘线', ['S2L_堆量挖坑宽', 'S0_试盘线']),
        ('S2堆量 OR S4芙蓉 OR 试盘线', ['S2_堆量挖坑', 'S4_出水芙蓉', 'S0_试盘线']),
        ('S2宽 OR S4宽 OR 试盘线', ['S2L_堆量挖坑宽', 'S4L_出水芙蓉宽', 'S0_试盘线']),
    ]

    or_results = {}
    for label, strats in or_combos:
        filtered = [s for s in all_sigs if any(s.get(f'h_{sn}', False) for sn in strats)]
        st = quick_stats(filtered, 5)
        or_results[label] = {'stats': st, 'sigs': filtered}
        print_stats_line(label, st, 35)

    # ═══ PART 4: 单过滤器对各策略的增强效果 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [4] 过滤器增强效果 (对最佳OR组合)")
    logger.info("=" * 85)

    # 选最佳OR组合
    best_or_label = max(or_results, key=lambda k: or_results[k]['stats'].get('wr', 0)
                        if or_results[k]['stats']['n'] >= 20 else 0)
    best_or_sigs = or_results[best_or_label]['sigs']
    base_st = quick_stats(best_or_sigs, 5)
    logger.info("  基准: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                 best_or_label, base_st['wr'], base_st['avg'], base_st['n'])
    logger.info("")
    logger.info("  %-18s %5s  %7s(%6s)  %7s(%7s)",
                "过滤器", "信号", "5d胜率", "提升", "5d均收", "提升")
    logger.info("  " + "-" * 70)

    filter_effects = []
    for fname in FILTERS:
        filtered = [s for s in best_or_sigs if s.get(f'f_{fname}', False)]
        st = quick_stats(filtered, 5)
        if st['n'] >= 5:
            wr_diff = st['wr'] - base_st['wr']
            avg_diff = st['avg'] - base_st['avg']
            filter_effects.append((fname, st, wr_diff, avg_diff))

    filter_effects.sort(key=lambda x: x[2], reverse=True)
    for fname, st, wr_d, avg_d in filter_effects:
        logger.info("  %-18s %5d  %6.1f%%(%+5.1f)  %6.2f%%(%+6.2f)",
                     fname, st['n'], st['wr'], wr_d, st['avg'], avg_d)


    # ═══ PART 5: 过滤器组合穷举 (2~3个) ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [5] 过滤器组合穷举 (对最佳OR组合, 2~3个过滤器AND)")
    logger.info("=" * 85)

    # 只用提升胜率的过滤器
    good_filters = [fname for fname, st, wr_d, avg_d in filter_effects if wr_d > 0]
    logger.info("  候选过滤器(%d个): %s", len(good_filters), ', '.join(good_filters))

    combo_results = []

    # 2个组合
    for combo in combinations(good_filters[:10], 2):  # 限制前10个
        filtered = [s for s in best_or_sigs
                    if all(s.get(f'f_{fn}', False) for fn in combo)]
        st = quick_stats(filtered, 5)
        if st['n'] >= 10:
            combo_results.append((' + '.join(combo), st))

    # 3个组合
    for combo in combinations(good_filters[:8], 3):
        filtered = [s for s in best_or_sigs
                    if all(s.get(f'f_{fn}', False) for fn in combo)]
        st = quick_stats(filtered, 5)
        if st['n'] >= 8:
            combo_results.append((' + '.join(combo), st))

    combo_results.sort(key=lambda x: x[1]['wr'], reverse=True)

    logger.info("")
    logger.info("  %-50s %5s  %7s  %7s  %7s",
                "过滤器组合", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 85)
    for label, st in combo_results[:20]:
        logger.info("  %-50s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                     label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 6: 策略+过滤器最优组合搜索 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [6] 策略+过滤器最优组合搜索")
    logger.info("=" * 85)

    # 对每个策略(含宽松)，搜索最佳过滤器组合
    best_combos = []
    for sname in STRATEGY_FUNCS:
        s_sigs = strat_perf[sname]['sigs']
        if len(s_sigs) < 10:
            continue
        base = quick_stats(s_sigs, 5)
        if base['n'] < 10:
            continue

        best_for_strat = None
        best_wr_for_strat = base['wr']

        # 单过滤器
        for fname in FILTERS:
            filtered = [s for s in s_sigs if s.get(f'f_{fname}', False)]
            st = quick_stats(filtered, 5)
            if st['n'] >= 8 and st['wr'] > best_wr_for_strat:
                best_wr_for_strat = st['wr']
                best_for_strat = (sname, [fname], st)

        # 2过滤器组合
        for combo in combinations(good_filters[:8], 2):
            filtered = [s for s in s_sigs if all(s.get(f'f_{fn}', False) for fn in combo)]
            st = quick_stats(filtered, 5)
            if st['n'] >= 6 and st['wr'] > best_wr_for_strat:
                best_wr_for_strat = st['wr']
                best_for_strat = (sname, list(combo), st)

        if best_for_strat:
            best_combos.append(best_for_strat)

    best_combos.sort(key=lambda x: x[2]['wr'], reverse=True)

    logger.info("  %-22s %-35s %5s  %7s  %7s",
                "策略", "过滤器", "信号", "5d胜率", "5d均收")
    logger.info("  " + "-" * 85)
    for sname, fnames, st in best_combos[:15]:
        logger.info("  %-22s %-35s %5d  %6.1f%%  %6.2f%%",
                     sname, '+'.join(fnames), st['n'], st['wr'], st['avg'])

    # ═══ PART 7: OR联合 + 最佳过滤器 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [7] OR联合策略 + 最佳过滤器组合")
    logger.info("=" * 85)

    # 取PART5中最佳过滤器组合
    if combo_results:
        top_filter_combo_label = combo_results[0][0]
        top_filter_names = top_filter_combo_label.split(' + ')
    else:
        top_filter_names = good_filters[:2] if len(good_filters) >= 2 else good_filters[:1]

    logger.info("  最佳过滤器: %s", ' + '.join(top_filter_names))
    logger.info("")
    logger.info("  %-40s %5s  %7s  %7s  %7s",
                "组合", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 75)

    final_combos = []
    for label, strats in or_combos:
        filtered = [s for s in all_sigs
                    if any(s.get(f'h_{sn}', False) for sn in strats)
                    and all(s.get(f'f_{fn}', False) for fn in top_filter_names)]
        st = quick_stats(filtered, 5)
        final_combos.append((label + ' +过滤', st, filtered))
        if st['n'] > 0:
            print_stats_line(label + ' +过滤', st, 40)

    # ═══ PART 8: 时间序列稳定性 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [8] 最优组合时间序列稳定性")
    logger.info("=" * 85)

    # 找最优final combo
    valid_finals = [(l, st, sigs) for l, st, sigs in final_combos if st['n'] >= 10]
    if not valid_finals:
        valid_finals = [(l, st, sigs) for l, st, sigs in final_combos if st['n'] >= 3]

    if valid_finals:
        best_final = max(valid_finals, key=lambda x: x[1].get('wr', 0))
        bf_label, bf_st, bf_sigs = best_final
        logger.info("  最优: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     bf_label, bf_st['wr'], bf_st['avg'], bf_st['n'])

        # 按月分析
        monthly = defaultdict(list)
        for s in bf_sigs:
            if 'r5' in s:
                monthly[s['date'][:7]].append(s['r5'])

        logger.info("")
        logger.info("  %-8s %5s  %7s  %7s", "月份", "信号", "胜率", "均收益")
        months_positive = 0
        for m in sorted(monthly):
            rets = monthly[m]
            wr = sum(1 for r in rets if r > 0) / len(rets) * 100
            avg = sum(rets) / len(rets)
            if wr > 50:
                months_positive += 1
            logger.info("  %-8s %5d  %6.1f%%  %6.2f%%", m, len(rets), wr, avg)

        total_months = len(monthly)
        logger.info("  稳定性: %d/%d月胜率>50%% (%.1f%%)",
                     months_positive, total_months,
                     months_positive / total_months * 100 if total_months > 0 else 0)

    # ═══ PART 9: 也对纯策略做时间序列 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [9] 各纯策略+最佳过滤器 时间序列")
    logger.info("=" * 85)

    for sname in STRATEGY_FUNCS:
        s_sigs = strat_perf[sname]['sigs']
        filtered = [s for s in s_sigs if all(s.get(f'f_{fn}', False) for fn in top_filter_names)]
        st = quick_stats(filtered, 5)
        if st['n'] < 5:
            continue

        monthly = defaultdict(list)
        for s in filtered:
            if 'r5' in s:
                monthly[s['date'][:7]].append(s['r5'])

        mp = sum(1 for rets in monthly.values() if sum(1 for r in rets if r > 0) / len(rets) > 0.5)
        tm = len(monthly)
        logger.info("  %-22s %4d信号  胜率%5.1f%%  均收%6.2f%%  稳定性%d/%d月(%.0f%%)",
                     sname, st['n'], st['wr'], st['avg'], mp, tm,
                     mp / tm * 100 if tm > 0 else 0)

    # ═══ PART 10: 综合结论 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [10] 综合结论")
    logger.info("=" * 85)

    # 收集所有测试过的组合
    all_tested = []

    # 纯策略
    for sname in STRATEGY_FUNCS:
        st = strat_perf[sname]['s5']
        if st['n'] >= 10:
            all_tested.append((sname, st['n'], st['wr'], st['avg']))

    # OR组合
    for label, data in or_results.items():
        st = data['stats']
        if st['n'] >= 10:
            all_tested.append((label, st['n'], st['wr'], st['avg']))

    # 策略+过滤器
    for sname, fnames, st in best_combos:
        if st['n'] >= 6:
            label = f"{sname} + {'+'.join(fnames)}"
            all_tested.append((label, st['n'], st['wr'], st['avg']))

    # final combos
    for label, st, _ in final_combos:
        if st['n'] >= 5:
            all_tested.append((label, st['n'], st['wr'], st['avg']))

    # 按胜率排序
    all_tested.sort(key=lambda x: x[2], reverse=True)

    logger.info("  TOP 15 组合 (按5日胜率排序):")
    logger.info("  %-55s %5s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收")
    logger.info("  " + "-" * 80)
    for label, n, wr, avg in all_tested[:15]:
        logger.info("  %-55s %5d  %6.1f%%  %6.2f%%", label, n, wr, avg)

    # 按收益排序
    all_tested.sort(key=lambda x: x[3], reverse=True)
    logger.info("")
    logger.info("  TOP 15 组合 (按5日均收益排序):")
    logger.info("  %-55s %5s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收")
    logger.info("  " + "-" * 80)
    for label, n, wr, avg in all_tested[:15]:
        logger.info("  %-55s %5d  %6.1f%%  %6.2f%%", label, n, wr, avg)

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 85)

    # 保存
    result = {
        'run_time': str(datetime.now()),
        'total_signals': len(all_sigs),
        'stock_count': len(codes),
        'top15_by_wr': [{'label': l, 'n': n, 'wr': wr, 'avg': avg}
                        for l, n, wr, avg in sorted(all_tested, key=lambda x: x[2], reverse=True)[:15]],
        'top15_by_avg': [{'label': l, 'n': n, 'wr': wr, 'avg': avg}
                         for l, n, wr, avg in sorted(all_tested, key=lambda x: x[3], reverse=True)[:15]],
    }
    try:
        with open('data_results/five_strategy_deep_v2_result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("  结果已保存到 data_results/five_strategy_deep_v2_result.json")
    except Exception as e:
        logger.warning("  保存失败: %s", e)

    return all_sigs


if __name__ == '__main__':
    run(sample_limit=1000)
