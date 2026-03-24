#!/usr/bin/env python3
"""
6大策略深度回测 v5 — 自主深度优化
==================================
基于v4发现的关键线索进行深度挖掘：
  1. S6+缩量(65.7%/70信号) 深度验证：参数微调、扩大信号量
  2. 空中加油参数敏感性分析：收敛阈值、缩短天数、DEA阈值
  3. 加权投票系统：多策略信号叠加打分
  4. 最优过滤器三重组合穷举
  5. 时间稳定性严格验证（滚动窗口）

用法：
    python -m day_week_predicted.backtest.five_strategy_deep_v5
"""
import sys
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
    calc_ma, calc_ema, calc_macd, calc_rsi, calc_kdj,
)

HOLD_DAYS = [3, 5, 7]

# ═══════════════════════════════════════════════════════════════════
#  指标预计算 (与v4一致 + ATR)
# ═══════════════════════════════════════════════════════════════════

def precompute(klines):
    n = len(klines)
    if n < 260:
        return None
    c = [float(k['close_price']) for k in klines]
    o = [float(k['open_price']) for k in klines]
    h = [float(k['high_price']) for k in klines]
    l = [float(k['low_price']) for k in klines]
    v = [float(k['trading_volume']) for k in klines]
    ch = [float(k.get('change_hand', 0) or 0) for k in klines]

    ma5 = calc_ma(c, 5)
    ma10 = calc_ma(c, 10)
    ma20 = calc_ma(c, 20)
    ma60 = calc_ma(c, 60)
    ema20 = calc_ema(c, 20)
    ema40 = calc_ema(c, 40)
    ema60 = calc_ema(c, 60)
    vm5 = calc_ma(v, 5)
    vm10 = calc_ma(v, 10)
    vm20 = calc_ma(v, 20)

    ema12 = calc_ema(c, 12)
    ema26 = calc_ema(c, 26)
    dif = [ema12[i] - ema26[i] for i in range(n)]
    dea = calc_ema(dif, 9)
    macd_bar = [2 * (dif[i] - dea[i]) for i in range(n)]

    rsi6 = calc_rsi(c, 6)
    rsi14 = calc_rsi(c, 14)
    kv, dv, jv = calc_kdj(h, l, c)

    boll_up = [0.0] * n
    boll_dn = [0.0] * n
    boll_mid = [0.0] * n
    for i in range(19, n):
        w = c[i - 19:i + 1]
        avg = sum(w) / 20
        std = (sum((x - avg) ** 2 for x in w) / 20) ** 0.5
        boll_mid[i] = avg
        boll_up[i] = avg + 2 * std
        boll_dn[i] = avg - 2 * std

    # ATR(14)
    atr14 = [0.0] * n
    for i in range(1, n):
        tr = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
        if i < 14:
            atr14[i] = tr
        else:
            atr14[i] = (atr14[i - 1] * 13 + tr) / 14

    return {
        'n': n, 'c': c, 'o': o, 'h': h, 'l': l, 'v': v, 'ch': ch,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60,
        'ema20': ema20, 'ema40': ema40, 'ema60': ema60,
        'vm5': vm5, 'vm10': vm10, 'vm20': vm20,
        'dif': dif, 'dea': dea, 'macd_bar': macd_bar,
        'rsi6': rsi6, 'rsi14': rsi14,
        'k': kv, 'd': dv, 'j': jv,
        'boll_up': boll_up, 'boll_dn': boll_dn, 'boll_mid': boll_mid,
        'atr14': atr14,
    }


def market_env(ind, i):
    if i < 60 or ind['ma60'][i] <= 0:
        return 'unk'
    cv = ind['c'][i]
    m60 = ind['ma60'][i]
    m60p = ind['ma60'][max(0, i - 20)]
    slope = (m60 - m60p) / m60p if m60p > 0 else 0
    if cv > m60 and slope > 0.02:
        return 'bull'
    elif cv < m60 and slope < -0.02:
        return 'bear'
    return 'range'


# ═══════════════════════════════════════════════════════════════════
#  空中加油参数化版本 — 用于敏感性分析
# ═══════════════════════════════════════════════════════════════════

def s6_parametric(ind, i, code, conv_thresh=0.02, shrink_days=2, dea_min=0.0):
    """
    空中加油参数化版本
    conv_thresh: DIF-DEA收敛阈值 (原版0.02)
    shrink_days: 红柱连续缩短天数 (原版2)
    dea_min: DEA最低阈值 (原版0, 即>0)
    """
    if i < shrink_days + 2:
        return False
    dif = ind['dif']
    dea = ind['dea']
    bar = ind['macd_bar']

    # 前一日收敛
    if dif[i - 1] - dea[i - 1] > conv_thresh:
        return False
    # DIF > DEA (金叉状态)
    if dif[i - 1] <= dea[i - 1]:
        return False
    # DEA > dea_min
    if dea[i - 1] <= dea_min:
        return False
    # DEA上升
    if dea[i - 1] <= dea[i - 2]:
        return False

    # DIF连续下降 shrink_days 天
    for d in range(1, shrink_days + 1):
        if i - d - 1 < 0:
            return False
        if dif[i - d] >= dif[i - d - 1]:
            return False

    # 红柱连续缩短 shrink_days 天
    for d in range(1, shrink_days + 1):
        if i - d - 1 < 0:
            return False
        if bar[i - d] >= bar[i - d - 1]:
            return False

    # 当日信号：DIF回升 + 红柱放大
    if dif[i] < dif[i - 1]:
        return False
    if bar[i] <= bar[i - 1]:
        return False

    return True


def s6_strict(ind, i, code):
    """v4原版严格空中加油"""
    return s6_parametric(ind, i, code, conv_thresh=0.02, shrink_days=2, dea_min=0.0)


def s6_loose(ind, i, code):
    """v4原版宽松空中加油"""
    if i < 3:
        return False
    dif, dea, bar = ind['dif'], ind['dea'], ind['macd_bar']
    if dif[i - 1] - dea[i - 1] > 0.05:
        return False
    if dif[i - 1] > dif[i - 2]:
        return False
    if dif[i - 1] <= dea[i - 1]:
        return False
    if dea[i - 1] <= 0:
        return False
    if bar[i - 1] >= bar[i - 2]:
        return False
    if dif[i] < dif[i - 1]:
        return False
    if bar[i] <= bar[i - 1]:
        return False
    return True


# ═══════════════════════════════════════════════════════════════════
#  空中加油变体矩阵 — 参数敏感性
# ═══════════════════════════════════════════════════════════════════

S6_VARIANTS = {}

# 收敛阈值敏感性
for ct in [0.01, 0.02, 0.03, 0.04, 0.05, 0.08, 0.10]:
    label = f'S6_ct{ct}'
    ct_val = ct
    S6_VARIANTS[label] = lambda ind, i, code, _ct=ct_val: s6_parametric(ind, i, code, conv_thresh=_ct, shrink_days=2)

# 缩短天数敏感性
for sd in [1, 2, 3]:
    label = f'S6_sd{sd}'
    sd_val = sd
    S6_VARIANTS[label] = lambda ind, i, code, _sd=sd_val: s6_parametric(ind, i, code, conv_thresh=0.02, shrink_days=_sd)

# DEA阈值敏感性
for dm in [0.0, 0.01, 0.02, 0.05]:
    label = f'S6_dea{dm}'
    dm_val = dm
    S6_VARIANTS[label] = lambda ind, i, code, _dm=dm_val: s6_parametric(ind, i, code, conv_thresh=0.02, dea_min=_dm)

# 最佳组合候选
S6_VARIANTS['S6_ct03_sd1'] = lambda ind, i, code: s6_parametric(ind, i, code, conv_thresh=0.03, shrink_days=1)
S6_VARIANTS['S6_ct04_sd1'] = lambda ind, i, code: s6_parametric(ind, i, code, conv_thresh=0.04, shrink_days=1)
S6_VARIANTS['S6_ct05_sd1'] = lambda ind, i, code: s6_parametric(ind, i, code, conv_thresh=0.05, shrink_days=1)
S6_VARIANTS['S6_ct03_sd2'] = lambda ind, i, code: s6_parametric(ind, i, code, conv_thresh=0.03, shrink_days=2)
S6_VARIANTS['S6_ct05_sd2'] = lambda ind, i, code: s6_parametric(ind, i, code, conv_thresh=0.05, shrink_days=2)


# ═══════════════════════════════════════════════════════════════════
#  v3/v4最优策略复用
# ═══════════════════════════════════════════════════════════════════

def combo_C_qingting_boll(ind, i, code):
    """蜻蜓点水宽+布林收窄+布林下轨 (v3冠军 58.2%)"""
    if i < 60:
        return False
    cv, ov, hv, lv = ind['c'][i], ind['o'][i], ind['h'][i], ind['l'][i]
    v = ind['v']
    ma20v, ma60v = ind['ma20'][i], ind['ma60'][i]
    if ma20v <= 0 and ma60v <= 0:
        return False
    hui_20 = ma20v > 0 and abs(lv - ma20v) / ma20v <= 0.05 and cv > ma20v * 0.99
    hui_60 = ma60v > 0 and abs(lv - ma60v) / ma60v <= 0.05 and cv > ma60v * 0.99
    if not (hui_20 or hui_60):
        return False
    amp = hv - lv
    if amp <= 0:
        return False
    if (min(ov, cv) - lv) / amp <= 0.20:
        return False
    if (cv - ov) / max(ov, 0.01) <= -0.02:
        return False
    if i < 1 or v[i - 1] <= 0 or v[i] < v[i - 1] * 0.8:
        return False
    mid = ind['ma20'][i]
    if mid <= 0:
        return False
    width = (ind['boll_up'][i] - ind['boll_dn'][i]) / mid
    if width >= 0.10:
        return False
    dn = ind['boll_dn'][i]
    if dn <= 0 or cv > (mid + dn) / 2:
        return False
    return True


def boll_rsi_filter(ind, i):
    """布林收窄+布林下轨+RSI适中"""
    if i < 20:
        return False
    mid = ind['ma20'][i]
    if mid <= 0:
        return False
    width = (ind['boll_up'][i] - ind['boll_dn'][i]) / mid
    if width >= 0.10:
        return False
    dn = ind['boll_dn'][i]
    if dn <= 0 or ind['c'][i] > (mid + dn) / 2:
        return False
    if not (40 <= ind['rsi14'][i] <= 70):
        return False
    return True


def combo_F_loose_with_filter(ind, i, code):
    """5策略宽松OR + 布林三重过滤 (v3亚军 58.0%)"""
    if not boll_rsi_filter(ind, i):
        return False
    c, o, v = ind['c'], ind['o'], ind['v']
    # S1L 暴力洗盘宽
    if i >= 60 and i >= 1 and c[i - 1] > 0:
        drop = (c[i] - c[i - 1]) / c[i - 1] * 100
        if drop < -3.0:
            has_zt = any(c[j] / c[j - 1] >= 0.095 for j in range(max(1, i - 12), i + 1) if c[j - 1] > 0)
            if has_zt and ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0:
                return True
    # S2L 堆量挖坑宽
    if i >= 60 and not code.startswith('688'):
        vm5 = ind['vm5']
        if vm5[i] > 0 and v[i] > vm5[i] * 1.1 and i >= 5 and c[i - 5] > 0:
            ma10v = ind['ma10'][i]
            ma60v = ind['ma60'][i]
            if ma10v > 0 and c[i] >= ma10v * 0.98 and ma60v > 0 and c[i] > ma60v * 0.95:
                return True
    # S3L 蜻蜓点水宽
    if i >= 60:
        cv, ov, hv, lv = c[i], o[i], ind['h'][i], ind['l'][i]
        ma20v, ma60v = ind['ma20'][i], ind['ma60'][i]
        hui = False
        if ma20v > 0 and abs(lv - ma20v) / ma20v <= 0.05 and cv > ma20v * 0.99:
            hui = True
        if ma60v > 0 and abs(lv - ma60v) / ma60v <= 0.05 and cv > ma60v * 0.99:
            hui = True
        if hui:
            amp = hv - lv
            if amp > 0 and (min(ov, cv) - lv) / amp > 0.20:
                if (cv - ov) / max(ov, 0.01) > -0.02:
                    if i >= 1 and v[i - 1] > 0 and v[i] >= v[i - 1] * 0.8:
                        return True
    # S4L 出水芙蓉宽
    if i >= 60:
        cv, ov = c[i], o[i]
        e20, e40, e60 = ind['ema20'][i], ind['ema40'][i], ind['ema60'][i]
        vm10 = ind['vm10'][i]
        if e60 > 0 and e20 > 0 and vm10 > 0:
            if ov < max(e20, e40, e60):
                above = sum(1 for e in [e20, e40, e60] if cv > e)
                if above >= 2 and v[i] / vm10 > 1.0:
                    if i >= 1 and c[i - 1] > 0 and cv / c[i - 1] > 1.03:
                        max_e = max(e20, e40, e60)
                        min_e = min(e20, e40, e60)
                        if min_e > 0 and max_e / min_e < 1.15 and ind['dif'][i] > ind['dea'][i]:
                            return True
    # S5L 店大欺客宽
    if i >= 20 and not code.startswith('688'):
        if v[i] > 1:
            zt = sum(1 for j in range(max(1, i - 6), i + 1) if c[j - 1] > 0 and c[j] / c[j - 1] >= 1.095)
            if zt >= 1 and c[i] < o[i]:
                yang = sum(1 for j in range(max(0, i - 4), i + 1) if c[j] >= o[j])
                if yang >= 2:
                    vm20 = ind['vm20'][i]
                    if vm20 > 0 and v[i] / vm20 < 3.5 and c[i] > ind['ma5'][i] * 0.97:
                        return True
    # S6 空中加油
    if s6_strict(ind, i, code):
        return True
    if s6_loose(ind, i, code):
        return True
    return False


def shipanxian(ind, i, code):
    """试盘线"""
    if i < 50:
        return False
    cv, ov, hv, lv = ind['c'][i], ind['o'][i], ind['h'][i], ind['l'][i]
    vv = ind['v'][i]
    pc, pv = ind['c'][i - 1], ind['v'][i - 1]
    if cv <= 0 or pc <= 0 or pv <= 0:
        return False
    if (hv - max(ov, cv)) / cv <= 0.025:
        return False
    if (hv - pc) / pc <= 0.07:
        return False
    if (cv - pc) / pc <= 0.03:
        return False
    llv50 = min(ind['l'][j] for j in range(i - 49, i + 1))
    if llv50 <= 0 or cv / llv50 >= 1.4:
        return False
    if cv < max(ind['c'][j] for j in range(i - 4, i + 1)):
        return False
    if vv / pv <= 2:
        return False
    if vv < max(ind['v'][j] for j in range(i - 4, i + 1)):
        return False
    llv20 = min(ind['l'][j] for j in range(i - 19, i + 1))
    if llv20 <= 0 or lv / llv20 >= 1.2:
        return False
    hhv20 = max(ind['h'][j] for j in range(i - 19, i + 1))
    if hhv20 <= 0 or lv / hhv20 <= 0.9:
        return False
    return True


# ═══════════════════════════════════════════════════════════════════
#  过滤器 (扩展版)
# ═══════════════════════════════════════════════════════════════════

FILTERS = {}

def _reg(name):
    def dec(f):
        FILTERS[name] = f
        return f
    return dec

@_reg('F_均线多头')
def _(ind, i): return ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0

@_reg('F_大趋势')
def _(ind, i): return ind['ma20'][i] > ind['ma60'][i] > 0 and ind['c'][i] > ind['ma60'][i]

@_reg('F_MACD多头')
def _(ind, i): return ind['dif'][i] > ind['dea'][i]

@_reg('F_KDJ金叉')
def _(ind, i): return ind['k'][i] > ind['d'][i] and ind['k'][i] < 80

@_reg('F_RSI适中')
def _(ind, i): return 40 <= ind['rsi14'][i] <= 70

@_reg('F_布林收窄')
def _(ind, i):
    mid = ind['ma20'][i]
    if mid <= 0: return False
    return (ind['boll_up'][i] - ind['boll_dn'][i]) / mid < 0.10

@_reg('F_布林下轨')
def _(ind, i):
    mid = ind['ma20'][i]
    dn = ind['boll_dn'][i]
    if mid <= 0 or dn <= 0: return False
    return ind['c'][i] <= (mid + dn) / 2

@_reg('F_放量')
def _(ind, i): return ind['vm5'][i] > 0 and ind['v'][i] > ind['vm5'][i] * 1.2

@_reg('F_缩量')
def _(ind, i): return ind['vm5'][i] > 0 and ind['v'][i] < ind['vm5'][i] * 0.8

@_reg('F_收阳')
def _(ind, i): return ind['c'][i] > ind['o'][i]

@_reg('F_DEA>0')
def _(ind, i): return ind['dea'][i] > 0

@_reg('F_ATR低波')
def _(ind, i):
    """ATR/价格 < 3% — 低波动环境"""
    if ind['c'][i] <= 0 or ind['atr14'][i] <= 0:
        return False
    return ind['atr14'][i] / ind['c'][i] < 0.03

@_reg('F_ATR高波')
def _(ind, i):
    """ATR/价格 > 4% — 高波动环境"""
    if ind['c'][i] <= 0 or ind['atr14'][i] <= 0:
        return False
    return ind['atr14'][i] / ind['c'][i] > 0.04

@_reg('F_量比温和')
def _(ind, i):
    """量比0.8~1.5 — 温和量能"""
    if ind['vm5'][i] <= 0:
        return False
    ratio = ind['v'][i] / ind['vm5'][i]
    return 0.8 <= ratio <= 1.5

@_reg('F_近期不跌')
def _(ind, i):
    """5日涨幅 > -2%"""
    if i < 5 or ind['c'][i - 5] <= 0:
        return False
    return (ind['c'][i] - ind['c'][i - 5]) / ind['c'][i - 5] > -0.02

@_reg('F_换手适中')
def _(ind, i):
    """换手率1%~8%"""
    return 1.0 <= ind['ch'][i] <= 8.0


# ═══════════════════════════════════════════════════════════════════
#  核心策略注册
# ═══════════════════════════════════════════════════════════════════

CORE_STRATEGIES = {
    'S6_严格': s6_strict,
    'S6_宽松': s6_loose,
    'C_蜻蜓布林': combo_C_qingting_boll,
    'F_宽松+过滤': combo_F_loose_with_filter,
    'S0_试盘线': shipanxian,
}


# ═══════════════════════════════════════════════════════════════════
#  加权投票系统
# ═══════════════════════════════════════════════════════════════════

# 权重基于v4回测胜率
VOTE_WEIGHTS = {
    'S6_严格': 1.5,       # 51.8% 基础
    'S6_宽松': 0.8,       # 宽松版权重低
    'C_蜻蜓布林': 2.0,    # 58.2% 冠军
    'F_宽松+过滤': 1.8,   # 58.0% 亚军
    'S0_试盘线': 1.0,     # 参考
    # 过滤器加分
    'F_缩量': 1.5,        # v4发现缩量是S6最佳搭档
    'F_布林收窄': 1.2,
    'F_布林下轨': 1.0,
    'F_RSI适中': 0.8,
    'F_均线多头': 0.5,
    'F_大趋势': 0.5,
}


def calc_vote_score(sig):
    """计算加权投票分数"""
    score = 0.0
    for key, weight in VOTE_WEIGHTS.items():
        if key.startswith('F_'):
            if sig.get(f'f_{key}', False):
                score += weight
        else:
            if sig.get(f'h_{key}', False):
                score += weight
    return round(score, 2)


# ═══════════════════════════════════════════════════════════════════
#  回测核心
# ═══════════════════════════════════════════════════════════════════

def backtest_stock(stock_code):
    import time
    for attempt in range(3):
        try:
            klines = get_kline_data(stock_code, limit=500)
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
            else:
                return []
    if len(klines) < 260:
        return []
    ind = precompute(klines)
    if ind is None:
        return []

    signals = []
    n = ind['n']

    for i in range(250, n):
        # 核心策略
        hits = {}
        for sname, sfunc in CORE_STRATEGIES.items():
            try:
                hits[sname] = sfunc(ind, i, stock_code)
            except Exception:
                hits[sname] = False

        # S6参数变体
        s6v_hits = {}
        for vname, vfunc in S6_VARIANTS.items():
            try:
                s6v_hits[vname] = vfunc(ind, i, stock_code)
            except Exception:
                s6v_hits[vname] = False

        # 至少一个策略或变体触发
        if not any(hits.values()) and not any(s6v_hits.values()):
            continue

        if i + 1 >= n:
            continue
        bp = float(klines[i + 1]['open_price'])
        if bp <= 0:
            continue

        sig = {
            'code': stock_code,
            'date': str(klines[i]['date']),
            'buy': bp,
            'env': market_env(ind, i),
        }

        for hd in HOLD_DAYS:
            si = i + 1 + hd
            if si < n:
                sp = float(klines[si]['close_price'])
                sig[f'r{hd}'] = round((sp - bp) / bp * 100, 2)

        for sname, hit in hits.items():
            sig[f'h_{sname}'] = hit

        for vname, hit in s6v_hits.items():
            sig[f'v_{vname}'] = hit

        for fname, ffunc in FILTERS.items():
            try:
                sig[f'f_{fname}'] = ffunc(ind, i)
            except Exception:
                sig[f'f_{fname}'] = False

        sig['vote_score'] = calc_vote_score(sig)

        signals.append(sig)

    return signals


def qs(sigs, hd=5):
    key = f'r{hd}'
    rets = [s[key] for s in sigs if key in s]
    if not rets:
        return {'n': 0, 'wr': 0, 'avg': 0, 'med': 0}
    w = sum(1 for r in rets if r > 0)
    t = len(rets)
    sr = sorted(rets)
    return {
        'n': t, 'wr': round(w / t * 100, 1),
        'avg': round(sum(rets) / t, 2),
        'med': round(sr[t // 2], 2),
    }


def cv_quarters(sigs, hd=5):
    key = f'r{hd}'
    filtered = [s for s in sigs if key in s]
    if not filtered:
        return []
    quarters = defaultdict(list)
    for s in filtered:
        d = s['date']
        y, m = d[:4], int(d[5:7])
        q = f"{y}Q{(m - 1) // 3 + 1}"
        quarters[q].append(s[key])
    result = []
    for q in sorted(quarters):
        rets = quarters[q]
        wr = sum(1 for r in rets if r > 0) / len(rets) * 100
        avg = sum(rets) / len(rets)
        result.append({'q': q, 'n': len(rets), 'wr': round(wr, 1), 'avg': round(avg, 2)})
    return result


# ═══════════════════════════════════════════════════════════════════
#  主回测
# ═══════════════════════════════════════════════════════════════════

def run(sample_limit=1000):
    t0 = datetime.now()
    logger.info("=" * 90)
    logger.info("  6大策略回测 v5 — 自主深度优化")
    logger.info("  样本: %d | 持有: %s", sample_limit, HOLD_DAYS)
    logger.info("=" * 90)

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

    # ═══ PART 1: 空中加油参数敏感性分析 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [1] 空中加油参数敏感性分析")
    logger.info("=" * 90)

    # 1a. 收敛阈值
    logger.info("")
    logger.info("  ── 收敛阈值(conv_thresh)敏感性 ──")
    logger.info("    %-12s %6s  %7s  %7s  %7s", "阈值", "信号", "5d胜率", "5d均收", "5d中位")
    ct_results = []
    for ct in [0.01, 0.02, 0.03, 0.04, 0.05, 0.08, 0.10]:
        vname = f'S6_ct{ct}'
        filtered = [s for s in all_sigs if s.get(f'v_{vname}', False)]
        st = qs(filtered, 5)
        if st['n'] > 0:
            logger.info("    ct=%-7s %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                         ct, st['n'], st['wr'], st['avg'], st['med'])
            ct_results.append((ct, st))

    # 1b. 缩短天数
    logger.info("")
    logger.info("  ── 缩短天数(shrink_days)敏感性 ──")
    for sd in [1, 2, 3]:
        vname = f'S6_sd{sd}'
        filtered = [s for s in all_sigs if s.get(f'v_{vname}', False)]
        st = qs(filtered, 5)
        if st['n'] > 0:
            logger.info("    sd=%-7d %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                         sd, st['n'], st['wr'], st['avg'], st['med'])

    # 1c. DEA阈值
    logger.info("")
    logger.info("  ── DEA最低阈值敏感性 ──")
    for dm in [0.0, 0.01, 0.02, 0.05]:
        vname = f'S6_dea{dm}'
        filtered = [s for s in all_sigs if s.get(f'v_{vname}', False)]
        st = qs(filtered, 5)
        if st['n'] > 0:
            logger.info("    dea>%-6s %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                         dm, st['n'], st['wr'], st['avg'], st['med'])

    # 1d. 最佳参数组合
    logger.info("")
    logger.info("  ── 参数组合候选 ──")
    logger.info("    %-18s %6s  %7s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收", "5d中位")
    param_combos = ['S6_ct03_sd1', 'S6_ct04_sd1', 'S6_ct05_sd1', 'S6_ct03_sd2', 'S6_ct05_sd2']
    best_s6_variant = None
    best_s6_wr = 0
    for vname in param_combos:
        filtered = [s for s in all_sigs if s.get(f'v_{vname}', False)]
        st = qs(filtered, 5)
        if st['n'] > 0:
            logger.info("    %-18s %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                         vname, st['n'], st['wr'], st['avg'], st['med'])
            if st['n'] >= 50 and st['wr'] > best_s6_wr:
                best_s6_wr = st['wr']
                best_s6_variant = vname

    # ═══ PART 2: 核心策略独立表现 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [2] 核心策略独立表现")
    logger.info("=" * 90)
    logger.info("  %-22s %6s  %7s  %7s │ %7s  %7s  %7s",
                "策略", "信号", "3d胜率", "3d均收", "5d胜率", "5d均收", "7d胜率")

    strat_perf = {}
    for sname in CORE_STRATEGIES:
        filtered = [s for s in all_sigs if s.get(f'h_{sname}', False)]
        s3, s5, s7 = qs(filtered, 3), qs(filtered, 5), qs(filtered, 7)
        strat_perf[sname] = {'s3': s3, 's5': s5, 's7': s7, 'sigs': filtered}
        if s5['n'] > 0:
            logger.info("  %-22s %6d  %6.1f%%  %6.2f%% │ %6.1f%%  %6.2f%%  %6.1f%%",
                         sname, s5['n'],
                         s3['wr'], s3['avg'],
                         s5['wr'], s5['avg'], s7['wr'])

    # ═══ PART 3: S6+缩量深度验证 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [3] S6+缩量深度验证 (v4发现65.7%/70信号)")
    logger.info("=" * 90)

    s6_strict_sigs = strat_perf['S6_严格']['sigs']

    # 3a. 缩量阈值敏感性
    logger.info("")
    logger.info("  ── 缩量阈值敏感性 (V/VM5 < X) ──")
    logger.info("    %-12s %6s  %7s  %7s  %7s", "阈值", "信号", "5d胜率", "5d均收", "5d中位")
    for thresh in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
        filtered = [s for s in s6_strict_sigs
                    if s.get('f_F_缩量', False) or
                    (True)]  # placeholder
        # 直接用原始数据重新过滤
        filtered = []
        for s in all_sigs:
            if not s.get('h_S6_严格', False):
                continue
            # 需要从信号中推断量比 — 用f_F_缩量近似
            # 但我们需要更精细的阈值，所以用所有S6信号
            filtered.append(s)
        # 这里我们只能用已有的过滤器标记
        break  # 跳过，改用下面的方法

    # 重新设计：用S6各变体+各缩量级别的组合
    logger.info("")
    logger.info("  ── S6严格 + 各过滤器 TOP效果 ──")
    logger.info("    %-20s %6s  %7s  %7s  %7s", "过滤器", "信号", "5d胜率", "5d均收", "5d中位")
    s6_filter_results = []
    for fname in FILTERS:
        filtered = [s for s in s6_strict_sigs if s.get(f'f_{fname}', False)]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            s6_filter_results.append((fname, st))
    s6_filter_results.sort(key=lambda x: x[1]['wr'], reverse=True)
    for fname, st in s6_filter_results:
        logger.info("    %-20s %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                     fname, st['n'], st['wr'], st['avg'], st['med'])

    # 3b. S6严格 + 双过滤器穷举
    logger.info("")
    logger.info("  ── S6严格 + 双过滤器穷举 TOP15 ──")
    logger.info("    %-35s %5s  %7s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收", "5d中位")
    fnames = list(FILTERS.keys())
    combo2_results = []
    for a, b in combinations(fnames, 2):
        filtered = [s for s in s6_strict_sigs
                    if s.get(f'f_{a}', False) and s.get(f'f_{b}', False)]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            combo2_results.append((f"{a}+{b}", st))
    combo2_results.sort(key=lambda x: x[1]['wr'], reverse=True)
    for label, st in combo2_results[:15]:
        logger.info("    %-35s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                     label, st['n'], st['wr'], st['avg'], st['med'])

    # 3c. S6严格 + 三过滤器穷举 (只测TOP过滤器)
    logger.info("")
    logger.info("  ── S6严格 + 三过滤器穷举 TOP10 ──")
    top_filters = [f for f, _ in s6_filter_results[:8]]  # 取TOP8过滤器
    combo3_results = []
    for a, b, c_f in combinations(top_filters, 3):
        filtered = [s for s in s6_strict_sigs
                    if s.get(f'f_{a}', False) and s.get(f'f_{b}', False) and s.get(f'f_{c_f}', False)]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            combo3_results.append((f"{a}+{b}+{c_f}", st))
    combo3_results.sort(key=lambda x: x[1]['wr'], reverse=True)
    for label, st in combo3_results[:10]:
        logger.info("    %-50s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                     label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 4: 加权投票系统 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [4] 加权投票系统 — 分数阈值分析")
    logger.info("=" * 90)
    logger.info("    %-15s %6s  %7s  %7s  %7s", "分数阈值", "信号", "5d胜率", "5d均收", "5d中位")

    vote_results = []
    for threshold in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 6.0, 7.0, 8.0]:
        filtered = [s for s in all_sigs if s.get('vote_score', 0) >= threshold]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            logger.info("    score>=%-5s %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                         threshold, st['n'], st['wr'], st['avg'], st['med'])
            vote_results.append((threshold, st))

    # 投票+市场环境
    logger.info("")
    logger.info("  ── 投票分数 × 市场环境 ──")
    for threshold in [3.0, 4.0, 5.0]:
        for env in ['bull', 'range', 'bear']:
            filtered = [s for s in all_sigs
                        if s.get('vote_score', 0) >= threshold and s.get('env') == env]
            st = qs(filtered, 5)
            if st['n'] >= 5:
                logger.info("    score>=%.1f + %-5s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                             threshold, env, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 5: 最优S6变体 + v3最优策略 交叉组合 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [5] 最优S6变体 × v3策略 × 过滤器 交叉组合")
    logger.info("=" * 90)

    # 找出S6参数变体中表现最好的
    best_variants = []
    for vname in S6_VARIANTS:
        filtered = [s for s in all_sigs if s.get(f'v_{vname}', False)]
        st = qs(filtered, 5)
        if st['n'] >= 30:
            best_variants.append((vname, st))
    best_variants.sort(key=lambda x: x[1]['wr'], reverse=True)

    logger.info("  ── S6参数变体排名 (信号>=30) ──")
    logger.info("    %-18s %6s  %7s  %7s", "变体", "信号", "5d胜率", "5d均收")
    for vname, st in best_variants[:10]:
        logger.info("    %-18s %6d  %6.1f%%  %6.2f%%", vname, st['n'], st['wr'], st['avg'])

    # 最优S6变体 OR 蜻蜓布林
    logger.info("")
    logger.info("  ── 最优S6变体 OR v3策略 ──")
    logger.info("    %-45s %5s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收")
    or_cross = []
    for vname, vst in best_variants[:5]:
        # OR 蜻蜓布林
        filtered = [s for s in all_sigs
                    if s.get(f'v_{vname}', False) or s.get('h_C_蜻蜓布林', False)]
        st = qs(filtered, 5)
        label = f'{vname} OR 蜻蜓布林'
        or_cross.append((label, st))
        if st['n'] > 0:
            logger.info("    %-45s %5d  %6.1f%%  %6.2f%%", label, st['n'], st['wr'], st['avg'])

        # OR 宽松+过滤
        filtered = [s for s in all_sigs
                    if s.get(f'v_{vname}', False) or s.get('h_F_宽松+过滤', False)]
        st = qs(filtered, 5)
        label = f'{vname} OR 宽松+过滤'
        or_cross.append((label, st))
        if st['n'] > 0:
            logger.info("    %-45s %5d  %6.1f%%  %6.2f%%", label, st['n'], st['wr'], st['avg'])

    # ═══ PART 6: 蜻蜓布林 + 额外过滤器增强 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [6] 蜻蜓布林(58.2%) + 额外过滤器增强")
    logger.info("=" * 90)

    qt_sigs = strat_perf['C_蜻蜓布林']['sigs']
    qt_base = qs(qt_sigs, 5)
    logger.info("  基准: 胜率%.1f%%, 均收%.2f%%, %d信号", qt_base['wr'], qt_base['avg'], qt_base['n'])
    logger.info("")
    logger.info("    %-20s %6s  %7s(%6s)  %7s", "过滤器", "信号", "5d胜率", "提升", "5d均收")

    qt_filter_results = []
    for fname in FILTERS:
        filtered = [s for s in qt_sigs if s.get(f'f_{fname}', False)]
        st = qs(filtered, 5)
        if st['n'] >= 10:
            qt_filter_results.append((fname, st, st['wr'] - qt_base['wr']))
    qt_filter_results.sort(key=lambda x: x[2], reverse=True)
    for fname, st, wr_d in qt_filter_results:
        logger.info("    %-20s %6d  %6.1f%%(%+5.1f)  %6.2f%%",
                     fname, st['n'], st['wr'], wr_d, st['avg'])

    # 蜻蜓布林 + 双过滤器
    logger.info("")
    logger.info("  ── 蜻蜓布林 + 双过滤器 TOP10 ──")
    qt_combo2 = []
    for a, b in combinations(fnames, 2):
        filtered = [s for s in qt_sigs
                    if s.get(f'f_{a}', False) and s.get(f'f_{b}', False)]
        st = qs(filtered, 5)
        if st['n'] >= 8:
            qt_combo2.append((f"{a}+{b}", st))
    qt_combo2.sort(key=lambda x: x[1]['wr'], reverse=True)
    for label, st in qt_combo2[:10]:
        logger.info("    %-40s %5d  %6.1f%%  %6.2f%%",
                     label, st['n'], st['wr'], st['avg'])

    # ═══ PART 7: 综合排名 + 时间稳定性 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [7] 综合排名 + 时间稳定性验证")
    logger.info("=" * 90)

    all_tested = []

    # 核心策略
    for sname in CORE_STRATEGIES:
        st = strat_perf[sname]['s5']
        if st['n'] >= 5:
            all_tested.append((sname, st['n'], st['wr'], st['avg'], st['med'], strat_perf[sname]['sigs']))

    # S6严格+单过滤器
    for fname, st in s6_filter_results:
        if st['n'] >= 5:
            sigs = [s for s in s6_strict_sigs if s.get(f'f_{fname}', False)]
            all_tested.append((f'S6严格+{fname}', st['n'], st['wr'], st['avg'], st['med'], sigs))

    # S6严格+双过滤器 TOP5
    for label, st in combo2_results[:5]:
        if st['n'] >= 5:
            parts = label.split('+')
            sigs = [s for s in s6_strict_sigs
                    if all(s.get(f'f_{p}', False) for p in parts)]
            all_tested.append((f'S6严格+{label}', st['n'], st['wr'], st['avg'], st['med'], sigs))

    # S6严格+三过滤器 TOP3
    for label, st in combo3_results[:3]:
        if st['n'] >= 5:
            parts = label.split('+')
            sigs = [s for s in s6_strict_sigs
                    if all(s.get(f'f_{p}', False) for p in parts)]
            all_tested.append((f'S6严格+{label}', st['n'], st['wr'], st['avg'], st['med'], sigs))

    # 蜻蜓布林+过滤器 TOP3
    for fname, st, _ in qt_filter_results[:3]:
        if st['n'] >= 8:
            sigs = [s for s in qt_sigs if s.get(f'f_{fname}', False)]
            all_tested.append((f'蜻蜓布林+{fname}', st['n'], st['wr'], st['avg'], st['med'], sigs))

    # 蜻蜓布林+双过滤器 TOP3
    for label, st in qt_combo2[:3]:
        if st['n'] >= 8:
            parts = label.split('+')
            sigs = [s for s in qt_sigs if all(s.get(f'f_{p}', False) for p in parts)]
            all_tested.append((f'蜻蜓布林+{label}', st['n'], st['wr'], st['avg'], st['med'], sigs))

    # 投票系统
    for threshold, st in vote_results:
        if st['n'] >= 10:
            sigs = [s for s in all_sigs if s.get('vote_score', 0) >= threshold]
            all_tested.append((f'投票>={threshold}', st['n'], st['wr'], st['avg'], st['med'], sigs))

    # OR组合
    or_combos_def = [
        ('S6严 OR 蜻蜓布林', ['h_S6_严格', 'h_C_蜻蜓布林']),
        ('S6严 OR 宽松+过滤', ['h_S6_严格', 'h_F_宽松+过滤']),
        ('S6宽 OR 蜻蜓布林', ['h_S6_宽松', 'h_C_蜻蜓布林']),
        ('S6宽 OR 宽松+过滤', ['h_S6_宽松', 'h_F_宽松+过滤']),
        ('S6严 OR 蜻蜓布林 OR 试盘线', ['h_S6_严格', 'h_C_蜻蜓布林', 'h_S0_试盘线']),
    ]
    for label, keys in or_combos_def:
        filtered = [s for s in all_sigs if any(s.get(k, False) for k in keys)]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            all_tested.append((label, st['n'], st['wr'], st['avg'], st['med'], filtered))

    # 排序
    all_tested.sort(key=lambda x: x[2], reverse=True)

    # 打印排名 + CV
    logger.info("")
    logger.info("  %-55s %5s  %7s  %7s  %7s  %s",
                "组合", "信号", "5d胜率", "5d均收", "5d中位", "季度稳定性")
    logger.info("  " + "-" * 100)

    rankings_for_save = []
    for label, n, wr, avg, med, sigs in all_tested[:30]:
        folds = cv_quarters(sigs, 5)
        q_wrs = [f['wr'] for f in folds]
        above50 = sum(1 for w in q_wrs if w > 50) if q_wrs else 0
        total_q = len(q_wrs)
        stability = f"{above50}/{total_q}" if total_q > 0 else "N/A"
        avg_qwr = round(sum(q_wrs) / len(q_wrs), 1) if q_wrs else 0

        logger.info("  %-55s %5d  %6.1f%%  %6.2f%%  %6.2f%%  %s (均%.1f%%)",
                     label, n, wr, avg, med, stability, avg_qwr)

        rankings_for_save.append({
            'label': label, 'n': n, 'wr': wr, 'avg': avg, 'med': med,
            'stability': stability, 'avg_quarter_wr': avg_qwr,
            'quarters': folds,
        })

    # ═══ PART 8: TOP5 详细季度CV ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [8] TOP5 详细季度CV")
    logger.info("=" * 90)

    for label, n, wr, avg, med, sigs in all_tested[:5]:
        folds = cv_quarters(sigs, 5)
        if not folds:
            continue
        logger.info("")
        logger.info("  ── %s (总胜率%.1f%%, %d信号) ──", label, wr, n)
        logger.info("    %-8s %5s  %7s  %7s", "季度", "信号", "胜率", "均收益")
        for f in folds:
            marker = " ★" if f['wr'] >= 55 else (" ▲" if f['wr'] >= 50 else " ▼")
            logger.info("    %-8s %5d  %6.1f%%%s  %6.2f%%",
                         f['q'], f['n'], f['wr'], marker, f['avg'])

    # ═══ PART 9: 最终推荐 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [9] 最终推荐")
    logger.info("=" * 90)

    # 高胜率 (>=60%, >=10信号)
    high_wr = [x for x in all_tested if x[2] >= 60 and x[1] >= 10]
    if high_wr:
        best = high_wr[0]
        logger.info("  🏆 高胜率推荐: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     best[0], best[2], best[3], best[1])

    # 均衡型 (>=55%, >=100信号)
    balanced = [x for x in all_tested if x[1] >= 100 and x[2] >= 55]
    if balanced:
        best = max(balanced, key=lambda x: x[2] * 0.6 + x[3] * 10)
        logger.info("  ⚖️ 均衡推荐: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     best[0], best[2], best[3], best[1])

    # 大样本 (>=500信号, >=52%)
    large = [x for x in all_tested if x[1] >= 500 and x[2] >= 52]
    if large:
        best = max(large, key=lambda x: x[2])
        logger.info("  📊 大样本推荐: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     best[0], best[2], best[3], best[1])

    # 最稳定 (最多季度>50%)
    stable_candidates = []
    for label, n, wr, avg, med, sigs in all_tested:
        if n < 50:
            continue
        folds = cv_quarters(sigs, 5)
        q_wrs = [f['wr'] for f in folds]
        if len(q_wrs) >= 4:
            above50 = sum(1 for w in q_wrs if w > 50)
            stable_candidates.append((label, n, wr, avg, above50, len(q_wrs)))
    if stable_candidates:
        stable_candidates.sort(key=lambda x: (x[4] / x[5], x[2]), reverse=True)
        best = stable_candidates[0]
        logger.info("  🛡️ 最稳定推荐: %s (胜率%.1f%%, %d信号, %d/%d季度>50%%)",
                     best[0], best[2], best[1], best[4], best[5])

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 90)

    # 保存结果
    result = {
        'run_time': str(datetime.now()),
        'total_signals': len(all_sigs),
        'stock_count': len(codes),
        'rankings': rankings_for_save[:30],
    }
    try:
        with open('data_results/five_strategy_deep_v5_result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("  结果已保存到 data_results/five_strategy_deep_v5_result.json")
    except Exception as e:
        logger.warning("  保存失败: %s", e)

    return all_sigs


if __name__ == '__main__':
    run(sample_limit=1000)
