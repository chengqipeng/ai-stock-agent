#!/usr/bin/env python3
"""
策略融合回测 v6 — v5最优策略 × TDX通达信公式 深度交叉验证
==========================================================
核心思路:
  v5最优: 蜻蜓布林(58.2%), 蜻蜓布林+OBV+收阳(75.8%), 空中加油+缩量(65.7%)
  TDX有价值条件: 多头排列, 六十向上, 贴线MA5, KD金叉, 刚启动
  TDX打分维度: 均线斜率, 动态贴线, 量价配合, 动量共振, 启动时机

融合方式:
  1. v5策略 + TDX条件作为额外过滤器
  2. v5策略 + TDX打分作为信号质量评分
  3. TDX原版/放宽版 + v5过滤器(布林/OBV/缩量)增强
  4. 全新融合策略: v5形态 AND TDX趋势确认
  5. 统一加权投票: v5策略分 + TDX打分 综合排序

用法:
    python -m day_week_predicted.backtest.five_strategy_deep_v6
"""
import sys
import logging
import json
import time
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
    calc_ma, calc_ema, calc_rsi, calc_kdj,
)

HOLD_DAYS = [3, 5, 7]


# ═══════════════════════════════════════════════════════════════════
#  指标预计算 (v5 + TDX所需指标合并)
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

    rsi14 = calc_rsi(c, 14)
    kv, dv, jv = calc_kdj(h, l, c)

    obv = [0.0] * n
    for i in range(1, n):
        if c[i] > c[i - 1]:
            obv[i] = obv[i - 1] + v[i]
        elif c[i] < c[i - 1]:
            obv[i] = obv[i - 1] - v[i]
        else:
            obv[i] = obv[i - 1]

    boll_up = [0.0] * n
    boll_dn = [0.0] * n
    for i in range(19, n):
        w = c[i - 19:i + 1]
        avg = sum(w) / 20
        std = (sum((x - avg) ** 2 for x in w) / 20) ** 0.5
        boll_up[i] = avg + 2 * std
        boll_dn[i] = avg - 2 * std

    atr14 = [0.0] * n
    for i in range(1, n):
        tr = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
        atr14[i] = tr if i < 14 else (atr14[i - 1] * 13 + tr) / 14

    return {
        'n': n, 'c': c, 'o': o, 'h': h, 'l': l, 'v': v, 'ch': ch,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60,
        'ema20': ema20, 'ema40': ema40, 'ema60': ema60,
        'vm5': vm5, 'vm10': vm10, 'vm20': vm20,
        'dif': dif, 'dea': dea, 'macd_bar': macd_bar,
        'rsi14': rsi14, 'k': kv, 'd': dv, 'j': jv,
        'obv': obv, 'boll_up': boll_up, 'boll_dn': boll_dn,
        'atr14': atr14,
    }


def market_env(ind, i):
    if i < 60 or ind['ma60'][i] <= 0:
        return 'unk'
    cv, m60 = ind['c'][i], ind['ma60'][i]
    m60p = ind['ma60'][max(0, i - 20)]
    slope = (m60 - m60p) / m60p if m60p > 0 else 0
    if cv > m60 and slope > 0.02:
        return 'bull'
    elif cv < m60 and slope < -0.02:
        return 'bear'
    return 'range'


# ═══════════════════════════════════════════════════════════════════
#  v5最优策略 (直接复用)
# ═══════════════════════════════════════════════════════════════════

def s6_strict(ind, i, code):
    """空中加油严格版"""
    if i < 4:
        return False
    dif, dea, bar = ind['dif'], ind['dea'], ind['macd_bar']
    if dif[i-1] - dea[i-1] > 0.02: return False
    if dif[i-1] > dif[i-2]: return False
    if dif[i-2] >= dif[i-3]: return False
    if dif[i-1] <= dea[i-1]: return False
    if dea[i-1] <= 0: return False
    if dea[i-1] <= dea[i-2]: return False
    if bar[i-1] >= bar[i-2]: return False
    if bar[i-2] >= bar[i-3]: return False
    if dif[i] < dif[i-1]: return False
    if bar[i] <= bar[i-1]: return False
    return True


def combo_C_qingting_boll(ind, i, code):
    """蜻蜓布林 (v5冠军58.2%)"""
    if i < 60: return False
    cv, ov, hv, lv = ind['c'][i], ind['o'][i], ind['h'][i], ind['l'][i]
    v = ind['v']
    ma20v, ma60v = ind['ma20'][i], ind['ma60'][i]
    if ma20v <= 0 and ma60v <= 0: return False
    hui_20 = ma20v > 0 and abs(lv - ma20v) / ma20v <= 0.05 and cv > ma20v * 0.99
    hui_60 = ma60v > 0 and abs(lv - ma60v) / ma60v <= 0.05 and cv > ma60v * 0.99
    if not (hui_20 or hui_60): return False
    amp = hv - lv
    if amp <= 0: return False
    if (min(ov, cv) - lv) / amp <= 0.20: return False
    if (cv - ov) / max(ov, 0.01) <= -0.02: return False
    if i < 1 or v[i-1] <= 0 or v[i] < v[i-1] * 0.8: return False
    mid = ind['ma20'][i]
    if mid <= 0: return False
    width = (ind['boll_up'][i] - ind['boll_dn'][i]) / mid
    if width >= 0.10: return False
    dn = ind['boll_dn'][i]
    if dn <= 0 or cv > (mid + dn) / 2: return False
    return True


def shipanxian(ind, i, code):
    """试盘线"""
    if i < 50: return False
    cv, ov, hv, lv = ind['c'][i], ind['o'][i], ind['h'][i], ind['l'][i]
    vv, pc, pv = ind['v'][i], ind['c'][i-1], ind['v'][i-1]
    if cv <= 0 or pc <= 0 or pv <= 0: return False
    if (hv - max(ov, cv)) / cv <= 0.025: return False
    if (hv - pc) / pc <= 0.07: return False
    if (cv - pc) / pc <= 0.03: return False
    llv50 = min(ind['l'][j] for j in range(i-49, i+1))
    if llv50 <= 0 or cv / llv50 >= 1.4: return False
    if cv < max(ind['c'][j] for j in range(i-4, i+1)): return False
    if vv / pv <= 2: return False
    if vv < max(ind['v'][j] for j in range(i-4, i+1)): return False
    llv20 = min(ind['l'][j] for j in range(i-19, i+1))
    if llv20 <= 0 or lv / llv20 >= 1.2: return False
    hhv20 = max(ind['h'][j] for j in range(i-19, i+1))
    if hhv20 <= 0 or lv / hhv20 <= 0.9: return False
    return True


# ═══════════════════════════════════════════════════════════════════
#  TDX通达信条件 (从tdx_formula_backtest提取)
# ═══════════════════════════════════════════════════════════════════

def tdx_bull_align(ind, i):
    """多头排列: MA5>MA10>MA20>MA60"""
    return ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > ind['ma60'][i] > 0

def tdx_ma60_up(ind, i):
    """六十向上: MA60 > REF(MA60,1)"""
    return i >= 1 and ind['ma60'][i] > ind['ma60'][i-1] > 0

def tdx_close_to_ma5(ind, i):
    """贴线: C>MA5 AND C/MA5<1.02"""
    ma5 = ind['ma5'][i]
    return ma5 > 0 and ind['c'][i] > ma5 and ind['c'][i] / ma5 < 1.02

def tdx_kd_cross(ind, i):
    """KD金叉: K上穿D"""
    return i >= 1 and ind['k'][i] > ind['d'][i] and ind['k'][i-1] <= ind['d'][i-1]

def tdx_just_start(ind, i):
    """刚启动: MA5/MA60 < 1.15"""
    return ind['ma60'][i] > 0 and ind['ma5'][i] / ind['ma60'][i] < 1.15

def tdx_profitable(ind, i):
    """盈利近似: 60日涨幅>0"""
    return i >= 60 and ind['c'][i] > ind['c'][i-60]

def tdx_original(ind, i):
    """TDX原版全AND"""
    return (tdx_bull_align(ind, i) and tdx_ma60_up(ind, i)
            and tdx_close_to_ma5(ind, i) and tdx_kd_cross(ind, i)
            and tdx_just_start(ind, i) and ind['c'][i] < 50
            and tdx_profitable(ind, i))

def tdx_relaxed(ind, i):
    """TDX放宽版: 多头+六十向上+贴线+KD金叉"""
    return (tdx_bull_align(ind, i) and tdx_ma60_up(ind, i)
            and tdx_close_to_ma5(ind, i) and tdx_kd_cross(ind, i))


# ═══════════════════════════════════════════════════════════════════
#  TDX打分维度 (从tdx_formula_backtest提取)
# ═══════════════════════════════════════════════════════════════════

def tdx_score_ma_slope(ind, i):
    """均线斜率分 (0/10/20)"""
    if i < 5 or ind['ma60'][i-5] <= 0: return 0
    slope = (ind['ma60'][i] - ind['ma60'][i-5]) / ind['ma60'][i-5] * 100
    if slope > 0.5: return 20
    elif slope > 0: return 10
    return 0

def tdx_score_close_line(ind, i):
    """动态贴线分 (0/10/20) — ATR归一化"""
    ma5, atr = ind['ma5'][i], ind['atr14'][i]
    if ma5 <= 0 or atr <= 0: return 0
    c = ind['c'][i]
    if c <= ma5: return 0
    dev = (c - ma5) / atr
    if dev < 0.3: return 20
    elif dev < 0.8: return 10
    return 0

def tdx_score_just_start(ind, i):
    """启动时机分 (0/10/20)"""
    days = 0
    for j in range(i, max(i-60, 0), -1):
        if not (ind['ma5'][j] > ind['ma10'][j] > ind['ma20'][j]):
            break
        days += 1
    if days < 5: return 20
    elif days < 15: return 10
    return 0

def tdx_score_volume(ind, i):
    """量价配合分 (0/10/20)"""
    vm20 = ind['vm20'][i]
    if vm20 <= 0: return 0
    ratio = ind['v'][i] / vm20
    if 1.2 <= ratio < 3.0: return 20
    elif ratio >= 0.8: return 10
    return 0

def tdx_score_momentum(ind, i):
    """动量共振分 (0/10/20)"""
    kd_cross = (i >= 1 and ind['k'][i] > ind['d'][i]
                and ind['k'][i-1] <= ind['d'][i-1] and ind['k'][i] < 50)
    macd_above = ind['dif'][i] > 0
    if kd_cross and macd_above: return 20
    elif kd_cross or macd_above: return 10
    return 0

def tdx_total_score(ind, i):
    """TDX优化版总分 (0~100)"""
    return (tdx_score_ma_slope(ind, i) + tdx_score_close_line(ind, i)
            + tdx_score_just_start(ind, i) + tdx_score_volume(ind, i)
            + tdx_score_momentum(ind, i))


# ═══════════════════════════════════════════════════════════════════
#  v5过滤器
# ═══════════════════════════════════════════════════════════════════

FILTERS = {}
def _reg(name):
    def dec(f):
        FILTERS[name] = f
        return f
    return dec

@_reg('F_收阳')
def _(ind, i): return ind['c'][i] > ind['o'][i]

@_reg('F_OBV上升')
def _(ind, i): return i >= 5 and ind['obv'][i] > ind['obv'][i-5]

@_reg('F_缩量')
def _(ind, i): return ind['vm5'][i] > 0 and ind['v'][i] < ind['vm5'][i] * 0.8

@_reg('F_布林收窄')
def _(ind, i):
    mid = ind['ma20'][i]
    if mid <= 0: return False
    return (ind['boll_up'][i] - ind['boll_dn'][i]) / mid < 0.10

@_reg('F_布林下轨')
def _(ind, i):
    mid, dn = ind['ma20'][i], ind['boll_dn'][i]
    if mid <= 0 or dn <= 0: return False
    return ind['c'][i] <= (mid + dn) / 2

@_reg('F_RSI适中')
def _(ind, i): return 40 <= ind['rsi14'][i] <= 70

@_reg('F_换手适中')
def _(ind, i): return 1.0 <= ind['ch'][i] <= 8.0

@_reg('F_近期不跌')
def _(ind, i):
    return i >= 5 and ind['c'][i-5] > 0 and (ind['c'][i] - ind['c'][i-5]) / ind['c'][i-5] > -0.02

@_reg('F_ATR低波')
def _(ind, i):
    return ind['c'][i] > 0 and ind['atr14'][i] > 0 and ind['atr14'][i] / ind['c'][i] < 0.03

@_reg('F_均线多头')
def _(ind, i): return ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0

@_reg('F_大趋势')
def _(ind, i): return ind['ma20'][i] > ind['ma60'][i] > 0 and ind['c'][i] > ind['ma60'][i]

@_reg('F_放量')
def _(ind, i): return ind['vm5'][i] > 0 and ind['v'][i] > ind['vm5'][i] * 1.2


# ═══════════════════════════════════════════════════════════════════
#  核心策略注册
# ═══════════════════════════════════════════════════════════════════

V5_STRATEGIES = {
    'V5_蜻蜓布林': combo_C_qingting_boll,
    'V5_空中加油': s6_strict,
    'V5_试盘线': shipanxian,
}

TDX_CONDITIONS = {
    'T_多头排列': tdx_bull_align,
    'T_六十向上': tdx_ma60_up,
    'T_贴线MA5': tdx_close_to_ma5,
    'T_KD金叉': tdx_kd_cross,
    'T_刚启动': tdx_just_start,
    'T_盈利': tdx_profitable,
    'T_原版全AND': tdx_original,
    'T_放宽版': tdx_relaxed,
}


# ═══════════════════════════════════════════════════════════════════
#  回测核心
# ═══════════════════════════════════════════════════════════════════

def backtest_stock(stock_code):
    for attempt in range(3):
        try:
            klines = get_kline_data(stock_code, limit=500)
            break
        except Exception:
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
        # v5策略
        v5_hits = {}
        for sname, sfunc in V5_STRATEGIES.items():
            try:
                v5_hits[sname] = sfunc(ind, i, stock_code)
            except Exception:
                v5_hits[sname] = False

        # TDX条件
        tdx_hits = {}
        for tname, tfunc in TDX_CONDITIONS.items():
            try:
                tdx_hits[tname] = tfunc(ind, i)
            except Exception:
                tdx_hits[tname] = False

        # TDX打分
        tdx_sc = tdx_total_score(ind, i)

        # 至少一个v5策略或TDX条件触发
        any_v5 = any(v5_hits.values())
        any_tdx = tdx_hits.get('T_放宽版', False) or tdx_hits.get('T_原版全AND', False)
        tdx_high = tdx_sc >= 60 and tdx_hits.get('T_多头排列', False)

        if not any_v5 and not any_tdx and not tdx_high:
            continue

        if i + 1 >= n:
            continue
        bp = float(klines[i+1]['open_price'])
        if bp <= 0:
            continue

        sig = {
            'code': stock_code,
            'date': str(klines[i]['date']),
            'buy': bp,
            'env': market_env(ind, i),
            'tdx_score': tdx_sc,
        }

        for hd in HOLD_DAYS:
            si = i + 1 + hd
            if si < n:
                sp = float(klines[si]['close_price'])
                sig[f'r{hd}'] = round((sp - bp) / bp * 100, 2)

        for k, hit in v5_hits.items():
            sig[f'h_{k}'] = hit
        for k, hit in tdx_hits.items():
            sig[f'h_{k}'] = hit

        for fname, ffunc in FILTERS.items():
            try:
                sig[f'f_{fname}'] = ffunc(ind, i)
            except Exception:
                sig[f'f_{fname}'] = False

        # TDX各维度分数
        sig['ts_slope'] = tdx_score_ma_slope(ind, i)
        sig['ts_line'] = tdx_score_close_line(ind, i)
        sig['ts_start'] = tdx_score_just_start(ind, i)
        sig['ts_vol'] = tdx_score_volume(ind, i)
        sig['ts_mom'] = tdx_score_momentum(ind, i)

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
    return {'n': t, 'wr': round(w/t*100, 1), 'avg': round(sum(rets)/t, 2), 'med': round(sr[t//2], 2)}


def cv_quarters(sigs, hd=5):
    key = f'r{hd}'
    filtered = [s for s in sigs if key in s]
    if not filtered: return []
    quarters = defaultdict(list)
    for s in filtered:
        d = s['date']
        q = f"{d[:4]}Q{(int(d[5:7])-1)//3+1}"
        quarters[q].append(s[key])
    result = []
    for q in sorted(quarters):
        rets = quarters[q]
        wr = sum(1 for r in rets if r > 0) / len(rets) * 100
        result.append({'q': q, 'n': len(rets), 'wr': round(wr, 1), 'avg': round(sum(rets)/len(rets), 2)})
    return result


# ═══════════════════════════════════════════════════════════════════
#  主回测
# ═══════════════════════════════════════════════════════════════════

def run(sample_limit=1000):
    t0 = datetime.now()
    logger.info("=" * 90)
    logger.info("  策略融合回测 v6 — v5最优 × TDX通达信 深度交叉")
    logger.info("  样本: %d | 持有: %s", sample_limit, HOLD_DAYS)
    logger.info("=" * 90)

    codes = sorted(get_all_stock_codes())
    if sample_limit > 0:
        codes = codes[:sample_limit]
    logger.info("股票数: %d", len(codes))

    all_sigs = []
    for idx, c in enumerate(codes):
        if (idx+1) % 100 == 0:
            logger.info("  进度: %d/%d (信号: %d)", idx+1, len(codes), len(all_sigs))
        all_sigs.extend(backtest_stock(c))

    logger.info("  总信号: %d", len(all_sigs))
    if not all_sigs:
        return

    # ═══ PART 1: 基线对比 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [1] 基线对比 — v5策略 vs TDX策略 独立表现")
    logger.info("=" * 90)
    logger.info("  %-30s %6s  %7s  %7s  %7s", "策略", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 65)

    baselines = {}
    # v5策略
    for sname in V5_STRATEGIES:
        sigs = [s for s in all_sigs if s.get(f'h_{sname}', False)]
        st = qs(sigs, 5)
        baselines[sname] = sigs
        if st['n'] > 0:
            logger.info("  %-30s %6d  %6.1f%%  %6.2f%%  %6.2f%%", sname, st['n'], st['wr'], st['avg'], st['med'])

    # v5最优组合
    qt_obv_yang = [s for s in all_sigs if s.get('h_V5_蜻蜓布林', False)
                   and s.get('f_F_OBV上升', False) and s.get('f_F_收阳', False)]
    baselines['V5_蜻蜓+OBV+阳'] = qt_obv_yang
    st = qs(qt_obv_yang, 5)
    if st['n'] > 0:
        logger.info("  %-30s %6d  %6.1f%%  %6.2f%%  %6.2f%%", 'V5_蜻蜓+OBV+阳', st['n'], st['wr'], st['avg'], st['med'])

    s6_suoliang = [s for s in all_sigs if s.get('h_V5_空中加油', False) and s.get('f_F_缩量', False)]
    baselines['V5_空中加油+缩量'] = s6_suoliang
    st = qs(s6_suoliang, 5)
    if st['n'] > 0:
        logger.info("  %-30s %6d  %6.1f%%  %6.2f%%  %6.2f%%", 'V5_空中加油+缩量', st['n'], st['wr'], st['avg'], st['med'])

    # TDX策略
    tdx_orig = [s for s in all_sigs if s.get('h_T_原版全AND', False)]
    baselines['TDX_原版'] = tdx_orig
    st = qs(tdx_orig, 5)
    if st['n'] > 0:
        logger.info("  %-30s %6d  %6.1f%%  %6.2f%%  %6.2f%%", 'TDX_原版', st['n'], st['wr'], st['avg'], st['med'])

    tdx_relax = [s for s in all_sigs if s.get('h_T_放宽版', False)]
    baselines['TDX_放宽'] = tdx_relax
    st = qs(tdx_relax, 5)
    if st['n'] > 0:
        logger.info("  %-30s %6d  %6.1f%%  %6.2f%%  %6.2f%%", 'TDX_放宽', st['n'], st['wr'], st['avg'], st['med'])

    for thr in [60, 70, 80]:
        sigs = [s for s in all_sigs if s.get('h_T_多头排列', False) and s.get('tdx_score', 0) >= thr]
        baselines[f'TDX_多头+score>={thr}'] = sigs
        st = qs(sigs, 5)
        if st['n'] > 0:
            logger.info("  %-30s %6d  %6.1f%%  %6.2f%%  %6.2f%%", f'TDX_多头+score>={thr}', st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 2: v5策略 + TDX条件作为过滤器 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [2] v5策略 + TDX条件作为额外过滤器")
    logger.info("=" * 90)

    qt_sigs = baselines['V5_蜻蜓布林']
    qt_base = qs(qt_sigs, 5)
    logger.info("")
    logger.info("  ── 蜻蜓布林(%.1f%%) + TDX条件 ──", qt_base['wr'])
    logger.info("    %-25s %6s  %7s(%6s)  %7s", "TDX条件", "信号", "5d胜率", "提升", "5d均收")

    tdx_filter_on_qt = []
    for tname in ['T_多头排列', 'T_六十向上', 'T_贴线MA5', 'T_KD金叉', 'T_刚启动', 'T_盈利']:
        filtered = [s for s in qt_sigs if s.get(f'h_{tname}', False)]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            delta = st['wr'] - qt_base['wr']
            tdx_filter_on_qt.append((tname, st, delta))
            logger.info("    %-25s %6d  %6.1f%%(%+5.1f)  %6.2f%%", tname, st['n'], st['wr'], delta, st['avg'])

    # 蜻蜓布林 + TDX打分阈值
    logger.info("")
    logger.info("  ── 蜻蜓布林 + TDX打分阈值 ──")
    for thr in [30, 40, 50, 60, 70, 80]:
        filtered = [s for s in qt_sigs if s.get('tdx_score', 0) >= thr]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            logger.info("    蜻蜓布林+TDX>=%d %8d  %6.1f%%(%+5.1f)  %6.2f%%",
                         thr, st['n'], st['wr'], st['wr'] - qt_base['wr'], st['avg'])

    # 空中加油 + TDX条件
    s6_sigs = baselines['V5_空中加油']
    s6_base = qs(s6_sigs, 5)
    if s6_base['n'] >= 10:
        logger.info("")
        logger.info("  ── 空中加油(%.1f%%) + TDX条件 ──", s6_base['wr'])
        for tname in ['T_多头排列', 'T_六十向上', 'T_贴线MA5', 'T_KD金叉', 'T_刚启动']:
            filtered = [s for s in s6_sigs if s.get(f'h_{tname}', False)]
            st = qs(filtered, 5)
            if st['n'] >= 5:
                logger.info("    %-25s %6d  %6.1f%%(%+5.1f)  %6.2f%%",
                             tname, st['n'], st['wr'], st['wr'] - s6_base['wr'], st['avg'])

    # ═══ PART 3: TDX策略 + v5过滤器增强 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [3] TDX策略 + v5过滤器增强")
    logger.info("=" * 90)

    # TDX放宽版 + v5过滤器
    if len(tdx_relax) >= 10:
        tdx_relax_base = qs(tdx_relax, 5)
        logger.info("")
        logger.info("  ── TDX放宽版(%.1f%%) + v5过滤器 ──", tdx_relax_base['wr'])
        logger.info("    %-20s %6s  %7s(%6s)  %7s", "过滤器", "信号", "5d胜率", "提升", "5d均收")
        tdx_v5_filter = []
        for fname in FILTERS:
            filtered = [s for s in tdx_relax if s.get(f'f_{fname}', False)]
            st = qs(filtered, 5)
            if st['n'] >= 5:
                delta = st['wr'] - tdx_relax_base['wr']
                tdx_v5_filter.append((fname, st, delta))
        tdx_v5_filter.sort(key=lambda x: x[2], reverse=True)
        for fname, st, delta in tdx_v5_filter:
            logger.info("    %-20s %6d  %6.1f%%(%+5.1f)  %6.2f%%", fname, st['n'], st['wr'], delta, st['avg'])

    # TDX原版 + v5过滤器
    if len(tdx_orig) >= 10:
        tdx_orig_base = qs(tdx_orig, 5)
        logger.info("")
        logger.info("  ── TDX原版(%.1f%%) + v5过滤器 ──", tdx_orig_base['wr'])
        for fname in FILTERS:
            filtered = [s for s in tdx_orig if s.get(f'f_{fname}', False)]
            st = qs(filtered, 5)
            if st['n'] >= 5:
                logger.info("    %-20s %6d  %6.1f%%(%+5.1f)  %6.2f%%",
                             fname, st['n'], st['wr'], st['wr'] - tdx_orig_base['wr'], st['avg'])

    # ═══ PART 4: 全新融合策略 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [4] 全新融合策略 — v5形态 × TDX趋势确认")
    logger.info("=" * 90)
    logger.info("  %-50s %5s  %7s  %7s  %7s", "融合策略", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 80)

    fusion_results = []

    # 4a. 蜻蜓布林 AND TDX条件组合
    for combo_label, tdx_keys in [
        ('蜻蜓布林 AND 多头排列', ['T_多头排列']),
        ('蜻蜓布林 AND 六十向上', ['T_六十向上']),
        ('蜻蜓布林 AND 多头+六十向上', ['T_多头排列', 'T_六十向上']),
        ('蜻蜓布林 AND 多头+贴线', ['T_多头排列', 'T_贴线MA5']),
        ('蜻蜓布林 AND 多头+KD金叉', ['T_多头排列', 'T_KD金叉']),
        ('蜻蜓布林 AND 多头+刚启动', ['T_多头排列', 'T_刚启动']),
        ('蜻蜓布林 AND TDX放宽版', ['T_放宽版']),
    ]:
        filtered = [s for s in all_sigs if s.get('h_V5_蜻蜓布林', False)
                    and all(s.get(f'h_{k}', False) for k in tdx_keys)]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            fusion_results.append((combo_label, st, filtered))
            logger.info("  %-50s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                         combo_label, st['n'], st['wr'], st['avg'], st['med'])

    # 4b. 蜻蜓布林+OBV+收阳 AND TDX条件
    for combo_label, tdx_keys in [
        ('蜻蜓+OBV+阳 AND 多头排列', ['T_多头排列']),
        ('蜻蜓+OBV+阳 AND 六十向上', ['T_六十向上']),
        ('蜻蜓+OBV+阳 AND 刚启动', ['T_刚启动']),
        ('蜻蜓+OBV+阳 AND 盈利', ['T_盈利']),
    ]:
        filtered = [s for s in qt_obv_yang if all(s.get(f'h_{k}', False) for k in tdx_keys)]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            fusion_results.append((combo_label, st, filtered))
            logger.info("  %-50s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                         combo_label, st['n'], st['wr'], st['avg'], st['med'])

    # 4c. 空中加油 AND TDX条件
    for combo_label, tdx_keys in [
        ('空中加油 AND 多头排列', ['T_多头排列']),
        ('空中加油 AND 六十向上', ['T_六十向上']),
        ('空中加油 AND 多头+六十向上', ['T_多头排列', 'T_六十向上']),
        ('空中加油+缩量 AND 多头排列', ['T_多头排列']),
    ]:
        if '缩量' in combo_label:
            base = s6_suoliang
        else:
            base = baselines['V5_空中加油']
        filtered = [s for s in base if all(s.get(f'h_{k}', False) for k in tdx_keys)]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            fusion_results.append((combo_label, st, filtered))
            logger.info("  %-50s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                         combo_label, st['n'], st['wr'], st['avg'], st['med'])

    # 4d. v5策略 + TDX打分阈值
    for v5name, v5sigs in [('蜻蜓布林', baselines['V5_蜻蜓布林']),
                            ('空中加油', baselines['V5_空中加油']),
                            ('蜻蜓+OBV+阳', qt_obv_yang)]:
        for thr in [50, 60, 70, 80]:
            filtered = [s for s in v5sigs if s.get('tdx_score', 0) >= thr]
            st = qs(filtered, 5)
            if st['n'] >= 5:
                label = f'{v5name} + TDX_score>={thr}'
                fusion_results.append((label, st, filtered))
                logger.info("  %-50s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                             label, st['n'], st['wr'], st['avg'], st['med'])

    # 4e. TDX放宽版 + v5最优过滤器组合
    for combo_label, filter_keys in [
        ('TDX放宽 + OBV+收阳', ['F_OBV上升', 'F_收阳']),
        ('TDX放宽 + 缩量', ['F_缩量']),
        ('TDX放宽 + 布林收窄', ['F_布林收窄']),
        ('TDX放宽 + 布林收窄+下轨', ['F_布林收窄', 'F_布林下轨']),
        ('TDX放宽 + OBV+收阳+近期不跌', ['F_OBV上升', 'F_收阳', 'F_近期不跌']),
    ]:
        filtered = [s for s in tdx_relax if all(s.get(f'f_{k}', False) for k in filter_keys)]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            fusion_results.append((combo_label, st, filtered))
            logger.info("  %-50s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                         combo_label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 5: OR联合 — 扩大信号覆盖 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [5] OR联合 — v5 × TDX 扩大信号覆盖")
    logger.info("=" * 90)
    logger.info("  %-50s %5s  %7s  %7s  %7s", "OR组合", "信号", "5d胜率", "5d均收", "5d中位")

    or_combos = [
        ('蜻蜓布林 OR TDX原版', lambda s: s.get('h_V5_蜻蜓布林') or s.get('h_T_原版全AND')),
        ('蜻蜓布林 OR TDX放宽', lambda s: s.get('h_V5_蜻蜓布林') or s.get('h_T_放宽版')),
        ('蜻蜓+OBV+阳 OR TDX原版', lambda s: (s.get('h_V5_蜻蜓布林') and s.get('f_F_OBV上升') and s.get('f_F_收阳')) or s.get('h_T_原版全AND')),
        ('空中加油+缩量 OR TDX原版', lambda s: (s.get('h_V5_空中加油') and s.get('f_F_缩量')) or s.get('h_T_原版全AND')),
        ('蜻蜓布林 OR 空中加油 OR TDX原版', lambda s: s.get('h_V5_蜻蜓布林') or s.get('h_V5_空中加油') or s.get('h_T_原版全AND')),
        ('v5全部 OR TDX原版', lambda s: s.get('h_V5_蜻蜓布林') or s.get('h_V5_空中加油') or s.get('h_V5_试盘线') or s.get('h_T_原版全AND')),
    ]

    or_results = []
    for label, pred in or_combos:
        filtered = [s for s in all_sigs if pred(s)]
        st = qs(filtered, 5)
        if st['n'] > 0:
            or_results.append((label, st, filtered))
            logger.info("  %-50s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                         label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 6: TDX各维度对v5策略的增强效果 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [6] TDX打分各维度对v5策略的增强效果")
    logger.info("=" * 90)

    dims = [('ts_slope', '均线斜率'), ('ts_line', '动态贴线'),
            ('ts_start', '启动时机'), ('ts_vol', '量价配合'), ('ts_mom', '动量共振')]

    for v5name, v5sigs in [('蜻蜓布林', baselines['V5_蜻蜓布林']),
                            ('空中加油', baselines['V5_空中加油'])]:
        if len(v5sigs) < 20:
            continue
        v5base = qs(v5sigs, 5)
        logger.info("")
        logger.info("  ── %s(%.1f%%) + TDX各维度满分(20) ──", v5name, v5base['wr'])
        logger.info("    %-12s %6s  %7s(%6s)  %7s", "维度", "信号", "5d胜率", "提升", "5d均收")
        for dim_key, dim_name in dims:
            filtered = [s for s in v5sigs if s.get(dim_key, 0) == 20]
            st = qs(filtered, 5)
            if st['n'] >= 5:
                logger.info("    %-12s %6d  %6.1f%%(%+5.1f)  %6.2f%%",
                             dim_name, st['n'], st['wr'], st['wr'] - v5base['wr'], st['avg'])

        # 双维度满分
        logger.info("    ── 双维度满分 ──")
        for (d1k, d1n), (d2k, d2n) in combinations(dims, 2):
            filtered = [s for s in v5sigs if s.get(d1k, 0) == 20 and s.get(d2k, 0) == 20]
            st = qs(filtered, 5)
            if st['n'] >= 5:
                logger.info("    %-12s %6d  %6.1f%%(%+5.1f)  %6.2f%%",
                             f'{d1n}+{d2n}', st['n'], st['wr'], st['wr'] - v5base['wr'], st['avg'])

    # ═══ PART 7: 综合排名 + 时间稳定性 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [7] 综合排名 + 时间稳定性")
    logger.info("=" * 90)

    all_tested = []

    # 基线
    for label, sigs in baselines.items():
        st = qs(sigs, 5)
        if st['n'] >= 5:
            all_tested.append((label, st['n'], st['wr'], st['avg'], st['med'], sigs))

    # 融合策略
    for label, st, sigs in fusion_results:
        if st['n'] >= 5:
            all_tested.append((f'融合:{label}', st['n'], st['wr'], st['avg'], st['med'], sigs))

    # OR组合
    for label, st, sigs in or_results:
        if st['n'] >= 5:
            all_tested.append((f'OR:{label}', st['n'], st['wr'], st['avg'], st['med'], sigs))

    all_tested.sort(key=lambda x: x[2], reverse=True)

    logger.info("")
    logger.info("  %-55s %5s  %7s  %7s  %7s  %s",
                "组合", "信号", "5d胜率", "5d均收", "5d中位", "季度稳定性")
    logger.info("  " + "-" * 105)

    rankings_for_save = []
    for label, n, wr, avg, med, sigs in all_tested[:35]:
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

    # ═══ PART 8: TOP5详细季度CV ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [8] TOP5 详细季度CV")
    logger.info("=" * 90)

    for label, n, wr, avg, med, sigs in all_tested[:5]:
        folds = cv_quarters(sigs, 5)
        if not folds: continue
        logger.info("")
        logger.info("  ── %s (总胜率%.1f%%, %d信号) ──", label, wr, n)
        logger.info("    %-8s %5s  %7s  %7s", "季度", "信号", "胜率", "均收益")
        for f in folds:
            marker = " ★" if f['wr'] >= 55 else (" ▲" if f['wr'] >= 50 else " ▼")
            logger.info("    %-8s %5d  %6.1f%%%s  %6.2f%%", f['q'], f['n'], f['wr'], marker, f['avg'])

    # ═══ PART 9: 最终推荐 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [9] 最终推荐 — v5 vs 融合版 对比")
    logger.info("=" * 90)

    # v5基线
    logger.info("  📌 v5基线:")
    logger.info("     蜻蜓布林+OBV+收阳: 75.8%%, 91信号 (v5最优)")
    logger.info("     蜻蜓布林: 58.2%%, 1726信号")

    # 融合版是否有提升
    fusion_only = [x for x in all_tested if x[0].startswith('融合:')]
    if fusion_only:
        best_fusion = fusion_only[0]
        logger.info("")
        logger.info("  🔗 融合版最优: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     best_fusion[0], best_fusion[2], best_fusion[3], best_fusion[1])

        # 与v5基线对比
        if best_fusion[2] > 75.8:
            logger.info("  ✅ 融合版超越v5最优! 提升 +%.1f%%", best_fusion[2] - 75.8)
        elif best_fusion[2] > 58.2 and best_fusion[1] > 91:
            logger.info("  ✅ 融合版在更大样本下表现优秀 (vs v5蜻蜓布林58.2%%)")
        else:
            logger.info("  ⚠️ 融合版未超越v5最优，TDX条件对v5策略增益有限")

    # 高胜率推荐
    high_wr = [x for x in all_tested if x[2] >= 60 and x[1] >= 10]
    if high_wr:
        logger.info("  🏆 高胜率: %s (%.1f%%, %d信号)", high_wr[0][0], high_wr[0][2], high_wr[0][1])

    # 均衡推荐
    balanced = [x for x in all_tested if x[1] >= 100 and x[2] >= 55]
    if balanced:
        best = max(balanced, key=lambda x: x[2] * 0.6 + x[3] * 10)
        logger.info("  ⚖️ 均衡: %s (%.1f%%, %.2f%%, %d信号)", best[0], best[2], best[3], best[1])

    # 最稳定
    stable = []
    for label, n, wr, avg, med, sigs in all_tested:
        if n < 30: continue
        folds = cv_quarters(sigs, 5)
        q_wrs = [f['wr'] for f in folds]
        if len(q_wrs) >= 4:
            above50 = sum(1 for w in q_wrs if w > 50)
            stable.append((label, n, wr, above50, len(q_wrs)))
    if stable:
        stable.sort(key=lambda x: (x[3]/x[4], x[2]), reverse=True)
        best = stable[0]
        logger.info("  🛡️ 最稳定: %s (%.1f%%, %d信号, %d/%d季度>50%%)",
                     best[0], best[2], best[1], best[3], best[4])

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 90)

    # 保存
    result = {
        'run_time': str(datetime.now()),
        'total_signals': len(all_sigs),
        'stock_count': len(codes),
        'rankings': rankings_for_save[:35],
    }
    try:
        with open('data_results/five_strategy_deep_v6_result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("  结果已保存到 data_results/five_strategy_deep_v6_result.json")
    except Exception as e:
        logger.warning("  保存失败: %s", e)

    return all_sigs


if __name__ == '__main__':
    run(sample_limit=1000)
