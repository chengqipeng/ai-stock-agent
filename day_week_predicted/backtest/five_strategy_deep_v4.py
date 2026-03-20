#!/usr/bin/env python3
"""
6大策略深度回测 v4 — 新增"空中加油"策略
========================================
在v3基础上新增第6个策略：
  S6. 空中加油 — MACD零轴上方红柱缩短后重新放大

同时对v3最优组合(蜻蜓布林58.2%, 宽松+过滤58.0%)进行交叉验证，
测试空中加油能否进一步提升胜率或增加信号覆盖。

用法：
    python -m day_week_predicted.backtest.five_strategy_deep_v4
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
#  指标预计算
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

    # MACD (标准12,26,9)
    ema12 = calc_ema(c, 12)
    ema26 = calc_ema(c, 26)
    dif = [ema12[i] - ema26[i] for i in range(n)]
    dea = calc_ema(dif, 9)
    macd_bar = [2 * (dif[i] - dea[i]) for i in range(n)]

    rsi6 = calc_rsi(c, 6)
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

    return {
        'n': n, 'c': c, 'o': o, 'h': h, 'l': l, 'v': v, 'ch': ch,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60,
        'ema20': ema20, 'ema40': ema40, 'ema60': ema60,
        'vm5': vm5, 'vm10': vm10, 'vm20': vm20,
        'dif': dif, 'dea': dea, 'macd_bar': macd_bar,
        'rsi6': rsi6, 'rsi14': rsi14,
        'k': kv, 'd': dv, 'j': jv,
        'obv': obv, 'boll_up': boll_up, 'boll_dn': boll_dn,
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
#  策略 S6: 空中加油 (新增)
# ═══════════════════════════════════════════════════════════════════
# MACD零轴上方，DIF与DEA收敛(差距<=0.02)，红柱连续缩短2日，
# 前一日满足收敛条件后，当日DIF止跌回升+红柱重新放大

def s6_kongzhong_jiayou(ind, i, code):
    """空中加油 — 严格版"""
    if i < 3:
        return False
    dif = ind['dif']
    dea = ind['dea']
    bar = ind['macd_bar']

    # 前一日(i-1)的条件：
    # (DIF-DEA) <= 0.02 (收敛)
    if dif[i - 1] - dea[i - 1] > 0.02:
        return False
    # DIF <= REF(DIF,1) 即 DIF[i-1] <= DIF[i-2]
    if dif[i - 1] > dif[i - 2]:
        return False
    # REF(DIF,1) < REF(DIF,2) 即 DIF[i-2] < DIF[i-3]
    if i < 4:
        return False
    if dif[i - 2] >= dif[i - 3]:
        return False
    # DIF > DEA (仍在零轴上方金叉状态)
    if dif[i - 1] <= dea[i - 1]:
        return False
    # DEA > 0
    if dea[i - 1] <= 0:
        return False
    # DEA > REF(DEA,1) 即 DEA[i-1] > DEA[i-2]
    if dea[i - 1] <= dea[i - 2]:
        return False
    # MACD柱状线 < REF(MACD柱状线,1) 即 bar[i-1] < bar[i-2]
    if bar[i - 1] >= bar[i - 2]:
        return False
    # REF(MACD柱状线,1) < REF(MACD柱状线,2) 即 bar[i-2] < bar[i-3]
    if bar[i - 2] >= bar[i - 3]:
        return False

    # 当日(i)的信号条件：
    # DIF >= REF(DIF,1) 即 DIF[i] >= DIF[i-1] (DIF止跌回升)
    if dif[i] < dif[i - 1]:
        return False
    # MACD柱状线 > REF(MACD柱状线,1) 即 bar[i] > bar[i-1] (红柱放大)
    if bar[i] <= bar[i - 1]:
        return False

    return True


def s6_loose(ind, i, code):
    """空中加油 — 宽松版：收敛阈值放宽到0.05，只要求1日缩短"""
    if i < 3:
        return False
    dif = ind['dif']
    dea = ind['dea']
    bar = ind['macd_bar']

    # 前一日收敛
    if dif[i - 1] - dea[i - 1] > 0.05:
        return False
    # DIF下降
    if dif[i - 1] > dif[i - 2]:
        return False
    # DIF > DEA (金叉状态)
    if dif[i - 1] <= dea[i - 1]:
        return False
    # DEA > 0
    if dea[i - 1] <= 0:
        return False
    # 红柱缩短
    if bar[i - 1] >= bar[i - 2]:
        return False

    # 当日DIF回升 + 红柱放大
    if dif[i] < dif[i - 1]:
        return False
    if bar[i] <= bar[i - 1]:
        return False

    return True


def s6_with_vol(ind, i, code):
    """空中加油 + 放量确认"""
    if not s6_kongzhong_jiayou(ind, i, code):
        return False
    # 当日放量: V > 5日均量
    if ind['vm5'][i] <= 0:
        return False
    return ind['v'][i] > ind['vm5'][i]


def s6_with_ma(ind, i, code):
    """空中加油 + 均线多头"""
    if not s6_kongzhong_jiayou(ind, i, code):
        return False
    return ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0


# ═══════════════════════════════════════════════════════════════════
#  v3最优策略复用
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
    """布林收窄+布林下轨+RSI适中 (v3最佳过滤器)"""
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
    if s6_kongzhong_jiayou(ind, i, code):
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
#  过滤器
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

@_reg('F_OBV上升')
def _(ind, i): return i >= 5 and ind['obv'][i] > ind['obv'][i - 5]

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


# ═══════════════════════════════════════════════════════════════════
#  策略注册表
# ═══════════════════════════════════════════════════════════════════

STRATEGIES = {
    'S6_空中加油': s6_kongzhong_jiayou,
    'S6L_空中加油宽': s6_loose,
    'S6V_空中加油+放量': s6_with_vol,
    'S6M_空中加油+均线': s6_with_ma,
    'C_蜻蜓布林': combo_C_qingting_boll,
    'F_宽松+过滤': combo_F_loose_with_filter,
    'S0_试盘线': shipanxian,
}


# ═══════════════════════════════════════════════════════════════════
#  回测核心
# ═══════════════════════════════════════════════════════════════════

def backtest_stock(stock_code):
    klines = get_kline_data(stock_code, limit=500)
    if len(klines) < 260:
        return []
    ind = precompute(klines)
    if ind is None:
        return []

    signals = []
    n = ind['n']

    for i in range(250, n):
        hits = {}
        for sname, sfunc in STRATEGIES.items():
            try:
                hits[sname] = sfunc(ind, i, stock_code)
            except Exception:
                hits[sname] = False

        if not any(hits.values()):
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

        # 过滤器状态
        for fname, ffunc in FILTERS.items():
            try:
                sig[f'f_{fname}'] = ffunc(ind, i)
            except Exception:
                sig[f'f_{fname}'] = False

        signals.append(sig)

    return signals


def qs(sigs, hd=5):
    key = f'r{hd}'
    rets = [s[key] for s in sigs if key in s]
    if not rets:
        return {'n': 0, 'wr': 0, 'avg': 0, 'med': 0, 'mx': 0, 'mn': 0}
    w = sum(1 for r in rets if r > 0)
    t = len(rets)
    sr = sorted(rets)
    return {
        'n': t, 'wr': round(w / t * 100, 1),
        'avg': round(sum(rets) / t, 2),
        'med': round(sr[t // 2], 2),
        'mx': round(max(rets), 2),
        'mn': round(min(rets), 2),
    }


def cv_quarters(sigs, hd=5):
    """按季度做时间序列CV"""
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
    logger.info("=" * 85)
    logger.info("  6大策略回测 v4 — 新增空中加油 + v3最优组合交叉验证")
    logger.info("  样本: %d | 持有: %s", sample_limit, HOLD_DAYS)
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

    # ═══ PART 1: 各策略独立表现 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [1] 各策略独立表现")
    logger.info("=" * 85)
    logger.info("  %-22s %6s  %7s  %7s  %7s │ %7s  %7s  %7s",
                "策略", "信号", "3d胜率", "3d均收", "3d中位", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 88)

    strat_perf = {}
    for sname in STRATEGIES:
        filtered = [s for s in all_sigs if s.get(f'h_{sname}', False)]
        s3, s5, s7 = qs(filtered, 3), qs(filtered, 5), qs(filtered, 7)
        strat_perf[sname] = {'s3': s3, 's5': s5, 's7': s7, 'sigs': filtered}
        if s5['n'] > 0:
            logger.info("  %-22s %6d  %6.1f%%  %6.2f%%  %6.2f%% │ %6.1f%%  %6.2f%%  %6.2f%%",
                         sname, s5['n'],
                         s3['wr'], s3['avg'], s3['med'],
                         s5['wr'], s5['avg'], s5['med'])
        else:
            logger.info("  %-22s %6d  (无信号)", sname, 0)

    # ═══ PART 2: 空中加油 市场环境分层 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [2] 空中加油各变体 市场环境分层 (5日)")
    logger.info("=" * 85)

    s6_variants = ['S6_空中加油', 'S6L_空中加油宽', 'S6V_空中加油+放量', 'S6M_空中加油+均线']
    for sname in s6_variants:
        sigs = strat_perf[sname]['sigs']
        if len(sigs) < 3:
            continue
        logger.info("  ── %s (%d信号) ──", sname, len(sigs))
        for env in ['bull', 'range', 'bear']:
            es = [s for s in sigs if s.get('env') == env]
            st = qs(es, 5)
            if st['n'] > 0:
                logger.info("    %-6s %5d信号  胜率%5.1f%%  均收%6.2f%%  中位%6.2f%%",
                             env, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 3: 空中加油 + 过滤器增强 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [3] 空中加油 + 单过滤器增强效果")
    logger.info("=" * 85)

    for s6name in s6_variants:
        s6_sigs = strat_perf[s6name]['sigs']
        base = qs(s6_sigs, 5)
        if base['n'] < 10:
            continue
        logger.info("")
        logger.info("  ── %s (基准: 胜率%.1f%%, 均收%.2f%%, %d信号) ──",
                     s6name, base['wr'], base['avg'], base['n'])
        logger.info("    %-14s %5s  %7s(%6s)  %7s(%7s)",
                     "过滤器", "信号", "5d胜率", "提升", "5d均收", "提升")

        effects = []
        for fname in FILTERS:
            filtered = [s for s in s6_sigs if s.get(f'f_{fname}', False)]
            st = qs(filtered, 5)
            if st['n'] >= 5:
                effects.append((fname, st, st['wr'] - base['wr'], st['avg'] - base['avg']))

        effects.sort(key=lambda x: x[2], reverse=True)
        for fname, st, wr_d, avg_d in effects:
            logger.info("    %-14s %5d  %6.1f%%(%+5.1f)  %6.2f%%(%+6.2f)",
                         fname, st['n'], st['wr'], wr_d, st['avg'], avg_d)

    # ═══ PART 4: 空中加油 + 过滤器组合穷举 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [4] 空中加油 + 过滤器组合穷举 (2个AND)")
    logger.info("=" * 85)

    for s6name in ['S6_空中加油', 'S6L_空中加油宽']:
        s6_sigs = strat_perf[s6name]['sigs']
        if len(s6_sigs) < 20:
            continue
        logger.info("")
        logger.info("  ── %s ──", s6name)
        logger.info("    %-35s %5s  %7s  %7s  %7s",
                     "过滤器组合", "信号", "5d胜率", "5d均收", "5d中位")

        combo_results = []
        fnames = list(FILTERS.keys())
        for a, b in combinations(fnames, 2):
            filtered = [s for s in s6_sigs
                        if s.get(f'f_{a}', False) and s.get(f'f_{b}', False)]
            st = qs(filtered, 5)
            if st['n'] >= 8:
                combo_results.append((f"{a}+{b}", st))

        combo_results.sort(key=lambda x: x[1]['wr'], reverse=True)
        for label, st in combo_results[:15]:
            logger.info("    %-35s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                         label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 5: 空中加油 与 v3最优策略 OR联合 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [5] 空中加油 × v3最优策略 OR联合")
    logger.info("=" * 85)
    logger.info("  %-45s %5s  %7s  %7s  %7s",
                "组合", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 75)

    or_combos = [
        ('S6 OR 蜻蜓布林', ['S6_空中加油', 'C_蜻蜓布林']),
        ('S6宽 OR 蜻蜓布林', ['S6L_空中加油宽', 'C_蜻蜓布林']),
        ('S6 OR 宽松+过滤', ['S6_空中加油', 'F_宽松+过滤']),
        ('S6宽 OR 宽松+过滤', ['S6L_空中加油宽', 'F_宽松+过滤']),
        ('S6 OR 试盘线', ['S6_空中加油', 'S0_试盘线']),
        ('S6 OR 蜻蜓布林 OR 试盘线', ['S6_空中加油', 'C_蜻蜓布林', 'S0_试盘线']),
        ('S6宽 OR 蜻蜓布林 OR 宽松+过滤', ['S6L_空中加油宽', 'C_蜻蜓布林', 'F_宽松+过滤']),
        ('S6+放量 OR 蜻蜓布林', ['S6V_空中加油+放量', 'C_蜻蜓布林']),
        ('S6+均线 OR 蜻蜓布林', ['S6M_空中加油+均线', 'C_蜻蜓布林']),
        ('全部OR', list(STRATEGIES.keys())),
    ]

    or_perf = {}
    for label, strats in or_combos:
        filtered = [s for s in all_sigs if any(s.get(f'h_{sn}', False) for sn in strats)]
        st = qs(filtered, 5)
        or_perf[label] = {'st': st, 'sigs': filtered}
        if st['n'] > 0:
            logger.info("  %-45s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                         label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 6: 空中加油 AND v3最优策略 (交叉确认) ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [6] 空中加油 AND v3策略 (双重确认)")
    logger.info("=" * 85)
    logger.info("  %-45s %5s  %7s  %7s  %7s",
                "组合", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 75)

    and_combos = [
        ('S6 AND 蜻蜓布林', 'S6_空中加油', 'C_蜻蜓布林'),
        ('S6宽 AND 蜻蜓布林', 'S6L_空中加油宽', 'C_蜻蜓布林'),
        ('S6 AND 宽松+过滤', 'S6_空中加油', 'F_宽松+过滤'),
        ('S6宽 AND 宽松+过滤', 'S6L_空中加油宽', 'F_宽松+过滤'),
        ('S6 AND 试盘线', 'S6_空中加油', 'S0_试盘线'),
    ]
    for label, sa, sb in and_combos:
        filtered = [s for s in all_sigs
                    if s.get(f'h_{sa}', False) and s.get(f'h_{sb}', False)]
        st = qs(filtered, 5)
        if st['n'] > 0:
            logger.info("  %-45s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                         label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 7: 时间序列CV ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [7] 关键组合时间序列CV (5日)")
    logger.info("=" * 85)

    cv_targets = {}
    for sname in ['S6_空中加油', 'S6L_空中加油宽', 'S6V_空中加油+放量', 'S6M_空中加油+均线',
                   'C_蜻蜓布林', 'F_宽松+过滤']:
        sigs = strat_perf[sname]['sigs']
        if len(sigs) >= 10:
            cv_targets[sname] = sigs
    for label, data in or_perf.items():
        if data['st']['n'] >= 20:
            cv_targets[f'OR:{label}'] = data['sigs']

    for cv_name, sigs in cv_targets.items():
        folds = cv_quarters(sigs, 5)
        if len(folds) < 2:
            continue
        logger.info("")
        logger.info("  ── %s (%d信号) ──", cv_name, sum(f['n'] for f in folds))
        logger.info("    %-8s %5s  %7s  %7s", "季度", "信号", "胜率", "均收益")
        q_wrs = []
        for f in folds:
            logger.info("    %-8s %5d  %6.1f%%  %6.2f%%", f['q'], f['n'], f['wr'], f['avg'])
            q_wrs.append(f['wr'])
        above50 = sum(1 for w in q_wrs if w > 50)
        avg_wr = sum(q_wrs) / len(q_wrs)
        logger.info("    稳定性: %d/%d季度>50%% (均胜率%.1f%%)", above50, len(q_wrs), avg_wr)

    # ═══ PART 8: 综合排名 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [8] 综合排名 (信号>=5, 按5日胜率)")
    logger.info("=" * 85)

    all_tested = []
    for sname in STRATEGIES:
        st = strat_perf[sname]['s5']
        if st['n'] >= 5:
            all_tested.append((sname, st['n'], st['wr'], st['avg'], st['med']))
    for label, data in or_perf.items():
        st = data['st']
        if st['n'] >= 5:
            all_tested.append((f'OR:{label}', st['n'], st['wr'], st['avg'], st['med']))

    # 空中加油+最佳过滤器组合
    for s6name in ['S6_空中加油', 'S6L_空中加油宽']:
        s6_sigs = strat_perf[s6name]['sigs']
        if len(s6_sigs) < 10:
            continue
        for fname in FILTERS:
            filtered = [s for s in s6_sigs if s.get(f'f_{fname}', False)]
            st = qs(filtered, 5)
            if st['n'] >= 8:
                all_tested.append((f'{s6name}+{fname}', st['n'], st['wr'], st['avg'], st['med']))

    all_tested.sort(key=lambda x: x[2], reverse=True)

    logger.info("  %-55s %5s  %7s  %7s  %7s",
                "组合", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 85)
    for label, n, wr, avg, med in all_tested[:25]:
        logger.info("  %-55s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                     label, n, wr, avg, med)

    # ═══ PART 9: 最终推荐 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [9] 最终推荐")
    logger.info("=" * 85)

    high_wr = [x for x in all_tested if x[2] >= 58 and x[1] >= 10]
    if high_wr:
        best = high_wr[0]
        logger.info("  高胜率推荐: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     best[0], best[2], best[3], best[1])

    large = [x for x in all_tested if x[1] >= 200 and x[2] >= 52]
    if large:
        best = max(large, key=lambda x: x[2])
        logger.info("  大样本推荐: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     best[0], best[2], best[3], best[1])

    balanced = [x for x in all_tested if x[1] >= 50 and x[2] >= 55]
    if balanced:
        best = max(balanced, key=lambda x: x[2] * 0.6 + x[3] * 10)
        logger.info("  均衡推荐: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     best[0], best[2], best[3], best[1])

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 85)

    # 保存
    result = {
        'run_time': str(datetime.now()),
        'total_signals': len(all_sigs),
        'stock_count': len(codes),
        'rankings': [{'label': l, 'n': n, 'wr': wr, 'avg': avg, 'med': med}
                     for l, n, wr, avg, med in all_tested[:25]],
    }
    try:
        with open('data_results/five_strategy_deep_v4_result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("  结果已保存到 data_results/five_strategy_deep_v4_result.json")
    except Exception as e:
        logger.warning("  保存失败: %s", e)

    return all_sigs


if __name__ == '__main__':
    run(sample_limit=1000)
