#!/usr/bin/env python3
"""
通达信选股公式回测 v2 — 基于v1发现的深度优化
=============================================
v1核心发现:
  1. 原版公式alpha来自"贴线+KD金叉"组合(53.4%)
  2. "低价"和"盈利"是负贡献噪声
  3. 动量共振(KD低位金叉+MACD水上)满分时59.8%
  4. 公式是牛市策略，需要大盘过滤

v2优化方向:
  A. 核心策略: 多头排列+贴线+KD低位金叉+量能确认
  B. 大盘过滤: 用上证指数MA20判断市场环境
  C. 动态贴线: ATR归一化替代固定2%
  D. 多头新鲜度: BARSLAST判断刚进入多头
  E. 穷举最优组合: 所有条件排列组合找最优子集

用法:
    python -m day_week_predicted.backtest.tdx_formula_backtest_v2
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
    calc_ma, calc_ema, calc_rsi, calc_kdj,
)

HOLD_DAYS = [3, 5, 7, 10]


# ═══════════════════════════════════════════════════════════════════
#  大盘数据 (上证指数 000001.SH)
# ═══════════════════════════════════════════════════════════════════

_INDEX_ENV = {}  # date_str -> 'bull'/'bear'/'range'


def load_index_env():
    """加载上证指数数据，计算大盘环境"""
    global _INDEX_ENV
    # 尝试用 000001.SH 或 sh000001 等常见代码
    for code in ['000001.SH', 'sh000001', '999999.SH', '1A0001.SH']:
        klines = get_kline_data(code, limit=500)
        if len(klines) >= 60:
            break
    else:
        logger.warning("  无法加载大盘指数数据，跳过大盘过滤")
        return False

    c = [float(k['close_price']) for k in klines]
    ma20 = calc_ma(c, 20)
    ma60 = calc_ma(c, 60)

    for i in range(60, len(klines)):
        d = str(klines[i]['date'])
        if c[i] > ma20[i] and ma20[i] > ma60[i]:
            _INDEX_ENV[d] = 'bull'
        elif c[i] < ma20[i] and ma20[i] < ma60[i]:
            _INDEX_ENV[d] = 'bear'
        else:
            _INDEX_ENV[d] = 'range'

    logger.info("  大盘环境数据: %d天, bull=%d, range=%d, bear=%d",
                len(_INDEX_ENV),
                sum(1 for v in _INDEX_ENV.values() if v == 'bull'),
                sum(1 for v in _INDEX_ENV.values() if v == 'range'),
                sum(1 for v in _INDEX_ENV.values() if v == 'bear'))
    return True


def get_market_env(date_str):
    """获取某天的大盘环境"""
    return _INDEX_ENV.get(date_str, 'unknown')


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

    ma5 = calc_ma(c, 5)
    ma10 = calc_ma(c, 10)
    ma20 = calc_ma(c, 20)
    ma60 = calc_ma(c, 60)
    vm5 = calc_ma(v, 5)
    vm20 = calc_ma(v, 20)

    # MACD
    ema12 = calc_ema(c, 12)
    ema26 = calc_ema(c, 26)
    dif = [ema12[i] - ema26[i] for i in range(n)]
    dea = calc_ema(dif, 9)

    # KDJ
    kv, dv, jv = calc_kdj(h, l, c)

    # ATR(14)
    atr14 = [0.0] * n
    for i in range(1, n):
        tr = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
        atr14[i] = tr if i < 14 else (atr14[i - 1] * 13 + tr) / 14

    # RSI
    rsi14 = calc_rsi(c, 14)

    return {
        'n': n, 'c': c, 'o': o, 'h': h, 'l': l, 'v': v,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60,
        'vm5': vm5, 'vm20': vm20,
        'dif': dif, 'dea': dea,
        'k': kv, 'd': dv, 'j': jv,
        'atr14': atr14, 'rsi14': rsi14,
    }


# ═══════════════════════════════════════════════════════════════════
#  条件库 — 每个条件独立，用于穷举组合
# ═══════════════════════════════════════════════════════════════════

def c_bull_align(ind, i):
    """A-多头排列: MA5>MA10>MA20>MA60"""
    return (ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > ind['ma60'][i]
            and ind['ma60'][i] > 0)


def c_bull_short(ind, i):
    """B-短期多头: MA5>MA10>MA20 (不要求MA60)"""
    return ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0


def c_ma60_up(ind, i):
    """C-MA60向上"""
    return i >= 1 and ind['ma60'][i] > ind['ma60'][i - 1] > 0


def c_ma60_up_3d(ind, i):
    """D-MA60连续3日向上"""
    if i < 3 or ind['ma60'][i - 3] <= 0:
        return False
    return all(ind['ma60'][i - j] > ind['ma60'][i - j - 1] for j in range(3))


def c_close_ma5_2pct(ind, i):
    """E-贴线(固定2%): C>MA5 AND C/MA5<1.02"""
    ma5 = ind['ma5'][i]
    return ma5 > 0 and ind['c'][i] > ma5 and ind['c'][i] / ma5 < 1.02


def c_close_ma5_atr03(ind, i):
    """F-动态贴线(ATR<0.3): 偏离MA5不超过0.3倍ATR"""
    ma5, atr = ind['ma5'][i], ind['atr14'][i]
    if ma5 <= 0 or atr <= 0:
        return False
    c = ind['c'][i]
    return c > ma5 and (c - ma5) / atr < 0.3


def c_close_ma5_atr05(ind, i):
    """G-动态贴线(ATR<0.5)"""
    ma5, atr = ind['ma5'][i], ind['atr14'][i]
    if ma5 <= 0 or atr <= 0:
        return False
    c = ind['c'][i]
    return c > ma5 and (c - ma5) / atr < 0.5


def c_close_ma5_atr08(ind, i):
    """H-动态贴线(ATR<0.8)"""
    ma5, atr = ind['ma5'][i], ind['atr14'][i]
    if ma5 <= 0 or atr <= 0:
        return False
    c = ind['c'][i]
    return c > ma5 and (c - ma5) / atr < 0.8


def c_kd_cross(ind, i):
    """I-KD金叉(不限位置)"""
    return i >= 1 and ind['k'][i] > ind['d'][i] and ind['k'][i - 1] <= ind['d'][i - 1]


def c_kd_cross_low30(ind, i):
    """J-KD低位金叉(K<30)"""
    return c_kd_cross(ind, i) and ind['k'][i] < 30


def c_kd_cross_low50(ind, i):
    """K-KD低位金叉(K<50)"""
    return c_kd_cross(ind, i) and ind['k'][i] < 50


def c_kd_cross_low70(ind, i):
    """L-KD中低位金叉(K<70)"""
    return c_kd_cross(ind, i) and ind['k'][i] < 70


def c_macd_above(ind, i):
    """M-MACD水上: DIF>0"""
    return ind['dif'][i] > 0


def c_macd_golden(ind, i):
    """N-MACD金叉: DIF上穿DEA"""
    return (i >= 1 and ind['dif'][i] > ind['dea'][i]
            and ind['dif'][i - 1] <= ind['dea'][i - 1])


def c_vol_up_12(ind, i):
    """O-温和放量: V > VM20*1.2 且 V < VM20*3"""
    vm20 = ind['vm20'][i]
    if vm20 <= 0:
        return False
    ratio = ind['v'][i] / vm20
    return 1.2 <= ratio < 3.0


def c_vol_up_08(ind, i):
    """P-量能不萎缩: V > VM20*0.8"""
    vm20 = ind['vm20'][i]
    return vm20 > 0 and ind['v'][i] > vm20 * 0.8


def c_vol_shrink(ind, i):
    """Q-缩量: V < VM5*0.8"""
    vm5 = ind['vm5'][i]
    return vm5 > 0 and ind['v'][i] < vm5 * 0.8


def c_fresh_bull_5(ind, i):
    """R-多头新鲜度<5天"""
    days = 0
    for j in range(i, max(i - 60, 0), -1):
        if not (ind['ma5'][j] > ind['ma10'][j] > ind['ma20'][j]):
            break
        days += 1
    return days < 5


def c_fresh_bull_10(ind, i):
    """S-多头新鲜度<10天"""
    days = 0
    for j in range(i, max(i - 60, 0), -1):
        if not (ind['ma5'][j] > ind['ma10'][j] > ind['ma20'][j]):
            break
        days += 1
    return days < 10


def c_fresh_bull_20(ind, i):
    """T-多头新鲜度<20天"""
    days = 0
    for j in range(i, max(i - 60, 0), -1):
        if not (ind['ma5'][j] > ind['ma10'][j] > ind['ma20'][j]):
            break
        days += 1
    return days < 20


def c_rsi_mid(ind, i):
    """U-RSI中性区: 40<=RSI14<=65"""
    return 40 <= ind['rsi14'][i] <= 65


def c_above_ma60(ind, i):
    """V-站上MA60: C > MA60"""
    return ind['ma60'][i] > 0 and ind['c'][i] > ind['ma60'][i]


def c_ma5_ma60_lt110(ind, i):
    """W-刚启动(MA5/MA60<1.10)"""
    ma60 = ind['ma60'][i]
    return ma60 > 0 and ind['ma5'][i] / ma60 < 1.10


def c_ma5_ma60_lt115(ind, i):
    """X-刚启动(MA5/MA60<1.15)"""
    ma60 = ind['ma60'][i]
    return ma60 > 0 and ind['ma5'][i] / ma60 < 1.15


# 所有条件注册
ALL_CONDITIONS = {
    'A_多头排列': c_bull_align,
    'B_短期多头': c_bull_short,
    'C_MA60向上': c_ma60_up,
    'D_MA60连3上': c_ma60_up_3d,
    'E_贴线2%': c_close_ma5_2pct,
    'F_贴线ATR03': c_close_ma5_atr03,
    'G_贴线ATR05': c_close_ma5_atr05,
    'H_贴线ATR08': c_close_ma5_atr08,
    'I_KD金叉': c_kd_cross,
    'J_KD低叉30': c_kd_cross_low30,
    'K_KD低叉50': c_kd_cross_low50,
    'L_KD中叉70': c_kd_cross_low70,
    'M_MACD水上': c_macd_above,
    'N_MACD金叉': c_macd_golden,
    'O_温和放量': c_vol_up_12,
    'P_量不萎缩': c_vol_up_08,
    'Q_缩量': c_vol_shrink,
    'R_新鲜5天': c_fresh_bull_5,
    'S_新鲜10天': c_fresh_bull_10,
    'T_新鲜20天': c_fresh_bull_20,
    'U_RSI中性': c_rsi_mid,
    'V_站上MA60': c_above_ma60,
    'W_启动110': c_ma5_ma60_lt110,
    'X_启动115': c_ma5_ma60_lt115,
}


# ═══════════════════════════════════════════════════════════════════
#  预设策略组合
# ═══════════════════════════════════════════════════════════════════

PRESET_STRATEGIES = {
    '原版公式': ['A_多头排列', 'C_MA60向上', 'E_贴线2%', 'I_KD金叉', 'X_启动115'],
    'v1最优_贴线+KD': ['A_多头排列', 'E_贴线2%', 'I_KD金叉'],
    '核心A_多头+贴线ATR+KD低叉+放量': ['A_多头排列', 'G_贴线ATR05', 'K_KD低叉50', 'O_温和放量'],
    '核心B_多头+贴线ATR+KD低叉+MACD': ['A_多头排列', 'G_贴线ATR05', 'K_KD低叉50', 'M_MACD水上'],
    '核心C_多头+贴线+KD低叉+新鲜': ['A_多头排列', 'E_贴线2%', 'K_KD低叉50', 'S_新鲜10天'],
    '核心D_多头+动态贴线+KD低叉+量+MACD': ['A_多头排列', 'G_贴线ATR05', 'K_KD低叉50', 'O_温和放量', 'M_MACD水上'],
    '核心E_短多+贴线+KD低叉+放量': ['B_短期多头', 'G_贴线ATR05', 'K_KD低叉50', 'O_温和放量'],
    '核心F_多头+贴线+MACD金叉': ['A_多头排列', 'G_贴线ATR05', 'N_MACD金叉'],
    '核心G_多头+贴线+KD低叉30+放量': ['A_多头排列', 'G_贴线ATR05', 'J_KD低叉30', 'O_温和放量'],
    '核心H_多头+贴线2%+KD中叉+RSI': ['A_多头排列', 'E_贴线2%', 'L_KD中叉70', 'U_RSI中性'],
    '核心I_多头+MA60连上+贴线+KD低叉': ['A_多头排列', 'D_MA60连3上', 'G_贴线ATR05', 'K_KD低叉50'],
    '核心J_多头+新鲜5+贴线+KD低叉+放量': ['A_多头排列', 'R_新鲜5天', 'G_贴线ATR05', 'K_KD低叉50', 'O_温和放量'],
}


# ═══════════════════════════════════════════════════════════════════
#  统计工具
# ═══════════════════════════════════════════════════════════════════

def qs(sigs, hd=5):
    key = f'r{hd}'
    rets = [s[key] for s in sigs if key in s]
    if not rets:
        return {'n': 0, 'wr': 0, 'avg': 0, 'med': 0, 'max': 0, 'min': 0}
    w = sum(1 for r in rets if r > 0)
    t = len(rets)
    sr = sorted(rets)
    return {
        'n': t, 'wr': round(w / t * 100, 1),
        'avg': round(sum(rets) / t, 2),
        'med': round(sr[t // 2], 2),
        'max': round(sr[-1], 2), 'min': round(sr[0], 2),
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
#  单股回测
# ═══════════════════════════════════════════════════════════════════

def backtest_stock(stock_code):
    # 排除北交所
    if stock_code.startswith('4') or stock_code.startswith('8'):
        return []

    klines = get_kline_data(stock_code, limit=500)
    if len(klines) < 260:
        return []
    ind = precompute(klines)
    if ind is None:
        return []

    signals = []
    n = ind['n']

    for i in range(120, n):
        # 基础门槛: 至少KD金叉发生(当天事件)，否则跳过
        if not c_kd_cross(ind, i):
            continue

        # 次日开盘买入
        if i + 1 >= n:
            continue
        bp = float(klines[i + 1]['open_price'])
        if bp <= 0:
            continue

        date_str = str(klines[i]['date'])

        sig = {
            'code': stock_code,
            'date': date_str,
            'buy': bp,
            'close': ind['c'][i],
            'env': get_market_env(date_str),
        }

        # 计算所有条件
        for cname, cfunc in ALL_CONDITIONS.items():
            try:
                sig[f'c_{cname}'] = cfunc(ind, i)
            except Exception:
                sig[f'c_{cname}'] = False

        # 计算持有收益
        for hd in HOLD_DAYS:
            si = i + 1 + hd
            if si < n:
                sp = float(klines[si]['close_price'])
                sig[f'r{hd}'] = round((sp - bp) / bp * 100, 2)

        signals.append(sig)

    return signals


# ═══════════════════════════════════════════════════════════════════
#  主回测
# ═══════════════════════════════════════════════════════════════════

def run(sample_limit=1000):
    t0 = datetime.now()
    logger.info("=" * 90)
    logger.info("  通达信公式回测 v2 — 深度优化")
    logger.info("  样本: %d | 持有: %s", sample_limit, HOLD_DAYS)
    logger.info("=" * 90)

    has_index = load_index_env()

    codes = sorted(get_all_stock_codes())
    if sample_limit > 0:
        codes = codes[:sample_limit]
    logger.info("股票数: %d", len(codes))

    all_sigs = []
    for idx, code in enumerate(codes):
        if (idx + 1) % 100 == 0:
            logger.info("  进度: %d/%d (信号: %d)", idx + 1, len(codes), len(all_sigs))
        all_sigs.extend(backtest_stock(code))

    logger.info("  总信号(KD金叉日): %d", len(all_sigs))
    if not all_sigs:
        return

    # ═══ PART 1: 基准 — KD金叉日的基础胜率 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [1] 基准: 所有KD金叉日的表现")
    logger.info("=" * 90)
    for hd in HOLD_DAYS:
        st = qs(all_sigs, hd)
        logger.info("  %d天: %d信号, 胜率%.1f%%, 均收%.2f%%", hd, st['n'], st['wr'], st['avg'])

    # ═══ PART 2: 每个条件的独立增量 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [2] 每个条件叠加KD金叉后的增量贡献")
    logger.info("=" * 90)
    logger.info("  %-15s %6s  %7s  %7s  %7s  %7s", "条件", "信号", "5d胜率", "5d均收", "7d胜率", "7d均收")

    base_wr = qs(all_sigs, 5)['wr']
    cond_perf = []
    for cname in ALL_CONDITIONS:
        if cname == 'I_KD金叉':
            continue  # 基准本身
        filtered = [s for s in all_sigs if s.get(f'c_{cname}', False)]
        s5 = qs(filtered, 5)
        s7 = qs(filtered, 7)
        if s5['n'] >= 10:
            delta = s5['wr'] - base_wr
            cond_perf.append((cname, s5, s7, delta))

    cond_perf.sort(key=lambda x: x[3], reverse=True)
    for cname, s5, s7, delta in cond_perf:
        marker = "↑" if delta > 0 else "↓"
        logger.info("  %-15s %6d  %6.1f%%(%s%+.1f)  %6.2f%%  %6.1f%%  %6.2f%%",
                     cname, s5['n'], s5['wr'], marker, delta, s5['avg'], s7['wr'], s7['avg'])

    # ═══ PART 3: 预设策略组合 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [3] 预设策略组合表现")
    logger.info("=" * 90)
    logger.info("  %-45s %5s  %7s  %7s │ %7s  %7s │ %7s",
                "策略", "信号", "3d胜率", "3d均收", "5d胜率", "5d均收", "10d胜率")

    preset_results = []
    for sname, conds in PRESET_STRATEGIES.items():
        filtered = [s for s in all_sigs if all(s.get(f'c_{c}', False) for c in conds)]
        s3, s5, s7, s10 = qs(filtered, 3), qs(filtered, 5), qs(filtered, 7), qs(filtered, 10)
        if s5['n'] > 0:
            logger.info("  %-45s %5d  %6.1f%%  %6.2f%% │ %6.1f%%  %6.2f%% │ %6.1f%%",
                         sname, s5['n'], s3['wr'], s3['avg'], s5['wr'], s5['avg'], s10['wr'])
            preset_results.append((sname, filtered, s5))

    # ═══ PART 4: 大盘环境过滤 ═══
    if has_index:
        logger.info("")
        logger.info("=" * 90)
        logger.info("  [4] 大盘环境过滤效果")
        logger.info("=" * 90)

        for sname, sigs, _ in preset_results:
            if len(sigs) < 20:
                continue
            logger.info("")
            logger.info("  ── %s ──", sname)
            logger.info("    %-8s %5s  %7s  %7s  %7s", "环境", "信号", "5d胜率", "5d均收", "7d胜率")
            for env in ['bull', 'range', 'bear', 'unknown']:
                filtered = [s for s in sigs if s.get('env') == env]
                s5 = qs(filtered, 5)
                s7 = qs(filtered, 7)
                if s5['n'] >= 3:
                    logger.info("    %-8s %5d  %6.1f%%  %6.2f%%  %6.1f%%",
                                 env, s5['n'], s5['wr'], s5['avg'], s7['wr'])


    # ═══ PART 5: 自动穷举最优3/4条件组合 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [5] 自动穷举最优条件组合 (含KD金叉基础)")
    logger.info("=" * 90)

    # 选取有正增量的条件做穷举
    good_conds = [c for c, _, _, d in cond_perf if d > 0 and c != 'I_KD金叉']
    logger.info("  正增量条件: %d个 — %s", len(good_conds), ', '.join(good_conds))

    # 3条件组合 (KD金叉 + 2个附加)
    logger.info("")
    logger.info("  ── 3条件组合 TOP20 (KD金叉 + 2条件) ──")
    logger.info("  %-40s %5s  %7s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收", "7d胜率")

    combo3_results = []
    for a, b in combinations(good_conds, 2):
        filtered = [s for s in all_sigs
                    if s.get(f'c_{a}', False) and s.get(f'c_{b}', False)]
        s5 = qs(filtered, 5)
        s7 = qs(filtered, 7)
        if s5['n'] >= 10:
            combo3_results.append((f"KD+{a}+{b}", s5, s7, filtered))

    combo3_results.sort(key=lambda x: x[1]['wr'], reverse=True)
    for label, s5, s7, _ in combo3_results[:20]:
        logger.info("  %-40s %5d  %6.1f%%  %6.2f%%  %6.1f%%",
                     label, s5['n'], s5['wr'], s5['avg'], s7['wr'])

    # 4条件组合 (KD金叉 + 3个附加)
    logger.info("")
    logger.info("  ── 4条件组合 TOP20 (KD金叉 + 3条件) ──")
    logger.info("  %-55s %5s  %7s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收", "7d胜率")

    combo4_results = []
    for a, b, c in combinations(good_conds, 3):
        filtered = [s for s in all_sigs
                    if s.get(f'c_{a}', False) and s.get(f'c_{b}', False) and s.get(f'c_{c}', False)]
        s5 = qs(filtered, 5)
        s7 = qs(filtered, 7)
        if s5['n'] >= 10:
            combo4_results.append((f"KD+{a}+{b}+{c}", s5, s7, filtered))

    combo4_results.sort(key=lambda x: x[1]['wr'], reverse=True)
    for label, s5, s7, _ in combo4_results[:20]:
        logger.info("  %-55s %5d  %6.1f%%  %6.2f%%  %6.1f%%",
                     label, s5['n'], s5['wr'], s5['avg'], s7['wr'])

    # 5条件组合 TOP10 (KD金叉 + 4个附加)
    logger.info("")
    logger.info("  ── 5条件组合 TOP10 (KD金叉 + 4条件, 信号>=10) ──")
    logger.info("  %-65s %5s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收")

    # 只用TOP12正增量条件做穷举，避免组合爆炸
    top_conds = [c for c, _, _, _ in cond_perf[:12] if c != 'I_KD金叉']
    combo5_results = []
    for combo in combinations(top_conds, 4):
        filtered = [s for s in all_sigs if all(s.get(f'c_{c}', False) for c in combo)]
        s5 = qs(filtered, 5)
        if s5['n'] >= 10:
            combo5_results.append((f"KD+{'+'.join(combo)}", s5, filtered))

    combo5_results.sort(key=lambda x: x[1]['wr'], reverse=True)
    for label, s5, _ in combo5_results[:10]:
        logger.info("  %-65s %5d  %6.1f%%  %6.2f%%",
                     label, s5['n'], s5['wr'], s5['avg'])

    # ═══ PART 6: TOP5策略 + 大盘过滤 + 季度CV ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [6] TOP策略 季度稳定性 + 大盘过滤")
    logger.info("=" * 90)

    # 收集所有候选
    all_candidates = []
    for label, s5, s7, sigs in combo3_results[:5]:
        all_candidates.append((label, sigs, s5))
    for label, s5, s7, sigs in combo4_results[:5]:
        all_candidates.append((label, sigs, s5))
    for sname, sigs, s5 in preset_results[:5]:
        all_candidates.append((sname, sigs, s5))

    # 去重并排序
    seen = set()
    unique_candidates = []
    for label, sigs, s5 in all_candidates:
        if label not in seen:
            seen.add(label)
            unique_candidates.append((label, sigs, s5))
    unique_candidates.sort(key=lambda x: x[2]['wr'], reverse=True)

    for label, sigs, s5 in unique_candidates[:10]:
        folds = cv_quarters(sigs, 5)
        if not folds:
            continue
        q_wrs = [f['wr'] for f in folds]
        above50 = sum(1 for w in q_wrs if w > 50)

        logger.info("")
        logger.info("  ── %s (胜率%.1f%%, %d信号, %d/%d季度>50%%) ──",
                     label, s5['wr'], s5['n'], above50, len(q_wrs))
        logger.info("    %-8s %5s  %7s  %7s", "季度", "信号", "胜率", "均收益")
        for f in folds:
            marker = " ★" if f['wr'] >= 55 else (" ▲" if f['wr'] >= 50 else " ▼")
            logger.info("    %-8s %5d  %6.1f%%%s  %6.2f%%",
                         f['q'], f['n'], f['wr'], marker, f['avg'])

        # 大盘过滤后
        if has_index:
            bull_sigs = [s for s in sigs if s.get('env') in ('bull', 'range')]
            s5_bull = qs(bull_sigs, 5)
            if s5_bull['n'] > 0:
                logger.info("    → 排除熊市后: %d信号, 胜率%.1f%%, 均收%.2f%%",
                             s5_bull['n'], s5_bull['wr'], s5_bull['avg'])

    # ═══ PART 7: 最终推荐 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [7] 最终推荐")
    logger.info("=" * 90)

    # 综合评分: 胜率*0.4 + 信号量归一化*0.2 + 季度稳定性*0.4
    final_ranking = []
    for label, sigs, s5 in unique_candidates:
        if s5['n'] < 10:
            continue
        folds = cv_quarters(sigs, 5)
        q_wrs = [f['wr'] for f in folds]
        stability = sum(1 for w in q_wrs if w > 50) / max(len(q_wrs), 1)
        sig_score = min(s5['n'] / 200, 1.0)  # 200信号满分
        composite = s5['wr'] * 0.4 + sig_score * 20 + stability * 40
        final_ranking.append((label, s5, stability, composite, sigs))

    final_ranking.sort(key=lambda x: x[3], reverse=True)

    logger.info("  %-55s %5s  %7s  %7s  %6s  %6s",
                "策略", "信号", "5d胜率", "5d均收", "稳定性", "综合分")
    for label, s5, stab, comp, _ in final_ranking[:10]:
        logger.info("  %-55s %5d  %6.1f%%  %6.2f%%  %5.0f%%  %6.1f",
                     label, s5['n'], s5['wr'], s5['avg'], stab * 100, comp)

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 90)

    # 保存
    result = {
        'run_time': str(datetime.now()),
        'total_signals': len(all_sigs),
        'stock_count': len(codes),
        'condition_contributions': [
            {'name': c, 'signals': s5['n'], 'wr_5d': s5['wr'], 'avg_5d': s5['avg'], 'delta': round(d, 1)}
            for c, s5, _, d in cond_perf
        ],
        'top_combos_3': [
            {'label': l, 'signals': s5['n'], 'wr_5d': s5['wr'], 'avg_5d': s5['avg']}
            for l, s5, _, _ in combo3_results[:10]
        ],
        'top_combos_4': [
            {'label': l, 'signals': s5['n'], 'wr_5d': s5['wr'], 'avg_5d': s5['avg']}
            for l, s5, _, _ in combo4_results[:10]
        ],
        'final_ranking': [
            {'label': l, 'signals': s5['n'], 'wr_5d': s5['wr'], 'avg_5d': s5['avg'],
             'stability': round(stab, 2), 'composite': round(comp, 1)}
            for l, s5, stab, comp, _ in final_ranking[:10]
        ],
    }
    try:
        with open('data_results/tdx_formula_v2_result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("  结果已保存到 data_results/tdx_formula_v2_result.json")
    except Exception as e:
        logger.warning("  保存失败: %s", e)

    return all_sigs


if __name__ == '__main__':
    run(sample_limit=1000)
