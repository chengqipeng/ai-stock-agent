#!/usr/bin/env python3
"""
5大策略深度调优 v3 — 聚焦最优组合精细验证
==========================================
基于v2发现，聚焦以下有价值的组合做精细验证：
  A. S1暴力洗盘 + KDJ金叉区 (75%胜率, 20信号)
  B. S2堆量挖坑 + 均线多头 (70.6%胜率, 17信号)
  C. S3L蜻蜓点水宽 + 布林收窄+下轨 (58.2%胜率, 1726信号)
  D. 5策略宽松OR + 布林收窄+下轨+RSI适中 (58.3%胜率, 1771信号)
  E. S0试盘线 + 布林收窄+RSI适中 (49.4%胜率, 243信号, 均收1.45%)
  F. S1暴力洗盘 牛市环境下 (56.2%胜率 in range)

本轮重点：
  1. 对A/B做参数微调扩大信号量
  2. 对C/D做严格时间序列CV
  3. 混合A+B+C+D+E做加权投票
  4. 市场环境条件过滤
  5. 最终推荐最优实战组合

用法：
    python -m day_week_predicted.backtest.five_strategy_deep_v3
"""
import sys
import logging
import json
from datetime import datetime
from collections import defaultdict

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


def _sd(a, b, d=0.0):
    return a / b if b and b != 0 else d


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
    ma250 = calc_ma(c, 250)
    ema20 = calc_ema(c, 20)
    ema40 = calc_ema(c, 40)
    ema60 = calc_ema(c, 60)
    vm5 = calc_ma(v, 5)
    vm10 = calc_ma(v, 10)
    vm20 = calc_ma(v, 20)
    dif, dea, mb = calc_macd(c)
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
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60, 'ma250': ma250,
        'ema20': ema20, 'ema40': ema40, 'ema60': ema60,
        'vm5': vm5, 'vm10': vm10, 'vm20': vm20,
        'dif': dif, 'dea': dea, 'mb': mb,
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
#  精调策略 — 基于v2发现的最优参数
# ═══════════════════════════════════════════════════════════════════

def combo_A(ind, i, code):
    """S1暴力洗盘 + KDJ金叉区 (v2最高收益组合)
    微调：跌幅>3.5%(放宽), 近12日有涨停, 多头MA5>MA10>MA20, KDJ金叉K<80"""
    if i < 60 or i < 1 or ind['c'][i - 1] <= 0:
        return False
    c = ind['c']
    drop = (c[i] - c[i - 1]) / c[i - 1] * 100
    if drop >= -3.5:
        return False
    has_zt = any(c[j] / c[j - 1] >= 0.095 for j in range(max(1, i - 11), i + 1) if c[j - 1] > 0)
    if not has_zt:
        return False
    if not (ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0):
        return False
    # KDJ金叉区
    if not (ind['k'][i] > ind['d'][i] and ind['k'][i] < 80):
        return False
    return True


def combo_A2(ind, i, code):
    """S1暴力洗盘 + KDJ金叉区 + MACD多头 (更严格)"""
    if not combo_A(ind, i, code):
        return False
    return ind['dif'][i] > ind['dea'][i]


def combo_B(ind, i, code):
    """S2堆量挖坑 + 均线多头 (v2高胜率组合)
    微调：放宽量能金叉为量能接近金叉"""
    if i < 60 or code.startswith('688'):
        return False
    c, l, v = ind['c'], ind['l'], ind['v']
    vm5, vm10 = ind['vm5'], ind['vm10']
    if vm5[i] <= 0 or vm10[i] <= 0 or i < 2:
        return False
    # 量能金叉或接近金叉(差距<5%)
    near_cross = vm5[i] > vm10[i] * 0.95
    if not near_cross:
        return False
    # 放量
    if v[i] <= vm5[i] * 1.1:
        return False
    # 近5日有回调
    if i < 5 or c[i - 5] <= 0:
        return False
    if min(c[j] for j in range(i - 4, i)) / c[i - 5] >= 1.0:
        return False
    # 收盘在MA10附近或上方
    ma10v = ind['ma10'][i]
    if ma10v <= 0 or c[i] < ma10v * 0.97:
        return False
    # 均线多头
    if not (ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0):
        return False
    # MA60向上
    ma60v = ind['ma60'][i]
    if ma60v <= 0 or c[i] <= ma60v * 0.95:
        return False
    return True


def combo_C(ind, i, code):
    """S3L蜻蜓点水宽 + 布林收窄 + 布林下轨 (v2大样本高胜率)"""
    if i < 60:
        return False
    cv, ov, hv, lv = ind['c'][i], ind['o'][i], ind['h'][i], ind['l'][i]
    v = ind['v']
    ma20v, ma60v = ind['ma20'][i], ind['ma60'][i]
    if ma20v <= 0 and ma60v <= 0:
        return False
    # 回踩均线
    hui_20 = ma20v > 0 and abs(lv - ma20v) / ma20v <= 0.05 and cv > ma20v * 0.99
    hui_60 = ma60v > 0 and abs(lv - ma60v) / ma60v <= 0.05 and cv > ma60v * 0.99
    if not (hui_20 or hui_60):
        return False
    # 下影线
    amp = hv - lv
    if amp <= 0:
        return False
    if (min(ov, cv) - lv) / amp <= 0.20:
        return False
    # 收阳
    if (cv - ov) / max(ov, 0.01) <= -0.02:
        return False
    # 量不萎缩
    if i < 1 or v[i - 1] <= 0 or v[i] < v[i - 1] * 0.8:
        return False
    # 布林收窄
    mid = ind['ma20'][i]
    if mid <= 0:
        return False
    width = (ind['boll_up'][i] - ind['boll_dn'][i]) / mid
    if width >= 0.10:
        return False
    # 布林下轨附近
    dn = ind['boll_dn'][i]
    if dn <= 0 or cv > (mid + dn) / 2:
        return False
    return True


def combo_D(ind, i, code):
    """布林收窄 + 布林下轨 + RSI适中 (通用过滤器, 适用于任何信号)"""
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


def combo_E(ind, i, code):
    """试盘线 + 布林收窄 + RSI适中"""
    if i < 50:
        return False
    cv, ov, hv, lv = ind['c'][i], ind['o'][i], ind['h'][i], ind['l'][i]
    v = ind['v'][i]
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
    if v / pv <= 2:
        return False
    if v < max(ind['v'][j] for j in range(i - 4, i + 1)):
        return False
    llv20 = min(ind['l'][j] for j in range(i - 19, i + 1))
    if llv20 <= 0 or lv / llv20 >= 1.2:
        return False
    hhv20 = max(ind['h'][j] for j in range(i - 19, i + 1))
    if hhv20 <= 0 or lv / hhv20 <= 0.9:
        return False
    # 布林收窄
    mid = ind['ma20'][i]
    if mid <= 0:
        return False
    width = (ind['boll_up'][i] - ind['boll_dn'][i]) / mid
    if width >= 0.10:
        return False
    # RSI适中
    if not (40 <= ind['rsi14'][i] <= 70):
        return False
    return True


def combo_F_any_signal_with_filter(ind, i, code):
    """任何5策略宽松信号 + 布林收窄+下轨+RSI (v2最佳大样本组合)"""
    # 先检查过滤器
    if not combo_D(ind, i, code):
        return False
    # 再检查是否有任何宽松策略触发
    # S1L
    if i >= 60 and i >= 1 and ind['c'][i - 1] > 0:
        c = ind['c']
        drop = (c[i] - c[i - 1]) / c[i - 1] * 100
        if drop < -3.0:
            has_zt = any(c[j] / c[j - 1] >= 0.095 for j in range(max(1, i - 12), i + 1) if c[j - 1] > 0)
            if has_zt and ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0:
                w = sum(1 for p in c[max(0, i - 60):i + 1] if p <= c[i]) / min(61, i + 1)
                if 0.50 <= w <= 0.95:
                    return True
    # S2L
    if i >= 60 and not code.startswith('688'):
        c, v = ind['c'], ind['v']
        vm5 = ind['vm5']
        if vm5[i] > 0 and v[i] > vm5[i] * 1.1 and i >= 5 and c[i - 5] > 0:
            ma10v = ind['ma10'][i]
            ma60v = ind['ma60'][i]
            if ma10v > 0 and c[i] >= ma10v * 0.98 and ma60v > 0 and c[i] > ma60v * 0.95:
                return True
    # S3L
    if i >= 60:
        cv, ov, hv, lv = ind['c'][i], ind['o'][i], ind['h'][i], ind['l'][i]
        ma20v, ma60v = ind['ma20'][i], ind['ma60'][i]
        if ma20v > 0 or ma60v > 0:
            hui = False
            if ma20v > 0 and abs(lv - ma20v) / ma20v <= 0.05 and cv > ma20v * 0.99:
                hui = True
            if ma60v > 0 and abs(lv - ma60v) / ma60v <= 0.05 and cv > ma60v * 0.99:
                hui = True
            if hui:
                amp = hv - lv
                if amp > 0 and (min(ov, cv) - lv) / amp > 0.20:
                    if (cv - ov) / max(ov, 0.01) > -0.02:
                        if i >= 1 and ind['v'][i - 1] > 0 and ind['v'][i] >= ind['v'][i - 1] * 0.8:
                            return True
    # S4L
    if i >= 60:
        cv, ov, v = ind['c'][i], ind['o'][i], ind['v'][i]
        e20, e40, e60 = ind['ema20'][i], ind['ema40'][i], ind['ema60'][i]
        vm10 = ind['vm10'][i]
        if e60 > 0 and e20 > 0 and vm10 > 0:
            if ov < max(e20, e40, e60):
                above = sum(1 for e in [e20, e40, e60] if cv > e)
                if above >= 2 and v / vm10 > 1.0:
                    if i >= 1 and ind['c'][i - 1] > 0 and cv / ind['c'][i - 1] > 1.03:
                        max_e = max(e20, e40, e60)
                        min_e = min(e20, e40, e60)
                        if min_e > 0 and max_e / min_e < 1.15 and ind['dif'][i] > ind['dea'][i]:
                            return True
    # S5L
    if i >= 20 and not code.startswith('688'):
        c, o, v = ind['c'], ind['o'], ind['v']
        if v[i] > 1:
            zt = sum(1 for j in range(max(1, i - 6), i + 1) if c[j - 1] > 0 and c[j] / c[j - 1] >= 1.095)
            if zt >= 1 and c[i] < o[i]:
                yang = sum(1 for j in range(max(0, i - 4), i + 1) if c[j] >= o[j])
                if yang >= 2:
                    vm20 = ind['vm20'][i]
                    if vm20 > 0 and v[i] / vm20 < 3.5 and c[i] > ind['ma5'][i] * 0.97:
                        return True
    return False


# ═══════════════════════════════════════════════════════════════════
#  加权投票系统
# ═══════════════════════════════════════════════════════════════════

def weighted_vote(hits: dict) -> float:
    """基于v2验证的胜率给各策略加权打分"""
    weights = {
        'A_洗盘KDJ': 3.0,      # 75%胜率
        'A2_洗盘KDJ_MACD': 3.5, # 更严格
        'B_堆量均线': 2.8,      # 70.6%胜率
        'C_蜻蜓布林': 2.0,      # 58.2%胜率, 大样本
        'E_试盘线布林': 1.5,     # 49.4%胜率, 高均收
        'F_宽松+过滤': 2.0,     # 58.3%胜率
    }
    score = 0.0
    for name, w in weights.items():
        if hits.get(name, False):
            score += w
    return score


COMBOS = {
    'A_洗盘KDJ': combo_A,
    'A2_洗盘KDJ_MACD': combo_A2,
    'B_堆量均线': combo_B,
    'C_蜻蜓布林': combo_C,
    'E_试盘线布林': combo_E,
    'F_宽松+过滤': combo_F_any_signal_with_filter,
}


# ═══════════════════════════════════════════════════════════════════
#  单股回测
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
        for name, func in COMBOS.items():
            try:
                hits[name] = func(ind, i, stock_code)
            except Exception:
                hits[name] = False

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
            'score': weighted_vote(hits),
        }

        for hd in HOLD_DAYS:
            si = i + 1 + hd
            if si < n:
                sp = float(klines[si]['close_price'])
                sig[f'r{hd}'] = round((sp - bp) / bp * 100, 2)

        for name, hit in hits.items():
            sig[f'h_{name}'] = hit

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
        'n': t,
        'wr': round(w / t * 100, 1),
        'avg': round(sum(rets) / t, 2),
        'med': round(sr[t // 2], 2),
        'mx': round(max(rets), 2),
        'mn': round(min(rets), 2),
    }


# ═══════════════════════════════════════════════════════════════════
#  主回测
# ═══════════════════════════════════════════════════════════════════

def run(sample_limit=1000):
    t0 = datetime.now()
    logger.info("=" * 85)
    logger.info("  5大策略深度调优 v3 — 聚焦最优组合精细验证")
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

    # ═══ PART 1: 各精调组合独立表现 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [1] 各精调组合独立表现")
    logger.info("=" * 85)
    logger.info("  %-22s %5s  %7s  %7s  %7s  │ %7s  %7s  %7s",
                "组合", "信号", "3d胜率", "3d均收", "3d中位", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 85)

    combo_perf = {}
    for name in COMBOS:
        filtered = [s for s in all_sigs if s.get(f'h_{name}', False)]
        s3 = qs(filtered, 3)
        s5 = qs(filtered, 5)
        s7 = qs(filtered, 7)
        combo_perf[name] = {'s3': s3, 's5': s5, 's7': s7, 'sigs': filtered}
        if s5['n'] > 0:
            logger.info("  %-22s %5d  %6.1f%%  %6.2f%%  %6.2f%%  │ %6.1f%%  %6.2f%%  %6.2f%%",
                         name, s5['n'],
                         s3['wr'], s3['avg'], s3['med'],
                         s5['wr'], s5['avg'], s5['med'])
        else:
            logger.info("  %-22s %5d  (无信号)", name, 0)

    # ═══ PART 2: 市场环境分层 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [2] 市场环境分层 (5日)")
    logger.info("=" * 85)

    for name in COMBOS:
        sigs = combo_perf[name]['sigs']
        if len(sigs) < 5:
            continue
        logger.info("  ── %s ──", name)
        for env in ['bull', 'range', 'bear']:
            es = [s for s in sigs if s.get('env') == env]
            st = qs(es, 5)
            if st['n'] > 0:
                logger.info("    %-6s %4d信号  胜率%5.1f%%  均收%6.2f%%  中位%6.2f%%",
                             env, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 3: 加权投票系统 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [3] 加权投票系统 (按得分阈值)")
    logger.info("=" * 85)
    logger.info("  %-25s %5s  %7s  %7s  %7s  %7s",
                "阈值", "信号", "3d胜率", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 70)

    for threshold in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]:
        filtered = [s for s in all_sigs if s.get('score', 0) >= threshold]
        s3 = qs(filtered, 3)
        s5 = qs(filtered, 5)
        if s5['n'] > 0:
            logger.info("  score >= %-5.1f           %5d  %6.1f%%  %6.1f%%  %6.2f%%  %6.2f%%",
                         threshold, s5['n'], s3['wr'], s5['wr'], s5['avg'], s5['med'])

    # ═══ PART 4: 加权投票 + 市场环境 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [4] 加权投票 + 市场环境过滤")
    logger.info("=" * 85)
    logger.info("  %-35s %5s  %7s  %7s  %7s",
                "条件", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 70)

    for threshold in [1.5, 2.0, 2.5, 3.0]:
        for env_filter in [None, 'bull', 'range', 'bear', 'not_bear']:
            if env_filter == 'not_bear':
                filtered = [s for s in all_sigs if s.get('score', 0) >= threshold and s.get('env') != 'bear']
            elif env_filter:
                filtered = [s for s in all_sigs if s.get('score', 0) >= threshold and s.get('env') == env_filter]
            else:
                filtered = [s for s in all_sigs if s.get('score', 0) >= threshold]
            st = qs(filtered, 5)
            if st['n'] >= 3:
                label = f"score>={threshold}" + (f" & {env_filter}" if env_filter else "")
                logger.info("  %-35s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                             label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 5: OR联合 + 过滤器 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [5] 策略OR联合组合")
    logger.info("=" * 85)
    logger.info("  %-40s %5s  %7s  %7s  %7s",
                "组合", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 70)

    or_combos = [
        ('A+B (洗盘KDJ OR 堆量均线)', ['A_洗盘KDJ', 'B_堆量均线']),
        ('A+C (洗盘KDJ OR 蜻蜓布林)', ['A_洗盘KDJ', 'C_蜻蜓布林']),
        ('A+B+C', ['A_洗盘KDJ', 'B_堆量均线', 'C_蜻蜓布林']),
        ('A+B+E (高质量三合一)', ['A_洗盘KDJ', 'B_堆量均线', 'E_试盘线布林']),
        ('A+B+C+E (四合一)', ['A_洗盘KDJ', 'B_堆量均线', 'C_蜻蜓布林', 'E_试盘线布林']),
        ('B+C (堆量+蜻蜓)', ['B_堆量均线', 'C_蜻蜓布林']),
        ('B+E (堆量+试盘线)', ['B_堆量均线', 'E_试盘线布林']),
        ('C+E (蜻蜓+试盘线)', ['C_蜻蜓布林', 'E_试盘线布林']),
        ('全部OR', list(COMBOS.keys())),
        ('F_宽松+过滤 单独', ['F_宽松+过滤']),
    ]

    or_perf = {}
    for label, strats in or_combos:
        filtered = [s for s in all_sigs if any(s.get(f'h_{sn}', False) for sn in strats)]
        st = qs(filtered, 5)
        or_perf[label] = {'st': st, 'sigs': filtered}
        if st['n'] > 0:
            logger.info("  %-40s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                         label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 6: 时间序列交叉验证 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [6] 时间序列交叉验证 (按季度)")
    logger.info("=" * 85)

    # 对所有有意义的组合做CV
    cv_targets = {}
    for name in COMBOS:
        sigs = combo_perf[name]['sigs']
        if len(sigs) >= 10:
            cv_targets[name] = sigs
    for label, data in or_perf.items():
        if data['st']['n'] >= 10:
            cv_targets[f'OR:{label}'] = data['sigs']
    # 加权投票
    for thr in [2.0, 3.0]:
        filtered = [s for s in all_sigs if s.get('score', 0) >= thr]
        if len(filtered) >= 10:
            cv_targets[f'Vote>={thr}'] = filtered

    for cv_name, sigs in cv_targets.items():
        filtered = [s for s in sigs if 'r5' in s]
        if len(filtered) < 10:
            continue

        quarters = defaultdict(list)
        for s in filtered:
            d = s['date']
            y, m = d[:4], int(d[5:7])
            q = f"{y}Q{(m - 1) // 3 + 1}"
            quarters[q].append(s['r5'])

        sorted_qs = sorted(quarters.keys())
        if len(sorted_qs) < 2:
            continue

        logger.info("")
        logger.info("  ── %s (%d信号) ──", cv_name, len(filtered))
        logger.info("    %-8s %5s  %7s  %7s", "季度", "信号", "胜率", "均收益")

        q_wrs = []
        for q in sorted_qs:
            rets = quarters[q]
            wr = sum(1 for r in rets if r > 0) / len(rets) * 100
            avg = sum(rets) / len(rets)
            q_wrs.append(wr)
            logger.info("    %-8s %5d  %6.1f%%  %6.2f%%", q, len(rets), wr, avg)

        # 稳定性指标
        above50 = sum(1 for w in q_wrs if w > 50)
        avg_wr = sum(q_wrs) / len(q_wrs)
        wr_std = (sum((w - avg_wr) ** 2 for w in q_wrs) / len(q_wrs)) ** 0.5
        logger.info("    汇总: %d/%d季度>50%% | 均胜率%.1f%% | 胜率标准差%.1f%%",
                     above50, len(q_wrs), avg_wr, wr_std)

    # ═══ PART 7: 综合排名 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [7] 综合排名 (信号>=5, 按5日胜率)")
    logger.info("=" * 85)

    all_tested = []
    for name in COMBOS:
        st = combo_perf[name]['s5']
        if st['n'] >= 5:
            all_tested.append((name, st['n'], st['wr'], st['avg'], st['med']))
    for label, data in or_perf.items():
        st = data['st']
        if st['n'] >= 5:
            all_tested.append((f'OR:{label}', st['n'], st['wr'], st['avg'], st['med']))
    for thr in [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]:
        filtered = [s for s in all_sigs if s.get('score', 0) >= thr]
        st = qs(filtered, 5)
        if st['n'] >= 5:
            all_tested.append((f'Vote>={thr}', st['n'], st['wr'], st['avg'], st['med']))
    # 投票+环境
    for thr in [2.0, 3.0]:
        for env in ['not_bear', 'range']:
            if env == 'not_bear':
                filtered = [s for s in all_sigs if s.get('score', 0) >= thr and s.get('env') != 'bear']
            else:
                filtered = [s for s in all_sigs if s.get('score', 0) >= thr and s.get('env') == env]
            st = qs(filtered, 5)
            if st['n'] >= 5:
                all_tested.append((f'Vote>={thr}&{env}', st['n'], st['wr'], st['avg'], st['med']))

    all_tested.sort(key=lambda x: x[2], reverse=True)

    logger.info("  %-50s %5s  %7s  %7s  %7s",
                "组合", "信号", "5d胜率", "5d均收", "5d中位")
    logger.info("  " + "-" * 85)
    for label, n, wr, avg, med in all_tested[:25]:
        logger.info("  %-50s %5d  %6.1f%%  %6.2f%%  %6.2f%%",
                     label, n, wr, avg, med)

    # ═══ PART 8: 最终推荐 ═══
    logger.info("")
    logger.info("=" * 85)
    logger.info("  [8] 最终推荐")
    logger.info("=" * 85)

    # 高胜率小样本
    high_wr = [x for x in all_tested if x[2] >= 60 and x[1] >= 5]
    if high_wr:
        best = high_wr[0]
        logger.info("  高胜率推荐: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     best[0], best[2], best[3], best[1])

    # 大样本稳定
    large_sample = [x for x in all_tested if x[1] >= 100 and x[2] >= 55]
    if large_sample:
        best = max(large_sample, key=lambda x: x[2])
        logger.info("  大样本推荐: %s (胜率%.1f%%, 均收%.2f%%, %d信号)",
                     best[0], best[2], best[3], best[1])

    # 均衡推荐
    balanced = [x for x in all_tested if x[1] >= 30 and x[2] >= 55]
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
        with open('data_results/five_strategy_deep_v3_result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("  结果已保存到 data_results/five_strategy_deep_v3_result.json")
    except Exception as e:
        logger.warning("  保存失败: %s", e)

    return all_sigs


if __name__ == '__main__':
    run(sample_limit=1000)
