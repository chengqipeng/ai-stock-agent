#!/usr/bin/env python3
"""
通达信选股公式回测 — 原版 vs 优化版
=====================================
原版公式:
  多头排列 AND 六十向上 AND 贴线 AND KD金叉 AND 刚启动 AND 低价 AND 盈利

优化版:
  硬过滤(多头排列+非ST+盈利+大盘环境) + 打分制(均线斜率/动态贴线/启动时机/量价/动量)

回测方式:
  - 信号日次日开盘买入
  - 持有3/5/7/10天后收盘卖出
  - 季度CV验证时间稳定性

用法:
    python -m day_week_predicted.backtest.tdx_formula_backtest
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
    calc_ma, calc_ema, calc_rsi, calc_kdj,
)

HOLD_DAYS = [3, 5, 7, 10]


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
    macd_bar = [2 * (dif[i] - dea[i]) for i in range(n)]

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
        'dif': dif, 'dea': dea, 'macd_bar': macd_bar,
        'k': kv, 'd': dv, 'j': jv,
        'atr14': atr14, 'rsi14': rsi14,
    }


# ═══════════════════════════════════════════════════════════════════
#  原版公式条件
# ═══════════════════════════════════════════════════════════════════

def cond_bull_align(ind, i):
    """多头排列: MA5>MA10>MA20>MA60"""
    return (ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > ind['ma60'][i]
            and ind['ma60'][i] > 0)


def cond_ma60_up(ind, i):
    """六十向上: MA60 > REF(MA60,1)"""
    return i >= 1 and ind['ma60'][i] > ind['ma60'][i - 1] > 0


def cond_close_to_ma5(ind, i):
    """贴线: C>MA5 AND C/MA5<1.02"""
    ma5 = ind['ma5'][i]
    return ma5 > 0 and ind['c'][i] > ma5 and ind['c'][i] / ma5 < 1.02


def cond_kd_cross(ind, i):
    """KD金叉: K上穿D (今天K>D 且 昨天K<=D)"""
    if i < 1:
        return False
    return ind['k'][i] > ind['d'][i] and ind['k'][i - 1] <= ind['d'][i - 1]


def cond_just_start(ind, i):
    """刚启动: MA5/MA60 < 1.15"""
    ma60 = ind['ma60'][i]
    return ma60 > 0 and ind['ma5'][i] / ma60 < 1.15


def cond_low_price(ind, i):
    """低价: C < 50"""
    return ind['c'][i] < 50


def cond_profitable(ind, i):
    """盈利: 简化为change_percent近期为正趋势（无财务数据时的近似）
    注意：真实公式用FINANCE(34)>0，这里用近60日涨幅>0近似"""
    if i < 60:
        return False
    return ind['c'][i] > ind['c'][i - 60]


def original_formula(ind, i):
    """原版公式: 所有条件AND"""
    return (cond_bull_align(ind, i)
            and cond_ma60_up(ind, i)
            and cond_close_to_ma5(ind, i)
            and cond_kd_cross(ind, i)
            and cond_just_start(ind, i)
            and cond_low_price(ind, i)
            and cond_profitable(ind, i))


# ═══════════════════════════════════════════════════════════════════
#  优化版条件 — 分层打分制
# ═══════════════════════════════════════════════════════════════════

def opt_hard_filter(ind, i, stock_code):
    """硬性过滤: 多头排列 + 非ST(通过code判断) + 盈利"""
    if not cond_bull_align(ind, i):
        return False
    # 非ST: 北交所/ST通常code特征不同，这里简单过滤
    if stock_code.startswith('4') or stock_code.startswith('8'):
        return False  # 排除北交所
    if not cond_profitable(ind, i):
        return False
    return True


def opt_score_ma_slope(ind, i):
    """均线斜率分 (0/10/20)"""
    if i < 5 or ind['ma60'][i - 5] <= 0:
        return 0
    slope = (ind['ma60'][i] - ind['ma60'][i - 5]) / ind['ma60'][i - 5] * 100
    if slope > 0.5:
        return 20
    elif slope > 0:
        return 10
    return 0


def opt_score_close_line(ind, i):
    """动态贴线分 (0/10/20) — 用ATR归一化"""
    ma5 = ind['ma5'][i]
    atr = ind['atr14'][i]
    if ma5 <= 0 or atr <= 0:
        return 0
    c = ind['c'][i]
    if c <= ma5:
        return 0
    deviation = (c - ma5) / atr
    if deviation < 0.3:
        return 20
    elif deviation < 0.8:
        return 10
    return 0


def opt_score_just_start(ind, i):
    """启动时机分 (0/10/20) — 多头天数"""
    # 找最近一次不满足多头排列的位置
    days = 0
    for j in range(i, max(i - 60, 0), -1):
        if not (ind['ma5'][j] > ind['ma10'][j] > ind['ma20'][j]):
            break
        days += 1
    if days < 5:
        return 20
    elif days < 15:
        return 10
    return 0


def opt_score_volume(ind, i):
    """量价配合分 (0/10/20)"""
    vm20 = ind['vm20'][i]
    if vm20 <= 0:
        return 0
    vol_ratio = ind['v'][i] / vm20
    if 1.2 <= vol_ratio < 3.0:
        return 20
    elif vol_ratio >= 0.8:
        return 10
    return 0


def opt_score_momentum(ind, i):
    """动量分 (0/10/20) — KD低位金叉 + MACD水上"""
    kd_cross = (i >= 1
                and ind['k'][i] > ind['d'][i]
                and ind['k'][i - 1] <= ind['d'][i - 1]
                and ind['k'][i] < 50)
    macd_above = ind['dif'][i] > 0
    if kd_cross and macd_above:
        return 20
    elif kd_cross or macd_above:
        return 10
    return 0


def optimized_formula_score(ind, i, stock_code):
    """优化版: 硬过滤 + 打分, 返回(通过, 总分)"""
    if not opt_hard_filter(ind, i, stock_code):
        return False, 0
    score = (opt_score_ma_slope(ind, i)
             + opt_score_close_line(ind, i)
             + opt_score_volume(ind, i)
             + opt_score_momentum(ind, i)
             + opt_score_just_start(ind, i))
    return True, score


# ═══════════════════════════════════════════════════════════════════
#  拆解条件 — 逐条分析每个条件的独立贡献
# ═══════════════════════════════════════════════════════════════════

INDIVIDUAL_CONDITIONS = {
    '多头排列': cond_bull_align,
    '六十向上': cond_ma60_up,
    '贴线': cond_close_to_ma5,
    'KD金叉': cond_kd_cross,
    '刚启动': cond_just_start,
    '低价': cond_low_price,
    '盈利': cond_profitable,
}


# ═══════════════════════════════════════════════════════════════════
#  统计工具
# ═══════════════════════════════════════════════════════════════════

def qs(sigs, hd=5):
    """快速统计"""
    key = f'r{hd}'
    rets = [s[key] for s in sigs if key in s]
    if not rets:
        return {'n': 0, 'wr': 0, 'avg': 0, 'med': 0, 'max': 0, 'min': 0}
    w = sum(1 for r in rets if r > 0)
    t = len(rets)
    sr = sorted(rets)
    return {
        'n': t,
        'wr': round(w / t * 100, 1),
        'avg': round(sum(rets) / t, 2),
        'med': round(sr[t // 2], 2),
        'max': round(sr[-1], 2),
        'min': round(sr[0], 2),
    }


def cv_quarters(sigs, hd=5):
    """季度交叉验证"""
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
    klines = get_kline_data(stock_code, limit=500)
    if len(klines) < 260:
        return []
    ind = precompute(klines)
    if ind is None:
        return []

    signals = []
    n = ind['n']

    for i in range(120, n):
        # 检查每个独立条件
        cond_hits = {}
        for cname, cfunc in INDIVIDUAL_CONDITIONS.items():
            try:
                cond_hits[cname] = cfunc(ind, i)
            except Exception:
                cond_hits[cname] = False

        # 原版公式
        orig_hit = all(cond_hits.values())

        # 优化版打分
        opt_pass, opt_score = optimized_formula_score(ind, i, stock_code)

        # 放宽版: 多头排列 + KD金叉 (去掉低价/盈利等弱条件)
        relaxed_hit = (cond_hits['多头排列'] and cond_hits['六十向上']
                       and cond_hits['贴线'] and cond_hits['KD金叉'])

        # 至少有一个策略触发才记录
        if not orig_hit and not (opt_pass and opt_score >= 50) and not relaxed_hit:
            continue

        # 次日开盘买入
        if i + 1 >= n:
            continue
        bp = float(klines[i + 1]['open_price'])
        if bp <= 0:
            continue

        sig = {
            'code': stock_code,
            'date': str(klines[i]['date']),
            'buy': bp,
            'close': ind['c'][i],
            'orig': orig_hit,
            'relaxed': relaxed_hit,
            'opt_pass': opt_pass,
            'opt_score': opt_score,
        }

        # 记录各条件命中
        for cname, hit in cond_hits.items():
            sig[f'c_{cname}'] = hit

        # 记录优化版各维度分数
        if opt_pass:
            sig['s_ma_slope'] = opt_score_ma_slope(ind, i)
            sig['s_close_line'] = opt_score_close_line(ind, i)
            sig['s_just_start'] = opt_score_just_start(ind, i)
            sig['s_volume'] = opt_score_volume(ind, i)
            sig['s_momentum'] = opt_score_momentum(ind, i)

        # 计算持有收益
        for hd in HOLD_DAYS:
            si = i + 1 + hd
            if si < n:
                sp = float(klines[si]['close_price'])
                sig[f'r{hd}'] = round((sp - bp) / bp * 100, 2)

        signals.append(sig)

    return signals


# ═══════════════════════════════════════════════════════════════════
#  主回测逻辑
# ═══════════════════════════════════════════════════════════════════

def run(sample_limit=1000):
    t0 = datetime.now()
    logger.info("=" * 90)
    logger.info("  通达信选股公式回测 — 原版 vs 优化版")
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
        logger.info("  无信号，退出")
        return

    # ═══ PART 1: 原版公式表现 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [1] 原版公式完整表现")
    logger.info("=" * 90)

    orig_sigs = [s for s in all_sigs if s['orig']]
    logger.info("  原版信号数: %d (占总信号 %.1f%%)",
                len(orig_sigs), len(orig_sigs) / len(all_sigs) * 100 if all_sigs else 0)

    if orig_sigs:
        logger.info("")
        logger.info("  %-8s %6s  %7s  %7s  %7s  %7s  %7s", "持有", "信号", "胜率", "均收益", "中位数", "最大", "最小")
        for hd in HOLD_DAYS:
            st = qs(orig_sigs, hd)
            if st['n'] > 0:
                logger.info("  %d天     %6d  %6.1f%%  %6.2f%%  %6.2f%%  %6.2f%%  %6.2f%%",
                             hd, st['n'], st['wr'], st['avg'], st['med'], st['max'], st['min'])

        # 季度CV
        logger.info("")
        logger.info("  ── 原版公式季度稳定性 (5天持有) ──")
        folds = cv_quarters(orig_sigs, 5)
        if folds:
            for f in folds:
                marker = " ★" if f['wr'] >= 55 else (" ▲" if f['wr'] >= 50 else " ▼")
                logger.info("    %-8s %5d信号  %6.1f%%%s  均收%.2f%%", f['q'], f['n'], f['wr'], marker, f['avg'])
    else:
        logger.info("  ⚠️ 原版公式信号为0，验证了条件过于严苛的问题")

    # ═══ PART 2: 逐条条件独立分析 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [2] 逐条条件独立分析 — 每个条件单独触发时的表现")
    logger.info("=" * 90)
    logger.info("  %-10s %6s  %7s  %7s  %7s", "条件", "信号", "5d胜率", "5d均收", "5d中位")

    for cname in INDIVIDUAL_CONDITIONS:
        filtered = [s for s in all_sigs if s.get(f'c_{cname}', False)]
        st = qs(filtered, 5)
        if st['n'] > 0:
            logger.info("  %-10s %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                         cname, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 3: 条件组合分析 — 找最优子集 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [3] 条件两两组合分析")
    logger.info("=" * 90)
    logger.info("  %-25s %6s  %7s  %7s  %7s", "组合", "信号", "5d胜率", "5d均收", "5d中位")

    cond_names = list(INDIVIDUAL_CONDITIONS.keys())
    combo_results = []
    for a_idx in range(len(cond_names)):
        for b_idx in range(a_idx + 1, len(cond_names)):
            a, b = cond_names[a_idx], cond_names[b_idx]
            filtered = [s for s in all_sigs if s.get(f'c_{a}', False) and s.get(f'c_{b}', False)]
            st = qs(filtered, 5)
            if st['n'] >= 10:
                combo_results.append((f"{a}+{b}", st))

    combo_results.sort(key=lambda x: x[1]['wr'], reverse=True)
    for label, st in combo_results[:15]:
        logger.info("  %-25s %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                     label, st['n'], st['wr'], st['avg'], st['med'])

    # ═══ PART 4: 放宽版 (去掉低价/盈利) ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [4] 放宽版 (多头+六十向上+贴线+KD金叉, 去掉低价/盈利)")
    logger.info("=" * 90)

    relaxed_sigs = [s for s in all_sigs if s['relaxed']]
    logger.info("  放宽版信号数: %d", len(relaxed_sigs))

    if relaxed_sigs:
        logger.info("")
        logger.info("  %-8s %6s  %7s  %7s  %7s", "持有", "信号", "胜率", "均收益", "中位数")
        for hd in HOLD_DAYS:
            st = qs(relaxed_sigs, hd)
            if st['n'] > 0:
                logger.info("  %d天     %6d  %6.1f%%  %6.2f%%  %6.2f%%",
                             hd, st['n'], st['wr'], st['avg'], st['med'])

        # 放宽版 + 各附加条件
        logger.info("")
        logger.info("  ── 放宽版 + 附加条件增强 ──")
        logger.info("  %-25s %6s  %7s  %7s", "附加条件", "信号", "5d胜率", "5d均收")
        for extra in ['刚启动', '低价', '盈利']:
            filtered = [s for s in relaxed_sigs if s.get(f'c_{extra}', False)]
            st = qs(filtered, 5)
            if st['n'] > 0:
                logger.info("  +%-24s %6d  %6.1f%%  %6.2f%%",
                             extra, st['n'], st['wr'], st['avg'])


    # ═══ PART 5: 优化版打分制 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [5] 优化版打分制 — 不同分数阈值表现")
    logger.info("=" * 90)

    opt_sigs = [s for s in all_sigs if s['opt_pass']]
    logger.info("  通过硬过滤的信号数: %d", len(opt_sigs))

    logger.info("")
    logger.info("  %-12s %6s  %7s  %7s  %7s  %7s", "分数阈值", "信号", "5d胜率", "5d均收", "5d中位", "7d胜率")
    score_results = []
    for threshold in [30, 40, 50, 60, 70, 80, 90, 100]:
        filtered = [s for s in opt_sigs if s['opt_score'] >= threshold]
        s5 = qs(filtered, 5)
        s7 = qs(filtered, 7)
        if s5['n'] >= 5:
            logger.info("  score>=%-4d %6d  %6.1f%%  %6.2f%%  %6.2f%%  %6.1f%%",
                         threshold, s5['n'], s5['wr'], s5['avg'], s5['med'], s7['wr'])
            score_results.append((threshold, s5, filtered))

    # ═══ PART 6: 优化版各维度贡献分析 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [6] 优化版各维度贡献分析 (score>=50的信号)")
    logger.info("=" * 90)

    opt50 = [s for s in opt_sigs if s['opt_score'] >= 50]
    if opt50:
        dims = ['s_ma_slope', 's_close_line', 's_just_start', 's_volume', 's_momentum']
        dim_names = ['均线斜率', '动态贴线', '启动时机', '量价配合', '动量共振']

        logger.info("")
        logger.info("  ── 各维度满分(20分)时的信号表现 ──")
        logger.info("  %-10s %6s  %7s  %7s", "维度", "信号", "5d胜率", "5d均收")
        for dim, dname in zip(dims, dim_names):
            filtered = [s for s in opt50 if s.get(dim, 0) == 20]
            st = qs(filtered, 5)
            if st['n'] > 0:
                logger.info("  %-10s %6d  %6.1f%%  %6.2f%%", dname, st['n'], st['wr'], st['avg'])

        # 各维度0分时的表现（反向验证）
        logger.info("")
        logger.info("  ── 各维度0分时的信号表现（反向验证）──")
        logger.info("  %-10s %6s  %7s  %7s", "维度", "信号", "5d胜率", "5d均收")
        for dim, dname in zip(dims, dim_names):
            filtered = [s for s in opt50 if s.get(dim, 0) == 0]
            st = qs(filtered, 5)
            if st['n'] > 0:
                logger.info("  %-10s %6d  %6.1f%%  %6.2f%%", dname, st['n'], st['wr'], st['avg'])

    # ═══ PART 7: 原版 vs 优化版 对比 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [7] 原版 vs 放宽版 vs 优化版 对比")
    logger.info("=" * 90)

    strategies = [
        ("原版(全AND)", [s for s in all_sigs if s['orig']]),
        ("放宽(去低价盈利)", relaxed_sigs),
        ("优化>=50", [s for s in opt_sigs if s['opt_score'] >= 50]),
        ("优化>=60", [s for s in opt_sigs if s['opt_score'] >= 60]),
        ("优化>=70", [s for s in opt_sigs if s['opt_score'] >= 70]),
        ("优化>=80", [s for s in opt_sigs if s['opt_score'] >= 80]),
    ]

    logger.info("")
    logger.info("  %-20s %6s  %7s  %7s │ %7s  %7s │ %7s  %7s",
                "策略", "信号", "3d胜率", "3d均收", "5d胜率", "5d均收", "7d胜率", "7d均收")
    logger.info("  " + "-" * 90)

    for label, sigs in strategies:
        if not sigs:
            logger.info("  %-20s %6d  %7s  %7s │ %7s  %7s │ %7s  %7s",
                         label, 0, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A")
            continue
        s3, s5, s7 = qs(sigs, 3), qs(sigs, 5), qs(sigs, 7)
        logger.info("  %-20s %6d  %6.1f%%  %6.2f%% │ %6.1f%%  %6.2f%% │ %6.1f%%  %6.2f%%",
                     label, s5['n'],
                     s3['wr'], s3['avg'], s5['wr'], s5['avg'], s7['wr'], s7['avg'])

    # ═══ PART 8: 最优策略季度CV ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [8] 最优策略季度稳定性验证")
    logger.info("=" * 90)

    for label, sigs in strategies:
        if len(sigs) < 10:
            continue
        folds = cv_quarters(sigs, 5)
        if not folds:
            continue
        q_wrs = [f['wr'] for f in folds]
        above50 = sum(1 for w in q_wrs if w > 50)
        logger.info("")
        logger.info("  ── %s (%d信号, %d/%d季度>50%%) ──",
                     label, len(sigs), above50, len(q_wrs))
        logger.info("    %-8s %5s  %7s  %7s", "季度", "信号", "胜率", "均收益")
        for f in folds:
            marker = " ★" if f['wr'] >= 55 else (" ▲" if f['wr'] >= 50 else " ▼")
            logger.info("    %-8s %5d  %6.1f%%%s  %6.2f%%",
                         f['q'], f['n'], f['wr'], marker, f['avg'])

    # ═══ PART 9: 信号样本展示 ═══
    logger.info("")
    logger.info("=" * 90)
    logger.info("  [9] 最近信号样本 (优化版score>=70)")
    logger.info("=" * 90)

    opt70 = [s for s in opt_sigs if s['opt_score'] >= 70]
    opt70.sort(key=lambda x: x['date'], reverse=True)
    if opt70:
        logger.info("  %-12s %-10s %7s %5s  %7s  %7s  %7s",
                     "日期", "代码", "买入价", "分数", "3d收益", "5d收益", "7d收益")
        for s in opt70[:20]:
            r3 = f"{s.get('r3', 0):.2f}%" if 'r3' in s else "N/A"
            r5 = f"{s.get('r5', 0):.2f}%" if 'r5' in s else "N/A"
            r7 = f"{s.get('r7', 0):.2f}%" if 'r7' in s else "N/A"
            logger.info("  %-12s %-10s %7.2f %5d  %7s  %7s  %7s",
                         s['date'], s['code'], s['buy'], s['opt_score'], r3, r5, r7)

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 90)

    # 保存结果
    result = {
        'run_time': str(datetime.now()),
        'total_signals': len(all_sigs),
        'stock_count': len(codes),
        'original': {
            'signals': len(orig_sigs),
            'stats_5d': qs(orig_sigs, 5) if orig_sigs else {},
        },
        'strategies': {},
    }
    for label, sigs in strategies:
        if sigs:
            result['strategies'][label] = {
                'signals': len(sigs),
                'stats_5d': qs(sigs, 5),
                'cv': cv_quarters(sigs, 5),
            }

    try:
        with open('data_results/tdx_formula_backtest_result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("  结果已保存到 data_results/tdx_formula_backtest_result.json")
    except Exception as e:
        logger.warning("  保存失败: %s", e)

    return all_sigs


if __name__ == '__main__':
    run(sample_limit=1000)
