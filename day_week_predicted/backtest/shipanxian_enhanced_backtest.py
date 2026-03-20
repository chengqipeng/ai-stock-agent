#!/usr/bin/env python3
"""
试盘线 + 技术指标增强回测
========================
在原始试盘线信号基础上，叠加多种技术指标过滤，对比各组合效果。

增强指标：
  A. MACD 金叉/多头（DIF > DEA）
  B. KDJ 金叉区（K > D 且 K < 80）
  C. RSI 适中区（40 < RSI14 < 70）
  D. 均线多头（MA5 > MA10 > MA20）
  E. 价格站上MA20
  F. 5日均量 > 10日均量（量能趋势）

用法：
    python -m day_week_predicted.backtest.shipanxian_enhanced_backtest
"""
import sys
import logging
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
from day_week_predicted.backtest.shipanxian_backtest import detect_shipanxian


# ─── 技术指标计算 ───────────────────────────────────────────────

def calc_ema(values: list[float], period: int) -> list[float]:
    """计算EMA序列。"""
    ema = [0.0] * len(values)
    if not values:
        return ema
    k = 2.0 / (period + 1)
    ema[0] = values[0]
    for i in range(1, len(values)):
        ema[i] = values[i] * k + ema[i - 1] * (1 - k)
    return ema


def calc_ma(values: list[float], period: int) -> list[float]:
    """计算简单移动平均。"""
    ma = [0.0] * len(values)
    for i in range(period - 1, len(values)):
        ma[i] = sum(values[i - period + 1: i + 1]) / period
    return ma


def calc_macd(closes: list[float]) -> tuple[list[float], list[float], list[float]]:
    """计算MACD(12,26,9)，返回 (DIF, DEA, MACD柱)。"""
    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    dif = [ema12[i] - ema26[i] for i in range(len(closes))]
    dea = calc_ema(dif, 9)
    macd_bar = [2 * (dif[i] - dea[i]) for i in range(len(closes))]
    return dif, dea, macd_bar


def calc_rsi(closes: list[float], period: int = 14) -> list[float]:
    """计算RSI。"""
    rsi = [50.0] * len(closes)
    if len(closes) < period + 1:
        return rsi
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        if i == period:
            if avg_loss == 0:
                rsi[i + 1] = 100.0
            else:
                rsi[i + 1] = 100 - 100 / (1 + avg_gain / avg_loss)
        else:
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            if avg_loss == 0:
                rsi[i + 1] = 100.0
            else:
                rsi[i + 1] = 100 - 100 / (1 + avg_gain / avg_loss)
    return rsi


def calc_kdj(highs: list[float], lows: list[float], closes: list[float],
             n: int = 9) -> tuple[list[float], list[float], list[float]]:
    """计算KDJ(9,3,3)。"""
    length = len(closes)
    k_vals = [50.0] * length
    d_vals = [50.0] * length
    j_vals = [50.0] * length

    for i in range(n - 1, length):
        hn = max(highs[i - n + 1: i + 1])
        ln = min(lows[i - n + 1: i + 1])
        if hn == ln:
            rsv = 50.0
        else:
            rsv = (closes[i] - ln) / (hn - ln) * 100

        if i == n - 1:
            k_vals[i] = rsv
            d_vals[i] = rsv
        else:
            k_vals[i] = 2 / 3 * k_vals[i - 1] + 1 / 3 * rsv
            d_vals[i] = 2 / 3 * d_vals[i - 1] + 1 / 3 * k_vals[i]
        j_vals[i] = 3 * k_vals[i] - 2 * d_vals[i]

    return k_vals, d_vals, j_vals


# ─── 预计算所有指标 ─────────────────────────────────────────────

def precompute_indicators(klines: list[dict]) -> dict:
    """对整段K线预计算所有技术指标，返回等长数组字典。"""
    closes = [float(k['close_price']) for k in klines]
    highs = [float(k['high_price']) for k in klines]
    lows = [float(k['low_price']) for k in klines]
    vols = [float(k['trading_volume']) for k in klines]

    dif, dea, macd_bar = calc_macd(closes)
    rsi14 = calc_rsi(closes, 14)
    k_vals, d_vals, j_vals = calc_kdj(highs, lows, closes)
    ma5 = calc_ma(closes, 5)
    ma10 = calc_ma(closes, 10)
    ma20 = calc_ma(closes, 20)
    vol_ma5 = calc_ma(vols, 5)
    vol_ma10 = calc_ma(vols, 10)

    return {
        'closes': closes, 'highs': highs, 'lows': lows, 'vols': vols,
        'dif': dif, 'dea': dea, 'macd_bar': macd_bar,
        'rsi14': rsi14,
        'k': k_vals, 'd': d_vals, 'j': j_vals,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20,
        'vol_ma5': vol_ma5, 'vol_ma10': vol_ma10,
    }


# ─── 过滤条件定义 ──────────────────────────────────────────────

FILTERS = {
    'MACD多头': lambda ind, i: ind['dif'][i] > ind['dea'][i],
    'MACD金叉': lambda ind, i: ind['dif'][i] > ind['dea'][i] and ind['dif'][i - 1] <= ind['dea'][i - 1],
    'KDJ金叉区': lambda ind, i: ind['k'][i] > ind['d'][i] and ind['k'][i] < 80,
    'RSI适中': lambda ind, i: 40 < ind['rsi14'][i] < 70,
    '均线多头': lambda ind, i: ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0,
    '站上MA20': lambda ind, i: ind['closes'][i] > ind['ma20'][i] > 0,
    '量能趋势': lambda ind, i: ind['vol_ma5'][i] > ind['vol_ma10'][i] > 0,
    'MACD柱翻红': lambda ind, i: ind['macd_bar'][i] > 0,
    'KDJ低位': lambda ind, i: ind['k'][i] < 50 and ind['d'][i] < 50,
    'RSI不超买': lambda ind, i: ind['rsi14'][i] < 65,
}

# 要测试的组合策略
STRATEGIES = {
    '原始试盘线': [],
    '+MACD多头': ['MACD多头'],
    '+RSI适中': ['RSI适中'],
    '+KDJ金叉区': ['KDJ金叉区'],
    '+均线多头': ['均线多头'],
    '+站上MA20': ['站上MA20'],
    '+量能趋势': ['量能趋势'],
    '+MACD+RSI': ['MACD多头', 'RSI适中'],
    '+MACD+KDJ': ['MACD多头', 'KDJ金叉区'],
    '+MACD+MA20': ['MACD多头', '站上MA20'],
    '+MACD+RSI+MA20': ['MACD多头', 'RSI适中', '站上MA20'],
    '+MACD+KDJ+MA20': ['MACD多头', 'KDJ金叉区', '站上MA20'],
    '+全指标': ['MACD多头', 'RSI适中', 'KDJ金叉区', '站上MA20', '量能趋势'],
}


# ─── 回测核心 ──────────────────────────────────────────────────

def backtest_stock_enhanced(stock_code: str, hold_days_list: list[int]) -> list[dict]:
    """对单只股票检测试盘线信号并记录各指标状态。"""
    klines = get_kline_data(stock_code, limit=500)
    if len(klines) < 60:
        return []

    indicators = precompute_indicators(klines)
    signals = []

    for i in range(50, len(klines)):
        if not detect_shipanxian(klines, i):
            continue

        # 次日开盘买入
        if i + 1 >= len(klines):
            continue
        buy_price = float(klines[i + 1]['open_price'])
        if buy_price <= 0:
            continue

        sig = {
            'stock_code': stock_code,
            'signal_date': str(klines[i]['date']),
            'buy_date': str(klines[i + 1]['date']),
            'buy_price': buy_price,
        }

        # 记录各持有期收益
        for hd in hold_days_list:
            sell_idx = i + 1 + hd
            if sell_idx < len(klines):
                sell_price = float(klines[sell_idx]['close_price'])
                sig[f'return_{hd}d'] = round((sell_price - buy_price) / buy_price * 100, 2)

        # 记录各过滤条件是否满足
        for fname, ffunc in FILTERS.items():
            try:
                sig[f'filter_{fname}'] = ffunc(indicators, i)
            except Exception:
                sig[f'filter_{fname}'] = False

        signals.append(sig)

    return signals


def calc_stats(signals: list[dict], hold_day: int) -> dict:
    """计算一组信号的统计指标。"""
    key = f'return_{hold_day}d'
    returns = [s[key] for s in signals if key in s]
    if not returns:
        return {'count': 0}

    wins = sum(1 for r in returns if r > 0)
    total = len(returns)
    avg = sum(returns) / total
    sorted_r = sorted(returns)
    median = sorted_r[total // 2]

    return {
        'count': total,
        'win_rate': round(wins / total * 100, 1),
        'avg_return': round(avg, 2),
        'median_return': round(median, 2),
        'max_return': round(max(returns), 2),
        'min_return': round(min(returns), 2),
    }


def run_enhanced_backtest(sample_limit: int = 200, hold_days_list: list[int] = None):
    """主入口：运行增强回测，对比各策略组合。"""
    if hold_days_list is None:
        hold_days_list = [1, 3, 5, 10]

    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  试盘线 + 技术指标增强回测")
    logger.info("  持有天数: %s | 样本上限: %d", hold_days_list, sample_limit)
    logger.info("=" * 70)

    all_codes = sorted(get_all_stock_codes())
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("回测股票数: %d", len(all_codes))

    # 收集所有信号
    all_signals = []
    for i, code in enumerate(all_codes):
        if (i + 1) % 50 == 0:
            logger.info("  进度: %d/%d ...", i + 1, len(all_codes))
        sigs = backtest_stock_enhanced(code, hold_days_list)
        all_signals.extend(sigs)

    logger.info("  总信号数: %d", len(all_signals))

    if not all_signals:
        logger.info("  无信号，回测结束。")
        return

    # ─── 对比各策略 ─────────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("  各策略组合对比 (5日持有)")
    logger.info("=" * 70)
    logger.info("  %-24s %6s %8s %8s %8s", "策略", "信号数", "胜率", "均收益", "中位数")
    logger.info("  " + "-" * 60)

    strategy_results = {}
    for sname, filter_names in STRATEGIES.items():
        # 过滤信号
        filtered = all_signals
        for fn in filter_names:
            filtered = [s for s in filtered if s.get(f'filter_{fn}', False)]

        stats = calc_stats(filtered, 5)
        strategy_results[sname] = {'stats_5d': stats, 'signals': filtered}

        if stats['count'] > 0:
            logger.info("  %-24s %6d %7.1f%% %7.2f%% %7.2f%%",
                         sname, stats['count'], stats['win_rate'],
                         stats['avg_return'], stats['median_return'])
        else:
            logger.info("  %-24s %6d      -        -        -", sname, 0)

    # ─── 多持有期详细对比（只展示有提升的策略）──────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("  各持有期详细对比")
    logger.info("=" * 70)

    base_stats = {}
    for hd in hold_days_list:
        base_stats[hd] = calc_stats(all_signals, hd)

    for sname, filter_names in STRATEGIES.items():
        if not filter_names:
            continue  # 跳过原始

        filtered = strategy_results[sname]['signals']
        if len(filtered) < 5:
            continue

        logger.info("")
        logger.info("  【%s】(%d个信号):", sname, len(filtered))
        for hd in hold_days_list:
            stats = calc_stats(filtered, hd)
            base = base_stats[hd]
            if stats['count'] == 0:
                continue
            wr_diff = stats['win_rate'] - base['win_rate']
            avg_diff = stats['avg_return'] - base['avg_return']
            logger.info("    %2d天: 胜率%.1f%%(%+.1f) 均收益%.2f%%(%+.2f) 中位%.2f%% [%d样本]",
                         hd, stats['win_rate'], wr_diff,
                         stats['avg_return'], avg_diff,
                         stats['median_return'], stats['count'])

    # ─── 单指标过滤效果排名 ────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("  单指标过滤效果排名 (5日胜率提升)")
    logger.info("=" * 70)

    base_5d = base_stats[5]
    single_effects = []
    for fname in FILTERS:
        filtered = [s for s in all_signals if s.get(f'filter_{fname}', False)]
        stats = calc_stats(filtered, 5)
        if stats['count'] >= 5:
            single_effects.append({
                'name': fname,
                'count': stats['count'],
                'win_rate': stats['win_rate'],
                'wr_diff': stats['win_rate'] - base_5d['win_rate'],
                'avg_return': stats['avg_return'],
                'avg_diff': stats['avg_return'] - base_5d['avg_return'],
            })

    single_effects.sort(key=lambda x: x['wr_diff'], reverse=True)
    for e in single_effects:
        logger.info("  %-14s 信号%3d 胜率%5.1f%%(%+5.1f) 均收益%6.2f%%(%+6.2f)",
                     e['name'], e['count'], e['win_rate'], e['wr_diff'],
                     e['avg_return'], e['avg_diff'])

    elapsed = (datetime.now() - t_start).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 70)

    return strategy_results


if __name__ == '__main__':
    run_enhanced_backtest(sample_limit=200, hold_days_list=[1, 3, 5, 10])
