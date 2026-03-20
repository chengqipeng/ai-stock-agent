#!/usr/bin/env python3
"""
试盘线 + 量价规则增强回测
========================
在试盘线信号基础上，叠加多种量价关系规则进行过滤验证。

量价规则：
  VP1. 量比适中（当日量/5日均量 在 2~5 之间，排除过度放量）
  VP2. 缩量回调后放量（前3日均量 < 10日均量，当日放量突破）
  VP3. 量价齐升（近3日收盘价逐日上升 + 近3日成交量逐日放大）
  VP4. 换手率适中（change_hand 在 3%~15%）
  VP5. 实体阳线占比高（实体/振幅 > 0.5，非十字星）
  VP6. 下影线短（下影线/振幅 < 0.3，非锤子线）
  VP7. 连续放量（近3日成交量均 > 10日均量）
  VP8. OBV趋势向上（5日OBV斜率为正）
  VP9. 量价背离过滤（排除价涨量缩的虚涨）
  VP10. 低位首次放量（前10日无量倍增信号）

用法：
    python -m day_week_predicted.backtest.shipanxian_volume_price_backtest
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
from day_week_predicted.backtest.shipanxian_enhanced_backtest import (
    calc_ma, calc_macd, calc_rsi, calc_kdj, calc_stats,
)


# ─── 量价指标预计算 ─────────────────────────────────────────────

def precompute_vp_indicators(klines: list[dict]) -> dict:
    """预计算量价相关指标。"""
    n = len(klines)
    closes = [float(k['close_price']) for k in klines]
    opens = [float(k['open_price']) for k in klines]
    highs = [float(k['high_price']) for k in klines]
    lows = [float(k['low_price']) for k in klines]
    vols = [float(k['trading_volume']) for k in klines]
    amounts = [float(k.get('trading_amount', 0)) for k in klines]
    change_hands = [float(k.get('change_hand', 0) or 0) for k in klines]
    amplitudes = [float(k.get('amplitude', 0) or 0) for k in klines]

    vol_ma3 = calc_ma(vols, 3)
    vol_ma5 = calc_ma(vols, 5)
    vol_ma10 = calc_ma(vols, 10)
    vol_ma20 = calc_ma(vols, 20)
    ma5 = calc_ma(closes, 5)
    ma10 = calc_ma(closes, 10)
    ma20 = calc_ma(closes, 20)

    # OBV
    obv = [0.0] * n
    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            obv[i] = obv[i - 1] + vols[i]
        elif closes[i] < closes[i - 1]:
            obv[i] = obv[i - 1] - vols[i]
        else:
            obv[i] = obv[i - 1]

    # MACD / KDJ / RSI for combo strategies
    dif, dea, macd_bar = calc_macd(closes)
    k_vals, d_vals, j_vals = calc_kdj(highs, lows, closes)

    return {
        'closes': closes, 'opens': opens, 'highs': highs, 'lows': lows,
        'vols': vols, 'amounts': amounts, 'change_hands': change_hands,
        'amplitudes': amplitudes,
        'vol_ma3': vol_ma3, 'vol_ma5': vol_ma5, 'vol_ma10': vol_ma10,
        'vol_ma20': vol_ma20,
        'ma5': ma5, 'ma10': ma10, 'ma20': ma20,
        'obv': obv,
        'dif': dif, 'dea': dea, 'macd_bar': macd_bar,
        'k': k_vals, 'd': d_vals,
    }


# ─── 量价过滤规则 ──────────────────────────────────────────────

def _safe_div(a, b):
    return a / b if b != 0 else 0


VP_FILTERS = {}


def vp_filter(name):
    """装饰器：注册量价过滤规则。"""
    def decorator(func):
        VP_FILTERS[name] = func
        return func
    return decorator


@vp_filter('VP1_量比适中')
def _(ind, i):
    """当日量/5日均量 在 2~5 之间，排除过度放量的一字板或异常交易"""
    if ind['vol_ma5'][i] <= 0:
        return False
    vol_ratio = ind['vols'][i] / ind['vol_ma5'][i]
    return 2.0 <= vol_ratio <= 5.0


@vp_filter('VP2_缩量后放量')
def _(ind, i):
    """前3日均量 < 10日均量（缩量整理），当日放量突破"""
    if i < 3 or ind['vol_ma10'][i] <= 0:
        return False
    prev3_avg = sum(ind['vols'][i - 3:i]) / 3
    return prev3_avg < ind['vol_ma10'][i] and ind['vols'][i] > ind['vol_ma10'][i] * 1.5


@vp_filter('VP3_量价齐升')
def _(ind, i):
    """近3日收盘价逐日上升 + 近3日成交量逐日放大"""
    if i < 2:
        return False
    c = ind['closes']
    v = ind['vols']
    return (c[i] > c[i - 1] > c[i - 2] and v[i] > v[i - 1] > v[i - 2])


@vp_filter('VP4_换手率适中')
def _(ind, i):
    """换手率在 3%~15%，排除过低（无人气）和过高（过度投机）"""
    ch = ind['change_hands'][i]
    return 3.0 <= ch <= 15.0


@vp_filter('VP5_实体阳线')
def _(ind, i):
    """实体占振幅比 > 0.5，确认是实体阳线而非十字星"""
    o, c, h, l = ind['opens'][i], ind['closes'][i], ind['highs'][i], ind['lows'][i]
    amplitude = h - l
    if amplitude <= 0:
        return False
    body = abs(c - o)
    return body / amplitude > 0.5 and c > o


@vp_filter('VP6_下影线短')
def _(ind, i):
    """下影线/振幅 < 0.3，排除锤子线（下方有较强抛压）"""
    o, c, h, l = ind['opens'][i], ind['closes'][i], ind['highs'][i], ind['lows'][i]
    amplitude = h - l
    if amplitude <= 0:
        return False
    lower_shadow = min(o, c) - l
    return lower_shadow / amplitude < 0.3


@vp_filter('VP7_连续放量')
def _(ind, i):
    """近3日成交量均 > 10日均量，持续有资金介入"""
    if i < 2 or ind['vol_ma10'][i] <= 0:
        return False
    v = ind['vols']
    ma10 = ind['vol_ma10'][i]
    return v[i] > ma10 and v[i - 1] > ma10 and v[i - 2] > ma10


@vp_filter('VP8_OBV上升')
def _(ind, i):
    """5日OBV斜率为正（资金持续流入）"""
    if i < 5:
        return False
    return ind['obv'][i] > ind['obv'][i - 5]


@vp_filter('VP9_非量价背离')
def _(ind, i):
    """排除价涨量缩：如果收盘涨幅>3%但量比<1.2则为背离"""
    if i < 1 or ind['closes'][i - 1] <= 0 or ind['vol_ma5'][i] <= 0:
        return False
    price_change = (ind['closes'][i] - ind['closes'][i - 1]) / ind['closes'][i - 1]
    vol_ratio = ind['vols'][i] / ind['vol_ma5'][i]
    # 涨幅大时量必须跟上
    if price_change > 0.03:
        return vol_ratio >= 1.2
    return True


@vp_filter('VP10_低位首次放量')
def _(ind, i):
    """前10日无量倍增（VOL/前日VOL>2）信号，确认是首次放量启动"""
    if i < 11:
        return False
    v = ind['vols']
    for j in range(i - 10, i):
        if v[j - 1] > 0 and v[j] / v[j - 1] > 2:
            return False
    return True


@vp_filter('VP11_量能温和放大')
def _(ind, i):
    """5日均量/20日均量 在 1.2~3.0 之间，温和放量而非暴量"""
    if ind['vol_ma20'][i] <= 0:
        return False
    ratio = ind['vol_ma5'][i] / ind['vol_ma20'][i]
    return 1.2 <= ratio <= 3.0


@vp_filter('VP12_收盘价上半区')
def _(ind, i):
    """收盘价在当日振幅上半区：(C-L)/(H-L) > 0.5"""
    h, l, c = ind['highs'][i], ind['lows'][i], ind['closes'][i]
    if h == l:
        return False
    return (c - l) / (h - l) > 0.5


# ─── 技术指标过滤（从上一版验证有效的）───────────────────────

TECH_FILTERS = {
    '均线多头': lambda ind, i: ind['ma5'][i] > ind['ma10'][i] > ind['ma20'][i] > 0,
    'KDJ金叉区': lambda ind, i: ind['k'][i] > ind['d'][i] and ind['k'][i] < 80,
    'MACD多头': lambda ind, i: ind['dif'][i] > ind['dea'][i],
}


# ─── 策略组合定义 ──────────────────────────────────────────────

ALL_FILTERS = {}
ALL_FILTERS.update(VP_FILTERS)
ALL_FILTERS.update(TECH_FILTERS)

STRATEGIES = {
    '原始试盘线': [],
    # 单量价规则
    '+VP1_量比适中': ['VP1_量比适中'],
    '+VP2_缩量后放量': ['VP2_缩量后放量'],
    '+VP3_量价齐升': ['VP3_量价齐升'],
    '+VP4_换手率适中': ['VP4_换手率适中'],
    '+VP5_实体阳线': ['VP5_实体阳线'],
    '+VP6_下影线短': ['VP6_下影线短'],
    '+VP7_连续放量': ['VP7_连续放量'],
    '+VP8_OBV上升': ['VP8_OBV上升'],
    '+VP9_非量价背离': ['VP9_非量价背离'],
    '+VP10_低位首次放量': ['VP10_低位首次放量'],
    '+VP11_量能温和放大': ['VP11_量能温和放大'],
    '+VP12_收盘价上半区': ['VP12_收盘价上半区'],
    # 量价组合
    '+VP1+VP4(量比+换手)': ['VP1_量比适中', 'VP4_换手率适中'],
    '+VP2+VP8(缩放+OBV)': ['VP2_缩量后放量', 'VP8_OBV上升'],
    '+VP5+VP6(阳线形态)': ['VP5_实体阳线', 'VP6_下影线短'],
    '+VP1+VP5+VP12': ['VP1_量比适中', 'VP5_实体阳线', 'VP12_收盘价上半区'],
    '+VP4+VP8+VP11': ['VP4_换手率适中', 'VP8_OBV上升', 'VP11_量能温和放大'],
    # 量价 + 技术指标组合
    '+VP1+均线多头': ['VP1_量比适中', '均线多头'],
    '+VP4+均线多头': ['VP4_换手率适中', '均线多头'],
    '+VP5+VP6+均线多头': ['VP5_实体阳线', 'VP6_下影线短', '均线多头'],
    '+VP1+VP4+KDJ': ['VP1_量比适中', 'VP4_换手率适中', 'KDJ金叉区'],
    '+VP10+均线多头': ['VP10_低位首次放量', '均线多头'],
    '+最佳量价组合': ['VP1_量比适中', 'VP4_换手率适中', 'VP5_实体阳线', 'VP12_收盘价上半区'],
}


# ─── 回测核心 ──────────────────────────────────────────────────

def backtest_stock_vp(stock_code: str, hold_days_list: list[int]) -> list[dict]:
    """对单只股票检测试盘线信号并记录量价指标状态。"""
    klines = get_kline_data(stock_code, limit=500)
    if len(klines) < 60:
        return []

    ind = precompute_vp_indicators(klines)
    signals = []

    for i in range(50, len(klines)):
        if not detect_shipanxian(klines, i):
            continue
        if i + 1 >= len(klines):
            continue
        buy_price = float(klines[i + 1]['open_price'])
        if buy_price <= 0:
            continue

        sig = {
            'stock_code': stock_code,
            'signal_date': str(klines[i]['date']),
            'buy_price': buy_price,
        }

        for hd in hold_days_list:
            sell_idx = i + 1 + hd
            if sell_idx < len(klines):
                sell_price = float(klines[sell_idx]['close_price'])
                sig[f'return_{hd}d'] = round((sell_price - buy_price) / buy_price * 100, 2)

        # 记录所有过滤条件
        for fname, ffunc in ALL_FILTERS.items():
            try:
                sig[f'f_{fname}'] = ffunc(ind, i)
            except Exception:
                sig[f'f_{fname}'] = False

        signals.append(sig)

    return signals


def run_vp_backtest(sample_limit: int = 200, hold_days_list: list[int] = None):
    """主入口。"""
    if hold_days_list is None:
        hold_days_list = [1, 3, 5, 10]

    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  试盘线 + 量价规则增强回测")
    logger.info("  持有天数: %s | 样本上限: %d", hold_days_list, sample_limit)
    logger.info("=" * 70)

    all_codes = sorted(get_all_stock_codes())
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("回测股票数: %d", len(all_codes))

    all_signals = []
    for i, code in enumerate(all_codes):
        if (i + 1) % 50 == 0:
            logger.info("  进度: %d/%d ...", i + 1, len(all_codes))
        sigs = backtest_stock_vp(code, hold_days_list)
        all_signals.extend(sigs)

    logger.info("  总信号数: %d", len(all_signals))
    if not all_signals:
        logger.info("  无信号，结束。")
        return

    # ─── 单规则效果排名 ────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("  单规则过滤效果排名")
    logger.info("=" * 70)

    base_stats = {}
    for hd in hold_days_list:
        base_stats[hd] = calc_stats(all_signals, hd)

    base_5d = base_stats[5]
    single_effects = []
    for fname in ALL_FILTERS:
        filtered = [s for s in all_signals if s.get(f'f_{fname}', False)]
        s3 = calc_stats(filtered, 3)
        s5 = calc_stats(filtered, 5)
        if s5['count'] >= 5:
            single_effects.append({
                'name': fname,
                'count_5d': s5['count'],
                'wr_5d': s5['win_rate'],
                'wr_diff_5d': s5['win_rate'] - base_5d['win_rate'],
                'avg_5d': s5['avg_return'],
                'avg_diff_5d': s5['avg_return'] - base_5d['avg_return'],
                'wr_3d': s3['win_rate'] if s3['count'] > 0 else 0,
                'avg_3d': s3['avg_return'] if s3['count'] > 0 else 0,
            })

    single_effects.sort(key=lambda x: x['wr_diff_5d'], reverse=True)

    logger.info("  %-20s %5s  %8s(%6s)  %8s(%7s)", "规则", "信号", "5d胜率", "提升", "5d均收", "提升")
    logger.info("  " + "-" * 70)
    for e in single_effects:
        logger.info("  %-20s %5d  %7.1f%%(%+5.1f)  %7.2f%%(%+6.2f)",
                     e['name'], e['count_5d'], e['wr_5d'], e['wr_diff_5d'],
                     e['avg_5d'], e['avg_diff_5d'])

    # ─── 策略组合对比 ──────────────────────────────────────
    logger.info("")
    logger.info("=" * 70)
    logger.info("  策略组合对比")
    logger.info("=" * 70)
    logger.info("  %-26s %5s  %7s  %7s  %7s  %7s",
                "策略", "信号", "3d胜率", "3d均收", "5d胜率", "5d均收")
    logger.info("  " + "-" * 70)

    best_strategy = None
    best_wr = 0

    for sname, filter_names in STRATEGIES.items():
        filtered = all_signals
        for fn in filter_names:
            filtered = [s for s in filtered if s.get(f'f_{fn}', False)]

        s3 = calc_stats(filtered, 3)
        s5 = calc_stats(filtered, 5)

        if s5['count'] >= 5:
            logger.info("  %-26s %5d  %6.1f%%  %6.2f%%  %6.1f%%  %6.2f%%",
                         sname, s5['count'],
                         s3['win_rate'] if s3['count'] > 0 else 0,
                         s3['avg_return'] if s3['count'] > 0 else 0,
                         s5['win_rate'], s5['avg_return'])
            if s5['win_rate'] > best_wr and s5['count'] >= 10:
                best_wr = s5['win_rate']
                best_strategy = sname
        else:
            logger.info("  %-26s %5d       -        -        -        -",
                         sname, s5.get('count', 0))

    # ─── 最佳策略多持有期详情 ──────────────────────────────
    if best_strategy and best_strategy != '原始试盘线':
        filter_names = STRATEGIES[best_strategy]
        filtered = all_signals
        for fn in filter_names:
            filtered = [s for s in filtered if s.get(f'f_{fn}', False)]

        logger.info("")
        logger.info("=" * 70)
        logger.info("  最佳策略详情: %s", best_strategy)
        logger.info("=" * 70)
        for hd in hold_days_list:
            stats = calc_stats(filtered, hd)
            base = base_stats[hd]
            if stats['count'] == 0:
                continue
            logger.info("  %2d天: 胜率%.1f%%(%+.1f) 均收益%.2f%%(%+.2f) "
                         "中位%.2f%% 最大%.2f%% 最小%.2f%% [%d样本]",
                         hd, stats['win_rate'], stats['win_rate'] - base['win_rate'],
                         stats['avg_return'], stats['avg_return'] - base['avg_return'],
                         stats['median_return'], stats['max_return'],
                         stats['min_return'], stats['count'])

    elapsed = (datetime.now() - t_start).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 70)


if __name__ == '__main__':
    run_vp_backtest(sample_limit=200, hold_days_list=[1, 3, 5, 10])
