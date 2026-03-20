#!/usr/bin/env python3
"""
试盘线选股公式回测
================
基于通达信"试盘线"公式，在历史K线数据上回测验证。

信号条件（全部AND）：
  1. 上影线 > 2.5%
  2. 最高涨幅 > 7%
  3. 股价低位（CLOSE/LLV(LOW,50) < 1.4）
  4. 收盘涨幅 > 3%
  5. 收盘创5日新高
  6. 量倍增（VOL/昨日VOL > 2）
  7. 量创5日新高
  8. 低价低位（LOW/LLV(LOW,20) < 1.2）
  9. 低价高位（LOW/HHV(HIGH,20) > 0.9）

回测逻辑：
  信号日次日开盘买入，持有 N 天后卖出，统计胜率和收益。

用法：
    python -m day_week_predicted.backtest.shipanxian_backtest
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


def detect_shipanxian(klines: list[dict], idx: int) -> bool:
    """在 klines[idx] 处检测试盘线信号。需要至少50根前置K线。"""
    if idx < 50:
        return False

    cur = klines[idx]
    prev = klines[idx - 1]

    o, c, h, l, vol = (
        float(cur['open_price']),
        float(cur['close_price']),
        float(cur['high_price']),
        float(cur['low_price']),
        float(cur['trading_volume']),
    )
    prev_c = float(prev['close_price'])
    prev_vol = float(prev['trading_volume'])

    if c <= 0 or prev_c <= 0 or prev_vol <= 0:
        return False

    # 1. 上影线 := (HIGH - MAX(OPEN, CLOSE)) / CLOSE > 0.025
    upper_shadow = (h - max(o, c)) / c
    if upper_shadow <= 0.025:
        return False

    # 2. 最高涨幅 := (HIGH - REF(CLOSE,1)) / REF(CLOSE,1) > 0.07
    high_gain = (h - prev_c) / prev_c
    if high_gain <= 0.07:
        return False

    # 3. 收盘涨幅 := (CLOSE - REF(CLOSE,1)) / REF(CLOSE,1) > 0.03
    close_gain = (c - prev_c) / prev_c
    if close_gain <= 0.03:
        return False

    # 4. 股价低位 := CLOSE / LLV(LOW, 50) < 1.4
    lows_50 = [float(klines[j]['low_price']) for j in range(idx - 49, idx + 1)]
    llv_low_50 = min(lows_50)
    if llv_low_50 <= 0 or c / llv_low_50 >= 1.4:
        return False

    # 5. 收盘新高 := CLOSE >= HHV(CLOSE, 5)
    closes_5 = [float(klines[j]['close_price']) for j in range(idx - 4, idx + 1)]
    if c < max(closes_5):
        return False

    # 6. 量倍增 := VOL / REF(VOL, 1) > 2
    if vol / prev_vol <= 2:
        return False

    # 7. 量新高 := VOL >= HHV(VOL, 5)
    vols_5 = [float(klines[j]['trading_volume']) for j in range(idx - 4, idx + 1)]
    if vol < max(vols_5):
        return False

    # 8. 低价低位 := LOW / LLV(LOW, 20) < 1.2
    lows_20 = [float(klines[j]['low_price']) for j in range(idx - 19, idx + 1)]
    llv_low_20 = min(lows_20)
    if llv_low_20 <= 0 or l / llv_low_20 >= 1.2:
        return False

    # 9. 低价高位 := LOW / HHV(HIGH, 20) > 0.9
    highs_20 = [float(klines[j]['high_price']) for j in range(idx - 19, idx + 1)]
    hhv_high_20 = max(highs_20)
    if hhv_high_20 <= 0 or l / hhv_high_20 <= 0.9:
        return False

    return True


def backtest_stock(stock_code: str, hold_days_list: list[int] = None) -> dict:
    """对单只股票进行试盘线回测。返回信号列表及各持有期收益。"""
    if hold_days_list is None:
        hold_days_list = [1, 3, 5, 10]

    klines = get_kline_data(stock_code, limit=500)
    if len(klines) < 60:
        return {'code': stock_code, 'signals': []}

    signals = []
    for i in range(50, len(klines)):
        if not detect_shipanxian(klines, i):
            continue

        signal_date = str(klines[i]['date'])
        signal_close = float(klines[i]['close_price'])
        result = {
            'signal_date': signal_date,
            'signal_close': signal_close,
        }

        # 次日开盘买入
        if i + 1 >= len(klines):
            continue
        buy_price = float(klines[i + 1]['open_price'])
        if buy_price <= 0:
            continue
        result['buy_date'] = str(klines[i + 1]['date'])
        result['buy_price'] = buy_price

        for hd in hold_days_list:
            sell_idx = i + 1 + hd
            if sell_idx < len(klines):
                sell_price = float(klines[sell_idx]['close_price'])
                ret = (sell_price - buy_price) / buy_price * 100
                result[f'return_{hd}d'] = round(ret, 2)

        signals.append(result)

    return {'code': stock_code, 'signals': signals}


def run_backtest(sample_limit: int = 200, hold_days_list: list[int] = None):
    """主回测入口。"""
    if hold_days_list is None:
        hold_days_list = [1, 3, 5, 10]

    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  试盘线选股公式回测")
    logger.info("  持有天数: %s | 样本上限: %d", hold_days_list, sample_limit)
    logger.info("=" * 70)

    all_codes = sorted(get_all_stock_codes())
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("回测股票数: %d", len(all_codes))

    all_signals = []
    stock_with_signals = 0

    for i, code in enumerate(all_codes):
        if (i + 1) % 50 == 0:
            logger.info("  进度: %d/%d ...", i + 1, len(all_codes))
        result = backtest_stock(code, hold_days_list)
        if result['signals']:
            stock_with_signals += 1
            for s in result['signals']:
                s['stock_code'] = code
                all_signals.append(s)

    logger.info("")
    logger.info("=" * 70)
    logger.info("  回测结果汇总")
    logger.info("=" * 70)
    logger.info("  触发信号总数: %d (来自 %d 只股票)", len(all_signals), stock_with_signals)

    if not all_signals:
        logger.info("  无信号触发，回测结束。")
        return

    # 按持有天数统计
    for hd in hold_days_list:
        key = f'return_{hd}d'
        returns = [s[key] for s in all_signals if key in s]
        if not returns:
            continue

        wins = sum(1 for r in returns if r > 0)
        total = len(returns)
        avg_ret = sum(returns) / total
        max_ret = max(returns)
        min_ret = min(returns)
        median_ret = sorted(returns)[total // 2]

        logger.info("")
        logger.info("  持有 %d 天:", hd)
        logger.info("    样本数: %d", total)
        logger.info("    胜率: %.1f%% (%d/%d)", wins / total * 100, wins, total)
        logger.info("    平均收益: %.2f%%", avg_ret)
        logger.info("    中位数收益: %.2f%%", median_ret)
        logger.info("    最大收益: %.2f%% | 最大亏损: %.2f%%", max_ret, min_ret)

    # 按月份分布
    monthly = defaultdict(list)
    for s in all_signals:
        month = s['signal_date'][:7]
        if 'return_5d' in s:
            monthly[month].append(s['return_5d'])

    if monthly:
        logger.info("")
        logger.info("  信号月度分布 (5日收益):")
        for month in sorted(monthly.keys()):
            rets = monthly[month]
            avg = sum(rets) / len(rets)
            wr = sum(1 for r in rets if r > 0) / len(rets) * 100
            logger.info("    %s: %d个信号, 胜率%.1f%%, 均收益%.2f%%", month, len(rets), wr, avg)

    # 打印最近10个信号明细
    recent = sorted(all_signals, key=lambda x: x['signal_date'], reverse=True)[:10]
    logger.info("")
    logger.info("  最近10个信号:")
    for s in recent:
        ret_str = " | ".join(
            f"{hd}d: {s.get(f'return_{hd}d', 'N/A')}%"
            for hd in hold_days_list
        )
        logger.info("    %s %s 买入%.2f | %s",
                     s['stock_code'], s['signal_date'], s['buy_price'], ret_str)

    elapsed = (datetime.now() - t_start).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 70)

    return all_signals


if __name__ == '__main__':
    run_backtest(sample_limit=200, hold_days_list=[1, 3, 5, 10])
