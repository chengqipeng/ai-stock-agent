#!/usr/bin/env python3
"""
成交量信号回测验证 — 验证成交量形态对下周预测的增强效果
======================================================
对比维度：
1. 基线(Baseline): 现有 _NW_RULES 规则引擎（无成交量增强）
2. 增强(Enhanced): 现有规则 + 成交量形态信号
3. 独立(Standalone): 纯成交量信号的独立预测能力
4. 置信度修正: 成交量信号对现有预测置信度的修正效果

用法：
    python -m day_week_predicted.backtest.volume_signal_backtest
"""
import sys
import logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes,
    _get_latest_trade_date,
    _to_float,
    _compound_return,
    _mean,
    _std,
    _get_stock_index,
    _nw_extract_features,
    _nw_match_rule,
)


# ═══════════════════════════════════════════════════════════
# 成交量形态检测函数
# ═══════════════════════════════════════════════════════════

def _detect_volume_signals(week_klines: list[dict], hist_klines: list[dict]) -> dict:
    """检测本周的成交量形态信号。

    Args:
        week_klines: 本周K线（含 date, open, close, high, low, volume, change_percent）
        hist_klines: 历史K线（本周之前，按日期升序，至少60日）

    Returns:
        dict of detected signals with boolean flags and numeric values
    """
    signals = {
        # P0 信号
        'sky_volume': False,           # 天量出现
        'sky_volume_bullish': None,    # 天量阳线(True)/阴线(False)
        'sky_volume_next_shrink': False,  # 天量后次日缩量
        'price_up_vol_down': False,    # 价升量缩
        'price_down_vol_up': False,    # 价跌量增
        'price_down_vol_up_at_low': False,  # 低位价跌量增(恐慌底)
        'new_high_vol_shrink': False,  # 高点缩量
        'vol_peak_in_week': False,     # 周内量峰
        # P1 信号
        'double_vol_yang_yin': False,  # 阳+阴双量柱
        'vol_trend_up': False,         # 周内成交量递增
        'vol_trend_down': False,       # 周内成交量递减
        'breakout_volume': False,      # 突破量(上穿MA120+放量)
        'pullback_shrink': False,      # 回撤缩量(MA120上方缩量回撤)
        'rebound_vol_below_ma120': False,  # MA120下方反弹放量
        'rush_up_then_shrink': False,  # 急涨增量后缩量
        # 数值
        'vol_ratio_20': None,          # 本周均量/20日均量
        'vol_ratio_60': None,          # 本周均量/60日均量
        'max_day_vol_ratio': None,     # 本周最大单日量/20日均量
        'week_chg': None,              # 本周涨跌幅
        'price_position': None,        # 价格位置(0=最低,1=最高,相对60日)
        'above_ma120': None,           # 是否在MA120上方
    }

    if not week_klines or len(hist_klines) < 20:
        return signals

    # ── 基础计算 ──
    week_vols = [k['volume'] for k in week_klines if k.get('volume', 0) > 0]
    if not week_vols:
        return signals

    week_avg_vol = _mean(week_vols)
    week_chg = _compound_return([k['change_percent'] for k in week_klines])
    signals['week_chg'] = round(week_chg, 4)

    # 历史均量
    hist_vols_20 = [k['volume'] for k in hist_klines[-20:] if k.get('volume', 0) > 0]
    hist_vols_60 = [k['volume'] for k in hist_klines[-60:] if k.get('volume', 0) > 0]
    avg_vol_20 = _mean(hist_vols_20) if hist_vols_20 else 0
    avg_vol_60 = _mean(hist_vols_60) if hist_vols_60 else 0

    if avg_vol_20 > 0:
        signals['vol_ratio_20'] = round(week_avg_vol / avg_vol_20, 4)
    if avg_vol_60 > 0:
        signals['vol_ratio_60'] = round(week_avg_vol / avg_vol_60, 4)

    # 本周最大单日量比
    max_day_vol = max(week_vols)
    if avg_vol_20 > 0:
        signals['max_day_vol_ratio'] = round(max_day_vol / avg_vol_20, 4)

    # 价格位置（相对60日高低点）
    hist_closes = [k['close'] for k in hist_klines[-60:] if k.get('close', 0) > 0]
    if hist_closes and week_klines:
        all_closes = hist_closes + [k['close'] for k in week_klines if k.get('close', 0) > 0]
        if all_closes:
            min_c = min(all_closes)
            max_c = max(all_closes)
            latest_close = week_klines[-1].get('close', 0)
            if max_c > min_c and latest_close > 0:
                signals['price_position'] = round((latest_close - min_c) / (max_c - min_c), 4)

    # MA120 位置
    all_hist_closes = [k['close'] for k in hist_klines if k.get('close', 0) > 0]
    if len(all_hist_closes) >= 120:
        ma120 = _mean(all_hist_closes[-120:])
        latest_close = week_klines[-1].get('close', 0)
        if ma120 > 0 and latest_close > 0:
            signals['above_ma120'] = latest_close > ma120
    elif len(all_hist_closes) >= 60:
        # 不足120日用60日均线近似
        ma60 = _mean(all_hist_closes[-60:])
        latest_close = week_klines[-1].get('close', 0)
        if ma60 > 0 and latest_close > 0:
            signals['above_ma120'] = latest_close > ma60

    # ── P0: 天量检测 ──
    if avg_vol_60 > 0:
        for i, k in enumerate(week_klines):
            vol = k.get('volume', 0)
            if vol > avg_vol_60 * 3.0:
                signals['sky_volume'] = True
                signals['sky_volume_bullish'] = k.get('close', 0) > k.get('open', 0)
                # 次日是否缩量
                if i + 1 < len(week_klines):
                    next_vol = week_klines[i + 1].get('volume', 0)
                    if next_vol < vol * 0.5:
                        signals['sky_volume_next_shrink'] = True
                break  # 只取第一个天量

    # ── P0: 价升量缩 ──
    if week_chg > 0.5 and signals['vol_ratio_20'] is not None and signals['vol_ratio_20'] < 0.8:
        signals['price_up_vol_down'] = True

    # ── P0: 价跌量增 ──
    if week_chg < -1.0 and signals['vol_ratio_20'] is not None and signals['vol_ratio_20'] > 1.3:
        signals['price_down_vol_up'] = True
        # 低位判断
        if signals['price_position'] is not None and signals['price_position'] < 0.25:
            signals['price_down_vol_up_at_low'] = True

    # ── P0: 高点缩量 ──
    if hist_closes:
        recent_high = max(hist_closes[-20:]) if len(hist_closes) >= 20 else max(hist_closes)
        week_high = max(k.get('high', 0) for k in week_klines)
        if week_high >= recent_high * 0.99 and signals['vol_ratio_20'] is not None:
            if signals['vol_ratio_20'] < 0.7:
                signals['new_high_vol_shrink'] = True

    # ── P0: 周内量峰 ──
    if len(week_vols) >= 3:
        peak_idx = week_vols.index(max(week_vols))
        if 0 < peak_idx < len(week_vols) - 1:
            # 峰值前后都有更小的量
            if week_vols[peak_idx] > week_vols[0] * 1.3 and week_vols[peak_idx] > week_vols[-1] * 1.3:
                signals['vol_peak_in_week'] = True

    # ── P1: 阳+阴双量柱 ──
    if avg_vol_20 > 0 and len(week_klines) >= 2:
        for i in range(len(week_klines) - 1):
            k1 = week_klines[i]
            k2 = week_klines[i + 1]
            v1 = k1.get('volume', 0)
            v2 = k2.get('volume', 0)
            # 连续两日放量
            if v1 > avg_vol_20 * 1.8 and v2 > avg_vol_20 * 1.8:
                # 第一日阳线，第二日阴线
                if k1.get('close', 0) > k1.get('open', 0) and k2.get('close', 0) < k2.get('open', 0):
                    signals['double_vol_yang_yin'] = True
                    break

    # ── P1: 周内成交量趋势 ──
    if len(week_vols) >= 3:
        # 简单线性趋势：后半段均量 vs 前半段均量
        mid = len(week_vols) // 2
        first_half = _mean(week_vols[:mid]) if mid > 0 else 0
        second_half = _mean(week_vols[mid:]) if mid < len(week_vols) else 0
        if first_half > 0:
            ratio = second_half / first_half
            if ratio > 1.3:
                signals['vol_trend_up'] = True
            elif ratio < 0.7:
                signals['vol_trend_down'] = True

    # ── P1: 突破量 ──
    if signals['above_ma120'] is True and len(all_hist_closes) >= 60:
        # 本周在MA120上方，检查上周是否在下方
        prev_week_closes = [k['close'] for k in hist_klines[-5:] if k.get('close', 0) > 0]
        if prev_week_closes:
            ma_val = _mean(all_hist_closes[-120:]) if len(all_hist_closes) >= 120 else _mean(all_hist_closes[-60:])
            prev_close = prev_week_closes[-1]
            if prev_close < ma_val and signals['vol_ratio_20'] is not None and signals['vol_ratio_20'] > 1.5:
                signals['breakout_volume'] = True

    # ── P1: 回撤缩量 ──
    if signals['above_ma120'] is True and week_chg < -0.5:
        if signals['vol_ratio_20'] is not None and signals['vol_ratio_20'] < 0.7:
            signals['pullback_shrink'] = True

    # ── P1: MA120下方反弹放量 ──
    if signals['above_ma120'] is False and week_chg > 1.0:
        if signals['vol_ratio_20'] is not None and signals['vol_ratio_20'] > 1.5:
            signals['rebound_vol_below_ma120'] = True

    # ── P1: 急涨增量后缩量 ──
    if len(week_klines) >= 4:
        # 前半周放量涨，后半周缩量
        mid = len(week_klines) // 2
        first_chg = _compound_return([k['change_percent'] for k in week_klines[:mid]])
        first_vol = _mean([k.get('volume', 0) for k in week_klines[:mid]])
        second_vol = _mean([k.get('volume', 0) for k in week_klines[mid:]])
        if first_chg > 2.0 and first_vol > 0 and second_vol < first_vol * 0.6:
            signals['rush_up_then_shrink'] = True

    return signals


# ═══════════════════════════════════════════════════════════
# 成交量增强规则
# ═══════════════════════════════════════════════════════════

def _vol_enhanced_predict(baseline_rule, vol_signals: dict) -> dict | None:
    """基于成交量信号增强/新增预测规则。

    Returns:
        dict with 'pred_up', 'name', 'source' ('enhanced'/'new'/'override')
        or None if no volume signal triggered
    """
    vs = vol_signals

    # ── 独立成交量规则（当基线无命中时） ──

    # V1: 天量阳线 → 涨（除非次日缩量）
    if vs['sky_volume'] and vs['sky_volume_bullish'] is True and not vs['sky_volume_next_shrink']:
        return {'pred_up': True, 'name': 'V1:天量阳线→涨', 'source': 'new', 'tier': 1}

    # V2: 天量阴线 → 跌
    if vs['sky_volume'] and vs['sky_volume_bullish'] is False:
        return {'pred_up': False, 'name': 'V2:天量阴线→跌', 'source': 'new', 'tier': 1}

    # V3: 天量后次日大幅缩量 → 跌（无论阴阳）
    if vs['sky_volume'] and vs['sky_volume_next_shrink']:
        return {'pred_up': False, 'name': 'V3:天量后缩量→跌', 'source': 'new', 'tier': 1}

    # V4: 高点缩量 → 跌
    if vs['new_high_vol_shrink']:
        return {'pred_up': False, 'name': 'V4:高点缩量→跌', 'source': 'new', 'tier': 1}

    # V5: 低位价跌量增(恐慌底) → 涨
    if vs['price_down_vol_up_at_low']:
        return {'pred_up': True, 'name': 'V5:恐慌底→涨', 'source': 'new', 'tier': 1}

    # V6: 价升量缩 → 跌
    if vs['price_up_vol_down']:
        return {'pred_up': False, 'name': 'V6:价升量缩→跌', 'source': 'new', 'tier': 2}

    # V7: 阳+阴双量柱 → 跌
    if vs['double_vol_yang_yin']:
        return {'pred_up': False, 'name': 'V7:阳阴双量柱→跌', 'source': 'new', 'tier': 2}

    # V8: 突破量 → 涨
    if vs['breakout_volume']:
        return {'pred_up': True, 'name': 'V8:突破量→涨', 'source': 'new', 'tier': 1}

    # V9: 回撤缩量(MA120上方) → 涨
    if vs['pullback_shrink']:
        return {'pred_up': True, 'name': 'V9:回撤缩量→涨', 'source': 'new', 'tier': 2}

    # V10: MA120下方反弹放量 → 跌（诱多）
    if vs['rebound_vol_below_ma120']:
        return {'pred_up': False, 'name': 'V10:MA120下反弹放量→跌', 'source': 'new', 'tier': 2}

    # V11: 急涨增量后缩量 → 跌
    if vs['rush_up_then_shrink']:
        return {'pred_up': False, 'name': 'V11:急涨后缩量→跌', 'source': 'new', 'tier': 2}

    # V12: 周内量峰 + 价格上涨 → 跌（量峰见顶）
    if vs['vol_peak_in_week'] and vs['week_chg'] is not None and vs['week_chg'] > 1.0:
        return {'pred_up': False, 'name': 'V12:量峰见顶→跌', 'source': 'new', 'tier': 2}

    # V13: 价跌量增(非低位) → 跌（主力出货）
    if vs['price_down_vol_up'] and not vs['price_down_vol_up_at_low']:
        return {'pred_up': False, 'name': 'V13:高位价跌量增→跌', 'source': 'new', 'tier': 2}

    return None


def _vol_override_baseline(baseline_rule, vol_signals: dict) -> dict | None:
    """成交量信号覆盖基线预测（仅在强矛盾时）。

    Returns:
        dict with override info, or None if no override
    """
    if baseline_rule is None:
        return None

    vs = vol_signals
    base_up = baseline_rule['pred_up']

    # 基线预测涨，但成交量信号强烈看跌
    if base_up:
        # 高点缩量 → 覆盖为跌
        if vs['new_high_vol_shrink']:
            return {'pred_up': False, 'name': f"覆盖:{baseline_rule['name']}→高点缩量跌",
                    'source': 'override'}
        # 天量阴线 → 覆盖为跌
        if vs['sky_volume'] and vs['sky_volume_bullish'] is False:
            return {'pred_up': False, 'name': f"覆盖:{baseline_rule['name']}→天量阴线跌",
                    'source': 'override'}
        # 天量后缩量 → 覆盖为跌
        if vs['sky_volume'] and vs['sky_volume_next_shrink']:
            return {'pred_up': False, 'name': f"覆盖:{baseline_rule['name']}→天量缩量跌",
                    'source': 'override'}
        # 急涨后缩量 → 覆盖为跌
        if vs['rush_up_then_shrink']:
            return {'pred_up': False, 'name': f"覆盖:{baseline_rule['name']}→急涨缩量跌",
                    'source': 'override'}

    # 基线预测跌，但成交量信号强烈看涨
    if not base_up:
        # 恐慌底 → 覆盖为涨
        if vs['price_down_vol_up_at_low']:
            return {'pred_up': True, 'name': f"覆盖:{baseline_rule['name']}→恐慌底涨",
                    'source': 'override'}
        # 突破量 → 覆盖为涨
        if vs['breakout_volume']:
            return {'pred_up': True, 'name': f"覆盖:{baseline_rule['name']}→突破量涨",
                    'source': 'override'}

    return None


# ═══════════════════════════════════════════════════════════
# 回测主函数
# ═══════════════════════════════════════════════════════════

def run_backtest(n_weeks=29, sample_limit=0):
    """运行成交量信号回测验证。"""
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  成交量信号回测验证 (n_weeks=%d)", n_weeks)
    logger.info("=" * 70)

    latest_date = _get_latest_trade_date()
    if not latest_date:
        logger.error("无法获取最新交易日")
        return
    logger.info("最新交易日: %s", latest_date)

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("回测股票数: %d", len(all_codes))

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    # 需要更多历史数据用于MA120和60日均量
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    # ── 加载数据（含成交量和价格） ──
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    logger.info("加载个股K线(含量价数据)...")
    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(all_codes), batch_size):
        batch = all_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, open_price, close_price, high_price, "
            f"low_price, change_percent, trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, latest_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'open': _to_float(row['open_price']),
                'close': _to_float(row['close_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'change_percent': _to_float(row['change_percent']),
                'volume': _to_float(row['trading_volume']),
            })

    # 指数K线
    logger.info("加载指数K线...")
    all_index_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in all_index_codes:
            all_index_codes.append(idx)
    ph_idx = ','.join(['%s'] * len(all_index_codes))
    cur.execute(
        f"SELECT stock_code, `date`, change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph_idx}) AND `date` >= %s AND `date` <= %s "
        f"ORDER BY `date`", all_index_codes + [start_date, latest_date])
    market_klines_by_index = defaultdict(list)
    for r in cur.fetchall():
        market_klines_by_index[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })
    conn.close()

    # 按ISO周分组指数
    market_by_week_by_index = {}
    for idx_code, klines_list in market_klines_by_index.items():
        by_week = defaultdict(list)
        for k in klines_list:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            by_week[iw].append(k)
        market_by_week_by_index[idx_code] = by_week

    logger.info("数据加载完成: %d只股票, %d个指数", len(stock_klines), len(market_klines_by_index))

    # ── 统计容器 ──
    # 1. 基线统计
    baseline = {'correct': 0, 'total': 0, 'all_weeks': 0}
    # 2. 增强统计（基线+成交量覆盖）
    enhanced = {'correct': 0, 'total': 0}
    # 3. 独立成交量信号统计
    vol_standalone = defaultdict(lambda: {'correct': 0, 'total': 0})
    # 4. 覆盖统计
    override_stats = {'improved': 0, 'worsened': 0, 'total': 0}
    # 5. 成交量信号触发频率
    signal_freq = defaultdict(int)
    # 6. 组合策略统计（基线命中时+成交量确认/矛盾）
    combo_stats = {
        'baseline_hit_vol_confirm': {'correct': 0, 'total': 0},
        'baseline_hit_vol_conflict': {'correct': 0, 'total': 0},
        'baseline_hit_vol_neutral': {'correct': 0, 'total': 0},
        'baseline_miss_vol_hit': {'correct': 0, 'total': 0},
        'both_miss': 0,
    }
    # 7. 按信号分层统计独立准确率
    vol_signal_accuracy = defaultdict(lambda: {'correct': 0, 'total': 0})
    # 8. 量价关系分类统计
    vol_price_pattern = defaultdict(lambda: {'next_up': 0, 'next_down': 0, 'total': 0})

    processed = 0
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        idx_by_week = market_by_week_by_index.get(stock_idx, {})

        # 按ISO周分组
        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())

        for i in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[i]
            iw_next = sorted_weeks[i + 1]

            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])

            if len(this_days) < 3 or len(next_days) < 3:
                continue

            # 只回测最近n_weeks周
            dt_this = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_this < dt_end - timedelta(days=n_weeks * 7 + 14):
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_week_chg = _compound_return(next_pcts)
            actual_next_up = next_week_chg >= 0

            # 大盘本周涨跌幅
            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            baseline['all_weeks'] += 1

            # ── 基线预测 ──
            feat = _nw_extract_features(this_pcts, market_chg, market_index=stock_idx)
            base_rule = _nw_match_rule(feat)

            # ── 成交量信号检测 ──
            # 获取本周之前的历史K线
            first_week_date = this_days[0]['date']
            hist = [k for k in klines if k['date'] < first_week_date]
            hist.sort(key=lambda x: x['date'])

            vol_signals = _detect_volume_signals(this_days, hist)

            # 统计信号触发频率
            for sig_name in ['sky_volume', 'price_up_vol_down', 'price_down_vol_up',
                             'price_down_vol_up_at_low', 'new_high_vol_shrink',
                             'vol_peak_in_week', 'double_vol_yang_yin',
                             'breakout_volume', 'pullback_shrink',
                             'rebound_vol_below_ma120', 'rush_up_then_shrink']:
                if vol_signals.get(sig_name):
                    signal_freq[sig_name] += 1

            # ── 独立成交量预测 ──
            vol_pred = _vol_enhanced_predict(base_rule, vol_signals)

            # ── 量价关系分类 ──
            vr = vol_signals.get('vol_ratio_20')
            wc = vol_signals.get('week_chg')
            if vr is not None and wc is not None:
                if wc > 0.5 and vr > 1.2:
                    pattern = '价升量增'
                elif wc > 0.5 and vr < 0.8:
                    pattern = '价升量缩'
                elif wc < -0.5 and vr > 1.2:
                    pattern = '价跌量增'
                elif wc < -0.5 and vr < 0.8:
                    pattern = '价跌量缩'
                elif abs(wc) <= 0.5 and vr > 1.2:
                    pattern = '价平量增'
                elif abs(wc) <= 0.5 and vr < 0.8:
                    pattern = '价平量缩'
                else:
                    pattern = '价平量平'

                vol_price_pattern[pattern]['total'] += 1
                if actual_next_up:
                    vol_price_pattern[pattern]['next_up'] += 1
                else:
                    vol_price_pattern[pattern]['next_down'] += 1

            # ── 统计逻辑 ──

            # 1. 基线统计
            if base_rule is not None:
                base_correct = base_rule['pred_up'] == actual_next_up
                baseline['total'] += 1
                if base_correct:
                    baseline['correct'] += 1

            # 2. 独立成交量信号准确率
            if vol_pred is not None:
                vol_correct = vol_pred['pred_up'] == actual_next_up
                vol_standalone[vol_pred['name']]['total'] += 1
                if vol_correct:
                    vol_standalone[vol_pred['name']]['correct'] += 1

                # 按信号类型统计
                vol_signal_accuracy[vol_pred['name']]['total'] += 1
                if vol_correct:
                    vol_signal_accuracy[vol_pred['name']]['correct'] += 1

            # 3. 增强策略（基线+覆盖）
            override = _vol_override_baseline(base_rule, vol_signals)

            if base_rule is not None:
                if override is not None:
                    # 有覆盖
                    enhanced_correct = override['pred_up'] == actual_next_up
                    enhanced['total'] += 1
                    if enhanced_correct:
                        enhanced['correct'] += 1

                    override_stats['total'] += 1
                    base_was_correct = base_rule['pred_up'] == actual_next_up
                    if enhanced_correct and not base_was_correct:
                        override_stats['improved'] += 1
                    elif not enhanced_correct and base_was_correct:
                        override_stats['worsened'] += 1
                else:
                    # 无覆盖，沿用基线
                    enhanced['total'] += 1
                    if base_rule['pred_up'] == actual_next_up:
                        enhanced['correct'] += 1
            elif vol_pred is not None:
                # 基线无命中，成交量有命中 → 增强策略使用成交量
                enhanced['total'] += 1
                if vol_pred['pred_up'] == actual_next_up:
                    enhanced['correct'] += 1

            # 4. 组合分析
            if base_rule is not None and vol_pred is not None:
                if base_rule['pred_up'] == vol_pred['pred_up']:
                    # 方向一致 → 确认
                    combo_stats['baseline_hit_vol_confirm']['total'] += 1
                    if base_rule['pred_up'] == actual_next_up:
                        combo_stats['baseline_hit_vol_confirm']['correct'] += 1
                else:
                    # 方向矛盾
                    combo_stats['baseline_hit_vol_conflict']['total'] += 1
                    if base_rule['pred_up'] == actual_next_up:
                        combo_stats['baseline_hit_vol_conflict']['correct'] += 1
            elif base_rule is not None and vol_pred is None:
                combo_stats['baseline_hit_vol_neutral']['total'] += 1
                if base_rule['pred_up'] == actual_next_up:
                    combo_stats['baseline_hit_vol_neutral']['correct'] += 1
            elif base_rule is None and vol_pred is not None:
                combo_stats['baseline_miss_vol_hit']['total'] += 1
                if vol_pred['pred_up'] == actual_next_up:
                    combo_stats['baseline_miss_vol_hit']['correct'] += 1
            else:
                combo_stats['both_miss'] += 1

        processed += 1
        if processed % 500 == 0:
            logger.info("  已处理 %d/%d 只股票...", processed, len(all_codes))

    # ── 输出结果 ──
    _print_results(baseline, enhanced, vol_standalone, override_stats,
                   signal_freq, combo_stats, vol_signal_accuracy,
                   vol_price_pattern, t_start)


def _pct(correct, total):
    """格式化百分比"""
    if total == 0:
        return 'N/A'
    return f'{correct/total*100:.1f}%'


def _print_results(baseline, enhanced, vol_standalone, override_stats,
                   signal_freq, combo_stats, vol_signal_accuracy,
                   vol_price_pattern, t_start):
    """输出回测结果"""
    elapsed = (datetime.now() - t_start).total_seconds()

    logger.info("")
    logger.info("=" * 80)
    logger.info("  成交量信号回测验证结果")
    logger.info("=" * 80)

    # ── 1. 总览 ──
    logger.info("")
    logger.info("  ── 1. 总览对比 ──")
    base_acc = _pct(baseline['correct'], baseline['total'])
    enh_acc = _pct(enhanced['correct'], enhanced['total'])
    base_cov = _pct(baseline['total'], baseline['all_weeks'])
    enh_cov = _pct(enhanced['total'], baseline['all_weeks'])
    logger.info("    总周数:     %d", baseline['all_weeks'])
    logger.info("    基线(Baseline):  准确率 %s (%d/%d), 覆盖率 %s",
                base_acc, baseline['correct'], baseline['total'], base_cov)
    logger.info("    增强(Enhanced):  准确率 %s (%d/%d), 覆盖率 %s",
                enh_acc, enhanced['correct'], enhanced['total'], enh_cov)
    if baseline['total'] > 0 and enhanced['total'] > 0:
        diff = enhanced['correct']/enhanced['total']*100 - baseline['correct']/baseline['total']*100
        logger.info("    准确率变化:  %+.2f%%", diff)
        cov_diff = enhanced['total']/baseline['all_weeks']*100 - baseline['total']/baseline['all_weeks']*100
        logger.info("    覆盖率变化:  %+.2f%%", cov_diff)

    # ── 2. 覆盖统计 ──
    logger.info("")
    logger.info("  ── 2. 成交量覆盖基线统计 ──")
    logger.info("    覆盖次数:    %d", override_stats['total'])
    logger.info("    改善(错→对): %d", override_stats['improved'])
    logger.info("    恶化(对→错): %d", override_stats['worsened'])
    if override_stats['total'] > 0:
        net = override_stats['improved'] - override_stats['worsened']
        logger.info("    净改善:      %+d (%.1f%%的覆盖是有益的)",
                    net, override_stats['improved']/override_stats['total']*100)

    # ── 3. 独立成交量信号准确率 ──
    logger.info("")
    logger.info("  ── 3. 独立成交量信号准确率 ──")
    sorted_signals = sorted(vol_signal_accuracy.items(),
                            key=lambda x: -x[1]['total'])
    for name, stats in sorted_signals:
        acc = _pct(stats['correct'], stats['total'])
        logger.info("    %-40s %s (%d/%d)",
                    name, acc, stats['correct'], stats['total'])

    # ── 4. 信号触发频率 ──
    logger.info("")
    logger.info("  ── 4. 信号触发频率(每周每股) ──")
    for sig, count in sorted(signal_freq.items(), key=lambda x: -x[1]):
        pct_of_total = count / baseline['all_weeks'] * 100 if baseline['all_weeks'] > 0 else 0
        logger.info("    %-35s %d次 (%.2f%%)", sig, count, pct_of_total)

    # ── 5. 组合分析 ──
    logger.info("")
    logger.info("  ── 5. 基线×成交量组合分析 ──")
    for key, label in [
        ('baseline_hit_vol_confirm', '基线命中+成交量确认(方向一致)'),
        ('baseline_hit_vol_conflict', '基线命中+成交量矛盾(方向相反)'),
        ('baseline_hit_vol_neutral', '基线命中+成交量无信号'),
        ('baseline_miss_vol_hit', '基线未命中+成交量命中'),
    ]:
        s = combo_stats[key]
        acc = _pct(s['correct'], s['total'])
        logger.info("    %-45s %s (%d/%d)", label, acc, s['correct'], s['total'])
    logger.info("    %-45s %d", '双方均未命中', combo_stats['both_miss'])

    # ── 6. 量价关系分类 → 下周方向 ──
    logger.info("")
    logger.info("  ── 6. 量价关系 → 下周涨跌概率 ──")
    for pattern in ['价升量增', '价升量缩', '价跌量增', '价跌量缩',
                    '价平量增', '价平量缩', '价平量平']:
        s = vol_price_pattern.get(pattern, {'next_up': 0, 'next_down': 0, 'total': 0})
        if s['total'] > 0:
            up_pct = s['next_up'] / s['total'] * 100
            logger.info("    %-12s → 下周涨 %.1f%% / 跌 %.1f%%  (样本%d)",
                        pattern, up_pct, 100 - up_pct, s['total'])

    # ── 7. 结论 ──
    logger.info("")
    logger.info("  ── 7. 结论 ──")
    if baseline['total'] > 0 and enhanced['total'] > 0:
        base_rate = baseline['correct'] / baseline['total'] * 100
        enh_rate = enhanced['correct'] / enhanced['total'] * 100
        if enh_rate > base_rate + 0.5:
            logger.info("    ✅ 成交量信号对下周预测有正向增强效果 (+%.2f%%)", enh_rate - base_rate)
        elif enh_rate < base_rate - 0.5:
            logger.info("    ❌ 成交量信号对下周预测有负面影响 (%.2f%%)", enh_rate - base_rate)
        else:
            logger.info("    ➖ 成交量信号对下周预测准确率影响不显著 (%.2f%%)", enh_rate - base_rate)

        if override_stats['total'] > 0:
            net = override_stats['improved'] - override_stats['worsened']
            if net > 0:
                logger.info("    ✅ 覆盖策略净改善 %d 个预测", net)
            else:
                logger.info("    ❌ 覆盖策略净恶化 %d 个预测", -net)

        # 找出最有价值的独立信号
        best_signals = []
        for name, stats in vol_signal_accuracy.items():
            if stats['total'] >= 10:  # 至少10个样本
                acc = stats['correct'] / stats['total'] * 100
                if acc >= 55:
                    best_signals.append((name, acc, stats['total']))
        if best_signals:
            best_signals.sort(key=lambda x: -x[1])
            logger.info("    有价值的独立信号(准确率≥55%%, 样本≥10):")
            for name, acc, n in best_signals[:10]:
                logger.info("      %-40s %.1f%% (n=%d)", name, acc, n)

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest(n_weeks=29, sample_limit=0)
