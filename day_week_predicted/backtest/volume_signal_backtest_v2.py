#!/usr/bin/env python3
"""
成交量信号回测验证 v2 — 精细化分析
===================================
基于v1回测发现：
1. 整体增强策略准确率下降(-10.4%)，主要因为覆盖率大幅提升(+39.5%)导致大量低质量预测
2. 有3个信号准确率>55%: 恐慌底(62.6%), 天量阴线(56.0%), 价升量缩(55.9%)
3. 量价关系验证了核心原则: 价跌量缩→下周涨55.1%, 价升量缩→下周跌55.8%
4. 基线+成交量确认(66.4%) vs 基线+成交量矛盾(58.4%) → 确认提升8%

v2改进：
- 只保留准确率>55%的信号
- 不做独立预测，只做置信度修正和确认/否定
- 分析成交量信号对基线预测的修正价值
- 按价格位置(高位/低位)分层分析

用法：
    python -m day_week_predicted.backtest.volume_signal_backtest_v2
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
# 精简版成交量信号检测（只保留有价值的信号）
# ═══════════════════════════════════════════════════════════

def _detect_vol_signals(week_klines: list[dict], hist_klines: list[dict]) -> dict:
    """检测成交量形态信号（精简版，只计算有统计意义的信号）。"""
    signals = {
        'vol_ratio_20': None,
        'vol_ratio_60': None,
        'week_chg': None,
        'price_position': None,
        'above_ma120': None,
        # 有效信号
        'panic_bottom': False,         # V5: 恐慌底 (62.6%)
        'sky_vol_bearish': False,      # V2: 天量阴线 (56.0%)
        'price_up_vol_down': False,    # V6: 价升量缩 (55.9%)
        'rush_up_shrink': False,       # V11: 急涨后缩量 (54.9%)
        'vol_peak_top': False,         # V12: 量峰见顶 (53.5%)
        'price_down_vol_down': False,  # 价跌量缩→下周涨55.1%
        'price_down_vol_up_high': False,  # 高位价跌量增→下周涨53.7%
        # 量价关系分类
        'vol_price_pattern': None,
    }

    if not week_klines or len(hist_klines) < 20:
        return signals

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

    # 价格位置
    hist_closes = [k['close'] for k in hist_klines[-60:] if k.get('close', 0) > 0]
    if hist_closes and week_klines:
        all_closes = hist_closes + [k['close'] for k in week_klines if k.get('close', 0) > 0]
        if all_closes:
            min_c, max_c = min(all_closes), max(all_closes)
            latest_close = week_klines[-1].get('close', 0)
            if max_c > min_c and latest_close > 0:
                signals['price_position'] = round((latest_close - min_c) / (max_c - min_c), 4)

    # MA120
    all_hist_closes = [k['close'] for k in hist_klines if k.get('close', 0) > 0]
    if len(all_hist_closes) >= 120:
        ma120 = _mean(all_hist_closes[-120:])
        latest_close = week_klines[-1].get('close', 0)
        if ma120 > 0 and latest_close > 0:
            signals['above_ma120'] = latest_close > ma120

    vr20 = signals['vol_ratio_20']
    pp = signals['price_position']

    # ── V5: 恐慌底 (62.6%) ──
    if week_chg < -1.0 and vr20 is not None and vr20 > 1.3 and pp is not None and pp < 0.25:
        signals['panic_bottom'] = True

    # ── V2: 天量阴线 (56.0%) ──
    if avg_vol_60 > 0:
        for k in week_klines:
            if k.get('volume', 0) > avg_vol_60 * 3.0:
                if k.get('close', 0) < k.get('open', 0):
                    signals['sky_vol_bearish'] = True
                break

    # ── V6: 价升量缩 (55.9%) ──
    if week_chg > 0.5 and vr20 is not None and vr20 < 0.8:
        signals['price_up_vol_down'] = True

    # ── V11: 急涨后缩量 (54.9%) ──
    if len(week_klines) >= 4:
        mid = len(week_klines) // 2
        first_chg = _compound_return([k['change_percent'] for k in week_klines[:mid]])
        first_vol = _mean([k.get('volume', 0) for k in week_klines[:mid]])
        second_vol = _mean([k.get('volume', 0) for k in week_klines[mid:]])
        if first_chg > 2.0 and first_vol > 0 and second_vol < first_vol * 0.6:
            signals['rush_up_shrink'] = True

    # ── V12: 量峰见顶 (53.5%) ──
    if len(week_vols) >= 3 and week_chg > 1.0:
        peak_idx = week_vols.index(max(week_vols))
        if 0 < peak_idx < len(week_vols) - 1:
            if week_vols[peak_idx] > week_vols[0] * 1.3 and week_vols[peak_idx] > week_vols[-1] * 1.3:
                signals['vol_peak_top'] = True

    # ── 量价关系分类 ──
    if vr20 is not None:
        if week_chg > 0.5 and vr20 > 1.2:
            signals['vol_price_pattern'] = 'price_up_vol_up'
        elif week_chg > 0.5 and vr20 < 0.8:
            signals['vol_price_pattern'] = 'price_up_vol_down'
        elif week_chg < -0.5 and vr20 > 1.2:
            signals['vol_price_pattern'] = 'price_down_vol_up'
        elif week_chg < -0.5 and vr20 < 0.8:
            signals['vol_price_pattern'] = 'price_down_vol_down'
        else:
            signals['vol_price_pattern'] = 'neutral'

    # 价跌量缩
    if week_chg < -0.5 and vr20 is not None and vr20 < 0.8:
        signals['price_down_vol_down'] = True

    # 高位价跌量增
    if week_chg < -0.5 and vr20 is not None and vr20 > 1.2 and pp is not None and pp > 0.7:
        signals['price_down_vol_up_high'] = True

    return signals


# ═══════════════════════════════════════════════════════════
# 置信度修正策略
# ═══════════════════════════════════════════════════════════

def _get_vol_direction(vs: dict) -> str | None:
    """从成交量信号推断方向。返回 'up'/'down'/None"""
    # 看涨信号
    if vs['panic_bottom']:
        return 'up'
    if vs['price_down_vol_down']:
        return 'up'  # 价跌量缩→下周涨55.1%

    # 看跌信号（按准确率排序）
    if vs['sky_vol_bearish']:
        return 'down'
    if vs['price_up_vol_down']:
        return 'down'
    if vs['rush_up_shrink']:
        return 'down'
    if vs['vol_peak_top']:
        return 'down'
    if vs['price_down_vol_up_high']:
        return 'down'

    return None


def run_backtest(n_weeks=29, sample_limit=0):
    """v2回测：精细化分析成交量信号的修正价值。"""
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  成交量信号v2回测 — 精细化分析 (n_weeks=%d)", n_weeks)
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
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    # ── 加载数据 ──
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    logger.info("加载个股K线...")
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

    market_by_week_by_index = {}
    for idx_code, klines_list in market_klines_by_index.items():
        by_week = defaultdict(list)
        for k in klines_list:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            by_week[iw].append(k)
        market_by_week_by_index[idx_code] = by_week

    logger.info("数据加载完成: %d只股票", len(stock_klines))


    # ── 统计容器 ──
    # A. 基线统计
    baseline = {'correct': 0, 'total': 0, 'all_weeks': 0}
    # B. 基线命中时，成交量确认/矛盾/无信号的准确率
    confirm = {'correct': 0, 'total': 0}
    conflict = {'correct': 0, 'total': 0}
    neutral = {'correct': 0, 'total': 0}
    # C. 基线未命中时，各成交量信号的独立准确率
    vol_only = defaultdict(lambda: {'correct': 0, 'total': 0})
    # D. 策略对比：基线 vs 基线+确认过滤 vs 基线+成交量扩展
    strategy_filter = {'correct': 0, 'total': 0}  # 只保留确认的基线预测
    strategy_extend = {'correct': 0, 'total': 0}  # 基线+成交量扩展
    # E. 按量价关系×价格位置分层
    vp_pos = defaultdict(lambda: {'next_up': 0, 'total': 0})
    # F. 按基线规则×成交量确认/矛盾分层
    rule_vol = defaultdict(lambda: {'confirm_ok': 0, 'confirm_n': 0,
                                     'conflict_ok': 0, 'conflict_n': 0,
                                     'neutral_ok': 0, 'neutral_n': 0})
    # G. 恐慌底细分
    panic_detail = defaultdict(lambda: {'correct': 0, 'total': 0})
    # H. 价升量缩细分（按价格位置）
    puvd_detail = defaultdict(lambda: {'correct': 0, 'total': 0})
    # I. 天量阴线细分
    sky_bear_detail = defaultdict(lambda: {'correct': 0, 'total': 0})
    # J. 最优组合策略
    best_combo = {'correct': 0, 'total': 0, 'all_weeks': 0}

    processed = 0
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        idx_by_week = market_by_week_by_index.get(stock_idx, {})

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

            dt_this = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_this < dt_end - timedelta(days=n_weeks * 7 + 14):
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_week_chg = _compound_return(next_pcts)
            actual_up = next_week_chg >= 0

            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            baseline['all_weeks'] += 1
            best_combo['all_weeks'] += 1

            # 基线预测
            feat = _nw_extract_features(this_pcts, market_chg, market_index=stock_idx)
            base_rule = _nw_match_rule(feat)

            # 成交量信号
            first_date = this_days[0]['date']
            hist = [k for k in klines if k['date'] < first_date]
            hist.sort(key=lambda x: x['date'])
            vs = _detect_vol_signals(this_days, hist)
            vol_dir = _get_vol_direction(vs)

            pp = vs.get('price_position')
            pos_label = 'high' if pp is not None and pp > 0.7 else (
                'low' if pp is not None and pp < 0.3 else 'mid')

            # ── A. 基线统计 ──
            if base_rule is not None:
                base_correct = base_rule['pred_up'] == actual_up
                baseline['total'] += 1
                if base_correct:
                    baseline['correct'] += 1

                rn = base_rule['name']

                # ── B. 确认/矛盾/无信号 ──
                if vol_dir is not None:
                    vol_agrees = (vol_dir == 'up') == base_rule['pred_up']
                    if vol_agrees:
                        confirm['total'] += 1
                        if base_correct:
                            confirm['correct'] += 1
                        rule_vol[rn]['confirm_n'] += 1
                        if base_correct:
                            rule_vol[rn]['confirm_ok'] += 1
                    else:
                        conflict['total'] += 1
                        if base_correct:
                            conflict['correct'] += 1
                        rule_vol[rn]['conflict_n'] += 1
                        if base_correct:
                            rule_vol[rn]['conflict_ok'] += 1
                else:
                    neutral['total'] += 1
                    if base_correct:
                        neutral['correct'] += 1
                    rule_vol[rn]['neutral_n'] += 1
                    if base_correct:
                        rule_vol[rn]['neutral_ok'] += 1

                # ── D. 策略对比 ──
                # 过滤策略：只保留成交量确认或无信号的
                if vol_dir is None or (vol_dir == 'up') == base_rule['pred_up']:
                    strategy_filter['total'] += 1
                    if base_correct:
                        strategy_filter['correct'] += 1

                # 扩展策略：基线命中就用基线
                strategy_extend['total'] += 1
                if base_correct:
                    strategy_extend['correct'] += 1

                # 最优组合：基线命中+成交量矛盾时不预测，其他用基线
                if vol_dir is not None and (vol_dir == 'up') != base_rule['pred_up']:
                    pass  # 矛盾时跳过
                else:
                    best_combo['total'] += 1
                    if base_correct:
                        best_combo['correct'] += 1

            else:
                # ── C. 基线未命中，成交量独立预测 ──
                if vol_dir is not None:
                    vol_correct = (vol_dir == 'up') == actual_up

                    # 扩展策略：基线未命中时用成交量
                    strategy_extend['total'] += 1
                    if vol_correct:
                        strategy_extend['correct'] += 1

                    # 按信号类型统计
                    if vs['panic_bottom']:
                        vol_only['恐慌底→涨']['total'] += 1
                        if vol_correct:
                            vol_only['恐慌底→涨']['correct'] += 1
                    elif vs['sky_vol_bearish']:
                        vol_only['天量阴线→跌']['total'] += 1
                        if vol_correct:
                            vol_only['天量阴线→跌']['correct'] += 1
                    elif vs['price_up_vol_down']:
                        vol_only['价升量缩→跌']['total'] += 1
                        if vol_correct:
                            vol_only['价升量缩→跌']['correct'] += 1
                    elif vs['rush_up_shrink']:
                        vol_only['急涨缩量→跌']['total'] += 1
                        if vol_correct:
                            vol_only['急涨缩量→跌']['correct'] += 1
                    elif vs['vol_peak_top']:
                        vol_only['量峰见顶→跌']['total'] += 1
                        if vol_correct:
                            vol_only['量峰见顶→跌']['correct'] += 1
                    elif vs['price_down_vol_down']:
                        vol_only['价跌量缩→涨']['total'] += 1
                        if vol_correct:
                            vol_only['价跌量缩→涨']['correct'] += 1
                    elif vs['price_down_vol_up_high']:
                        vol_only['高位价跌量增→跌']['total'] += 1
                        if vol_correct:
                            vol_only['高位价跌量增→跌']['correct'] += 1

                    # 最优组合：只用高准确率信号
                    if vs['panic_bottom'] or vs['sky_vol_bearish']:
                        best_combo['total'] += 1
                        if vol_correct:
                            best_combo['correct'] += 1

            # ── E. 量价关系×位置 ──
            vpp = vs.get('vol_price_pattern')
            if vpp:
                key = f"{vpp}@{pos_label}"
                vp_pos[key]['total'] += 1
                if actual_up:
                    vp_pos[key]['next_up'] += 1

            # ── G/H/I. 信号细分 ──
            if vs['panic_bottom']:
                panic_detail[f"pos={pos_label}"]["total"] += 1
                if actual_up:
                    panic_detail[f"pos={pos_label}"]["correct"] += 1
                mkt_label = '大盘跌' if market_chg < -1 else ('大盘涨' if market_chg > 1 else '大盘平')
                panic_detail[f"mkt={mkt_label}"]["total"] += 1
                if actual_up:
                    panic_detail[f"mkt={mkt_label}"]["correct"] += 1

            if vs['price_up_vol_down']:
                puvd_detail[f"pos={pos_label}"]["total"] += 1
                if not actual_up:  # 预测跌
                    puvd_detail[f"pos={pos_label}"]["correct"] += 1

            if vs['sky_vol_bearish']:
                sky_bear_detail[f"pos={pos_label}"]["total"] += 1
                if not actual_up:
                    sky_bear_detail[f"pos={pos_label}"]["correct"] += 1

        processed += 1
        if processed % 500 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))


    # ── 输出结果 ──
    elapsed = (datetime.now() - t_start).total_seconds()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  成交量信号v2回测结果 — 精细化分析")
    logger.info("=" * 80)

    # 1. 总览
    logger.info("")
    logger.info("  ── 1. 策略对比 ──")
    logger.info("    总周数: %d", baseline['all_weeks'])
    logger.info("    A. 基线(原始):       %s (%d/%d) 覆盖%s",
                _p(baseline['correct'], baseline['total']),
                baseline['correct'], baseline['total'],
                _p(baseline['total'], baseline['all_weeks']))
    logger.info("    B. 过滤策略(去矛盾): %s (%d/%d) 覆盖%s",
                _p(strategy_filter['correct'], strategy_filter['total']),
                strategy_filter['correct'], strategy_filter['total'],
                _p(strategy_filter['total'], baseline['all_weeks']))
    logger.info("    C. 扩展策略(+成交量): %s (%d/%d) 覆盖%s",
                _p(strategy_extend['correct'], strategy_extend['total']),
                strategy_extend['correct'], strategy_extend['total'],
                _p(strategy_extend['total'], baseline['all_weeks']))
    logger.info("    D. 最优组合(去矛盾+高信号): %s (%d/%d) 覆盖%s",
                _p(best_combo['correct'], best_combo['total']),
                best_combo['correct'], best_combo['total'],
                _p(best_combo['total'], best_combo['all_weeks']))

    # 2. 确认/矛盾分析
    logger.info("")
    logger.info("  ── 2. 基线命中时 成交量确认/矛盾效果 ──")
    logger.info("    确认(方向一致): %s (%d/%d)",
                _p(confirm['correct'], confirm['total']),
                confirm['correct'], confirm['total'])
    logger.info("    矛盾(方向相反): %s (%d/%d)",
                _p(conflict['correct'], conflict['total']),
                conflict['correct'], conflict['total'])
    logger.info("    无信号:         %s (%d/%d)",
                _p(neutral['correct'], neutral['total']),
                neutral['correct'], neutral['total'])
    if confirm['total'] > 0 and conflict['total'] > 0:
        diff = confirm['correct']/confirm['total']*100 - conflict['correct']/conflict['total']*100
        logger.info("    确认vs矛盾差异: %+.1f%% → %s",
                    diff, "确认有增强效果" if diff > 2 else "差异不显著")

    # 3. 按基线规则×成交量分层
    logger.info("")
    logger.info("  ── 3. 按基线规则×成交量确认/矛盾分层 ──")
    for rn in sorted(rule_vol.keys()):
        rv = rule_vol[rn]
        parts = []
        if rv['confirm_n'] > 0:
            parts.append(f"确认{_p(rv['confirm_ok'], rv['confirm_n'])}({rv['confirm_n']})")
        if rv['conflict_n'] > 0:
            parts.append(f"矛盾{_p(rv['conflict_ok'], rv['conflict_n'])}({rv['conflict_n']})")
        if rv['neutral_n'] > 0:
            parts.append(f"无信号{_p(rv['neutral_ok'], rv['neutral_n'])}({rv['neutral_n']})")
        logger.info("    %-30s %s", rn, ' | '.join(parts))

    # 4. 基线未命中时的独立信号
    logger.info("")
    logger.info("  ── 4. 基线未命中时 成交量独立预测 ──")
    for name in sorted(vol_only.keys(), key=lambda x: -vol_only[x]['total']):
        s = vol_only[name]
        logger.info("    %-25s %s (%d/%d)",
                    name, _p(s['correct'], s['total']), s['correct'], s['total'])

    # 5. 量价关系×位置
    logger.info("")
    logger.info("  ── 5. 量价关系×价格位置 → 下周涨概率 ──")
    for key in sorted(vp_pos.keys()):
        s = vp_pos[key]
        if s['total'] >= 50:
            up_pct = s['next_up'] / s['total'] * 100
            logger.info("    %-30s 涨%.1f%% (n=%d)", key, up_pct, s['total'])

    # 6. 恐慌底细分
    logger.info("")
    logger.info("  ── 6. 恐慌底信号细分 ──")
    for key in sorted(panic_detail.keys()):
        s = panic_detail[key]
        logger.info("    %-20s %s (%d/%d)",
                    key, _p(s['correct'], s['total']), s['correct'], s['total'])

    # 7. 价升量缩细分
    logger.info("")
    logger.info("  ── 7. 价升量缩→跌 细分(按位置) ──")
    for key in sorted(puvd_detail.keys()):
        s = puvd_detail[key]
        logger.info("    %-20s %s (%d/%d)",
                    key, _p(s['correct'], s['total']), s['correct'], s['total'])

    # 8. 天量阴线细分
    logger.info("")
    logger.info("  ── 8. 天量阴线→跌 细分(按位置) ──")
    for key in sorted(sky_bear_detail.keys()):
        s = sky_bear_detail[key]
        logger.info("    %-20s %s (%d/%d)",
                    key, _p(s['correct'], s['total']), s['correct'], s['total'])

    # 9. 结论
    logger.info("")
    logger.info("  ── 9. 结论 ──")
    if baseline['total'] > 0:
        base_rate = baseline['correct'] / baseline['total'] * 100

        # 过滤策略
        if strategy_filter['total'] > 0:
            filt_rate = strategy_filter['correct'] / strategy_filter['total'] * 100
            filt_diff = filt_rate - base_rate
            if filt_diff > 0.5:
                logger.info("    ✅ 过滤策略(去除矛盾预测): +%.2f%% 准确率提升", filt_diff)
            else:
                logger.info("    ➖ 过滤策略效果不显著: %+.2f%%", filt_diff)

        # 最优组合
        if best_combo['total'] > 0:
            best_rate = best_combo['correct'] / best_combo['total'] * 100
            best_diff = best_rate - base_rate
            if best_diff > 0.5:
                logger.info("    ✅ 最优组合策略: +%.2f%% 准确率提升 (覆盖%s)",
                            best_diff, _p(best_combo['total'], best_combo['all_weeks']))
            else:
                logger.info("    ➖ 最优组合策略: %+.2f%%", best_diff)

        # 确认增强
        if confirm['total'] > 0:
            conf_rate = confirm['correct'] / confirm['total'] * 100
            conf_diff = conf_rate - base_rate
            logger.info("    %s 成交量确认增强: %+.2f%% (基线%.1f%% → 确认后%.1f%%)",
                        "✅" if conf_diff > 1 else "➖", conf_diff, base_rate, conf_rate)

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest(n_weeks=29, sample_limit=0)
