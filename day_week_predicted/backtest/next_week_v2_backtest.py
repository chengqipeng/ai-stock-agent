#!/usr/bin/env python3
"""
下周预测v3回测 — 多指数 + 价格位置 + 前周动量 + 成交量修正
==========================================================
验证V3规则引擎准确率，按规则、指数、市场、置信度分别统计。

V3新增特征:
  - price_pos_60: 价格在60日高低点中的位置(0~1)
  - prev_week_chg: 前一周涨跌幅
  - 成交量置信度修正

用法：
    python -m day_week_predicted.backtest.next_week_v2_backtest
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
    _get_stock_index,
    _nw_extract_features,
    _nw_match_rule,
    _NW_RULES,
    _detect_volume_patterns,
    _adjust_nw_confidence_by_volume,
)


def run_backtest(n_weeks=29, sample_limit=0):
    """运行下周预测v2回测，按规则/指数分层统计。"""
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  下周预测v2回测 (n_weeks=%d)", n_weeks)
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

    # 个股K线（含 close_price 用于价格位置计算）
    logger.info("加载个股K线...")
    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(all_codes), batch_size):
        batch = all_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, change_percent, "
            f"high_price, low_price, trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, latest_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'close': _to_float(row['close_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'volume': _to_float(row['trading_volume']),
                'change_percent': _to_float(row['change_percent']),
            })

    # 所有需要的指数K线
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

    # 按ISO周分组各指数
    market_by_week_by_index = {}
    for idx_code, klines_list in market_klines_by_index.items():
        by_week = defaultdict(list)
        for k in klines_list:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            by_week[iw].append(k)
        market_by_week_by_index[idx_code] = by_week

    logger.info("数据加载完成: %d只股票, %d个指数", len(stock_klines), len(market_klines_by_index))

    # ── 回测 ──
    # 统计维度
    global_correct = 0
    global_total = 0
    global_all_weeks = 0

    # 按规则统计
    rule_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    # 按指数统计
    index_stats = defaultdict(lambda: {'correct': 0, 'total': 0, 'all_weeks': 0})
    # 按市场(SH/SZ/BJ)统计
    market_stats = defaultdict(lambda: {'correct': 0, 'total': 0, 'all_weeks': 0})
    # 按tier统计
    tier_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
    # 按置信度统计（含成交量修正）
    conf_stats = defaultdict(lambda: {'correct': 0, 'total': 0})

    idx_names = {'000001.SH': '上证指数', '399001.SZ': '深证成指', '899050.SZ': '北证50'}

    # 用于限制回测周范围
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)

    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        suffix = code.split('.')[-1] if '.' in code else 'SH'
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

            dt_this = datetime.strptime(this_days[0]['date'], '%Y-%m-%d')
            if dt_this < dt_cutoff:
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_week_chg = _compound_return(next_pcts)
            actual_next_up = next_week_chg >= 0

            # 大盘本周涨跌幅（使用个股对应的指数）
            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            global_all_weeks += 1
            index_stats[stock_idx]['all_weeks'] += 1
            market_stats[suffix]['all_weeks'] += 1

            # 计算 price_pos_60（与生产代码 _predict_next_week 一致）
            sorted_all = sorted(klines, key=lambda x: x['date'])
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            price_pos_60 = None
            if len(hist) >= 20:
                hist_closes = [k.get('close', 0) for k in hist[-60:] if k.get('close', 0) > 0]
                if hist_closes:
                    all_c = hist_closes + [k.get('close', 0) for k in this_days if k.get('close', 0) > 0]
                    min_c, max_c = min(all_c), max(all_c)
                    latest_c = this_days[-1].get('close', 0)
                    if max_c > min_c and latest_c > 0:
                        price_pos_60 = round((latest_c - min_c) / (max_c - min_c), 4)

            # 计算 prev_week_chg
            prev_week_chg = None
            prev_klines = hist[-5:] if len(hist) >= 5 else hist
            if prev_klines:
                prev_week_chg = _compound_return([k['change_percent'] for k in prev_klines])

            # 提取特征 & 匹配规则（V3: 含价格位置+前周动量）
            feat = _nw_extract_features(this_pcts, market_chg,
                                        market_index=stock_idx,
                                        price_pos_60=price_pos_60,
                                        prev_week_chg=prev_week_chg)
            rule = _nw_match_rule(feat)

            if rule is None:
                continue

            pred_next_up = rule['pred_up']
            correct = pred_next_up == actual_next_up
            rule_name = rule['name']
            tier = rule['tier']

            # 成交量置信度修正
            confidence = 'high' if tier == 1 else 'reference'
            vol_patterns = _detect_volume_patterns(this_days, klines)
            if vol_patterns.get('vol_direction'):
                confidence, _ = _adjust_nw_confidence_by_volume(
                    pred_next_up, confidence, vol_patterns)

            if correct:
                global_correct += 1
                rule_stats[rule_name]['correct'] += 1
                index_stats[stock_idx]['correct'] += 1
                market_stats[suffix]['correct'] += 1
                tier_stats[tier]['correct'] += 1
                conf_stats[confidence]['correct'] += 1
            global_total += 1
            rule_stats[rule_name]['total'] += 1
            index_stats[stock_idx]['total'] += 1
            market_stats[suffix]['total'] += 1
            tier_stats[tier]['total'] += 1
            conf_stats[confidence]['total'] += 1

    # ── 输出结果 ──
    elapsed = (datetime.now() - t_start).total_seconds()
    global_acc = round(global_correct / global_total * 100, 1) if global_total > 0 else 0
    coverage = round(global_total / global_all_weeks * 100, 1) if global_all_weeks > 0 else 0

    logger.info("")
    logger.info("=" * 70)
    logger.info("  下周预测v3回测结果 (多指数+价格位置+前周动量+成交量修正)")
    logger.info("=" * 70)
    logger.info("  全局准确率: %.1f%% (%d/%d样本, 覆盖%.1f%%)",
                global_acc, global_correct, global_total, coverage)

    # 按Tier统计
    logger.info("")
    logger.info("  ── 按Tier分层 ──")
    for tier in sorted(tier_stats.keys()):
        s = tier_stats[tier]
        acc = round(s['correct'] / s['total'] * 100, 1) if s['total'] > 0 else 0
        logger.info("    Tier %d: %.1f%% (%d/%d)", tier, acc, s['correct'], s['total'])

    # 按置信度统计（含成交量修正）
    logger.info("")
    logger.info("  ── 按置信度分层（含成交量修正） ──")
    for conf in sorted(conf_stats.keys()):
        s = conf_stats[conf]
        acc = round(s['correct'] / s['total'] * 100, 1) if s['total'] > 0 else 0
        logger.info("    %-12s %.1f%% (%d/%d)", conf, acc, s['correct'], s['total'])

    # 按规则统计
    logger.info("")
    logger.info("  ── 按规则统计 ──")
    for rule_name in sorted(rule_stats.keys(), key=lambda x: -rule_stats[x]['total']):
        s = rule_stats[rule_name]
        acc = round(s['correct'] / s['total'] * 100, 1) if s['total'] > 0 else 0
        logger.info("    %-30s %.1f%% (%d/%d)", rule_name, acc, s['correct'], s['total'])

    # 按指数统计
    logger.info("")
    logger.info("  ── 按大盘指数统计 ──")
    for idx_code in sorted(index_stats.keys()):
        s = index_stats[idx_code]
        acc = round(s['correct'] / s['total'] * 100, 1) if s['total'] > 0 else 0
        cov = round(s['total'] / s['all_weeks'] * 100, 1) if s['all_weeks'] > 0 else 0
        name = idx_names.get(idx_code, idx_code)
        logger.info("    %-12s (%s): %.1f%% (%d/%d, 覆盖%.1f%%)",
                    name, idx_code, acc, s['correct'], s['total'], cov)

    # 按市场统计
    logger.info("")
    logger.info("  ── 按市场统计 ──")
    for mkt in sorted(market_stats.keys()):
        s = market_stats[mkt]
        acc = round(s['correct'] / s['total'] * 100, 1) if s['total'] > 0 else 0
        cov = round(s['total'] / s['all_weeks'] * 100, 1) if s['all_weeks'] > 0 else 0
        logger.info("    %s: %.1f%% (%d/%d, 覆盖%.1f%%)", mkt, acc, s['correct'], s['total'], cov)

    # 按指数+规则交叉统计
    logger.info("")
    logger.info("  ── 按指数×规则交叉统计 ──")
    index_rule_stats = defaultdict(lambda: defaultdict(lambda: {'correct': 0, 'total': 0}))

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
            if dt_this < dt_cutoff:
                continue
            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_week_chg = _compound_return(next_pcts)
            actual_next_up = next_week_chg >= 0
            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            sorted_all = sorted(klines, key=lambda x: x['date'])
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]
            price_pos_60 = None
            if len(hist) >= 20:
                hist_closes = [k.get('close', 0) for k in hist[-60:] if k.get('close', 0) > 0]
                if hist_closes:
                    all_c = hist_closes + [k.get('close', 0) for k in this_days if k.get('close', 0) > 0]
                    min_c, max_c = min(all_c), max(all_c)
                    latest_c = this_days[-1].get('close', 0)
                    if max_c > min_c and latest_c > 0:
                        price_pos_60 = round((latest_c - min_c) / (max_c - min_c), 4)
            prev_week_chg = None
            prev_klines = hist[-5:] if len(hist) >= 5 else hist
            if prev_klines:
                prev_week_chg = _compound_return([k['change_percent'] for k in prev_klines])

            feat = _nw_extract_features(this_pcts, market_chg,
                                        market_index=stock_idx,
                                        price_pos_60=price_pos_60,
                                        prev_week_chg=prev_week_chg)
            rule = _nw_match_rule(feat)
            if rule is None:
                continue
            rn = rule['name']
            correct = rule['pred_up'] == actual_next_up
            if correct:
                index_rule_stats[stock_idx][rn]['correct'] += 1
            index_rule_stats[stock_idx][rn]['total'] += 1

    for idx_code in sorted(index_rule_stats.keys()):
        name = idx_names.get(idx_code, idx_code)
        logger.info("    %s (%s):", name, idx_code)
        for rn in sorted(index_rule_stats[idx_code].keys()):
            s = index_rule_stats[idx_code][rn]
            acc = round(s['correct'] / s['total'] * 100, 1) if s['total'] > 0 else 0
            logger.info("      %-30s %.1f%% (%d/%d)", rn, acc, s['correct'], s['total'])

    # 对比：如果全部用上证指数会怎样
    logger.info("")
    logger.info("  ── 对比：全部用上证指数 vs 多指数 ──")
    sh_by_week = market_by_week_by_index.get('000001.SH', {})
    old_correct = 0
    old_total = 0
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue
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
            if dt_this < dt_cutoff:
                continue
            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_week_chg = _compound_return(next_pcts)
            actual_next_up = next_week_chg >= 0
            mw = sh_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            sorted_all = sorted(klines, key=lambda x: x['date'])
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]
            price_pos_60 = None
            if len(hist) >= 20:
                hist_closes = [k.get('close', 0) for k in hist[-60:] if k.get('close', 0) > 0]
                if hist_closes:
                    all_c = hist_closes + [k.get('close', 0) for k in this_days if k.get('close', 0) > 0]
                    min_c, max_c = min(all_c), max(all_c)
                    latest_c = this_days[-1].get('close', 0)
                    if max_c > min_c and latest_c > 0:
                        price_pos_60 = round((latest_c - min_c) / (max_c - min_c), 4)
            prev_week_chg = None
            prev_klines = hist[-5:] if len(hist) >= 5 else hist
            if prev_klines:
                prev_week_chg = _compound_return([k['change_percent'] for k in prev_klines])

            feat = _nw_extract_features(this_pcts, market_chg,
                                        market_index='000001.SH',
                                        price_pos_60=price_pos_60,
                                        prev_week_chg=prev_week_chg)
            rule = _nw_match_rule(feat)
            if rule is None:
                continue
            if rule['pred_up'] == actual_next_up:
                old_correct += 1
            old_total += 1

    old_acc = round(old_correct / old_total * 100, 1) if old_total > 0 else 0
    logger.info("    固定上证指数: %.1f%% (%d/%d)", old_acc, old_correct, old_total)
    logger.info("    多指数匹配:   %.1f%% (%d/%d)", global_acc, global_correct, global_total)
    diff = global_acc - old_acc
    logger.info("    差异: %+.1f%%", diff)

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 70)


if __name__ == '__main__':
    run_backtest(n_weeks=29, sample_limit=0)
