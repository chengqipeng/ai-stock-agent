#!/usr/bin/env python3
"""
V3规则引擎 A/B 回测验证
=========================
对比：
  A组(v2基线): 旧规则引擎 (跌>2%+大盘深跌→涨 + 周跌>5%→继续跌)
  B组(v3优化): 新规则引擎 (大盘深跌>3% + 上证多因子过滤)

用法：
    python -m day_week_predicted.backtest.volume_ab_test_v3
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
    _get_stock_index,
    _nw_extract_features,
    _nw_match_rule,
    _detect_volume_patterns,
    _adjust_nw_confidence_by_volume,
)

# v2旧规则（硬编码用于对比）
_OLD_INDEX_MKT_THRESHOLD = {
    '000001.SH': 1.0,
    '399001.SZ': 1.5,
    '899050.SZ': 2.0,
}

def _old_match_rule(this_chg, market_chg, stock_idx):
    """v2旧规则匹配。"""
    mkt_t = _OLD_INDEX_MKT_THRESHOLD.get(stock_idx, 1.0)
    suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
    if this_chg < -2 and market_chg < -mkt_t:
        return ('跌>2%+大盘深跌→涨', True, 1)
    if this_chg < -5 and suffix == 'SH':
        return ('周跌>5%→继续跌', False, 2)
    return None


def run_backtest(n_weeks=29, sample_limit=0):
    """V3 A/B回测。"""
    t_start = datetime.now()
    logger.info("=" * 80)
    logger.info("  V3规则引擎 A/B 回测验证 (n_weeks=%d)", n_weeks)
    logger.info("=" * 80)

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
    all_weeks = 0
    # A组: v2旧规则
    a_total = 0; a_correct = 0
    a_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    # B组: v3新规则
    b_total = 0; b_correct = 0
    b_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    # B组+成交量修正
    bv_by_conf = defaultdict(lambda: {'correct': 0, 'total': 0})

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
            this_week_chg = _compound_return(this_pcts)

            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            all_weeks += 1

            # A组: v2旧规则
            old_result = _old_match_rule(this_week_chg, market_chg, stock_idx)
            if old_result:
                rn, pred_up, tier = old_result
                a_total += 1
                a_by_rule[rn]['total'] += 1
                if pred_up == actual_up:
                    a_correct += 1
                    a_by_rule[rn]['correct'] += 1

            # B组: v3新规则 (使用生产代码)
            # 计算新特征
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
                        price_pos_60 = (latest_c - min_c) / (max_c - min_c)

            prev_week_chg = None
            prev_klines = hist[-5:] if len(hist) >= 5 else hist
            if prev_klines:
                prev_week_chg = _compound_return([k['change_percent'] for k in prev_klines])

            feat = _nw_extract_features(
                this_pcts, market_chg, market_index=stock_idx,
                price_pos_60=price_pos_60, prev_week_chg=prev_week_chg)
            rule = _nw_match_rule(feat)

            if rule:
                pred_up = rule['pred_up']
                b_total += 1
                b_by_rule[rule['name']]['total'] += 1
                is_correct = pred_up == actual_up
                if is_correct:
                    b_correct += 1
                    b_by_rule[rule['name']]['correct'] += 1

                # 成交量修正
                tier = rule['tier']
                conf = 'high' if tier == 1 else 'reference'
                vol_patterns = _detect_volume_patterns(this_days, klines)
                if vol_patterns.get('vol_direction'):
                    conf, _ = _adjust_nw_confidence_by_volume(pred_up, conf, vol_patterns)
                bv_by_conf[conf]['total'] += 1
                if is_correct:
                    bv_by_conf[conf]['correct'] += 1

        processed += 1
        if processed % 500 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    # ── 输出结果 ──
    elapsed = (datetime.now() - t_start).total_seconds()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  V3规则引擎 A/B 回测结果")
    logger.info("=" * 80)

    logger.info("")
    logger.info("  ── 1. 整体对比 ──")
    logger.info("    总可评估周数: %d", all_weeks)
    logger.info("    A组(v2旧规则): 准确率 %s (%d/%d) 覆盖%s",
                _p(a_correct, a_total), a_correct, a_total, _p(a_total, all_weeks))
    logger.info("    B组(v3新规则): 准确率 %s (%d/%d) 覆盖%s",
                _p(b_correct, b_total), b_correct, b_total, _p(b_total, all_weeks))

    logger.info("")
    logger.info("  ── 2. A组(v2)按规则分层 ──")
    for rn in sorted(a_by_rule.keys()):
        s = a_by_rule[rn]
        logger.info("    %-35s %s (%d/%d)", rn, _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 3. B组(v3)按规则分层 ──")
    for rn in sorted(b_by_rule.keys()):
        s = b_by_rule[rn]
        logger.info("    %-40s %s (%d/%d)", rn, _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 4. B组+成交量修正 按置信度分层 ──")
    for conf in sorted(bv_by_conf.keys()):
        s = bv_by_conf[conf]
        logger.info("    %-12s %s (%d/%d)", conf, _p(s['correct'], s['total']), s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 5. 改进幅度 ──")
    if a_total > 0 and b_total > 0:
        a_acc = a_correct / a_total * 100
        b_acc = b_correct / b_total * 100
        logger.info("    准确率: %.1f%% → %.1f%% (%+.1f%%)", a_acc, b_acc, b_acc - a_acc)
        logger.info("    覆盖率: %.1f%% → %.1f%%",
                    a_total / all_weeks * 100, b_total / all_weeks * 100)
        logger.info("    有效预测数: %d → %d (%+d)", a_total, b_total, b_total - a_total)

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest(n_weeks=29, sample_limit=0)
