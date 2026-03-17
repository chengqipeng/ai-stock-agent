#!/usr/bin/env python3
"""
成交量置信度修正 A/B 回测
=========================
对比：
  A组(基线): 原始 _NW_RULES 规则引擎
  B组(增强): 原始规则 + 成交量置信度修正

验证指标：
  1. 整体准确率对比
  2. 按置信度分层准确率
  3. 成交量确认/矛盾/无信号的准确率分布
  4. 按规则×置信度交叉统计

用法：
    python -m day_week_predicted.backtest.volume_ab_test
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


def run_backtest(n_weeks=29, sample_limit=0):
    """A/B回测：基线 vs 成交量增强。"""
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  成交量置信度修正 A/B 回测 (n_weeks=%d)", n_weeks)
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

    # ── 加载数据（含量价） ──
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    logger.info("加载个股K线(含量价)...")
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

    # A组: 基线
    a_total = 0
    a_correct = 0
    a_by_conf = defaultdict(lambda: {'correct': 0, 'total': 0})
    a_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})

    # B组: 增强
    b_total = 0
    b_correct = 0
    b_by_conf = defaultdict(lambda: {'correct': 0, 'total': 0})
    b_by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})

    # 成交量效果分析
    vol_effect = {
        'confirm': {'correct': 0, 'total': 0},
        'conflict': {'correct': 0, 'total': 0},
        'neutral': {'correct': 0, 'total': 0},
    }
    # 置信度变化统计
    conf_changes = defaultdict(int)  # "high→reference": count
    # 按规则×成交量分层
    rule_vol_detail = defaultdict(lambda: {
        'confirm': {'correct': 0, 'total': 0},
        'conflict': {'correct': 0, 'total': 0},
        'neutral': {'correct': 0, 'total': 0},
    })
    # 过滤策略（去除矛盾预测）
    c_filter = {'correct': 0, 'total': 0}

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

            all_weeks += 1

            # 基线预测
            feat = _nw_extract_features(this_pcts, market_chg, market_index=stock_idx)
            rule = _nw_match_rule(feat)

            if rule is None:
                continue

            pred_up = rule['pred_up']
            base_correct = pred_up == actual_up
            tier = rule['tier']
            rn = rule['name']

            # A组: 基线置信度
            if tier == 1:
                a_conf = 'high'
            else:
                a_conf = 'reference'

            a_total += 1
            a_by_conf[a_conf]['total'] += 1
            a_by_rule[rn]['total'] += 1
            if base_correct:
                a_correct += 1
                a_by_conf[a_conf]['correct'] += 1
                a_by_rule[rn]['correct'] += 1

            # B组: 成交量增强置信度
            vol_patterns = _detect_volume_patterns(this_days, klines)
            b_conf = a_conf  # 初始与基线相同
            vol_note = ''
            vol_dir = vol_patterns.get('vol_direction')

            if vol_dir is not None:
                b_conf, vol_note = _adjust_nw_confidence_by_volume(pred_up, b_conf, vol_patterns)

            b_total += 1
            b_by_conf[b_conf]['total'] += 1
            b_by_rule[rn]['total'] += 1
            if base_correct:
                b_correct += 1
                b_by_conf[b_conf]['correct'] += 1
                b_by_rule[rn]['correct'] += 1

            # 置信度变化
            if a_conf != b_conf:
                conf_changes[f"{a_conf}→{b_conf}"] += 1

            # 成交量效果分析
            if vol_dir is not None:
                vol_agrees = (vol_dir == 'up') == pred_up
                if vol_agrees:
                    vol_effect['confirm']['total'] += 1
                    if base_correct:
                        vol_effect['confirm']['correct'] += 1
                    rule_vol_detail[rn]['confirm']['total'] += 1
                    if base_correct:
                        rule_vol_detail[rn]['confirm']['correct'] += 1
                    # 过滤策略：确认的保留
                    c_filter['total'] += 1
                    if base_correct:
                        c_filter['correct'] += 1
                else:
                    vol_effect['conflict']['total'] += 1
                    if base_correct:
                        vol_effect['conflict']['correct'] += 1
                    rule_vol_detail[rn]['conflict']['total'] += 1
                    if base_correct:
                        rule_vol_detail[rn]['conflict']['correct'] += 1
                    # 过滤策略：矛盾的不保留
            else:
                vol_effect['neutral']['total'] += 1
                if base_correct:
                    vol_effect['neutral']['correct'] += 1
                rule_vol_detail[rn]['neutral']['total'] += 1
                if base_correct:
                    rule_vol_detail[rn]['neutral']['correct'] += 1
                # 过滤策略：无信号的保留
                c_filter['total'] += 1
                if base_correct:
                    c_filter['correct'] += 1

        processed += 1
        if processed % 500 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))


    # ── 输出结果 ──
    elapsed = (datetime.now() - t_start).total_seconds()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  成交量置信度修正 A/B 回测结果")
    logger.info("=" * 80)

    logger.info("")
    logger.info("  ── 1. 整体对比 ──")
    logger.info("    总可评估周数: %d", all_weeks)
    logger.info("    A组(基线):  准确率 %s (%d/%d) 覆盖%s",
                _p(a_correct, a_total), a_correct, a_total, _p(a_total, all_weeks))
    logger.info("    B组(增强):  准确率 %s (%d/%d) 覆盖%s (方向不变,仅置信度调整)",
                _p(b_correct, b_total), b_correct, b_total, _p(b_total, all_weeks))
    logger.info("    C组(过滤):  准确率 %s (%d/%d) 覆盖%s (去除矛盾预测)",
                _p(c_filter['correct'], c_filter['total']),
                c_filter['correct'], c_filter['total'],
                _p(c_filter['total'], all_weeks))
    logger.info("    注: B组方向与A组完全相同，仅置信度不同")

    logger.info("")
    logger.info("  ── 2. 按置信度分层 ──")
    logger.info("    A组(基线):")
    for conf in sorted(a_by_conf.keys()):
        s = a_by_conf[conf]
        logger.info("      %-12s %s (%d/%d)", conf, _p(s['correct'], s['total']),
                    s['correct'], s['total'])
    logger.info("    B组(增强):")
    for conf in sorted(b_by_conf.keys()):
        s = b_by_conf[conf]
        logger.info("      %-12s %s (%d/%d)", conf, _p(s['correct'], s['total']),
                    s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 3. 置信度变化统计 ──")
    total_changes = sum(conf_changes.values())
    logger.info("    置信度发生变化: %d/%d (%.1f%%)",
                total_changes, a_total, total_changes/a_total*100 if a_total > 0 else 0)
    for change, count in sorted(conf_changes.items(), key=lambda x: -x[1]):
        logger.info("      %-25s %d次", change, count)

    logger.info("")
    logger.info("  ── 4. 成交量确认/矛盾/无信号 准确率 ──")
    for key, label in [('confirm', '确认(方向一致)'), ('conflict', '矛盾(方向相反)'),
                       ('neutral', '无信号')]:
        s = vol_effect[key]
        logger.info("    %-20s %s (%d/%d)", label, _p(s['correct'], s['total']),
                    s['correct'], s['total'])
    if vol_effect['confirm']['total'] > 0 and vol_effect['conflict']['total'] > 0:
        diff = (vol_effect['confirm']['correct']/vol_effect['confirm']['total']*100 -
                vol_effect['conflict']['correct']/vol_effect['conflict']['total']*100)
        logger.info("    确认vs矛盾差距: %.1f个百分点", diff)

    logger.info("")
    logger.info("  ── 5. 按规则×成交量分层 ──")
    for rn in sorted(rule_vol_detail.keys()):
        rv = rule_vol_detail[rn]
        parts = []
        for key, label in [('confirm', '确认'), ('conflict', '矛盾'), ('neutral', '无信号')]:
            s = rv[key]
            if s['total'] > 0:
                parts.append(f"{label}{_p(s['correct'], s['total'])}({s['total']})")
        logger.info("    %-30s %s", rn, ' | '.join(parts))

    logger.info("")
    logger.info("  ── 6. 置信度分层的实际价值 ──")
    logger.info("    B组置信度分层后，用户可以：")
    for conf in sorted(b_by_conf.keys()):
        s = b_by_conf[conf]
        if s['total'] > 0:
            acc = s['correct'] / s['total'] * 100
            logger.info("      只看 %-10s 预测: %.1f%% 准确率, %d个预测",
                        conf, acc, s['total'])

    logger.info("")
    logger.info("  ── 7. 结论 ──")
    if a_total > 0:
        # 过滤策略效果
        if c_filter['total'] > 0:
            filt_acc = c_filter['correct'] / c_filter['total'] * 100
            base_acc = a_correct / a_total * 100
            logger.info("    过滤策略(去矛盾): %+.2f%% 准确率 (%.1f%% → %.1f%%)",
                        filt_acc - base_acc, base_acc, filt_acc)
            logger.info("    过滤策略覆盖率:   %.1f%% → %.1f%%",
                        a_total/all_weeks*100, c_filter['total']/all_weeks*100)

        # 置信度分层价值
        if b_by_conf.get('high', {}).get('total', 0) > 0:
            high_acc = b_by_conf['high']['correct'] / b_by_conf['high']['total'] * 100
            logger.info("    高置信度预测准确率: %.1f%% (%d个)",
                        high_acc, b_by_conf['high']['total'])

        # 确认增强
        if vol_effect['confirm']['total'] > 0:
            conf_acc = vol_effect['confirm']['correct'] / vol_effect['confirm']['total'] * 100
            logger.info("    成交量确认后准确率: %.1f%% (%d个)",
                        conf_acc, vol_effect['confirm']['total'])

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)


if __name__ == '__main__':
    run_backtest(n_weeks=29, sample_limit=0)
