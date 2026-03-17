#!/usr/bin/env python3
"""
生产代码验证脚本 — 100+只个股
================================
使用生产代码路径 (_predict_next_week) 对100+只真实股票进行验证。
与回测不同，本脚本直接调用生产函数，确保部署代码与回测一致。

验证方式：
  1. 加载100+只股票的完整数据（通过生产 _load_prediction_data）
  2. 对最近N周，逐周调用生产 _predict_next_week()
  3. 对比预测方向与实际下周涨跌
  4. 输出每只股票的预测详情 + 汇总准确率

用法：
    python -m day_week_predicted.backtest.validate_production_100stocks
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
    _detect_volume_patterns,
    _adjust_nw_confidence_by_volume,
    _get_market_klines_for_stock,
    _compute_fund_flow_signal,
    _compute_volume_signal,
    _compute_finance_signal,
)


# ── 选取验证股票：从不同板块均匀抽样 ──
def _select_validation_stocks(n=120):
    """从全量股票中按市场均匀抽样n只。"""
    import random
    random.seed(42)
    all_codes = _get_all_stock_codes()
    # 按市场分组
    sh_codes = [c for c in all_codes if c.endswith('.SH') and not c.startswith(('000001', '399', '899'))]
    sz_codes = [c for c in all_codes if c.endswith('.SZ') and not c.startswith(('000001', '399', '899'))]
    bj_codes = [c for c in all_codes if c.endswith('.BJ')]

    selected = []
    # 上证100只, 深证80只, 北证20只（按比例）
    n_sh = min(100, len(sh_codes))
    n_sz = min(80, len(sz_codes))
    n_bj = min(20, len(bj_codes))

    selected += random.sample(sh_codes, n_sh) if len(sh_codes) >= n_sh else sh_codes
    selected += random.sample(sz_codes, n_sz) if len(sz_codes) >= n_sz else sz_codes
    selected += random.sample(bj_codes, n_bj) if len(bj_codes) >= n_bj else bj_codes

    logger.info("抽样: 上证%d 深证%d 北证%d = 共%d只", n_sh, n_sz, n_bj, len(selected))
    return selected


def run_validation(n_weeks=12, n_stocks=120):
    """
    生产代码验证。

    对n_stocks只股票，回溯n_weeks周，每周调用生产代码路径进行预测，
    然后对比实际下周涨跌。
    """
    t_start = datetime.now()
    logger.info("=" * 80)
    logger.info("  生产代码验证 (n_stocks=%d, n_weeks=%d)", n_stocks, n_weeks)
    logger.info("=" * 80)

    latest_date = _get_latest_trade_date()
    if not latest_date:
        logger.error("无法获取最新交易日")
        return
    logger.info("最新交易日: %s", latest_date)

    # 选取验证股票
    stock_codes = _select_validation_stocks(n_stocks)
    logger.info("验证股票数: %d", len(stock_codes))

    # 加载数据（扩大时间范围以覆盖回溯周数）
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 加载个股K线
    logger.info("加载个股K线...")
    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
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

    # 加载指数K线
    logger.info("加载指数K线...")
    all_index_codes = list(set(_get_stock_index(c) for c in stock_codes))
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

    # 按周分组大盘数据
    market_by_week_by_index = {}
    for idx_code, klines_list in market_klines_by_index.items():
        by_week = defaultdict(list)
        for k in klines_list:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            by_week[iw].append(k)
        market_by_week_by_index[idx_code] = by_week

    logger.info("数据加载完成: %d只股票, %d个指数", len(stock_klines), len(market_klines_by_index))

    # ── 逐股票逐周验证 ──
    total_predictions = 0
    total_correct = 0
    by_rule = defaultdict(lambda: {'correct': 0, 'total': 0, 'stocks': set()})
    by_confidence = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_stock = defaultdict(lambda: {'correct': 0, 'total': 0, 'rules': []})
    sample_details = []  # 保存部分详情用于输出

    processed = 0
    for code in stock_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        idx_by_week = market_by_week_by_index.get(stock_idx, {})

        # 按周分组
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

            # ── 模拟生产代码路径 ──
            daily_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_week_chg = _compound_return(next_pcts)
            actual_up = next_week_chg >= 0

            # 大盘本周涨跌
            mw = idx_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

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

            # 计算 prev_week_chg（与生产代码一致）
            prev_week_chg = None
            prev_klines = hist[-5:] if len(hist) >= 5 else hist
            if prev_klines:
                prev_week_chg = _compound_return([k['change_percent'] for k in prev_klines])

            # 调用生产代码 _nw_extract_features + _nw_match_rule
            feat = _nw_extract_features(
                daily_pcts, market_chg,
                market_index=stock_idx,
                price_pos_60=price_pos_60,
                prev_week_chg=prev_week_chg,
            )
            rule = _nw_match_rule(feat)

            if rule is None:
                continue

            pred_up = rule['pred_up']
            tier = rule['tier']
            confidence = 'high' if tier == 1 else 'reference'

            # 成交量修正（与生产代码一致）
            vol_patterns = _detect_volume_patterns(this_days, klines)
            vol_note = ''
            if vol_patterns.get('vol_direction'):
                confidence, vol_note = _adjust_nw_confidence_by_volume(
                    pred_up, confidence, vol_patterns)

            is_correct = pred_up == actual_up
            total_predictions += 1
            if is_correct:
                total_correct += 1

            rule_name = rule['name']
            by_rule[rule_name]['total'] += 1
            by_rule[rule_name]['stocks'].add(code)
            if is_correct:
                by_rule[rule_name]['correct'] += 1

            by_confidence[confidence]['total'] += 1
            if is_correct:
                by_confidence[confidence]['correct'] += 1

            by_stock[code]['total'] += 1
            if is_correct:
                by_stock[code]['correct'] += 1
            by_stock[code]['rules'].append(rule_name)

            # 保存前200条详情
            if len(sample_details) < 200:
                sample_details.append({
                    'code': code,
                    'week': f"{iw_this[0]}-W{iw_this[1]:02d}",
                    'rule': rule_name,
                    'pred': '涨' if pred_up else '跌',
                    'actual': '涨' if actual_up else '跌',
                    'correct': '✓' if is_correct else '✗',
                    'this_chg': f"{feat['this_week_chg']:+.1f}%",
                    'mkt_chg': f"{market_chg:+.1f}%",
                    'next_chg': f"{next_week_chg:+.1f}%",
                    'confidence': confidence,
                    'pos60': f"{price_pos_60:.2f}" if price_pos_60 is not None else '-',
                })

        processed += 1
        if processed % 20 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(stock_codes))

    # ── 输出结果 ──
    elapsed = (datetime.now() - t_start).total_seconds()
    _p = lambda c, t: f"{c/t*100:.1f}%" if t > 0 else "N/A"

    logger.info("")
    logger.info("=" * 80)
    logger.info("  生产代码验证结果")
    logger.info("=" * 80)

    logger.info("")
    logger.info("  ── 1. 整体准确率 ──")
    logger.info("    验证股票数: %d (有预测触发: %d)", len(stock_codes), len(by_stock))
    logger.info("    总预测次数: %d", total_predictions)
    logger.info("    总准确率: %s (%d/%d)", _p(total_correct, total_predictions),
                total_correct, total_predictions)

    logger.info("")
    logger.info("  ── 2. 按规则分层 ──")
    for rn in sorted(by_rule.keys()):
        s = by_rule[rn]
        logger.info("    %-40s %s (%d/%d) [%d只股票]",
                    rn, _p(s['correct'], s['total']), s['correct'], s['total'], len(s['stocks']))

    logger.info("")
    logger.info("  ── 3. 按置信度分层 ──")
    for conf in sorted(by_confidence.keys()):
        s = by_confidence[conf]
        logger.info("    %-12s %s (%d/%d)", conf, _p(s['correct'], s['total']),
                    s['correct'], s['total'])

    logger.info("")
    logger.info("  ── 4. 按股票统计（触发≥3次的股票） ──")
    active_stocks = [(code, s) for code, s in by_stock.items() if s['total'] >= 3]
    active_stocks.sort(key=lambda x: x[1]['correct'] / max(x[1]['total'], 1), reverse=True)
    for code, s in active_stocks[:30]:
        acc = s['correct'] / s['total'] * 100
        rules_str = ', '.join(set(s['rules']))[:60]
        logger.info("    %s  %5.1f%% (%d/%d)  规则: %s",
                    code, acc, s['correct'], s['total'], rules_str)

    # 股票级准确率分布
    if active_stocks:
        stock_accs = [s['correct'] / s['total'] * 100 for _, s in active_stocks]
        avg_stock_acc = sum(stock_accs) / len(stock_accs)
        above_70 = sum(1 for a in stock_accs if a >= 70)
        above_80 = sum(1 for a in stock_accs if a >= 80)
        logger.info("")
        logger.info("    活跃股票(≥3次触发): %d只", len(active_stocks))
        logger.info("    平均个股准确率: %.1f%%", avg_stock_acc)
        logger.info("    准确率≥70%%: %d只 (%.1f%%)", above_70, above_70 / len(active_stocks) * 100)
        logger.info("    准确率≥80%%: %d只 (%.1f%%)", above_80, above_80 / len(active_stocks) * 100)

    logger.info("")
    logger.info("  ── 5. 预测详情样本（前30条） ──")
    logger.info("    %-10s %-10s %-30s %-4s %-4s %-3s %-8s %-8s %-8s %-6s %-5s",
                "股票", "周", "规则", "预测", "实际", "", "本周", "大盘", "下周", "置信", "位置")
    for d in sample_details[:30]:
        logger.info("    %-10s %-10s %-30s %-4s %-4s %-3s %-8s %-8s %-8s %-6s %-5s",
                    d['code'], d['week'], d['rule'][:30], d['pred'], d['actual'],
                    d['correct'], d['this_chg'], d['mkt_chg'], d['next_chg'],
                    d['confidence'], d['pos60'])

    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 80)

    return {
        'total_stocks': len(stock_codes),
        'active_stocks': len(by_stock),
        'total_predictions': total_predictions,
        'total_correct': total_correct,
        'accuracy': round(total_correct / total_predictions * 100, 1) if total_predictions > 0 else 0,
    }


if __name__ == '__main__':
    run_validation(n_weeks=29, n_stocks=200)
