"""验证规则引擎下周预测的回测准确率。

使用与 _nw_signal_deep_analysis2.py 相同的数据加载方式，
但使用 service 中的规则引擎进行预测。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection
import logging
logger = logging.getLogger(__name__)

def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def _compound_return(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return (r - 1) * 100

def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0

def main():
    # Import the rule engine from service
    from service.weekly_prediction_service import _nw_extract_features, _nw_match_rule, _NW_RULES

    # Load data (same as _nw_signal_deep_analysis2.py)
    n_weeks = 29
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # Get latest trade date
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code = '000001.SH'")
    latest_date = cur.fetchone()['d']
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7)
    start_date = dt_start.strftime('%Y-%m-%d')

    # Get all stock codes
    cur.execute("SELECT DISTINCT stock_code FROM stock_kline "
                "WHERE `date` = %s AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ') "
                "AND stock_code NOT LIKE '399%%' AND stock_code != '000001.SH'", (latest_date,))
    all_codes = [r['stock_code'] for r in cur.fetchall()]

    # Sample for speed
    import random
    random.seed(42)
    sample_codes = random.sample(all_codes, min(2000, len(all_codes)))
    logger.info("验证 %d 只股票, 区间 %s ~ %s", len(sample_codes), start_date, latest_date)

    # Load klines
    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(sample_codes), batch_size):
        batch = sample_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, change_percent "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, latest_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'change_percent': _to_float(row['change_percent']),
            })

    # Market klines
    cur.execute(
        "SELECT `date`, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (start_date, latest_date))
    market_klines = [{'date': r['date'], 'change_percent': _to_float(r['change_percent'])} for r in cur.fetchall()]
    conn.close()

    market_by_week = defaultdict(list)
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        market_by_week[iw].append(k)

    # Run backtest
    total_correct = 0
    total_predicted = 0
    total_all = 0
    rule_stats = defaultdict(lambda: {'correct': 0, 'total': 0})

    for code in sample_codes:
        klines = stock_klines.get(code, [])
        if len(klines) < 20:
            continue

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())

        for idx in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[idx]
            iw_next = sorted_weeks[idx + 1]

            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])

            if len(this_days) < 3 or len(next_days) < 3:
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_chg = _compound_return(next_pcts)
            actual_next_up = next_chg >= 0

            mw = market_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            total_all += 1

            feat = _nw_extract_features(this_pcts, market_chg)
            rule = _nw_match_rule(feat)

            if rule is None:
                continue

            pred_up = rule['pred_up']
            correct = pred_up == actual_next_up
            total_predicted += 1
            if correct:
                total_correct += 1

            rule_stats[rule['name']]['total'] += 1
            if correct:
                rule_stats[rule['name']]['correct'] += 1

    coverage = total_predicted / total_all * 100 if total_all > 0 else 0
    accuracy = total_correct / total_predicted * 100 if total_predicted > 0 else 0

    print(f"\n{'='*70}")
    print(f"  规则引擎下周预测回测验证")
    print(f"{'='*70}")
    print(f"  样本: {len(sample_codes)} 只股票, {n_weeks} 周")
    print(f"  总周数: {total_all}")
    print(f"  预测周数: {total_predicted} (覆盖率: {coverage:.1f}%)")
    print(f"  正确: {total_correct}")
    print(f"  准确率: {accuracy:.1f}%")
    print(f"\n  各规则统计:")
    for name, stats in sorted(rule_stats.items(), key=lambda x: -x[1]['total']):
        acc = stats['correct'] / stats['total'] * 100 if stats['total'] > 0 else 0
        marker = ' ★★★' if acc >= 70 else (' ★★' if acc >= 65 else (' ★' if acc >= 60 else ''))
        print(f"    {name:35s}  N={stats['total']:5d}  准确率={acc:5.1f}%{marker}")
    print(f"{'='*70}")

    # Also test: what if we only keep rules with residual accuracy >= 65%?
    print(f"\n{'='*70}")
    print(f"  逐步添加规则（观察残余准确率）")
    print(f"{'='*70}")

    from service.weekly_prediction_service import _NW_RULES

    covered = set()
    cum_correct = 0
    cum_total = 0

    # Rebuild all records with features
    all_records = []
    for code in sample_codes:
        klines = stock_klines.get(code, [])
        if len(klines) < 20:
            continue
        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)
        sorted_weeks = sorted(wg.keys())
        for idx in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[idx]
            iw_next = sorted_weeks[idx + 1]
            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])
            if len(this_days) < 3 or len(next_days) < 3:
                continue
            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_chg = _compound_return(next_pcts)
            mw = market_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0
            feat = _nw_extract_features(this_pcts, market_chg)
            all_records.append({
                'feat': feat,
                'next_up': next_chg >= 0,
                'next_chg': next_chg,
            })

    print(f"  总记录: {len(all_records)}")

    for rule in _NW_RULES:
        matching = []
        for i, rec in enumerate(all_records):
            if i in covered:
                continue
            f = rec['feat']
            if rule['check'](f['this_week_chg'], f['market_chg'], f['consec_down'], f['consec_up'], f['last_day_chg']):
                matching.append((i, rec))

        if not matching:
            print(f"    {rule['name']:35s}  N=    0  (无新增)")
            continue

        ok = sum(1 for _, rec in matching if rule['pred_up'] == rec['next_up'])
        acc = ok / len(matching) * 100

        for i, _ in matching:
            covered.add(i)
        cum_correct += ok
        cum_total += len(matching)
        cum_acc = cum_correct / cum_total * 100

        marker = ' ★★★' if acc >= 70 else (' ★★' if acc >= 65 else (' ★' if acc >= 60 else ' ✗'))
        print(f"    {rule['name']:35s}  +{len(matching):5d}  残余准确率={acc:5.1f}%{marker}  累计={cum_acc:.1f}% ({cum_total})")

    coverage_pct = len(covered) / len(all_records) * 100
    print(f"\n  最终: 覆盖{len(covered)}/{len(all_records)} ({coverage_pct:.1f}%), 准确率={cum_correct}/{cum_total}={cum_correct/cum_total*100:.1f}%")


if __name__ == '__main__':
    main()
