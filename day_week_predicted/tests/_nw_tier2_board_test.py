"""用361板块样本测试不同Tier2阈值"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from day_week_predicted.tests._nw_rule_engine_validate import select_stocks_from_boards, _to_float, _compound_return
from service.weekly_prediction_service import _nw_extract_features
from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection

def main():
    sample_codes, board_count, latest_date = select_stocks_from_boards(min_boards=200)
    n_weeks = 29
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7)
    start_date = dt_start.strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(sample_codes), batch_size):
        batch = sample_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, change_percent FROM stock_kline "
            f"WHERE stock_code IN ({ph}) AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, latest_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'], 'change_percent': _to_float(row['change_percent'])})

    cur.execute(
        "SELECT `date`, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (start_date, latest_date))
    market_klines = [{'date': r['date'], 'change_percent': _to_float(r['change_percent'])} for r in cur.fetchall()]
    conn.close()

    market_by_week = defaultdict(list)
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        market_by_week[dt.isocalendar()[:2]].append(k)

    records = []
    for code in sample_codes:
        klines = stock_klines.get(code, [])
        if len(klines) < 20:
            continue
        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            wg[dt.isocalendar()[:2]].append(k)
        sorted_weeks = sorted(wg.keys())
        for idx in range(len(sorted_weeks) - 1):
            iw_this, iw_next = sorted_weeks[idx], sorted_weeks[idx + 1]
            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])
            if len(this_days) < 3 or len(next_days) < 3:
                continue
            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            this_chg = _compound_return(this_pcts)
            next_chg = _compound_return(next_pcts)
            mw = market_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0
            records.append({
                'this_chg': this_chg, 'next_up': next_chg >= 0, 'market_chg': market_chg,
            })

    print(f"板块数: {board_count}, 股票数: {len(sample_codes)}, 记录数: {len(records)}")

    t1_cond = lambda r: r['this_chg'] < -2 and r['market_chg'] < -1

    print("\n单Tier2阈值 (361板块样本):")
    for thresh in [3, 4, 5, 6, 7, 8, 9, 10]:
        covered = set()
        t1_ok = t1_n = 0
        t2_ok = t2_n = 0

        for i, r in enumerate(records):
            if t1_cond(r):
                covered.add(i)
                t1_n += 1
                if r['next_up']: t1_ok += 1

        for i, r in enumerate(records):
            if i not in covered and r['this_chg'] < -thresh:
                covered.add(i)
                t2_n += 1
                if not r['next_up']: t2_ok += 1

        total_ok = t1_ok + t2_ok
        total_n = t1_n + t2_n
        total_acc = total_ok / total_n * 100 if total_n > 0 else 0
        total_cov = total_n / len(records) * 100
        t1_acc = t1_ok / t1_n * 100 if t1_n > 0 else 0
        t2_acc = t2_ok / t2_n * 100 if t2_n > 0 else 0

        marker = ' ★★★' if total_acc >= 70 else (' ★★' if total_acc >= 68 else (' ★' if total_acc >= 65 else ''))
        print(f"  T1 + T2(跌>{thresh}%→跌): T1={t1_acc:.1f}%(N={t1_n})  T2={t2_acc:.1f}%(N={t2_n})  总={total_acc:.1f}% 覆盖={total_cov:.1f}%{marker}")

    # T1 only
    t1_ok = sum(1 for r in records if t1_cond(r) and r['next_up'])
    t1_n = sum(1 for r in records if t1_cond(r))
    print(f"\n  T1 only: {t1_ok/t1_n*100:.1f}% (N={t1_n}, 覆盖={t1_n/len(records)*100:.1f}%)")


if __name__ == '__main__':
    main()
