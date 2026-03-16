"""测试组合规则方案"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from service.weekly_prediction_service import _compound_return, _to_float
from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection

def main():
    n_weeks = 29
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) as d FROM stock_kline WHERE stock_code = '000001.SH'")
    latest_date = cur.fetchone()['d']
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7)
    start_date = dt_start.strftime('%Y-%m-%d')

    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline "
        "WHERE date = %s AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' "
        "OR stock_code LIKE '3%%.SZ') AND stock_code NOT LIKE '399%%' "
        "AND stock_code != '000001.SH' LIMIT 500", (latest_date,))
    sample = [r['stock_code'] for r in cur.fetchall()]

    stock_klines = defaultdict(list)
    ph = ','.join(['%s'] * len(sample))
    cur.execute(
        f"SELECT stock_code, date, change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND date >= %s AND date <= %s ORDER BY date",
        sample + [start_date, latest_date])
    for row in cur.fetchall():
        stock_klines[row['stock_code']].append({
            'date': row['date'],
            'change_percent': _to_float(row['change_percent']),
        })

    cur.execute(
        "SELECT date, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND date >= %s AND date <= %s ORDER BY date",
        (start_date, latest_date))
    market_klines = cur.fetchall()
    conn.close()

    market_by_week = defaultdict(list)
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        market_by_week[dt.isocalendar()[:2]].append(k)

    records = []
    for code in sample:
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
            this_pcts = [_to_float(d['change_percent']) for d in this_days]
            next_pcts = [_to_float(d['change_percent']) for d in next_days]
            this_chg = _compound_return(this_pcts)
            next_chg = _compound_return(next_pcts)
            mw = market_by_week.get(iw_this, [])
            market_chg = _compound_return(
                [_to_float(k['change_percent']) for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0
            last_day = this_pcts[-1]
            cd, cu = 0, 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                elif p > 0:
                    cu += 1
                else:
                    break
                if cd > 0 and cu > 0:
                    break
            records.append({
                'this_chg': this_chg, 'next_up': next_chg >= 0,
                'market_chg': market_chg, 'last_day': last_day,
                'consec_down': cd, 'consec_up': cu,
            })

    print(f"总记录: {len(records)}")

    # 测试组合方案（互斥匹配）
    rules_list = [
        ('跌>2%+大盘跌>1%→涨', lambda r: r['this_chg'] < -2 and r['market_chg'] < -1, True),
        ('跌>5%+尾日跌>2%→涨', lambda r: r['this_chg'] < -5 and r['last_day'] < -2, True),
        ('跌>4%+连跌>=3天+尾日跌>2%→涨', lambda r: r['this_chg'] < -4 and r['consec_down'] >= 3 and r['last_day'] < -2, True),
        ('跌>3%+连跌>=4天+尾日跌>2%→涨', lambda r: r['this_chg'] < -3 and r['consec_down'] >= 4 and r['last_day'] < -2, True),
        ('连跌>=4天+尾日跌>3%→涨', lambda r: r['consec_down'] >= 4 and r['last_day'] < -3, True),
    ]

    covered = set()
    cum_ok = 0
    cum_n = 0
    for name, cond, pred_up in rules_list:
        matching = [(i, r) for i, r in enumerate(records) if cond(r) and i not in covered]
        if not matching:
            continue
        ok = sum(1 for _, r in matching if pred_up == r['next_up'])
        acc = ok / len(matching) * 100
        for i, _ in matching:
            covered.add(i)
        cum_ok += ok
        cum_n += len(matching)
        cum_acc = cum_ok / cum_n * 100
        marker = ' ★★★' if acc >= 70 else (' ★★' if acc >= 65 else (' ★' if acc >= 60 else ' ✗'))
        print(f"  {name:45s}  +{len(matching):5d}  残余={acc:5.1f}%{marker}  累计={cum_acc:.1f}% (N={cum_n})")

    total_acc = cum_ok / cum_n * 100 if cum_n > 0 else 0
    total_cov = cum_n / len(records) * 100 if records else 0
    print(f"\n  总计: N={cum_n}  准确率={total_acc:.1f}%  覆盖={total_cov:.1f}%")

    # 检查大盘不跌周中的命中情况
    mkt_up_idx = set(i for i, r in enumerate(records) if r['market_chg'] >= 0)
    mkt_mild_idx = set(i for i, r in enumerate(records) if -1 <= r['market_chg'] < 0)
    covered_up = covered & mkt_up_idx
    covered_mild = covered & mkt_mild_idx
    covered_deep = covered - mkt_up_idx - mkt_mild_idx
    print(f"  大盘涨周命中: {len(covered_up)}, 轻跌周命中: {len(covered_mild)}, 深跌周命中: {len(covered_deep)}")

    # 各环境下的准确率
    for label, idx_set in [("大盘涨周", covered_up), ("大盘轻跌周", covered_mild), ("大盘深跌周", covered_deep)]:
        if not idx_set:
            continue
        ok = sum(1 for i in idx_set if records[i]['next_up'])  # 所有规则都是pred_up=True
        print(f"  {label}: N={len(idx_set)}  准确率={ok/len(idx_set)*100:.1f}%")


if __name__ == '__main__':
    main()
