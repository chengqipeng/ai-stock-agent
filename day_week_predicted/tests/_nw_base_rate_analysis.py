"""分析各大盘环境下的基础涨跌概率 + 寻找可利用的偏差"""
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
        "AND stock_code != '000001.SH' LIMIT 2000", (latest_date,))
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

    # 计算每周大盘涨跌幅
    market_weekly = {}
    for iw, klines in market_by_week.items():
        if len(klines) >= 3:
            pcts = [_to_float(k['change_percent']) for k in sorted(klines, key=lambda x: x['date'])]
            market_weekly[iw] = _compound_return(pcts)

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
            mkt_this = market_weekly.get(iw_this, 0.0)
            mkt_next = market_weekly.get(iw_next, 0.0)
            last_day = this_pcts[-1]
            cd, cu = 0, 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else:
                    break
            records.append({
                'this_chg': this_chg, 'next_chg': next_chg,
                'next_up': next_chg >= 0,
                'market_chg': mkt_this, 'market_next': mkt_next,
                'last_day': last_day,
                'consec_down': cd, 'consec_up': cu,
            })

    print(f"总记录: {len(records)}")

    # 基础涨跌概率
    print("\n=== 基础涨跌概率 ===")
    for label, subset in [
        ("全部", records),
        ("大盘涨周(>=0)", [r for r in records if r['market_chg'] >= 0]),
        ("大盘涨>1%", [r for r in records if r['market_chg'] > 1]),
        ("大盘涨>2%", [r for r in records if r['market_chg'] > 2]),
        ("大盘轻跌(-1%~0)", [r for r in records if -1 <= r['market_chg'] < 0]),
        ("大盘深跌(<-1%)", [r for r in records if r['market_chg'] < -1]),
        ("大盘深跌(<-2%)", [r for r in records if r['market_chg'] < -2]),
    ]:
        if not subset:
            continue
        up_rate = sum(1 for r in subset if r['next_up']) / len(subset) * 100
        print(f"  {label:<25s}  N={len(subset):>6d}  下周涨概率={up_rate:.1f}%")

    # 下周大盘方向对个股的影响
    print("\n=== 下周大盘方向对个股涨跌的影响 ===")
    for label, subset in [
        ("下周大盘涨(>=0)", [r for r in records if r['market_next'] >= 0]),
        ("下周大盘涨>1%", [r for r in records if r['market_next'] > 1]),
        ("下周大盘跌(<0)", [r for r in records if r['market_next'] < 0]),
        ("下周大盘跌<-1%", [r for r in records if r['market_next'] < -1]),
    ]:
        if not subset:
            continue
        up_rate = sum(1 for r in subset if r['next_up']) / len(subset) * 100
        print(f"  {label:<25s}  N={len(subset):>6d}  个股涨概率={up_rate:.1f}%")

    # 本周大盘涨 → 下周大盘也涨的概率（动量效应）
    print("\n=== 大盘周动量效应 ===")
    weeks_sorted = sorted(market_weekly.keys())
    for i in range(len(weeks_sorted) - 1):
        pass  # 简化
    mkt_momentum = []
    for i in range(len(weeks_sorted) - 1):
        this_w = weeks_sorted[i]
        next_w = weeks_sorted[i + 1]
        mkt_momentum.append({
            'this': market_weekly[this_w],
            'next': market_weekly[next_w],
        })
    for label, subset in [
        ("本周大盘涨→下周大盘涨", [m for m in mkt_momentum if m['this'] >= 0]),
        ("本周大盘涨>1%→下周大盘涨", [m for m in mkt_momentum if m['this'] > 1]),
        ("本周大盘跌→下周大盘涨", [m for m in mkt_momentum if m['this'] < 0]),
        ("本周大盘跌>1%→下周大盘涨", [m for m in mkt_momentum if m['this'] < -1]),
    ]:
        if not subset:
            continue
        up_rate = sum(1 for m in subset if m['next'] >= 0) / len(subset) * 100
        print(f"  {label:<35s}  N={len(subset):>4d}  概率={up_rate:.1f}%")

    # 关键测试：在非深跌周，如果我们预测所有股票"涨"，准确率是多少？
    print("\n=== 非深跌周全部预测涨的准确率 ===")
    non_deep = [r for r in records if r['market_chg'] >= -1]
    up_rate = sum(1 for r in non_deep if r['next_up']) / len(non_deep) * 100
    print(f"  全部预测涨: {up_rate:.1f}% (N={len(non_deep)})")

    # 在非深跌周，加条件后预测涨
    print("\n=== 非深跌周条件预测涨 ===")
    conditions = [
        ('本周跌>2%', lambda r: r['this_chg'] < -2),
        ('本周跌>3%', lambda r: r['this_chg'] < -3),
        ('本周跌>5%', lambda r: r['this_chg'] < -5),
        ('本周涨>0%', lambda r: r['this_chg'] > 0),
        ('本周涨>2%', lambda r: r['this_chg'] > 2),
        ('本周涨>5%', lambda r: r['this_chg'] > 5),
        ('本周涨>0%+大盘涨>0%', lambda r: r['this_chg'] > 0 and r['market_chg'] > 0),
        ('本周涨>2%+大盘涨>1%', lambda r: r['this_chg'] > 2 and r['market_chg'] > 1),
        ('本周涨>3%+大盘涨>1%', lambda r: r['this_chg'] > 3 and r['market_chg'] > 1),
        ('本周涨>5%+大盘涨>1%', lambda r: r['this_chg'] > 5 and r['market_chg'] > 1),
        ('连涨>=3天', lambda r: r['consec_up'] >= 3),
        ('连涨>=4天', lambda r: r['consec_up'] >= 4),
        ('连涨>=3天+涨>2%', lambda r: r['consec_up'] >= 3 and r['this_chg'] > 2),
        ('连涨>=3天+涨>3%+大盘涨>0%', lambda r: r['consec_up'] >= 3 and r['this_chg'] > 3 and r['market_chg'] > 0),
        ('尾日涨>2%', lambda r: r['last_day'] > 2),
        ('尾日涨>3%', lambda r: r['last_day'] > 3),
        ('尾日涨>2%+涨>2%', lambda r: r['last_day'] > 2 and r['this_chg'] > 2),
        ('尾日涨>3%+涨>3%', lambda r: r['last_day'] > 3 and r['this_chg'] > 3),
        # 跌后反弹 - 预测跌
        ('本周跌>2%→跌', lambda r: r['this_chg'] < -2),
        ('本周跌>3%→跌', lambda r: r['this_chg'] < -3),
        ('本周跌>5%→跌', lambda r: r['this_chg'] < -5),
    ]

    for name, cond in conditions:
        pred_up = '→跌' not in name
        subset = [r for r in non_deep if cond(r)]
        if not subset:
            continue
        if pred_up:
            ok = sum(1 for r in subset if r['next_up'])
        else:
            ok = sum(1 for r in subset if not r['next_up'])
        acc = ok / len(subset) * 100
        marker = ' ★★★' if acc >= 65 else (' ★★' if acc >= 60 else (' ★' if acc >= 55 else ''))
        print(f"  {name:<40s}  N={len(subset):>6d}  准确率={acc:.1f}%{marker}")


if __name__ == '__main__':
    main()
