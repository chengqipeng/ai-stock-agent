"""分析不依赖大盘的Tier2规则候选 - 专注非大盘深跌周的准确率"""
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

    # 获取样本股票 - 多取一些
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
            # 连跌/连涨
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
            # 周内最大单日跌幅
            max_drop = min(this_pcts)
            # 周内波动率
            vol = sum(abs(p) for p in this_pcts) / len(this_pcts)
            # 前半周 vs 后半周
            mid = len(this_pcts) // 2
            first_half = _compound_return(this_pcts[:mid]) if mid > 0 else 0
            second_half = _compound_return(this_pcts[mid:]) if mid < len(this_pcts) else 0

            records.append({
                'this_chg': this_chg, 'next_chg': next_chg,
                'next_up': next_chg >= 0,
                'market_chg': market_chg, 'last_day': last_day,
                'consec_down': cd, 'consec_up': cu,
                'max_drop': max_drop, 'vol': vol,
                'first_half': first_half, 'second_half': second_half,
                'n_days': len(this_pcts),
            })

    print(f"总记录: {len(records)}")
    # 按大盘分类
    deep_drop = [r for r in records if r['market_chg'] < -1]
    mild_drop = [r for r in records if -1 <= r['market_chg'] < 0]
    mkt_up = [r for r in records if r['market_chg'] >= 0]
    non_deep = [r for r in records if r['market_chg'] >= -1]
    print(f"大盘深跌(<-1%): {len(deep_drop)}, 轻跌(-1%~0): {len(mild_drop)}, 涨(>=0): {len(mkt_up)}")
    print(f"非深跌(>=-1%): {len(non_deep)}")
    print()

    # ============================================================
    # 测试各种条件在【非大盘深跌周】的准确率
    # ============================================================
    print("=" * 80)
    print("【非大盘深跌周】条件测试 (market_chg >= -1%)")
    print("=" * 80)

    conditions = [
        # 大跌反弹类
        ('跌>3%→涨', lambda r: r['this_chg'] < -3, True),
        ('跌>4%→涨', lambda r: r['this_chg'] < -4, True),
        ('跌>5%→涨', lambda r: r['this_chg'] < -5, True),
        ('跌>6%→涨', lambda r: r['this_chg'] < -6, True),
        ('跌>7%→涨', lambda r: r['this_chg'] < -7, True),
        ('跌>8%→涨', lambda r: r['this_chg'] < -8, True),
        ('跌>10%→涨', lambda r: r['this_chg'] < -10, True),

        # 连跌类
        ('连跌>=3天→涨', lambda r: r['consec_down'] >= 3, True),
        ('连跌>=4天→涨', lambda r: r['consec_down'] >= 4, True),
        ('连跌>=5天→涨', lambda r: r['consec_down'] >= 5, True),

        # 尾日大跌类
        ('尾日跌>2%→涨', lambda r: r['last_day'] < -2, True),
        ('尾日跌>3%→涨', lambda r: r['last_day'] < -3, True),
        ('尾日跌>4%→涨', lambda r: r['last_day'] < -4, True),

        # 组合条件
        ('跌>3%+尾日跌>2%→涨', lambda r: r['this_chg'] < -3 and r['last_day'] < -2, True),
        ('跌>4%+尾日跌>2%→涨', lambda r: r['this_chg'] < -4 and r['last_day'] < -2, True),
        ('跌>5%+尾日跌>2%→涨', lambda r: r['this_chg'] < -5 and r['last_day'] < -2, True),
        ('跌>5%+尾日跌>3%→涨', lambda r: r['this_chg'] < -5 and r['last_day'] < -3, True),
        ('跌>6%+尾日跌>2%→涨', lambda r: r['this_chg'] < -6 and r['last_day'] < -2, True),
        ('跌>7%+尾日跌>2%→涨', lambda r: r['this_chg'] < -7 and r['last_day'] < -2, True),

        # 连跌+跌幅组合
        ('连跌>=3天+跌>3%→涨', lambda r: r['consec_down'] >= 3 and r['this_chg'] < -3, True),
        ('连跌>=3天+跌>4%→涨', lambda r: r['consec_down'] >= 3 and r['this_chg'] < -4, True),
        ('连跌>=3天+跌>5%→涨', lambda r: r['consec_down'] >= 3 and r['this_chg'] < -5, True),
        ('连跌>=4天+跌>3%→涨', lambda r: r['consec_down'] >= 4 and r['this_chg'] < -3, True),
        ('连跌>=4天+跌>4%→涨', lambda r: r['consec_down'] >= 4 and r['this_chg'] < -4, True),

        # 连跌+尾日组合
        ('连跌>=3天+尾日跌>2%→涨', lambda r: r['consec_down'] >= 3 and r['last_day'] < -2, True),
        ('连跌>=3天+尾日跌>3%→涨', lambda r: r['consec_down'] >= 3 and r['last_day'] < -3, True),
        ('连跌>=4天+尾日跌>2%→涨', lambda r: r['consec_down'] >= 4 and r['last_day'] < -2, True),
        ('连跌>=4天+尾日跌>3%→涨', lambda r: r['consec_down'] >= 4 and r['last_day'] < -3, True),

        # 三重组合
        ('跌>4%+连跌>=3天+尾日跌>1%→涨', lambda r: r['this_chg'] < -4 and r['consec_down'] >= 3 and r['last_day'] < -1, True),
        ('跌>5%+连跌>=3天+尾日跌>1%→涨', lambda r: r['this_chg'] < -5 and r['consec_down'] >= 3 and r['last_day'] < -1, True),
        ('跌>3%+连跌>=3天+尾日跌>2%→涨', lambda r: r['this_chg'] < -3 and r['consec_down'] >= 3 and r['last_day'] < -2, True),
        ('跌>4%+连跌>=3天+尾日跌>2%→涨', lambda r: r['this_chg'] < -4 and r['consec_down'] >= 3 and r['last_day'] < -2, True),

        # 大盘轻跌+个股跌
        ('大盘跌>0.5%+跌>2%→涨', lambda r: r['market_chg'] < -0.5 and r['this_chg'] < -2, True),
        ('大盘跌>0.5%+跌>3%→涨', lambda r: r['market_chg'] < -0.5 and r['this_chg'] < -3, True),

        # 涨势延续类
        ('涨>3%→涨', lambda r: r['this_chg'] > 3, True),
        ('涨>5%→涨', lambda r: r['this_chg'] > 5, True),
        ('涨>8%→涨', lambda r: r['this_chg'] > 8, True),
        ('涨>10%→涨', lambda r: r['this_chg'] > 10, True),
        ('连涨>=3天+涨>3%→涨', lambda r: r['consec_up'] >= 3 and r['this_chg'] > 3, True),
        ('连涨>=4天+涨>3%→涨', lambda r: r['consec_up'] >= 4 and r['this_chg'] > 3, True),

        # 涨后回调类
        ('涨>5%→跌', lambda r: r['this_chg'] > 5, False),
        ('涨>8%→跌', lambda r: r['this_chg'] > 8, False),
        ('涨>10%→跌', lambda r: r['this_chg'] > 10, False),

        # 最大单日跌幅
        ('最大单日跌>5%→涨', lambda r: r['max_drop'] < -5, True),
        ('最大单日跌>7%→涨', lambda r: r['max_drop'] < -7, True),
        ('最大单日跌>9%→涨', lambda r: r['max_drop'] < -9, True),

        # 高波动
        ('日均波动>3%+跌>3%→涨', lambda r: r['vol'] > 3 and r['this_chg'] < -3, True),
        ('日均波动>4%+跌>3%→涨', lambda r: r['vol'] > 4 and r['this_chg'] < -3, True),

        # 前半周涨后半周跌（反转信号）
        ('前半涨>2%+后半跌>3%→涨', lambda r: r['first_half'] > 2 and r['second_half'] < -3, True),
        ('前半跌>3%+后半涨>2%→涨', lambda r: r['first_half'] < -3 and r['second_half'] > 2, True),
    ]

    print(f"\n{'条件':<45s} {'全局N':>7s} {'全局准确率':>10s} {'非深跌N':>8s} {'非深跌准确率':>12s} {'涨周N':>7s} {'涨周准确率':>10s}")
    print("-" * 110)

    viable = []
    for name, cond, pred_up in conditions:
        # 全局
        g_match = [r for r in records if cond(r)]
        g_ok = sum(1 for r in g_match if pred_up == r['next_up']) if g_match else 0
        g_acc = g_ok / len(g_match) * 100 if g_match else 0

        # 非深跌
        nd_match = [r for r in non_deep if cond(r)]
        nd_ok = sum(1 for r in nd_match if pred_up == r['next_up']) if nd_match else 0
        nd_acc = nd_ok / len(nd_match) * 100 if nd_match else 0

        # 涨周
        up_match = [r for r in mkt_up if cond(r)]
        up_ok = sum(1 for r in up_match if pred_up == r['next_up']) if up_match else 0
        up_acc = up_ok / len(up_match) * 100 if up_match else 0

        marker = ''
        if nd_acc >= 65 and len(nd_match) >= 50:
            marker = ' ★★★'
            viable.append((name, cond, pred_up, nd_acc, len(nd_match), up_acc, len(up_match)))
        elif nd_acc >= 60 and len(nd_match) >= 50:
            marker = ' ★★'
            viable.append((name, cond, pred_up, nd_acc, len(nd_match), up_acc, len(up_match)))
        elif nd_acc >= 55 and len(nd_match) >= 100:
            marker = ' ★'

        print(f"{name:<45s} {len(g_match):>7d} {g_acc:>9.1f}% {len(nd_match):>8d} {nd_acc:>11.1f}% {len(up_match):>7d} {up_acc:>9.1f}%{marker}")

    print("\n" + "=" * 80)
    print("可行候选 (非深跌周准确率>=60%, N>=50):")
    print("=" * 80)
    for name, _, _, nd_acc, nd_n, up_acc, up_n in sorted(viable, key=lambda x: -x[3]):
        print(f"  {name:<45s}  非深跌: {nd_acc:.1f}% (N={nd_n})  涨周: {up_acc:.1f}% (N={up_n})")

    # ============================================================
    # 测试互斥组合方案
    # ============================================================
    print("\n" + "=" * 80)
    print("互斥组合方案测试")
    print("=" * 80)

    # 方案1: Tier1(大盘深跌) + Tier2(极端跌幅，不要求大盘)
    combos = [
        ("方案A: T1+跌>8%→涨", [
            ('T1:跌>2%+大盘跌>1%→涨', lambda r: r['this_chg'] < -2 and r['market_chg'] < -1, True),
            ('T2:跌>8%→涨', lambda r: r['this_chg'] < -8, True),
        ]),
        ("方案B: T1+跌>7%+尾日跌>2%→涨", [
            ('T1:跌>2%+大盘跌>1%→涨', lambda r: r['this_chg'] < -2 and r['market_chg'] < -1, True),
            ('T2:跌>7%+尾日跌>2%→涨', lambda r: r['this_chg'] < -7 and r['last_day'] < -2, True),
        ]),
        ("方案C: T1+跌>6%+尾日跌>2%→涨", [
            ('T1:跌>2%+大盘跌>1%→涨', lambda r: r['this_chg'] < -2 and r['market_chg'] < -1, True),
            ('T2:跌>6%+尾日跌>2%→涨', lambda r: r['this_chg'] < -6 and r['last_day'] < -2, True),
        ]),
        ("方案D: T1+跌>10%→涨", [
            ('T1:跌>2%+大盘跌>1%→涨', lambda r: r['this_chg'] < -2 and r['market_chg'] < -1, True),
            ('T2:跌>10%→涨', lambda r: r['this_chg'] < -10, True),
        ]),
        ("方案E: T1+最大单日跌>9%→涨", [
            ('T1:跌>2%+大盘跌>1%→涨', lambda r: r['this_chg'] < -2 and r['market_chg'] < -1, True),
            ('T2:最大单日跌>9%→涨', lambda r: r['max_drop'] < -9, True),
        ]),
        ("方案F: T1+连跌>=4天+跌>4%→涨", [
            ('T1:跌>2%+大盘跌>1%→涨', lambda r: r['this_chg'] < -2 and r['market_chg'] < -1, True),
            ('T2:连跌>=4天+跌>4%→涨', lambda r: r['consec_down'] >= 4 and r['this_chg'] < -4, True),
        ]),
        ("方案G: T1+跌>5%+连跌>=3天+尾日跌>1%→涨", [
            ('T1:跌>2%+大盘跌>1%→涨', lambda r: r['this_chg'] < -2 and r['market_chg'] < -1, True),
            ('T2:跌>5%+连跌>=3+尾日跌>1%→涨', lambda r: r['this_chg'] < -5 and r['consec_down'] >= 3 and r['last_day'] < -1, True),
        ]),
    ]

    for combo_name, rules in combos:
        print(f"\n{combo_name}:")
        covered = set()
        cum_ok = 0
        cum_n = 0
        for name, cond, pred_up in rules:
            matching = [(i, r) for i, r in enumerate(records) if cond(r) and i not in covered]
            if not matching:
                print(f"  {name:<50s}  无新增匹配")
                continue
            ok = sum(1 for _, r in matching if pred_up == r['next_up'])
            acc = ok / len(matching) * 100
            for i, _ in matching:
                covered.add(i)
            cum_ok += ok
            cum_n += len(matching)
            cum_acc = cum_ok / cum_n * 100
            # 非深跌周的残余准确率
            nd_match = [(i, r) for i, r in matching if r['market_chg'] >= -1]
            nd_ok = sum(1 for _, r in nd_match if pred_up == r['next_up'])
            nd_acc = nd_ok / len(nd_match) * 100 if nd_match else 0
            print(f"  {name:<50s}  +{len(matching):5d}  残余={acc:5.1f}%  非深跌残余={nd_acc:.1f}%(N={len(nd_match)})  累计={cum_acc:.1f}%(N={cum_n})")

        total_acc = cum_ok / cum_n * 100 if cum_n > 0 else 0
        total_cov = cum_n / len(records) * 100 if records else 0
        # 非深跌周覆盖
        nd_covered = [i for i in covered if records[i]['market_chg'] >= -1]
        nd_ok_total = sum(1 for i in nd_covered if records[i]['next_up'])
        nd_acc_total = nd_ok_total / len(nd_covered) * 100 if nd_covered else 0
        print(f"  → 总计: N={cum_n}  准确率={total_acc:.1f}%  覆盖={total_cov:.1f}%")
        print(f"    非深跌周: N={len(nd_covered)}  准确率={nd_acc_total:.1f}%")


if __name__ == '__main__':
    main()
