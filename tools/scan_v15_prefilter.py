#!/usr/bin/env python3
"""
扫描全市场 ~5000 只股票，找出符合 V15 预过滤条件的股票。

V15 预过滤条件（全部满足才通过）：
  1. 本周涨幅 >= 8%
  2. 60日价格位置 >= 70%
  3. 量比 >= 1.0
  4. 大盘本周涨跌 > -3%

输出：符合条件的股票列表及其特征数据。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection


def compound_return(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100.0)
    return round((r - 1) * 100, 2)


def to_float(v):
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def main():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 1. 获取最新交易日
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code = '000001.SH'")
    latest_date = cur.fetchone()['d']
    print(f"最新交易日: {latest_date}")

    dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
    iso_cal = dt_latest.isocalendar()
    iso_year, iso_week = iso_cal[0], iso_cal[1]
    print(f"当前周: Y{iso_year}-W{iso_week}")

    # 2. 先算大盘本周涨跌（上证指数 000001.SH + 深证成指 399001.SZ）
    market_codes = ['000001.SH', '399001.SZ', '399006.SZ']
    market_chgs = {}
    for mc in market_codes:
        cur.execute(
            "SELECT `date`, change_percent FROM stock_kline "
            "WHERE stock_code = %s AND `date` >= %s ORDER BY `date`",
            (mc, (dt_latest - timedelta(days=10)).strftime('%Y-%m-%d'))
        )
        rows = cur.fetchall()
        week_pcts = []
        for r in rows:
            d = datetime.strptime(r['date'], '%Y-%m-%d')
            if d.isocalendar()[0] == iso_year and d.isocalendar()[1] == iso_week:
                week_pcts.append(to_float(r['change_percent']))
        if week_pcts:
            market_chgs[mc] = compound_return(week_pcts)
    
    sh_chg = market_chgs.get('000001.SH', 0)
    sz_chg = market_chgs.get('399001.SZ', 0)
    cy_chg = market_chgs.get('399006.SZ', 0)
    print(f"大盘本周: 上证{sh_chg:+.2f}%  深证{sz_chg:+.2f}%  创业板{cy_chg:+.2f}%")

    # V15条件4: 大盘>-3% (用上证)
    if sh_chg < -3.0:
        print(f"\n⚠ 大盘暴跌 {sh_chg:+.2f}% < -3%，V15预过滤不适用（崩盘周暴涨股往往继续涨）")
        print("本周无符合V15条件的股票。")
        conn.close()
        return

    # 3. 获取所有股票代码（排除指数）
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline "
        "WHERE stock_code NOT LIKE '%%.SH' OR stock_code LIKE '6%%'"
    )
    # 更精确：排除指数代码
    cur.execute("SELECT DISTINCT stock_code FROM stock_kline")
    all_codes = [r['stock_code'] for r in cur.fetchall()]
    # 过滤掉指数
    stock_codes = [c for c in all_codes if not (
        c.startswith('000') and c.endswith('.SH')  # 上证指数
        or c.startswith('399') and c.endswith('.SZ')  # 深证指数
    )]
    print(f"总股票数: {len(stock_codes)}")

    # 4. 批量加载本周K线数据 — 用SQL直接筛选本周日期
    week_start = dt_latest - timedelta(days=dt_latest.weekday())  # 本周一
    week_start_str = week_start.strftime('%Y-%m-%d')
    lookback_str = (dt_latest - timedelta(days=90)).strftime('%Y-%m-%d')

    print(f"本周范围: {week_start_str} ~ {latest_date}")
    print("加载K线数据...")

    # 第一步：快速筛选本周涨幅>=8%的股票（SQL层面先过滤）
    # 用本周所有交易日的 change_percent 计算复合涨跌幅
    cur.execute(
        "SELECT stock_code, GROUP_CONCAT(change_percent ORDER BY `date`) as pcts, "
        "COUNT(*) as day_cnt "
        "FROM stock_kline "
        "WHERE `date` >= %s AND `date` <= %s "
        "GROUP BY stock_code "
        "HAVING day_cnt >= 3",
        (week_start_str, latest_date)
    )
    week_data = cur.fetchall()
    print(f"本周有>=3天数据的股票: {len(week_data)}")

    # 计算本周涨幅，初筛>=8%
    candidates = []  # (code, this_week_chg, day_count)
    for row in week_data:
        code = row['stock_code']
        # 排除指数
        if (code.startswith('000') and code.endswith('.SH')) or \
           (code.startswith('399') and code.endswith('.SZ')):
            continue
        pcts = [to_float(p) for p in row['pcts'].split(',')]
        chg = compound_return(pcts)
        if chg >= 8.0:
            candidates.append((code, chg, row['day_cnt']))

    print(f"\n═══ 第1步: 本周涨幅>=8% ═══")
    print(f"符合: {len(candidates)} 只")

    if not candidates:
        print("无符合条件的股票。")
        conn.close()
        return

    # 第二步：对候选股票计算60日价格位置和量比
    candidate_codes = [c[0] for c in candidates]
    chg_map = {c[0]: c[1] for c in candidates}

    # 批量加载近90天K线
    print(f"加载 {len(candidate_codes)} 只候选股票的近90天K线...")
    klines = defaultdict(list)
    bs = 500
    for i in range(0, len(candidate_codes), bs):
        batch = candidate_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY stock_code, `date`",
            batch + [lookback_str, latest_date]
        )
        for r in cur.fetchall():
            klines[r['stock_code']].append(r)

    conn.close()

    # 计算60日价格位置和量比
    results = []
    filtered_pos = 0
    filtered_vol = 0

    for code, this_chg, day_cnt in candidates:
        ks = klines.get(code, [])
        if len(ks) < 25:
            continue

        # 分离本周和历史
        week_ks = [k for k in ks if k['date'] >= week_start_str]
        hist_ks = [k for k in ks if k['date'] < week_start_str]

        if len(hist_ks) < 20 or not week_ks:
            continue

        # 60日价格位置
        hist_close = [to_float(k['close_price']) for k in hist_ks[-60:] if to_float(k['close_price']) > 0]
        week_close = [to_float(k['close_price']) for k in week_ks if to_float(k['close_price']) > 0]
        if not hist_close or not week_close:
            continue

        all_close = hist_close + week_close
        mn, mx = min(all_close), max(all_close)
        last_close = week_close[-1]
        if mx <= mn or last_close <= 0:
            continue
        price_pos = round((last_close - mn) / (mx - mn), 4)

        if price_pos < 0.7:
            filtered_pos += 1
            continue

        # 量比
        avg_vol_20 = sum(to_float(k['trading_volume']) for k in hist_ks[-20:]) / 20
        week_avg_vol = sum(to_float(k['trading_volume']) for k in week_ks) / len(week_ks)
        vol_ratio = round(week_avg_vol / avg_vol_20, 2) if avg_vol_20 > 0 else 0

        if vol_ratio < 1.0:
            filtered_vol += 1
            continue

        # 获取大盘涨跌（根据市场）
        if code.endswith('.SH'):
            mkt_chg = sh_chg
        else:
            mkt_chg = sz_chg

        results.append({
            'code': code,
            'this_week_chg': this_chg,
            'price_pos_60': price_pos,
            'vol_ratio': vol_ratio,
            'market_chg': mkt_chg,
            'last_close': last_close,
            'days': day_cnt,
        })

    print(f"\n═══ 第2步: 60日位置>=70% ═══")
    print(f"被过滤（位置<70%）: {filtered_pos} 只")

    print(f"\n═══ 第3步: 量比>=1.0 ═══")
    print(f"被过滤（量比<1.0）: {filtered_vol} 只")

    print(f"\n═══ 第4步: 大盘>-3%（已通过）═══")

    # 按涨幅排序
    results.sort(key=lambda x: -x['this_week_chg'])

    print(f"\n{'='*80}")
    print(f"✅ 符合V15全部条件的股票: {len(results)} 只")
    print(f"{'='*80}")
    print(f"{'代码':<12} {'本周涨幅':>8} {'60日位置':>8} {'量比':>6} {'大盘':>6} {'收盘价':>8}")
    print('-' * 80)
    for r in results:
        print(f"{r['code']:<12} {r['this_week_chg']:>+7.2f}% {r['price_pos_60']:>7.1%} "
              f"{r['vol_ratio']:>6.2f} {r['market_chg']:>+5.2f}% {r['last_close']:>8.2f}")

    print(f"\n汇总:")
    print(f"  全市场股票: ~{len(stock_codes)}")
    print(f"  本周涨>=8%: {len(candidates)}")
    print(f"  + 60日位>=70%: {len(candidates) - filtered_pos}")
    print(f"  + 量比>=1.0: {len(results)}")
    print(f"  + 大盘>-3%: {len(results)} (本周大盘{sh_chg:+.2f}%，已满足)")
    print(f"  通过率: {len(results)/len(stock_codes)*100:.2f}%")

    # 输出可直接用于DeepSeek预测的代码列表
    if results:
        codes_str = ','.join(r['code'] for r in results)
        print(f"\n可直接用于DeepSeek预测的代码列表:")
        print(codes_str)


if __name__ == '__main__':
    main()
