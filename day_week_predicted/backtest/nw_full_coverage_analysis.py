#!/usr/bin/env python3
"""
下周预测全场景因子挖掘
======================
目标：找到在所有大盘环境下都能预测下周涨跌的规则，不限于大盘深跌。

扫描维度：
  1. 本周涨跌幅区间 × 大盘涨跌幅区间
  2. 价格位置(60日) × 本周涨跌
  3. 前周动量 × 本周涨跌
  4. 连涨/连跌天数
  5. 成交量变化（缩量/放量）
  6. 组合因子

用法：
    python -m day_week_predicted.backtest.nw_full_coverage_analysis
"""
import sys, logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float, _compound_return, _get_stock_index,
)

N_WEEKS = 29

def run():
    t0 = datetime.now()
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)

    all_codes = _get_all_stock_codes()
    logger.info("股票数: %d, 最新日: %s", len(all_codes), latest_date)

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    logger.info("加载个股K线...")
    stock_klines = defaultdict(list)
    bs = 200
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i+bs]
        ph = ','.join(['%s']*len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,close_price,change_percent,trading_volume "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date`>=%s AND `date`<=%s ORDER BY `date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'], 'close': _to_float(r['close_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
            })

    logger.info("加载指数K线...")
    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH','399001.SZ','899050.SZ'):
        if idx not in idx_codes: idx_codes.append(idx)
    ph = ','.join(['%s']*len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({'date': r['date'], 'change_percent': _to_float(r['change_percent'])})
    conn.close()

    mkt_by_week = {}
    for ic, kl in mkt_kl.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    logger.info("数据加载完成, 开始扫描...")

    # ── 收集所有样本的特征 ──
    samples = []
    processed = 0
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        idx_bw = mkt_by_week.get(stock_idx, {})
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''

        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            wg[dt.isocalendar()[:2]].append(k)

        sorted_weeks = sorted(wg.keys())
        sorted_all = sorted(klines, key=lambda x: x['date'])

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
            this_chg = _compound_return(this_pcts)
            next_chg = _compound_return([d['change_percent'] for d in next_days])
            actual_up = next_chg >= 0

            mw = idx_bw.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0

            # price_pos_60
            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            # prev_week_chg
            prev_chg = None
            pk = hist[-5:] if len(hist) >= 5 else hist
            if pk:
                prev_chg = _compound_return([k['change_percent'] for k in pk])

            # 连涨/连跌
            cd = 0; cu = 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else: break

            # 量比 (本周均量 / 前20日均量)
            vol_ratio = None
            this_vols = [d['volume'] for d in this_days if d['volume'] > 0]
            hist_vols = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if this_vols and hist_vols:
                avg_this = sum(this_vols) / len(this_vols)
                avg_hist = sum(hist_vols) / len(hist_vols)
                if avg_hist > 0:
                    vol_ratio = avg_this / avg_hist

            samples.append({
                'code': code, 'suffix': suffix,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'vol_ratio': vol_ratio,
                'actual_up': actual_up, 'next_chg': next_chg,
                'last_day': this_pcts[-1],
            })

        processed += 1
        if processed % 1000 == 0:
            logger.info("  已处理 %d/%d ...", processed, len(all_codes))

    logger.info("总样本数: %d", len(samples))
    base_up = sum(1 for s in samples if s['actual_up'])
    logger.info("基准涨率: %.1f%% (%d/%d)", base_up/len(samples)*100, base_up, len(samples))

    # ═══════════════════════════════════════════════════════
    # 1. 大盘环境 × 个股涨跌 交叉分析
    # ═══════════════════════════════════════════════════════
    print("\n" + "="*80)
    print("  1. 大盘环境 × 个股涨跌 交叉分析")
    print("="*80)

    mkt_bins = [(-999, -3, '大盘跌>3%'), (-3, -1, '大盘跌1~3%'), (-1, 0, '大盘微跌'),
                (0, 1, '大盘微涨'), (1, 3, '大盘涨1~3%'), (3, 999, '大盘涨>3%')]
    stk_bins = [(-999, -8, '跌>8%'), (-8, -5, '跌5~8%'), (-5, -3, '跌3~5%'),
                (-3, -1, '跌1~3%'), (-1, 0, '微跌'), (0, 1, '微涨'),
                (1, 3, '涨1~3%'), (3, 5, '涨3~5%'), (5, 8, '涨5~8%'), (8, 999, '涨>8%')]

    header = '个股\\大盘'
    print(f"\n{header:<12}", end='')
    for _, _, ml in mkt_bins:
        print(f"{ml:>14}", end='')
    print()

    for sl, sh, sn in stk_bins:
        print(f"{sn:<12}", end='')
        for ml, mh, mn in mkt_bins:
            ss = [s for s in samples if sl <= s['this_chg'] < sh and ml <= s['mkt_chg'] < mh]
            if len(ss) >= 30:
                up = sum(1 for s in ss if s['actual_up'])
                acc = up / len(ss) * 100
                # 涨率>55%标记为可预测涨，<45%标记为可预测跌
                mark = '↑' if acc >= 58 else ('↓' if acc <= 42 else ' ')
                print(f"  {acc:4.0f}%/{len(ss):>4d}{mark}", end='')
            else:
                print(f"  {'---':>10}", end='')
        print()

    # ═══════════════════════════════════════════════════════
    # 2. 价格位置 × 本周涨跌 分析
    # ═══════════════════════════════════════════════════════
    print("\n" + "="*80)
    print("  2. 价格位置(60日) × 本周涨跌")
    print("="*80)

    pos_bins = [(0, 0.2, '低位<0.2'), (0.2, 0.4, '偏低0.2~0.4'), (0.4, 0.6, '中位0.4~0.6'),
                (0.6, 0.8, '偏高0.6~0.8'), (0.8, 1.01, '高位>0.8')]

    h2 = '位置\\涨跌'
    print(f"\n{h2:<16}", end='')
    for _, _, sn in stk_bins:
        print(f"{sn:>12}", end='')
    print()

    for pl, ph, pn in pos_bins:
        print(f"{pn:<16}", end='')
        for sl, sh, sn in stk_bins:
            ss = [s for s in samples if s['pos60'] is not None and pl <= s['pos60'] < ph and sl <= s['this_chg'] < sh]
            if len(ss) >= 30:
                up = sum(1 for s in ss if s['actual_up'])
                acc = up / len(ss) * 100
                mark = '↑' if acc >= 58 else ('↓' if acc <= 42 else ' ')
                print(f" {acc:4.0f}%/{len(ss):>4d}{mark}", end='')
            else:
                print(f" {'---':>10}", end='')
        print()

    # ═══════════════════════════════════════════════════════
    # 3. 前周动量 × 本周涨跌
    # ═══════════════════════════════════════════════════════
    print("\n" + "="*80)
    print("  3. 前周动量 × 本周涨跌")
    print("="*80)

    prev_bins = [(-999, -5, '前周大跌<-5%'), (-5, -2, '前周跌2~5%'), (-2, 0, '前周微跌'),
                 (0, 2, '前周微涨'), (2, 5, '前周涨2~5%'), (5, 999, '前周大涨>5%')]

    h3 = '前周\\本周'
    print(f"\n{h3:<18}", end='')
    for _, _, sn in stk_bins:
        print(f"{sn:>12}", end='')
    print()

    for pvl, pvh, pvn in prev_bins:
        print(f"{pvn:<18}", end='')
        for sl, sh, sn in stk_bins:
            ss = [s for s in samples if s['prev_chg'] is not None and pvl <= s['prev_chg'] < pvh and sl <= s['this_chg'] < sh]
            if len(ss) >= 30:
                up = sum(1 for s in ss if s['actual_up'])
                acc = up / len(ss) * 100
                mark = '↑' if acc >= 58 else ('↓' if acc <= 42 else ' ')
                print(f" {acc:4.0f}%/{len(ss):>4d}{mark}", end='')
            else:
                print(f" {'---':>10}", end='')
        print()

    # ═══════════════════════════════════════════════════════
    # 4. 量比 × 本周涨跌
    # ═══════════════════════════════════════════════════════
    print("\n" + "="*80)
    print("  4. 量比 × 本周涨跌")
    print("="*80)

    vol_bins = [(0, 0.5, '极缩量<0.5'), (0.5, 0.8, '缩量0.5~0.8'), (0.8, 1.2, '平量0.8~1.2'),
                (1.2, 2.0, '放量1.2~2'), (2.0, 999, '巨量>2')]

    h4 = '量比\\涨跌'
    print(f"\n{h4:<16}", end='')
    for _, _, sn in stk_bins:
        print(f"{sn:>12}", end='')
    print()

    for vl, vh, vn in vol_bins:
        print(f"{vn:<16}", end='')
        for sl, sh, sn in stk_bins:
            ss = [s for s in samples if s['vol_ratio'] is not None and vl <= s['vol_ratio'] < vh and sl <= s['this_chg'] < sh]
            if len(ss) >= 30:
                up = sum(1 for s in ss if s['actual_up'])
                acc = up / len(ss) * 100
                mark = '↑' if acc >= 58 else ('↓' if acc <= 42 else ' ')
                print(f" {acc:4.0f}%/{len(ss):>4d}{mark}", end='')
            else:
                print(f" {'---':>10}", end='')
        print()

    # ═══════════════════════════════════════════════════════
    # 5. 连涨/连跌天数 × 本周涨跌
    # ═══════════════════════════════════════════════════════
    print("\n" + "="*80)
    print("  5. 连涨/连跌天数分析")
    print("="*80)

    for label, key in [('连跌天数', 'cd'), ('连涨天数', 'cu')]:
        print(f"\n  {label}:")
        for d in range(1, 6):
            ss = [s for s in samples if s[key] >= d]
            if len(ss) >= 30:
                up = sum(1 for s in ss if s['actual_up'])
                print(f"    >={d}天: 涨率{up/len(ss)*100:.1f}% ({up}/{len(ss)})")

    # ═══════════════════════════════════════════════════════
    # 6. 高胜率组合因子搜索（≥58%涨率 或 ≤42%涨率）
    # ═══════════════════════════════════════════════════════
    print("\n" + "="*80)
    print("  6. 高胜率组合因子搜索 (≥60%涨率 或 ≤40%跌率, 样本≥100)")
    print("="*80)

    combos = []

    # 6a: 大盘 × 个股 × 位置
    for ml, mh, mn in mkt_bins:
        for sl, sh, sn in stk_bins:
            for pl, ph2, pn in pos_bins:
                ss = [s for s in samples if ml <= s['mkt_chg'] < mh and sl <= s['this_chg'] < sh
                      and s['pos60'] is not None and pl <= s['pos60'] < ph2]
                if len(ss) >= 100:
                    up = sum(1 for s in ss if s['actual_up'])
                    rate = up / len(ss) * 100
                    if rate >= 60 or rate <= 40:
                        combos.append((rate, len(ss), f"{mn}+{sn}+{pn}", rate >= 60))

    # 6b: 大盘 × 个股 × 前周
    for ml, mh, mn in mkt_bins:
        for sl, sh, sn in stk_bins:
            for pvl, pvh, pvn in prev_bins:
                ss = [s for s in samples if ml <= s['mkt_chg'] < mh and sl <= s['this_chg'] < sh
                      and s['prev_chg'] is not None and pvl <= s['prev_chg'] < pvh]
                if len(ss) >= 100:
                    up = sum(1 for s in ss if s['actual_up'])
                    rate = up / len(ss) * 100
                    if rate >= 60 or rate <= 40:
                        combos.append((rate, len(ss), f"{mn}+{sn}+{pvn}", rate >= 60))

    # 6c: 大盘 × 个股 × 量比
    for ml, mh, mn in mkt_bins:
        for sl, sh, sn in stk_bins:
            for vl, vh, vn in vol_bins:
                ss = [s for s in samples if ml <= s['mkt_chg'] < mh and sl <= s['this_chg'] < sh
                      and s['vol_ratio'] is not None and vl <= s['vol_ratio'] < vh]
                if len(ss) >= 100:
                    up = sum(1 for s in ss if s['actual_up'])
                    rate = up / len(ss) * 100
                    if rate >= 60 or rate <= 40:
                        combos.append((rate, len(ss), f"{mn}+{sn}+{vn}", rate >= 60))

    # 6d: 个股 × 位置 × 前周 (不限大盘)
    for sl, sh, sn in stk_bins:
        for pl, ph2, pn in pos_bins:
            for pvl, pvh, pvn in prev_bins:
                ss = [s for s in samples if sl <= s['this_chg'] < sh
                      and s['pos60'] is not None and pl <= s['pos60'] < ph2
                      and s['prev_chg'] is not None and pvl <= s['prev_chg'] < pvh]
                if len(ss) >= 100:
                    up = sum(1 for s in ss if s['actual_up'])
                    rate = up / len(ss) * 100
                    if rate >= 60 or rate <= 40:
                        combos.append((rate, len(ss), f"{sn}+{pn}+{pvn}", rate >= 60))

    # 6e: 个股 × 位置 × 量比 (不限大盘)
    for sl, sh, sn in stk_bins:
        for pl, ph2, pn in pos_bins:
            for vl, vh, vn in vol_bins:
                ss = [s for s in samples if sl <= s['this_chg'] < sh
                      and s['pos60'] is not None and pl <= s['pos60'] < ph2
                      and s['vol_ratio'] is not None and vl <= s['vol_ratio'] < vh]
                if len(ss) >= 100:
                    up = sum(1 for s in ss if s['actual_up'])
                    rate = up / len(ss) * 100
                    if rate >= 60 or rate <= 40:
                        combos.append((rate, len(ss), f"{sn}+{pn}+{vn}", rate >= 60))

    # 排序输出
    combos.sort(key=lambda x: (-x[0] if x[3] else x[0], -x[1]))

    print(f"\n  预测涨 (涨率≥60%):")
    up_combos = [c for c in combos if c[3]]
    for rate, n, desc, _ in up_combos[:40]:
        print(f"    {rate:5.1f}% ({n:>5d}样本)  {desc}")

    print(f"\n  预测跌 (涨率≤40%):")
    dn_combos = [c for c in combos if not c[3]]
    for rate, n, desc, _ in dn_combos[:40]:
        print(f"    {rate:5.1f}% ({n:>5d}样本)  {desc}")

    # ═══════════════════════════════════════════════════════
    # 7. 单因子涨跌率（不限大盘）
    # ═══════════════════════════════════════════════════════
    print("\n" + "="*80)
    print("  7. 单因子涨跌率（不限大盘，样本≥500）")
    print("="*80)

    single_factors = []
    # 个股涨跌
    for sl, sh, sn in stk_bins:
        ss = [s for s in samples if sl <= s['this_chg'] < sh]
        if len(ss) >= 500:
            up = sum(1 for s in ss if s['actual_up'])
            single_factors.append((up/len(ss)*100, len(ss), sn))
    # 位置
    for pl, ph2, pn in pos_bins:
        ss = [s for s in samples if s['pos60'] is not None and pl <= s['pos60'] < ph2]
        if len(ss) >= 500:
            up = sum(1 for s in ss if s['actual_up'])
            single_factors.append((up/len(ss)*100, len(ss), pn))
    # 前周
    for pvl, pvh, pvn in prev_bins:
        ss = [s for s in samples if s['prev_chg'] is not None and pvl <= s['prev_chg'] < pvh]
        if len(ss) >= 500:
            up = sum(1 for s in ss if s['actual_up'])
            single_factors.append((up/len(ss)*100, len(ss), pvn))
    # 量比
    for vl, vh, vn in vol_bins:
        ss = [s for s in samples if s['vol_ratio'] is not None and vl <= s['vol_ratio'] < vh]
        if len(ss) >= 500:
            up = sum(1 for s in ss if s['actual_up'])
            single_factors.append((up/len(ss)*100, len(ss), vn))

    single_factors.sort(key=lambda x: -x[0])
    for rate, n, desc in single_factors:
        mark = '↑' if rate >= 55 else ('↓' if rate <= 45 else ' ')
        print(f"    {rate:5.1f}% ({n:>6d}样本)  {desc} {mark}")

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n耗时: {elapsed:.1f}s")


if __name__ == '__main__':
    run()
