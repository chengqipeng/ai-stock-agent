#!/usr/bin/env python3
"""
V4深度因子分析 - 寻找不限大盘环境下的高胜率规则
================================================
V4初版回测发现R5-R8(涨信号)全部失败(46-52%)，需要更精细的因子组合。

策略：
  1. 对每个大盘环境分别分析，找到各环境下的有效规则
  2. 多因子组合搜索，要求准确率≥63%且样本≥200
  3. 特别关注深证股票（V3几乎无覆盖）

用法：
    python -m day_week_predicted.backtest.nw_v4_deep_factor_analysis
"""
import sys, logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _get_all_stock_codes, _get_latest_trade_date, _to_float,
    _compound_return, _get_stock_index,
)

N_WEEKS = 29


def load_samples():
    """加载所有样本数据"""
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)

    all_codes = _get_all_stock_codes()
    logger.info("股票数: %d", len(all_codes))

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    stock_klines = defaultdict(list)
    bs = 200
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
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

    idx_codes = list(set(_get_stock_index(c) for c in all_codes))
    for idx in ('000001.SH', '399001.SZ', '899050.SZ'):
        if idx not in idx_codes:
            idx_codes.append(idx)
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({
            'date': r['date'],
            'change_percent': _to_float(r['change_percent']),
        })
    conn.close()

    mkt_by_week = {}
    for ic, kl in mkt_kl.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    samples = []
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

            prev_chg = None
            pk = hist[-5:] if len(hist) >= 5 else hist
            if pk:
                prev_chg = _compound_return([k['change_percent'] for k in pk])

            cd = 0
            cu = 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1
                    if cu > 0:
                        break
                elif p > 0:
                    cu += 1
                    if cd > 0:
                        break
                else:
                    break

            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = sum(tv) / len(tv)
                ah = sum(hv) / len(hv)
                if ah > 0:
                    vol_ratio = at / ah

            # 最后一天涨跌
            last_day_chg = this_pcts[-1]
            # 周内最大跌幅（单日）
            max_daily_drop = min(this_pcts) if this_pcts else 0
            # 周内最大涨幅（单日）
            max_daily_rise = max(this_pcts) if this_pcts else 0

            samples.append({
                'code': code, 'suffix': suffix,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'vol_ratio': vol_ratio,
                'actual_up': actual_up, 'next_chg': next_chg,
                'last_day': last_day_chg,
                'max_drop': max_daily_drop,
                'max_rise': max_daily_rise,
            })

    return samples


def analyze(samples):
    """深度分析各场景下的高胜率因子组合"""
    total = len(samples)
    base_up = sum(1 for s in samples if s['actual_up'])
    print(f"\n总样本: {total}, 基准涨率: {base_up/total*100:.1f}%")

    # ═══════════════════════════════════════════════════════
    # A. 按大盘环境分组，在每个环境内搜索有效规则
    # ═══════════════════════════════════════════════════════
    mkt_envs = [
        ('大盘跌>3%', lambda s: s['mkt_chg'] < -3),
        ('大盘跌1~3%', lambda s: -3 <= s['mkt_chg'] < -1),
        ('大盘微跌0~1%', lambda s: -1 <= s['mkt_chg'] < 0),
        ('大盘微涨0~1%', lambda s: 0 <= s['mkt_chg'] < 1),
        ('大盘涨1~3%', lambda s: 1 <= s['mkt_chg'] < 3),
        ('大盘涨>3%', lambda s: s['mkt_chg'] >= 3),
        ('不限大盘', lambda s: True),
    ]

    # 个股涨跌条件
    stk_conds = [
        ('跌>10%', lambda s: s['this_chg'] < -10),
        ('跌>8%', lambda s: s['this_chg'] < -8),
        ('跌>5%', lambda s: s['this_chg'] < -5),
        ('跌>3%', lambda s: s['this_chg'] < -3),
        ('跌>2%', lambda s: s['this_chg'] < -2),
        ('涨>2%', lambda s: s['this_chg'] > 2),
        ('涨>3%', lambda s: s['this_chg'] > 3),
        ('涨>5%', lambda s: s['this_chg'] > 5),
        ('涨>8%', lambda s: s['this_chg'] > 8),
        ('涨>10%', lambda s: s['this_chg'] > 10),
    ]

    # 位置条件
    pos_conds = [
        ('低位<0.15', lambda s: s['pos60'] is not None and s['pos60'] < 0.15),
        ('低位<0.2', lambda s: s['pos60'] is not None and s['pos60'] < 0.2),
        ('低位<0.3', lambda s: s['pos60'] is not None and s['pos60'] < 0.3),
        ('非高位<0.6', lambda s: s['pos60'] is not None and s['pos60'] < 0.6),
        ('非高位<0.7', lambda s: s['pos60'] is None or s['pos60'] < 0.7),
        ('偏高>0.6', lambda s: s['pos60'] is not None and s['pos60'] > 0.6),
        ('高位>0.7', lambda s: s['pos60'] is not None and s['pos60'] > 0.7),
        ('高位>0.8', lambda s: s['pos60'] is not None and s['pos60'] > 0.8),
    ]

    # 前周条件
    prev_conds = [
        ('前周大跌<-5%', lambda s: s['prev_chg'] is not None and s['prev_chg'] < -5),
        ('前周跌<-3%', lambda s: s['prev_chg'] is not None and s['prev_chg'] < -3),
        ('前周跌<-2%', lambda s: s['prev_chg'] is not None and s['prev_chg'] < -2),
        ('前周涨>2%', lambda s: s['prev_chg'] is not None and s['prev_chg'] > 2),
        ('前周涨>3%', lambda s: s['prev_chg'] is not None and s['prev_chg'] > 3),
        ('前周大涨>5%', lambda s: s['prev_chg'] is not None and s['prev_chg'] > 5),
    ]

    # 量比条件
    vol_conds = [
        ('极缩量<0.5', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] < 0.5),
        ('缩量<0.7', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] < 0.7),
        ('放量>1.5', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] > 1.5),
        ('放量>2.0', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] > 2.0),
    ]

    # 连涨连跌条件
    consec_conds = [
        ('连跌≥3天', lambda s: s['cd'] >= 3),
        ('连跌≥4天', lambda s: s['cd'] >= 4),
        ('连涨≥3天', lambda s: s['cu'] >= 3),
        ('连涨≥4天', lambda s: s['cu'] >= 4),
    ]

    # 最后一天条件
    lastday_conds = [
        ('尾日跌>2%', lambda s: s['last_day'] < -2),
        ('尾日跌>3%', lambda s: s['last_day'] < -3),
        ('尾日涨>2%', lambda s: s['last_day'] > 2),
        ('尾日涨>3%', lambda s: s['last_day'] > 3),
    ]

    # 市场后缀条件
    suffix_conds = [
        ('上证', lambda s: s['suffix'] == 'SH'),
        ('深证', lambda s: s['suffix'] == 'SZ'),
        ('不限', lambda s: True),
    ]

    all_extra_conds = pos_conds + prev_conds + vol_conds + consec_conds + lastday_conds

    print("\n" + "=" * 100)
    print("  A. 各大盘环境下的高胜率组合搜索 (准确率≥63%, 样本≥150)")
    print("=" * 100)

    results = []

    for mkt_name, mkt_fn in mkt_envs:
        mkt_samples = [s for s in samples if mkt_fn(s)]
        if len(mkt_samples) < 100:
            continue

        for sfx_name, sfx_fn in suffix_conds:
            sfx_samples = [s for s in mkt_samples if sfx_fn(s)]
            if len(sfx_samples) < 100:
                continue

            for stk_name, stk_fn in stk_conds:
                stk_samples = [s for s in sfx_samples if stk_fn(s)]
                if len(stk_samples) < 50:
                    continue

                # 单条件
                up = sum(1 for s in stk_samples if s['actual_up'])
                rate = up / len(stk_samples) * 100
                is_up_pred = rate >= 63
                is_dn_pred = rate <= 37
                if (is_up_pred or is_dn_pred) and len(stk_samples) >= 150:
                    results.append((rate, len(stk_samples),
                                    f"{mkt_name}+{sfx_name}+{stk_name}",
                                    is_up_pred))

                # 双条件组合
                for ext_name, ext_fn in all_extra_conds:
                    combo = [s for s in stk_samples if ext_fn(s)]
                    if len(combo) < 150:
                        continue
                    up2 = sum(1 for s in combo if s['actual_up'])
                    rate2 = up2 / len(combo) * 100
                    is_up2 = rate2 >= 63
                    is_dn2 = rate2 <= 37
                    if is_up2 or is_dn2:
                        results.append((rate2, len(combo),
                                        f"{mkt_name}+{sfx_name}+{stk_name}+{ext_name}",
                                        is_up2))

                    # 三条件组合（在双条件基础上再加一个）
                    for ext2_name, ext2_fn in all_extra_conds:
                        if ext2_name == ext_name:
                            continue
                        combo2 = [s for s in combo if ext2_fn(s)]
                        if len(combo2) < 150:
                            continue
                        up3 = sum(1 for s in combo2 if s['actual_up'])
                        rate3 = up3 / len(combo2) * 100
                        is_up3 = rate3 >= 63
                        is_dn3 = rate3 <= 37
                        if is_up3 or is_dn3:
                            results.append((rate3, len(combo2),
                                            f"{mkt_name}+{sfx_name}+{stk_name}+{ext_name}+{ext2_name}",
                                            is_up3))

    # 去重并排序
    seen = set()
    unique_results = []
    for r in results:
        key = r[2]
        if key not in seen:
            seen.add(key)
            unique_results.append(r)

    # 涨信号
    up_results = sorted([r for r in unique_results if r[3]],
                        key=lambda x: (-x[0], -x[1]))
    print(f"\n  ── 预测涨 (涨率≥63%, 样本≥150) ── 共{len(up_results)}条")
    for rate, n, desc, _ in up_results[:60]:
        print(f"    {rate:5.1f}% ({n:>5d}样本)  {desc}")

    # 跌信号
    dn_results = sorted([r for r in unique_results if not r[3]],
                        key=lambda x: (x[0], -x[1]))
    print(f"\n  ── 预测跌 (涨率≤37%, 样本≥150) ── 共{len(dn_results)}条")
    for rate, n, desc, _ in dn_results[:60]:
        print(f"    {rate:5.1f}% ({n:>5d}样本)  {desc}")

    # ═══════════════════════════════════════════════════════
    # B. 特别分析深证股票（V3几乎无覆盖）
    # ═══════════════════════════════════════════════════════
    print("\n" + "=" * 100)
    print("  B. 深证股票专项分析")
    print("=" * 100)

    sz_samples = [s for s in samples if s['suffix'] == 'SZ']
    sz_up = sum(1 for s in sz_samples if s['actual_up'])
    print(f"  深证样本: {len(sz_samples)}, 基准涨率: {sz_up/len(sz_samples)*100:.1f}%")

    # 深证各大盘环境
    for mkt_name, mkt_fn in mkt_envs:
        ms = [s for s in sz_samples if mkt_fn(s)]
        if len(ms) < 100:
            continue
        up = sum(1 for s in ms if s['actual_up'])
        print(f"  {mkt_name}: 涨率{up/len(ms)*100:.1f}% ({up}/{len(ms)})")

    # ═══════════════════════════════════════════════════════
    # C. 不限大盘的纯个股因子组合
    # ═══════════════════════════════════════════════════════
    print("\n" + "=" * 100)
    print("  C. 不限大盘的纯个股因子组合 (准确率≥60%, 样本≥300)")
    print("=" * 100)

    pure_results = []
    for stk_name, stk_fn in stk_conds:
        stk_s = [s for s in samples if stk_fn(s)]
        if len(stk_s) < 100:
            continue
        for ext_name, ext_fn in all_extra_conds:
            combo = [s for s in stk_s if ext_fn(s)]
            if len(combo) < 300:
                continue
            up = sum(1 for s in combo if s['actual_up'])
            rate = up / len(combo) * 100
            if rate >= 60 or rate <= 40:
                pure_results.append((rate, len(combo),
                                     f"{stk_name}+{ext_name}",
                                     rate >= 60))
            # 再加一层
            for ext2_name, ext2_fn in all_extra_conds:
                if ext2_name == ext_name:
                    continue
                combo2 = [s for s in combo if ext2_fn(s)]
                if len(combo2) < 300:
                    continue
                up2 = sum(1 for s in combo2 if s['actual_up'])
                rate2 = up2 / len(combo2) * 100
                if rate2 >= 60 or rate2 <= 40:
                    pure_results.append((rate2, len(combo2),
                                         f"{stk_name}+{ext_name}+{ext2_name}",
                                         rate2 >= 60))

    seen2 = set()
    unique_pure = []
    for r in pure_results:
        if r[2] not in seen2:
            seen2.add(r[2])
            unique_pure.append(r)

    up_pure = sorted([r for r in unique_pure if r[3]], key=lambda x: (-x[0], -x[1]))
    print(f"\n  预测涨 (≥60%): 共{len(up_pure)}条")
    for rate, n, desc, _ in up_pure[:40]:
        print(f"    {rate:5.1f}% ({n:>5d}样本)  {desc}")

    dn_pure = sorted([r for r in unique_pure if not r[3]], key=lambda x: (x[0], -x[1]))
    print(f"\n  预测跌 (≤40%): 共{len(dn_pure)}条")
    for rate, n, desc, _ in dn_pure[:40]:
        print(f"    {rate:5.1f}% ({n:>5d}样本)  {desc}")


if __name__ == '__main__':
    samples = load_samples()
    analyze(samples)
