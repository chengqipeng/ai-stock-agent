#!/usr/bin/env python3
"""
分析"连续多周下跌→超跌反弹"新规则的影响面 (V2: 高效版)
===========================================================
直接从stock_kline表批量加载，优化SQL查询速度。
只加载沪深主板+创业板(排除北交所)以加速。
"""
import sys, os, logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _to_float, _compound_return, _get_stock_index,
    _nw_match_rule, _NW_V11_ENGINE, _NW_V11_LAYERS,
)

N_WEEKS = 29

def _mean(lst):
    return sum(lst) / len(lst) if lst else 0

def _pct(c, t):
    return round(c / t * 100, 1) if t > 0 else 0

def load_all_data():
    """一次性加载全部K线数据到内存。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 最新交易日
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code='399001.SZ'")
    latest_date = cur.fetchone()['d']
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 4) * 7 + 60)
    start_date = dt_start.strftime('%Y-%m-%d')
    logger.info(f"日期范围: {start_date} ~ {latest_date}")

    # 一次性加载全部个股K线(只取沪深)
    logger.info("加载全部K线(单次查询)...")
    cur.execute(
        "SELECT stock_code, `date`, close_price, change_percent, trading_volume "
        "FROM stock_kline "
        "WHERE `date` >= %s AND `date` <= %s "
        "AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ') "
        "AND stock_code NOT LIKE '399%%' AND stock_code != '000001.SH' "
        "ORDER BY stock_code, `date`",
        [start_date, latest_date])
    stock_klines = defaultdict(list)
    for r in cur.fetchall():
        stock_klines[r['stock_code']].append({
            'date': r['date'],
            'close': _to_float(r['close_price']),
            'change_percent': _to_float(r['change_percent']),
            'volume': _to_float(r['trading_volume']),
        })
    logger.info(f"个股: {len(stock_klines)} 只")

    # 大盘指数
    cur.execute(
        "SELECT stock_code, `date`, change_percent FROM stock_kline "
        "WHERE stock_code IN ('000001.SH','399001.SZ','899050.SZ') "
        "AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({
            'date': r['date'], 'change_percent': _to_float(r['change_percent']),
        })
    logger.info(f"指数: {len(mkt_kl)} 个")
    conn.close()

    # 指数按周分组
    mkt_by_week = {}
    for ic, kl in mkt_kl.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    return latest_date, dt_end, stock_klines, mkt_by_week

def build_samples(stock_klines, mkt_by_week, dt_end):
    """构建全量样本。"""
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)
    samples = []
    processed = 0

    for code, klines in stock_klines.items():
        if len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
        idx_bw = mkt_by_week.get(stock_idx, {})

        # 按周分组
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

            # 大盘
            mw = idx_bw.get(iw_this, [])
            mkt_chg = _compound_return(
                [k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]
            ) if len(mw) >= 3 else 0.0
            mkt_last_day = sorted(mw, key=lambda x: x['date'])[-1]['change_percent'] if mw else None

            first_date = this_days[0]['date']
            hist = [k for k in sorted_all if k['date'] < first_date]

            # 价格位置
            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        pos60 = (lc - mn) / (mx - mn)

            # 前N周涨跌
            def get_prev_chg(offset):
                idx = i - offset
                if idx < 0: return None
                pw = sorted_weeks[idx]
                pd = sorted(wg[pw], key=lambda x: x['date'])
                if len(pd) >= 3:
                    return _compound_return([d['change_percent'] for d in pd])
                return None

            prev_chg = get_prev_chg(1)
            prev2_chg = get_prev_chg(2)
            prev3_chg = get_prev_chg(3)

            # 连涨连跌(日)
            cd, cu = 0, 0
            for p in reversed(this_pcts):
                if p < 0:
                    cd += 1; (cu > 0 and (_ for _ in ()).throw(StopIteration)) if False else None
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else: break

            last_day = this_pcts[-1] if this_pcts else 0

            # 量比
            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at, ah = _mean(tv), _mean(hv)
                if ah > 0: vol_ratio = at / ah

            # 累计跌幅
            cum_2w = (this_chg + prev_chg) if prev_chg is not None else None
            cum_3w = (this_chg + prev_chg + prev2_chg) if (prev_chg is not None and prev2_chg is not None) else None

            # 连续下跌周数
            cdw = 0
            if this_chg < 0: cdw += 1
            if prev_chg is not None and prev_chg < 0 and cdw > 0: cdw += 1
            if prev2_chg is not None and prev2_chg < 0 and cdw > 1: cdw += 1
            if prev3_chg is not None and prev3_chg < 0 and cdw > 2: cdw += 1

            # V11特征
            feat = {
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'suffix': suffix, 'pos60': pos60,
                'prev_chg': prev_chg, 'prev2_chg': prev2_chg,
                'mkt_last_day': mkt_last_day, 'vol_ratio': vol_ratio,
                'turnover_ratio': None, 'board_momentum': None,
                'concept_consensus': None, 'big_net_pct_avg': None,
                'rush_up_pullback': False, 'dip_recovery': False,
                'upper_shadow_ratio': None,
            }
            if len(this_pcts) >= 4:
                mid = len(this_pcts) // 2
                fh = _compound_return(this_pcts[:mid])
                sh = _compound_return(this_pcts[mid:])
                if fh > 2 and sh < -1: feat['rush_up_pullback'] = True
                if fh < -2 and sh > 1: feat['dip_recovery'] = True

            v11_rule = _nw_match_rule(feat)

            samples.append({
                'code': code, 'suffix': suffix,
                'iw_this': iw_this, 'iw_next': iw_next,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'prev2_chg': prev2_chg, 'prev3_chg': prev3_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'vol_ratio': vol_ratio, 'mkt_last_day': mkt_last_day,
                'cum_2w': cum_2w, 'cum_3w': cum_3w,
                'consec_down_weeks': cdw,
                'next_chg': next_chg, 'actual_up': next_chg >= 0,
                'v11_rule': v11_rule, 'v11_covered': v11_rule is not None,
            })

        processed += 1
        if processed % 1000 == 0:
            logger.info(f"  样本构建: {processed}/{len(stock_klines)} ...")

    logger.info(f"总样本: {len(samples)}")
    return samples

def match_new_rule(s, cum_th, wc, pos_lim, tc_lim):
    """检查样本是否命中新规则。"""
    if s['this_chg'] >= tc_lim: return False
    if pos_lim < 1.0 and (s['pos60'] is None or s['pos60'] >= pos_lim): return False
    if s['consec_down_weeks'] < wc: return False
    if wc == 2 and (s['cum_2w'] is None or s['cum_2w'] >= cum_th): return False
    if wc == 3 and (s['cum_3w'] is None or s['cum_3w'] >= cum_th): return False
    return True


def grid_search(samples):
    """网格搜索候选规则参数。"""
    print("\n" + "=" * 80)
    print("  1. 候选规则网格搜索 (预测涨)")
    print("=" * 80)

    cum_ths = [-8, -10, -12, -15]
    week_counts = [2, 3]
    pos_limits = [0.5, 0.6, 0.7, 0.8, 1.0]
    tc_limits = [0, -2, -3, -4]

    results = []
    for cum_th in cum_ths:
        for wc in week_counts:
            for pos_lim in pos_limits:
                for tc_lim in tc_limits:
                    matched = [s for s in samples if match_new_rule(s, cum_th, wc, pos_lim, tc_lim)]
                    new = [s for s in matched if not s['v11_covered']]
                    if len(new) < 20: continue

                    total, correct = len(matched), sum(1 for s in matched if s['actual_up'])
                    nt, nc = len(new), sum(1 for s in new if s['actual_up'])
                    results.append({
                        'cum_th': cum_th, 'wc': wc, 'pos_lim': pos_lim, 'tc_lim': tc_lim,
                        'total': total, 'acc': correct/total*100,
                        'new_total': nt, 'new_correct': nc,
                        'new_acc': nc/nt*100 if nt else 0,
                        'new_cov': nt/len(samples)*100,
                    })

    results.sort(key=lambda x: (-x['new_acc'], -x['new_total']))

    print(f"\n  总样本: {len(samples)}, V11已覆盖: {sum(1 for s in samples if s['v11_covered'])}")
    print(f"\n  {'累计':>6} {'周':>3} {'pos':>5} {'本周':>5} | {'全量':>5} {'全准确':>6} | {'新增':>5} {'新准确':>6} {'新覆盖':>6}")
    print("  " + "-" * 70)
    shown = 0
    for r in results:
        if r['new_acc'] < 53: continue
        print(f"  {r['cum_th']:>5}% {r['wc']:>3} {r['pos_lim']:>5.1f} {r['tc_lim']:>4}% | "
              f"{r['total']:>5} {r['acc']:>5.1f}% | "
              f"{r['new_total']:>5} {r['new_acc']:>5.1f}% {r['new_cov']:>5.2f}%")
        shown += 1
        if shown >= 40: break

    return results


def detailed_analysis(samples, params, label=''):
    """对指定规则进行详细分析。"""
    cum_th, wc, pos_lim, tc_lim = params
    new = [s for s in samples if match_new_rule(s, cum_th, wc, pos_lim, tc_lim) and not s['v11_covered']]
    if not new:
        print(f"  {label}: 无新增样本")
        return

    total = len(new)
    correct = sum(1 for s in new if s['actual_up'])
    acc = correct / total * 100

    print(f"\n  ── {label}: 累计{cum_th}% / {wc}周 / pos<{pos_lim} / 本周<{tc_lim}% ──")
    print(f"  新增样本: {total}, 准确率: {acc:.1f}% ({correct}/{total})")

    # 按大盘
    for label2, cond in [('大盘涨(≥0)', lambda s: s['mkt_chg'] >= 0),
                          ('大盘跌(<0)', lambda s: s['mkt_chg'] < 0)]:
        sub = [s for s in new if cond(s)]
        if sub:
            a = _pct(sum(1 for s in sub if s['actual_up']), len(sub))
            print(f"    {label2}: {len(sub)}样本, {a}%")

    # 按市场
    for label2, sfx in [('上证', 'SH'), ('深证', 'SZ')]:
        sub = [s for s in new if s['suffix'] == sfx]
        if sub:
            a = _pct(sum(1 for s in sub if s['actual_up']), len(sub))
            print(f"    {label2}: {len(sub)}样本, {a}%")

    # 按价格位置
    for lo, hi, lb in [(0, 0.2, '<0.2'), (0.2, 0.4, '0.2~0.4'), (0.4, 0.6, '0.4~0.6'), (0.6, 0.8, '0.6~0.8'), (0.8, 1.01, '≥0.8')]:
        sub = [s for s in new if s['pos60'] is not None and lo <= s['pos60'] < hi]
        if sub:
            a = _pct(sum(1 for s in sub if s['actual_up']), len(sub))
            print(f"    pos {lb}: {len(sub)}样本, {a}%")

    # 下周涨跌幅
    ncs = sorted([s['next_chg'] for s in new])
    print(f"  下周涨跌: 均值{_mean(ncs):+.2f}%, 中位{ncs[len(ncs)//2]:+.2f}%, "
          f"最大涨{max(ncs):+.2f}%, 最大跌{min(ncs):+.2f}%")

    # 英维克W11
    yw = [s for s in new if s['code'] == '002837.SZ' and s['iw_this'] == (2026, 11)]
    if yw:
        print(f"  ★ 英维克W11: 命中! 下周{yw[0]['next_chg']:+.2f}% ({'✓' if yw[0]['actual_up'] else '✗'})")
    else:
        print(f"  ★ 英维克W11: 未命中")

    return new

def time_series_cv(samples, params, n_folds=5):
    """时间序列交叉验证。"""
    cum_th, wc, pos_lim, tc_lim = params
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    fold_size = len(all_weeks) // n_folds

    fold_accs = []
    print(f"\n  CV(5折): ", end='')
    for fold in range(n_folds):
        ts = fold * fold_size
        te = ts + fold_size if fold < n_folds - 1 else len(all_weeks)
        test_weeks = set(all_weeks[ts:te])
        test = [s for s in samples if s['iw_this'] in test_weeks]
        matched = [s for s in test if match_new_rule(s, cum_th, wc, pos_lim, tc_lim) and not s['v11_covered']]
        if matched:
            acc = sum(1 for s in matched if s['actual_up']) / len(matched) * 100
            fold_accs.append(acc)
            print(f"{acc:.1f}%({len(matched)}) ", end='')
        else:
            print(f"N/A ", end='')

    if fold_accs:
        avg = _mean(fold_accs)
        print(f"\n  CV均值: {avg:.1f}%, 最低: {min(fold_accs):.1f}%, 波动: {max(fold_accs)-min(fold_accs):.1f}pp")
        return avg
    print("\n  CV: 无有效折")
    return 0


def v11_impact(samples, params):
    """V11整体影响。"""
    cum_th, wc, pos_lim, tc_lim = params

    # 基线
    v11m = [s for s in samples if s['v11_covered']]
    v11c = sum(1 for s in v11m if (s['v11_rule']['pred_up'] == s['actual_up']))
    v11t = len(v11m)
    v11a = v11c / v11t * 100 if v11t else 0

    # 新增
    new = [s for s in samples if match_new_rule(s, cum_th, wc, pos_lim, tc_lim) and not s['v11_covered']]
    nc = sum(1 for s in new if s['actual_up'])
    nt = len(new)
    na = nc / nt * 100 if nt else 0

    # 合并
    ct = v11t + nt
    cc = v11c + nc
    ca = cc / ct * 100 if ct else 0

    print(f"\n  V11影响:")
    print(f"    基线: {v11t}样本, {v11a:.1f}%准确, {v11t/len(samples)*100:.2f}%覆盖")
    print(f"    新增: {nt}样本, {na:.1f}%准确, {nt/len(samples)*100:.2f}%覆盖")
    print(f"    合并: {ct}样本, {ca:.1f}%准确, {ct/len(samples)*100:.2f}%覆盖")
    print(f"    准确率变化: {ca - v11a:+.2f}pp, 覆盖率变化: {(ct-v11t)/len(samples)*100:+.2f}pp")


def overlap_analysis(samples, params):
    """重叠/冲突分析。"""
    cum_th, wc, pos_lim, tc_lim = params
    all_m = [s for s in samples if match_new_rule(s, cum_th, wc, pos_lim, tc_lim)]
    covered = [s for s in all_m if s['v11_covered']]
    uncovered = [s for s in all_m if not s['v11_covered']]

    print(f"\n  重叠分析:")
    print(f"    总命中: {len(all_m)}, 已被V11覆盖: {len(covered)}, 新增: {len(uncovered)}")

    if covered:
        rc = defaultdict(int)
        for s in covered:
            rc[s['v11_rule']['name']] += 1
        print(f"    被覆盖的V11规则:")
        for rn, cnt in sorted(rc.items(), key=lambda x: -x[1])[:8]:
            print(f"      {rn}: {cnt}")

    # 冲突: V11预测跌但新规则预测涨
    conflicts = [s for s in covered if not s['v11_rule']['pred_up']]
    if conflicts:
        ca = sum(1 for s in conflicts if s['actual_up'])
        print(f"    ⚠️ 冲突(V11跌 vs 新规则涨): {len(conflicts)}样本, 实际涨{ca}({_pct(ca, len(conflicts))}%)")


def weekly_distribution(samples, params):
    """按周触发分布。"""
    cum_th, wc, pos_lim, tc_lim = params
    ws = defaultdict(lambda: [0, 0])
    for s in samples:
        if match_new_rule(s, cum_th, wc, pos_lim, tc_lim) and not s['v11_covered']:
            ws[s['iw_this']][0] += 1
            if s['actual_up']:
                ws[s['iw_this']][1] += 1

    print(f"\n  按周分布:")
    for wk in sorted(ws.keys()):
        t, c = ws[wk]
        print(f"    {wk[0]}-W{wk[1]:02d}: {t:>4}触发, {c:>4}正确, {_pct(c,t):>5.1f}%")


def main():
    logger.info("加载数据...")
    latest_date, dt_end, stock_klines, mkt_by_week = load_all_data()

    logger.info("构建样本...")
    samples = build_samples(stock_klines, mkt_by_week, dt_end)

    # 1. 网格搜索
    results = grid_search(samples)

    # 选最优(新增准确率>58%且样本>50)
    top = [r for r in results if r['new_acc'] >= 58 and r['new_total'] >= 50]
    if not top:
        top = [r for r in results if r['new_total'] >= 30][:5]

    # 2. 详细分析top规则
    print("\n" + "=" * 80)
    print("  2. 最优候选规则详细分析")
    print("=" * 80)
    for i, r in enumerate(top[:5]):
        p = (r['cum_th'], r['wc'], r['pos_lim'], r['tc_lim'])
        detailed_analysis(samples, p, f'候选#{i+1}')

    # 3. 对最优规则做CV + 影响分析
    if top:
        best = top[0]
        bp = (best['cum_th'], best['wc'], best['pos_lim'], best['tc_lim'])
        print(f"\n{'='*80}")
        print(f"  3. 最优规则深入分析: 累计{best['cum_th']}%/{best['wc']}周/pos<{best['pos_lim']}/本周<{best['tc_lim']}%")
        print(f"{'='*80}")
        time_series_cv(samples, bp)
        v11_impact(samples, bp)
        overlap_analysis(samples, bp)
        weekly_distribution(samples, bp)

        # 备选
        for i, alt in enumerate(top[1:4]):
            ap = (alt['cum_th'], alt['wc'], alt['pos_lim'], alt['tc_lim'])
            print(f"\n  ── 备选#{i+2}: 累计{alt['cum_th']}%/{alt['wc']}周/pos<{alt['pos_lim']}/本周<{alt['tc_lim']}% ──")
            print(f"  新增: {alt['new_total']}样本, {alt['new_acc']:.1f}%")
            time_series_cv(samples, ap)

    print(f"\n{'='*80}")
    print("  分析完成")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()
