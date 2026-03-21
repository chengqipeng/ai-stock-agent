#!/usr/bin/env python3
"""
分析"连续多周下跌→超跌反弹"新规则的影响面
==============================================
目标: 评估在V11引擎中新增一条规则的全面影响:
  "连续2-3周累计跌幅超过一定阈值 + 非极高位 → 预测下周涨"

分析维度:
1. 候选规则的准确率、样本量、覆盖率
2. 与现有V11规则的重叠/冲突分析
3. 不同参数组合的网格搜索
4. 按市场环境(大盘涨/跌)分层分析
5. 按价格位置分层分析
6. 时间序列交叉验证(防过拟合)
7. 对V11整体准确率和覆盖率的影响
"""
import sys
import os
import logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _to_float, _compound_return, _get_stock_index,
    _nw_extract_features, _nw_match_rule,
    _NW_V11_ENGINE, _NW_V11_LAYERS,
    _get_latest_trade_date,
)

N_WEEKS = 29


def _mean(lst):
    return sum(lst) / len(lst) if lst else 0


def _pct(c, t):
    return round(c / t * 100, 1) if t > 0 else 0


def load_data():
    """加载全量A股K线数据用于回测。"""
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 获取全部A股代码
    cur.execute(
        "SELECT DISTINCT stock_code FROM stock_kline "
        "WHERE (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ') "
        "AND stock_code NOT LIKE '399%%' AND stock_code != '000001.SH' "
        "AND `date` >= %s LIMIT 99999", [latest_date])
    all_codes = [r['stock_code'] for r in cur.fetchall()]
    logger.info(f"股票数: {len(all_codes)}, 日期范围: {start_date} ~ {latest_date}")

    # 个股K线
    stock_klines = defaultdict(list)
    bs = 2000
    for i in range(0, len(all_codes), bs):
        batch = all_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code,`date`,open_price,close_price,high_price,low_price,"
            f"change_percent,trading_volume FROM stock_kline "
            f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s "
            f"ORDER BY stock_code,`date`",
            batch + [start_date, latest_date])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': r['date'],
                'open': _to_float(r['open_price']),
                'close': _to_float(r['close_price']),
                'high': _to_float(r['high_price']),
                'low': _to_float(r['low_price']),
                'change_percent': _to_float(r['change_percent']),
                'volume': _to_float(r['trading_volume']),
            })
        logger.info(f"  K线: {min(i+bs, len(all_codes))}/{len(all_codes)} ...")

    # 大盘指数K线
    idx_codes = ['000001.SH', '399001.SZ', '899050.SZ']
    ph = ','.join(['%s'] * len(idx_codes))
    cur.execute(
        f"SELECT stock_code,`date`,change_percent FROM stock_kline "
        f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s ORDER BY `date`",
        idx_codes + [start_date, latest_date])
    mkt_kl = defaultdict(list)
    for r in cur.fetchall():
        mkt_kl[r['stock_code']].append({
            'date': r['date'], 'change_percent': _to_float(r['change_percent']),
        })

    conn.close()

    # 指数按周分组
    mkt_by_week = {}
    for ic, kl in mkt_kl.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mkt_by_week[ic] = bw

    return latest_date, dt_end, all_codes, stock_klines, mkt_by_week


def build_samples(all_codes, stock_klines, mkt_by_week, dt_end):
    """构建全量样本，包含多周累计跌幅特征。"""
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)
    samples = []

    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 60:
            continue

        stock_idx = _get_stock_index(code)
        suffix = stock_idx.split('.')[-1] if '.' in stock_idx else ''
        idx_bw = mkt_by_week.get(stock_idx, {})

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

            # 前周/前两周/前三周涨跌
            prev_chg = None
            if i > 0:
                prev_iw = sorted_weeks[i - 1]
                prev_days = sorted(wg[prev_iw], key=lambda x: x['date'])
                if len(prev_days) >= 3:
                    prev_chg = _compound_return([d['change_percent'] for d in prev_days])

            prev2_chg = None
            if i >= 2:
                prev2_iw = sorted_weeks[i - 2]
                prev2_days = sorted(wg[prev2_iw], key=lambda x: x['date'])
                if len(prev2_days) >= 3:
                    prev2_chg = _compound_return([d['change_percent'] for d in prev2_days])

            prev3_chg = None
            if i >= 3:
                prev3_iw = sorted_weeks[i - 3]
                prev3_days = sorted(wg[prev3_iw], key=lambda x: x['date'])
                if len(prev3_days) >= 3:
                    prev3_chg = _compound_return([d['change_percent'] for d in prev3_days])

            # 连涨连跌
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

            last_day = this_pcts[-1] if this_pcts else 0

            # 量比
            vol_ratio = None
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at = _mean(tv)
                ah = _mean(hv)
                if ah > 0:
                    vol_ratio = at / ah

            # 2周累计跌幅
            cum_2w = None
            if prev_chg is not None:
                cum_2w = this_chg + prev_chg

            # 3周累计跌幅
            cum_3w = None
            if prev_chg is not None and prev2_chg is not None:
                cum_3w = this_chg + prev_chg + prev2_chg

            # 连续下跌周数
            consec_down_weeks = 0
            if this_chg < 0:
                consec_down_weeks += 1
            if prev_chg is not None and prev_chg < 0 and consec_down_weeks > 0:
                consec_down_weeks += 1
            if prev2_chg is not None and prev2_chg < 0 and consec_down_weeks > 1:
                consec_down_weeks += 1
            if prev3_chg is not None and prev3_chg < 0 and consec_down_weeks > 2:
                consec_down_weeks += 1

            # V11 特征(用于检查现有规则是否已覆盖)
            feat = {
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'suffix': suffix, 'pos60': pos60,
                'prev_chg': prev_chg, 'prev2_chg': prev2_chg,
                'mkt_last_day': mkt_last_day,
                'vol_ratio': vol_ratio,
                'turnover_ratio': None,
                'board_momentum': None, 'concept_consensus': None,
                'big_net_pct_avg': None,
                'rush_up_pullback': False, 'dip_recovery': False,
                'upper_shadow_ratio': None,
            }
            # 冲高回落/探底回升
            if len(this_pcts) >= 4:
                mid = len(this_pcts) // 2
                fh = _compound_return(this_pcts[:mid])
                sh = _compound_return(this_pcts[mid:])
                if fh > 2 and sh < -1:
                    feat['rush_up_pullback'] = True
                if fh < -2 and sh > 1:
                    feat['dip_recovery'] = True

            # 检查V11是否已覆盖
            v11_rule = _nw_match_rule(feat)

            samples.append({
                'code': code, 'suffix': suffix,
                'iw_this': iw_this, 'iw_next': iw_next,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'prev2_chg': prev2_chg, 'prev3_chg': prev3_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'vol_ratio': vol_ratio,
                'mkt_last_day': mkt_last_day,
                'cum_2w': cum_2w, 'cum_3w': cum_3w,
                'consec_down_weeks': consec_down_weeks,
                'next_chg': next_chg, 'actual_up': next_chg >= 0,
                'v11_rule': v11_rule,
                'v11_covered': v11_rule is not None,
                'feat': feat,
            })

    logger.info(f"总样本数: {len(samples)}")
    return samples


def analyze_candidate_rules(samples):
    """网格搜索候选规则参数，评估准确率和覆盖率。"""
    print("\n" + "=" * 80)
    print("  1. 候选规则网格搜索")
    print("=" * 80)

    # 参数空间
    cum_thresholds = [-8, -10, -12, -15]  # 累计跌幅阈值
    week_counts = [2, 3]                   # 连续下跌周数
    pos_limits = [0.5, 0.6, 0.7, 0.8, 1.0]  # 价格位置上限(排除极高位)
    this_chg_limits = [-2, -3, -4, 0]      # 本周跌幅要求

    results = []

    for cum_th in cum_thresholds:
        for wc in week_counts:
            for pos_lim in pos_limits:
                for tc_lim in this_chg_limits:
                    matched = []
                    matched_new = []  # V11未覆盖的新增样本

                    for s in samples:
                        # 基本条件: 本周跌
                        if s['this_chg'] >= tc_lim:
                            continue

                        # 价格位置限制
                        if pos_lim < 1.0 and (s['pos60'] is None or s['pos60'] >= pos_lim):
                            continue

                        # 连续下跌周数
                        if s['consec_down_weeks'] < wc:
                            continue

                        # 累计跌幅
                        if wc == 2 and (s['cum_2w'] is None or s['cum_2w'] >= cum_th):
                            continue
                        if wc == 3 and (s['cum_3w'] is None or s['cum_3w'] >= cum_th):
                            continue

                        matched.append(s)
                        if not s['v11_covered']:
                            matched_new.append(s)

                    if len(matched) < 30:
                        continue

                    total = len(matched)
                    correct = sum(1 for s in matched if s['actual_up'])
                    acc = correct / total * 100

                    new_total = len(matched_new)
                    new_correct = sum(1 for s in matched_new if s['actual_up'])
                    new_acc = new_correct / new_total * 100 if new_total > 0 else 0

                    results.append({
                        'cum_th': cum_th, 'wc': wc, 'pos_lim': pos_lim,
                        'tc_lim': tc_lim,
                        'total': total, 'correct': correct, 'acc': acc,
                        'new_total': new_total, 'new_correct': new_correct,
                        'new_acc': new_acc,
                        'coverage': total / len(samples) * 100,
                        'new_coverage': new_total / len(samples) * 100,
                    })

    # 按新增样本准确率排序
    results.sort(key=lambda x: (-x['new_acc'], -x['new_total']))

    print(f"\n  总样本: {len(samples)}")
    print(f"\n  {'累计跌幅':>8} {'周数':>4} {'pos上限':>7} {'本周跌':>6} | "
          f"{'全量':>5} {'准确率':>6} | {'新增':>5} {'新增准确率':>8} {'新增覆盖':>7}")
    print("  " + "-" * 85)

    shown = 0
    for r in results:
        if r['new_total'] < 20:
            continue
        if r['new_acc'] < 55:
            continue
        print(f"  {r['cum_th']:>7}% {r['wc']:>4}周 {r['pos_lim']:>6.1f} {r['tc_lim']:>5}% | "
              f"{r['total']:>5} {r['acc']:>5.1f}% | "
              f"{r['new_total']:>5} {r['new_acc']:>7.1f}% {r['new_coverage']:>6.2f}%")
        shown += 1
        if shown >= 30:
            break

    return results


def analyze_best_rules_detail(samples, top_rules):
    """对最优候选规则进行详细分析。"""
    print("\n" + "=" * 80)
    print("  2. 最优候选规则详细分析")
    print("=" * 80)

    for idx, r in enumerate(top_rules[:5]):
        cum_th = r['cum_th']
        wc = r['wc']
        pos_lim = r['pos_lim']
        tc_lim = r['tc_lim']

        matched_new = []
        for s in samples:
            if s['this_chg'] >= tc_lim:
                continue
            if pos_lim < 1.0 and (s['pos60'] is None or s['pos60'] >= pos_lim):
                continue
            if s['consec_down_weeks'] < wc:
                continue
            if wc == 2 and (s['cum_2w'] is None or s['cum_2w'] >= cum_th):
                continue
            if wc == 3 and (s['cum_3w'] is None or s['cum_3w'] >= cum_th):
                continue
            if not s['v11_covered']:
                matched_new.append(s)

        if not matched_new:
            continue

        print(f"\n  ── 候选规则 #{idx+1}: 累计{cum_th}% / {wc}周 / pos<{pos_lim} / 本周<{tc_lim}% ──")
        print(f"  新增样本: {len(matched_new)}, 准确率: {_pct(sum(1 for s in matched_new if s['actual_up']), len(matched_new))}%")

        # 按大盘环境分层
        mkt_up = [s for s in matched_new if s['mkt_chg'] >= 0]
        mkt_down = [s for s in matched_new if s['mkt_chg'] < 0]
        print(f"\n  按大盘环境:")
        if mkt_up:
            acc_up = _pct(sum(1 for s in mkt_up if s['actual_up']), len(mkt_up))
            print(f"    大盘涨(≥0): {len(mkt_up)}样本, 准确率{acc_up}%")
        if mkt_down:
            acc_dn = _pct(sum(1 for s in mkt_down if s['actual_up']), len(mkt_down))
            print(f"    大盘跌(<0): {len(mkt_down)}样本, 准确率{acc_dn}%")

        # 按市场(SH/SZ)分层
        sh = [s for s in matched_new if s['suffix'] == 'SH']
        sz = [s for s in matched_new if s['suffix'] == 'SZ']
        print(f"\n  按市场:")
        if sh:
            print(f"    上证: {len(sh)}样本, 准确率{_pct(sum(1 for s in sh if s['actual_up']), len(sh))}%")
        if sz:
            print(f"    深证: {len(sz)}样本, 准确率{_pct(sum(1 for s in sz if s['actual_up']), len(sz))}%")

        # 按价格位置分层
        print(f"\n  按价格位置:")
        for lo, hi, label in [(0, 0.2, '<0.2'), (0.2, 0.4, '0.2~0.4'),
                               (0.4, 0.6, '0.4~0.6'), (0.6, 0.8, '0.6~0.8'),
                               (0.8, 1.01, '≥0.8')]:
            sub = [s for s in matched_new if s['pos60'] is not None and lo <= s['pos60'] < hi]
            if sub:
                acc = _pct(sum(1 for s in sub if s['actual_up']), len(sub))
                print(f"    pos {label}: {len(sub)}样本, 准确率{acc}%")

        # 下周实际涨跌幅分布
        next_chgs = [s['next_chg'] for s in matched_new]
        print(f"\n  下周涨跌幅分布:")
        print(f"    均值: {_mean(next_chgs):+.2f}%")
        print(f"    中位数: {sorted(next_chgs)[len(next_chgs)//2]:+.2f}%")
        print(f"    最大涨: {max(next_chgs):+.2f}%")
        print(f"    最大跌: {min(next_chgs):+.2f}%")

        # 英维克W11是否被覆盖
        yingweike = [s for s in matched_new if s['code'] == '002837.SZ'
                     and s['iw_this'] == (2026, 11)]
        if yingweike:
            s = yingweike[0]
            print(f"\n  ★ 英维克W11: 命中! 下周实际{s['next_chg']:+.2f}% ({'✓' if s['actual_up'] else '✗'})")
        else:
            print(f"\n  ★ 英维克W11: 未命中")


def time_series_cv(samples, rule_params, n_folds=5):
    """时间序列交叉验证，防止过拟合。"""
    print("\n" + "=" * 80)
    print("  3. 时间序列交叉验证 (防过拟合)")
    print("=" * 80)

    cum_th, wc, pos_lim, tc_lim = rule_params

    # 按周排序所有样本
    all_weeks = sorted(set(s['iw_this'] for s in samples))
    fold_size = len(all_weeks) // n_folds

    fold_results = []
    for fold in range(n_folds):
        test_start = fold * fold_size
        test_end = test_start + fold_size if fold < n_folds - 1 else len(all_weeks)
        test_weeks = set(all_weeks[test_start:test_end])

        test_samples = [s for s in samples if s['iw_this'] in test_weeks]
        matched = []
        for s in test_samples:
            if s['this_chg'] >= tc_lim:
                continue
            if pos_lim < 1.0 and (s['pos60'] is None or s['pos60'] >= pos_lim):
                continue
            if s['consec_down_weeks'] < wc:
                continue
            if wc == 2 and (s['cum_2w'] is None or s['cum_2w'] >= cum_th):
                continue
            if wc == 3 and (s['cum_3w'] is None or s['cum_3w'] >= cum_th):
                continue
            if not s['v11_covered']:
                matched.append(s)

        if matched:
            correct = sum(1 for s in matched if s['actual_up'])
            acc = correct / len(matched) * 100
            fold_results.append({
                'fold': fold + 1, 'weeks': f"W{all_weeks[test_start][1]:02d}~W{all_weeks[min(test_end-1, len(all_weeks)-1)][1]:02d}",
                'total': len(matched), 'correct': correct, 'acc': acc,
            })

    print(f"\n  规则: 累计{cum_th}% / {wc}周 / pos<{pos_lim} / 本周<{tc_lim}%")
    print(f"\n  {'Fold':>6} {'周范围':>15} {'样本':>6} {'正确':>6} {'准确率':>7}")
    print("  " + "-" * 45)
    for fr in fold_results:
        print(f"  {fr['fold']:>6} {fr['weeks']:>15} {fr['total']:>6} {fr['correct']:>6} {fr['acc']:>6.1f}%")

    if fold_results:
        accs = [fr['acc'] for fr in fold_results]
        avg_acc = _mean(accs)
        min_acc = min(accs)
        max_acc = max(accs)
        print(f"\n  CV均值: {avg_acc:.1f}%  最低: {min_acc:.1f}%  最高: {max_acc:.1f}%")
        print(f"  稳定性(max-min): {max_acc - min_acc:.1f}pp")
        return avg_acc
    return 0


def analyze_v11_impact(samples, rule_params):
    """分析新规则对V11整体准确率和覆盖率的影响。"""
    print("\n" + "=" * 80)
    print("  4. 对V11整体的影响分析")
    print("=" * 80)

    cum_th, wc, pos_lim, tc_lim = rule_params

    # 基线: 现有V11
    v11_matched = [s for s in samples if s['v11_covered']]
    v11_correct = sum(1 for s in v11_matched if
                      (s['v11_rule']['pred_up'] and s['actual_up']) or
                      (not s['v11_rule']['pred_up'] and not s['actual_up']))
    v11_total = len(v11_matched)
    v11_acc = v11_correct / v11_total * 100 if v11_total > 0 else 0
    v11_coverage = v11_total / len(samples) * 100

    # 新规则命中的样本(仅V11未覆盖的)
    new_matched = []
    for s in samples:
        if s['v11_covered']:
            continue
        if s['this_chg'] >= tc_lim:
            continue
        if pos_lim < 1.0 and (s['pos60'] is None or s['pos60'] >= pos_lim):
            continue
        if s['consec_down_weeks'] < wc:
            continue
        if wc == 2 and (s['cum_2w'] is None or s['cum_2w'] >= cum_th):
            continue
        if wc == 3 and (s['cum_3w'] is None or s['cum_3w'] >= cum_th):
            continue
        new_matched.append(s)

    new_correct = sum(1 for s in new_matched if s['actual_up'])
    new_total = len(new_matched)
    new_acc = new_correct / new_total * 100 if new_total > 0 else 0

    # 合并后
    combined_total = v11_total + new_total
    combined_correct = v11_correct + new_correct
    combined_acc = combined_correct / combined_total * 100 if combined_total > 0 else 0
    combined_coverage = combined_total / len(samples) * 100

    print(f"\n  {'指标':>20} {'V11基线':>10} {'新规则':>10} {'合并后':>10} {'变化':>10}")
    print("  " + "-" * 65)
    print(f"  {'覆盖样本':>20} {v11_total:>10} {new_total:>10} {combined_total:>10} {'+' + str(new_total):>10}")
    print(f"  {'正确预测':>20} {v11_correct:>10} {new_correct:>10} {combined_correct:>10} {'+' + str(new_correct):>10}")
    print(f"  {'准确率':>20} {v11_acc:>9.1f}% {new_acc:>9.1f}% {combined_acc:>9.1f}% {combined_acc - v11_acc:>+9.1f}%")
    print(f"  {'覆盖率':>20} {v11_coverage:>9.2f}% {new_total/len(samples)*100:>9.2f}% {combined_coverage:>9.2f}% {combined_coverage - v11_coverage:>+9.2f}%")

    # 按置信度分层(新规则作为tier2/reference)
    print(f"\n  新规则置信度建议: reference (Tier 2)")
    print(f"  理由: 新增规则CV准确率需>65%才建议high，否则reference")

    return {
        'v11_acc': v11_acc, 'v11_coverage': v11_coverage,
        'new_acc': new_acc, 'new_total': new_total,
        'combined_acc': combined_acc, 'combined_coverage': combined_coverage,
    }


def analyze_overlap_with_v11(samples, rule_params):
    """分析新规则与V11现有规则的重叠情况。"""
    print("\n" + "=" * 80)
    print("  5. 与V11现有规则的重叠/冲突分析")
    print("=" * 80)

    cum_th, wc, pos_lim, tc_lim = rule_params

    all_matched = []
    for s in samples:
        if s['this_chg'] >= tc_lim:
            continue
        if pos_lim < 1.0 and (s['pos60'] is None or s['pos60'] >= pos_lim):
            continue
        if s['consec_down_weeks'] < wc:
            continue
        if wc == 2 and (s['cum_2w'] is None or s['cum_2w'] >= cum_th):
            continue
        if wc == 3 and (s['cum_3w'] is None or s['cum_3w'] >= cum_th):
            continue
        all_matched.append(s)

    covered = [s for s in all_matched if s['v11_covered']]
    uncovered = [s for s in all_matched if not s['v11_covered']]

    print(f"\n  新规则总命中: {len(all_matched)}")
    print(f"  已被V11覆盖: {len(covered)} ({_pct(len(covered), len(all_matched))}%)")
    print(f"  V11未覆盖(新增): {len(uncovered)} ({_pct(len(uncovered), len(all_matched))}%)")

    if covered:
        # 分析被哪些V11规则覆盖
        rule_counts = defaultdict(int)
        rule_correct = defaultdict(int)
        for s in covered:
            rn = s['v11_rule']['name']
            rule_counts[rn] += 1
            pred_up = s['v11_rule']['pred_up']
            if (pred_up and s['actual_up']) or (not pred_up and not s['actual_up']):
                rule_correct[rn] += 1

        print(f"\n  被覆盖样本的V11规则分布:")
        for rn, cnt in sorted(rule_counts.items(), key=lambda x: -x[1]):
            acc = _pct(rule_correct[rn], cnt)
            print(f"    {rn}: {cnt}样本, V11准确率{acc}%")

        # 冲突分析: V11预测跌但新规则预测涨
        conflicts = [s for s in covered if not s['v11_rule']['pred_up']]
        if conflicts:
            print(f"\n  ⚠️ 冲突样本(V11预测跌 vs 新规则预测涨): {len(conflicts)}")
            conflict_actual_up = sum(1 for s in conflicts if s['actual_up'])
            print(f"    实际涨: {conflict_actual_up}/{len(conflicts)} ({_pct(conflict_actual_up, len(conflicts))}%)")
            print(f"    → 新规则正确率 > V11 说明新规则在此场景更优")


def analyze_weekly_distribution(samples, rule_params):
    """分析新规则在各周的触发分布。"""
    print("\n" + "=" * 80)
    print("  6. 按周触发分布")
    print("=" * 80)

    cum_th, wc, pos_lim, tc_lim = rule_params

    week_stats = defaultdict(lambda: {'total': 0, 'correct': 0})
    for s in samples:
        if s['v11_covered']:
            continue
        if s['this_chg'] >= tc_lim:
            continue
        if pos_lim < 1.0 and (s['pos60'] is None or s['pos60'] >= pos_lim):
            continue
        if s['consec_down_weeks'] < wc:
            continue
        if wc == 2 and (s['cum_2w'] is None or s['cum_2w'] >= cum_th):
            continue
        if wc == 3 and (s['cum_3w'] is None or s['cum_3w'] >= cum_th):
            continue

        wk = s['iw_this']
        week_stats[wk]['total'] += 1
        if s['actual_up']:
            week_stats[wk]['correct'] += 1

    print(f"\n  {'周':>12} {'触发数':>6} {'正确':>6} {'准确率':>7}")
    print("  " + "-" * 35)
    for wk in sorted(week_stats.keys()):
        ws = week_stats[wk]
        acc = _pct(ws['correct'], ws['total'])
        print(f"  {wk[0]}-W{wk[1]:02d} {ws['total']:>6} {ws['correct']:>6} {acc:>6.1f}%")


def main():
    logger.info("加载数据...")
    latest_date, dt_end, all_codes, stock_klines, mkt_by_week = load_data()

    logger.info("构建样本...")
    samples = build_samples(all_codes, stock_klines, mkt_by_week, dt_end)

    # 1. 网格搜索
    results = analyze_candidate_rules(samples)

    # 选出最优的几个规则(新增准确率>60%且新增样本>50)
    top_rules = [r for r in results if r['new_acc'] >= 58 and r['new_total'] >= 50]
    if not top_rules:
        top_rules = [r for r in results if r['new_total'] >= 30][:5]

    # 2. 详细分析
    analyze_best_rules_detail(samples, top_rules)

    # 3~6. 对最优规则进行深入分析
    if top_rules:
        best = top_rules[0]
        best_params = (best['cum_th'], best['wc'], best['pos_lim'], best['tc_lim'])
        print(f"\n{'='*80}")
        print(f"  选定最优规则: 累计{best['cum_th']}% / {best['wc']}周 / pos<{best['pos_lim']} / 本周<{best['tc_lim']}%")
        print(f"  新增准确率: {best['new_acc']:.1f}% ({best['new_correct']}/{best['new_total']})")
        print(f"{'='*80}")

        cv_acc = time_series_cv(samples, best_params)
        analyze_v11_impact(samples, best_params)
        analyze_overlap_with_v11(samples, best_params)
        analyze_weekly_distribution(samples, best_params)

        # 也测试几个备选
        print(f"\n{'='*80}")
        print(f"  备选规则对比")
        print(f"{'='*80}")
        for i, alt in enumerate(top_rules[1:4]):
            alt_params = (alt['cum_th'], alt['wc'], alt['pos_lim'], alt['tc_lim'])
            print(f"\n  ── 备选 #{i+2}: 累计{alt['cum_th']}% / {alt['wc']}周 / pos<{alt['pos_lim']} / 本周<{alt['tc_lim']}% ──")
            print(f"  新增: {alt['new_total']}样本, 准确率{alt['new_acc']:.1f}%")
            cv = time_series_cv(samples, alt_params)

    print(f"\n{'='*80}")
    print(f"  分析完成")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()
