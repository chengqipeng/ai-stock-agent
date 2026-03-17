#!/usr/bin/env python3
"""
V4全场景规则回测V3 - 精选高胜率规则
====================================
V4v2回测发现：
  - R5-R8(新增涨信号)在R1-R4之后准确率大幅下降(因为高准确率样本被R1-R4吃掉)
  - R9(深证+大盘跌+涨>5%→跌)=73.0% 很好
  - R10(跌+连涨+非高位→跌)=68.6% 不错
  - R14(大涨+偏高+尾日冲高→跌)=54.7% 太差

V4v3策略：
  1. 保留R1-R4(已验证的涨信号)
  2. 移除R5-R8(涨信号在非大盘深跌时无效)
  3. 保留R9(深证跌信号)，扩展到更多场景
  4. 保留R10(趋势反转跌信号)
  5. 新增：分析非大盘深跌时的有效规则
  6. 移除R14(准确率太低)

新增分析：在R1-R4未命中的样本中搜索有效规则

用法：
    python -m day_week_predicted.backtest.nw_v4_rules_backtest_v3
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

# V3基线规则（R1-R4）
V3_RULES = [
    {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
    {'name': 'R2:上证+大盘跌+跌>5%+非高位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -5 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and not (f['pos60'] is not None and f['pos60'] >= 0.7))},
    {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['prev_chg'] is not None and f['prev_chg'] < -2
                         and not (f['pos60'] is not None and f['pos60'] >= 0.8))},
    {'name': 'R4:上证+大盘跌+跌>3%+低位→涨', 'pred_up': True, 'tier': 1,
     'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                         and -3 <= f['mkt_chg'] < -1
                         and f['pos60'] is not None and f['pos60'] < 0.2)},
]


def load_data(n_weeks):
    """加载数据并构建样本"""
    latest_date = _get_latest_trade_date()
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(n_weeks + 2) * 7 + 180)
    start_date = dt_start.strftime('%Y-%m-%d')
    dt_cutoff = dt_end - timedelta(days=n_weeks * 7 + 14)

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

    # 构建样本
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

            last_day = this_pcts[-1] if this_pcts else 0

            samples.append({
                'code': code, 'suffix': suffix,
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'cd': cd, 'cu': cu, 'vol_ratio': vol_ratio,
                'actual_up': actual_up, 'next_chg': next_chg,
                'last_day': last_day,
            })

    return samples


def analyze_residual(samples):
    """在R1-R4未命中的样本中搜索有效规则"""
    # 先过滤掉R1-R4命中的样本
    residual = []
    for s in samples:
        feat = s
        matched = False
        for rule in V3_RULES:
            if rule['check'](feat):
                matched = True
                break
        if not matched:
            residual.append(s)

    total = len(residual)
    base_up = sum(1 for s in residual if s['actual_up'])
    print(f"\n{'='*80}")
    print(f"  R1-R4未命中的残差样本分析")
    print(f"{'='*80}")
    print(f"  残差样本: {total} (原始{len(samples)}, R1-R4命中{len(samples)-total})")
    print(f"  残差基准涨率: {base_up/total*100:.1f}%")

    # 按大盘环境 × 市场 分析
    print(f"\n  ── 残差样本: 大盘环境 × 市场 ──")
    mkt_envs = [
        ('大盘跌>3%', lambda s: s['mkt_chg'] < -3),
        ('大盘跌1~3%', lambda s: -3 <= s['mkt_chg'] < -1),
        ('大盘微跌', lambda s: -1 <= s['mkt_chg'] < 0),
        ('大盘微涨', lambda s: 0 <= s['mkt_chg'] < 1),
        ('大盘涨1~3%', lambda s: 1 <= s['mkt_chg'] < 3),
        ('大盘涨>3%', lambda s: s['mkt_chg'] >= 3),
    ]
    for mn, mf in mkt_envs:
        for sfx in ['SH', 'SZ']:
            ss = [s for s in residual if mf(s) and s['suffix'] == sfx]
            if len(ss) >= 50:
                up = sum(1 for s in ss if s['actual_up'])
                print(f"    {mn}+{sfx}: 涨率{up/len(ss)*100:.1f}% ({up}/{len(ss)})")

    # 在残差中搜索高胜率组合
    print(f"\n  ── 残差中的高胜率组合 (≥63%涨率或≤37%涨率, 样本≥200) ──")

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

    extra_conds = [
        ('低位<0.15', lambda s: s['pos60'] is not None and s['pos60'] < 0.15),
        ('低位<0.2', lambda s: s['pos60'] is not None and s['pos60'] < 0.2),
        ('低位<0.3', lambda s: s['pos60'] is not None and s['pos60'] < 0.3),
        ('非高位<0.6', lambda s: s['pos60'] is not None and s['pos60'] < 0.6),
        ('偏高>0.6', lambda s: s['pos60'] is not None and s['pos60'] > 0.6),
        ('高位>0.7', lambda s: s['pos60'] is not None and s['pos60'] > 0.7),
        ('高位>0.8', lambda s: s['pos60'] is not None and s['pos60'] > 0.8),
        ('前周大跌<-5%', lambda s: s['prev_chg'] is not None and s['prev_chg'] < -5),
        ('前周跌<-3%', lambda s: s['prev_chg'] is not None and s['prev_chg'] < -3),
        ('前周跌<-2%', lambda s: s['prev_chg'] is not None and s['prev_chg'] < -2),
        ('前周涨>2%', lambda s: s['prev_chg'] is not None and s['prev_chg'] > 2),
        ('前周涨>3%', lambda s: s['prev_chg'] is not None and s['prev_chg'] > 3),
        ('前周大涨>5%', lambda s: s['prev_chg'] is not None and s['prev_chg'] > 5),
        ('极缩量<0.5', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] < 0.5),
        ('缩量<0.7', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] < 0.7),
        ('放量>1.5', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] > 1.5),
        ('放量>2.0', lambda s: s['vol_ratio'] is not None and s['vol_ratio'] > 2.0),
        ('连跌≥3天', lambda s: s['cd'] >= 3),
        ('连跌≥4天', lambda s: s['cd'] >= 4),
        ('连涨≥3天', lambda s: s['cu'] >= 3),
        ('连涨≥4天', lambda s: s['cu'] >= 4),
        ('尾日跌>2%', lambda s: s['last_day'] < -2),
        ('尾日跌>3%', lambda s: s['last_day'] < -3),
        ('尾日涨>2%', lambda s: s['last_day'] > 2),
        ('尾日涨>3%', lambda s: s['last_day'] > 3),
    ]

    suffix_conds = [
        ('上证', lambda s: s['suffix'] == 'SH'),
        ('深证', lambda s: s['suffix'] == 'SZ'),
        ('不限', lambda s: True),
    ]

    results = []
    for mn, mf in mkt_envs + [('不限大盘', lambda s: True)]:
        ms = [s for s in residual if mf(s)]
        if len(ms) < 100:
            continue
        for sfn, sff in suffix_conds:
            ss = [s for s in ms if sff(s)]
            if len(ss) < 100:
                continue
            for stn, stf in stk_conds:
                st = [s for s in ss if stf(s)]
                if len(st) < 50:
                    continue
                # 单条件
                up = sum(1 for s in st if s['actual_up'])
                rate = up / len(st) * 100
                if (rate >= 63 or rate <= 37) and len(st) >= 200:
                    results.append((rate, len(st), f"{mn}+{sfn}+{stn}", rate >= 63))
                # 双条件
                for en, ef in extra_conds:
                    combo = [s for s in st if ef(s)]
                    if len(combo) < 200:
                        continue
                    up2 = sum(1 for s in combo if s['actual_up'])
                    rate2 = up2 / len(combo) * 100
                    if rate2 >= 63 or rate2 <= 37:
                        results.append((rate2, len(combo), f"{mn}+{sfn}+{stn}+{en}", rate2 >= 63))

    seen = set()
    unique = []
    for r in results:
        if r[2] not in seen:
            seen.add(r[2])
            unique.append(r)

    up_r = sorted([r for r in unique if r[3]], key=lambda x: (-x[0], -x[1]))
    print(f"\n  预测涨 (≥63%): 共{len(up_r)}条")
    for rate, n, desc, _ in up_r[:50]:
        print(f"    {rate:5.1f}% ({n:>5d}样本)  {desc}")

    dn_r = sorted([r for r in unique if not r[3]], key=lambda x: (x[0], -x[1]))
    print(f"\n  预测跌 (≤37%): 共{len(dn_r)}条")
    for rate, n, desc, _ in dn_r[:50]:
        print(f"    {rate:5.1f}% ({n:>5d}样本)  {desc}")

    return residual


def run_v4v3_backtest(samples):
    """用精选规则回测"""
    # V4v3规则：R1-R4 + 精选新规则
    V4V3_RULES = [
        # ── 涨信号(已验证) ──
        {'name': 'R1:大盘深跌+个股跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: f['this_chg'] < -2 and f['mkt_chg'] < -3},
        {'name': 'R2:上证+大盘跌+跌>5%+非高位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['this_chg'] < -5 and f['suffix'] == 'SH'
                             and -3 <= f['mkt_chg'] < -1
                             and not (f['pos60'] is not None and f['pos60'] >= 0.7))},
        {'name': 'R3:上证+大盘跌+跌>3%+前周跌→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                             and -3 <= f['mkt_chg'] < -1
                             and f['prev_chg'] is not None and f['prev_chg'] < -2
                             and not (f['pos60'] is not None and f['pos60'] >= 0.8))},
        {'name': 'R4:上证+大盘跌+跌>3%+低位→涨', 'pred_up': True, 'tier': 1,
         'check': lambda f: (f['this_chg'] < -3 and f['suffix'] == 'SH'
                             and -3 <= f['mkt_chg'] < -1
                             and f['pos60'] is not None and f['pos60'] < 0.2)},

        # ── 跌信号(新增) ──
        # R5: 深证+大盘跌1~3%+个股涨>5% → 跌 (73.0%, 1292样本)
        {'name': 'R5:深证+大盘跌+涨>5%→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ'
                             and -3 <= f['mkt_chg'] < -1
                             and f['this_chg'] > 5)},

        # R6: 深证+大盘跌1~3%+个股涨>2%+连涨≥4天 → 跌 (72.5%)
        {'name': 'R6:深证+大盘跌+涨+连涨→跌', 'pred_up': False, 'tier': 1,
         'check': lambda f: (f['suffix'] == 'SZ'
                             and -3 <= f['mkt_chg'] < -1
                             and f['this_chg'] > 2
                             and f['cu'] >= 4)},

        # R7: 跌>3%+连涨≥3天+非高位<0.6 → 跌 (68.5%, 340样本)
        {'name': 'R7:跌+前期连涨+非高位→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['this_chg'] < -3
                             and f['cu'] >= 3
                             and f['pos60'] is not None and f['pos60'] < 0.6)},

        # R8: 涨>10%+尾日跌>3%+前周涨>3% → 跌 (63.8%)
        {'name': 'R8:大涨+尾日回落+前周涨→跌', 'pred_up': False, 'tier': 2,
         'check': lambda f: (f['this_chg'] > 10
                             and f['last_day'] < -3
                             and f['prev_chg'] is not None and f['prev_chg'] > 3)},
    ]

    all_weeks = 0
    by_rule = defaultdict(lambda: {'correct': 0, 'total': 0})
    by_tier = defaultdict(lambda: {'correct': 0, 'total': 0})
    total_pred = 0
    total_correct = 0
    v3_pred = 0
    v3_correct = 0
    by_suffix = defaultdict(lambda: {'pred': 0, 'correct': 0, 'total': 0})

    for s in samples:
        all_weeks += 1
        by_suffix[s['suffix']]['total'] += 1

        matched = None
        for rule in V4V3_RULES:
            if rule['check'](s):
                matched = rule
                break

        if matched:
            is_correct = matched['pred_up'] == s['actual_up']
            total_pred += 1
            if is_correct:
                total_correct += 1
            by_rule[matched['name']]['total'] += 1
            if is_correct:
                by_rule[matched['name']]['correct'] += 1
            by_tier[matched['tier']]['total'] += 1
            if is_correct:
                by_tier[matched['tier']]['correct'] += 1
            by_suffix[s['suffix']]['pred'] += 1
            if is_correct:
                by_suffix[s['suffix']]['correct'] += 1

        # V3对比
        for rule in V3_RULES:
            if rule['check'](s):
                is_c = rule['pred_up'] == s['actual_up']
                v3_pred += 1
                if is_c:
                    v3_correct += 1
                break

    _p = lambda c, t: f"{c / t * 100:.1f}%" if t > 0 else "N/A"

    print(f"\n{'='*80}")
    print(f"  V4v3精选规则回测结果")
    print(f"{'='*80}")
    print(f"  总可评估周数: {all_weeks}")
    print(f"  V4v3预测: {_p(total_correct, total_pred)} ({total_correct}/{total_pred}) "
          f"覆盖{_p(total_pred, all_weeks)}")
    print(f"  V3对比:   {_p(v3_correct, v3_pred)} ({v3_correct}/{v3_pred}) "
          f"覆盖{_p(v3_pred, all_weeks)}")

    print(f"\n  ── 按Tier ──")
    for t in sorted(by_tier.keys()):
        s = by_tier[t]
        print(f"    Tier {t}: {_p(s['correct'], s['total'])} ({s['correct']}/{s['total']})")

    print(f"\n  ── 按规则 ──")
    for rn in sorted(by_rule.keys()):
        s = by_rule[rn]
        print(f"    {rn:<45s} {_p(s['correct'], s['total'])} ({s['correct']}/{s['total']})")

    print(f"\n  ── 按市场 ──")
    for sfx in sorted(by_suffix.keys()):
        s = by_suffix[sfx]
        print(f"    {sfx}: 预测{_p(s['correct'], s['pred'])} ({s['correct']}/{s['pred']}) "
              f"覆盖{_p(s['pred'], s['total'])}")

    print(f"\n  ── 改进 ──")
    if v3_pred > 0 and total_pred > 0:
        v3_acc = v3_correct / v3_pred * 100
        v4_acc = total_correct / total_pred * 100
        print(f"    准确率: V3 {v3_acc:.1f}% → V4v3 {v4_acc:.1f}% ({v4_acc-v3_acc:+.1f}%)")
        print(f"    覆盖率: V3 {v3_pred/all_weeks*100:.1f}% → V4v3 {total_pred/all_weeks*100:.1f}% "
              f"({(total_pred-v3_pred)/all_weeks*100:+.1f}%)")
        print(f"    预测数: V3 {v3_pred} → V4v3 {total_pred} ({total_pred-v3_pred:+d})")

    return V4V3_RULES


if __name__ == '__main__':
    logger.info("加载数据...")
    samples = load_data(N_WEEKS)
    logger.info("样本数: %d", len(samples))

    # 先分析残差
    residual = analyze_residual(samples)

    # 再用精选规则回测
    run_v4v3_backtest(samples)
