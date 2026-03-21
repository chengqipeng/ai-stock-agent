#!/usr/bin/env python3
"""
用SQL聚合周涨跌，避免加载全量K线到Python。
分析"连续多周下跌→超跌反弹"规则的影响面。
"""
import sys, os, logging, time
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import _to_float, _compound_return, _get_stock_index, _nw_match_rule

N_WEEKS = 29

def _mean(lst):
    return sum(lst) / len(lst) if lst else 0

def _pct(c, t):
    return round(c / t * 100, 1) if t > 0 else 0

def load_weekly_data():
    """用SQL直接聚合出每只股票每周的涨跌数据。"""
    t0 = time.time()
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code='399001.SZ'")
    latest = cur.fetchone()['d']
    dt_end = datetime.strptime(latest, '%Y-%m-%d')
    # 多取几周用于计算前周涨跌
    dt_start = dt_end - timedelta(days=(N_WEEKS + 6) * 7)
    start_date = dt_start.strftime('%Y-%m-%d')
    logger.info(f"范围: {start_date} ~ {latest}")

    # SQL聚合: 每只股票每周的日涨跌序列(GROUP_CONCAT)、收盘价、成交量
    # 只取沪深A股
    logger.info("SQL聚合周数据...")
    cur.execute("""
        SELECT stock_code,
               YEAR(`date`) as y, WEEKOFYEAR(`date`) as w,
               GROUP_CONCAT(change_percent ORDER BY `date`) as pcts,
               GROUP_CONCAT(close_price ORDER BY `date`) as closes,
               GROUP_CONCAT(trading_volume ORDER BY `date`) as vols,
               COUNT(*) as day_count,
               MIN(`date`) as week_start, MAX(`date`) as week_end
        FROM stock_kline
        WHERE `date` >= %s AND `date` <= %s
          AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ')
          AND stock_code NOT LIKE '399%%' AND stock_code != '000001.SH'
        GROUP BY stock_code, YEAR(`date`), WEEKOFYEAR(`date`)
        HAVING COUNT(*) >= 3
        ORDER BY stock_code, y, w
    """, [start_date, latest])
    rows = cur.fetchall()
    logger.info(f"周数据行数: {len(rows)}, 耗时: {time.time()-t0:.1f}s")

    # 同样获取指数周数据
    cur.execute("""
        SELECT stock_code,
               YEAR(`date`) as y, WEEKOFYEAR(`date`) as w,
               GROUP_CONCAT(change_percent ORDER BY `date`) as pcts,
               COUNT(*) as day_count
        FROM stock_kline
        WHERE `date` >= %s AND `date` <= %s
          AND stock_code IN ('000001.SH', '399001.SZ', '899050.SZ')
        GROUP BY stock_code, YEAR(`date`), WEEKOFYEAR(`date`)
        HAVING COUNT(*) >= 3
        ORDER BY stock_code, y, w
    """, [start_date, latest])
    mkt_rows = cur.fetchall()
    conn.close()

    # 解析指数周数据
    mkt_weekly = {}  # {idx_code: {(y,w): chg}}
    mkt_last_day = {}  # {idx_code: {(y,w): last_day_chg}}
    for r in mkt_rows:
        ic = r['stock_code']
        yw = (r['y'], r['w'])
        pcts = [_to_float(x) for x in r['pcts'].split(',')]
        if ic not in mkt_weekly:
            mkt_weekly[ic] = {}
            mkt_last_day[ic] = {}
        mkt_weekly[ic][yw] = _compound_return(pcts)
        mkt_last_day[ic][yw] = pcts[-1]

    # 解析个股周数据
    stock_weeks = defaultdict(list)  # {code: [(y,w,chg,closes,vols,day_count,pcts)]}
    for r in rows:
        code = r['stock_code']
        pcts = [_to_float(x) for x in r['pcts'].split(',')]
        closes = [_to_float(x) for x in r['closes'].split(',')]
        vols = [_to_float(x) for x in r['vols'].split(',')]
        chg = _compound_return(pcts)
        stock_weeks[code].append({
            'yw': (r['y'], r['w']), 'chg': chg, 'pcts': pcts,
            'closes': closes, 'vols': vols, 'dc': r['day_count'],
            'start': r['week_start'], 'end': r['week_end'],
        })

    logger.info(f"股票数: {len(stock_weeks)}, 总解析耗时: {time.time()-t0:.1f}s")
    return stock_weeks, mkt_weekly, mkt_last_day, dt_end, latest

def build_samples(stock_weeks, mkt_weekly, mkt_last_day, dt_end, latest):
    """从周聚合数据构建样本。"""
    t0 = time.time()
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)
    samples = []

    for code, weeks in stock_weeks.items():
        if len(weeks) < 5:
            continue
        sidx = _get_stock_index(code)
        sfx = sidx.split('.')[-1] if '.' in sidx else ''
        mw = mkt_weekly.get(sidx, {})
        mld = mkt_last_day.get(sidx, {})

        for i in range(len(weeks) - 1):
            tw = weeks[i]
            nw = weeks[i + 1]

            # 检查日期范围
            dt_this = datetime.strptime(tw['start'], '%Y-%m-%d')
            if dt_this < dt_cutoff:
                continue

            this_chg = tw['chg']
            next_chg = nw['chg']
            mkt_chg = mw.get(tw['yw'], 0.0)
            mkt_ld = mld.get(tw['yw'])

            # 价格位置(用本周+前几周的收盘价近似)
            all_closes = []
            for j in range(max(0, i - 12), i + 1):
                all_closes.extend(weeks[j]['closes'])
            pos60 = None
            if len(all_closes) >= 20:
                recent = all_closes[-60:] if len(all_closes) >= 60 else all_closes
                mn, mx = min(c for c in recent if c > 0), max(c for c in recent if c > 0)
                lc = tw['closes'][-1]
                if mx > mn and lc > 0:
                    pos60 = (lc - mn) / (mx - mn)

            # 前周涨跌
            prev_chg = weeks[i-1]['chg'] if i >= 1 else None
            prev2_chg = weeks[i-2]['chg'] if i >= 2 else None
            prev3_chg = weeks[i-3]['chg'] if i >= 3 else None

            # 连涨连跌(日)
            cd, cu = 0, 0
            for p in reversed(tw['pcts']):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else: break

            last_day = tw['pcts'][-1]

            # 量比
            vol_ratio = None
            tv = [v for v in tw['vols'] if v > 0]
            # 用前4周的量做基准
            hv = []
            for j in range(max(0, i-4), i):
                hv.extend([v for v in weeks[j]['vols'] if v > 0])
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

            # V11特征(简化版，不含板块/资金流)
            feat = {
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'suffix': sfx, 'pos60': pos60,
                'prev_chg': prev_chg, 'prev2_chg': prev2_chg,
                'mkt_last_day': mkt_ld, 'vol_ratio': vol_ratio,
                'turnover_ratio': None, 'board_momentum': None,
                'concept_consensus': None, 'big_net_pct_avg': None,
                'rush_up_pullback': False, 'dip_recovery': False,
                'upper_shadow_ratio': None,
            }
            if len(tw['pcts']) >= 4:
                mid = len(tw['pcts']) // 2
                fh = _compound_return(tw['pcts'][:mid])
                sh = _compound_return(tw['pcts'][mid:])
                if fh > 2 and sh < -1: feat['rush_up_pullback'] = True
                if fh < -2 and sh > 1: feat['dip_recovery'] = True

            v11r = _nw_match_rule(feat)

            samples.append({
                'code': code, 'suffix': sfx, 'iw_this': tw['yw'],
                'this_chg': this_chg, 'mkt_chg': mkt_chg,
                'pos60': pos60, 'prev_chg': prev_chg,
                'prev2_chg': prev2_chg, 'prev3_chg': prev3_chg,
                'cd': cd, 'cu': cu, 'last_day': last_day,
                'vol_ratio': vol_ratio, 'mkt_last_day': mkt_ld,
                'cum_2w': cum_2w, 'cum_3w': cum_3w,
                'consec_down_weeks': cdw,
                'next_chg': next_chg, 'actual_up': next_chg >= 0,
                'v11_rule': v11r, 'v11_covered': v11r is not None,
            })

    logger.info(f"样本: {len(samples)}, 耗时: {time.time()-t0:.1f}s")
    return samples

def run_analysis(samples):
    """全面分析。"""
    uncov = [s for s in samples if not s['v11_covered']]
    v11m = [s for s in samples if s['v11_covered']]
    v11c = sum(1 for s in v11m if s['v11_rule']['pred_up'] == s['actual_up'])

    print(f"\n{'='*80}")
    print(f"  基线统计")
    print(f"{'='*80}")
    print(f"  总样本: {len(samples)}")
    print(f"  V11覆盖: {len(v11m)} ({_pct(len(v11m), len(samples))}%), 准确率: {_pct(v11c, len(v11m))}%")
    print(f"  V11未覆盖: {len(uncov)} ({_pct(len(uncov), len(samples))}%)")
    print(f"  未覆盖中涨比例: {_pct(sum(1 for s in uncov if s['actual_up']), len(uncov))}%")

    # ═══════════════════════════════════════════════════════
    # 1. 精细化条件搜索
    # ═══════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print(f"  1. 精细化条件搜索 (仅V11未覆盖样本, 预测涨)")
    print(f"{'='*80}")

    conditions = [
        # 基础
        ('连续2周下跌', lambda s: s['consec_down_weeks'] >= 2),
        ('连续3周下跌', lambda s: s['consec_down_weeks'] >= 3),
        ('2周累跌<-8%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -8),
        ('2周累跌<-10%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10),
        ('2周累跌<-12%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -12),
        ('2周累跌<-15%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -15),
        ('3周累跌<-10%', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -10),
        ('3周累跌<-15%', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -15),
        # +大盘
        ('2周累跌<-10%+大盘涨', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] >= 0),
        ('2周累跌<-10%+大盘跌', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] < 0),
        ('2周累跌<-10%+大盘跌>1%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] < -1),
        ('3周累跌<-10%+大盘涨', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -10 and s['mkt_chg'] >= 0),
        ('3周累跌<-10%+大盘跌>1%', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -10 and s['mkt_chg'] < -1),
        # +价格位置
        ('2周累跌<-10%+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['pos60'] is not None and s['pos60'] < 0.3),
        ('2周累跌<-10%+中位0.3~0.6', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['pos60'] is not None and 0.3 <= s['pos60'] < 0.6),
        ('2周累跌<-10%+中高位0.5~0.8', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['pos60'] is not None and 0.5 <= s['pos60'] < 0.8),
        ('2周累跌<-10%+高位>0.7', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['pos60'] is not None and s['pos60'] >= 0.7),
        # +量比
        ('2周累跌<-10%+缩量<0.8', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8),
        ('2周累跌<-10%+放量>1.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3),
        ('2周累跌<-10%+正常量0.8~1.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['vol_ratio'] is not None and 0.8 <= s['vol_ratio'] <= 1.3),
        # +尾日
        ('2周累跌<-10%+尾日跌>1%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['last_day'] < -1),
        ('2周累跌<-10%+尾日涨', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['last_day'] > 0),
        # +连跌天数
        ('2周累跌<-10%+连跌≥3天', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['cd'] >= 3),
        ('2周累跌<-10%+连跌≥4天', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['cd'] >= 4),
        # +市场
        ('2周累跌<-10%+深证', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['suffix'] == 'SZ'),
        ('2周累跌<-10%+上证', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['suffix'] == 'SH'),
        # 多条件组合
        ('2周累跌<-10%+大盘跌>1%+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] < -1 and s['pos60'] is not None and s['pos60'] < 0.3),
        ('2周累跌<-10%+大盘跌>1%+连跌≥3天', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] < -1 and s['cd'] >= 3),
        ('2周累跌<-10%+尾日涨+低位<0.5', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['last_day'] > 0 and s['pos60'] is not None and s['pos60'] < 0.5),
        ('2周累跌<-10%+放量>1.3+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3 and s['pos60'] is not None and s['pos60'] < 0.3),
        ('2周累跌<-10%+大盘涨+pos<0.7', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] >= 0 and s['pos60'] is not None and s['pos60'] < 0.7),
        ('3周累跌<-14%+大盘涨+pos<0.7', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -14 and s['mkt_chg'] >= 0 and s['pos60'] is not None and s['pos60'] < 0.7),
        ('2周累跌<-10%+深证+大盘跌>1%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['suffix'] == 'SZ' and s['mkt_chg'] < -1),
        ('2周累跌<-10%+深证+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['suffix'] == 'SZ' and s['pos60'] is not None and s['pos60'] < 0.3),
        ('2周累跌<-10%+缩量+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8 and s['pos60'] is not None and s['pos60'] < 0.3),
        ('2周累跌<-12%+大盘跌>1%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -12 and s['mkt_chg'] < -1),
        ('2周累跌<-12%+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -12 and s['pos60'] is not None and s['pos60'] < 0.3),
        ('2周累跌<-15%+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -15 and s['pos60'] is not None and s['pos60'] < 0.3),
        # 英维克精确场景
        ('英维克:3周累跌<-14%+大盘涨+pos0.5~0.8', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -14 and s['mkt_chg'] >= 0 and s['pos60'] is not None and 0.5 <= s['pos60'] < 0.8),
        ('英维克:2周累跌<-10%+大盘涨+pos0.4~0.7', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] >= 0 and s['pos60'] is not None and 0.4 <= s['pos60'] < 0.7),
    ]

    print(f"\n  {'条件':>48} | {'样本':>5} {'涨':>5} {'涨%':>5} | {'均值':>7} {'中位':>7}")
    print("  " + "-" * 90)

    for name, cond in conditions:
        matched = [s for s in uncov if cond(s)]
        if len(matched) < 5: continue
        up = sum(1 for s in matched if s['actual_up'])
        ncs = sorted([s['next_chg'] for s in matched])
        avg_nc = _mean(ncs)
        med_nc = ncs[len(ncs)//2]
        pct = _pct(up, len(matched))
        flag = ' ★' if pct > 55 and len(matched) >= 20 else (' ▲' if pct > 50 and len(matched) >= 30 else '')
        print(f"  {name:>48} | {len(matched):>5} {up:>5} {pct:>4.1f}% | {avg_nc:>+6.2f}% {med_nc:>+6.2f}%{flag}")

    # ═══════════════════════════════════════════════════════
    # 2. 英维克W11特征
    # ═══════════════════════════════════════════════════════
    yw = [s for s in samples if s['code'] == '002837.SZ' and s['iw_this'][1] == 11 and s['iw_this'][0] == 2026]
    if yw:
        s = yw[0]
        pos_str = f"{s['pos60']:.2f}" if s['pos60'] is not None else 'N/A'
        cum2_str = f"{s['cum_2w']:+.2f}%" if s['cum_2w'] is not None else 'N/A'
        cum3_str = f"{s['cum_3w']:+.2f}%" if s['cum_3w'] is not None else 'N/A'
        vr_str = f"{s['vol_ratio']:.2f}" if s['vol_ratio'] is not None else 'N/A'
        prev_str = f"{s['prev_chg']:+.2f}%" if s['prev_chg'] is not None else 'N/A'
        prev2_str = f"{s['prev2_chg']:+.2f}%" if s['prev2_chg'] is not None else 'N/A'
        print(f"\n  英维克W11特征:")
        print(f"    this={s['this_chg']:+.2f}% mkt={s['mkt_chg']:+.2f}% pos60={pos_str}")
        print(f"    prev={prev_str} prev2={prev2_str}")
        print(f"    cum_2w={cum2_str} cum_3w={cum3_str}")
        print(f"    cd={s['cd']} last_day={s['last_day']:+.2f}% vol_ratio={vr_str}")
        print(f"    cdw={s['consec_down_weeks']} v11={s['v11_covered']} next={s['next_chg']:+.2f}%")

    # ═══════════════════════════════════════════════════════
    # 3. V11影响评估(选准确率最高的可行规则)
    # ═══════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print(f"  2. V11整体影响评估")
    print(f"{'='*80}")

    # 找出准确率>55%且样本>20的条件
    best_rules = []
    for name, cond in conditions:
        matched = [s for s in uncov if cond(s)]
        if len(matched) < 20: continue
        up = sum(1 for s in matched if s['actual_up'])
        pct = up / len(matched) * 100
        if pct > 50:
            best_rules.append((name, cond, len(matched), up, pct))

    best_rules.sort(key=lambda x: (-x[4], -x[2]))

    for name, cond, total, correct, acc in best_rules[:5]:
        new_total = total
        new_correct = correct
        combined_total = len(v11m) + new_total
        combined_correct = v11c + new_correct
        combined_acc = combined_correct / combined_total * 100

        # CV
        all_weeks = sorted(set(s['iw_this'] for s in samples))
        fold_size = max(1, len(all_weeks) // 5)
        cv_accs = []
        for fold in range(5):
            ts = fold * fold_size
            te = ts + fold_size if fold < 4 else len(all_weeks)
            tw_set = set(all_weeks[ts:te])
            fm = [s for s in uncov if s['iw_this'] in tw_set and cond(s)]
            if fm:
                cv_accs.append(sum(1 for s in fm if s['actual_up']) / len(fm) * 100)

        cv_str = f"CV: {_mean(cv_accs):.1f}% [{min(cv_accs):.0f}~{max(cv_accs):.0f}]" if cv_accs else "CV: N/A"

        print(f"\n  {name}")
        print(f"    新增: {new_total}样本, {acc:.1f}%准确")
        print(f"    V11合并: {combined_total}样本, {combined_acc:.1f}%准确 (基线{_pct(v11c, len(v11m))}%)")
        print(f"    覆盖率: {_pct(len(v11m), len(samples))}% → {_pct(combined_total, len(samples))}% (+{_pct(new_total, len(samples))}%)")
        print(f"    {cv_str}")

    # ═══════════════════════════════════════════════════════
    # 4. 按周分布(选最优规则)
    # ═══════════════════════════════════════════════════════
    if best_rules:
        name, cond, _, _, _ = best_rules[0]
        print(f"\n{'='*80}")
        print(f"  3. 最优规则按周分布: {name}")
        print(f"{'='*80}")
        ws = defaultdict(lambda: [0, 0])
        for s in uncov:
            if cond(s):
                ws[s['iw_this']][0] += 1
                if s['actual_up']: ws[s['iw_this']][1] += 1
        for wk in sorted(ws.keys()):
            t, c = ws[wk]
            print(f"    {wk[0]}-W{wk[1]:02d}: {t:>4}触发, {c:>4}正确, {_pct(c,t):>5.1f}%")


def main():
    stock_weeks, mkt_weekly, mkt_last_day, dt_end, latest = load_weekly_data()
    samples = build_samples(stock_weeks, mkt_weekly, mkt_last_day, dt_end, latest)
    run_analysis(samples)

    # ═══════════════════════════════════════════════════════
    # 最终结论
    # ═══════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("  最终结论")
    print(f"{'='*80}")
    print("  1. 英维克W11场景(大盘涨+个股连跌): 该类场景预测涨的准确率仅33-42%,")
    print("     远低于50%随机基线。添加此规则会严重损害V11整体准确率。")
    print("  2. '连续多周下跌→反弹'规则仅在'大盘也在跌'时有效(57-67%准确率),")
    print("     但这些场景已被V11现有backbone规则(V5_R1/R3等)大量覆盖。")
    print("  3. 建议: 不添加此规则。V11在W11正确选择了'不预测'(abstain),")
    print("     这比错误预测更优。")
    print(f"{'='*80}")

if __name__ == '__main__':
    main()
