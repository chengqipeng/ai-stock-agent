#!/usr/bin/env python3
"""
补充分析: 连续多周下跌规则的精细化条件搜索
==========================================
V2结果显示简单的"连续多周下跌→涨"准确率仅41%，
本脚本进一步探索加入更多约束条件后是否能提升准确率。
"""
import sys, os, logging, pickle
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

from dao import get_connection
from service.weekly_prediction_service import (
    _to_float, _compound_return, _get_stock_index, _nw_match_rule,
)

N_WEEKS = 29

def _mean(lst):
    return sum(lst) / len(lst) if lst else 0

def _pct(c, t):
    return round(c / t * 100, 1) if t > 0 else 0

def load_and_build():
    """加载数据并构建样本(复用V2逻辑)。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("SELECT MAX(`date`) as d FROM stock_kline WHERE stock_code='399001.SZ'")
    latest_date = cur.fetchone()['d']
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=(N_WEEKS + 4) * 7 + 60)
    start_date = dt_start.strftime('%Y-%m-%d')
    logger.info(f"日期: {start_date} ~ {latest_date}")

    logger.info("加载K线...")
    cur.execute(
        "SELECT stock_code, `date`, close_price, change_percent, trading_volume "
        "FROM stock_kline WHERE `date` >= %s AND `date` <= %s "
        "AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ') "
        "AND stock_code NOT LIKE '399%%' AND stock_code != '000001.SH' "
        "ORDER BY stock_code, `date`", [start_date, latest_date])
    sk = defaultdict(list)
    for r in cur.fetchall():
        sk[r['stock_code']].append({
            'date': r['date'], 'close': _to_float(r['close_price']),
            'change_percent': _to_float(r['change_percent']),
            'volume': _to_float(r['trading_volume']),
        })
    logger.info(f"个股: {len(sk)}")

    cur.execute(
        "SELECT stock_code, `date`, change_percent FROM stock_kline "
        "WHERE stock_code IN ('000001.SH','399001.SZ','899050.SZ') "
        "AND `date` >= %s AND `date` <= %s ORDER BY `date`", [start_date, latest_date])
    mk = defaultdict(list)
    for r in cur.fetchall():
        mk[r['stock_code']].append({'date': r['date'], 'change_percent': _to_float(r['change_percent'])})
    conn.close()

    mbw = {}
    for ic, kl in mk.items():
        bw = defaultdict(list)
        for k in kl:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            bw[dt.isocalendar()[:2]].append(k)
        mbw[ic] = bw

    # 构建样本
    dt_cutoff = dt_end - timedelta(days=N_WEEKS * 7 + 14)
    samples = []
    for code, klines in sk.items():
        if len(klines) < 60: continue
        sidx = _get_stock_index(code)
        sfx = sidx.split('.')[-1] if '.' in sidx else ''
        ibw = mbw.get(sidx, {})
        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            wg[dt.isocalendar()[:2]].append(k)
        sw = sorted(wg.keys())
        sa = sorted(klines, key=lambda x: x['date'])

        for i in range(len(sw) - 1):
            iw, inw = sw[i], sw[i+1]
            td = sorted(wg[iw], key=lambda x: x['date'])
            nd = sorted(wg[inw], key=lambda x: x['date'])
            if len(td) < 3 or len(nd) < 3: continue
            dt_this = datetime.strptime(td[0]['date'], '%Y-%m-%d')
            if dt_this < dt_cutoff: continue

            tp = [d['change_percent'] for d in td]
            tc = _compound_return(tp)
            nc = _compound_return([d['change_percent'] for d in nd])

            mw = ibw.get(iw, [])
            mc = _compound_return([k['change_percent'] for k in sorted(mw, key=lambda x: x['date'])]) if len(mw) >= 3 else 0.0
            mld = sorted(mw, key=lambda x: x['date'])[-1]['change_percent'] if mw else None

            fd = td[0]['date']
            hist = [k for k in sa if k['date'] < fd]

            pos60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in td if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = td[-1]['close']
                    if mx > mn and lc > 0: pos60 = (lc - mn) / (mx - mn)

            def gpc(off):
                idx2 = i - off
                if idx2 < 0: return None
                pd = sorted(wg[sw[idx2]], key=lambda x: x['date'])
                return _compound_return([d['change_percent'] for d in pd]) if len(pd) >= 3 else None

            pc, p2c, p3c = gpc(1), gpc(2), gpc(3)

            cd, cu = 0, 0
            for p in reversed(tp):
                if p < 0:
                    cd += 1
                    if cu > 0: break
                elif p > 0:
                    cu += 1
                    if cd > 0: break
                else: break

            vr = None
            tv = [d['volume'] for d in td if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            if tv and hv:
                at, ah = _mean(tv), _mean(hv)
                if ah > 0: vr = at / ah

            c2w = (tc + pc) if pc is not None else None
            c3w = (tc + pc + p2c) if (pc is not None and p2c is not None) else None

            cdw = 0
            if tc < 0: cdw += 1
            if pc is not None and pc < 0 and cdw > 0: cdw += 1
            if p2c is not None and p2c < 0 and cdw > 1: cdw += 1

            feat = {
                'this_chg': tc, 'mkt_chg': mc, 'cd': cd, 'cu': cu,
                'last_day': tp[-1], 'suffix': sfx, 'pos60': pos60,
                'prev_chg': pc, 'prev2_chg': p2c, 'mkt_last_day': mld,
                'vol_ratio': vr, 'turnover_ratio': None,
                'board_momentum': None, 'concept_consensus': None,
                'big_net_pct_avg': None, 'rush_up_pullback': False,
                'dip_recovery': False, 'upper_shadow_ratio': None,
            }
            if len(tp) >= 4:
                mid = len(tp) // 2
                fh, sh = _compound_return(tp[:mid]), _compound_return(tp[mid:])
                if fh > 2 and sh < -1: feat['rush_up_pullback'] = True
                if fh < -2 and sh > 1: feat['dip_recovery'] = True

            v11r = _nw_match_rule(feat)

            samples.append({
                'code': code, 'suffix': sfx, 'iw_this': iw,
                'this_chg': tc, 'mkt_chg': mc, 'pos60': pos60,
                'prev_chg': pc, 'prev2_chg': p2c, 'prev3_chg': p3c,
                'cd': cd, 'cu': cu, 'last_day': tp[-1],
                'vol_ratio': vr, 'mkt_last_day': mld,
                'cum_2w': c2w, 'cum_3w': c3w, 'consec_down_weeks': cdw,
                'next_chg': nc, 'actual_up': nc >= 0,
                'v11_rule': v11r, 'v11_covered': v11r is not None,
            })

    logger.info(f"样本: {len(samples)}")
    return samples

def refined_search(samples):
    """精细化条件搜索: 加入更多约束。"""
    print("\n" + "=" * 80)
    print("  精细化条件搜索: 连续多周下跌 + 附加条件")
    print("=" * 80)

    uncovered = [s for s in samples if not s['v11_covered']]
    total_unc = len(uncovered)
    base_up = sum(1 for s in uncovered if s['actual_up'])
    print(f"\n  V11未覆盖样本: {total_unc}, 基线涨比例: {_pct(base_up, total_unc)}%")

    # 先看基线: 连续2周下跌的未覆盖样本
    base_2w = [s for s in uncovered if s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None]
    base_3w = [s for s in uncovered if s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None]
    print(f"\n  连续2周下跌(未覆盖): {len(base_2w)}, 涨比例: {_pct(sum(1 for s in base_2w if s['actual_up']), len(base_2w))}%")
    print(f"  连续3周下跌(未覆盖): {len(base_3w)}, 涨比例: {_pct(sum(1 for s in base_3w if s['actual_up']), len(base_3w))}%")

    # 精细化条件组合
    conditions = [
        # (名称, 过滤函数)
        ('2周累跌<-8%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -8),
        ('2周累跌<-10%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10),
        ('2周累跌<-12%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -12),
        ('3周累跌<-10%', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -10),
        ('3周累跌<-15%', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -15),
        # 加入大盘条件
        ('2周累跌<-10%+大盘涨', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] >= 0),
        ('2周累跌<-10%+大盘跌', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] < 0),
        ('2周累跌<-10%+大盘跌>1%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] < -1),
        # 加入价格位置
        ('2周累跌<-10%+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['pos60'] is not None and s['pos60'] < 0.3),
        ('2周累跌<-10%+中位0.3~0.7', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['pos60'] is not None and 0.3 <= s['pos60'] < 0.7),
        ('2周累跌<-10%+高位>0.7', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['pos60'] is not None and s['pos60'] >= 0.7),
        # 加入量比
        ('2周累跌<-10%+缩量<0.8', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['vol_ratio'] is not None and s['vol_ratio'] < 0.8),
        ('2周累跌<-10%+放量>1.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3),
        # 加入尾日
        ('2周累跌<-10%+尾日跌>1%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['last_day'] < -1),
        ('2周累跌<-10%+尾日涨', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['last_day'] > 0),
        # 加入连跌天数
        ('2周累跌<-10%+连跌≥3天', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['cd'] >= 3),
        ('2周累跌<-10%+连跌≥4天', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['cd'] >= 4),
        # 组合条件
        ('2周累跌<-10%+大盘跌>1%+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] < -1 and s['pos60'] is not None and s['pos60'] < 0.3),
        ('2周累跌<-10%+大盘跌>1%+连跌≥3天', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] < -1 and s['cd'] >= 3),
        ('2周累跌<-10%+尾日涨+低位<0.5', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['last_day'] > 0 and s['pos60'] is not None and s['pos60'] < 0.5),
        ('2周累跌<-10%+放量>1.3+低位<0.3', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['vol_ratio'] is not None and s['vol_ratio'] > 1.3 and s['pos60'] is not None and s['pos60'] < 0.3),
        # 英维克场景: 大盘涨+个股连跌+中位
        ('2周累跌<-10%+大盘涨+pos<0.7', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['mkt_chg'] >= 0 and s['pos60'] is not None and s['pos60'] < 0.7),
        ('3周累跌<-10%+大盘涨+pos<0.7', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -10 and s['mkt_chg'] >= 0 and s['pos60'] is not None and s['pos60'] < 0.7),
        ('3周累跌<-12%+大盘涨+pos<0.7', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -12 and s['mkt_chg'] >= 0 and s['pos60'] is not None and s['pos60'] < 0.7),
        # 本周跌幅限制
        ('2周累跌<-10%+本周跌>3%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['this_chg'] < -3),
        ('2周累跌<-10%+本周跌>5%', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['this_chg'] < -5),
        # 深证/上证分开
        ('2周累跌<-10%+深证', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['suffix'] == 'SZ'),
        ('2周累跌<-10%+上证', lambda s: s['consec_down_weeks'] >= 2 and s['cum_2w'] is not None and s['cum_2w'] < -10 and s['suffix'] == 'SH'),
        # 英维克精确场景
        ('英维克场景:3周累跌<-14%+大盘涨+pos0.5~0.8', lambda s: s['consec_down_weeks'] >= 3 and s['cum_3w'] is not None and s['cum_3w'] < -14 and s['mkt_chg'] >= 0 and s['pos60'] is not None and 0.5 <= s['pos60'] < 0.8),
    ]

    print(f"\n  {'条件':>45} | {'样本':>5} {'涨':>5} {'涨比例':>6} | {'均值涨跌':>8}")
    print("  " + "-" * 85)

    for name, cond in conditions:
        matched = [s for s in uncovered if cond(s)]
        if len(matched) < 5: continue
        up = sum(1 for s in matched if s['actual_up'])
        avg_nc = _mean([s['next_chg'] for s in matched])
        marker = ' ★' if up / len(matched) > 0.55 and len(matched) >= 20 else ''
        marker2 = ' ▲' if up / len(matched) > 0.50 and len(matched) >= 30 else ''
        print(f"  {name:>45} | {len(matched):>5} {up:>5} {_pct(up, len(matched)):>5.1f}% | {avg_nc:>+7.2f}%{marker}{marker2}")

    # 英维克W11特征
    yw = [s for s in samples if s['code'] == '002837.SZ' and s['iw_this'] == (2026, 11)]
    if yw:
        s = yw[0]
        print(f"\n  英维克W11特征:")
        print(f"    this_chg={s['this_chg']:+.2f}%, mkt_chg={s['mkt_chg']:+.2f}%, pos60={s['pos60']:.2f}")
        print(f"    prev_chg={s['prev_chg']:+.2f}%, prev2_chg={s['prev2_chg']:+.2f}%")
        print(f"    cum_2w={s['cum_2w']:+.2f}%, cum_3w={s['cum_3w']:+.2f}%")
        print(f"    cd={s['cd']}, last_day={s['last_day']:+.2f}%, vol_ratio={s['vol_ratio']}")
        print(f"    consec_down_weeks={s['consec_down_weeks']}")
        print(f"    v11_covered={s['v11_covered']}, next_chg={s['next_chg']:+.2f}%")


def main():
    samples = load_and_build()
    refined_search(samples)
    print(f"\n{'='*80}")
    print("  分析完成")
    print(f"{'='*80}")

if __name__ == '__main__':
    main()
