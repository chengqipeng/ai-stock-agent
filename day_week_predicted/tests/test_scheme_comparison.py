#!/usr/bin/env python3
"""
方案A/B/C大样本历史验证
======================
用全市场K线数据(2025-06~2026-03, ~40周)模拟V11规则引擎，
在大样本上对比三个方案的准确率，避免只用3周prediction_history的过拟合风险。

方法：
  1. 对每只股票每周，用V11规则引擎匹配（与生产一致）
  2. 计算下周实际涨跌作为验证
  3. 分别应用方案A/B/C，对比准确率

用法: .venv/bin/python day_week_predicted/tests/test_scheme_comparison.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import random
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

random.seed(42)

# 采样股票数（全量太慢，采样300只覆盖各板块）
SAMPLE_SIZE = 300
START_DATE = '2025-06-01'
END_DATE = '2026-03-27'
MIN_KLINES = 100


# ═══════════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════════

def load_sample_stocks():
    """随机采样300只A股。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT stock_code FROM stock_kline
        WHERE date >= %s AND date <= %s
          AND (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ')
          AND stock_code NOT LIKE '399%%' AND stock_code != '000001.SH'
          AND trading_volume > 0
        GROUP BY stock_code HAVING COUNT(*) >= %s
    """, (START_DATE, END_DATE, MIN_KLINES))
    all_codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close(); conn.close()
    random.shuffle(all_codes)
    codes = all_codes[:SAMPLE_SIZE]
    logger.info("采样 %d/%d 只股票", len(codes), len(all_codes))
    return codes


def load_klines(codes):
    """批量加载K线。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    # 多加载6个月历史用于计算均量等
    ext_start = (datetime.strptime(START_DATE, '%Y-%m-%d') - timedelta(days=180)).strftime('%Y-%m-%d')
    stock_klines = defaultdict(list)
    batch = 200
    for i in range(0, len(codes), batch):
        b = codes[i:i+batch]
        ph = ','.join(['%s'] * len(b))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, open_price, high_price, "
            f"low_price, trading_volume, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            b + [ext_start, END_DATE])
        for r in cur.fetchall():
            stock_klines[r['stock_code']].append({
                'date': str(r['date']),
                'close': float(r['close_price'] or 0),
                'open': float(r['open_price'] or 0),
                'high': float(r['high_price'] or 0),
                'low': float(r['low_price'] or 0),
                'volume': float(r['trading_volume'] or 0),
                'change_percent': float(r['change_percent'] or 0),
                'turnover': float(r.get('change_hand') or 0),
            })
    cur.close(); conn.close()
    logger.info("加载 %d 只股票K线", len(stock_klines))
    return dict(stock_klines)


def load_market_klines():
    """加载大盘K线。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    ext_start = (datetime.strptime(START_DATE, '%Y-%m-%d') - timedelta(days=180)).strftime('%Y-%m-%d')
    cur.execute(
        "SELECT `date`, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        (ext_start, END_DATE))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{'date': str(r['date']), 'change_percent': float(r['change_percent'] or 0)} for r in rows]


def load_fund_flow(codes):
    """加载资金流数据。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    ff_map = defaultdict(list)
    batch = 200
    for i in range(0, len(codes), batch):
        b = codes[i:i+batch]
        ph = ','.join(['%s'] * len(b))
        cur.execute(
            f"SELECT stock_code, `date`, big_net_pct FROM stock_fund_flow "
            f"WHERE stock_code IN ({ph}) AND `date` >= %s ORDER BY `date`",
            b + [START_DATE])
        for r in cur.fetchall():
            ff_map[r['stock_code']].append({
                'date': str(r['date']),
                'big_net_pct': float(r.get('big_net_pct') or 0),
            })
    cur.close(); conn.close()
    return dict(ff_map)


# ═══════════════════════════════════════════════════════════════
# 周数据构建 + V11规则匹配
# ═══════════════════════════════════════════════════════════════

def _compound(pcts):
    r = 1.0
    for p in pcts:
        r *= (1 + p / 100)
    return (r - 1) * 100


def _mean(lst):
    return sum(lst) / len(lst) if lst else 0


def build_weekly_samples(stock_klines, market_klines, ff_map):
    """构建每只股票每周的样本，包含V11特征和下周涨跌。"""
    from service.weekly_prediction_service import (
        _nw_extract_features, _nw_match_rule, _v11_apply_confidence_modifier,
        _get_stock_index,
    )

    # 大盘按周分组
    mkt_by_week = defaultdict(list)
    for k in market_klines:
        dt = datetime.strptime(k['date'], '%Y-%m-%d')
        iw = dt.isocalendar()[:2]
        mkt_by_week[iw].append(k)

    samples = []
    for code, klines in stock_klines.items():
        if len(klines) < 60:
            continue

        # 按周分组
        wg = defaultdict(list)
        for k in klines:
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        sorted_weeks = sorted(wg.keys())
        # 只取START_DATE之后的周
        start_dt = datetime.strptime(START_DATE, '%Y-%m-%d')
        start_iw = start_dt.isocalendar()[:2]

        ff_list = ff_map.get(code, [])
        ff_by_date = {f['date']: f for f in ff_list}

        for idx in range(len(sorted_weeks) - 1):
            iw_this = sorted_weeks[idx]
            iw_next = sorted_weeks[idx + 1]

            if iw_this < start_iw:
                continue

            this_days = sorted(wg[iw_this], key=lambda x: x['date'])
            next_days = sorted(wg[iw_next], key=lambda x: x['date'])

            if len(this_days) < 3 or len(next_days) < 3:
                continue

            this_pcts = [d['change_percent'] for d in this_days]
            next_pcts = [d['change_percent'] for d in next_days]
            next_chg = _compound(next_pcts)
            actual_next_up = next_chg >= 0

            # 大盘本周
            mw = mkt_by_week.get(iw_this, [])
            mw_sorted = sorted(mw, key=lambda x: x['date'])
            mkt_chg = _compound([k['change_percent'] for k in mw_sorted]) if len(mw_sorted) >= 3 else 0
            mkt_last_day = mw_sorted[-1]['change_percent'] if mw_sorted else None

            # 历史K线
            first_date = this_days[0]['date']
            all_sorted = sorted(klines, key=lambda x: x['date'])
            hist = [k for k in all_sorted if k['date'] < first_date]

            # 价格位置
            price_pos_60 = None
            if len(hist) >= 20:
                hc = [k['close'] for k in hist[-60:] if k['close'] > 0]
                if hc:
                    ac = hc + [k['close'] for k in this_days if k['close'] > 0]
                    mn, mx = min(ac), max(ac)
                    lc = this_days[-1]['close']
                    if mx > mn and lc > 0:
                        price_pos_60 = (lc - mn) / (mx - mn)

            # 前周涨跌
            prev_chg = None
            if len(hist) >= 5:
                prev_chg = _compound([k['change_percent'] for k in hist[-5:]])
            prev2_chg = None
            if len(hist) >= 10:
                prev2_chg = _compound([k['change_percent'] for k in hist[-10:-5]])

            # 量比
            tv = [d['volume'] for d in this_days if d['volume'] > 0]
            hv = [k['volume'] for k in hist[-20:] if k['volume'] > 0]
            vol_ratio = None
            if tv and hv:
                avg_tv = _mean(tv)
                avg_hv = _mean(hv)
                if avg_hv > 0:
                    vol_ratio = avg_tv / avg_hv

            # 换手率比
            turnover_ratio = None
            tw = [d['turnover'] for d in this_days if d.get('turnover') and d['turnover'] > 0]
            ht = [k['turnover'] for k in hist[-20:] if k.get('turnover') and k['turnover'] > 0]
            if tw and ht:
                avg_tw = _mean(tw)
                avg_ht = _mean(ht)
                if avg_ht > 0:
                    turnover_ratio = avg_tw / avg_ht

            # 资金流
            big_net_pct_avg = None
            ff_week = [ff_by_date[d['date']] for d in this_days if d['date'] in ff_by_date]
            if ff_week:
                pcts = [f['big_net_pct'] for f in ff_week if f['big_net_pct'] != 0]
                if pcts:
                    big_net_pct_avg = _mean(pcts)

            stock_idx = _get_stock_index(code)

            feat = _nw_extract_features(
                this_pcts, mkt_chg,
                market_index=stock_idx,
                price_pos_60=price_pos_60,
                prev_week_chg=prev_chg,
                prev2_week_chg=prev2_chg,
                mkt_last_day=mkt_last_day,
                vol_ratio=vol_ratio,
                turnover_ratio=turnover_ratio,
                week_klines=this_days,
                hist_klines=hist,
                big_net_pct_avg=big_net_pct_avg)

            rule = _nw_match_rule(feat)
            if rule is None:
                continue

            pred_up = rule['pred_up']
            layer = rule.get('layer', 'backbone')
            tier = rule['tier']
            base_conf = 'high' if tier == 1 else 'reference'
            confidence = _v11_apply_confidence_modifier(pred_up, base_conf, feat)

            correct = pred_up == actual_next_up

            samples.append({
                'code': code,
                'iso_week': iw_this,
                'pred_up': pred_up,
                'confidence': confidence,
                'layer': layer,
                'rule_name': rule['name'],
                'correct': correct,
                'next_chg': next_chg,
                'this_chg': feat['this_chg'],
                'mkt_chg': mkt_chg,
                'vol_ratio': vol_ratio,
                'big_net_pct_avg': big_net_pct_avg,
                'price_pos_60': price_pos_60,
            })

    logger.info("构建 %d 个周样本", len(samples))
    return samples


# ═══════════════════════════════════════════════════════════════
# 方案定义
# ═══════════════════════════════════════════════════════════════

def scheme_original(s):
    """原始方案：直接用V11置信度。"""
    return s['confidence']


def scheme_A(s, rolling_acc, rolling_n, backtest_acc=70.0, min_samples=30, blend_n=100):
    """方案A：自适应置信度。"""
    if rolling_n >= 10 and rolling_acc is not None:
        w = min(rolling_n / blend_n, 1.0)
        blended = backtest_acc * (1 - w) + rolling_acc * w
    else:
        blended = backtest_acc

    if blended >= 60:
        conf = 'high'
    elif blended >= 50:
        conf = 'medium'
    else:
        conf = 'low'

    # 冷启动
    if rolling_n < min_samples and conf == 'high':
        conf = 'medium'
    return conf


def scheme_B_deny(s):
    """方案B：deny过滤（缩量+资金流出时降级）。"""
    base = s['confidence']
    vr = s.get('vol_ratio')
    ff = s.get('big_net_pct_avg')

    # deny条件：缩量且资金流出
    if vr is not None and vr < 0.7 and ff is not None and ff < 0:
        if base == 'high':
            return 'medium'
        elif base == 'medium' or base == 'reference':
            return 'low'
    return base


def scheme_AB(s, rolling_acc, rolling_n, backtest_acc=70.0):
    """方案A+B：自适应 + deny过滤。"""
    conf_a = scheme_A(s, rolling_acc, rolling_n, backtest_acc)
    # 在A的基础上应用deny
    vr = s.get('vol_ratio')
    ff = s.get('big_net_pct_avg')
    if vr is not None and vr < 0.7 and ff is not None and ff < 0:
        if conf_a == 'high':
            return 'medium'
        elif conf_a in ('medium', 'reference'):
            return 'low'
    return conf_a


def scheme_C_orthogonal(s):
    """方案C：正交共识。"""
    base = s['confidence']
    if base not in ('high', 'reference'):
        return base

    vr = s.get('vol_ratio')
    ff = s.get('big_net_pct_avg')

    # 维度2: 资金面
    cap_score = 0
    if vr is not None:
        if vr >= 1.0:
            cap_score += 1
        elif vr < 0.7:
            cap_score -= 1
    if ff is not None:
        if ff > 0:
            cap_score += 1
        elif ff < 0:
            cap_score -= 1

    if cap_score >= 1:
        d2 = 'confirm'
    elif cap_score <= -1:
        d2 = 'deny'
    else:
        d2 = 'neutral'

    # 综合
    if d2 == 'deny':
        return 'low'
    elif d2 == 'confirm':
        return 'high'
    else:
        return 'medium'


# ═══════════════════════════════════════════════════════════════
# 模拟运行
# ═══════════════════════════════════════════════════════════════

def _pct(n, d):
    return f"{n/d*100:.1f}%" if d > 0 else "-"


def simulate(samples):
    """按时间顺序模拟各方案，方案A用滚动准确率。"""

    # 按周排序
    sorted_weeks = sorted(set(s['iso_week'] for s in samples))
    by_week = defaultdict(list)
    for s in samples:
        by_week[s['iso_week']].append(s)

    # 方案A的滚动历史
    rolling = defaultdict(lambda: {'correct': 0, 'total': 0})

    # 结果收集
    results = {name: {'high_c': 0, 'high_n': 0, 'hm_c': 0, 'hm_n': 0,
                       'all_c': 0, 'all_n': 0,
                       'weekly': []}
               for name in ['原始', 'A自适应', 'B_deny', 'A+B', 'C正交']}

    for week in sorted_weeks:
        week_samples = by_week[week]
        week_results = {name: {'high_c': 0, 'high_n': 0, 'hm_c': 0, 'hm_n': 0}
                        for name in results}

        for s in week_samples:
            rule_key = f"{s['layer']}_{('UP' if s['pred_up'] else 'DOWN')}"
            r = rolling[rule_key]
            r_acc = r['correct'] / r['total'] * 100 if r['total'] > 0 else None
            r_n = r['total']

            schemes = {
                '原始': scheme_original(s),
                'A自适应': scheme_A(s, r_acc, r_n),
                'B_deny': scheme_B_deny(s),
                'A+B': scheme_AB(s, r_acc, r_n),
                'C正交': scheme_C_orthogonal(s),
            }

            for name, conf in schemes.items():
                if conf == 'high':
                    week_results[name]['high_n'] += 1
                    week_results[name]['high_c'] += int(s['correct'])
                if conf in ('high', 'medium'):
                    week_results[name]['hm_n'] += 1
                    week_results[name]['hm_c'] += int(s['correct'])

        # 记录本周结果到滚动历史（供下周使用）
        for s in week_samples:
            rule_key = f"{s['layer']}_{('UP' if s['pred_up'] else 'DOWN')}"
            rolling[rule_key]['total'] += 1
            rolling[rule_key]['correct'] += int(s['correct'])

        # 汇总
        for name in results:
            wr = week_results[name]
            results[name]['high_c'] += wr['high_c']
            results[name]['high_n'] += wr['high_n']
            results[name]['hm_c'] += wr['hm_c']
            results[name]['hm_n'] += wr['hm_n']
            results[name]['all_c'] += sum(1 for s in week_samples if s['correct'])
            results[name]['all_n'] += len(week_samples)
            results[name]['weekly'].append({
                'week': week,
                'high_c': wr['high_c'], 'high_n': wr['high_n'],
                'hm_c': wr['hm_c'], 'hm_n': wr['hm_n'],
                'total': len(week_samples),
            })

    return results, sorted_weeks


def print_results(results, sorted_weeks):
    """打印对比结果。"""

    print(f"\n{'='*85}")
    print(f"  方案对比 — 汇总")
    print(f"{'='*85}")
    print(f"  {'方案':<12} {'high准确率':>14} {'high数量':>10} {'high+med准确率':>16} {'h+m数量':>10}")
    print(f"  {'─'*75}")

    for name in ['原始', 'A自适应', 'B_deny', 'A+B', 'C正交']:
        r = results[name]
        h_acc = _pct(r['high_c'], r['high_n'])
        hm_acc = _pct(r['hm_c'], r['hm_n'])
        print(f"  {name:<12} {h_acc:>14} {r['high_n']:>10} {hm_acc:>16} {r['hm_n']:>10}")

    # 逐周对比（只看high）
    print(f"\n{'='*85}")
    print(f"  逐周high准确率对比 (每4周汇总)")
    print(f"{'='*85}")

    # 每4周汇总
    chunk_size = 4
    week_list = sorted_weeks
    for ci in range(0, len(week_list), chunk_size):
        chunk_weeks = set(week_list[ci:ci+chunk_size])
        label = f"W{week_list[ci][1]:02d}-W{week_list[min(ci+chunk_size-1, len(week_list)-1)][1]:02d}"

        print(f"\n  {label}:")
        for name in ['原始', 'A自适应', 'B_deny', 'A+B', 'C正交']:
            hc = sum(w['high_c'] for w in results[name]['weekly'] if w['week'] in chunk_weeks)
            hn = sum(w['high_n'] for w in results[name]['weekly'] if w['week'] in chunk_weeks)
            hmc = sum(w['hm_c'] for w in results[name]['weekly'] if w['week'] in chunk_weeks)
            hmn = sum(w['hm_n'] for w in results[name]['weekly'] if w['week'] in chunk_weeks)
            print(f"    {name:<12} high={_pct(hc,hn):>6}({hn:>5})  h+m={_pct(hmc,hmn):>6}({hmn:>5})")

    # 置信度校准
    print(f"\n{'='*85}")
    print(f"  置信度校准检验 (理想: high>60%, medium 50-60%, low<50%)")
    print(f"{'='*85}")
    # 需要重新跑一遍收集各级别
    # 简化：直接用汇总数据
    for name in ['原始', 'A自适应', 'B_deny', 'A+B', 'C正交']:
        r = results[name]
        h_acc = r['high_c'] / r['high_n'] * 100 if r['high_n'] > 0 else 0
        # medium = hm - high
        m_c = r['hm_c'] - r['high_c']
        m_n = r['hm_n'] - r['high_n']
        m_acc = m_c / m_n * 100 if m_n > 0 else 0
        # low = all - hm
        l_c = r['all_c'] - r['hm_c']
        l_n = r['all_n'] - r['hm_n']
        l_acc = l_c / l_n * 100 if l_n > 0 else 0
        print(f"  {name:<12} high={h_acc:>5.1f}%({r['high_n']:>5})  "
              f"med={m_acc:>5.1f}%({m_n:>5})  low={l_acc:>5.1f}%({l_n:>5})")

    # 稳定性：逐周high准确率的标准差
    print(f"\n{'='*85}")
    print(f"  稳定性 (逐周high准确率的均值±标准差)")
    print(f"{'='*85}")
    for name in ['原始', 'B_deny', 'C正交']:
        week_accs = []
        for w in results[name]['weekly']:
            if w['high_n'] >= 5:
                week_accs.append(w['high_c'] / w['high_n'] * 100)
        if len(week_accs) >= 3:
            avg = sum(week_accs) / len(week_accs)
            std = (sum((a - avg)**2 for a in week_accs) / (len(week_accs) - 1)) ** 0.5
            mn, mx = min(week_accs), max(week_accs)
            print(f"  {name:<12} {avg:.1f}% ± {std:.1f}%  range=[{mn:.0f}%, {mx:.0f}%]  weeks={len(week_accs)}")
        else:
            print(f"  {name:<12} 数据不足(仅{len(week_accs)}周有>=5个high)")


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 85)
    print("  方案A/B/C大样本历史验证 (300只股票 × ~40周)")
    print("=" * 85)

    codes = load_sample_stocks()
    stock_klines = load_klines(codes)
    market_klines = load_market_klines()
    ff_map = load_fund_flow(codes)

    logger.info("构建周样本...")
    samples = build_weekly_samples(stock_klines, market_klines, ff_map)

    # 基本统计
    weeks = sorted(set(s['iso_week'] for s in samples))
    print(f"\n  样本: {len(samples)}个  周数: {len(weeks)}  "
          f"范围: W{weeks[0][1]:02d}~W{weeks[-1][1]:02d}")
    total_correct = sum(1 for s in samples if s['correct'])
    print(f"  V11规则总准确率: {total_correct}/{len(samples)} = {total_correct/len(samples)*100:.1f}%")

    up_samples = [s for s in samples if s['pred_up']]
    up_correct = sum(1 for s in up_samples if s['correct'])
    dn_samples = [s for s in samples if not s['pred_up']]
    dn_correct = sum(1 for s in dn_samples if s['correct'])
    print(f"  UP预测: {up_correct}/{len(up_samples)} = {_pct(up_correct, len(up_samples))}")
    print(f"  DOWN预测: {dn_correct}/{len(dn_samples)} = {_pct(dn_correct, len(dn_samples))}")

    results, sorted_weeks = simulate(samples)
    print_results(results, sorted_weeks)

    print(f"\n{'='*85}")
    print(f"  完成")
    print(f"{'='*85}")


if __name__ == '__main__':
    main()
