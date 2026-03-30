#!/usr/bin/env python3
"""
正交化多模型共识验证
====================
问题：V11/V20/V30三模型同时高置信时准确率(30.6%)反而低于V11单独(46.5%)，
因为三个模型共享"超跌"这个单一维度，共识只是三重放大同一个判断。

方案：将三个模型的职责正交化，让每个模型负责一个独立维度：
  维度1(方向判断): V11规则引擎 — 本周涨跌+大盘环境 → 预测方向
  维度2(资金确认): 量比+资金流 — 是否有资金支持该方向
  维度3(趋势确认): 板块动量+前周趋势 — 是否有中期趋势支撑

综合高置信 = 三个维度都确认（而非三个模型都说"跌多了"）

验证方法：用W11-W13实盘数据，对比正交共识 vs 原始三模型共识的准确率。

用法: .venv/bin/python day_week_predicted/tests/test_orthogonal_consensus.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import logging
from collections import defaultdict
from dao import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STOCK_FILTER = (
    "(h.stock_code LIKE '6%%.SH' OR h.stock_code LIKE '0%%.SZ' OR h.stock_code LIKE '3%%.SZ')"
    " AND h.stock_code NOT LIKE '399%%'"
    " AND h.stock_code != '000001.SH'"
)


# ═══════════════════════════════════════════════════════════════
# 正交维度定义
# ═══════════════════════════════════════════════════════════════

def dim1_direction(row) -> str | None:
    """维度1: V11方向判断。返回 'confirm'/'deny'/None。
    V11给出高置信预测方向时为confirm。
    """
    if not row.get('nw_pred_direction'):
        return None
    conf = row.get('nw_confidence') or 'low'
    if conf in ('high', 'reference'):
        return 'confirm'
    return None


def dim2_capital(row) -> str:
    """维度2: 资金面确认。返回 'confirm'/'deny'/'neutral'。

    逻辑：
      放量(vol_ratio>=1.0) = 有资金参与，确认方向
      缩量(vol_ratio<0.7) = 无资金支持，否定方向
      资金流入(fund_flow_signal>0) 额外加分

    这个维度与"超跌"正交：
      超跌+放量 = 有人抄底 → 可能反弹
      超跌+缩量 = 无人接盘 → 可能继续跌
    """
    vr = row.get('vol_ratio')
    ff = row.get('fund_flow_signal')

    if vr is None:
        return 'neutral'

    score = 0
    if vr >= 1.0:
        score += 1
    elif vr < 0.7:
        score -= 1

    if ff is not None:
        if ff > 0:
            score += 1
        elif ff < 0:
            score -= 1

    if score >= 1:
        return 'confirm'
    elif score <= -1:
        return 'deny'
    return 'neutral'


def dim3_trend(row) -> str:
    """维度3: 中期趋势确认。返回 'confirm'/'deny'/'neutral'。

    逻辑：
      板块动量>0 = 所属板块走强，确认方向
      板块动量<-2 = 所属板块走弱，否定方向
      前周涨跌与本周同向 = 趋势延续

    这个维度与"超跌"正交：
      超跌+板块强 = 个股超跌但板块健康 → 可能补涨
      超跌+板块弱 = 整个板块在跌 → 系统性风险
    """
    bm = row.get('board_momentum')
    # concept_consensus在当前数据中几乎全是0或null，不用

    if bm is None:
        return 'neutral'

    if bm > 0:
        return 'confirm'
    elif bm < -2:
        return 'deny'
    return 'neutral'


def compute_orthogonal_consensus(row) -> dict:
    """计算正交共识置信度。

    Returns:
        {
            'confidence': 'high'/'medium'/'low',
            'd1': 'confirm'/None,
            'd2': 'confirm'/'deny'/'neutral',
            'd3': 'confirm'/'deny'/'neutral',
            'confirm_count': int (0-3),
            'deny_count': int (0-3),
        }
    """
    d1 = dim1_direction(row)
    d2 = dim2_capital(row)
    d3 = dim3_trend(row)

    if d1 is None:
        return {'confidence': None, 'd1': d1, 'd2': d2, 'd3': d3,
                'confirm_count': 0, 'deny_count': 0}

    confirms = sum(1 for d in [d1, d2, d3] if d == 'confirm')
    denies = sum(1 for d in [d2, d3] if d == 'deny')

    # 置信度规则：
    # 3个confirm + 0个deny = high
    # 2个confirm + 0个deny = medium
    # 有deny = 降级
    if confirms >= 3 and denies == 0:
        confidence = 'high'
    elif confirms >= 2 and denies == 0:
        confidence = 'medium'
    elif denies >= 1:
        confidence = 'low'
    else:
        confidence = 'low'

    return {
        'confidence': confidence,
        'd1': d1, 'd2': d2, 'd3': d3,
        'confirm_count': confirms,
        'deny_count': denies,
    }


# ═══════════════════════════════════════════════════════════════
# 原始三模型共识（对照组）
# ═══════════════════════════════════════════════════════════════

def compute_original_consensus(row) -> dict:
    """计算原始三模型共识（V11+V20+V30同时高置信）。"""
    v11_high = (row.get('nw_pred_direction') is not None and
                (row.get('nw_confidence') or '') == 'high')
    v20_high = (row.get('v20_pred_direction') is not None and
                (row.get('v20_confidence') or '') == 'high')
    v30_high = (row.get('v30_pred_direction') is not None and
                (row.get('v30_confidence') or '') == 'high')

    high_count = int(v11_high) + int(v20_high) + int(v30_high)

    if high_count >= 3:
        confidence = 'high'
    elif high_count >= 2:
        confidence = 'medium'
    elif high_count >= 1:
        confidence = 'low'
    else:
        confidence = None

    return {
        'confidence': confidence,
        'v11_high': v11_high,
        'v20_high': v20_high,
        'v30_high': v30_high,
        'high_count': high_count,
    }


# ═══════════════════════════════════════════════════════════════
# 数据加载与验证
# ═══════════════════════════════════════════════════════════════

def load_data():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(f"""
            SELECT h.stock_code, h.iso_week,
                   h.nw_pred_direction, h.nw_confidence, h.nw_strategy,
                   h.nw_is_correct, h.nw_actual_weekly_chg,
                   h.actual_weekly_chg,
                   h.vol_ratio, h.fund_flow_signal, h.board_momentum,
                   h.concept_consensus, h.finance_score, h.vol_trend,
                   h.v20_pred_direction, h.v20_confidence, h.v20_rule_name,
                   h.v20_is_correct, h.v20_actual_5d_chg,
                   h.v30_pred_direction, h.v30_confidence,
                   h.v30_is_correct, h.v30_actual_5d_chg
            FROM stock_weekly_prediction_history h
            WHERE {STOCK_FILTER}
              AND h.iso_year = 2026
              AND h.iso_week IN (11, 12, 13)
            ORDER BY h.iso_week, h.stock_code
        """)
        rows = cur.fetchall()
        by_week = defaultdict(list)
        for r in rows:
            by_week[r['iso_week']].append(r)
        return by_week
    finally:
        cur.close()
        conn.close()


def _pct(n, d):
    return f"{n/d*100:.1f}%" if d > 0 else "-"


def run_comparison(by_week):
    """对比正交共识 vs 原始三模型共识。"""

    print(f"\n{'='*80}")
    print(f"  正交共识 vs 原始三模型共识 — 逐周对比")
    print(f"{'='*80}")

    # 表头
    header = (f"  {'':>4}  "
              f"{'原始三模型high':>18}  "
              f"{'正交high':>18}  "
              f"{'正交high+med':>18}  "
              f"{'V11高置信(基准)':>18}")
    print(header)
    print(f"  {'─'*76}")

    grand = {k: {'c': 0, 'n': 0} for k in
             ['orig3', 'orth_h', 'orth_hm', 'v11_h']}

    for week in sorted(by_week.keys()):
        rows = by_week[week]

        orig3_c = orig3_n = 0
        orth_h_c = orth_h_n = 0
        orth_hm_c = orth_hm_n = 0
        v11_h_c = v11_h_n = 0

        for row in rows:
            # 只看有V11 NW预测且已验证的
            if not (row.get('nw_pred_direction') and row.get('nw_is_correct') is not None):
                continue

            is_correct = bool(row['nw_is_correct'])

            # V11高置信基准
            if (row.get('nw_confidence') or '') == 'high':
                v11_h_n += 1
                v11_h_c += int(is_correct)

            # 原始三模型共识
            orig = compute_original_consensus(row)
            if orig['confidence'] == 'high':
                orig3_n += 1
                orig3_c += int(is_correct)

            # 正交共识
            orth = compute_orthogonal_consensus(row)
            if orth['confidence'] == 'high':
                orth_h_n += 1
                orth_h_c += int(is_correct)
            if orth['confidence'] in ('high', 'medium'):
                orth_hm_n += 1
                orth_hm_c += int(is_correct)

        print(f"  W{week:02d}  "
              f"{orig3_c:>3}/{orig3_n:>4}={_pct(orig3_c, orig3_n):>6}  "
              f"{orth_h_c:>3}/{orth_h_n:>4}={_pct(orth_h_c, orth_h_n):>6}  "
              f"{orth_hm_c:>3}/{orth_hm_n:>4}={_pct(orth_hm_c, orth_hm_n):>6}  "
              f"{v11_h_c:>3}/{v11_h_n:>4}={_pct(v11_h_c, v11_h_n):>6}")

        for k, c, n in [('orig3', orig3_c, orig3_n), ('orth_h', orth_h_c, orth_h_n),
                         ('orth_hm', orth_hm_c, orth_hm_n), ('v11_h', v11_h_c, v11_h_n)]:
            grand[k]['c'] += c; grand[k]['n'] += n

    print(f"  {'─'*76}")
    g = grand
    print(f"  汇总  "
          f"{g['orig3']['c']:>3}/{g['orig3']['n']:>4}={_pct(g['orig3']['c'], g['orig3']['n']):>6}  "
          f"{g['orth_h']['c']:>3}/{g['orth_h']['n']:>4}={_pct(g['orth_h']['c'], g['orth_h']['n']):>6}  "
          f"{g['orth_hm']['c']:>3}/{g['orth_hm']['n']:>4}={_pct(g['orth_hm']['c'], g['orth_hm']['n']):>6}  "
          f"{g['v11_h']['c']:>3}/{g['v11_h']['n']:>4}={_pct(g['v11_h']['c'], g['v11_h']['n']):>6}")


def detail_analysis(by_week):
    """正交维度的详细分层分析。"""

    print(f"\n{'='*80}")
    print(f"  正交维度详细分层 (所有有V11 NW预测的股票)")
    print(f"{'='*80}")

    all_rows = []
    for week in sorted(by_week.keys()):
        for row in by_week[week]:
            if row.get('nw_pred_direction') and row.get('nw_is_correct') is not None:
                all_rows.append(row)

    # 按维度2(资金面)分层
    print(f"\n  --- 维度2: 资金面 ---")
    for label, cond in [('confirm', 'confirm'), ('neutral', 'neutral'), ('deny', 'deny')]:
        subset = [r for r in all_rows if dim2_capital(r) == cond]
        if not subset:
            continue
        c = sum(1 for r in subset if r['nw_is_correct'])
        n = len(subset)
        avg_chg = sum(float(r.get('nw_actual_weekly_chg') or 0) for r in subset) / n
        print(f"    {label:>10}: {c}/{n} = {_pct(c, n)}  avg_chg={avg_chg:+.2f}%")

    # 按维度3(趋势)分层
    print(f"\n  --- 维度3: 趋势面 ---")
    for label, cond in [('confirm', 'confirm'), ('neutral', 'neutral'), ('deny', 'deny')]:
        subset = [r for r in all_rows if dim3_trend(r) == cond]
        if not subset:
            continue
        c = sum(1 for r in subset if r['nw_is_correct'])
        n = len(subset)
        avg_chg = sum(float(r.get('nw_actual_weekly_chg') or 0) for r in subset) / n
        print(f"    {label:>10}: {c}/{n} = {_pct(c, n)}  avg_chg={avg_chg:+.2f}%")

    # 正交共识分层
    print(f"\n  --- 正交共识分层 ---")
    by_conf = defaultdict(list)
    for r in all_rows:
        orth = compute_orthogonal_consensus(r)
        if orth['confidence']:
            by_conf[orth['confidence']].append(r)

    for conf in ['high', 'medium', 'low']:
        subset = by_conf.get(conf, [])
        if not subset:
            continue
        c = sum(1 for r in subset if r['nw_is_correct'])
        n = len(subset)
        avg_chg = sum(float(r.get('nw_actual_weekly_chg') or 0) for r in subset) / n
        print(f"    {conf:>10}: {c}/{n} = {_pct(c, n)}  avg_chg={avg_chg:+.2f}%")

    # deny维度的过滤效果
    print(f"\n  --- deny过滤效果 ---")
    has_deny = [r for r in all_rows if dim2_capital(r) == 'deny' or dim3_trend(r) == 'deny']
    no_deny = [r for r in all_rows if dim2_capital(r) != 'deny' and dim3_trend(r) != 'deny']
    if has_deny:
        c = sum(1 for r in has_deny if r['nw_is_correct'])
        print(f"    有deny维度: {c}/{len(has_deny)} = {_pct(c, len(has_deny))}")
    if no_deny:
        c = sum(1 for r in no_deny if r['nw_is_correct'])
        print(f"    无deny维度: {c}/{len(no_deny)} = {_pct(c, len(no_deny))}")

    # 与原始V20/V30的对比
    print(f"\n  --- 正交deny vs 原始V20/V30高置信 ---")
    # 被正交deny但被V20/V30看好的
    denied_but_v20v30_high = [r for r in all_rows
        if (dim2_capital(r) == 'deny' or dim3_trend(r) == 'deny')
        and (r.get('v20_confidence') == 'high' or r.get('v30_confidence') == 'high')]
    if denied_but_v20v30_high:
        c = sum(1 for r in denied_but_v20v30_high if r['nw_is_correct'])
        n = len(denied_but_v20v30_high)
        print(f"    正交deny + V20/V30高置信: {c}/{n} = {_pct(c, n)} (这些应该被过滤)")

    # 正交confirm但V20/V30无信号的
    confirmed_no_v20v30 = [r for r in all_rows
        if compute_orthogonal_consensus(r)['confidence'] in ('high', 'medium')
        and not r.get('v20_pred_direction')
        and not r.get('v30_pred_direction')]
    if confirmed_no_v20v30:
        c = sum(1 for r in confirmed_no_v20v30 if r['nw_is_correct'])
        n = len(confirmed_no_v20v30)
        print(f"    正交confirm + V20/V30无信号: {c}/{n} = {_pct(c, n)} (这些应该被保留)")


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 80)
    print("  正交化多模型共识验证")
    print("=" * 80)

    by_week = load_data()

    for week in sorted(by_week.keys()):
        rows = by_week[week]
        nw = sum(1 for r in rows if r.get('nw_pred_direction') and r.get('nw_is_correct') is not None)
        print(f"  W{week:02d}: {nw}条有V11 NW验证数据")

    run_comparison(by_week)
    detail_analysis(by_week)

    print(f"\n{'='*80}")
    print(f"  完成")
    print(f"{'='*80}")


if __name__ == '__main__':
    main()
