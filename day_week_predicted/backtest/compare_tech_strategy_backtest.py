#!/usr/bin/env python3
"""
科技行业策略改动 A/B 回测对比
==============================
对比改动前(momentum)和改动后(adaptive)对科技行业股票的准确率影响。

同时输出：
- 科技行业整体准确率变化
- 科创板(688)子集准确率变化
- 非科创板科技股准确率变化
- 其他行业（不受影响）的准确率作为对照组
- 按 reversal_rate 分桶的准确率变化
- 易思维等典型股票的逐周预测明细

用法：
    python -m day_week_predicted.backtest.compare_tech_strategy_backtest
"""
import sys
import logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')

from dao import get_connection
from service.weekly_prediction_service import (
    _to_float, _compound_return, _mean, _std,
    _classify_stock_behavior, _predict_with_profile,
    _get_all_stock_codes, _get_latest_trade_date,
    _STRATEGY_PROFILES,
)
from common.utils.sector_mapping_utils import parse_industry_list_md

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)


# ── 策略配置生成（模拟改动前/后） ──

def _make_profile_old(code, sector, behavior):
    """改动前：科技 → momentum（固定 follow）"""
    if sector == '科技':
        return _STRATEGY_PROFILES['momentum'].copy()
    # 其他行业走正常逻辑（与改动后一致）
    return _make_profile_new(code, sector, behavior)


def _make_profile_new(code, sector, behavior):
    """改动后：科技 → adaptive（根据个股行为自动选择）"""
    from service.weekly_prediction_service import _get_stock_strategy_profile
    return _get_stock_strategy_profile(code, sector, behavior)


def _rate(ok, total):
    if total == 0:
        return 'N/A'
    return f'{ok}/{total} ({ok / total * 100:.1f}%)'


# ── 数据加载 ──

def _load_data(stock_codes, start_date, end_date):
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    dt_start = datetime.strptime(start_date, '%Y-%m-%d')
    behavior_start = (dt_start - timedelta(days=90)).strftime('%Y-%m-%d')

    stock_klines = defaultdict(list)
    batch_size = 200
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, change_percent "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [behavior_start, end_date])
        for row in cur.fetchall():
            stock_klines[row['stock_code']].append({
                'date': row['date'],
                'change_percent': _to_float(row['change_percent']),
            })
    conn.close()

    stock_sectors = {}
    sector_mapping = parse_industry_list_md()
    for code in stock_codes:
        if code in sector_mapping:
            stock_sectors[code] = sector_mapping[code]

    logger.info("[数据] %d只股票K线, 行业映射%d只", len(stock_klines), len(stock_sectors))
    return dict(stock_klines), stock_sectors


# ── 核心回测 ──

def run_compare(n_weeks=29):
    t0 = datetime.now()
    logger.info("=" * 70)
    logger.info("  科技行业策略 A/B 回测对比 (n_weeks=%d)", n_weeks)
    logger.info("=" * 70)

    latest_date = _get_latest_trade_date()
    if not latest_date:
        logger.error("无法获取最新交易日")
        return
    logger.info("最新交易日: %s", latest_date)

    all_codes = _get_all_stock_codes()
    logger.info("总股票数: %d", len(all_codes))

    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=n_weeks * 7 + 7)
    start_date = dt_start.strftime('%Y-%m-%d')
    logger.info("回测区间: %s ~ %s", start_date, latest_date)

    stock_klines, stock_sectors = _load_data(all_codes, start_date, latest_date)

    # 分组
    tech_codes = [c for c in all_codes if stock_sectors.get(c) == '科技']
    kcb_codes = set(c for c in tech_codes if c.startswith('688'))
    non_tech_codes = [c for c in all_codes if stock_sectors.get(c, '') != '科技'
                      and stock_sectors.get(c, '') != '']
    logger.info("科技股: %d (科创板%d), 非科技股: %d",
                len(tech_codes), len(kcb_codes), len(non_tech_codes))

    # 关注的典型股票
    watch_stocks = {}
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines:
            continue
        # 找易思维
        if '688686' in code:
            watch_stocks[code] = '易思维'
        elif '688816' in code:
            watch_stocks[code] = '易思维(备选)'

    # ── 逐股票回测 ──
    # 统计结构: {group: {version: [correct, total]}}
    groups = ['科技_全部', '科技_科创板', '科技_非科创板', '非科技_对照组', '全市场']
    stats = {g: {'old': [0, 0], 'new': [0, 0]} for g in groups}

    # 按 reversal_rate 分桶
    rr_buckets = {'rr<0.35': {'old': [0, 0], 'new': [0, 0]},
                  'rr_0.35~0.55': {'old': [0, 0], 'new': [0, 0]},
                  'rr>0.55': {'old': [0, 0], 'new': [0, 0]}}

    # 按策略分桶（仅科技股）
    strat_stats_old = defaultdict(lambda: [0, 0])
    strat_stats_new = defaultdict(lambda: [0, 0])

    # 典型股票逐周明细
    watch_details = defaultdict(list)  # code -> [(week, actual, pred_old, pred_new), ...]

    # 个股准确率变化（仅科技股）
    per_stock_old = defaultdict(lambda: [0, 0])
    per_stock_new = defaultdict(lambda: [0, 0])

    processed = 0
    for code in all_codes:
        klines = stock_klines.get(code, [])
        if not klines or len(klines) < 20:
            continue

        sector = stock_sectors.get(code, '')
        is_tech = sector == '科技'
        is_kcb = code.startswith('688')

        # 行为分析
        behavior_klines = [k for k in klines if k['date'] < start_date]
        if len(behavior_klines) < 20:
            behavior_klines = klines[:20]
        behavior = _classify_stock_behavior(behavior_klines)
        rr = behavior.get('reversal_rate', 0)

        # 生成两套 profile
        profile_old = _make_profile_old(code, sector, behavior)
        profile_new = _make_profile_new(code, sector, behavior)

        # 按周分组
        wg = defaultdict(list)
        for k in klines:
            if k['date'] < start_date or k['date'] > latest_date:
                continue
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        for iw, days in sorted(wg.items()):
            days.sort(key=lambda x: x['date'])
            if len(days) < 3:
                continue

            pcts = [d['change_percent'] for d in days]
            weekly_chg = _compound_return(pcts)
            actual_up = weekly_chg >= 0

            d3 = _compound_return(pcts[:3])
            d4 = _compound_return(pcts[:4]) if len(days) >= 4 else None
            is_susp = all(p == 0 for p in pcts[:3])

            pred_old_up, conf_old, strat_old, _ = _predict_with_profile(
                d4, d3, is_susp, len(days), pcts, profile_old)
            pred_new_up, conf_new, strat_new, _ = _predict_with_profile(
                d4, d3, is_susp, len(days), pcts, profile_new)

            correct_old = pred_old_up == actual_up
            correct_new = pred_new_up == actual_up

            # 全市场
            stats['全市场']['old'][1] += 1
            stats['全市场']['new'][1] += 1
            if correct_old: stats['全市场']['old'][0] += 1
            if correct_new: stats['全市场']['new'][0] += 1

            if is_tech:
                for g in ['科技_全部']:
                    stats[g]['old'][1] += 1
                    stats[g]['new'][1] += 1
                    if correct_old: stats[g]['old'][0] += 1
                    if correct_new: stats[g]['new'][0] += 1

                if is_kcb:
                    stats['科技_科创板']['old'][1] += 1
                    stats['科技_科创板']['new'][1] += 1
                    if correct_old: stats['科技_科创板']['old'][0] += 1
                    if correct_new: stats['科技_科创板']['new'][0] += 1
                else:
                    stats['科技_非科创板']['old'][1] += 1
                    stats['科技_非科创板']['new'][1] += 1
                    if correct_old: stats['科技_非科创板']['old'][0] += 1
                    if correct_new: stats['科技_非科创板']['new'][0] += 1

                # reversal_rate 分桶
                if rr < 0.35:
                    bucket = 'rr<0.35'
                elif rr <= 0.55:
                    bucket = 'rr_0.35~0.55'
                else:
                    bucket = 'rr>0.55'
                rr_buckets[bucket]['old'][1] += 1
                rr_buckets[bucket]['new'][1] += 1
                if correct_old: rr_buckets[bucket]['old'][0] += 1
                if correct_new: rr_buckets[bucket]['new'][0] += 1

                # 策略分桶
                strat_stats_old[strat_old][1] += 1
                strat_stats_new[strat_new][1] += 1
                if correct_old: strat_stats_old[strat_old][0] += 1
                if correct_new: strat_stats_new[strat_new][0] += 1

                # 个股
                per_stock_old[code][1] += 1
                per_stock_new[code][1] += 1
                if correct_old: per_stock_old[code][0] += 1
                if correct_new: per_stock_new[code][0] += 1

            elif sector:
                stats['非科技_对照组']['old'][1] += 1
                stats['非科技_对照组']['new'][1] += 1
                if correct_old: stats['非科技_对照组']['old'][0] += 1
                if correct_new: stats['非科技_对照组']['new'][0] += 1

            # 典型股票明细
            if code in watch_stocks:
                week_str = f"Y{iw[0]}-W{iw[1]:02d}"
                actual_dir = '涨' if actual_up else '跌'
                old_dir = '涨' if pred_old_up else '跌'
                new_dir = '涨' if pred_new_up else '跌'
                watch_details[code].append({
                    'week': week_str,
                    'actual': f'{actual_dir}({weekly_chg:+.2f}%)',
                    'old': f'{old_dir}({strat_old},{conf_old})',
                    'new': f'{new_dir}({strat_new},{conf_new})',
                    'old_ok': '✓' if correct_old else '✗',
                    'new_ok': '✓' if correct_new else '✗',
                })

        processed += 1
        if processed % 500 == 0:
            logger.info("  已处理 %d 只...", processed)

    elapsed = (datetime.now() - t0).total_seconds()
    logger.info("  回测完成, 耗时 %.1fs", elapsed)

    # ═══════════════════════════════════════════════════════════
    # 输出结果
    # ═══════════════════════════════════════════════════════════
    print()
    print("=" * 80)
    print("  A/B 回测对比结果")
    print("  A = 改动前(科技→momentum)  B = 改动后(科技→adaptive)")
    print("=" * 80)

    # 1. 分组准确率
    print()
    print(f"{'分组':<20} {'A(改动前)':<20} {'B(改动后)':<20} {'差异':>8}")
    print("-" * 70)
    for g in groups:
        old_ok, old_n = stats[g]['old']
        new_ok, new_n = stats[g]['new']
        old_pct = old_ok / old_n * 100 if old_n > 0 else 0
        new_pct = new_ok / new_n * 100 if new_n > 0 else 0
        diff = new_pct - old_pct
        diff_str = f'{diff:+.2f}%' if old_n > 0 else 'N/A'
        print(f"{g:<20} {_rate(old_ok, old_n):<20} {_rate(new_ok, new_n):<20} {diff_str:>8}")

    # 2. reversal_rate 分桶
    print()
    print("科技股按 reversal_rate 分桶:")
    print(f"{'分桶':<20} {'A(改动前)':<20} {'B(改动后)':<20} {'差异':>8}")
    print("-" * 70)
    for bucket in ['rr<0.35', 'rr_0.35~0.55', 'rr>0.55']:
        old_ok, old_n = rr_buckets[bucket]['old']
        new_ok, new_n = rr_buckets[bucket]['new']
        old_pct = old_ok / old_n * 100 if old_n > 0 else 0
        new_pct = new_ok / new_n * 100 if new_n > 0 else 0
        diff = new_pct - old_pct
        print(f"{bucket:<20} {_rate(old_ok, old_n):<20} {_rate(new_ok, new_n):<20} {diff:+.2f}%")

    # 3. 策略分布变化（科技股）
    print()
    print("科技股策略分布变化:")
    print(f"  改动前(momentum固定):")
    for s in sorted(strat_stats_old, key=lambda x: -strat_stats_old[x][1]):
        ok, n = strat_stats_old[s]
        print(f"    {s:<30} {_rate(ok, n)}")
    print(f"  改动后(adaptive自适应):")
    for s in sorted(strat_stats_new, key=lambda x: -strat_stats_new[x][1]):
        ok, n = strat_stats_new[s]
        print(f"    {s:<30} {_rate(ok, n)}")

    # 4. 准确率变化最大的个股 TOP20
    print()
    print("科技股准确率变化 TOP20 (提升最大):")
    print(f"{'股票代码':<15} {'A(改动前)':<18} {'B(改动后)':<18} {'差异':>8}")
    print("-" * 60)
    diffs = []
    for code in per_stock_old:
        old_ok, old_n = per_stock_old[code]
        new_ok, new_n = per_stock_new[code]
        if old_n < 5:
            continue
        old_pct = old_ok / old_n * 100
        new_pct = new_ok / new_n * 100
        diffs.append((code, old_ok, old_n, new_ok, new_n, new_pct - old_pct))

    diffs.sort(key=lambda x: -x[5])
    for code, old_ok, old_n, new_ok, new_n, diff in diffs[:20]:
        print(f"{code:<15} {_rate(old_ok, old_n):<18} {_rate(new_ok, new_n):<18} {diff:+.1f}%")

    # 5. 准确率下降最大的个股 TOP10
    print()
    print("科技股准确率变化 BOTTOM10 (下降最大):")
    print(f"{'股票代码':<15} {'A(改动前)':<18} {'B(改动后)':<18} {'差异':>8}")
    print("-" * 60)
    diffs.sort(key=lambda x: x[5])
    for code, old_ok, old_n, new_ok, new_n, diff in diffs[:10]:
        print(f"{code:<15} {_rate(old_ok, old_n):<18} {_rate(new_ok, new_n):<18} {diff:+.1f}%")

    # 6. 典型股票逐周明细
    for code, name in watch_stocks.items():
        details = watch_details.get(code, [])
        if not details:
            continue
        print()
        print(f"典型股票: {name} ({code})")
        old_ok_cnt = sum(1 for d in details if d['old_ok'] == '✓')
        new_ok_cnt = sum(1 for d in details if d['new_ok'] == '✓')
        print(f"  改动前准确率: {_rate(old_ok_cnt, len(details))}")
        print(f"  改动后准确率: {_rate(new_ok_cnt, len(details))}")
        print(f"  {'周':<12} {'实际':<16} {'A(改动前)':<28} {'B(改动后)':<28}")
        print("  " + "-" * 80)
        for d in details:
            print(f"  {d['week']:<12} {d['actual']:<16} "
                  f"{d['old_ok']} {d['old']:<25} {d['new_ok']} {d['new']:<25}")

    print()
    print("=" * 80)
    print(f"  回测耗时: {elapsed:.1f}s")
    print("=" * 80)


if __name__ == '__main__':
    run_compare(n_weeks=29)
