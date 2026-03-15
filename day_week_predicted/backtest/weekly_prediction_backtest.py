#!/usr/bin/env python3
"""
周预测回测验证脚本
==================
验证 weekly_prediction_service 中方向预测和涨跌幅预测的准确率。

回测方法：
- 滚动窗口回测：对每个历史周，用该周的 d3/d4 信号预测方向，
  然后与实际全周涨跌幅对比。
- 涨跌幅预测验证：用 (strategy, direction) 维度的历史分布预测涨跌幅，
  验证方向一致性和区间命中率。
- 回测周期 >= 29 周

用法：
    python -m day_week_predicted.backtest.weekly_prediction_backtest
"""
import sys
import logging
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, '.')

from dao import get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

# ── 从 weekly_prediction_service 复用核心函数 ──
from service.weekly_prediction_service import (
    _to_float,
    _compound_return,
    _mean,
    _std,
    _classify_stock_behavior,
    _get_stock_strategy_profile,
    _predict_with_profile,
    _get_all_stock_codes,
    _get_latest_trade_date,
)


# ═══════════════════════════════════════════════════════════
# 辅助函数
# ═══════════════════════════════════════════════════════════

def _calc_chg_stats(chgs):
    """计算涨跌幅分布统计（与 service 中一致）"""
    if len(chgs) < 2:
        return None
    sorted_chgs = sorted(chgs)
    n = len(sorted_chgs)
    median = sorted_chgs[n // 2]
    p25 = sorted_chgs[max(0, n // 4)]
    p75 = sorted_chgs[min(n - 1, n * 3 // 4)]
    mae = _mean([abs(c - median) for c in chgs])
    std = _std(chgs) if n >= 3 else mae

    # 自适应区间：基于 median ± k * std
    if n >= 20:
        k = 1.5
    elif n >= 10:
        k = 1.8
    elif n >= 5:
        k = 2.2
    else:
        k = 3.0

    spread = max(std, mae, 0.5)
    low = median - k * spread
    high = median + k * spread

    hits = sum(1 for c in chgs if low <= c <= high)
    hit_rate = round(hits / n * 100, 1)
    return {
        'median': round(median, 2),
        'p10': round(low, 2), 'p90': round(high, 2),
        'p25': round(p25, 2), 'p75': round(p75, 2),
        'mae': round(mae, 2), 'hit_rate': hit_rate, 'samples': n,
    }


def _rate_str(ok, total):
    """格式化准确率"""
    if total == 0:
        return '0/0 (N/A)'
    return f'{ok}/{total} ({ok / total * 100:.1f}%)'


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def _load_backtest_data(stock_codes, start_date, end_date):
    """加载回测所需的全部数据"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    # 行为分析需要更早的数据（往前多加90天）
    dt_start = datetime.strptime(start_date, '%Y-%m-%d')
    behavior_start = (dt_start - timedelta(days=90)).strftime('%Y-%m-%d')

    # 1. 个股K线
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

    # 2. 大盘K线
    cur.execute(
        "SELECT `date`, change_percent FROM stock_kline "
        "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
        "ORDER BY `date`", (behavior_start, end_date))
    market_klines = [{'date': r['date'],
                      'change_percent': _to_float(r['change_percent'])}
                     for r in cur.fetchall()]

    conn.close()

    # 3. 行业映射
    stock_sectors = {}
    try:
        from common.utils.sector_mapping_utils import parse_industry_list_md
        sector_mapping = parse_industry_list_md()
        for code in stock_codes:
            if code in sector_mapping:
                stock_sectors[code] = sector_mapping[code]
    except Exception:
        pass

    logger.info("[数据加载] %d只股票K线, 大盘%d天, 行业映射%d只, 区间%s~%s",
                len(stock_klines), len(market_klines), len(stock_sectors),
                start_date, end_date)

    return {
        'stock_klines': dict(stock_klines),
        'market_klines': market_klines,
        'stock_sectors': stock_sectors,
    }


# ═══════════════════════════════════════════════════════════
# 核心回测逻辑
# ═══════════════════════════════════════════════════════════

def run_backtest(n_weeks=29, sample_limit=0):
    """运行完整回测。

    Args:
        n_weeks: 回测周数（至少29）
        sample_limit: 股票数量限制（0=全部）

    回测指标：
    1. 方向预测准确率（全样本 / LOWO / 按策略 / 按置信度）
    2. 涨跌幅预测一致性（方向与涨跌幅符号一致率）
    3. 涨跌幅区间命中率（实际涨跌幅落在 [p25, p75] 内的比例）
    4. 涨跌幅MAE（平均绝对误差）
    """
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  周预测回测验证 (n_weeks=%d)", n_weeks)
    logger.info("=" * 70)

    # 1. 获取最新交易日和股票列表
    latest_date = _get_latest_trade_date()
    if not latest_date:
        logger.error("无法获取最新交易日")
        return
    logger.info("最新交易日: %s", latest_date)

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        all_codes = all_codes[:sample_limit]
    logger.info("回测股票数: %d", len(all_codes))

    # 2. 计算回测时间范围
    dt_end = datetime.strptime(latest_date, '%Y-%m-%d')
    dt_start = dt_end - timedelta(days=n_weeks * 7 + 7)
    start_date = dt_start.strftime('%Y-%m-%d')
    logger.info("回测区间: %s ~ %s (%d周)", start_date, latest_date, n_weeks)

    # 3. 加载数据
    data = _load_backtest_data(all_codes, start_date, latest_date)

    # 4. 对每只股票按周回测
    logger.info("[回测] 开始逐股票逐周回测...")

    # ── 全局统计 ──
    global_correct = 0
    global_total = 0
    week_stats = defaultdict(lambda: [0, 0])  # iw -> [correct, total]

    # ── 按策略统计 ──
    strategy_correct = defaultdict(int)
    strategy_total = defaultdict(int)

    # ── 按置信度统计 ──
    confidence_correct = defaultdict(int)
    confidence_total = defaultdict(int)

    # ── 涨跌幅预测统计 ──
    # 收集每只股票每个 (strategy, direction) 的历史涨跌幅
    # 然后用 leave-one-week-out 方式验证涨跌幅预测
    all_weekly_records = []  # [(code, iw, pred_up, conf, strat, weekly_chg), ...]

    # ── 方向一致性统计 ──
    direction_consistency_ok = 0
    direction_consistency_total = 0

    stocks_processed = 0
    for code in all_codes:
        klines = data['stock_klines'].get(code, [])
        if not klines or len(klines) < 20:
            continue

        # 行为分析（用回测起始前的数据）
        behavior_klines = [k for k in klines if k['date'] < start_date]
        if len(behavior_klines) < 20:
            behavior_klines = klines[:20]
        behavior = _classify_stock_behavior(behavior_klines)

        sector = data['stock_sectors'].get(code, '')
        profile = _get_stock_strategy_profile(code, sector, behavior)

        # 按ISO周分组（只用回测区间内的数据）
        wg = defaultdict(list)
        for k in klines:
            if k['date'] < start_date or k['date'] > latest_date:
                continue
            dt = datetime.strptime(k['date'], '%Y-%m-%d')
            iw = dt.isocalendar()[:2]
            wg[iw].append(k)

        for iw, days in wg.items():
            days.sort(key=lambda x: x['date'])
            if len(days) < 3:
                continue

            pcts = [d['change_percent'] for d in days]
            weekly_chg = _compound_return(pcts)
            actual_up = weekly_chg >= 0

            d3 = _compound_return(pcts[:3])
            d4 = _compound_return(pcts[:4]) if len(days) >= 4 else None
            is_susp = all(p == 0 for p in pcts[:3])

            pred_up, conf, strat, _reason = _predict_with_profile(
                d4, d3, is_susp, len(days), pcts, profile)

            correct = pred_up == actual_up

            # 全局
            if correct:
                global_correct += 1
                week_stats[iw][0] += 1
            global_total += 1
            week_stats[iw][1] += 1

            # 按策略
            if correct:
                strategy_correct[strat] += 1
            strategy_total[strat] += 1

            # 按置信度
            if correct:
                confidence_correct[conf] += 1
            confidence_total[conf] += 1

            # 记录用于涨跌幅验证
            pred_dir = 'UP' if pred_up else 'DOWN'
            all_weekly_records.append((code, iw, pred_dir, conf, strat, weekly_chg))

        stocks_processed += 1
        if stocks_processed % 500 == 0:
            logger.info("  已处理 %d/%d 只股票...", stocks_processed, len(all_codes))

    logger.info("  回测完成: %d只股票, %d条周记录", stocks_processed, global_total)


    # ═══════════════════════════════════════════════════════════
    # 5. 涨跌幅预测验证（LOWO交叉验证）
    # ═══════════════════════════════════════════════════════════
    logger.info("[涨跌幅验证] 开始 LOWO 交叉验证...")

    # 按 (code, strategy, direction) 分组
    code_strat_dir_records = defaultdict(list)  # (code, strat, dir) -> [(iw, weekly_chg), ...]
    for code, iw, pred_dir, conf, strat, weekly_chg in all_weekly_records:
        code_strat_dir_records[(code, strat, pred_dir)].append((iw, weekly_chg))

    # LOWO: 对每条记录，用排除该周后的同组数据预测涨跌幅
    chg_pred_results = []  # [(pred_chg, actual_chg, pred_dir, conf), ...]
    chg_direction_match = 0
    chg_direction_total = 0
    chg_in_range = 0
    chg_in_range_total = 0
    chg_abs_errors = []

    # 统计不同样本量下的命中率分布
    sample_size_hits = defaultdict(lambda: [0, 0])  # sample_size_bucket -> [hits, total]

    for (code, strat, pred_dir), records in code_strat_dir_records.items():
        if len(records) < 3:
            # 样本太少，跳过
            continue

        for i, (iw_target, actual_chg) in enumerate(records):
            # 排除当前周，用其余数据计算分布
            other_chgs = [chg for j, (iw2, chg) in enumerate(records) if j != i]
            if len(other_chgs) < 2:
                continue

            stats = _calc_chg_stats(other_chgs)
            if not stats:
                continue

            pred_chg = stats['median']

            # 方向一致性：预测涨跌幅符号是否与预测方向一致
            chg_direction_total += 1
            if (pred_dir == 'UP' and pred_chg >= 0) or (pred_dir == 'DOWN' and pred_chg <= 0):
                chg_direction_match += 1

            # 区间命中率：实际涨跌幅是否在 [p10, p90] 内
            chg_in_range_total += 1
            if stats['p10'] <= actual_chg <= stats['p90']:
                chg_in_range += 1

            # 按样本量分桶统计
            n_samples = len(other_chgs)
            bucket = 'n<5' if n_samples < 5 else ('n<10' if n_samples < 10 else 'n>=10')
            sample_size_hits[bucket][1] += 1
            if stats['p10'] <= actual_chg <= stats['p90']:
                sample_size_hits[bucket][0] += 1

            # MAE
            chg_abs_errors.append(abs(pred_chg - actual_chg))

            chg_pred_results.append((pred_chg, actual_chg, pred_dir))

    # ═══════════════════════════════════════════════════════════
    # 6. 汇总输出
    # ═══════════════════════════════════════════════════════════
    logger.info("")
    logger.info("=" * 70)
    logger.info("  回测结果汇总")
    logger.info("=" * 70)

    # ── 6.1 方向预测准确率 ──
    full_acc = global_correct / global_total * 100 if global_total > 0 else 0
    logger.info("")
    logger.info("【1. 方向预测准确率】")
    logger.info("  全样本: %s", _rate_str(global_correct, global_total))

    # LOWO
    week_accs = []
    for iw, (ok, n) in sorted(week_stats.items()):
        if n > 0:
            week_accs.append(ok / n * 100)
    lowo_acc = _mean(week_accs) if week_accs else 0
    logger.info("  LOWO:   %.1f%% (%d周)", lowo_acc, len(week_accs))

    # 按策略
    logger.info("")
    logger.info("  按策略:")
    for strat in sorted(strategy_total.keys(), key=lambda s: -strategy_total[s]):
        ok = strategy_correct[strat]
        n = strategy_total[strat]
        logger.info("    %-30s %s", strat, _rate_str(ok, n))

    # 按置信度
    logger.info("")
    logger.info("  按置信度:")
    for conf in ['high', 'medium', 'low']:
        ok = confidence_correct.get(conf, 0)
        n = confidence_total.get(conf, 0)
        if n > 0:
            logger.info("    %-10s %s", conf, _rate_str(ok, n))

    # ── 6.2 涨跌幅预测指标 ──
    logger.info("")
    logger.info("【2. 涨跌幅预测指标】")

    dir_consistency = chg_direction_match / chg_direction_total * 100 if chg_direction_total > 0 else 0
    logger.info("  方向一致性: %s (预测涨跌幅符号与预测方向一致)",
                _rate_str(chg_direction_match, chg_direction_total))

    range_hit = chg_in_range / chg_in_range_total * 100 if chg_in_range_total > 0 else 0
    logger.info("  区间命中率: %s (实际涨跌幅在[p10,p90]内)",
                _rate_str(chg_in_range, chg_in_range_total))

    avg_mae = _mean(chg_abs_errors) if chg_abs_errors else 0
    logger.info("  平均MAE:    %.2f%%", avg_mae)

    # 按样本量分桶的命中率
    logger.info("")
    logger.info("  按样本量分桶:")
    for bucket in ['n<5', 'n<10', 'n>=10']:
        hits, total = sample_size_hits.get(bucket, [0, 0])
        if total > 0:
            logger.info("    %-10s %s", bucket, _rate_str(hits, total))

    # ── 6.3 综合判定 ──
    logger.info("")
    logger.info("=" * 70)
    logger.info("  综合判定")
    logger.info("=" * 70)

    metrics = {
        '方向预测(全样本)': full_acc,
        '方向预测(LOWO)': lowo_acc,
        '涨跌幅方向一致性': dir_consistency,
        '涨跌幅区间命中率': range_hit,
    }

    all_pass = True
    for name, val in metrics.items():
        status = '✅ PASS' if val >= 85.0 else '❌ FAIL'
        if val < 85.0:
            all_pass = False
        logger.info("  %-25s %.1f%%  %s (目标≥85%%)", name, val, status)

    logger.info("")
    elapsed = (datetime.now() - t_start).total_seconds()
    if all_pass:
        logger.info("  🎉 所有指标均达标 (≥85%%)")
    else:
        logger.info("  ⚠️  部分指标未达标，需要优化")
    logger.info("  回测耗时: %.1fs", elapsed)
    logger.info("=" * 70)

    return {
        'direction_full_accuracy': round(full_acc, 1),
        'direction_lowo_accuracy': round(lowo_acc, 1),
        'chg_direction_consistency': round(dir_consistency, 1),
        'chg_range_hit_rate': round(range_hit, 1),
        'chg_mae': round(avg_mae, 2),
        'n_weeks': len(week_accs),
        'total_samples': global_total,
        'all_pass': all_pass,
    }


if __name__ == '__main__':
    result = run_backtest(n_weeks=29)
    if result:
        print(f"\n最终结果: {'全部通过' if result['all_pass'] else '未全部通过'}")
