#!/usr/bin/env python3
"""
多因子预测体系 — 回测验证主程序
================================
基于已有数据库数据，执行完整的因子回测验证流程：

  1. 从 stock_kline / stock_finance / stock_fund_flow 加载历史数据
  2. 按周滚动计算因子
  3. 执行 Purged K-Fold CV + Walk-Forward 回测
  4. 输出因子IC报告 + 过拟合诊断

用法：
    python -m tools.run_factor_backtest [--weeks 30] [--stocks 200]
"""
import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dao import get_connection
from service.factor_prediction.factor_engine import (
    compute_price_volume_factors,
    compute_fundamental_factors,
    compute_alternative_factors,
)
from service.factor_prediction.prediction_system import (
    FactorPredictionEngine,
    FACTOR_DIRECTIONS,
)
from service.factor_prediction.backtest_validation import (
    compute_factor_ic,
    purged_kfold_backtest,
    walk_forward_backtest,
    diagnose_overfitting,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "data_results"


def _to_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


# ═══════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════

def load_stock_codes(limit: int = 200) -> list[str]:
    """获取有足够K线数据的A股代码（排除北交所、ST）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT stock_code, COUNT(*) AS cnt
        FROM stock_kline
        WHERE stock_code NOT LIKE '%%.BJ'
        GROUP BY stock_code
        HAVING cnt >= 120
        ORDER BY cnt DESC
        LIMIT %s
    """, (limit,))
    codes = [r['stock_code'] for r in cur.fetchall()]
    cur.close()
    conn.close()
    logger.info("加载 %d 只股票", len(codes))
    return codes


def load_kline_data(stock_codes: list[str], start_date: str, end_date: str) -> dict:
    """加载K线数据。返回 {code: [{date, close, open, high, low, volume, ...}]}"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, close_price, open_price, high_price, "
            f"low_price, trading_volume, change_percent, change_hand "
            f"FROM stock_kline WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
            batch + [start_date, end_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'close': _to_float(row['close_price']),
                'open': _to_float(row['open_price']),
                'high': _to_float(row['high_price']),
                'low': _to_float(row['low_price']),
                'volume': _to_float(row['trading_volume']),
                'change_percent': _to_float(row['change_percent']),
                'turnover': _to_float(row.get('change_hand')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_finance_data(stock_codes: list[str]) -> dict:
    """加载财报数据。返回 {code: [record_dict, ...]}（按报告日期降序）"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = {}
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, report_date, data_json "
            f"FROM stock_finance WHERE stock_code IN ({ph}) "
            f"ORDER BY report_date DESC",
            batch)
        for row in cur.fetchall():
            code = row['stock_code']
            if code not in result:
                result[code] = []
            try:
                data = json.loads(row['data_json']) if isinstance(row['data_json'], str) else row['data_json']
                result[code].append(data)
            except (json.JSONDecodeError, TypeError):
                pass
    cur.close()
    conn.close()
    return result


def load_fund_flow_data(stock_codes: list[str], start_date: str) -> dict:
    """加载资金流向数据。返回 {code: [{date, big_net_pct, net_flow, ...}]}"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, `date`, big_net_pct, net_flow, main_net_5day "
            f"FROM stock_fund_flow WHERE stock_code IN ({ph}) "
            f"AND `date` >= %s ORDER BY `date` DESC",
            batch + [start_date])
        for row in cur.fetchall():
            result[row['stock_code']].append({
                'date': str(row['date']),
                'big_net_pct': _to_float(row.get('big_net_pct')),
                'net_flow': _to_float(row.get('net_flow')),
                'main_net_5day': _to_float(row.get('main_net_5day')),
            })
    cur.close()
    conn.close()
    return dict(result)


def load_concept_strength(stock_codes: list[str]) -> dict:
    """加载概念板块强弱势评分。返回 {code: [{strength_score, ...}]}"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    result = defaultdict(list)
    bs = 300
    for i in range(0, len(stock_codes), bs):
        batch = stock_codes[i:i + bs]
        ph = ','.join(['%s'] * len(batch))
        cur.execute(
            f"SELECT stock_code, board_code, board_name, strength_score, strength_level "
            f"FROM stock_concept_strength WHERE stock_code IN ({ph})",
            batch)
        for row in cur.fetchall():
            result[row['stock_code']].append(dict(row))
    cur.close()
    conn.close()
    return dict(result)


def load_market_klines(start_date: str, end_date: str) -> dict:
    """加载大盘指数K线。返回 {index_code: [{date, change_percent}]}"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    indices = ['000001.SH', '399001.SZ', '899050.SZ']
    ph = ','.join(['%s'] * len(indices))
    cur.execute(
        f"SELECT stock_code, `date`, change_percent, close_price "
        f"FROM stock_kline WHERE stock_code IN ({ph}) "
        f"AND `date` >= %s AND `date` <= %s ORDER BY `date`",
        indices + [start_date, end_date])
    result = defaultdict(list)
    for row in cur.fetchall():
        result[row['stock_code']].append({
            'date': str(row['date']),
            'change_percent': _to_float(row['change_percent']),
            'close': _to_float(row.get('close_price')),
        })
    cur.close()
    conn.close()
    return dict(result)


# ═══════════════════════════════════════════════════════════
# 按周组织因子数据（回测核心）
# ═══════════════════════════════════════════════════════════

def build_weekly_factor_data(stock_codes: list[str], kline_data: dict,
                              finance_data: dict, fund_flow_data: dict,
                              concept_data: dict, market_data: dict,
                              n_weeks: int = 30) -> dict:
    """
    按ISO周组织因子和下周收益数据，供回测使用。

    Returns:
        {iso_week_key: {code: {'factors': dict, 'actual_return': float}}}
    """
    # 按周分组K线
    def _group_by_week(klines):
        weeks = defaultdict(list)
        for k in klines:
            d = datetime.strptime(k['date'][:10], '%Y-%m-%d')
            iso = d.isocalendar()
            key = f"{iso[0]}-W{iso[1]:02d}"
            weeks[key].append(k)
        return dict(weeks)

    # 获取大盘K线（默认上证）
    market_klines = market_data.get('000001.SH', [])

    all_weekly_data = defaultdict(dict)
    processed = 0

    for code in stock_codes:
        klines = kline_data.get(code, [])
        if len(klines) < 80:
            continue

        weekly_groups = _group_by_week(klines)
        sorted_week_keys = sorted(weekly_groups.keys())

        if len(sorted_week_keys) < n_weeks + 2:
            continue

        # 取最近 n_weeks 周
        target_weeks = sorted_week_keys[-(n_weeks + 1):-1]  # 最后一周留作"下周"

        for wi, week_key in enumerate(target_weeks):
            # 找到本周最后一天在klines中的位置
            week_klines = weekly_groups[week_key]
            last_date = week_klines[-1]['date']

            # 找到该日期在完整klines中的索引
            idx = None
            for j, k in enumerate(klines):
                if k['date'] == last_date:
                    idx = j
                    break
            if idx is None or idx < 60:
                continue

            # 用截至本周末的历史数据计算因子
            hist_klines = klines[:idx + 1]
            factors = compute_price_volume_factors(hist_klines)

            # 基本面因子
            fin = finance_data.get(code, [])
            if fin:
                factors.update(compute_fundamental_factors(fin))

            # 另类因子
            ff = fund_flow_data.get(code, [])
            cs = concept_data.get(code, [])
            # 获取对应大盘K线
            mkt_idx_code = '399001.SZ' if code.endswith('.SZ') else '000001.SH'
            mkt_klines = market_data.get(mkt_idx_code, market_klines)
            # 截取到本周末的大盘数据
            mkt_hist = [m for m in mkt_klines if m['date'] <= last_date]

            alt = compute_alternative_factors(
                fund_flow=ff[:20] if ff else [],
                concept_strength=cs,
                market_klines=mkt_hist,
                stock_klines=hist_klines,
            )
            factors.update(alt)

            # 下周实际收益
            next_week_key_idx = sorted_week_keys.index(week_key) + 1 if week_key in sorted_week_keys else None
            actual_return = None
            if next_week_key_idx and next_week_key_idx < len(sorted_week_keys):
                next_week_key = sorted_week_keys[next_week_key_idx]
                next_klines = weekly_groups.get(next_week_key, [])
                if next_klines and week_klines:
                    base_close = week_klines[-1].get('close', 0)
                    end_close = next_klines[-1].get('close', 0)
                    if base_close > 0 and end_close > 0:
                        actual_return = (end_close / base_close - 1) * 100

            if factors and actual_return is not None:
                all_weekly_data[week_key][code] = {
                    'factors': factors,
                    'actual_return': actual_return,
                }

        processed += 1
        if processed % 50 == 0:
            logger.info("已处理 %d/%d 只股票", processed, len(stock_codes))

    logger.info("构建完成: %d 周, 平均每周 %.0f 只股票",
                len(all_weekly_data),
                sum(len(v) for v in all_weekly_data.values()) / max(len(all_weekly_data), 1))
    return dict(all_weekly_data)


# ═══════════════════════════════════════════════════════════
# 主程序
# ═══════════════════════════════════════════════════════════

def run_full_backtest(n_weeks: int = 30, n_stocks: int = 200):
    """执行完整的多因子回测验证。"""
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("多因子预测体系 — 回测验证")
    logger.info("参数: weeks=%d, stocks=%d", n_weeks, n_stocks)
    logger.info("=" * 60)

    # 1. 加载数据
    logger.info("[1/5] 加载股票列表...")
    stock_codes = load_stock_codes(n_stocks)

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=n_weeks * 7 + 120)).strftime('%Y-%m-%d')

    logger.info("[2/5] 加载K线数据 (%s ~ %s)...", start_date, end_date)
    kline_data = load_kline_data(stock_codes, start_date, end_date)
    finance_data = load_finance_data(stock_codes)
    fund_flow_data = load_fund_flow_data(stock_codes, start_date)
    concept_data = load_concept_strength(stock_codes)
    market_data = load_market_klines(start_date, end_date)

    logger.info("  K线: %d只, 财报: %d只, 资金流: %d只, 概念: %d只",
                len(kline_data), len(finance_data), len(fund_flow_data), len(concept_data))

    # 2. 构建周度因子数据
    logger.info("[3/5] 构建周度因子数据...")
    weekly_data = build_weekly_factor_data(
        stock_codes, kline_data, finance_data, fund_flow_data,
        concept_data, market_data, n_weeks=n_weeks,
    )

    if not weekly_data:
        logger.error("无法构建周度数据，退出")
        return

    # 3. 单因子IC检验
    logger.info("[4/5] 执行因子IC检验 + 交叉验证...")
    all_weeks = sorted(weekly_data.keys())

    # 全样本IC
    factor_ic_report = {}
    for fname in FACTOR_DIRECTIONS:
        ics = []
        for week in all_weeks:
            week_d = weekly_data[week]
            if len(week_d) < 10:
                continue
            sf = {c: d['factors'] for c, d in week_d.items()}
            sr = {c: d['actual_return'] for c, d in week_d.items()}
            ic_r = compute_factor_ic(sf, sr, fname)
            if ic_r['ic'] is not None:
                ics.append(ic_r['ic'])
        if ics:
            mean_ic = sum(ics) / len(ics)
            std_ic = (sum((v - mean_ic) ** 2 for v in ics) / max(len(ics) - 1, 1)) ** 0.5
            ir = mean_ic / std_ic if std_ic > 0 else 0
            factor_ic_report[fname] = {
                'mean_ic': round(mean_ic, 4),
                'std_ic': round(std_ic, 4),
                'ir': round(ir, 4),
                'n_periods': len(ics),
                'significant': abs(mean_ic) > 0.02 and abs(ir) > 0.3,
            }

    # 4. Purged K-Fold CV
    cv_result = purged_kfold_backtest(weekly_data, n_folds=5, purge_weeks=1)

    # 4b. IC加权模式
    ic_weights = {}
    for fname, info in factor_ic_report.items():
        if info.get('significant') or abs(info.get('mean_ic', 0)) > 0.015:
            ic_weights[fname] = abs(info['mean_ic'])

    logger.info("IC加权因子: %d个 — %s", len(ic_weights), list(ic_weights.keys()))

    cv_result_ic = purged_kfold_backtest(weekly_data, n_folds=5, purge_weeks=1)

    # 5. Walk-Forward（等权 + IC加权 + 高置信度过滤）
    wf_result = walk_forward_backtest(weekly_data, train_window=16, test_window=4, purge_weeks=1)
    wf_result_ic = walk_forward_backtest(weekly_data, train_window=16, test_window=4,
                                          purge_weeks=1, ic_weights=ic_weights)
    wf_result_high = walk_forward_backtest(weekly_data, train_window=16, test_window=4,
                                            purge_weeks=1, confidence_filter='medium')
    wf_result_extreme = walk_forward_backtest(weekly_data, train_window=16, test_window=4,
                                               purge_weeks=1, extreme_only=True)
    wf_result_extreme_ic = walk_forward_backtest(weekly_data, train_window=16, test_window=4,
                                                  purge_weeks=1, ic_weights=ic_weights,
                                                  extreme_only=True)
    wf_result_strong = walk_forward_backtest(weekly_data, train_window=16, test_window=4,
                                              purge_weeks=1, strong_signal_only=True)
    wf_result_strong_extreme = walk_forward_backtest(weekly_data, train_window=16, test_window=4,
                                                      purge_weeks=1, strong_signal_only=True,
                                                      extreme_only=True)

    # 6. 过拟合诊断
    logger.info("[5/5] 过拟合诊断...")
    overfit_diag = diagnose_overfitting(cv_result, wf_result)

    # 输出报告
    report = {
        'meta': {
            'n_stocks': len(stock_codes),
            'n_weeks': n_weeks,
            'date_range': f"{start_date} ~ {end_date}",
            'run_time_sec': round(time.time() - t0, 1),
        },
        'factor_ic_report': factor_ic_report,
        'purged_cv': cv_result,
        'walk_forward_equal_weight': wf_result,
        'walk_forward_ic_weighted': wf_result_ic,
        'walk_forward_high_confidence': wf_result_high,
        'walk_forward_extreme': wf_result_extreme,
        'walk_forward_extreme_ic': wf_result_extreme_ic,
        'walk_forward_strong_signal': wf_result_strong,
        'walk_forward_strong_extreme': wf_result_strong_extreme,
        'overfitting_diagnosis': overfit_diag,
        'ic_weighted_factors': ic_weights,
    }

    # 保存结果
    output_path = OUTPUT_DIR / "factor_prediction_backtest_result.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info("结果已保存: %s", output_path)

    # 打印摘要
    print("\n" + "=" * 60)
    print("多因子预测体系 — 回测结果摘要")
    print("=" * 60)

    print(f"\n📊 数据规模: {len(stock_codes)}只股票, {len(weekly_data)}周")

    # 有效因子
    sig_factors = {k: v for k, v in factor_ic_report.items() if v.get('significant')}
    print(f"\n📈 有效因子 (|IC|>0.02, |IR|>0.3): {len(sig_factors)}/{len(factor_ic_report)}")
    for fname, info in sorted(sig_factors.items(), key=lambda x: abs(x[1]['ir']), reverse=True)[:10]:
        direction = '↑' if FACTOR_DIRECTIONS.get(fname, 1) > 0 else '↓'
        print(f"  {direction} {fname:25s}  IC={info['mean_ic']:+.4f}  IR={info['ir']:+.4f}")

    print(f"\n🔄 Purged 5-Fold CV:")
    print(f"  准确率: {cv_result['mean_accuracy']:.1%} ± {cv_result['std_accuracy']:.1%}")
    print(f"  各折: {cv_result['fold_accuracies']}")
    print(f"  过拟合分: {cv_result.get('overfit_score', 'N/A')}")

    print(f"\n📅 Walk-Forward 滚动回测:")
    print(f"  等权模式:       {wf_result['overall_accuracy']:.1%} ({wf_result['total_predictions']}条)")
    print(f"  IC加权模式:     {wf_result_ic['overall_accuracy']:.1%} ({wf_result_ic['total_predictions']}条)")
    print(f"  中高置信度:     {wf_result_high['overall_accuracy']:.1%} ({wf_result_high['total_predictions']}条)")
    print(f"  极端信号(T/B20%): {wf_result_extreme['overall_accuracy']:.1%} ({wf_result_extreme['total_predictions']}条)")
    print(f"  极端+IC加权:    {wf_result_extreme_ic['overall_accuracy']:.1%} ({wf_result_extreme_ic['total_predictions']}条)")
    print(f"  强信号(多维一致): {wf_result_strong['overall_accuracy']:.1%} ({wf_result_strong['total_predictions']}条)")
    print(f"  强信号+极端:    {wf_result_strong_extreme['overall_accuracy']:.1%} ({wf_result_strong_extreme['total_predictions']}条)")
    print(f"  趋势: {wf_result['accuracy_trend']}")

    print(f"\n⚠️  过拟合诊断:")
    print(f"  风险等级: {overfit_diag['overfit_risk']}")
    for rec in overfit_diag['recommendations']:
        print(f"  · {rec}")

    print(f"\n⏱️  耗时: {time.time() - t0:.1f}秒")
    print("=" * 60)

    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='多因子预测体系回测验证')
    parser.add_argument('--weeks', type=int, default=30, help='回测周数')
    parser.add_argument('--stocks', type=int, default=200, help='股票数量')
    args = parser.parse_args()
    run_full_backtest(n_weeks=args.weeks, n_stocks=args.stocks)
