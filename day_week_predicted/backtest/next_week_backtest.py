#!/usr/bin/env python3
"""
下周预测回测验证脚本
==================
验证 _predict_next_week 和 _compute_next_week_backtest 的准确率。

用法：
    python -m day_week_predicted.backtest.next_week_backtest
"""
import sys
import logging
from datetime import datetime

sys.path.insert(0, '.')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

from service.weekly_prediction_service import (
    _get_all_stock_codes,
    _get_latest_trade_date,
    _load_prediction_data,
    _compute_next_week_backtest,
)


def run_next_week_backtest(n_weeks=29, sample_limit=500):
    """运行下周预测回测。"""
    t_start = datetime.now()
    logger.info("=" * 70)
    logger.info("  下周预测回测验证 (n_weeks=%d, sample_limit=%d)", n_weeks, sample_limit)
    logger.info("=" * 70)

    latest_date = _get_latest_trade_date()
    if not latest_date:
        logger.error("无法获取最新交易日")
        return
    logger.info("最新交易日: %s", latest_date)

    all_codes = _get_all_stock_codes()
    if sample_limit > 0:
        # 取前 sample_limit 只（按代码排序）
        all_codes = all_codes[:sample_limit]
    logger.info("回测股票数: %d", len(all_codes))

    # 加载数据（用于行业映射等）
    data = _load_prediction_data(all_codes, latest_date)

    # 运行回测
    result = _compute_next_week_backtest(all_codes, data, latest_date, n_weeks)

    global_bt = result['global']
    per_stock = result['per_stock']

    logger.info("")
    logger.info("=" * 70)
    logger.info("  下周预测回测结果")
    logger.info("=" * 70)
    logger.info("  全局准确率: %.1f%% (%d样本)", global_bt['accuracy'], global_bt['total'])
    logger.info("  有回测数据的股票: %d只", len(per_stock))

    # 按准确率分布统计
    acc_buckets = {'>=60%': 0, '55-60%': 0, '50-55%': 0, '<50%': 0}
    for code, info in per_stock.items():
        acc = info['accuracy']
        if acc >= 60:
            acc_buckets['>=60%'] += 1
        elif acc >= 55:
            acc_buckets['55-60%'] += 1
        elif acc >= 50:
            acc_buckets['50-55%'] += 1
        else:
            acc_buckets['<50%'] += 1

    logger.info("")
    logger.info("  准确率分布:")
    for bucket, count in acc_buckets.items():
        pct = count / len(per_stock) * 100 if per_stock else 0
        logger.info("    %-10s %d只 (%.1f%%)", bucket, count, pct)

    elapsed = (datetime.now() - t_start).total_seconds()
    logger.info("")
    logger.info("  耗时: %.1fs", elapsed)
    logger.info("=" * 70)

    return result


if __name__ == '__main__':
    run_next_week_backtest(n_weeks=29, sample_limit=500)
