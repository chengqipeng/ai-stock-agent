#!/usr/bin/env python3
"""
v19 综合数据审计与信号剪枝分析 — 测试入口

运行完整的8维度数据审计分析:
1. DB数据源可用性审计
2. 美股半导体个股K线信号分析
3. 已有因子有效性审计
4. 因子剪枝模拟（阈值搜索）
5. 美股半导体信号增强搜索
6. 留一日交叉验证
7. 纯统计基线分析（理论天花板）
8. 综合结论

回测区间: 2025-12-10 ~ 2026-03-10, 50只股票, 7个板块
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.analysis.data_audit_service import run_data_audit


def main():
    result = run_data_audit(
        backtest_result_path='data_results/backtest_prediction_enhanced_v9_50stocks_result.json',
    )

    # 输出关键指标
    baseline = result['baseline']
    cv = result['cv_result']
    baselines = result['baselines']

    print(f"\n{'═' * 60}")
    print(f"关键指标汇总:")
    print(f"  当前准确率: {baseline['loose_ok']}/{baseline['total']} "
          f"({baseline['loose_ok'] / baseline['total'] * 100:.1f}%)")
    print(f"  LOO-CV 当前: {cv['orig_mean']:.2f}% ± {cv['orig_std']:.2f}%")
    print(f"  LOO-CV 优化: {cv['opt_mean']:.2f}% ± {cv['opt_std']:.2f}%")
    print(f"  理论上限(板块×日): {baselines['sector_daily_best']:.1f}%")
    print(f"{'═' * 60}")


if __name__ == '__main__':
    main()
