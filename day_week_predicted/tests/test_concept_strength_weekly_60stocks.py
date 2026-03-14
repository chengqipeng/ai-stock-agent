#!/usr/bin/env python3
"""
概念板块强弱势增强周预测回测测试 — 60只股票，覆盖15个概念板块

要求：
- 至少60只不同概念板块的个股
- 每个概念板块个股不少于4个
- 模拟至少12周数据
- 目标：周预测准确率 ≥ 80%

概念板块选择（15个板块，每板块4只）：
1. 人工智能 (4只)
2. 新能源汽车 (4只)
3. 半导体 (4只)
4. 锂电池 (4只)
5. 光伏 (4只)
6. 医药生物 (4只)
7. 白酒 (4只)
8. 军工 (4只)
9. 储能 (4只)
10. 机器人 (4只)
11. 消费电子 (4只)
12. 稀土永磁 (4只)
13. 化工新材料 (4只)
14. 数据中心 (4只)
15. 汽车零部件 (4只)

回测区间：2025-12-01 ~ 2026-03-10（约14周）
"""
import json
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from day_week_predicted.backtest.concept_strength_weekly_backtest import (
    run_concept_strength_backtest,
    run_concept_strength_backtest_simulated,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def _check_db_available():
    """检查DB是否可达。"""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('106.14.194.144', 3306))
        sock.close()
        return result == 0
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════
# 60只股票，覆盖15个概念板块，每板块4只
# ═══════════════════════════════════════════════════════════

CONCEPT_BOARD_STOCKS = {
    # ── 1. 人工智能 (4只) ──
    '人工智能': [
        '002230.SZ',  # 科大讯飞
        '300496.SZ',  # 中科创达
        '688111.SH',  # 金山办公
        '300474.SZ',  # 景嘉微
    ],
    # ── 2. 新能源汽车 (4只) ──
    '新能源汽车': [
        '002594.SZ',  # 比亚迪
        '601238.SH',  # 广汽集团
        '600733.SH',  # 北汽蓝谷
        '002074.SZ',  # 国轩高科
    ],
    # ── 3. 半导体 (4只) ──
    '半导体': [
        '002371.SZ',  # 北方华创
        '603986.SH',  # 兆易创新
        '688012.SH',  # 中微公司
        '002049.SZ',  # 紫光国微
    ],
    # ── 4. 锂电池 (4只) ──
    '锂电池': [
        '300750.SZ',  # 宁德时代
        '002709.SZ',  # 天赐材料
        '300014.SZ',  # 亿纬锂能
        '002460.SZ',  # 赣锋锂业
    ],
    # ── 5. 光伏 (4只) ──
    '光伏': [
        '601012.SH',  # 隆基绿能
        '300763.SZ',  # 锦浪科技
        '688599.SH',  # 天合光能
        '002129.SZ',  # TCL中环
    ],
    # ── 6. 医药生物 (4只) ──
    '医药生物': [
        '600276.SH',  # 恒瑞医药
        '300760.SZ',  # 迈瑞医疗
        '603259.SH',  # 药明康德
        '600436.SH',  # 片仔癀
    ],
    # ── 7. 白酒 (4只) ──
    '白酒': [
        '600519.SH',  # 贵州茅台
        '000858.SZ',  # 五粮液
        '000568.SZ',  # 泸州老窖
        '002304.SZ',  # 洋河股份
    ],
    # ── 8. 军工 (4只) ──
    '军工': [
        '600893.SH',  # 航发动力
        '600760.SH',  # 中航沈飞
        '002179.SZ',  # 中航光电
        '600862.SH',  # 中航高科
    ],
    # ── 9. 储能 (4只) ──
    '储能': [
        '300274.SZ',  # 阳光电源
        '002812.SZ',  # 恩捷股份
        '300037.SZ',  # 新宙邦
        '688390.SH',  # 固德威
    ],
    # ── 10. 机器人 (4只) ──
    '机器人': [
        '300124.SZ',  # 汇川技术
        '688169.SH',  # 石头科技
        '002747.SZ',  # 埃斯顿
        '300024.SZ',  # 机器人(新松)
    ],
    # ── 11. 消费电子 (4只) ──
    '消费电子': [
        '002475.SZ',  # 立讯精密
        '600584.SH',  # 长电科技
        '002241.SZ',  # 歌尔股份
        '002938.SZ',  # 鹏鼎控股
    ],
    # ── 12. 稀土永磁 (4只) ──
    '稀土永磁': [
        '600111.SH',  # 北方稀土
        '300748.SZ',  # 金力永磁
        '600366.SH',  # 宁波韵升
        '002600.SZ',  # 领益智造
    ],
    # ── 13. 化工新材料 (4只) ──
    '化工新材料': [
        '002648.SZ',  # 卫星化学
        '300438.SZ',  # 鹏辉能源
        '600309.SH',  # 万华化学
        '002601.SZ',  # 龙蟒佰利
    ],
    # ── 14. 数据中心 (4只) ──
    '数据中心': [
        '603019.SH',  # 中科曙光
        '000977.SZ',  # 浪潮信息
        '002236.SZ',  # 大华股份
        '300308.SZ',  # 中际旭创
    ],
    # ── 15. 汽车零部件 (4只) ──
    '汽车零部件': [
        '601799.SH',  # 星宇股份
        '603596.SH',  # 伯特利
        '002920.SZ',  # 德赛西威
        '603786.SH',  # 科博达
    ],
}


def build_stock_list():
    """构建去重的股票列表和概念板块映射。"""
    all_codes = []
    code_to_board = {}
    seen = set()
    for board_name, codes in CONCEPT_BOARD_STOCKS.items():
        for code in codes:
            if code not in seen:
                all_codes.append(code)
                code_to_board[code] = board_name
                seen.add(code)
    return all_codes, code_to_board


def main():
    stock_codes, concept_board_map = build_stock_list()

    print("=" * 70)
    print("  概念板块强弱势增强周预测回测 v2 — 60只股票 × 15个概念板块 × 12+周")
    print("=" * 70)
    print(f"  股票总数: {len(stock_codes)} (去重后)")
    print(f"  概念板块: {len(CONCEPT_BOARD_STOCKS)}个")
    for board, codes in CONCEPT_BOARD_STOCKS.items():
        print(f"    {board}: {len(codes)}只")
    print(f"  回测区间: 2025-12-01 ~ 2026-03-10 (约14周)")
    print()

    # 先尝试DB，不可达则用模拟数据
    db_available = _check_db_available()

    if db_available:
        print("  数据模式: 数据库（DB）")
        result = run_concept_strength_backtest(
            stock_codes=stock_codes,
            start_date='2025-12-01',
            end_date='2026-03-10',
            concept_board_map=concept_board_map,
        )
    else:
        print("  数据模式: 模拟数据（DB不可达）")
        result = run_concept_strength_backtest_simulated(
            stock_codes=stock_codes,
            concept_board_stocks=CONCEPT_BOARD_STOCKS,
            start_date='2025-12-01',
            end_date='2026-03-10',
            concept_board_map=concept_board_map,
        )

    if 'error' in result:
        print(f"回测失败: {result['error']}")
        return

    # ═══════════════════════════════════════════════════════════
    # 输出结果
    # ═══════════════════════════════════════════════════════════

    summary = result['summary']
    full = result['full_sample']
    lowo = result['lowo_cv']
    boards = result['by_concept_board']

    print("\n" + "=" * 70)
    print("  回测结果汇总")
    print("=" * 70)

    print(f"\n  基本信息:")
    print(f"  股票数: {summary['stock_count']}")
    print(f"  周数: {summary['week_count']}")
    print(f"  周样本总数: {summary['weekly_sample_count']}")
    print(f"  概念信号覆盖率: {summary['concept_signal_coverage']}%")
    print(f"  回测区间: {summary['backtest_period']}")
    print(f"  耗时: {summary['elapsed_seconds']}秒")

    print(f"\n  全样本准确率:")
    print(f"  ★ 总体准确率: {full['accuracy']}% ({full['correct']}/{full['total']})")
    print(f"  有概念信号: {full['with_concept_signal']['accuracy']}% "
          f"(样本{full['with_concept_signal']['count']})")
    print(f"  无概念信号: {full['without_concept_signal']['accuracy']}% "
          f"(样本{full['without_concept_signal']['count']})")

    print(f"\n  按置信度:")
    for conf, stats in full['by_confidence'].items():
        print(f"    {conf}: {stats['accuracy']}% (样本{stats['count']})")

    print(f"\n  模糊区(|d3_chg|<=0.8%): {full['fuzzy_zone']['accuracy']}% "
          f"(样本{full['fuzzy_zone']['count']})")

    print(f"\n  LOWO交叉验证（无泄露）:")
    print(f"  ★ 总体准确率: {lowo['overall_accuracy']}% "
          f"({lowo['total_correct']}/{lowo['total_count']})")
    print(f"  平均周准确率: {lowo['avg_week_accuracy']}%")
    print(f"  最低周准确率: {lowo['min_week_accuracy']}%")
    print(f"  最高周准确率: {lowo['max_week_accuracy']}%")
    print(f"  验证周数: {lowo['n_weeks']}")

    if lowo['week_accuracies']:
        print(f"  各周准确率: {lowo['week_accuracies']}")

    print(f"\n  按概念板块分析:")
    print(f"  {'板块':<12} {'准确率':>8} {'正确/总数':>12} {'股票数':>6}")
    print(f"  {'-'*42}")
    for b in boards:
        print(f"  {b['board_name']:<12} {b['accuracy']:>7.1f}% "
              f"{b['correct']:>4}/{b['total']:<4} {b['stock_count']:>5}")

    # 检查是否达标
    print("\n" + "=" * 70)
    target = 80.0
    full_ok = full['accuracy'] >= target
    lowo_ok = lowo['overall_accuracy'] >= target
    week_ok = summary['week_count'] >= 12
    stock_ok = summary['stock_count'] >= 60

    print(f"  周数 >= 12: {'通过' if week_ok else '未通过'} ({summary['week_count']}周)")
    print(f"  股票 >= 60: {'通过' if stock_ok else '未通过'} ({summary['stock_count']}只)")
    print(f"  全样本准确率 >= {target}%: {'通过' if full_ok else '未通过'} ({full['accuracy']}%)")
    print(f"  LOWO准确率 >= {target}%: {'通过' if lowo_ok else '未通过'} ({lowo['overall_accuracy']}%)")

    all_pass = full_ok and lowo_ok and week_ok and stock_ok
    print(f"\n  {'全部达标' if all_pass else '部分指标未达标'}")
    print("=" * 70)

    # 保存结果
    output_path = 'data_results/backtest_concept_strength_weekly_60stocks_result.json'
    save_result = {
        'summary': summary,
        'full_sample': {k: v for k, v in full.items() if k != 'details'},
        'lowo_cv': lowo,
        'by_concept_board': boards,
        'pass_criteria': {
            'week_count_ok': week_ok,
            'stock_count_ok': stock_ok,
            'full_accuracy_ok': full_ok,
            'lowo_accuracy_ok': lowo_ok,
            'all_pass': all_pass,
        },
    }
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(save_result, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存到: {output_path}")


if __name__ == '__main__':
    main()
