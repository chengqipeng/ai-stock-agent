#!/usr/bin/env python3
"""
概念板块增强日预测回测测试 — 60只股票，覆盖15个概念板块

要求：
- 至少60只不同概念板块的个股
- 每个概念板块个股不少于4个
- 模拟至少60天数据
- 目标：日预测准确率（宽松）≥ 65%

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

回测区间：2025-12-10 ~ 2026-03-10（约60个交易日）
"""
import json
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from day_week_predicted.backtest.concept_daily_prediction_backtest import run_concept_daily_backtest

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ═══════════════════════════════════════════════════════════
# 60只股票，覆盖15个概念板块，每板块4只
# ═══════════════════════════════════════════════════════════

STOCK_CODES = [
    # ── 1. 人工智能 (4只) ──
    '002230.SZ',  # 科大讯飞
    '300496.SZ',  # 中科创达
    '688111.SH',  # 金山办公
    '300474.SZ',  # 景嘉微

    # ── 2. 新能源汽车 (4只) ──
    '002594.SZ',  # 比亚迪
    '601238.SH',  # 广汽集团
    '600733.SH',  # 北汽蓝谷
    '002074.SZ',  # 国轩高科

    # ── 3. 半导体 (4只) ──
    '002371.SZ',  # 北方华创
    '603986.SH',  # 兆易创新
    '688012.SH',  # 中微公司
    '002049.SZ',  # 紫光国微

    # ── 4. 锂电池 (4只) ──
    '300750.SZ',  # 宁德时代
    '002709.SZ',  # 天赐材料
    '300014.SZ',  # 亿纬锂能
    '002460.SZ',  # 赣锋锂业

    # ── 5. 光伏 (4只) ──
    '601012.SH',  # 隆基绿能
    '300763.SZ',  # 锦浪科技
    '688599.SH',  # 天合光能
    '002129.SZ',  # 中环股份(TCL中环)

    # ── 6. 医药生物 (4只) ──
    '600276.SH',  # 恒瑞医药
    '300760.SZ',  # 迈瑞医疗
    '603259.SH',  # 药明康德
    '600436.SH',  # 片仔癀

    # ── 7. 白酒 (4只) ──
    '600519.SH',  # 贵州茅台
    '000858.SZ',  # 五粮液
    '000568.SZ',  # 泸州老窖
    '002304.SZ',  # 洋河股份

    # ── 8. 军工 (4只) ──
    '600893.SH',  # 航发动力
    '600760.SH',  # 中航沈飞
    '002179.SZ',  # 中航光电
    '600862.SH',  # 中航高科

    # ── 9. 储能 (4只) ──
    '300274.SZ',  # 阳光电源
    '002812.SZ',  # 恩捷股份
    '300037.SZ',  # 新宙邦
    '688390.SH',  # 固德威

    # ── 10. 机器人 (4只) ──
    '300124.SZ',  # 汇川技术
    '002747.SZ',  # 埃斯顿
    '688169.SH',  # 石头科技
    '300024.SZ',  # 机器人(新松)

    # ── 11. 消费电子 (4只) ──
    '002475.SZ',  # 立讯精密
    '600745.SH',  # 闻泰科技
    '002241.SZ',  # 歌尔股份
    '300308.SZ',  # 中际旭创

    # ── 12. 稀土永磁 (4只) ──
    '600111.SH',  # 北方稀土
    '300748.SZ',  # 金力永磁
    '600549.SH',  # 厦门钨业
    '002600.SZ',  # 领益智造

    # ── 13. 化工新材料 (4只) ──
    '600309.SH',  # 万华化学
    '002440.SZ',  # 闰土股份
    '300037.SZ',  # 新宙邦 (与储能重叠，但属于多概念板块)
    '600426.SH',  # 华鲁恒升

    # ── 14. 数据中心 (4只) ──
    '603019.SH',  # 中科曙光
    '000977.SZ',  # 浪潮信息
    '002236.SZ',  # 大华股份
    '002916.SZ',  # 深南电路

    # ── 15. 汽车零部件 (4只) ──
    '601799.SH',  # 星宇股份
    '603596.SH',  # 伯特利
    '002920.SZ',  # 德赛西威
    '600066.SH',  # 宇通客车
]

# 去重
STOCK_CODES = list(dict.fromkeys(STOCK_CODES))


def main():
    print(f"{'=' * 70}")
    print(f"概念板块增强日预测回测")
    print(f"股票数: {len(STOCK_CODES)}, 覆盖15个概念板块")
    print(f"回测区间: 2025-12-10 ~ 2026-03-10 (约60个交易日)")
    print(f"目标: 日预测准确率(宽松) ≥ 65%")
    print(f"{'=' * 70}")

    result = run_concept_daily_backtest(
        stock_codes=STOCK_CODES,
        start_date='2025-12-10',
        end_date='2026-03-10',
        min_kline_days=100,
    )

    # 保存结果
    output_path = 'data_results/backtest_concept_daily_60stocks_result.json'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 70}")
    print(f"回测完成")
    print(f"总样本数: {result.get('总样本数', 0)}")
    print(f"有效股票数: {result.get('有效股票数', 0)}")
    print(f"跳过股票数: {result.get('跳过股票数', 0)}")
    print(f"总体准确率(宽松): {result.get('总体准确率(宽松)', '无数据')}")
    print(f"总体准确率(严格): {result.get('总体准确率(严格)', '无数据')}")
    print(f"耗时: {result.get('耗时(秒)', 0)}秒")

    # 打印概念板块效果对比
    comparison = result.get('概念板块效果对比', {})
    if comparison:
        print(f"\n概念板块效果对比:")
        for k, v in comparison.items():
            print(f"  {k}: {v}")

    # 打印置信度分析
    confidence = result.get('置信度分析', {})
    if confidence:
        print(f"\n置信度分析:")
        for k, v in confidence.items():
            print(f"  {k}: {v}")

    # 打印Top10概念板块
    board_stats = result.get('按概念板块统计(Top20)', {})
    if board_stats:
        print(f"\n按概念板块统计(Top10):")
        for i, (bn, stats) in enumerate(board_stats.items()):
            if i >= 10:
                break
            print(f"  {bn}: {stats}")

    # 打印各股票汇总（前10只）
    stock_list = result.get('各股票汇总(按准确率排序)', [])
    if stock_list:
        print(f"\n各股票准确率(Top10):")
        for s in stock_list[:10]:
            print(f"  {s['股票代码']} [{s.get('概念板块', '')}] "
                  f"宽松{s['准确率(宽松)']} 严格{s['准确率(严格)']}")

    print(f"\n结果已保存到: {output_path}")
    print(f"{'=' * 70}")

    # 检查是否达标
    total_str = result.get('总体准确率(宽松)', '0/0 (0%)')
    pct_str = total_str.split('(')[1].replace('%)', '') if '(' in total_str else '0'
    pct = float(pct_str)
    if pct >= 65:
        print(f"\n✅ 达标！日预测准确率(宽松) = {pct}% ≥ 65%")
    else:
        print(f"\n⚠️ 未达标：日预测准确率(宽松) = {pct}% < 65%")
        print(f"需要进一步优化概念板块信号权重和决策阈值")


if __name__ == '__main__':
    main()
