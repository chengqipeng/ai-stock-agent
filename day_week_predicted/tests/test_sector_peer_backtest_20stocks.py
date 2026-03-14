"""
板块同行走势增强回测（20只股票，覆盖7个板块）

从 stock_score_list.md 中选取不同板块的代表性股票，
利用 stock_industry_list.md 的板块分类获取同行个股走势，
验证板块同行信号是否能提升回测准确率。

回测区间：2025-12-10 ~ 2026-03-10（3个月）
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from day_week_predicted.backtest.sector_peer_backtest import run_sector_peer_backtest


# 20只股票，覆盖7个板块，从 stock_score_list.md 中按评分优先选取
# 科技(4): 北方华创(半导体设备), 中际旭创(光通信), 深南电路(PCB), 兆易创新(芯片设计)
# 有色金属(3): 湖南黄金(黄金), 紫金矿业(铜矿), 厦门钨业(钨)
# 汽车(2): 比亚迪(整车), 宇通客车(客车)
# 新能源(3): 宁德时代(锂电池), 锦浪科技(光伏), 天赐材料(锂电材料)
# 医药(3): 恒瑞医药(创新药), 片仔癀(中药), 药明康德(CXO)
# 化工(3): 万华化学(基础化工), 闰土股份(精细化工), 雅化集团(农药)
# 制造(2): 三一重工(工程机械), 汇川技术(工业自动化)

STOCK_CODES = [
    # ── 科技板块 (4只) ──
    '002371.SZ',  # 北方华创 - 半导体设备 - 评分50
    '300308.SZ',  # 中际旭创 - 光通信 - 评分50
    '002916.SZ',  # 深南电路 - PCB - 评分78
    '603986.SH',  # 兆易创新 - 芯片设计 - 评分62

    # ── 有色金属板块 (3只) ──
    '002155.SZ',  # 湖南黄金 - 黄金 - 评分70
    '601899.SH',  # 紫金矿业 - 铜矿 - 评分68
    '600549.SH',  # 厦门钨业 - 钨 - 评分72

    # ── 汽车板块 (2只) ──
    '002594.SZ',  # 比亚迪 - 整车 - 评分58
    '600066.SH',  # 宇通客车 - 客车 - 评分72

    # ── 新能源板块 (3只) ──
    '300750.SZ',  # 宁德时代 - 锂电池 - 评分62
    '300763.SZ',  # 锦浪科技 - 光伏 - 评分58
    '002709.SZ',  # 天赐材料 - 锂电材料 - 评分68

    # ── 医药板块 (3只) ──
    '600276.SH',  # 恒瑞医药 - 创新药 - 评分58
    '600436.SH',  # 片仔癀 - 中药 - 评分58
    '603259.SH',  # 药明康德 - CXO - 评分62

    # ── 化工板块 (3只) ──
    '600309.SH',  # 万华化学 - 基础化工 - 评分58
    '002440.SZ',  # 闰土股份 - 精细化工 - 评分78
    '002497.SZ',  # 雅化集团 - 农药 - 评分78

    # ── 制造板块 (2只) ──
    '600031.SH',  # 三一重工 - 工程机械 - 评分58
    '300124.SZ',  # 汇川技术 - 工业自动化 - 评分70
]


async def main():
    print(f"{'=' * 70}")
    print(f"板块同行走势增强回测 v5")
    print(f"股票数: {len(STOCK_CODES)}, 覆盖7个板块")
    print(f"回测区间: 2025-12-10 ~ 2026-03-10")
    print(f"板块分类来源: stock_industry_list.md（本地文件，不调用API）")
    print(f"{'=' * 70}")

    result = await run_sector_peer_backtest(
        stock_codes=STOCK_CODES,
        start_date='2025-12-10',
        end_date='2026-03-10',
        max_peers=8,
    )

    # 保存结果
    output_path = 'data_results/backtest_sector_peer_20stocks_result.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 70}")
    print(f"回测完成")
    print(f"总样本数: {result.get('总样本数', 0)}")
    print(f"同行K线加载数: {result.get('同行K线加载数', 0)}")
    print(f"总体准确率(宽松): {result.get('总体准确率(宽松)', 'N/A')}")
    print(f"总体准确率(严格): {result.get('总体准确率(严格)', 'N/A')}")
    print(f"耗时: {result.get('耗时(秒)', 0)}秒")

    print(f"\n按预测方向统计:")
    for d, stats in result.get('按预测方向统计', {}).items():
        print(f"  {d}: 样本{stats['样本数']}, 宽松{stats['准确率(宽松)']}, 严格{stats['准确率(严格)']}")

    print(f"\n按板块统计:")
    for sec, stats in result.get('按板块统计', {}).items():
        print(f"  {sec}({stats['股票数']}只): 样本{stats['样本数']}, "
              f"宽松{stats['准确率(宽松)']}, 严格{stats['准确率(严格)']}")

    print(f"\n板块同行信号分析:")
    for key, stats in result.get('板块同行信号分析', {}).items():
        print(f"  {key}: 样本{stats['样本数']}, 宽松{stats.get('宽松准确率', 'N/A')}")

    print(f"\n各股票汇总:")
    for s in result.get('各股票汇总', []):
        print(f"  {s['股票名称']}({s['股票代码']})[{s['板块']}]: {s['回测天数']}天, "
              f"宽松{s['准确率(宽松)']}, 严格{s['准确率(严格)']}, "
              f"平均涨跌{s['平均实际涨跌']}")

    print(f"\n结果已保存到: {output_path}")


if __name__ == '__main__':
    asyncio.run(main())
