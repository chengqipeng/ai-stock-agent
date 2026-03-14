"""
增强预测回测 v6 测试（20只股票，覆盖7个板块）

验证板块个性化多因子预测逻辑的准确率提升效果。
板块分类来自 stock_industry_list.md。

回测区间：2025-12-10 ~ 2026-03-10（3个月）
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from day_week_predicted.backtest.prediction_enhanced_backtest import run_prediction_enhanced_backtest


# 20只股票，覆盖7个板块（与 test_sector_peer_backtest_20stocks 一致，便于对比）
STOCK_CODES = [
    # ── 科技板块 (4只) ──
    '002371.SZ',  # 北方华创 - 半导体设备
    '300308.SZ',  # 中际旭创 - 光通信
    '002916.SZ',  # 深南电路 - PCB
    '603986.SH',  # 兆易创新 - 芯片设计

    # ── 有色金属板块 (3只) ──
    '002155.SZ',  # 湖南黄金
    '601899.SH',  # 紫金矿业
    '600549.SH',  # 厦门钨业

    # ── 汽车板块 (2只) ──
    '002594.SZ',  # 比亚迪
    '600066.SH',  # 宇通客车

    # ── 新能源板块 (3只) ──
    '300750.SZ',  # 宁德时代
    '300763.SZ',  # 锦浪科技
    '002709.SZ',  # 天赐材料

    # ── 医药板块 (3只) ──
    '600276.SH',  # 恒瑞医药
    '600436.SH',  # 片仔癀
    '603259.SH',  # 药明康德

    # ── 化工板块 (3只) ──
    '600309.SH',  # 万华化学
    '002440.SZ',  # 闰土股份
    '002497.SZ',  # 雅化集团

    # ── 制造板块 (2只) ──
    '600031.SH',  # 三一重工
    '300124.SZ',  # 汇川技术
]


async def main():
    print(f"{'=' * 70}")
    print(f"增强预测回测 v6（多因子综合+板块个性化）")
    print(f"股票数: {len(STOCK_CODES)}, 覆盖7个板块")
    print(f"回测区间: 2025-12-10 ~ 2026-03-10")
    print(f"板块分类来源: stock_industry_list.md")
    print(f"{'=' * 70}")

    result = await run_prediction_enhanced_backtest(
        stock_codes=STOCK_CODES,
        start_date='2025-12-10',
        end_date='2026-03-10',
        max_peers=8,
    )

    # 保存结果
    output_path = 'data_results/backtest_prediction_enhanced_result.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 70}")
    print(f"回测完成")
    print(f"总样本数: {result.get('总样本数', 0)}")
    print(f"同行K线加载数: {result.get('同行K线加载数', 0)}")
    print(f"总体准确率(宽松): {result.get('总体准确率(宽松)', 'N/A')}")
    print(f"总体准确率(严格): {result.get('总体准确率(严格)', 'N/A')}")
    print(f"耗时: {result.get('耗时(秒)', 0)}秒")

    # v6/v5对比基准
    print(f"\n{'─' * 40}")
    print(f"v6基准（同样20只股票）: 宽松48.9%, 严格42.7%")
    print(f"v5基准（同样20只股票）: 宽松48.6%, 严格43.0%")
    print(f"v4基准（50只股票）:     宽松52.1%, 严格47.3%")
    print(f"{'─' * 40}")

    print(f"\n按预测方向统计:")
    for d, stats in result.get('按预测方向统计', {}).items():
        print(f"  {d}: 样本{stats['样本数']}, 宽松{stats['准确率(宽松)']}, 严格{stats['准确率(严格)']}")

    print(f"\n按板块统计:")
    for sec, stats in result.get('按板块统计', {}).items():
        print(f"  {sec}({stats['股票数']}只): 样本{stats['样本数']}, "
              f"宽松{stats['准确率(宽松)']}, 严格{stats['准确率(严格)']}")

    print(f"\n板块同行信号分析:")
    peer = result.get('板块同行信号分析', {})
    for key in ['同行看涨时', '同行看跌时', '同行中性时']:
        stats = peer.get(key, {})
        print(f"  {key}: 样本{stats.get('样本数', 0)}, 宽松{stats.get('宽松准确率', 'N/A')}")

    print(f"\n  按板块同行信号:")
    for sec, stats in peer.get('按板块同行信号', {}).items():
        print(f"    {sec}: 一致时{stats.get('信号一致时', 'N/A')}, 矛盾时{stats.get('信号矛盾时', 'N/A')}")

    print(f"\n因子有效性分析(按板块):")
    for sec, factors in result.get('因子有效性分析(按板块)', {}).items():
        effective = [f for f, d in factors.items() if d.get('有效性') == '有效']
        ineffective = [f for f, d in factors.items() if d.get('有效性') == '无效']
        print(f"  {sec}: 有效因子={effective or '无'}, 无效因子={ineffective or '无'}")

    print(f"\n板块个性化配置:")
    for sec, cfg in result.get('板块个性化配置', {}).items():
        fw = cfg.get('因子权重', {})
        th = cfg.get('方向阈值', {})
        pw = cfg.get('同行联动权重', 0)
        top3 = sorted(fw.items(), key=lambda x: x[1], reverse=True)[:3]
        print(f"  {sec}: 同行权重={pw}, 看涨阈值={th.get('bullish')}, "
              f"看跌阈值={th.get('bearish')}, TOP3因子={top3}")

    print(f"\n各股票汇总:")
    for s in result.get('各股票汇总', []):
        print(f"  {s['股票名称']}({s['股票代码']})[{s['板块']}]: {s['回测天数']}天, "
              f"宽松{s['准确率(宽松)']}, 严格{s['准确率(严格)']}, "
              f"平均涨跌{s['平均实际涨跌']}")

    print(f"\n结果已保存到: {output_path}")


if __name__ == '__main__':
    asyncio.run(main())
