"""
增强预测回测 v10 验证测试（50只股票，覆盖7个板块）

验证v10优化后的预测准确率是否达到65%以上（宽松模式）。
v10核心改进：
  - 选择性因子投票：只使用>55%有效性因子，零权重淘汰噪声
  - 同行反转信号独立利用（化工矛盾69.2%，科技矛盾63.0%）
  - 宽松模式优化：偏涨板块低置信度全预测上涨
  - 因子投票计数替代加权和，更稳健

回测区间：2025-12-10 ~ 2026-03-10（3个月）
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.backtest.prediction_enhanced_backtest import run_prediction_enhanced_backtest


# 50只股票，覆盖7个板块（每板块6-8只，确保板块多样性和代表性）
STOCK_CODES = [
    # ── 科技板块 (8只) ──
    '002371.SZ',  # 北方华创 - 半导体设备
    '300308.SZ',  # 中际旭创 - 光通信
    '002916.SZ',  # 深南电路 - PCB
    '603986.SH',  # 兆易创新 - 芯片设计
    '688981.SH',  # 中芯国际 - 晶圆代工
    '002475.SZ',  # 立讯精密 - 消费电子
    '300502.SZ',  # 新易盛 - 光通信
    '002049.SZ',  # 紫光国微 - 芯片设计

    # ── 有色金属板块 (7只) ──
    '002155.SZ',  # 湖南黄金
    '601899.SH',  # 紫金矿业
    '600549.SH',  # 厦门钨业
    '600547.SH',  # 山东黄金
    '600489.SH',  # 中金黄金
    '600988.SH',  # 赤峰黄金
    '300748.SZ',  # 金力永磁 - 磁性材料

    # ── 汽车板块 (7只) ──
    '002594.SZ',  # 比亚迪
    '600066.SH',  # 宇通客车
    '601689.SH',  # 拓普集团 - 汽车零部件
    '002920.SZ',  # 德赛西威 - 汽车电子
    '002050.SZ',  # 三花智控 - 热管理
    '603596.SH',  # 伯特利 - 制动系统
    '601127.SH',  # 赛力斯 - 新能源汽车

    # ── 新能源板块 (8只) ──
    '300750.SZ',  # 宁德时代 - 锂电池
    '300763.SZ',  # 锦浪科技 - 光伏逆变器
    '002709.SZ',  # 天赐材料 - 锂电材料
    '002074.SZ',  # 国轩高科 - 锂电池
    '300073.SZ',  # 当升科技 - 正极材料
    '600406.SH',  # 国电南瑞 - 电网设备
    '002202.SZ',  # 金风科技 - 风电
    '300450.SZ',  # 先导智能 - 锂电设备

    # ── 医药板块 (7只) ──
    '600276.SH',  # 恒瑞医药 - 创新药
    '600436.SH',  # 片仔癀 - 中药
    '603259.SH',  # 药明康德 - CXO
    '000963.SZ',  # 华东医药 - 化学制药
    '688271.SH',  # 联影医疗 - 医疗器械
    '300759.SZ',  # 康龙化成 - CXO
    '000538.SZ',  # 云南白药 - 中药

    # ── 化工板块 (7只) ──
    '600309.SH',  # 万华化学 - 基础化工
    '002440.SZ',  # 闰土股份 - 精细化工
    '002497.SZ',  # 雅化集团 - 农药化肥
    '600426.SH',  # 华鲁恒升 - 基础化工
    '002648.SZ',  # 卫星化学 - 基础化工
    '600989.SH',  # 宝丰能源 - 基础化工
    '002250.SZ',  # 联化科技 - 精细化工

    # ── 制造板块 (6只) ──
    '600031.SH',  # 三一重工 - 工程机械
    '300124.SZ',  # 汇川技术 - 工业自动化
    '000157.SZ',  # 中联重科 - 工程机械
    '601100.SH',  # 恒立液压 - 液压设备
    '000425.SZ',  # 徐工机械 - 工程机械
    '600150.SH',  # 中国船舶 - 重型装备
]


async def main():
    print(f"{'=' * 70}")
    print(f"增强预测回测 v10（选择性因子投票+同行反转+宽松模式优化）")
    print(f"股票数: {len(STOCK_CODES)}, 覆盖7个板块")
    print(f"回测区间: 2025-12-10 ~ 2026-03-10")
    print(f"目标: 宽松准确率 ≥ 65%")
    print(f"{'=' * 70}")

    result = await run_prediction_enhanced_backtest(
        stock_codes=STOCK_CODES,
        start_date='2025-12-10',
        end_date='2026-03-10',
        max_peers=8,
    )

    # 保存结果
    output_path = 'data_results/backtest_prediction_enhanced_v9_50stocks_result.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 70}")
    print(f"回测完成")
    print(f"总样本数: {result.get('总样本数', 0)}")
    print(f"同行K线加载数: {result.get('同行K线加载数', 0)}")
    print(f"总体准确率(宽松): {result.get('总体准确率(宽松)', 'N/A')}")
    print(f"总体准确率(严格): {result.get('总体准确率(严格)', 'N/A')}")
    print(f"耗时: {result.get('耗时(秒)', 0)}秒")

    # 历史版本对比基准
    print(f"\n{'─' * 50}")
    print(f"历史版本对比:")
    print(f"  v8b基准（20只股票）: 宽松60.0%, 严格52.6%")
    print(f"  v6基准（20只股票）:  宽松48.9%, 严格42.7%")
    print(f"  v4基准（50只股票）:  宽松52.1%, 严格47.3%")
    print(f"  v9目标:              宽松≥65%")
    print(f"{'─' * 50}")

    # 解析宽松准确率数值
    loose_str = result.get('总体准确率(宽松)', '0/0 (0%)')
    try:
        pct_str = loose_str.split('(')[1].rstrip('%)')
        loose_pct = float(pct_str)
    except (IndexError, ValueError):
        loose_pct = 0.0

    if loose_pct >= 65.0:
        print(f"\n✅ 目标达成! 宽松准确率 {loose_pct:.1f}% ≥ 65%")
    else:
        print(f"\n⚠️ 未达目标: 宽松准确率 {loose_pct:.1f}% < 65%")
        print(f"   差距: {65.0 - loose_pct:.1f}个百分点")

    print(f"\n按预测方向统计:")
    for d, stats in result.get('按预测方向统计', {}).items():
        print(f"  {d}: 样本{stats['样本数']}, "
              f"宽松{stats['准确率(宽松)']}, 严格{stats['准确率(严格)']}")

    print(f"\n按评分区间:")
    for b, stats in result.get('按评分区间', {}).items():
        print(f"  {b}: 样本{stats['样本数']}, "
              f"宽松{stats['准确率(宽松)']}, 严格{stats['准确率(严格)']}")

    print(f"\n按板块统计:")
    for sec, stats in result.get('按板块统计', {}).items():
        print(f"  {sec}({stats['股票数']}只): 样本{stats['样本数']}, "
              f"宽松{stats['准确率(宽松)']}, 严格{stats['准确率(严格)']}")

    print(f"\n置信度分析:")
    for tier, stats in result.get('置信度分析', {}).items():
        print(f"  {tier}: 样本{stats.get('样本数', 0)}({stats.get('占比', '')}), "
              f"宽松{stats.get('准确率(宽松)', 'N/A')}, "
              f"严格{stats.get('准确率(严格)', 'N/A')}")

    print(f"\n板块同行信号分析:")
    peer = result.get('板块同行信号分析', {})
    for key in ['同行看涨时', '同行看跌时', '同行中性时']:
        stats = peer.get(key, {})
        print(f"  {key}: 样本{stats.get('样本数', 0)}, "
              f"宽松{stats.get('宽松准确率', 'N/A')}")

    print(f"\n因子有效性分析(按板块):")
    for sec, factors in result.get('因子有效性分析(按板块)', {}).items():
        effective = [f for f, d in factors.items() if d.get('有效性') == '有效']
        ineffective = [f for f, d in factors.items() if d.get('有效性') == '无效']
        print(f"  {sec}: 有效={effective or '无'}, 无效={ineffective or '无'}")

    print(f"\n各股票汇总:")
    for s in result.get('各股票汇总', []):
        print(f"  {s['股票名称']}({s['股票代码']})[{s['板块']}]: {s['回测天数']}天, "
              f"宽松{s['准确率(宽松)']}, 严格{s['准确率(严格)']}, "
              f"均涨跌{s['平均实际涨跌']}")

    # 统计各板块是否达标
    print(f"\n{'─' * 50}")
    print(f"板块达标情况(宽松≥65%):")
    sector_stats = result.get('按板块统计', {})
    for sec, stats in sector_stats.items():
        rate_str = stats.get('准确率(宽松)', '0/0 (0%)')
        try:
            pct = float(rate_str.split('(')[1].rstrip('%)'))
        except (IndexError, ValueError):
            pct = 0.0
        status = '✅' if pct >= 65.0 else '⚠️'
        print(f"  {status} {sec}: {rate_str}")

    print(f"\n结果已保存到: {output_path}")


if __name__ == '__main__':
    asyncio.run(main())
