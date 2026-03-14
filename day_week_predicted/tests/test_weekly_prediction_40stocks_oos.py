"""
周预测样本外验证（40只新股票，覆盖7个板块，每板块≥4只）

目的：验证周预测策略B和C在完全未见过的股票上是否仍能达到≥65%准确率。
所有股票均不在原始50只回测股票中，且来自不同概念子行业以确保多样性。

回测区间：2025-12-10 ~ 2026-03-10（与原始回测一致）
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.backtest.prediction_enhanced_backtest import run_prediction_enhanced_backtest

# 原始50只股票（用于确认无重叠）
ORIGINAL_50 = {
    '002371.SZ', '300308.SZ', '002916.SZ', '603986.SH', '688981.SH',
    '002475.SZ', '300502.SZ', '002049.SZ',  # 科技
    '002155.SZ', '601899.SH', '600549.SH', '600547.SH', '600489.SH',
    '600988.SH', '300748.SZ',  # 有色金属
    '002594.SZ', '600066.SH', '601689.SH', '002920.SZ', '002050.SZ',
    '603596.SH', '601127.SH',  # 汽车
    '300750.SZ', '300763.SZ', '002709.SZ', '002074.SZ', '300073.SZ',
    '600406.SH', '002202.SZ', '300450.SZ',  # 新能源
    '600276.SH', '600436.SH', '603259.SH', '000963.SZ', '688271.SH',
    '300759.SZ', '000538.SZ',  # 医药
    '600309.SH', '002440.SZ', '002497.SZ', '600426.SH', '002648.SZ',
    '600989.SH', '002250.SZ',  # 化工
    '600031.SH', '300124.SZ', '000157.SZ', '601100.SH', '000425.SZ',
    '600150.SH',  # 制造
}

# 40只新股票，覆盖7个板块，每板块≥4只，来自不同概念子行业
STOCK_CODES = [
    # ── 科技板块 (7只) ── 选自不同子行业：AI芯片/封测/光刻/通信/面板/安防/存储
    '688256.SH',  # 寒武纪 - AI芯片
    '002156.SZ',  # 通富微电 - 封测
    '688012.SH',  # 中微公司 - 半导体设备
    '002384.SZ',  # 东山精密 - PCB/FPC
    '000725.SZ',  # 京东方A - 面板显示
    '688008.SH',  # 澜起科技 - 内存接口芯片
    '002241.SZ',  # 歌尔股份 - 消费电子/VR

    # ── 有色金属板块 (5只) ── 选自不同子行业：铜/铝/锂/钴/稀土
    '603993.SH',  # 洛阳钼业 - 钼/钴/铜
    '600362.SH',  # 江西铜业 - 铜
    '600219.SH',  # 南山铝业 - 铝
    '002460.SZ',  # 赣锋锂业 - 锂
    '600111.SH',  # 北方稀土 - 稀土

    # ── 汽车板块 (5只) ── 选自不同子行业：整车/轮胎/线束/座椅/变速箱
    '600104.SH',  # 上汽集团 - 整车
    '601799.SH',  # 星宇股份 - 车灯
    '603348.SH',  # 文灿股份 - 压铸件
    '002906.SZ',  # 华阳集团 - 智能座舱
    '601058.SH',  # 赛轮轮胎 - 轮胎

    # ── 新能源板块 (6只) ── 选自不同子行业：光伏组件/逆变器/储能/氢能/风电塔筒/电池回收
    '601012.SH',  # 隆基绿能 - 光伏硅片
    '300274.SZ',  # 阳光电源 - 逆变器/储能
    '688599.SH',  # 天合光能 - 光伏组件
    '300014.SZ',  # 亿纬锂能 - 锂电池
    '002129.SZ',  # 中环股份(TCL中环) - 光伏硅片
    '300037.SZ',  # 新宙邦 - 电解液

    # ── 医药板块 (6只) ── 选自不同子行业：疫苗/血制品/医疗器械/中药/生物药/医美
    '300760.SZ',  # 迈瑞医疗 - 医疗器械
    '300122.SZ',  # 智飞生物 - 疫苗
    '002007.SZ',  # 华兰生物 - 血制品
    '300347.SZ',  # 泰格医药 - CRO
    '600196.SH',  # 复星医药 - 综合医药
    '300015.SZ',  # 爱尔眼科 - 眼科连锁

    # ── 化工板块 (5只) ── 选自不同子行业：钛白粉/MDI/氟化工/农药/涂料
    '002601.SZ',  # 龙蟒佰利 - 钛白粉
    '600486.SH',  # 扬农化工 - 农药
    '002064.SZ',  # 华峰化学 - 氨纶
    '603260.SH',  # 合盛硅业 - 有机硅
    '000830.SZ',  # 鲁西化工 - 基础化工

    # ── 制造板块 (6只) ── 选自不同子行业：激光/轨交/电梯/卫星/工程机械/数控机床
    '002008.SZ',  # 大族激光 - 激光设备
    '601766.SH',  # 中国中车 - 轨交装备
    '600835.SH',  # 上海机电 - 电梯
    '601698.SH',  # 中国卫通 - 卫星通信
    '002097.SZ',  # 山河智能 - 工程机械
    '601882.SH',  # 海天精工 - 数控机床
]

# 验证无重叠
overlap = set(STOCK_CODES) & ORIGINAL_50
assert not overlap, f"发现重叠股票: {overlap}"
assert len(STOCK_CODES) == 40, f"股票数量应为40，实际{len(STOCK_CODES)}"


async def main():
    print(f"{'=' * 70}")
    print(f"周预测样本外验证（40只新股票，7个板块）")
    print(f"股票数: {len(STOCK_CODES)}")
    print(f"回测区间: 2025-12-10 ~ 2026-03-10")
    print(f"目标: 周预测策略B/C泛化准确率 ≥ 65%")
    print(f"{'=' * 70}")

    result = await run_prediction_enhanced_backtest(
        stock_codes=STOCK_CODES,
        start_date='2025-12-10',
        end_date='2026-03-10',
        max_peers=8,
    )

    # 保存结果
    output_path = 'data_results/backtest_weekly_oos_40stocks_result.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 70}")
    print(f"回测完成")
    print(f"总样本数: {result.get('总样本数', 0)}")
    print(f"总体准确率(宽松): {result.get('总体准确率(宽松)', 'N/A')}")
    print(f"耗时: {result.get('耗时(秒)', 0)}秒")

    # 周预测分析
    wp = result.get('周预测分析', {})
    if wp:
        print(f"\n{'=' * 70}")
        print(f"周预测分析（样本外验证）")
        print(f"{'=' * 70}")
        print(f"周样本数: {wp.get('周样本数', 0)}")
        print(f"周数: {wp.get('周数', 0)}")

        print(f"\n策略汇总:")
        for name, rate in wp.get('策略汇总', {}).items():
            marker = ' ★' if '混合' in name or '前3天涨跌>0' == name.split(':')[-1] else ''
            print(f"  {name}: {rate}{marker}")

        print(f"\n按板块:")
        for sec, stats in wp.get('按板块', {}).items():
            print(f"  {sec}: 周样本={stats['周样本数']}, "
                  f"B:周一混合={stats['B:周一混合']}, "
                  f"C:前3天方向={stats['C:前3天方向']}")
    else:
        print("\n⚠️ 未找到周预测分析数据")

    # 按板块统计日频
    print(f"\n按板块统计(日频):")
    for sec, stats in result.get('按板块统计', {}).items():
        print(f"  {sec}({stats['股票数']}只): 样本{stats['样本数']}, "
              f"宽松{stats['准确率(宽松)']}")

    # 各股票汇总
    print(f"\n各股票汇总:")
    for s in result.get('各股票汇总', []):
        print(f"  {s['股票名称']}({s['股票代码']})[{s['板块']}]: {s['回测天数']}天, "
              f"宽松{s['准确率(宽松)']}")

    print(f"\n结果已保存到: {output_path}")


if __name__ == '__main__':
    asyncio.run(main())
