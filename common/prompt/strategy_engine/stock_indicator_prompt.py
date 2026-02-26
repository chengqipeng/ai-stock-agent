import json

from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_kline_cn
from service.eastmoney.strategy_engine.stock_BOLL_rule import get_boll_rule_boll_only
from service.eastmoney.strategy_engine.stock_KDJ_rule import get_kdj_rule_kdj_only
from service.eastmoney.strategy_engine.stock_MACD_rule import get_macd_signals_macd_only


async def get_stock_indicator_prompt(stock_info: StockInfo):
    data_num = 120
    boll_rule_boll = await get_boll_rule_boll_only(stock_info)
    stock_day_kline = await get_stock_day_kline_cn(stock_info, data_num)
    kdj_rule_kdj = await get_kdj_rule_kdj_only(stock_info, data_num)
    macd_signals_macd = await get_macd_signals_macd_only(stock_info, data_num)

    return f"""
# 角色设定
你现在是一位拥有20年实战经验的A股资深技术面分析师，精通各类技术指标的底层逻辑、量价关系以及A股市场的资金博弈规律。你的分析客观、严谨，能够透过数据表象看清多空力量的真实对比。

# 任务目标
基于我提供的某只A股的具体数据，从技术维度进行深度剖析，综合研判多空趋势，并给出明确的“明日操作建议”以及“股票综合打分（满分100分）”。

# 分析维度与逻辑要求
请严格基于以下4个核心维度进行交叉验证与深度分析：
1. **MACD指标分析**：
    MACD 多空状态、交叉信号与背离信号计算

    Rule a（多空市场界定）：
      DIF>0 且 DEA>0 → Bull_Strong（强多头）
      DIF>0 且 DEA≤0 → Bull_Weak（弱多头）
      DIF<0          → Bear（空头）

    Rule b（交叉信号）：
      Golden_Cross  ：DIF 由下穿上 DEA（金叉，看涨）
      Death_Cross   ：DIF 由上穿下 DEA（死叉，看跌）
      Zero_Above_GC ：金叉且 DIF>0 & DEA>0（零轴上金叉，抓主升段）
      Zero_Below_DC ：死叉且 DIF<0（零轴下死叉，防暴跌）

    Rule c（背离预警，状态机，无未来函数）：
      底背离（Bottom_Divergence，看涨）触发条件：
        条件A：当前空头波段最低价（不一定形成了金叉，形成过程中也需要计算） < 上一空头波段最低价（股价创新低）
        条件B：当前空头波段 DIF 最低值 > 上一空头波段 DIF 最低值（DIF 未创新低）
        窗口 ：两波谷索引间距在 20~60 个交易日内
      顶背离（Top_Divergence，看跌）触发条件：
        条件A：当前多头波段最高价（不一定形成了死叉，形成过程中也需要计算） > 上一多头波段最高价（股价创新高）
        条件B：当前多头波段 DIF 最高值 < 上一多头波段 DIF 最高值（DIF 未创新高）
        窗口 ：两波峰索引间距在 20~60 个交易日内
2. **KDJ指标分析**：
    KDJ 法则（微观动能）
    买入：过去5天内曾进入超卖区（K<20, D<20, J<0），且发生金叉（昨日K<=昨日D，今日K>今日D），且今日J>昨日J → Buy
    卖出（钝化）：K>80 连续 blunt_n 天（is_high_blunted），收盘跌破 MA5/MA20 或 defense_line → Sell (Blunted Exit)；防守线未破则持股死捂，死叉信号被屏蔽
    卖出（普通）：非钝化状态下，过去5天内曾进入超买区（K>80, D>80, J>100），且发生死叉（昨日K>=昨日D，今日K<今日D）→ Sell (Standard)
3. **BOLL（布林带）分析**：
    布林线法则 (运行空间) 核心逻辑
    强势开启信号：放量突破中轨（昨收 <= 昨中轨 且 今收 > 今中轨 且 量 > 50日均量×1.5）
    波段结束信号：跌破中轨（昨收 >= 昨中轨 且 今收 < 今中轨）
    可操作区：收盘 > 中轨 且 中轨向上倾斜
    喇叭口扩张加速：上下轨反向张开 且 带宽单日放大超10%
4. **近{data_num}日交易数据**：
    研判中线趋势结构与关键支撑/压力。结合过去{data_num}个交易日的K线走势、均线系统（如MA20/MA50等）、阶段性高低点及整体形态（如箱体震荡、上升通道、W底、头肩顶等），分析多空力量的中期博弈格局。

## 明日操作建议与沙盘推演
* **明日操作定调**：[明确给出：积极买入 / 逢低建仓 / 持股待涨 / 逢高减仓 / 清仓离场 / 保持观望]
* **支撑位与阻力位**：[结合近{data_num}日数据，给出具体的强支撑位和重要压力位]
* **应对策略推演**：
    * **情景A（若明日向上突破阻力/放量拉升）**：该如何操作。
    * **情景B（若明日向下跌破支撑/缩量阴跌）**：该如何操作。

**以下是我为您提供的该A股最新技术数据，请开始您的分析：**

* **1. MACD数据**：{json.dumps(macd_signals_macd, ensure_ascii=False, indent=2)}
* **2. KDJ数据**：{json.dumps(kdj_rule_kdj, ensure_ascii=False, indent=2)}
* **3. BOLL数据**：{json.dumps(boll_rule_boll, ensure_ascii=False, indent=2)}
* **4. 近{data_num}日交易数据**：{json.dumps(stock_day_kline, ensure_ascii=False, indent=2)}

[最终输出] 只能输出json格式数据：
{{
  'stock_code': '<股票代码>',
  'stock_name': '<股票名称>',
  'not_hold_grade': '<未持有建议，积极买入 / 逢低建仓 / 持股待涨 / 逢高减仓 / 清仓离场 / 保持观望>',
  'hold_grade': '<持有建议，积极买入 / 逢低建仓 / 持股待涨 / 逢高减仓 / 清仓离场 / 保持观望>',
  'not_hold_content': '<未持有应该怎么操作，结合MACD、KDJ、BOLL、近日交易数据指标分析，若为积极买入 / 逢低建仓 / 保持观望时需要提供合理买入点建议，200字以内>'
  'hold_content': '<持有应该怎么操作，结合MACD、KDJ、BOLL、近日交易数据指标分析，若为持股待涨 / 逢高减仓 / 清仓离场时需要提供合理卖出点建议，200字以内>'
}}
"""

