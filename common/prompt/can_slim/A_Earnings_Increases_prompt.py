from datetime import datetime
import json
from service.eastmoney.stock_info.stock_financial_main import get_financial_data_to_json
from common.utils.financial_calculator import calculate_eps_from_deducted_profit


async def get_A_Earnings_Increases_prompt(secucode, stock_name):
    eps_data = await calculate_eps_from_deducted_profit(secucode)
    roe_data = await get_financial_data_to_json(secucode, indicator_keys=['REPORT_DATE', 'ROEKCJQ'])
    cash_flow_data = await get_financial_data_to_json(secucode, indicator_keys=['REPORT_DATE', 'MGJYXJJE'])
    profit_growth_data = await get_financial_data_to_json(secucode, indicator_keys=['REPORT_DATE', 'KCFJCXSYJLRTZ'])
    
    return f"""
在华尔街，我们常说："C 吸引眼球，A 留住资金。"（"C" catches the eye, "A" keeps the money.）如果一家公司只有强劲的季度报表，但缺乏稳健的年度增长记录，那它很可能只是昙花一现的"烟花股"。
以下是基于欧奈尔 CAN SLIM 模型的 A 维度 深度拆解，包含了你必须抓取的核心数据、底层逻辑以及实战判读标准。

#分析的股票（{datetime.now().strftime('%Y-%m-%d')}）
{stock_name}({secucode})

一、 核心数据清单 (The "Must-Have" Data)
在分析 A 维度时，请基于我接下来提供的财报数据，调取以下 4 组关键年度数据，严格按照以下标准进行评估：
1. 过去 3-5 年的年度 EPS（扣非每股收益）
   提取指标：扣非净利润(元) / 总股本 - 取过去三年
   作用：计算复合增长率。
   分析使用的数据源：
   <扣非净利润(元) / 总股本>
   {json.dumps(eps_data, ensure_ascii=False)}

2. 过去 3-5 年的年度 ROE（净资产收益率(扣非/加权)）
   提取指标：净资产收益率(加权)(%) - 取过去三年
   作用：衡量资金使用效率。
   分析使用的数据源：
   <净资产收益率(扣非/加权)>
   {json.dumps(roe_data, ensure_ascii=False)}

3. 每股经营现金流 (Operating Cash Flow per Share)
   提取指标：每股经营现金流(元) - 取过去三年
   作用：验证利润的"含金量"。
   分析使用的数据源：
   <每股经营现金流(元)>
   {json.dumps(cash_flow_data, ensure_ascii=False)}

4. 扣非净利润同比增长(%)
   提取指标：扣非净利润同比增长(%) - 取过去三年
   作用：判断盈利能力的趋势。
   分析使用的数据源：
   <扣非净利润同比增长(%)>
   {json.dumps(profit_growth_data, ensure_ascii=False)}

二、 深度分析逻辑与解读 (The Logic & Interpretation)
1. 年度 EPS 增长率：寻找"复利机器"
   欧奈尔标准：过去 3 年的年度 EPS 复合增长率（CAGR）必须 > 25%。
   深度解读：
     为什么是 25%？根据"72法则"，如果一家公司每年增长 25%，它的利润在 3 年内就能翻倍。这种内生性增长是支撑股价翻倍的唯一物理基础。
     稳定性（Stability）：我们不仅看增速，还要看稳定性。
       优质形态：EPS 从 1.0 -> 1.3 -> 1.7 -> 2.2（逐年稳步走高）。
       劣质形态：EPS 从 1.0 -> 0.5 -> 2.0 -> 1.2（剧烈波动，说明业务不稳定或受周期影响太大）。

2. ROE（净资产收益率）：识别"高效能管理层"
   欧奈尔标准：年度 ROE 必须 > 17%。
   深度解读：
     这是区分"成长股"与"平庸股"最锋利的刀。
     高 ROE 意味着公司不需要频繁融资（圈钱）就能利用现有资本创造高额回报。对于像北方华创这样的硬科技公司，高 ROE 通常代表其在供应链中拥有话语权（可以压榨上游、预收下游），或者技术壁垒极高（高毛利）。
   实战经验：超级牛股（如早期的微软、谷歌、茅台）的 ROE 通常在 20%-50% 之间。

3. 每股经营现金流：利润的"测谎仪"
   欧奈尔标准：每股经营现金流 > 每股收益（EPS），或至少接近。
   深度解读：
     "利润是观点，现金是事实。"
     如果一家公司年度 EPS 显示赚了 5 块钱，但每股现金流只有 0.5 元，这说明利润要么是堆积在仓库里的"存货"，要么是挂在账上的"应收账款"。
     在 A 股科技股中，必须警惕"纸面富贵"。若 经营现金流/EPS < 20%，即使 EPS 增速再高，在 CAN SLIM 模型中也需要打大折扣。
4. 困境反转（Turnaround）的特例
   欧奈尔规则：如果是受宏观环境影响导致某一年业绩下滑，随后的恢复必须创出新高。
   深度解读：
     如果某公司 2023 年 EPS 下滑了，2024 年必须强力反弹，且数值必须超过 2022 年的高点。如果只是反弹但没创新高，那叫"死猫跳"，不是"成长"。

三、 专家级实战判定流程 (The Decision Flow)
请按照以下步骤对股票进行 A 维度打分：
步骤 1：计算复合增速 (CAGR)
   CAGR = (\frac{{\text{{最新年度EPS}}}}{{\text{{3年前EPS}}}})^{{\frac{{1}}{{3}}}} - 1$$
   合格：CAGR > 25%。
   优秀：CAGR > 50%。

步骤 2：检查 ROE 质量
  查看最新年报 ROE。
  合格：> 17%。
  加分项：ROE 逐年提升（例如：15% -> 18% -> 22%）。

步骤 3：现金流验证 (The Reality Check)
  公式：$\frac{{\text{{每股经营现金流}}}}{{\text{{每股收益 EPS}}$
  安全区：> 1.0 （说明利润全部变成了现金）。
  警戒区：< 0.8 （需要检查应收账款是否暴增）。

四、 针对 A 股半导体（如北方华创）的修正视角
作为专家，在应用 CAN SLIM 分析中国硬科技股时，我会对 A 维度 做微调：
1.研发费用加回（R&D Adjustment）：
  对于北方华创这类高研发投入公司，有时利润会被巨额研发费用"吃掉"。欧奈尔允许在心中将"研发投入"视为一种对未来的投资。如果 (净利润 + 研发费用) / 市值 很有吸引力，且营收增速（C 维度）极高，可以适当放宽 EPS 的绝对数值要求，但 ROE 必须坚挺。
2.政府补助的剥离：
  再次强调，A 维度看的是长跑能力。如果过去 3 年的 EPS 增长主要靠"政府补助"维持，一旦补贴退坡，增长逻辑就会崩塌。必须确认扣非后的 ROE 是否依然 > 15%。

总结一句：
在 A 维度，我们要找的是**"利润长牛"和"现金奶牛"的结合体。如果一只股票能连续 3 年保持 EPS 30% 以上增长，且 ROE 维持在 20% 以上，这就是机构抱团**最坚实的理由。
"""
