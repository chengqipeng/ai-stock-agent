import json

from common.utils.stock_info_utils import StockInfo
from service.eastmoney.indices.stock_market_data import get_stock_relative_strength
from service.eastmoney.stock_info.stock_industry_ranking import get_stock_industry_ranking_json


async def get_L_or_Laggard_prompt(stock_info: StockInfo) -> str:
    stock_relative_strength = await get_stock_relative_strength(stock_info)
    stock_industry_ranking_json = await get_stock_industry_ranking_json(stock_info)

    return f"""
    
大模型不能凭空猜谁是老大，你需要喂给它具体的**“比武数据”**。最关键的是区分 RS（相对强度） 和 RSI（相对强弱指标）——这是两个完全不同的概念，千万别搞混。
1. 相对价格强度 (Price Relative Strength, RS，近一年数据) 
   ** 注意： 这不是技术指标里的 RSI (0-100 的震荡指标)。欧奈尔 RS 评级： IBD 的独家数据（1-99分）。如果没有 IBD 数据，你需要提供**“过去 12 个月的股价涨幅 vs 基准指数（如 沪深300 或 S&P 500）涨幅”**。
   {json.dumps(stock_relative_strength, ensure_ascii=False, indent=2)}

2. 抗跌性表现 (Resilience during Correction):
   最近一次大盘回调（Market Correction）时，该股票的跌幅是多少？
   同期大盘跌幅是多少？

3. 行业地位 (Sector Rank):
   ** 该股票在所属细分行业（如“半导体设备”而非笼统的“科技”）中的市值排名，同板块其他竞争对手的近期表现。** 
   {json.dumps(stock_industry_ranking_json, ensure_ascii=False, indent=2)}
   

[角色设定] 你现在是一位冷酷无情的“选股裁判”。我们遵循 CAN SLIM 原则中的 "L" (Leader or Laggard)。你的任务是剔除平庸的跟随者，只保留市场真正的领军股。

[关键概念澄清]
RS (Relative Strength)： 指股价相对于大盘指数的强弱表现，而非 RSI 震荡指标。
领军股定义： 它是率先突围、涨幅最大、抗跌最强的股票，通常是行业前 1-2 名。

[分析逻辑与评分标准]
1. 相对强度评分 (The RS Test)
   数据输入： [股票 A] 过去 12 个月涨幅为 [X]%，同期 [基准指数] 涨幅为 [Y]%。
   判定逻辑：
     领军者 (Pass)： 股价走势显著强于大盘。当大盘创新低时，它拒绝创新低；当大盘反弹时，它率先创新高。RS 数值（如果使用 1-99 评分）必须 > 80，甚至 > 90。
     落后者 (Fail)： 股价跟随大盘波动，或者比大盘更弱。如果大盘涨它不涨，大盘跌它暴跌，直接淘汰。

2. “大盘回调”压力测试 (The Correction Test)
   场景： 回顾最近一次大盘调整（例如指数下跌 10%）。
   判定逻辑：
     超级强势： 大盘跌 10%，该个股仅跌 5% 甚至逆势横盘（构建 Base）。这是最强烈的买入信号，说明有机构在护盘。
     普通跟随： 大盘跌 10%，个股也跌 10-15%。尚可接受，但需观察。
     弱势崩溃： 大盘跌 10%，个股崩盘下跌 20-30%。这是典型的落后股特征，直接剔除。

3. 行业地位确认 (Industry Leadership)
   提问： 在 [细分行业名称] 中，该公司的利润增速和股价表现是否排名前 2？
   判定逻辑： 如果你买的不是行业里的“龙一”或“龙二”，而是“龙五”、“龙六”，请给出警告：“这是同情补涨股（Sympathy Play），风险极高。”

[最终输出] 请基于以上分析，给出结论： “该股票在 L 维度属于【绝对领军 / 跟随者 / 弱势股】。其相对强度 (RS) 表现【优于 / 弱于】 90% 的市场标的。”
"""