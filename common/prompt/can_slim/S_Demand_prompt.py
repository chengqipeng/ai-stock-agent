"""S供需分析提示词模板"""

S_DEMAND_PROMPT_TEMPLATE = """
当前系统时间：{system_time}

#分析的股票（{current_date}）
{stock_name}（{stock_code}）

要让大模型分析 S，你不能只给它 K 线图（大多数模型看不懂图形细节），你需要提供以下量化数据：
1. 股本结构数据：
   ** 总股本 (Total Shares Outstanding) （近三年数据)）**
   {total_shares_json}
   
   ** 流通股本 (Floating Shares / The Float) —— 最关键数据（近三年数据）**
   {unlimited_shares_json}
   
   ** 前十大流通股股东持股比例 (Top 10 Holders %) **
   {top_ten_holders_json}
   
   ** 管理层、机构持股比例 (Management Ownership %) **
   {org_holder_json}
  
2. 交易量数据：
   ** 日均成交量 (Average Daily Volume, ADV) — 20日平均**
   {day_20_volume_avg_cn}
   
   ** 最新成交量 (Current Volume) **
   {stock_realtime_json}
   
   ** 最新量比 (Volume Ratio) 今日成交量 / 5日均量（50日数据） **
   {five_day_volume_ratio_json}

3. A股特色指标：
   ** 换手率 (Turnover Rate) 近半年**
   {fund_flow_history_json_cn}
   
   ** 解禁日期 (Lock-up Expiration Date) — 巨大的潜在供给 **
   {stock_lock_up_period_json}
   
   ** 回购注销数据 (Buybacks) — 供给减少的最强信号 **
   {stock_repurchase_json}

你现在是一位精通"筹码供需理论"的资深交易员。请根据我提供的股本和交易数据，对该股票的 CAN SLIM "S" 维度进行压力测试。

1. 盘子大小与弹性 (The Float Supply)
   核心逻辑： 小盘股爆发力强，大盘股稳定性高但爆发难。
   判断规则：
     极佳： 流通股本 < 1 亿股（或 A 股流通市值 < 100 亿人民币）。这种股票主要筹码被锁定，稍有买盘就能涨停。
     中等： 流通市值 100亿 - 500亿。适合机构建仓，稳健上涨。
     沉重： 流通市值 > 1000亿（如银行、两桶油）。除非有滔天巨量的资金推动，否则很难在短期内翻倍。

2. 量价行为分析 (Volume Analysis)
   核心逻辑： 成交量是大资金留下的脚印。我们要找的是"吸筹"，避开的是"出货"。
   判断规则：
     上涨放量 (Accumulation)： 在股价上涨的日子里，成交量是否显著放大（> 50日均量的 40% 以上）？这是机构抢筹的证据。
     下跌缩量 (Dry Up)： 在股价回调或盘整时，成交量是否急剧萎缩？这意味着抛压枯竭，没人愿意卖了（供给短缺）。
     警戒信号： 如果股价滞涨或下跌，但成交量巨大（放量滞涨），这是典型的"出货"信号，直接否决。

3. 筹码锁定与管理层信心 (Ownership & Buybacks)
   核心逻辑： 供给越少越好。
   判断规则：
     管理层持股： 创始人或高管持股比例较高（> 20%）是加分项，说明他们与股东利益一致。
     股票回购 (Buybacks)： 公司近期是否有"注销式回购"？这是直接减少供给（分母变小），从而提高 EPS 的行为，是 S 维度的最强加分项。

4. A 股特别警示：解禁与换手 (The Local Risks)
   解禁悬崖： 检查未来 3-6 个月是否有大规模限售股解禁？如果有 > 5% 总股本的解禁，视为"供给海啸"，建议回避。
   换手率监控：
     健康活跃：3% - 7%。
     过热预警：> 15% - 20%（往往是短线游资博弈，筹码松动，非 CAN SLIM 长持风格）。
"""
