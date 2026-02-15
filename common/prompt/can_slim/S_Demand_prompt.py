import json
from datetime import datetime

from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_base_info import get_stock_base_info_json
from service.eastmoney.stock_info.stock_financial_main_with_total_share import get_equity_data_to_json
from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history_json, get_fund_flow_history_json_cn
from service.eastmoney.stock_info.stock_holder_data import get_org_holder_json
from service.eastmoney.stock_info.stock_lock_up_period import get_stock_lock_up_period_year_range
from service.eastmoney.stock_info.stock_realtime import get_stock_realtime_json
from service.eastmoney.stock_info.stock_repurchase import get_stock_repurchase_json
from service.eastmoney.stock_info.stock_top_ten_shareholders_circulation import \
    get_top_ten_shareholders_circulation_by_dates
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json

async def get_5_day_volume_ratio(stock_info: StockInfo):
    moving_averages_json = await get_moving_averages_json(stock_info, ['close_5_sma'], 50)
    fund_flow_history_json = await get_fund_flow_history_json(stock_info, ['date', 'close_price', 'change_pct'])

    ma_dict = {item['date']: item['close_5_sma'] for item in moving_averages_json}
    
    result = []
    for item in fund_flow_history_json[:50]:
        date = item['date']
        close_price = item['close_price']
        change_pct = item['change_pct']
        
        if date in ma_dict and ma_dict[date]:
            close_5_sma = ma_dict[date]
            quantity_relative_ratio = close_price / close_5_sma
            result.append({
                'date': date,
                'close_price': close_price,
                'change_pct': change_pct,
                'quantity_relative_ratio': round(quantity_relative_ratio, 4)
            })
    
    return result

async def get_S_Demand_prompt(stock_info: StockInfo) -> str:
    equity_data_with_total_shares_to_json = await get_equity_data_to_json(stock_info, ['END_DATE', 'TOTAL_SHARES'])
    equity_data_with_unlimited_shares_to_json = await get_equity_data_to_json(stock_info, ['END_DATE', 'UNLIMITED_SHARES'])
    top_ten_shareholders_circulation_by_dates = await get_top_ten_shareholders_circulation_by_dates(stock_info)
    org_holder_json = await get_org_holder_json(stock_info)
    moving_averages_json = await get_moving_averages_json(stock_info, ['close_50_sma'], 50)
    stock_realtime_json = get_stock_realtime_json(stock_info, ['stock_name', 'stock_code', 'volume'])
    fund_flow_history_json_cn = await get_fund_flow_history_json_cn(stock_info, ['date', 'change_hand', 'trading_volume', 'trading_amount'])
    stock_lock_up_period_year_range = await get_stock_lock_up_period_year_range(stock_info)
    stock_repurchase_json = await get_stock_repurchase_json(stock_info)
    return f"""
#分析的股票（{datetime.now().strftime('%Y-%m-%d')}）
{stock_info.stock_name}（{stock_info.stock_code_normalize}）

要让大模型分析 S，你不能只给它 K 线图（大多数模型看不懂图形细节），你需要提供以下量化数据：
1. 股本结构数据：
   ** 总股本 (Total Shares Outstanding) （近三年数据)）**
   {json.dumps(equity_data_with_total_shares_to_json, ensure_ascii=False, indent=2)}
   
   ** 流通股本 (Floating Shares / The Float) —— 最关键数据（近三年数据）**
   {json.dumps(equity_data_with_unlimited_shares_to_json, ensure_ascii=False, indent=2)}
   
   ** 前十大流通股股东持股比例 (Top 10 Holders %) **
   {json.dumps(top_ten_shareholders_circulation_by_dates[:10], ensure_ascii=False, indent=2)}
   
   ** 管理层、机构持股比例 (Management Ownership %) **
   {json.dumps(org_holder_json[0], ensure_ascii=False, indent=2)}
  
2. 交易量数据：
   ** 平均日均成交量 (Average Daily Volume, ADV) — 50日平均线**
   {json.dumps(moving_averages_json, ensure_ascii=False, indent=2)}
   
   ** 最新成交量 (Current Volume) **
   {json.dumps(stock_realtime_json, ensure_ascii=False, indent=2)}
   
   ** 最新量比 (Volume Ratio) 今日成交量 / 5日均量（50日数据） **
   {json.dumps(get_5_day_volume_ratio, ensure_ascii=False, indent=2)}

3. A股特色指标：
   ** 换手率 (Turnover Rate) 近半年**
   {json.dumps(fund_flow_history_json_cn, ensure_ascii=False, indent=2)}
   
   ** 解禁日期 (Lock-up Expiration Date) — 巨大的潜在供给 **
   {json.dumps(stock_lock_up_period_year_range, ensure_ascii=False, indent=2)}
   
   ** 回购注销数据 (Buybacks) — 供给减少的最强信号 **
   {json.dumps(stock_repurchase_json, ensure_ascii=False, indent=2)}

你现在是一位精通“筹码供需理论”的资深交易员。请根据我提供的股本和交易数据，对该股票的 CAN SLIM "S" 维度进行压力测试。

1. 盘子大小与弹性 (The Float Supply)
   核心逻辑： 小盘股爆发力强，大盘股稳定性高但爆发难。
   判断规则：
     极佳： 流通股本 < 1 亿股（或 A 股流通市值 < 100 亿人民币）。这种股票主要筹码被锁定，稍有买盘就能涨停。
     中等： 流通市值 100亿 - 500亿。适合机构建仓，稳健上涨。
     沉重： 流通市值 > 1000亿（如银行、两桶油）。除非有滔天巨量的资金推动，否则很难在短期内翻倍。

2. 量价行为分析 (Volume Analysis)
   核心逻辑： 成交量是大资金留下的脚印。我们要找的是“吸筹”，避开的是“出货”。
   判断规则：
     上涨放量 (Accumulation)： 在股价上涨的日子里，成交量是否显著放大（> 50日均量的 40% 以上）？这是机构抢筹的证据。
     下跌缩量 (Dry Up)： 在股价回调或盘整时，成交量是否急剧萎缩？这意味着抛压枯竭，没人愿意卖了（供给短缺）。
     警戒信号： 如果股价滞涨或下跌，但成交量巨大（放量滞涨），这是典型的“出货”信号，直接否决。

3. 筹码锁定与管理层信心 (Ownership & Buybacks)
   核心逻辑： 供给越少越好。
   判断规则：
     管理层持股： 创始人或高管持股比例较高（> 20%）是加分项，说明他们与股东利益一致。
     股票回购 (Buybacks)： 公司近期是否有“注销式回购”？这是直接减少供给（分母变小），从而提高 EPS 的行为，是 S 维度的最强加分项。

4. A 股特别警示：解禁与换手 (The Local Risks)
   解禁悬崖： 检查未来 3-6 个月是否有大规模限售股解禁？如果有 > 5% 总股本的解禁，视为“供给海啸”，建议回避。
   换手率监控：
     健康活跃：3% - 7%。
     过热预警：> 15% - 20%（往往是短线游资博弈，筹码松动，非 CAN SLIM 长持风格）。

[最终输出] 请基于上述逻辑，输出结论： “该股票的筹码结构是【轻盈/适中/沉重】，且供需状态处于【机构吸筹/散户博弈/主力出货】阶段。”
"""