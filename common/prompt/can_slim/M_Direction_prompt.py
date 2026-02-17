import json
from datetime import datetime
import pandas as pd

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.technical.stock_day_range_kline import calculate_moving_averages
from service.eastmoney.technical.abs.stock_indicator_base import get_stock_history_kline_max_min

"""
这是一个非常经典的量化交易信号，源自威廉·欧奈尔（William O'Neil）的 CAN SLIM 系统。
这个指标用于判断大盘是否面临抛压，从而决定是否需要减轻仓位。
以下是实现"出货日（Distribution Day）"计数的详细逻辑分析和 Python 代码实现。
1. 逻辑拆解 (Logical Analysis)我们需要将文字描述转化为数学公式和代码逻辑：价格条件 (Price Condition):当日收盘价相对于前一日收盘价下跌。
跌幅必须大于 0.2%。
公式：$\frac{Close_t - Close_{t-1}}{Close_{t-1}} < -0.002$成交量条件 (Volume Condition):当日成交量大于前一日成交量。
公式：$Volume_t > Volume_{t-1}$时间窗口 (Time Window):过去 4-5 周。
在交易日历中，一周通常只有 5 个交易日。因此，我们关注的时间窗口是最近的 20 到 25 个交易日。计数逻辑 (Counting):在选定的时间窗口内，同时满足上述两个条件的日子总数。
"""

async def distribution_Days_Count(stock_info: StockInfo, window_days: int = 25) -> dict:
    """计算出货日计数
    
    Args:
        stock_info: 股票信息
        window_days: 回溯天数，默认25天（5周）
        
    Returns:
        包含出货日分析结果的字典
    """
    kline_data = await get_stock_history_kline_max_min(stock_info)
    
    # 转换为DataFrame
    df = pd.DataFrame.from_dict(kline_data, orient='index')
    df.index.name = 'date'
    df = df.sort_index()
    
    # 计算涨跌幅
    df['pct_chg'] = df['close_price'].pct_change()
    
    # 计算前一日成交量
    df['prev_vol'] = df['trading_volume'].shift(1)
    
    # 判断是否为出货日：跌幅>0.2% 且 成交量放大
    df['is_distribution'] = (df['pct_chg'] < -0.002) & (df['trading_volume'] > df['prev_vol'])
    
    # 滚动窗口计数
    df['distribution_count'] = df['is_distribution'].rolling(window=window_days).sum()
    
    # 获取最新数据
    latest = df.iloc[-1]
    
    # 获取过去window_days内的出货日详情
    recent_df = df.tail(window_days)
    distribution_days_list = recent_df[recent_df['is_distribution']].index.tolist()
    
    return {
        "分析日期": df.index[-1],
        "今日是否出货日": bool(latest['is_distribution']),
        "过去{}个交易日出货日总数".format(window_days): int(latest['distribution_count']),
        "出货日列表": distribution_days_list,
        "当前收盘价": latest['close_price'],
        "涨跌幅": round(latest['pct_chg'] * 100, 2) if pd.notna(latest['pct_chg']) else None
    }

async def build_M_Direction_prompt(stock_info: StockInfo) -> str:
    indices_stock_info = get_stock_info_by_name(stock_info.indices_stock_name)
    indices_moving_averages = await calculate_moving_averages(indices_stock_info)
    distribution_days = await distribution_Days_Count(indices_stock_info)

    return f"""
#分析的股票（{datetime.now().strftime('%Y-%m-%d')}）
    {stock_info.stock_name}（{stock_info.stock_code_normalize}）    

** 指数（{stock_info.indices_stock_name}）的"价格与均线"位置 **
    {json.dumps(indices_moving_averages, ensure_ascii=False, indent=2)}
    
"出货日"计数 (Distribution Days Count):
    {json.dumps(distribution_days, ensure_ascii=False, indent=2)}

市场广度与领军股表现 (Breadth & Leaders):
创新高 vs 创新低家数 (NH/NL)： 是创新高的多，还是创新低的多？
领军股状态： 市场里最强的 5-10 只股票（如之前的龙头）最近是在创新高，还是在破位大跌？

[角色设定] 你现在是一位极其保守的、遵循欧奈尔趋势跟踪策略的市场分析师。你的任务不是预测明天会涨还是跌，而是根据当下的量价数据，精准判断目前的**"市场阶段" (Market Status)**。
[判断逻辑与红绿灯机制]
请根据我提供的指数数据，将市场状态归类为以下三种之一，并给出相应的仓位建议：
1. 确立上升趋势 (Confirmed Uptrend) —— 🟢 绿灯
    特征：
    指数位于 21日 和 50日均线上方。
    近期出现过**"跟进日" (Follow-Through Day)**（即在触底后的第 4-10 天，指数大涨 > 1.5% 且成交量显著放大）。
    很少有"出货日"聚集。
    策略建议：全仓进攻 (Aggressive Buy)。 此时是 CAN SLIM 策略成功率最高的时候，积极买入突破新高的领军股。
2. 趋势承压 (Uptrend Under Pressure) —— 🟡 黄灯
    特征：
    指数虽然还在上涨，但成交量开始萎缩（量价背离）。
    或者，在过去 3-4 周内，累计出现了 3-5 个"出货日"（机构在偷偷卖出）。
    指数跌破了 21日均线，正在测试 50日均线支撑。
    策略建议：谨慎防守 (Caution)。 停止开设新仓位。对于现有持仓，一旦触及止损线坚决卖出。如果是盈利的股票，考虑落袋为安，将仓位降至 50% 左右。
3. 市场调整/下跌趋势 (Market in Correction) —— 🔴 红灯
    特征：
    指数有效跌破 50日均线，且均线方向向下。
    "出货日" 密集出现（> 6 天）。
    前期的领军股纷纷破位大跌（补跌）。
    创新低家数 > 创新高家数。
策略建议：现金为王 (Cash is King)。 此时 100% 空仓或持有现金。严禁买入任何股票，哪怕它的财报（C/A）再好。不要试图抄底，直到下一个"跟进日"出现。
[最终输出] 请基于上述逻辑，输出结论： "当前市场处于【🟢 确立上升 / 🟡 趋势承压 / 🔴 下跌调整】阶段。基于 CAN SLIM 规则，建议总仓位控制在【0% / 30-50% / 80-100%】。主要风险点是【指出具体的出货日或均线压力】。"


"""

if __name__ == "__main__":
    import asyncio

    async def main():
        stock_info = get_stock_info_by_name("上证指数")
        result = await distribution_Days_Count(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
