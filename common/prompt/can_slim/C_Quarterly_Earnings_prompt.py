from service.eastmoney.forecast.stock_institution_forecast_list import get_institution_forecast_future_to_json, \
    get_institution_forecast_historical_to_json
from service.eastmoney.forecast.stock_institution_forecast_summary import get_institution_forecast_summary_future_json, \
    get_institution_forecast_summary_historical_json
from service.eastmoney.stock_info.stock_financial_main import get_financial_data_to_json
import json
from datetime import datetime


async def get_C_Quarterly_Earnings_prompt(secucode, stock_name):
    financial_revenue = await get_financial_data_to_json(secucode=secucode, indicator_keys=['REPORT_DATE', 'TOTALOPERATEREVETZ', 'SINGLE_QUARTER_REVENUE', 'TOTALOPERATEREVE'])
    financial_profit = await get_financial_data_to_json(secucode=secucode, indicator_keys=['REPORT_DATE', 'SINGLE_QUARTER_PARENTNETPROFITTZ', 'SINGLE_QUARTER_KCFJCXSYJLRTZ'])
    financial_eps = await get_financial_data_to_json(secucode=secucode, indicator_keys=['REPORT_DATE', 'EPSJB'])
    historical_forecast_json = get_institution_forecast_historical_to_json(secucode=secucode)
    future_forecast_json = get_institution_forecast_future_to_json(secucode=secucode)
    historical_forecast_summary = get_institution_forecast_summary_historical_json(secucode=secucode)
    future_forecast_summary = get_institution_forecast_summary_future_json(secucode=secucode)
    
    return f"""

作为拥有 30 年经验的华尔街投资专家，我必须强调：在 CAN SLIM 模型的 C (Current Quarterly Earnings) 维度中，“扣非净利润”只是底线（排雷项），而非进攻信号（买入项）。
要捕捉到真正的“超级成长股”（Super Growth Stocks），你必须组合观察以下 3 个核心杀手级指标。缺一不可，这就是区别“平庸股”与“大牛股”的分水岭。

#分析的股票（{datetime.now().strftime('%Y-%m-%d')}）
{stock_name}({secucode})

#分析要求：
## 所有增长率计算必须基于'同比'，即本季度与去年同一季度对比。严禁使用环比数据，以消除季节性干扰。
## 请基于我接下来提供的财报数据，严格按照以下标准进行评估。

## 1. 单季营业收入同比增长率
   指标定义：公司主营业务收入的季度同比增长。
   欧奈尔铁律：盈利增长必须由营收增长驱动，而非削减成本。
   判定标准：
     及格线：> 25%。
     卓越线：> 50% 或更高。
   逻辑：只有产品卖得好，业绩才能持续。如果一家公司 EPS 暴涨 50%，但营收只增长 5%，这通常是靠“勒紧裤腰带”（削减成本）挤出来的利润，这种增长极度脆弱，必须回避。
   分析使用的数据源：
   <营业总收入同比增长(%)>
   {json.dumps(financial_revenue, ensure_ascii=False)}

## 2. 业绩加速趋势
   指标定义：当季归属净利润同比增长(%)，当季扣非净利润同比增长(%)。
   欧奈尔铁律：我们要买的是“加速度”，而不是单纯的“速度”。
   判定标准：
     趋势向上，例如，Q1 增速 20% -> Q2 增速 35% -> Q3 增速 50%。
     创新高：本季度的净利率是否达到年度新高？
     同业对比：是否处于行业前列？
     当归属净利润同比增长和扣非净利润同比增长不一致时优先使用扣非净利润同比增长
   分析使用的数据源：
   <单季度归属净利润同比增长(%)、单季度扣非净利润同比增长(%) >
   {json.dumps(financial_profit, ensure_ascii=False)}

## 3. 超出市场预期幅度
   指标定义：(实际 EPS - 分析师一致预期 EPS) / 分析师一致预期 EPS。
   欧奈尔铁律：惊喜是股价上涨的燃料。
   判定标准：
     正向惊喜：实际业绩大幅好于分析师预期（Surprise > 10-20%）。
   逻辑：机构通常会根据“一致预期”定价。一旦业绩大幅超预期，机构必须重新调整估值模型，被迫在盘中抢筹，从而推高股价，需要对比往年预测和真实数据，再基于未来预测进行分析。
   分析使用的数据源：
   <基本每股收益>
   {json.dumps(financial_eps, ensure_ascii=False)} \n
   <机构往年预测数据（每季每股收益、市盈率）>
   {json.dumps(historical_forecast_json, ensure_ascii=False)} \n
   <机构未来预测数据（每季每股收益、市盈率）>
   {json.dumps(future_forecast_json, ensure_ascii=False)} \n
   <机构往年预测数据（财务指标）>
   {historical_forecast_summary} \n
   <机构未来预测数据（财务指标）>
   {future_forecast_summary} \n
   
"""