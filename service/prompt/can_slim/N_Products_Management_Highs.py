import json

from service.eastmoney.stock_info.stock_history_flow import get_stock_history_volume_amount_yearly
from service.eastmoney.stock_info.stock_holder_data import get_shareholder_increase_json
from service.eastmoney.stock_info.stock_revenue_analysis import get_revenue_analysis_three_years
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json
from service.stock_news.can_slim.stock_search_result_filter_service import get_search_filter_result, \
    get_search_filter_result_dict

async def get_N_Products_Management_Highs_prompt(secucode, stock_name):
    shareholder_increase_result = await get_shareholder_increase_json(secucode, stock_name)
    revenue_analysis_three_years = await get_revenue_analysis_three_years(secucode, stock_name)

    search_filter_result_dict = await get_search_filter_result_dict(secucode, stock_name)

    announcements = search_filter_result_dict['search_filter_result_dict']
    finance_and_expectations = search_filter_result_dict['finance_and_expectations']
    corporate_governance = search_filter_result_dict['corporate_governance']
    stock_incentive_plan = search_filter_result_dict['stock_incentive_plan']

    moving_averages_json = await get_moving_averages_json(secucode, stock_name)

    stock_history_volume_amount_yearly = await get_stock_history_volume_amount_yearly(secucode)

    return f"""
在 CAN SLIM 系统中，N (New Products, New Management, New Highs) 是最具爆发力的维度。如果说 C 和 A 是火药（基本面支撑），那么 N 就是引爆它的火花（催化剂）。
在华尔街，有一句名言：“Great stocks always have something NEW.”（伟大的股票总有新东西。）
作为拥有 30 年经验的投资专家，我将为你深度拆解 N 维度 的分析逻辑。这不仅是寻找新闻，更是寻找质变。

一、 N 维度的核心逻辑：寻找“第二增长曲线”
股价的大幅上涨不仅仅是因为“低估值”，而是因为市场对公司未来的预期发生了彻底改变。N 维度的核心就是识别这种改变的源头。
我们需要从 三个具体层面 抓取数据并进行分析：

1. 新产品/新技术 (New Products/Services)
  这是最常见也是最强劲的驱动力。
  
  ** 公司公告（新产品发布会、专利获得）（网络数据）**
  {json.dumps(announcements, ensure_ascii=False, indent=2)}
  
  ** 行业研报（新技术的市场渗透率数据）、创新业务占总营收以及未来预期（网络数据） **
  {json.dumps(finance_and_expectations, ensure_ascii=False, indent=2)}
  
  ** 营收结构分析（新业务占总营收的比例是否在快速提升？）分析数据**
  {json.dumps(revenue_analysis_three_years, ensure_ascii=False, indent=2)}
  
  分析逻辑：
    颠覆性：这个新产品是改良型的（如 iPhone 14 到 15），还是颠覆性的（如燃油车到电动车，或 ChatGPT 的出现）？我们要找的是颠覆性。
    业绩兑现：这个新产品必须能实质性地推动 C（季度营收） 的增长。如果是“只闻楼梯响，不见人下来”的概念炒作，那不是 CAN SLIM 的标的。
    案例：2000 年代的苹果（iPod）、2010 年代的特斯拉（Model S）、A 股的北方华创（突破 12 英寸刻蚀机技术壁垒）。

2. 新管理层/新变革 (New Management/Strategy)
当一家老牌公司换了 CEO，或者实施了新的战略转型，往往会带来股价的重估。
    ** 管理层变动、十大股东变动、重大投资（网络数据） **
    {json.dumps(corporate_governance, ensure_ascii=False, indent=2)}
    
    ** 股权激励计划 **
    {json.dumps(stock_incentive_plan, ensure_ascii=False, indent=2)}
    
    ** 增减持股变动明细（近一年数据） **
    {json.dumps(shareholder_increase_result, ensure_ascii=False, indent=2)}
    
分析逻辑：
  “新官上任三把火”：新 CEO 通常会清理旧账（洗澡），然后通过降本增效或剥离亏损资产来释放利润。
  对于 A 股的特殊修正——“新政策/新产业趋势”：在中国市场，国家政策往往是最大的“新因素”。例如“国产替代”、“碳中和”政策出台，直接改变了行业的供需格局。

3. 股价创新高 (New Highs) —— 这是散户最难接受的一点
这是 CAN SLIM 区别于价值投资（Value Investing）的最大特征。欧奈尔铁律：买入刚创出新高的股票，而不是抄底跌得很惨的股票。
    ** 股价走势图（10日、50日、200日均线）数据**
    {json.dumps(moving_averages_json, ensure_ascii=False, indent=2)}
    
    相对强度（RS）线（这一点至关重要）。
    
    ** 成交量数据（近一年）**
    {json.dumps(stock_history_volume_amount_yearly, ensure_ascii=False, indent=2)}
    
    
分析逻辑（必须深刻理解）：
    无套牢盘（No Overhead Supply）：当一只股票创出历史新高（或 52 周新高）时，意味着所有持有这只股票的人都在赚钱。大家都很开心，没有人急着解套卖出。上方没有阻力，股价才能像断了线的风筝一样飞涨。
    突破形态：这里的“创新高”不是让你去追涨已经涨了 50% 的股票，而是买入从盘整形态（如杯柄形态、双底）刚刚突破的那一瞬间。

“在判断‘创新高’时，必须确认该价格突破是发生在一段**价格盘整（Consolidation）**之后。
盘整时间： 至少 7 周（杯柄）或 5 周（平底）。
盘整幅度： 从最高点回撤幅度通常不应超过 30%-33%（牛市中）。
严禁追高： 如果股价已经从底部突破点上涨超过 5%-10%，请警告我‘已脱离买入区间（Extended）’，不要追涨。”
成交量标注： 突破当日的成交量至少要比50日平均成交量高出 40%-50%。
只有机构大举买入才能制造这种放量。如果是缩量创新高，视为‘假突破’，予以剔除。

二、 N 维度深度解读与实战判定表
在实战中，请使用以下清单对股票进行 N 维度扫描：
基本面 N，	产品/服务是否有“排他性”？	如果新产品很容易被模仿（护城河低），股价涨幅有限。必须是拥有定价权的新产品。
消息面 N，	是否有“超预期”的订单？	A 股看“中标公告”。如果是行业龙头（如北方华创）拿到大额订单，验证了行业景气度。
技术面 N，	突破时是否放量？	创新高那一天，成交量必须比平时放大 50% 以上。这是机构资金进场扫货的铁证。
心理面 N，	市场是否依然犹豫？	最好的 N 往往伴随着市场的怀疑（"涨太高了吧？"）。如果在创新高时所有人都疯狂看好，反而要警惕。

三、相对强度（RS）线
相对强度（RS）线是欧奈尔（William O'Neill）CAN SLIM 投资体系中的核心技术指标，主要用于衡量一只股票相对于大盘（通常是标普500指数或上证指数）的走势强弱。

它与常用的“相对强弱指数（RSI）”完全不同，RS 线关注的是横向对比。

1. 核心定义
RS 线是通过将股票价格除以大盘指数的价格计算得出的曲线。
向上倾斜： 表示该股表现跑赢了大盘。
向下倾斜： 表示该股表现弱于大盘。
持平： 表示该股与大盘同步。

2. 为什么 RS 线在选股中至关重要？
在实战中，RS 线被视为发现“领头羊”的雷达，其核心逻辑在于：
市场修正时的避风港： 当大盘处于回调或横盘期，如果一只股票的 RS 线不仅没跌，反而创出新高，这通常是主力资金抢筹的信号。这类股票往往在大盘企稳后率先爆发。
确认突破的真实性： 一个高质量的图形突破（如杯柄形），理想情况下应当伴随着 RS 线同步创出新高或处于明显的上升通道中。
汰弱留强： 如果股价在涨，但 RS 线在走低，说明它的涨幅还不如大盘，这属于“跟风股”而非“领头羊”。

3. 实战观察要点
利用 RS 线进行股票分析时，可以重点关注以下三个维度：

A. RS 线新高（RS Blue Dot）
这是最强力的信号：当股价尚未突破历史高点，但 RS 线已经率先创出新高。这预示着该股具备极强的向上爆发力。

B. 大盘回调期间的走势
大盘跌，股价横盘： RS 线会迅速拉升。

"""