import json

from service.eastmoney.stock_info.stock_holder_data import get_shareholder_increase_json
from service.eastmoney.stock_info.stock_revenue_analysis import get_revenue_analysis_three_years
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
  具体数据：
    股价走势图（日线/周线）。
    相对强度（RS）线（这一点至关重要）。
    成交量数据。
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

"""