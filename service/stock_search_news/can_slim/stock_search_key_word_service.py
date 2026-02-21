import asyncio
import json
from datetime import datetime

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from common.utils.llm_utils import parse_llm_json
from service.llm.deepseek_client import DeepSeekClient
from service.stock_search_news.can_slim.stock_global_search_category_service import get_global_search_category_result
from service.stock_search_news.can_slim.stock_industry_service import get_industry_result


async def get_domestic_search_key_prompt(stock_info: StockInfo, search_intent= None, search_content = None):
    industry_data_str = await get_industry_result(stock_info)
    industry_data = parse_llm_json(industry_data_str)
    return f"""
# Role: 资深金融投资研究员 / 证券分析师

当前日期：{datetime.now().strftime('%Y-%m-%d')}

## Profile
你是一位拥有10年经验的资深金融投资研究员，擅长对上市公司进行深度的基本面分析、产业链调研以及竞争格局梳理。你精通各类搜索引擎及专业金融数据库的高级搜索技巧。

## Task
请根据我提供的【上市公司名称】和【所属行业】，生成一套用于深度尽职调查的高效、精准的搜索关键词组合。
**注意：你的回复必须是严格的 JSON 格式，不能包含任何额外的自然语言解释或 Markdown 格式（如 ```json 标签），以便于程序直接解析。**

## Input
- 公司名称：{stock_info.stock_name}
- 所属行业：{industry_data['industry']}
- 主营业务描述: {industry_data['description']}

## Workflow
请针对给定的目标公司，依据以下业务逻辑，为我生成具体的搜索关键词：
1. {search_intent}
 - {search_content}
 
2. 搜索关键词的构建原则：
   - 关键词应具备高度的针对性和专业性
   - 避免使用过于宽泛或模糊的词汇
   - 每个关键词都应能直接关联到公司的核心竞争力或行业地位
   - 每个关键词不要直接带时间
   - search_key_time_range不能超过30，需要根据关键词对应的特性判断对股票影响的时效性
   
## Output Format
请严格按照以下 JSON 结构输出，提供精准的行业属性和公司属性的搜索关键词，最多返回2个最优的关键词：

{{
  "search_news": ["搜索词1", "搜索词2"],
  "search_key_time_range": <搜索数据的时间范围，具体数字，禁止直接返回‘90天’这类格式>
}}

"""


async def get_global_search_key_prompt(stock_info: StockInfo, search_intent=None, search_content=None):
    return f"""
# Role: 资深金融投资研究员 / 证券分析师

当前日期：{datetime.now().strftime('%Y-%m-%d')}

## Profile
你是一位拥有10年经验的资深金融投资研究员，擅长对上市公司进行深度的基本面分析、产业链调研以及竞争格局梳理。你精通各类搜索引擎及专业金融数据库的高级搜索技巧。

## Task
请根据我提供的信息，生成一套用于深度尽职调查的高效、精准的搜索关键词组合。
**注意：你的回复必须是英文且严格的 JSON 格式，不能包含任何额外的自然语言解释或 Markdown 格式（如 ```json 标签），以便于程序直接解析。**

## Workflow
请针对给定的目标信息，依据以下描述，为我生成具体的搜索关键词：
1. {search_intent}
 - {search_content}

2. 搜索关键词的构建原则：
   - 关键词应具备高度的针对性和专业性
   - 避免使用过于宽泛或模糊的词汇
   - 每个关键词都应能直接关联到公司的核心竞争力或行业地位
   - 每个关键词不要直接带时间
   - search_key_time_range不能超过30，需要根据关键词对应的特性判断对股票影响的时效性

## Output Format
请严格按照以下 JSON 结构输出，提供精准的行业属性和公司属性的搜索关键词，最多返回5个最优的关键词：

{{
  "search_news": ["英文搜索词1", "英文搜索词2"],
  "search_key_time_range": <搜索数据的时间范围，具体数字，禁止直接返回‘90天’这类格式>
}}

"""


SEARCH_CATEGORY = [
    {
        "category": "announcements",
        "intent": "创新护城河",
        "type": "domestic",
        "search_content" : "创新产品发布、技术专利申请及行业壁垒"
    },
    {
        "category": "finance_and_expectations",
        "intent": "财务与预期",
        "type": "domestic",
        "search_content" : "创新/新业务占总营收比例及未来业绩预期"
    },
    {
        "category": "corporate_governance",
        "intent": "公司治理",
        "type": "domestic",
        "search_content" : "管理层变动、十大股东变动、重大投资"
    },
    {
        "category": "stock_incentive_plan",
        "intent": "股权激励",
        "type": "domestic",
        "search_content": "股权激励计划"
    },
    {
        "category": "dividend_policy",
        "intent": "政策红利",
        "type": "domestic",
        "search_content" : "国产替代、业绩预告、产能扩张及大基金动向"
    }
]

async def get_search_key_result_single(stock_info: StockInfo, category_info):
    """单个类别的搜索关键词获取"""
    if category_info['type'] == 'global':
        prompt = await get_global_search_key_prompt(
            stock_info,
            search_intent=category_info['intent'],
            search_content=category_info['search_content']
        )
    else:
        prompt = await get_domestic_search_key_prompt(
            stock_info,
            search_intent=category_info['intent'],
            search_content=category_info['search_content']
        )
    client = DeepSeekClient()
    response = await client.chat(
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        model="deepseek-chat"
    )
    content = response['choices'][0]['message']['content']
    try:
        data = parse_llm_json(content)
        return {
            "category": category_info['category'],
            "intent": category_info['intent'],
            "type": category_info['type'],
            "search_keys": data.get('search_news', []),
            "search_key_time_range": data.get('search_key_time_range', 30)
        }
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError parsing search key result: {e}, content: {content[:100]}")
        return {
            "category": category_info['category'],
            "intent": category_info['intent'],
            "type": category_info['type'],
            "search_keys": [],
            "search_key_time_range": 30
        }

async def get_search_key_result(stock_info: StockInfo):
    """并发获取所有类别的搜索关键词，限制5个并发"""
    semaphore = asyncio.Semaphore(5)

    ALL_SEARCH_CATEGORY = await get_merged_search_categories(stock_info)
    
    async def limited_task(category):
        async with semaphore:
            return await get_search_key_result_single(stock_info, category)
    
    tasks = [limited_task(category) for category in ALL_SEARCH_CATEGORY]
    results = await asyncio.gather(*tasks)
    return list(results)

async def get_merged_search_categories(stock_info: StockInfo):
    """合并SEARCH_CATEGORY和全球搜索结果"""
    companies, customers = await get_global_search_category_result(stock_info)

    if customers and companies:
        merged_categories = SEARCH_CATEGORY.copy()
        merged_categories.extend([
            {
                "category": "global_competitors",
                "intent": "全球竞争对手",
                "type": "global",
                "search_content": "、".join(companies) + " 未来产能预测或建厂规划"
            },
            {
                "category": "global_customers",
                "intent": "核心客户",
                "type": "global",
                "search_content": "、 ".join(customers) + " 未来采购预期、资本开支指引"
            },
            {
                "category": "geopolitics",
                "intent": "地缘政治",
                "type": "global",
                "search_content" : "聚焦出口管制、BIS禁令、美联储宏观政策及全球竞争对手（如 AMAT, Lam Research）的对比"
            }
        ])
    
        return merged_categories
    else:
        return SEARCH_CATEGORY


if __name__ == "__main__":
    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        result = await get_search_key_result(stock_info)
        print(result)
    
    asyncio.run(main())