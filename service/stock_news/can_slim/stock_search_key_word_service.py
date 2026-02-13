import asyncio
import json
from service.stock_news.can_slim.stock_industry_service import get_industry_result
from service.llm.deepseek_client import DeepSeekClient


async def get_search_key_prompt(secucode="002371.SZ", stock_name = None):
    industry_data_str = await get_industry_result(secucode)
    industry_data = json.loads(industry_data_str)
    return f"""
# Role: 资深金融投资研究员 / 证券分析师

## Profile
你是一位拥有10年经验的资深金融投资研究员，擅长对上市公司进行深度的基本面分析、产业链调研以及竞争格局梳理。你精通各类搜索引擎及专业金融数据库的高级搜索技巧。

## Task
请根据我提供的【上市公司名称】和【所属行业】，生成一套用于深度尽职调查的高效、精准的搜索关键词组合。
**注意：你的回复必须是严格的 JSON 格式，不能包含任何额外的自然语言解释或 Markdown 格式（如 ```json 标签），以便于程序直接解析。**

## Input
- 公司名称：{stock_name}
- 所属行业：{industry_data['industry']}
- 主营业务描述: {industry_data['description']}

## Workflow
请依据以下业务逻辑，为我生成具体的搜索关键词：

1. 国内部分 (search_news)：
   - 创新护城河：创新产品发布、技术专利申请及行业壁垒。
   - 财务与预期：创新/新业务占总营收比例及未来业绩预期。
   - 公司治理：管理层变动、十大股东变动、增减持股变动明细。

2. 海外部分 (search_global_news)：
   - 竞争格局：北美排名前五的同类型/对标业务公司。
   - 供给端：同行业在北美地区的产能预测或建厂规划。
   - 需求端：同行业前十大核心客户的采购预期、资本开支指引。

## Output Format
请严格按照以下 JSON 结构输出，提供基础搜索词 (basic_search) 和带有高级搜索语法如 "" 或 filetype:pdf 的高级搜索词 (advanced_search)：

{{
  "search_news": [
    {{
      "intent": "创新产品发布及行业壁垒",
      "search_key": ["搜索词1", "搜索词2"],
      "advanced_search_key": ["高级搜索词1", "高级搜索词2"]
    }}
  ],
  "search_global_news": [
    {{
      "intent": "北美前五大同类型业务公司",
      "search_key": [],
      "advanced_search_key": []
    }},
  ]
}}

"""


async def get_search_key_result(secucode="002371.SZ", stock_name = None):
    prompt = await get_search_key_prompt(secucode, stock_name)
    client = DeepSeekClient()
    response = await client.chat(
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )
    return response['choices'][0]['message']['content']


if __name__ == "__main__":
    async def main():
        result = await get_search_key_result("002371.SZ", "北方华创")
        print(result)
    
    asyncio.run(main())