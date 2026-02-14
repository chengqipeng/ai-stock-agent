import json
from datetime import datetime

from service.llm.deepseek_client import DeepSeekClient
from service.llm.volcengine_client import VolcengineClient
from service.stock_news.can_slim.stock_research_keywork_service import research_stock_news


async def get_search_result_filter_prompt(secucode="002371.SZ", stock_name = None, search_result = None):
    return f"""
# Role
你是一位资深的证券分析师与量化策略专家，擅长从海量碎片化信息中识别具有"股价驱动力"的核心事件。

当前日期：{datetime.now().strftime('%Y-%m-%d')}
公司名称：{stock_name}({secucode})

# Task
请对提供的网络搜索结果（Context）进行深度筛选。你的目标是识别出那些会对上市公司基本面产生**直接且重大**影响、且足以驱动股价当前或未来走势的核心信息。

# Selection Criteria (筛选准则)
必须选择符合以下维度之一的信息：
1. **资本运作：** 并购重组、定增融资、股权激励、大股东增减持、股权质押风险。
2. **业绩变动：** 财报数据超预期、业绩预增/预减告警、重大资产减值。
3. **经营动态：** 签下重大合同（占营收比重高）、核心高管变动、核心技术突破/专利获批。
4. **外部环境：** 行业重磅政策调整、重大法律诉讼、被监管机构立案调查。

# Negative Constraints (严格禁选)
- **禁止选择：** 实时股价波动、行情走势（如K线、涨跌幅）、技术指标。
- **禁止选择：** 常规产品功能介绍、规格参数、日常市场推广、用户评价或营销软文。
- **禁止选择：** 与当前目标公司无直接关联的行业宏观泛谈、竞品无关动态。

# 数据选择
- 需要保证消息的时效性（一个月以内）
- 相关性和时效性最高的排序在最前面
- 选择分数最高的前15条数据

# 网络搜索结果：
{json.dumps(search_result, ensure_ascii=False)}

# Output Format
只能返回符合上述要求的检索数据 ID 列表JSON数据，以标准 JSON 数组格式输出。禁止输出任何解释性文字。
[1, 2, 5]

"""


async def get_search_filter_result(secucode="002371.SZ", stock_name=None):
    """调用豆包大模型过滤搜索结果，返回符合条件的搜索信息列表"""
    search_result = await research_stock_news(secucode, stock_name)
    prompt = await get_search_result_filter_prompt(secucode, stock_name, search_result)
    print(prompt)
    print("\n\n")

    client = DeepSeekClient()

    model = "deepseek-chat"
    response = await client.chat(
        messages=[{"role": "user", "content": prompt}],
        model = model,
        temperature=0.3
    )
    content = response['choices'][0]['message']['content']
    
    try:
        filtered_ids = json.loads(content)
        if not isinstance(search_result, dict):
            return {"domestic_news": [], "global_news": []}
        
        filtered_result = {"domestic_news": [], "global_news": []}
        
        # 过滤并保留分组结构
        for news_type in ['domestic_news', 'global_news']:
            for news_group in search_result.get(news_type, []):
                filtered_results = [item for item in news_group.get('results', []) if item.get('id') in filtered_ids]
                if filtered_results:
                    filtered_result[news_type].append({
                        "intent": news_group.get("intent"),
                        "keyword": news_group.get("keyword"),
                        "results": filtered_results
                    })
        
        return filtered_result
    except (json.JSONDecodeError, KeyError):
        print((f"解析错误: {content}"))
        return {"domestic_news": [], "global_news": []}


if __name__ == "__main__":
    import asyncio
    
    async def main():
        result = await get_search_filter_result("002371.SZ", "北方华创")
        print(json.dumps(result, ensure_ascii=False, indent=2))

        print("\n ==================== \n")
    
    asyncio.run(main())
