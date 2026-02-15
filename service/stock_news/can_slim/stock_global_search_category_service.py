import asyncio
import json
from datetime import datetime

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.stock_news.can_slim.stock_industry_service import get_industry_result
from service.llm.volcengine_client import VolcengineClient


async def get_search_key_prompt(stock_info: StockInfo, industry_result):
    return f"""
# Role
你是一位半导体行业资深分析师，精通{industry_result['industry']}市场格局。

当前日期：{datetime.now().strftime('%Y-%m-%d')}

# Task
请调研并分析“{stock_info.stock_name} ({stock_info.stock_code_normalize})”在北美市场的竞争对手。

# Requirements
1. **对标公司选择**：筛选 5 家北美排名前五的同类型或业务高度对标的公司（主要集中在刻蚀、薄膜沉积、清洗、热处理等前道工艺设备）。
2. **客户背景调研**：针对上述每家对标公司，列出其全球范围内的前 10 大核心客户。
3. **数据准确性**：优先参考最新财报及半导体行业分析报告（如 Gartner 或 TechInsights 数据）。

# Output Format (JSON)
请严格按以下 JSON 结构返回数据，确保 "customer" 数组嵌套在对应的公司对象中，以体现所属关系：

{{
  "comparisons": [
    {{
      "company_name": "公司名称",
      "ticker": "股票代码",
      "similarity_score": "相似度评分 (0-100)",
      "top_10_customers": [
        {{"name": "客户名称", "industry_segment": "所属领域（如：Foundry/Memory）"}}
      ]
    }}
  ]
}}
"""

async def get_global_search_category_result(stock_info: StockInfo) -> tuple[list[str], list[str]]:
    """调用火山大模型并返回公司名称列表和客户名称列表"""
    industry_result_str = await get_industry_result(stock_info)
    industry_result = json.loads(industry_result_str)

    print(industry_result_str)

    if industry_result['is_science'] == 1 or industry_result['is_science'] == "1":
        prompt = await get_search_key_prompt(stock_info, industry_result)
        client = VolcengineClient()
        response = await client.chat(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        content = response.get("choices", [{}])[0].get("message", {}).get("content", "")

        data = json.loads(content)
        sorted_comparisons = sorted(data["comparisons"], key=lambda x: float(x["similarity_score"]), reverse=True)

        companies = list(dict.fromkeys(comp["company_name"] for comp in sorted_comparisons))
        customers = list(dict.fromkeys(cust["name"] for comp in sorted_comparisons for cust in comp["top_10_customers"]))[:10]

        return companies, customers
    else:
        return [], []


if __name__ == "__main__":
    async def main():
        stock_info: StockInfo = get_stock_info_by_name("北方华创")
        prompt = await get_global_search_category_result(stock_info)
        print(prompt)
    
    asyncio.run(main())