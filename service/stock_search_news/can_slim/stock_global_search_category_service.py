import asyncio
import json

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from common.utils.llm_utils import parse_llm_json
from common.prompt.search.global_search_category_prompt import get_global_search_category_prompt
from service.llm.deepseek_client import DeepSeekClient
from service.stock_search_news.can_slim.stock_industry_service import get_industry_result


async def get_global_search_category_result(stock_info: StockInfo) -> tuple[list[str], list[str]]:
    """调用火山大模型并返回公司名称列表和客户名称列表"""
    industry_result_str = await get_industry_result(stock_info)
    industry_result = parse_llm_json(industry_result_str)

    if industry_result['is_science'] == 1 or industry_result['is_science'] == "1":
        prompt = await get_global_search_category_prompt(stock_info, industry_result)
        client = DeepSeekClient()
        response = await client.chat(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        try:
            data = parse_llm_json(content)
        except (ValueError, json.JSONDecodeError):
            return [], []
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
