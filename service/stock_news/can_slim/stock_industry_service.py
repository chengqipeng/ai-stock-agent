import asyncio
import json
from datetime import datetime

from service.eastmoney.stock_info.stock_busi_desc import get_stock_board_type
from service.llm.volcengine_client import VolcengineClient


async def get_industry_prompt(secucode="002371.SZ"):
    industry_data = await get_stock_board_type(secucode)
    return f"""
# Role
你是一位资深的证券分析师，擅长从复杂的上市公司非结构化或结构化数据中，精准提炼核心商业逻辑和行业分类。

当前日期：{datetime.now().strftime('%Y-%m-%d')}

# Task
请阅读并分析提供的原始 JSON 数据，按照以下要求提取该上市公司的“所属行业”与“产品描述”。

# Constraints
1. **所属行业 (industry)**：请根据数据内容，参照主流行业分类标准（如证监会行业分类、中信或申万行业分类）给出最准确的细分行业名称。
2. **产品描述 (description)**：
   - 必须涵盖公司的主导产品或核心业务。
   - 描述需专业且精炼，重点说明产品的应用领域或核心竞争力。
   - 字数严格控制在 50-120 字之间。
3. **输出格式**：必须严格遵守 JSON 格式规范，严禁包含任何多余的开场白或解释。

# Output Format
{{
   "industry": "<所属行业>",
   "description": "<高度精炼的产品及业务逻辑描述>"
}}

# Input Data
{json.dumps(industry_data, ensure_ascii=False)}
"""

async def get_industry_result(secucode="002371.SZ") -> str:
    """调用豆包大模型并返回content结果"""
    prompt = await get_industry_prompt(secucode)
    client = VolcengineClient()
    response = await client.chat(
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )
    return response.get("choices", [{}])[0].get("message", {}).get("content", "")


if __name__ == '__main__':
    result = asyncio.run(get_industry_result("002371.SZ"))
    print(result)