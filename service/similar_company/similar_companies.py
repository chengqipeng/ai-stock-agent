import json
from service.llm.deepseek_client import DeepSeekClient
from service.prompt.stock_similar_companies_prompt import SIMILAR_COMPANIES_PROMPT

class SimilarCompaniesGenerator:
    def __init__(self):
        self.client = DeepSeekClient()
    
    async def generate(self, company_name: str, stock_code: str, company_num: int = 5) -> dict:
        """生成相似公司推荐"""
        prompt = SIMILAR_COMPANIES_PROMPT.format(
            company_name=company_name,
            stock_code=stock_code,
            company_num=company_num
        )
        
        messages = [{"role": "user", "content": prompt}]
        response = await self.client.chat(messages)
        
        content = response["choices"][0]["message"]["content"]
        # 提取JSON内容
        start = content.find("```json")
        end = content.find("```", start + 7)
        if start != -1 and end != -1:
            json_str = content[start + 7:end].strip()
            return json.loads(json_str)
        return json.loads(content)
