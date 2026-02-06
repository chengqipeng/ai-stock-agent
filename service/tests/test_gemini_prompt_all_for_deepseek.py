import asyncio
import json
import re

from common.constants.stocks_data import STOCKS
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_report import get_stock_markdown, get_stock_markdown_all
from service.llm.deepseek_client import DeepSeekClient


async def main():
    """
    遍历stocks_data.py中的股票清单，在循环中调用get_stock_markdown
    """
    client = DeepSeekClient()
    good_stocks = []
    total = len(STOCKS)
    
    for index, stock in enumerate(STOCKS, 1):
        stock_code = stock['code']
        stock_name = stock['name']
        main_stock_result = await get_stock_markdown_all(normalize_stock_code(stock_code), stock_name)
        
        messages = [{"role": "user", "content": main_stock_result}]
        response = await client.chat(messages)
        content = response.get('choices', [{}])[0].get('message', {}).get('content', '')
        
        # 解析响应判断is_good和score
        is_good = '0'
        try:
            json_match = re.search(r'```json\s*({.*?})\s*```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                result = json.loads(json_str)
            else:
                result = json.loads(content)
            
            is_good = result.get('is_good', '0')
            
            if is_good == '1' or is_good == 1:
                good_stocks.append({
                    'stock_name': stock_name,
                    'stock_code': stock_code,
                    'score': result.get('score', 0),
                    'reason': result.get('reason', ''),
                    'is_good': is_good
                })
                
                # 每次添加后排序并写入文件
                good_stocks.sort(key=lambda x: x['score'], reverse=True)
                
                with open('good_stock_data_list.md', 'w', encoding='utf-8') as f:
                    for s in good_stocks:
                        f.write(f"## {s['stock_name']} ({s['stock_code']}) - 分数: {s['score']}\n\n")
                        f.write(f"**原因**: {s['reason']}\n\n")
                        f.write("\n")
        except Exception as e:
            print(f"解析异常: {e}")
        
        print(f"\n[{index}/{total}] {stock_name} ({stock_code}) - is_good: {is_good}")


if __name__ == "__main__":
    asyncio.run(main())
