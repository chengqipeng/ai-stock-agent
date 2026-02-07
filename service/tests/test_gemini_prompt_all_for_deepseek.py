import asyncio
import json
import re
from asyncio import Semaphore

from common.constants.stocks_data import STOCKS
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_report import get_stock_markdown, get_stock_markdown_all
from service.llm.deepseek_client import DeepSeekClient


async def process_stock(stock, index, total, client, good_stocks, semaphore):
    async with semaphore:
        stock_code = stock['code']
        stock_name = stock['name']
        main_stock_result = await get_stock_markdown_all(normalize_stock_code(stock_code), stock_name)
        
        messages = [{"role": "user", "content": main_stock_result}]
        response = await client.chat(messages)
        content = response.get('choices', [{}])[0].get('message', {}).get('content', '')
        
        is_good = '0'
        score = '0'
        reason = ''
        try:
            json_match = re.search(r'```json\s*({.*?})\s*```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                result = json.loads(json_str)
            else:
                result = json.loads(content)
            
            is_good = result.get('is_good', '0')
            score = result.get('score', '0')
            reason = result.get('reason', '')
            
            if is_good == '1' or is_good == 1:
                good_stocks.append({
                    'stock_name': stock_name,
                    'stock_code': stock_code,
                    'score': score,
                    'reason': reason,
                    'is_good': is_good
                })
                
                with open('good_stock_data_list.md', 'a', encoding='utf-8') as f:
                    f.write(f"## {stock_name} ({stock_code}) - 分数: {score}\n\n")

            print(f"\n[{index}/{total}] {stock_name} ({stock_code}) - score:{score} - is_good: {is_good} - reason: {reason}")
        except Exception as e:
            print(f"\n[{index}/{total}] {stock_name} ({stock_code}) - 异常: {e}")


async def main():
    """
    遍历stocks_data.py中的股票清单，在循环中调用get_stock_markdown
    """
    client = DeepSeekClient()
    good_stocks = []
    
    # 找到天士力的索引
    start_index = next((i for i, s in enumerate(STOCKS) if s['code'] == '688035.SH'), 0)
    filtered_stocks = STOCKS[start_index + 1:]  # 从天士力之后开始
    
    total = len(filtered_stocks)
    semaphore = Semaphore(5)
    
    tasks = [
        process_stock(stock, index, total, client, good_stocks, semaphore)
        for index, stock in enumerate(filtered_stocks, 1)
    ]
    
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
