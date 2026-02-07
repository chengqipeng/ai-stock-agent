import asyncio
import json
import re
from asyncio import Semaphore
from datetime import datetime
from pathlib import Path

from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_report import get_stock_markdown_for_score
from service.llm.deepseek_client import DeepSeekClient


async def process_stock(stock, index, total, client, semaphore, lock, file_path):
    async with semaphore:
        stock_code = stock['code']
        stock_name = stock['name']
        main_stock_result = await get_stock_markdown_for_score(normalize_stock_code(stock_code), stock_name)
        
        messages = [{"role": "user", "content": main_stock_result}]
        response = await client.chat(messages)
        content = response.get('choices', [{}])[0].get('message', {}).get('content', '')
        
        try:
            json_match = re.search(r'```json\s*({.*?})\s*```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                result = json.loads(json_str)
            else:
                result = json.loads(content)
            
            score = result.get('score', '0')
            print(f"\n[{index}/{total}] {stock_name} ({stock_code}) - score:{score}")
            
            async with lock:
                with open(file_path, 'a', encoding='utf-8') as f:
                    f.write(f"{stock_name} ({stock_code}) - 分数: {score}\n")
        except Exception as e:
            print(f"\n[{index}/{total}] {stock_name} ({stock_code}) - 异常: {e}")


async def main():
    """
    遍历stocks_data.py中的股票清单，在循环中调用get_stock_markdown
    """
    client = DeepSeekClient()

    # 生成文件名
    now = datetime.now()
    file_name = f"stock_data_list_score_{now.strftime('%Y_%m_%d_%H')}.md"
    file_path = Path('stock_score_result') / file_name
    
    # 确保目录存在
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # 从文件读取股票代码和名称
    stocks = []
    with open('stock_score_file/stock_score_list.md', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                parts = line.split(' (')
                name = parts[0]
                code = parts[1].split(')')[0]
                stocks.append({'code': code, 'name': name})

    total = len(stocks)
    semaphore = Semaphore(5)
    lock = asyncio.Lock()
    
    tasks = [
        process_stock(stock, index, total, client, semaphore, lock, file_path)
        for index, stock in enumerate(stocks, 1)
    ]
    
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
