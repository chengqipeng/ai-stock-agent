import asyncio
import json
import re
from asyncio import Semaphore
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod

from common.utils.stock_info_utils import get_stock_info_by_name, StockInfo
from common.utils.llm_utils import parse_llm_json
from service.eastmoney.stock_structure_markdown import get_stock_markdown_for_score


class BaseStockProcessor(ABC):
    def __init__(self, model_name, concurrency=5):
        self.model_name = model_name
        self.concurrency = concurrency
    
    @abstractmethod
    def create_client(self):
        """创建LLM客户端"""
        pass
    
    async def process_stock(self, stock, index, total, client, semaphore, lock, file_path):
        async with semaphore:
            stock_code = stock['code']
            stock_name = stock['name']

            stock_info: StockInfo = get_stock_info_by_name(stock_name)
            main_stock_result = await get_stock_markdown_for_score(stock_info)
            
            messages = [{"role": "user", "content": main_stock_result}]

            try:
                response = await client.chat(messages)
                content = response.get('choices', [{}])[0].get('message', {}).get('content', '')
                result = parse_llm_json(content)
                
                score = result.get('score', '0')
                print(f"\n[{index}/{total}] {stock_name} ({stock_code}) - score:{score}")
                
                async with lock:
                    lines = []
                    if file_path.exists():
                        with open(file_path, 'r', encoding='utf-8') as f:
                            lines = f.readlines()
                    
                    new_line = f"{stock_name} ({stock_code}) - 分数: {score}\n"
                    lines = [line for line in lines if f"({stock_code})" not in line]
                    lines.append(new_line)
                    
                    def get_score(line):
                        match = re.search(r'分数: (\d+)', line)
                        return int(match.group(1)) if match else 0
                    
                    lines.sort(key=get_score, reverse=True)
                    
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.writelines(lines)
            except Exception as e:
                print(f"\n[{index}/{total}] {stock_name} ({stock_code}) - 异常: {e}")
    
    async def run(self):
        client = self.create_client()
        
        now = datetime.now()
        file_name = f"stock_data_list_score_{self.model_name}_{now.strftime('%Y_%m_%d_%H')}.md"
        file_path = Path('data_results/stock_score_list_result') / file_name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        stocks = []
        with open('data_results/stock_to_score_list/stock_score_list.md', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(' (')
                    name = parts[0]
                    code = parts[1].split(')')[0]
                    stocks.append({'code': code, 'name': name})
        
        total = len(stocks)
        semaphore = Semaphore(self.concurrency)
        lock = asyncio.Lock()
        
        tasks = [
            self.process_stock(stock, index, total, client, semaphore, lock, file_path)
            for index, stock in enumerate(stocks, 1)
        ]
        
        await asyncio.gather(*tasks)
