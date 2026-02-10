import re
from typing import List, Dict

def parse_stock_list(file_path: str) -> List[Dict]:
    """
    从markdown文件中解析股票列表
    格式: 股票名称 (股票代码) - 打分：分数
    例如: 北方华创 (002371.SZ) - 打分：85
    """
    stocks = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # 匹配格式: 股票名称 (股票代码) - 打分：分数
            pattern = r'(.+?)\s*\((\S+?)\)\s*-\s*打分[：:]\s*(\d+)'
            matches = re.findall(pattern, content)
            
            for match in matches:
                stock_name = match[0].strip()
                stock_code = match[1].strip()
                score = int(match[2])
                stocks.append({
                    'name': stock_name,
                    'code': stock_code,
                    'score': score
                })
    except Exception as e:
        print(f"解析股票列表失败: {e}")
    
    return stocks
