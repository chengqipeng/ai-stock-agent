import logging
import re
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

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
            
            seen_names = set()
            seen_codes = set()
            for match in matches:
                stock_name = match[0].strip()
                stock_code = match[1].strip()
                score = int(match[2])
                if stock_name in seen_names or stock_code in seen_codes:
                    continue
                seen_names.add(stock_name)
                seen_codes.add(stock_code)
                stocks.append({
                    'name': stock_name,
                    'code': stock_code,
                    'score': score
                })
    except Exception as e:
        logger.error("解析股票列表失败: %s", e)
    
    return stocks


def update_stock_score(file_path: str, stock_name: str, stock_code: str, new_score: int) -> bool:
    """
    更新 markdown 文件中指定股票的打分
    优先按股票代码匹配，回退到股票名称匹配
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 按股票代码匹配（更精确）
        pattern = rf'(.+?)\s*\({re.escape(stock_code)}\)\s*-\s*打分[：:](\s*\d+)'
        new_content, count = re.subn(
            pattern,
            lambda m: f'{m.group(1)} ({stock_code}) - 打分：{new_score}',
            content
        )
        if count == 0:
            # 回退：按股票名称匹配
            pattern = rf'{re.escape(stock_name)}\s*\((\S+?)\)\s*-\s*打分[：:](\s*\d+)'
            new_content, count = re.subn(
                pattern,
                lambda m: f'{stock_name} ({m.group(1)}) - 打分：{new_score}',
                content
            )
        if count == 0:
            return False

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        return True
    except Exception as e:
        logger.error("更新股票打分失败: %s", e)
        return False
