import json
import os
from datetime import datetime

def get_project_root():
    """获取项目根目录"""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_cache_path(business_type, stock_code):
    """获取缓存文件路径
    
    Args:
        business_type: 业务类型（如 fund_flow, kline）
        stock_code: 股票编码
    
    Returns:
        缓存文件的完整路径
    """
    today = datetime.now().strftime("%Y%m%d")
    cache_dir = os.path.join(get_project_root(), "tmp_data")
    return os.path.join(cache_dir, f"{business_type}-{today}-{stock_code}.json")

def load_cache(cache_path):
    """从缓存文件加载数据
    
    Args:
        cache_path: 缓存文件路径
    
    Returns:
        缓存的数据，如果不存在返回 None
    """
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def save_cache(cache_path, data):
    """保存数据到缓存文件
    
    Args:
        cache_path: 缓存文件路径
        data: 要缓存的数据
    """
    cache_dir = os.path.dirname(cache_path)
    os.makedirs(cache_dir, exist_ok=True)
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
