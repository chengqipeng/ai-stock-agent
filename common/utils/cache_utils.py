import json
import os
from datetime import datetime

def get_project_root():
    """获取项目根目录"""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_market_cache_key() -> str:
    """根据当前时间生成缓存 key 后缀：交易时段内返回精确到5分钟的时间戳，否则返回日期。
    交易时段：周一至周五 09:30–15:00
    """
    now = datetime.now()
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    if weekday < 5:  # 工作日
        t = now.time()
        from datetime import time
        if time(9, 30) <= t <= time(15, 0):
            # 精确到5分钟
            minute_block = (now.minute // 5) * 5
            return now.strftime(f"%Y%m%d_%H{minute_block:02d}")
    return now.strftime("%Y%m%d")

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
