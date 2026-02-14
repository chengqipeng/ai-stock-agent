import asyncio
from common.http.http_utils import EASTMONEY_PUSH2_API_URL, fetch_eastmoney_api
from common.utils.cache_utils import get_cache_path, load_cache, save_cache


async def get_all_market_indices(market_code="b:MK0010", page=1, page_size=50):
    """获取市场指数列表数据
    
    Args:
        market_code: 市场代码，默认为 b:MK0010
        page: 页码，默认为1
        page_size: 每页数量，默认为50
    
    Returns:
        dict: 包含市场指数列表的数据
    """
    cache_key = f"{market_code}_{page}_{page_size}".replace(":", "_")
    cache_path = get_cache_path("market_indices", cache_key)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    # 获取数据
    url = f"{EASTMONEY_PUSH2_API_URL}/clist/get"
    params = {
        "np": "1",
        "fltt": "1",
        "invt": "2",
        "fs": market_code,
        "fields": "f12,f13,f14,f1,f2,f4,f3,f152,f5,f6,f18,f17,f15,f16",
        "pn": str(page),
        "pz": str(page_size),
        "po": "1",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "dect": "1"
    }
    
    data = await fetch_eastmoney_api(
        url, 
        params, 
        referer="https://quote.eastmoney.com/center/hszs.html"
    )
    
    if data.get("data") and data["data"].get("diff"):
        result = data["data"]
        # 保存缓存
        save_cache(cache_path, result)
        return result
    else:
        raise Exception(f"未获取到市场 {market_code} 的指数数据")


async def get_market_indices_list(market_code="b:MK0010", page=1, page_size=50):
    """获取市场指数列表，返回简化格式
    
    Returns:
        list: [{"code": "000001.SH", "name": "上证指数"}, ...]
    """
    data = await get_all_market_indices(market_code, page, page_size)
    result = []
    
    for item in data.get("diff", []):
        code = f"{item['f12']}.{('SH' if item['f13'] == 1 else 'SZ')}"
        name = item['f14']
        result.append({"code": code, "name": name})
    
    return result


async def main():
    result = await get_market_indices_list()
    print("市场指数列表:")
    import json
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
