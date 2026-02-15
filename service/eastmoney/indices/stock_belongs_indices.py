import asyncio
from common.http.http_utils import EASTMONEY_PUSH2_API_URL, fetch_eastmoney_api
from common.utils.cache_utils import get_cache_path, load_cache, save_cache

INDICES_LIST = [
    {
        "indices_stock_name": "深证成指",
        "indices_stock_alias_name" : "深圳A股",
        "indices_stock_code": "399001.SZ",
        "fs": "m:0+t:6+f:!2,m:0+t:80+f:!2"
    },
    {
        "indices_stock_name": "上证指数",
        "indices_stock_alias_name": "上证A股",
        "indices_stock_code": "000001.SH",
        "fs": "m:1+t:2+f:!2,m:1+t:23+f:!2"
    },
    # {
    #     "indices_stock_name": "北证50",
    #     "indices_stock_alias_name": "北证A股",
    #     "indices_stock_code": "899050.SZ",
    #     "fs": "m:0+t:81+s:262144+f:!2"
    # },
    {
        "indices_stock_name": "创业板指",
        "indices_stock_alias_name": "创业板",
        "indices_stock_code": "399006.SZ",
         "fs": "m:0+t:80+f:!2"
    },
    {
        "indices_stock_name": "科创综指",
        "indices_stock_alias_name": "科创板",
        "indices_stock_code": "000680.SH",
        "fs": "m:1+t:23+f:!2"
    }
    # {
    #     "indices_stock_name": "Ｂ股指数",
    #     "indices_stock_alias_name": "B股",
    #     "indices_stock_code": "000003.SH",
    #     "fs": "m:0+t:7+f:!2,m:1+t:3+f:!2"
    # }
]

async def get_stock_list(fs="m:0+t:6+f:!2,m:0+t:80+f:!2", page=1, page_size=3000):
    """获取股票列表
    
    Args:
        fs: 筛选条件，默认为"m:0+t:6+f:!2,m:0+t:80+f:!2"（A股，排除ST）
        page: 页码，默认为1
        page_size: 每页数量，默认为20
    
    Returns:
        dict: 包含股票列表的数据
    """
    cache_key = "indices_" + fs.replace(":", "_").replace("+", "_").replace(",", "_").replace("!", "_")
    cache_path = get_cache_path("stock_list", cache_key)
    
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
        "fs": fs,
        "fields": "f12,f13,f14,f1,f2,f4,f3,f152,f5,f6,f7,f15,f18,f16,f17,f10,f8,f9,f23",
        "fid": "f3",
        "pn": str(page),
        "pz": str(page_size),
        "po": "1",
        "dect": "1",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b"
    }
    
    data = await fetch_eastmoney_api(
        url, 
        params, 
        referer="https://quote.eastmoney.com/center/gridlist.html"
    )
    
    if data.get("data") and data["data"].get("diff"):
        result = data["data"]
        # 保存缓存
        save_cache(cache_path, result)
        return result
    else:
        raise Exception("未获取到股票列表数据")

async def get_stock_list_simple(fs="m:0+t:6+f:!2,m:0+t:80+f:!2", page=1, page_size=20):
    """获取股票列表，返回简化格式
    
    Args:
        fs: 筛选条件，默认为"m:0+t:6+f:!2,m:0+t:80+f:!2"（A股，排除ST）
        page: 页码
        page_size: 每页数量
    
    Returns:
        list: [{
            "code": "002371",
            "market": "0",
            "name": "北方华创",
            "price": 123.45,
            "change_pct": 5.67,
            "change_amount": 6.78,
            ...
        }, ...]
    """
    data = await get_stock_list(fs, page, page_size)
    result = []
    
    for item in data.get("diff", []):
        result.append({
            "code": item.get("f12"),
            "market": item.get("f13"),
            "name": item.get("f14"),
            "price": item.get("f2"),
            "change_pct": item.get("f3"),
            "change_amount": item.get("f4"),
            "volume": item.get("f5"),
            "amount": item.get("f6"),
            "amplitude": item.get("f7"),
            "high": item.get("f15"),
            "low": item.get("f16"),
            "open": item.get("f17"),
            "prev_close": item.get("f18"),
            "turnover_rate": item.get("f8"),
            "pe_ratio": item.get("f9"),
            "volume_ratio": item.get("f10"),
            "market_cap": item.get("f23")
        })
    
    return result

async def fetch_all_indices_stocks():
    """遍历所有指数，获取成分股数据并去重"""
    import json
    all_stocks = {}
    
    for index in INDICES_LIST:
        print(f"正在获取 {index['indices_stock_name']} 的成分股...")
        try:
            page = 1
            while True:
                stocks = await get_stock_list_simple(fs=index['fs'], page=page, page_size=3000)
                if not stocks:
                    break
                
                for stock in stocks:
                    stock_key = f"{stock['code']}.{'SH' if stock['market'] == 1 else 'SZ'}"
                    if stock_key not in all_stocks:
                        all_stocks[stock_key] = {
                            "code": stock_key,
                            "name": stock['name'],
                            "indices_stock_codes": [index['indices_stock_code']],
                            "indices_stock_names": [index['indices_stock_name']]
                        }
                    else:
                        if index['indices_stock_code'] not in all_stocks[stock_key]['indices_stock_codes']:
                            all_stocks[stock_key]['indices_stock_codes'].append(index['indices_stock_code'])
                            all_stocks[stock_key]['indices_stock_names'].append(index['indices_stock_name'])
                            print(f"发现重复股票: {stock_key} ({stock['name']}) - 已存在于 {all_stocks[stock_key]['indices_stock_names'][0]}, 当前在 {index['indices_stock_name']}")
                
                if len(stocks) < 3000:
                    break
                page += 1
        except Exception as e:
            print(f"获取 {index['name']} 失败: {e}")
    
    # 保存到文件
    output_file = "indices_all_data.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(list(all_stocks.values()), f, ensure_ascii=False, indent=2)
    
    print(f"\n共获取 {len(all_stocks)} 只股票，已保存到 {output_file}")
    return list(all_stocks.values())


async def main():
    result = await fetch_all_indices_stocks()


if __name__ == "__main__":
    asyncio.run(main())
