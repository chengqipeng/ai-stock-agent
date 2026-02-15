from common.http.http_utils import EASTMONEY_API_URL, fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from datetime import datetime, timedelta


async def get_stock_repurchase_data(stock_info: StockInfo, page_size: int = 50, page_number: int = 1):
    """获取股票回购数据
    
    Args:
        stock_info: 股票信息对象
        page_size: 每页数量，默认50
        page_number: 页码，默认1
    
    Returns:
        dict: 回购数据
    """
    cache_path = get_cache_path("repurchase", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    # 获取数据
    url = EASTMONEY_API_URL
    params = {
        "sortColumns": "UPD,DIM_DATE,DIM_SCODE",
        "sortTypes": "-1,-1,-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "reportName": "RPTA_WEB_GETHGLIST_NEW",
        "columns": "ALL",
        "source": "WEB",
        "filter": f'(DIM_SCODE="{stock_info.stock_code}")'
    }
    
    data = await fetch_eastmoney_api(url, params, referer=f"https://data.eastmoney.com/gphg/{stock_info.stock_code}.html")
    
    if data.get("result") and data["result"].get("data"):
        result = data["result"]
        # 保存缓存
        save_cache(cache_path, result)
        return result
    else:
        return {"data": [], "pages": 0, "count": 0}


async def get_stock_repurchase_json(stock_info: StockInfo, page_size: int = 50):
    """获取股票回购数据并转换为JSON格式（中文key）
    
    Args:
        stock_info: 股票信息对象
        page_size: 每页数量，默认50
    
    Returns:
        list: JSON格式的回购数据列表
    """
    result = await get_stock_repurchase_data(stock_info, page_size=page_size)
    
    if not result.get("data"):
        return []
    
    repurchase_list = []
    for item in result["data"]:
        repurchase_list.append({
            "公告日期": item.get('DIM_DATE', '--'),
            "股票代码": item.get('DIM_SCODE', '--'),
            "股票名称": item.get('SECURITYSHORTNAME', '--'),
            "回购进度": item.get('REPURPROGRESS', '--'),
            "回购目的": item.get('REPUROBJECTIVE', '--'),
            "最新价": item.get('NEWPRICE', '--'),
            "回购价格上限": item.get('REPURPRICECAP', '--'),
            "回购价格下限": item.get('REPURPRICELOWER', '--'),
            "已回购价格上限": item.get('REPURPRICECAP1', '--'),
            "已回购价格下限": item.get('REPURPRICELOWER1', '--'),
            "回购数量上限": item.get('REPURNUMCAP', '--'),
            "回购数量下限": item.get('REPURNUMLOWER', '--'),
            "占公告前一日总股本比例(%)": (f"{item.get('ZSZXX'):.2f}" if isinstance(item.get('ZSZXX'), (int, float)) else '--') + "~" + (f"{item.get('ZSZSX'):.2f}" if isinstance(item.get('ZSZSX'), (int, float)) else '--'),
            "回购金额上限": item.get('REPURAMOUNTLIMIT', '--'),
            "回购金额下限": item.get('REPURAMOUNTLOWER', '--'),

            "已回购数量": item.get('REPURNUM', '--'),
            "已回购金额": item.get('REPURAMOUNT', '--'),
            "平均回购价格": item.get('ZJJG', '--'),
            "回购开始日期": item.get('REPURSTARTDATE', '--'),
            "回购结束日期": item.get('REPURENDDATE', '--'),
            "备注": item.get('REMARK', '--')
        })
    
    return repurchase_list

async def get_stock_repurchase_recent_json(stock_info: StockInfo):
    """获取过去一年和未来一年的股票回购数据
    
    Args:
        stock_info: 股票信息对象
    
    Returns:
        list: 过去一年和未来一年的回购数据列表
    """
    all_data = await get_stock_repurchase_json(stock_info, page_size=200)
    
    if not all_data:
        return []
    
    now = datetime.now()
    one_year_ago = now - timedelta(days=365)
    one_year_later = now + timedelta(days=365)
    
    filtered_data = []
    for item in all_data:
        date_str = item.get("公告日期")
        if date_str and date_str != '--':
            try:
                # 处理日期格式 "2024-12-31 00:00:00"
                item_date = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
                if one_year_ago <= item_date <= one_year_later:
                    filtered_data.append(item)
            except (ValueError, IndexError):
                continue
    
    return filtered_data

if __name__ == "__main__":
    import asyncio
    import json
    
    async def main():
        stock_info = get_stock_info_by_name("三花智控")
        result = await get_stock_repurchase_json(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())
