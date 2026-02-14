import asyncio
from common.http.http_utils import fetch_eastmoney_api
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name

async def get_stock_board_type(stock_info: StockInfo):
    """获取股票板块类型"""
    cache_path = get_cache_path("board_type", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    # 获取数据
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_F10_CORETHEME_BOARDTYPE",
        "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,NEW_BOARD_CODE,BOARD_NAME,SELECTED_BOARD_REASON,IS_PRECISE,BOARD_RANK,BOARD_YIELD,DERIVE_BOARD_CODE",
        "quoteColumns": "f3~05~NEW_BOARD_CODE~BOARD_YIELD",
        "filter": f'(SECUCODE="{stock_info.stock_code_normalize}")(IS_PRECISE="1")',
        "pageNumber": "1",
        "pageSize": "",
        "sortTypes": "1",
        "sortColumns": "BOARD_RANK",
        "source": "HSF10",
        "client": "PC",
        "v": "0732758307065009"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://emweb.securities.eastmoney.com/")
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        
        # 保存缓存
        save_cache(cache_path, result)
        
        return result
    else:
        raise Exception(f"未获取到股票 {stock_info.stock_code_normalize} 的板块类型数据")


if __name__ == "__main__":
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    result = asyncio.run(get_stock_board_type(stock_info))
    print(result)
