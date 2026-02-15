import asyncio

import requests
from typing import Optional, List
from datetime import datetime, timedelta
import json

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name


async def get_stock_lock_up_period(stock_info: StockInfo, page_size: int = 500, page_number: int = 1) -> Optional[dict]:
    """
    获取股票限售解禁数据
    
    Args:
        stock_info: 股票代码，如 "002050"
        page_size: 每页数量，默认500
        page_number: 页码，默认1
        
    Returns:
        解禁数据字典，失败返回None
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    
    params = {
        "sortColumns": "FREE_DATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "reportName": "RPT_LIFT_STAGE",
        "filter": f'(SECURITY_CODE="{stock_info.stock_code}")',
        "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,FREE_DATE,CURRENT_FREE_SHARES,ABLE_FREE_SHARES,LIFT_MARKET_CAP,FREE_RATIO,NEW,B20_ADJCHRATE,A20_ADJCHRATE,FREE_SHARES_TYPE,TOTAL_RATIO,NON_FREE_SHARES,BATCH_HOLDER_NUM",
        "source": "WEB",
        "client": "WEB"
    }
    
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": f"https://data.eastmoney.com/dxf/q/{stock_info.stock_code}.html",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"获取限售解禁数据失败: {e}")
        return None


async def get_stock_lock_up_period_year_range(stock_info: StockInfo) -> Optional[List[dict]]:
    """
    获取过去一年和未来一年的解禁数据
    
    Args:
        stock_code: 股票代码，如 "002050"
        
    Returns:
        解禁数据列表（中文key），失败返回None
    """
    data = await get_stock_lock_up_period(stock_info)
    if not data or not data.get("result") or not data["result"].get("data"):
        return None
    
    now = datetime.now()
    one_year_ago = now - timedelta(days=365)
    one_year_later = now + timedelta(days=365)
    
    result = []
    for item in data["result"]["data"]:
        free_date_str = item.get("FREE_DATE")
        if not free_date_str:
            continue
        
        free_date = datetime.strptime(free_date_str.split(" ")[0], "%Y-%m-%d")
        if one_year_ago <= free_date <= one_year_later:
            result.append({
                "股票代码": item.get("SECURITY_CODE"),
                "股票简称": item.get("SECURITY_NAME_ABBR"),
                "解禁日期": item.get("FREE_DATE"),
                "实际解禁数量": item.get("CURRENT_FREE_SHARES"),
                "可上市流通股份": item.get("ABLE_FREE_SHARES"),
                "解禁市值": item.get("LIFT_MARKET_CAP"),
                "占解禁前流通市值比例": item.get("FREE_RATIO"),
                "解禁类型": item.get("FREE_SHARES_TYPE"),
                "占总股本比例": item.get("TOTAL_RATIO"),
                "限售股份": item.get("NON_FREE_SHARES"),
                "股东户数": item.get("BATCH_HOLDER_NUM"),
                "解禁前20日涨跌幅": item.get("B20_ADJCHRATE"),
                "解禁后20日涨跌幅": item.get("A20_ADJCHRATE")
            })
    
    return result if result else None


if __name__ == "__main__":
    async def main():
        stock_name = "三花智控"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)

        result = await get_stock_lock_up_period_year_range(stock_info)
        if result:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print("未查询到数据")

    asyncio.run(main())

