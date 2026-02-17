from common.http.http_utils import fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.utils.amount_utils import convert_amount_org_holder_1, convert_amount_org_holder_2


async def get_org_hold_by_sh_sz_hk_rank(stock_info: StockInfo, page_size: int = 4):
    """获取沪深港通持股排名数据"""
    cache_path = get_cache_path("mutual_stock_hold_rank", stock_info.stock_code)
    
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_MUTUAL_STOCK_HOLDRANKN_NEW",
        "columns": "ALL",
        "quoteColumns": "",
        "filter": f'(INTERVAL_TYPE="001")(SECUCODE="{stock_info.stock_code_normalize}")',
        "pageNumber": 1,
        "pageSize": page_size,
        "sortTypes": -1,
        "sortColumns": "TRADE_DATE",
        "source": "HSF10",
        "client": "PC"
    }
    
    data = await fetch_eastmoney_api(
        url, 
        params, 
        referer="https://emweb.securities.eastmoney.com/"
    )
    
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    else:
        return []


async def get_org_hold_by_sh_sz_hk_rank_cn(stock_info: StockInfo, fields=None, page_size: int = 4):
    """获取沪深港通持股排名数据并转换为中文键
    
    Args:
        stock_info: 股票信息
        fields: 可选字段列表（英文key），None表示返回所有字段
        page_size: 返回数据条数
    """
    data = await get_org_hold_by_sh_sz_hk_rank(stock_info, page_size)
    
    field_map = {
        "TRADE_DATE": "交易日期",
        "SECUCODE": "股票代码",
        "SECURITY_NAME": "股票名称",
        "HOLD_SHARES": "持股数量（万股）",
        "HOLD_MARKET_CAP": "持股市值（亿元）",
        "FREE_SHARES_RATIO": "占流通股比",
        "TOTAL_SHARES_RATIO": "占总股本比",
        "HOLD_SHARES_CHANGE": "增持数量（万股）",
        "ADD_MARKET_CAP": "增持市值（亿元）",
        "ADD_SHARES_AMP": "增持幅度",
        "CLOSE_PRICE": "收盘价",
        "CHANGE_RATE": "涨跌幅"
    }
    
    result = []
    for item in data:
        cn_item = {}
        for k, v in item.items():
            if k in field_map:
                if fields is None or k in fields:
                    cn_item[field_map[k]] = v
        
        if "交易日期" in cn_item and cn_item["交易日期"]:
            cn_item["交易日期"] = cn_item["交易日期"].split()[0]
        
        if "持股数量（万股）" in cn_item and cn_item["持股数量（万股）"] is not None:
            cn_item["持股数量（万股）"] = convert_amount_org_holder_1(cn_item["持股数量（万股）"])
        
        if "持股市值（亿元）" in cn_item and cn_item["持股市值（亿元）"] is not None:
            cn_item["持股市值（亿元）"] = convert_amount_org_holder_2(cn_item["持股市值（亿元）"])
        
        if "增持市值（亿元）" in cn_item and cn_item["增持市值（亿元）"] is not None:
            cn_item["增持市值（亿元）"] = convert_amount_org_holder_2(cn_item["增持市值（亿元）"])
        
        if "占流通股比" in cn_item and cn_item["占流通股比"] is not None:
            cn_item["占流通股比"] = str(round(cn_item["占流通股比"], 2)) + "%"
        
        if "占总股本比" in cn_item and cn_item["占总股本比"] is not None:
            cn_item["占总股本比"] = str(round(cn_item["占总股本比"], 2)) + "%"
        
        if "增持数量（万股）" in cn_item and cn_item["增持数量（万股）"] is not None:
            cn_item["增持数量（万股）"] = convert_amount_org_holder_1(cn_item["增持数量（万股）"])
        
        if "涨跌幅" in cn_item and cn_item["涨跌幅"] is not None:
            cn_item["涨跌幅"] = str(round(cn_item["涨跌幅"], 2)) + "%"
        
        if "增持幅度" in cn_item and cn_item["增持幅度"] is not None:
            cn_item["增持幅度"] = str(round(cn_item["增持幅度"], 2)) + "%"
        
        result.append(cn_item)
    
    return result


if __name__ == "__main__":
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name
    
    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        result = await get_org_hold_by_sh_sz_hk_rank_cn(stock_info)
        import json
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())
