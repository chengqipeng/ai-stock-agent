from common.http.http_utils import fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.utils.amount_utils import convert_amount_org_holder


async def get_northbound_funds(stock_info: StockInfo, page_size: int = 4):
    """获取北向资金持股数据"""
    cache_path = get_cache_path("northbound_funds", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    # 获取数据
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
        "client": "PC",
        "v": "014543143615131127"
    }
    
    data = await fetch_eastmoney_api(
        url, 
        params, 
        referer="https://emweb.securities.eastmoney.com/"
    )
    
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        # 保存缓存
        save_cache(cache_path, result)
        return result
    else:
        raise Exception(f"未获取到股票 {stock_info.stock_name} 的北向资金持股数据")


async def get_northbound_funds_cn(stock_info: StockInfo, fields=None, page_size: int = 4):
    """获取北向资金持股数据并转换为中文键
    
    Args:
        stock_info: 股票信息
        fields: 可选字段列表（英文key），如 ['TRADE_DATE', 'HOLD_SHARES', 'HOLD_MARKET_CAP']，None表示返回所有字段
        page_size: 返回数据条数
    """
    data = await get_northbound_funds(stock_info, page_size)
    
    field_map = {
        "MUTUAL_TYPE": "互通类型",
        "TRADE_DATE": "交易日期",
        "SECUCODE": "股票代码",
        "SECURITY_CODE": "证券代码",
        "SECURITY_INNER_CODE": "证券内部代码",
        "SECURITY_NAME": "股票名称",
        "ADD_MARKET_CAP": "增持市值",
        "ADD_SHARES_AMP": "增持幅度",
        "HOLD_SHARES_CHANGE": "持股变化",
        "HOLDSHARES_CHANGE_FREERATIO": "持股变化占流通股比",
        "HOLDSHARES_CHANGE_TOTALRATIO": "持股变化占总股本比",
        "HOLD_MARKET_CAP": "持股市值",
        "HOLD_SHARES": "持股数量",
        "FREE_SHARES_RATIO": "占流通股比",
        "TOTAL_SHARES_RATIO": "占总股本比",
        "DATE_TYPE": "日期类型",
        "CLOSE_PRICE": "收盘价",
        "CHANGE_RATE": "涨跌幅",
        "REGION_CHANGE_RATE": "区间涨跌幅",
        "INDUSTRY": "行业",
        "BOARD_CODE": "板块代码",
        "INTERVAL_TYPE": "周期类型",
        "PARTICIPANT_NUM": "参与机构数",
        "SECUCODE_HQ": "行情代码",
        "BK_BOARD_NAME": "板块名称",
        "BK_BOARD_CODE": "板块代码",
        "CC_DATE": "统计日期"
    }
    
    result = []
    for item in data:
        if fields:
            cn_item = {field_map[k]: v for k, v in item.items() if k in fields}
        else:
            cn_item = {field_map.get(k, k): v for k, v in item.items()}
        
        # 格式化交易日期字段，只保留年月日
        if "交易日期" in cn_item and cn_item["交易日期"]:
            cn_item["交易日期"] = cn_item["交易日期"].split()[0]
        
        # 格式化增持幅度字段，保留两位小数
        if "增持幅度" in cn_item and cn_item["增持幅度"] is not None:
            cn_item["增持幅度"] = round(cn_item["增持幅度"], 2)
        
        # 格式化增持市值字段
        if "增持市值" in cn_item and cn_item["增持市值"] is not None:
            cn_item["增持市值"] = convert_amount_org_holder(cn_item["增持市值"])
        
        result.append(cn_item)
    
    return result


if __name__ == "__main__":
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name
    
    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        result = await get_northbound_funds_cn(stock_info, ['TRADE_DATE', 'ADD_MARKET_CAP','ADD_SHARES_AMP', 'ADD_SHARES_AMP'])
        print(result)
    
    asyncio.run(main())
