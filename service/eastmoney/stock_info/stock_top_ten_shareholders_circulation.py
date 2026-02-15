from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.http.http_utils import EASTMONEY_DATA_API_URL, fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from common.utils.amount_utils import convert_amount_unit


async def get_top_ten_shareholders_circulation(stock_info: StockInfo, end_date: str = None):
    """获取流通股前十大股东数据
    
    Args:
        stock_info: 股票信息对象
        end_date: 报告期，格式：YYYY-MM-DD，如 '2025-09-30'
    
    Returns:
        list: 流通股前十大股东数据列表
    """
    cache_key = f"top_ten_shareholders_circulation_{end_date}" if end_date else "top_ten_shareholders_circulation"
    cache_path = get_cache_path(cache_key, stock_info.stock_code)
    
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    filter_str = f'(SECUCODE="{stock_info.stock_code_normalize}")'
    if end_date:
        filter_str += f'(END_DATE=\'{end_date}\')'
    
    params = {
        "reportName": "RPT_F10_EH_FREEHOLDERS",
        "columns": "SECUCODE,SECURITY_CODE,END_DATE,HOLDER_RANK,HOLDER_NEW,HOLDER_NAME,HOLDER_TYPE,SHARES_TYPE,HOLD_NUM,FREE_HOLDNUM_RATIO,HOLD_NUM_CHANGE,CHANGE_RATIO",
        "quoteColumns": "",
        "filter": filter_str,
        "pageNumber": "1",
        "pageSize": "",
        "sortTypes": "1",
        "sortColumns": "HOLDER_RANK",
        "source": "HSF10",
        "client": "PC"
    }
    
    data = await fetch_eastmoney_api(
        EASTMONEY_DATA_API_URL, 
        params,
        referer="https://emweb.securities.eastmoney.com/"
    )
    
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    return []


async def get_top_ten_shareholders_circulation_markdown(stock_info: StockInfo, end_date: str = None):
    """获取流通股前十大股东数据并转换为markdown"""
    items = await get_top_ten_shareholders_circulation(stock_info, end_date)
    if not items:
        return ""
    
    report_date = items[0].get('END_DATE', '')[:10] if items else end_date
    header = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - 流通股前十大股东 ({report_date})"
    markdown = f"""{header}
| 排名 | 股东名称 | 股东类型 | 持股数量(股) | 占流通股比例 | 持股变化(万股) | 变化比例 |
|-----|---------|---------|--------------|------------|--------------|--------|
"""
    for item in items:
        rank = item.get('HOLDER_RANK', '--')
        holder_name = item.get('HOLDER_NAME', '--')
        holder_type = item.get('HOLDER_TYPE', '--')
        hold_num = item.get('HOLD_NUM')
        free_ratio = f"{round(item.get('FREE_HOLDNUM_RATIO', 0), 2)}%" if item.get('FREE_HOLDNUM_RATIO') else '--'
        hold_change = item.get('HOLD_NUM_CHANGE')
        change_ratio = f"{round(item.get('CHANGE_RATIO', 0), 2)}%" if item.get('CHANGE_RATIO') else '--'
        markdown += f"| {rank} | {holder_name} | {holder_type} | {hold_num} | {free_ratio} | {hold_change} | {change_ratio} |\n"
    return markdown + "\n"


async def get_top_ten_shareholders_circulation_json(stock_info: StockInfo, end_date: str = None):
    """获取流通股前十大股东数据并转换为JSON格式"""
    items = await get_top_ten_shareholders_circulation(stock_info, end_date)
    if not items:
        return []
    
    result = []
    for item in items:
        result.append({
            "排名": item.get('HOLDER_RANK', '--'),
            "股东名称": item.get('HOLDER_NAME', '--'),
            "股东类型": item.get('HOLDER_TYPE', '--'),
            "持股数量(股)": item.get('HOLD_NUM'),
            "占流通股比例": f"{round(item.get('FREE_HOLDNUM_RATIO', 0), 2)}%" if item.get('FREE_HOLDNUM_RATIO') else '--',
            "持股变化(股)": item.get('HOLD_NUM_CHANGE'),
            "变化比例": f"{round(item.get('CHANGE_RATIO', 0), 2)}%" if item.get('CHANGE_RATIO') else '--',
            "报告期": item.get('END_DATE', '')[:10] if item.get('END_DATE') else '--'
        })
    return result


async def get_org_hold_report_dates(stock_info: StockInfo, org_type: str = "00", page_size: int = 5):
    """获取机构持股报告期列表
    
    Args:
        stock_info: 股票信息对象
        org_type: 机构类型，00-全部机构
        page_size: 返回记录数
    
    Returns:
        list: 报告期列表
    """
    cache_key = f"org_hold_dates_{org_type}_{page_size}"
    cache_path = get_cache_path(cache_key, stock_info.stock_code)
    
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    params = {
        "reportName": "RPT_F10_MAIN_ORGHOLDDETAILS",
        "columns": "SECUCODE,REPORT_DATE",
        "quoteColumns": "",
        "filter": f'(SECUCODE="{stock_info.stock_code_normalize}")(ORG_TYPE="{org_type}")',
        "pageNumber": "1",
        "pageSize": str(page_size),
        "sortTypes": "-1",
        "sortColumns": "REPORT_DATE",
        "source": "HSF10",
        "client": "PC"
    }
    
    data = await fetch_eastmoney_api(
        EASTMONEY_DATA_API_URL,
        params,
        referer="https://emweb.securities.eastmoney.com/"
    )
    
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        # 转换 REPORT_DATE 格式：从 "YYYY-MM-DD HH:MM:SS" 转换为 "YYYY-MM-DD"
        for item in result:
            if item.get('REPORT_DATE'):
                item['REPORT_DATE'] = item['REPORT_DATE'][:10]
        save_cache(cache_path, result)
        return result
    return []


async def get_top_ten_shareholders_circulation_by_dates(stock_info: StockInfo, org_type: str = "00", page_size: int = 1, limit: int = 3):
    """获取多个报告期的流通股前十大股东数据
    
    Args:
        stock_info: 股票信息对象
        org_type: 机构类型，00-全部机构
        page_size: 返回报告期数量
        limit: 限制遍历的报告期数量，如传入1则只遍历一次
    
    Returns:
        list: JSON格式的股东数据列表
    """
    dates_data = await get_org_hold_report_dates(stock_info, org_type, page_size)
    if not dates_data:
        return []
    
    result = []
    for idx, item in enumerate(dates_data):
        if limit and idx >= limit:
            break
        report_date = item.get('REPORT_DATE', '')
        if report_date:
            shareholders = await get_top_ten_shareholders_circulation_json(stock_info, report_date)
            result.extend(shareholders)
    
    return result


if __name__ == "__main__":
    import asyncio
    import json
    
    async def main():
        stock_name = "中际旭创"
        stock_info = get_stock_info_by_name(stock_name)
        
        # 测试 Markdown 格式
        # markdown = await get_top_ten_shareholders_circulation_markdown(stock_info, "2024-09-30")
        # print("流通股前十大股东 (Markdown格式):")
        # print(markdown)
        #
        # # 测试 JSON 格式
        # result = await get_top_ten_shareholders_circulation_json(stock_info, "2024-09-30")
        # print("\n流通股前十大股东 (JSON格式):")
        # print(json.dumps(result, ensure_ascii=False, indent=2))
        #
        # # 测试机构持股报告期
        # dates = await get_org_hold_report_dates(stock_info)
        # print("\n机构持股报告期列表:")
        # print(json.dumps(dates, ensure_ascii=False, indent=2))
        
        # 测试多个报告期的股东数据
        multi_dates = await get_top_ten_shareholders_circulation_by_dates(stock_info, page_size=3, limit=3)
        print("\n多个报告期的流通股前十大股东:")
        print(json.dumps(multi_dates[:10], ensure_ascii=False, indent=2))
    
    asyncio.run(main())
