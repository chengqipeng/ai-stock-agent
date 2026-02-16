from common.utils.amount_utils import convert_amount_unit, convert_amount_org_holder, convert_amount_org_holder_1, \
    convert_amount_org_holder_2
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.http.http_utils import EASTMONEY_API_URL, fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name


async def get_org_holder(stock_info: StockInfo, page_size=8, page_number=1):
    """获取机构持仓数据"""
    cache_path = get_cache_path("org_holder", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    params = {
        "reportName": "RPT_MAIN_ORGHOLD",
        "columns": "ALL",
        "quoteColumns": "",
        "filter": f"(SECURITY_CODE=\"{stock_info.stock_code}\")",
        "pageNumber": str(page_number),
        "pageSize": str(page_size),
        "sortTypes": "",
        "sortColumns": "",
        "source": "WEB",
        "client": "WEB"
    }
    
    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    else:
        return []


async def get_shareholder_increase(stock_info: StockInfo, page_size=300, page_number=1):
    """获取股东增持数据"""
    cache_path = get_cache_path("shareholder_increase", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    params = {
        "sortColumns": "END_DATE,SECURITY_CODE,EITIME",
        "sortTypes": "-1,-1,-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "reportName": "RPT_SHARE_HOLDER_INCREASE",
        "quoteColumns": "f2~01~SECURITY_CODE~NEWEST_PRICE,f3~01~SECURITY_CODE~CHANGE_RATE_QUOTES",
        "quoteType": "0",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(SECURITY_CODE=\"{stock_info.stock_code}\")"
    }
    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    else:
        return None


async def get_holder_detail(stock_info: StockInfo, report_date=None, page_num=1, page_size=100, sh_type="", sh_code="", sort_field="HOLDER_CODE", sort_direc=1):
    """获取股票主力持仓明细"""
    from datetime import datetime
    if report_date is None:
        report_date = datetime.now().strftime("%Y-%m-%d")
    
    cache_path = get_cache_path("holder_detail", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    url = "https://data.eastmoney.com/dataapi/zlsj/detail"
    params = {
        "SHType": sh_type,
        "SHCode": sh_code,
        "SCode": stock_info.stock_code,
        "ReportDate": report_date,
        "sortField": sort_field,
        "sortDirec": sort_direc,
        "pageNum": page_num,
        "pageSize": page_size
    }
    data = await fetch_eastmoney_api(url, params)
    if data and data.get('data'):
        result = data['data']
        save_cache(cache_path, result)
        return result
    else:
        return []


async def get_shareholder_increase_markdown(stock_info: StockInfo, page_size=20):
    """获取股东增减持明细并转换为markdown"""
    items = await get_shareholder_increase(stock_info, page_size)
    if not items:
        return ""
    header = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - 股东增减持明细"
    markdown = f"{header}\n\n"
    markdown += "| 股东名称 | 增减 | 变动数量(万股) | 占总股本比例 | 占流通股比例 | 持股总数(万股) | 占总股本比例 | 持流通股数(万股) | 占流通股比例 | 变动开始日 | 变动截止日 | 公告日 |\n"
    markdown += "|---------|------|--------------|------------|------------|--------------|------------|----------------|------------|----------|----------|--------|\n"
    for item in items[:page_size]:
        holder_name = item.get('HOLDER_NAME', '--')
        direction = item.get('DIRECTION', '--')
        change_num = convert_amount_unit((item.get('CHANGE_NUM') or 0) * 10000)
        change_rate = f"{round(item.get('AFTER_CHANGE_RATE', 0), 2)}%" if item.get('AFTER_CHANGE_RATE') else '--'
        change_free_ratio = f"{round(item.get('CHANGE_FREE_RATIO', 0), 2)}%" if item.get('CHANGE_FREE_RATIO') else '--'
        after_holder_num = convert_amount_unit((item.get('AFTER_HOLDER_NUM') or 0) * 10000)
        hold_ratio = f"{round(item.get('HOLD_RATIO', 0), 2)}%" if item.get('HOLD_RATIO') else '--'
        free_shares = convert_amount_unit((item.get('FREE_SHARES') or 0) * 10000)
        free_shares_ratio = f"{round(item.get('FREE_SHARES_RATIO', 0), 2)}%" if item.get('FREE_SHARES_RATIO') else '--'
        start_date = item.get('START_DATE', '--')[:10] if item.get('START_DATE') else '--'
        end_date = item.get('END_DATE', '--')[:10] if item.get('END_DATE') else '--'
        notice_date = item.get('NOTICE_DATE', '--')[:10] if item.get('NOTICE_DATE') else '--'
        markdown += f"| {holder_name} | {direction} | {change_num} | {change_rate} | {change_free_ratio} | {after_holder_num} | {hold_ratio} | {free_shares} | {free_shares_ratio} | {start_date} | {end_date} | {notice_date} |"
    return markdown + "\n"


async def get_shareholder_increase_json(stock_info: StockInfo, page_size=20):
    """获取股东增减持明细并转换为JSON格式"""
    from datetime import datetime, timedelta

    items = await get_shareholder_increase(stock_info, page_size)
    if not items:
        return []
    
    # 计算一年前的日期
    one_year_ago = datetime.now() - timedelta(days=365)
    
    result = []
    for item in items[:page_size]:
        notice_date_str = item.get('NOTICE_DATE', '')
        if notice_date_str:
            notice_date = datetime.strptime(notice_date_str[:10], '%Y-%m-%d')
            # 过滤近一年的数据
            if notice_date < one_year_ago:
                continue
        
        result.append({
            "股东名称": item.get('HOLDER_NAME', '--'),
            "增减": item.get('DIRECTION', '--'),
            "变动数量(万股)": convert_amount_unit((item.get('CHANGE_NUM') or 0) * 10000),
            "占总股本比例": f"{round(item.get('AFTER_CHANGE_RATE', 0), 2)}%" if item.get('AFTER_CHANGE_RATE') else '--',
            "占流通股比例": f"{round(item.get('CHANGE_FREE_RATIO', 0), 2)}%" if item.get('CHANGE_FREE_RATIO') else '--',
            "持股总数(万股)": convert_amount_unit((item.get('AFTER_HOLDER_NUM') or 0) * 10000),
            "持股占总股本比例": f"{round(item.get('HOLD_RATIO', 0), 2)}%" if item.get('HOLD_RATIO') else '--',
            "持流通股数(万股)": convert_amount_unit((item.get('FREE_SHARES') or 0) * 10000),
            "持股占流通股比例": f"{round(item.get('FREE_SHARES_RATIO', 0), 2)}%" if item.get('FREE_SHARES_RATIO') else '--',
            "变动开始日": item.get('START_DATE', '--')[:10] if item.get('START_DATE') else '--',
            "变动截止日": item.get('END_DATE', '--')[:10] if item.get('END_DATE') else '--',
            "公告日": notice_date_str[:10] if notice_date_str else '--'
        })
    return result


async def get_holder_detail_markdown(stock_info: StockInfo, report_date=None, page_size=100):
    """获取股票主力持仓明细并转换为markdown"""
    from datetime import datetime
    if report_date is None:
        report_date = datetime.now().strftime("%Y-%m-%d")
    data = await get_holder_detail(stock_info, report_date, page_size=page_size)
    if not data:
        return ""
    markdown = f"## 主力持仓明细 (报告日期: {report_date})\n\n"
    markdown += "| 序号 | 机构名称 | 机构属性 | 持股总数(万股) | 持股市值(亿元) | 占总股本比例(%) | 占流通股本比例(%) |\n"
    markdown += "|------|---------|---------|--------------|--------------|----------------|-----------------|\n"
    for idx, item in enumerate(data, 1):
        holder_name = item.get('HOLDER_NAME', '--')
        org_type = item.get('ORG_TYPE', '--')
        total_shares = convert_amount_unit(item.get('TOTAL_SHARES', 0))
        market_cap = convert_amount_unit(item.get('HOLD_MARKET_CAP', 0))
        total_ratio = round(item.get('TOTAL_SHARES_RATIO', 0), 2)
        free_ratio = round(item.get('FREE_SHARES_RATIO', 0), 2)
        markdown += f"| {idx} | {holder_name} | {org_type} | {total_shares} | {market_cap} | {total_ratio} | {free_ratio} |\n"
    return markdown + "\n"


async def get_org_holder_markdown(stock_info: StockInfo, page_size=8):
    """获取机构持仓明细并转换为markdown"""
    holder_data = await get_org_holder(stock_info, page_size)
    if not holder_data:
        return ""
    from collections import defaultdict
    grouped_data = defaultdict(list)
    for item in holder_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        grouped_data[report_date].append(item)
    markdown = ""
    for report_date, items in grouped_data.items():
        header = f"## <{stock_info.stock_name}（{stock_info.stock_code_normalize}）> - {report_date} 机构持仓明细"
        markdown += f"""{header}

| 机构名称 | 持股家数(家) | 持股总数(万股) | 持股市值(亿元) |占总股本比例(%) | 占流通股比例(%) |
|---------|------------|--------------|--------------|--------------|---------------|
"""
        has_other = any(item.get('ORG_TYPE_NAME') == '其他' for item in items)
        
        for item in items:
            org_name = item.get('ORG_TYPE_NAME', '--')
            hold_num = item.get('HOULD_NUM')
            free_share = f"{convert_amount_org_holder(item.get('FREE_SHARES', 0))}" if item.get('FREE_SHARES') else '--'
            free_market_cap = f"{convert_amount_org_holder_1(item.get('FREE_MARKET_CAP', 0))}" if item.get('FREE_MARKET_CAP') else '--'
            free_total_ratio = f"{round(item.get('TOTALSHARES_RATIO', 0), 2)}%" if item.get('TOTALSHARES_RATIO') else '--'
            free_share_ratio = f"{round((item.get('FREESHARES_RATIO') or 0), 2)}%"
            markdown += f"| {org_name} | {hold_num} | {free_share} | {free_market_cap} | {free_total_ratio} | {free_share_ratio} |\n"
        
        if not has_other:
            markdown += "| 其他 | - | 0.00 | - | - | - |\n"
        
        markdown += "\n"
    return markdown


async def get_org_holder_json(stock_info: StockInfo, page_size=8):
    """获取机构持仓明细并转换为JSON格式"""
    holder_data = await get_org_holder(stock_info, page_size)
    if not holder_data:
        return []
    from collections import defaultdict
    grouped_data = defaultdict(list)
    for item in holder_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        grouped_data[report_date].append(item)
    
    result = []
    for report_date, items in grouped_data.items():
        report_items = []
        has_other = any(item.get('ORG_TYPE_NAME') == '其他' for item in items)
        
        for item in items:
            report_items.append({
                "机构名称": item.get('ORG_TYPE_NAME', '--'),
                "持股家数(家)": item.get('HOULD_NUM'),
                "持股总数(万股)": convert_amount_org_holder_1(item.get('FREE_SHARES', 0)) if item.get('FREE_SHARES') else '--',
                "持股市值(亿元)": convert_amount_org_holder_2(item.get('FREE_MARKET_CAP', 0)) if item.get('FREE_MARKET_CAP') else '--',
                "占总股本比例(%)": f"{round(item.get('TOTALSHARES_RATIO', 0), 2)}%" if item.get('TOTALSHARES_RATIO') else '--',
                "占流通股比例(%)": f"{round((item.get('FREESHARES_RATIO') or 0), 2)}%",
                "持股变化(万股)": convert_amount_org_holder_1(item.get('HOLDCHA_NUM', 0))
            })
        
        if not has_other:
            report_items.append({
                "机构名称": "其他",
                "持股家数(家)": "-",
                "持股总数(万股)": "0.00",
                "持股市值(亿元)": "-",
                "占总股本比例(%)": "-",
                "占流通股比例(%)": "-",
                "持股变化(万股)": "-"
            })
        
        result.append({
            "报告日期": report_date,
            "机构持仓": report_items
        })
    return result


async def get_org_holder_count(stock_info: StockInfo, page_size=8):
    """获取每个季度持股机构的总数"""
    holder_data = await get_org_holder(stock_info, page_size)
    if not holder_data:
        return []
    from collections import defaultdict
    grouped_data = defaultdict(int)
    for item in holder_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        hold_num = item.get('HOULD_NUM', 0) or 0
        grouped_data[report_date] += hold_num
    
    return [{"报告日期": date, "机构总数": count} for date, count in grouped_data.items()]


async def get_holder_number(stock_info: StockInfo, page_size=12, page_number=1):
    """获取股东人数数据"""
    cache_path = get_cache_path("holder_number_" + str(page_number), stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    params = {
        "reportName": "RPT_F10_EH_HOLDERNUM",
        "columns": "SECUCODE,SECURITY_CODE,END_DATE,HOLDER_TOTAL_NUM,TOTAL_NUM_RATIO,AVG_FREE_SHARES,AVG_FREESHARES_RATIO,HOLD_FOCUS,PRICE,AVG_HOLD_AMT,HOLD_RATIO_TOTAL,FREEHOLD_RATIO_TOTAL",
        "quoteColumns": "",
        "filter": f"(SECUCODE=\"{stock_info.stock_code_normalize}\")",
        "pageNumber": str(page_number),
        "pageSize": str(page_size),
        "sortTypes": "-1",
        "sortColumns": "END_DATE",
        "source": "HSF10",
        "client": "PC"
    }
    
    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        save_cache(cache_path, result)
        return result
    else:
        return []


async def get_holder_number_json(stock_info: StockInfo, page_size=20):
    """获取股东人数数据并转换为JSON格式"""
    data = await get_holder_number(stock_info, 8, 1)
    if page_size > 8:
        data_1 = await get_holder_number(stock_info, 8, 2)
        data.extend(data_1)
    if not data:
        return []
    
    result = []
    for item in data:
        # 处理筹码集中度字段，可能是数字或文本描述
        hold_focus = item.get('HOLD_FOCUS')
        if hold_focus:
            try:
                hold_focus_str = f"{round(float(hold_focus), 2)}%"
            except (ValueError, TypeError):
                hold_focus_str = str(hold_focus)
        else:
            hold_focus_str = '--'
        
        result.append({
            "截止日期": item.get('END_DATE', '--')[:10] if item.get('END_DATE') else '--',
            "股东总数": item.get('HOLDER_TOTAL_NUM', '--'),
            "较上期变化": f"{round(float(item.get('TOTAL_NUM_RATIO', 0)), 2)}%" if item.get('TOTAL_NUM_RATIO') else '--',
            "人均流通股(股)": f"{float(item.get('AVG_FREE_SHARES', 0)):.2f}" if item.get('AVG_FREE_SHARES') else '--',
            "较上期变化率": f"{round(float(item.get('AVG_FREESHARES_RATIO', 0)), 2)}%" if item.get('AVG_FREESHARES_RATIO') else '--',
            "筹码集中度": hold_focus_str,
            "股价(元)": f"{float(item.get('PRICE', 0)):.2f}" if item.get('PRICE') else '--',
            "人均持股金额(元)": f"{float(item.get('AVG_HOLD_AMT', 0)):.2f}" if item.get('AVG_HOLD_AMT') else '--'
        })
    return result


if __name__ == "__main__":
    import asyncio
    import json
    
    async def main():
        stock_name = "北方华创"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)
        # 测试股东人数数据
        result = await get_holder_number_json(stock_info, 20)
        print("股东人数数据 (JSON格式):")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())
