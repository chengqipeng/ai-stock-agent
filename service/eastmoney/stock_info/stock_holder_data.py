from common.utils.amount_utils import convert_amount_unit, convert_amount_org_holder, convert_amount_org_holder_1
from common.http.http_utils import EASTMONEY_API_URL, fetch_eastmoney_api


async def get_org_holder(stock_code="002371", page_size=8, page_number=1):
    """获取机构持仓数据"""
    params = {
        "reportName": "RPT_MAIN_ORGHOLD",
        "columns": "ALL",
        "quoteColumns": "",
        "filter": f"(SECURITY_CODE=\"{stock_code}\")",
        "pageNumber": str(page_number),
        "pageSize": str(page_size),
        "sortTypes": "",
        "sortColumns": "",
        "source": "WEB",
        "client": "WEB"
    }
    
    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        return []


async def get_shareholder_increase(stock_code="601698", page_size=300, page_number=1):
    """获取股东增持数据"""
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
        "filter": f"(SECURITY_CODE=\"{stock_code}\")"
    }
    data = await fetch_eastmoney_api(EASTMONEY_API_URL, params)
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        raise Exception(f"未获取到股票 {stock_code} 的股东增持数据")


async def get_holder_detail(scode, report_date=None, page_num=1, page_size=100, sh_type="", sh_code="", sort_field="HOLDER_CODE", sort_direc=1):
    """获取股票主力持仓明细"""
    from datetime import datetime
    if report_date is None:
        report_date = datetime.now().strftime("%Y-%m-%d")
    url = "https://data.eastmoney.com/dataapi/zlsj/detail"
    params = {
        "SHType": sh_type,
        "SHCode": sh_code,
        "SCode": scode,
        "ReportDate": report_date,
        "sortField": sort_field,
        "sortDirec": sort_direc,
        "pageNum": page_num,
        "pageSize": page_size
    }
    data = await fetch_eastmoney_api(url, params)
    if data and data.get('data'):
        return data['data']
    else:
        return []


async def get_shareholder_increase_markdown(stock_code="601698", page_size=20, stock_name=None):
    """获取股东增减持明细并转换为markdown"""
    items = await get_shareholder_increase(stock_code, page_size)
    if not items:
        return ""
    header = f"## <{stock_code} {stock_name}> - 股东增减持明细" if stock_name else f"## 股东增减持明细 (股票代码: {stock_code})"
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
        markdown += f"| {holder_name} | {direction} | {change_num} | {change_rate} | {change_free_ratio} | {after_holder_num} | {hold_ratio} | {free_shares} | {free_shares_ratio} | {start_date} | {end_date} | {notice_date} |\n"
    return markdown + "\n"


async def get_holder_detail_markdown(scode, report_date=None, page_size=100):
    """获取股票主力持仓明细并转换为markdown"""
    from datetime import datetime
    if report_date is None:
        report_date = datetime.now().strftime("%Y-%m-%d")
    data = await get_holder_detail(scode, report_date, page_size=page_size)
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


async def get_org_holder_markdown(stock_code, page_size=8, stock_name=None):
    """获取机构持仓明细并转换为markdown"""
    holder_data = await get_org_holder(stock_code, page_size)
    if not holder_data:
        return ""
    from collections import defaultdict
    grouped_data = defaultdict(list)
    for item in holder_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        grouped_data[report_date].append(item)
    markdown = ""
    for report_date, items in grouped_data.items():
        header = f"## <{stock_code} {stock_name}> - {report_date} 机构持仓明细" if stock_name else f"## {report_date} 机构持仓明细"
        markdown += f"""{header}

| 机构名称 | 持股家数(家) | 持股总数(万股) | 持股市值(亿元) |占总股本比例(%) | 占流通股比例(%) |
|---------|------------|--------------|--------------|--------------|---------------|
"""
        for item in items:
            org_name = item.get('ORG_TYPE_NAME', '--')
            hold_num = item.get('HOULD_NUM')
            free_share = f"{convert_amount_org_holder(item.get('FREE_SHARES', 0))}" if item.get('FREE_SHARES') else '--'
            free_market_cap = f"{convert_amount_org_holder_1(item.get('FREE_MARKET_CAP', 0))}" if item.get('FREE_MARKET_CAP') else '--'
            free_total_ratio = f"{round(item.get('TOTALSHARES_RATIO', 0), 2)}%" if item.get('TOTALSHARES_RATIO') else '--'
            free_share_ratio = f"{round((item.get('FREESHARES_RATIO') or 0), 2)}%"
            markdown += f"| {org_name} | {hold_num} | {free_share} | {free_market_cap} | {free_total_ratio} | {free_share_ratio} |\n"
        markdown += "\n"
    return markdown
