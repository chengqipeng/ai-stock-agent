from common.utils.amount_utils import convert_amount_unit
from .common_utils import EASTMONEY_DATA_API_URL, fetch_eastmoney_api


async def get_industry_market_data(secucode="002371.SZ", page_size=5, page_number=1):
    """获取同行业公司市场数据"""
    params = {
        "reportName": "RPT_PCF10_INDUSTRY_MARKET",
        "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,ORG_CODE,CORRE_SECUCODE,CORRE_SECURITY_CODE,CORRE_SECURITY_NAME,CORRE_ORG_CODE,TOTAL_CAP,FREECAP,TOTAL_OPERATEINCOME,NETPROFIT,REPORT_TYPE,TOTAL_CAP_RANK,FREECAP_RANK,TOTAL_OPERATEINCOME_RANK,NETPROFIT_RANK",
        "quoteColumns": "",
        "filter": f"(SECUCODE=\"{secucode}\")(CORRE_SECUCODE<>\"{secucode}\")(CORRE_SECUCODE<>\"行业平均\")(CORRE_SECUCODE<>\"行业中值\")",
        "pageNumber": str(page_number),
        "pageSize": str(page_size),
        "sortTypes": "-1",
        "sortColumns": "FREECAP",
        "source": "HSF10",
        "client": "PC"
    }
    data = await fetch_eastmoney_api(EASTMONEY_DATA_API_URL, params, referer="https://datacenter.eastmoney.com/")
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        raise Exception(f"未获取到股票 {secucode} 的同行业公司数据")


async def get_industry_market_data_markdown(secucode="002371.SZ", page_size=5):
    """获取同行业公司市场数据并转换为markdown"""
    industry_data = await get_industry_market_data(secucode, page_size)
    if not industry_data:
        return ""
    markdown = """## 同行业公司市场数据
| 股票代码 | 股票名称 | 总市值 | 流通市值 | 营业总收入 | 净利润 | 总市值排名 | 流通市值排名 | 营业收入排名 | 净利润排名 |
|---------|---------|--------|---------|-----------|--------|----------|------------|------------|----------|
"""
    for item in industry_data:
        code = item.get('CORRE_SECURITY_CODE', '--')
        name = item.get('CORRE_SECURITY_NAME', '--')
        total_cap = convert_amount_unit(item.get('TOTAL_CAP')) if item.get('TOTAL_CAP') else '--'
        free_cap = convert_amount_unit(item.get('FREECAP')) if item.get('FREECAP') else '--'
        income = convert_amount_unit(item.get('TOTAL_OPERATEINCOME')) if item.get('TOTAL_OPERATEINCOME') else '--'
        profit = convert_amount_unit(item.get('NETPROFIT')) if item.get('NETPROFIT') else '--'
        total_cap_rank = item.get('TOTAL_CAP_RANK', '--')
        free_cap_rank = item.get('FREECAP_RANK', '--')
        income_rank = item.get('TOTAL_OPERATEINCOME_RANK', '--')
        profit_rank = item.get('NETPROFIT_RANK', '--')
        markdown += f"| {code} | {name} | {total_cap} | {free_cap} | {income} | {profit} | {total_cap_rank} | {free_cap_rank} | {income_rank} | {profit_rank} |\n"
    return markdown
