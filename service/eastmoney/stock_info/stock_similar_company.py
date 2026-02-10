from common.utils.amount_utils import normalize_stock_code
from common.http.http_utils import fetch_eastmoney_api, EASTMONEY_DATA_API_URL
from service.eastmoney.stock_info.stock_fund_flow import get_main_fund_flow_markdown
from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history_markdown


async def get_similar_companies_data(stock_name, stock_code, similar_company_num=5):
    """获取相似公司的资金流向数据"""
    industry_data = await get_industry_market_data(stock_code, similar_company_num)
    similar_prompt = f"\n**以下是A股市场中和<{stock_code} {stock_name}>业务相关性最高的{similar_company_num}家上市公司的资金流向数据**\n"
    for company in industry_data:
        code = company.get('SECUCODE')
        name = company.get('CORRE_SECURITY_NAME')
        similar_secid = normalize_stock_code(f"{code}")

        similar_prompt = await get_main_fund_flow_markdown(similar_secid, code, name)
        #similar_prompt += await get_trade_distribution_markdown(similar_secid)

        similar_prompt += await get_fund_flow_history_markdown(similar_secid, code, name)
    return similar_prompt

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