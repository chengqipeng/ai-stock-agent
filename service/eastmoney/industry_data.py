import aiohttp
import json
from .common_utils import EASTMONEY_DATA_API_URL


async def get_industry_market_data(secucode="002371.SZ", page_size=5):
    """获取同行业公司市场数据"""
    params = {
        "reportName": "RPT_PCF10_INDUSTRY_MARKET",
        "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,ORG_CODE,CORRE_SECUCODE,CORRE_SECURITY_CODE,CORRE_SECURITY_NAME,CORRE_ORG_CODE,TOTAL_CAP,FREECAP,TOTAL_OPERATEINCOME,NETPROFIT,REPORT_TYPE,TOTAL_CAP_RANK,FREECAP_RANK,TOTAL_OPERATEINCOME_RANK,NETPROFIT_RANK",
        "quoteColumns": "",
        "filter": f"(SECUCODE=\"{secucode}\")(CORRE_SECUCODE<>\"{secucode}\")(CORRE_SECUCODE<>\"行业平均\")(CORRE_SECUCODE<>\"行业中值\")",
        "pageNumber": "1",
        "pageSize": str(page_size),
        "sortTypes": "-1",
        "sortColumns": "FREECAP",
        "source": "HSF10",
        "client": "PC"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://datacenter.eastmoney.com/"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(EASTMONEY_DATA_API_URL, params=params, headers=headers) as response:
            text = await response.text()
            data = json.loads(text)
            if data.get("result") and data["result"].get("data"):
                return data["result"]["data"]
            else:
                raise Exception(f"未获取到股票 {secucode} 的同行业公司数据")
