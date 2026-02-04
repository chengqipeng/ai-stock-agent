import aiohttp
import json
import re


async def get_financial_data(stock_code="002371", page_size=5, page_number=1):
    """获取财务数据"""
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    params = {
        "sortColumns": "REPORTDATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "columns": "ALL",
        "filter": f"(SECURITY_CODE=\"{stock_code}\")",
        "reportName": "RPT_LICO_FN_CPD"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://datacenter.eastmoney.com/"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()

            json_text = re.sub(r'^jQuery\d+_\d+\(', '', text)
            json_text = re.sub(r'\)$', '', json_text)

            data = json.loads(json_text)

            if data.get("result") and data["result"].get("data"):
                return data["result"]["data"]
            else:
                raise Exception(f"未获取到股票 {stock_code} 的财务数据")


async def get_financial_report(stock_code="002371", page_size=15, page_number=1):
    """业绩报表明细"""
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    params = {
        "sortColumns": "REPORTDATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "columns": "ALL",
        "filter": f"(SECURITY_CODE=\"{stock_code}\")",
        "reportName": "RPT_LICO_FN_CPD"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://datacenter.eastmoney.com/"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()

            json_text = re.sub(r'^jQuery\d+_\d+\(', '', text)
            json_text = re.sub(r'\)$', '', json_text)

            data = json.loads(json_text)

            if data.get("result") and data["result"].get("data"):
                return data["result"]["data"]
            else:
                raise Exception(f"未获取到股票 {stock_code} 的财务报表数据")


async def get_financial_fast_report(stock_code="002371", page_size=15, page_number=1):
    """获取业绩预告数据"""
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "columns": "ALL",
        "filter": f"(SECURITY_CODE=\"{stock_code}\")",
        "reportName": "RPT_FCI_PERFORMANCEE"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://datacenter.eastmoney.com/"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()

            json_text = re.sub(r'^jQuery\d+_\d+\(', '', text)
            json_text = re.sub(r'\)$', '', json_text)

            data = json.loads(json_text)

            if data.get("result") and data["result"].get("data"):
                return data["result"]["data"]
            else:
                raise Exception(f"未获取到股票 {stock_code} 的业绩预告数据")


async def get_performance_forecast(stock_code="002371", page_size=15, page_number=1):
    """获取业绩预告数据"""
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page_number),
        "columns": "ALL",
        "filter": f"(SECURITY_CODE=\"{stock_code}\")",
        "reportName": "RPT_PUBLIC_OP_NEWPREDICT"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://datacenter.eastmoney.com/"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()

            json_text = re.sub(r'^jQuery\d+_\d+\(', '', text)
            json_text = re.sub(r'\)$', '', json_text)

            data = json.loads(json_text)

            if data.get("result") and data["result"].get("data"):
                return data["result"]["data"]
            else:
                raise Exception(f"未获取到股票 {stock_code} 的业绩预告数据")
