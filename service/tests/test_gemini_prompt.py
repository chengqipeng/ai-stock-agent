import aiohttp
import asyncio
import json
import re
from datetime import datetime

from common.constants.stock_constants import refresh_token, choose_stocks
from common.constants.stocks_data import get_stock_code
from service.ifind.get_client_token import THSTokenClient
from service.ifind.smart_stock_picking import SmartStockPicking
from service.generate.similar_companies import SimilarCompaniesGenerator

def convert_amount_unit(amount):
    """根据金额大小自动转换单位：大于亿转换为亿，大于万转换为万"""
    if amount is None:
        return "--"
    if abs(amount) >= 100000000:  # >= 1亿
        return f"{round(amount / 100000000, 4)}亿"
    elif abs(amount) >= 10000:  # >= 1万
        return f"{round(amount / 10000, 4)}万"
    else:
        return str(amount)

def convert_amount_org_holder(amount):
    """根据金额大小自动转换单位：大于亿转换为亿，大于万转换为万"""
    if amount is None:
        return "--"
    if abs(amount) >= 100000000:  # >= 1亿
        return f"{round(amount / 100000000, 4)}"
    if abs(amount) >= 10000:  # >= 1万
        return f"{round(amount / 10000, 4)}"
    else:
        return str(amount)

def convert_amount_org_holder_1(amount):
    """根据金额大小自动转换单位：大于亿转换为亿，大于万转换为万"""
    if amount is None:
        return "--"
    if abs(amount) >= 10000:  # >= 1亿
        return f"{round(amount / 100000000, 4)}"
    else:
        return str(amount)

async def get_stock_detail(secid="0.002371"):
    """获取股票详细数据"""
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "invt": "2",
        "fltt": "1",
        "fields": "f58,f734,f107,f57,f43,f59,f169,f301,f60,f170,f152,f177,f111,f46,f44,f45,f47,f260,f48,f261,f279,f277,f278,f288,f19,f17,f531,f15,f13,f11,f20,f18,f16,f14,f12,f39,f37,f35,f33,f31,f40,f38,f36,f34,f32,f211,f212,f213,f214,f215,f210,f209,f208,f207,f206,f161,f49,f171,f50,f86,f84,f85,f168,f108,f116,f167,f164,f162,f163,f92,f71,f117,f292,f51,f52,f191,f192,f262,f294,f181,f295,f269,f270,f256,f257,f285,f286,f748,f747",
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "wbp2u": "|0|0|0|web",
        "dect": "1"
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()
            json_text = re.sub(r'^jQuery\d+_\d+\(', '', text)
            json_text = re.sub(r'\)$', '', json_text)
            data = json.loads(json_text)
            if data.get("data"):
                return data["data"]
            else:
                raise Exception(f"未获取到股票 {secid} 的详细数据")

async def get_stock_base_info(secid="0.002371"):
    """获取股票基本信息并转换为markdown"""
    detail_data = await get_stock_detail(secid)
    markdown = """## 股票基本信息\n"""
    markdown += f"- **股票代码**: {detail_data.get('f57', '--')}\n"
    markdown += f"- **股票名称**: {detail_data.get('f58', '--')}\n"
    markdown += f"- **最新价**: {round(detail_data.get('f43', 0) / 100, 2) if detail_data.get('f43') else '--'}\n"
    markdown += f"- **涨跌幅**: {round(detail_data.get('f170', 0) / 100, 2) if detail_data.get('f170') else '--'}%\n"
    markdown += f"- **涨跌额**: {round(detail_data.get('f169', 0) / 100, 2) if detail_data.get('f169') else '--'}\n"
    markdown += f"- **成交量**: {convert_amount_unit(detail_data.get('f47'))}\n"
    markdown += f"- **成交额**: {convert_amount_unit(detail_data.get('f48'))}\n"
    markdown += f"- **市盈率**: {round(detail_data.get('f162', 0) / 100, 2) if detail_data.get('f162') else '--'}\n"
    markdown += f"- **总市值**: {convert_amount_unit(detail_data.get('f116'))}\n"
    markdown += f"- **流通市值**: {convert_amount_unit(detail_data.get('f117'))}\n"
    markdown += f"- **换手率**: {round(detail_data.get('f168', 0) / 100, 2) if detail_data.get('f168') else '--'}%\n"
    markdown += f"- **量比**: {round(detail_data.get('f50', 0) / 100, 2) if detail_data.get('f50') else '--'}\n"
    markdown += f"- **最高**: {round(detail_data.get('f44', 0) / 100, 2) if detail_data.get('f44') else '--'}\n"
    markdown += f"- **最低**: {round(detail_data.get('f45', 0) / 100, 2) if detail_data.get('f45') else '--'}\n"
    markdown += f"- **今开**: {round(detail_data.get('f46', 0) / 100, 2) if detail_data.get('f46') else '--'}\n"
    markdown += f"- **昨收**: {round(detail_data.get('f60', 0) / 100, 2) if detail_data.get('f60') else '--'}\n"
    markdown += f"- **涨停**: {round(detail_data.get('f51', 0) / 100, 2) if detail_data.get('f51') else '--'}\n"
    markdown += f"- **跌停**: {round(detail_data.get('f52', 0) / 100, 2) if detail_data.get('f52') else '--'}\n"
    return markdown

async def get_stock_realtime(secid="1.601698"):
    """
    获取股票实时数据
    secid格式: 市场代码.股票代码
    1 = 上海, 0 = 深圳
    """
    url = "https://push2delay.eastmoney.com/api/qt/stock/get"

    params = {
        "fltt": "2",
        "invt": "2",
        "secid": secid,
        "fields": "f57,f58,f43,f47,f48,f168,f169,f170,f152",
        "ut": "b2884a393a59ad64002292a3e90d46a5"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()

            # 移除 JSONP 回调函数包装
            json_text = re.sub(r'^jQuery\d+_\d+\(', '', text)
            json_text = re.sub(r'\)$', '', json_text)

            data = json.loads(json_text)

            if data.get("data"):
                stock_data = data["data"]

                return stock_data
            else:
                raise Exception(f"未获取到股票 {secid} 的实时数据")


async def get_main_fund_flow(secids="0.002371"):
    """
    获取主力资金流向数据
    secids格式: 市场代码.股票代码，多个用逗号分隔
    1 = 上海, 0 = 深圳
    """
    url = "https://push2delay.eastmoney.com/api/qt/ulist.np/get"

    params = {
        "fltt": "2",
        "secids": secids,
        "fields": "f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f64,f65,f70,f71,f76,f77,f82,f83,f164,f166,f168,f170,f172,f252,f253,f254,f255,f256,f124,f6,f278,f279,f280,f281,f282",
        "ut": "b2884a393a59ad64002292a3e90d46a5"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()

            # 移除 JSONP 回调函数包装
            json_text = re.sub(r'^jQuery\d+_\d+\(', '', text)
            json_text = re.sub(r'\)$', '', json_text)

            data = json.loads(json_text)

            if data.get("data") and data["data"].get("diff"):
                result = []
                for stock in data["data"]["diff"]:
                    # 获取成交额用于计算净比
                    amount = stock.get('f6', 1)  # f6是成交额，避免除0

                    # 计算各单净比 = 净流入 / 成交额 * 100
                    super_ratio = round(stock.get('f66', 0) / amount * 100, 2) if amount else 0
                    big_ratio = round(stock.get('f72', 0) / amount * 100, 2) if amount else 0
                    mid_ratio = round(stock.get('f78', 0) / amount * 100, 2) if amount else 0
                    small_ratio = round(stock.get('f84', 0) / amount * 100, 2) if amount else 0

                    stock_info = {
                        #"股票代码": stock.get('f12'),
                        #"股票名称": stock.get('f14'),
                        #"最新价": stock.get('f2'),
                        #"涨跌幅": f"{stock.get('f3')}%",
                        "成交额": convert_amount_unit(amount),
                        "主力净流入": convert_amount_unit(stock.get('f62')),
                        "主力净流入占比": f"{round(stock.get('f184', 0), 2)}%",
                        "超大单净流入": convert_amount_unit(stock.get('f66')),
                        "超大单净比": f"{round(super_ratio, 2)}%",
                        "大单净流入": convert_amount_unit(stock.get('f72')),
                        "大单净比": f"{round(big_ratio, 2)}%",
                        "中单净流入": convert_amount_unit(stock.get('f78')),
                        "中单净比": f"{round(mid_ratio, 2)}%",
                        "小单净流入": convert_amount_unit(stock.get('f84')),
                        "小单净比": f"{round(small_ratio, 2)}%",

                        "超大单流入": f"{convert_amount_unit(stock.get('f64'))}",
                        "超大单流出": f"{convert_amount_unit(stock.get('f65'))}",
                        "大单流入": f"{convert_amount_unit(stock.get('f70'))}",
                        "大单流出": f"{convert_amount_unit(stock.get('f71'))}",
                        "中单流入": f"{convert_amount_unit(stock.get('f76'))}",
                        "中单流出": f"{convert_amount_unit(stock.get('f77'))}",
                        "小单流入": f"{convert_amount_unit(stock.get('f82'))}",
                        "小单流出": f"{convert_amount_unit(stock.get('f83'))}"
                    }
                    result.append(stock_info)


                return result
            else:
                raise Exception(f"未获取到股票 {secids} 的主力资金流向数据")


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

async def get_org_holder(stock_code="002371", page_size=8, page_number=1):
    """获取机构持仓数据"""
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    
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
                return []

async def get_shareholder_increase(stock_code="601698", page_size=300, page_number=1):
    """获取股东增持数据"""
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

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
                items = data["result"]["data"]
                if not items:
                    return ""

                markdown = f"## 股东增减持明细 (股票代码: {stock_code})\n\n"
                markdown += "| 股东名称 | 增减 | 变动数量(万股) | 占总股本比例 | 占流通股比例 | 持股总数(万股) | 占总股本比例 | 持流通股数(万股) | 占流通股比例 | 变动开始日 | 变动截止日 | 公告日 |\n"
                markdown += "|---------|------|--------------|------------|------------|--------------|------------|----------------|------------|----------|----------|--------|\n"

                for item in items[:20]:
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

                return markdown
            else:
                raise Exception(f"未获取到股票 {stock_code} 的股东增持数据")

async def get_holder_detail(scode, report_date=None, page_num=1, page_size=100, sh_type="", sh_code="", sort_field="HOLDER_CODE", sort_direc=1):
    """获取股票主力持仓明细"""
    # 如果没有提供report_date，使用当前日期
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

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status != 200:
                text = await response.text()
                raise Exception(f"请求失败: {response.status}, 响应: {text}")
            result = await response.json()

            if not result or 'data' not in result:
                return ""

            data = result['data']
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

            return markdown

def format_realtime_markdown(realtime_data):
    """格式化实时交易信息为markdown"""
    return f"""## 当日交易信息
- **股票代码**: {realtime_data.get('f57', '--')}
- **最新价**: {realtime_data.get('f43', '--')}
- **涨跌幅**: {realtime_data.get('f170', '--')}%
- **涨跌额**: {realtime_data.get('f169', '--')}
- **成交量**: {convert_amount_unit(realtime_data.get('f47'))}
- **成交额**: {convert_amount_unit(realtime_data.get('f48'))}
- **换手率**: {realtime_data.get('f168', '--')}%"""

def format_fund_flow_markdown(fund_flow_data):
    """格式化主力资金流向为markdown"""
    if not fund_flow_data:
        return ""
    flow_data = fund_flow_data[0]
    return f"""## 主力当日资金流向
- **成交额**: {flow_data.get('成交额', '--')}
- **主力净流入**: {flow_data.get('主力净流入', '--')}
- **超大单净流入**: {flow_data.get('超大单净流入', '--')}
- **大单净流入**: {flow_data.get('大单净流入', '--')}
- **中单净流入**: {flow_data.get('中单净流入', '--')}
- **小单净流入**: {flow_data.get('小单净流入', '--')}
- **主力净流入占比**: {flow_data.get('主力净流入占比', '--')}
- **超大单净流入占比**: {flow_data.get('超大单净比', '--')}
- **大单净流入占比**: {flow_data.get('大单净比', '--')}
- **中单净流入占比**: {flow_data.get('中单净比', '--')}
- **小单净流入占比**: {flow_data.get('小单净比', '--')}"""

def format_trade_distribution_markdown(fund_flow_data):
    """格式化实时成交分布为markdown"""
    if not fund_flow_data:
        return ""
    flow_data = fund_flow_data[0]
    return f"""## 实时成交分布
- **超大单流入**: {flow_data.get('超大单流入', '--')}
- **超大单流出**: {flow_data.get('超大单流出', '--')}
- **大单流入**: {flow_data.get('大单流入', '--')}
- **大单流出**: {flow_data.get('大单流出', '--')}
- **中单流入**: {flow_data.get('中单流入', '--')}
- **中单流出**: {flow_data.get('中单流出', '--')}
- **小单流入**: {flow_data.get('小单流入', '--')}
- **小单流出**: {flow_data.get('小单流出', '--')}"""

async def get_fund_flow_history(secid="0.002371"):
    """获取资金流向历史数据"""
    url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"

    params = {
        "lmt": "0",
        "klt": "101",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "secid": secid
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()

            json_text = re.sub(r'^jQuery\d+_\d+\(', '', text)
            json_text = re.sub(r'\)$', '', json_text)

            data = json.loads(json_text)

            if data.get("data") and data["data"].get("klines"):
                klines = data["data"]["klines"]
                klines.reverse()
                return klines
            else:
                raise Exception(f"未获取到股票 {secid} 的资金流向历史数据")

async def get_fund_flow_history_markdown(secid="0.002371", limit=60):
    """获取资金流向历史数据并转换为markdown"""
    klines = await get_fund_flow_history(secid)
    markdown = f"""## 历史资金流向
| 日期 | 收盘价 | 涨跌幅 | 主力净流入净额 | 主力净流入净占比 | 超大单净流入净额 | 超大单净流入净占比 | 大单净流入净额 | 大单净流入净占比 | 中单净流入净额 | 中单净流入占比 | 小单净流入净额 | 小单净流入净占比 |
|-----|-------|-------|--------------|---------------|----------------|-----------------|-------------|----------------|-------------|--------------|--------------|---------------|
"""
    for kline in klines[:limit]:
        fields = kline.split(',')
        if len(fields) >= 15:
            date = fields[0]
            #收盘价
            close_price = round(float(fields[11]), 2) if fields[11] != '-' else '--'
            # 涨跌幅
            change_pct = f"{round(float(fields[12]), 2)}%" if fields[12] != '-' else "--"

            #超大单
            super_net = float(fields[5]) if fields[5] != '-' else 0

            #000
            super_pct = f"{round(float(fields[10]), 2)}%" if fields[10] != '-' else "--"
            super_net_str = convert_amount_unit(super_net)

            # 大单
            big_net = float(fields[4]) if fields[4] != '-' else 0
            big_net_str = convert_amount_unit(big_net)
            big_pct = f"{round(float(fields[9]), 2)}%" if fields[9] != '-' else "--"

            #中单
            mid_net = float(fields[3]) if fields[3] != '-' else 0
            mid_net_str = convert_amount_unit(mid_net)
            mid_pct = f"{round(float(fields[8]), 2)}%" if fields[8] != '-' else "--"

            #小单
            small_net = float(fields[2]) if fields[2] != '-' else 0
            small_net_str = convert_amount_unit(small_net)
            small_pct = f"{round(float(fields[7]), 2)}%" if fields[7] != '-' else "--"

            # 主力净流入净额
            main_net = super_net + big_net
            # 主力净流入净占比
            main_net_str = convert_amount_unit(main_net)
            main_pct = f"{round(float(fields[6]), 2)}%" if fields[6] != '-' else "--"
            markdown += f"| {date} | {close_price} | {change_pct} | {main_net_str} | {main_pct} | {super_net_str} | {super_pct} | {big_net_str} | {big_pct} | {mid_net_str} | {mid_pct} | {small_net_str} | {small_pct} |\n"
    return markdown

async def get_industry_market_data(secucode="002371.SZ", page_size=5):
    """获取同行业公司市场数据"""
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    
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
        async with session.get(url, params=params, headers=headers) as response:
            text = await response.text()
            data = json.loads(text)
            if data.get("result") and data["result"].get("data"):
                return data["result"]["data"]
            else:
                raise Exception(f"未获取到股票 {secucode} 的同行业公司数据")

async def get_financial_report_markdown(stock_code, page_size=5):
    """获取业绩报表明细并转换为markdown"""
    report_data = await get_financial_report(stock_code, page_size)
    if not report_data:
        return ""
    markdown = """## 业绩报表明细
| 截止日期 | 每股收益(元) | 每股收益(扣除)(元) | 营业总收入 | | | 净利润 | | | 每股净资产(元) | 净资产收益率(%) | 每股经营现金流量(元) | 销售毛利率(%) | 利润分配 | 首次公告日期 |
|----------|-------------|-------------------|-----------|---------|---------|--------|---------|---------|---------------|----------------|-------------------|--------------|---------|-------------|
| | | | 营业总收入(元) | 同比增长(%) | 季度环比增长(%) | 净利润(元) | 同比增长(%) | 季度环比增长(%) | | | | | | |
"""
    for item in report_data:
        report_date = item.get('REPORTDATE', '--')[:10] if item.get('REPORTDATE') else '--'
        basic_eps = round(item.get('BASIC_EPS', 0), 2) if item.get('BASIC_EPS') else '--'
        deduct_eps = round(item.get('DEDUCT_BASIC_EPS', 0), 2) if item.get('DEDUCT_BASIC_EPS') else '-'
        total_income = item.get('TOTAL_OPERATE_INCOME')
        income_str = convert_amount_unit(total_income) if total_income else '--'
        ystz = f"{round(item.get('YSTZ', 0), 2)}%" if item.get('YSTZ') else '--'
        yshz = f"{round(item.get('YSHZ', 0), 2)}%" if item.get('YSHZ') else '--'
        net_profit = item.get('PARENT_NETPROFIT')
        profit_str = convert_amount_unit(net_profit) if net_profit else '--'
        sjltz = f"{round(item.get('SJLTZ', 0), 2)}%" if item.get('SJLTZ') else '--'
        sjlhz = f"{round(item.get('SJLHZ', 0), 2)}%" if item.get('SJLHZ') else '--'
        bps = round(item.get('BPS', 0), 2) if item.get('BPS') else '--'
        roe = f"{round(item.get('WEIGHTAVG_ROE', 0), 2)}%" if item.get('WEIGHTAVG_ROE') else '--'
        mgjyxjje = round(item.get('MGJYXJJE', 0), 2) if item.get('MGJYXJJE') else '--'
        xsmll = f"{round(item.get('XSMLL', 0), 2)}%" if item.get('XSMLL') else '--'
        assigndscrpt = item.get('ASSIGNDSCRPT', '-') if item.get('ASSIGNDSCRPT') else '-'
        notice_date = item.get('NOTICE_DATE', '--')[:10] if item.get('NOTICE_DATE') else '--'
        markdown += f"| {report_date} | {basic_eps} | {deduct_eps} | {income_str} | {ystz} | {yshz} | {profit_str} | {sjltz} | {sjlhz} | {bps} | {roe} | {mgjyxjje} | {xsmll} | {assigndscrpt} | {notice_date} |\n"
    return markdown

async def get_financial_fast_report_markdown(stock_code, page_size=15):
    """获取业绩快报明细并转换为markdown"""
    forecast_data = await get_financial_fast_report(stock_code, page_size)
    if not forecast_data:
        return ""
    markdown = """## 业绩快报明细

| 截止日期 | 每股收益(元) | 营业总收入 | | | | 净利润 | | | | 每股净资产(元) | 净资产收益率(%) | 公告日期 |
|----------|-------------|-----------|---------|---------|---------|--------|---------|---------|---------|---------------|----------------|----------|
| | | 营业收入(元) | 去年同期(元) | 同比增长(%) | 季度环比增长(%) | 净利润(元) | 去年同期(元) | 同比增长(%) | 季度环比增长(%) | | | |
"""
    for item in forecast_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        basic_eps = round(item.get('BASIC_EPS', 0), 2) if item.get('BASIC_EPS') else '--'
        total_income = item.get('TOTAL_OPERATE_INCOME')
        income_str = convert_amount_unit(total_income) if total_income else '--'
        total_income_sq = item.get('TOTAL_OPERATE_INCOME_SQ')
        income_sq_str = convert_amount_unit(total_income_sq) if total_income_sq else '--'
        ystz = f"{round(item.get('YSTZ', 0), 2)}%" if item.get('YSTZ') else '--'
        djdyshz = f"{round(item.get('DJDYSHZ', 0), 2)}%" if item.get('DJDYSHZ') else '--'
        net_profit = item.get('PARENT_NETPROFIT')
        profit_str = convert_amount_unit(net_profit) if net_profit else '--'
        net_profit_sq = item.get('PARENT_NETPROFIT_SQ')
        profit_sq_str = convert_amount_unit(net_profit_sq) if net_profit_sq else '--'
        jlrtbzcl = f"{round(item.get('JLRTBZCL', 0), 2)}%" if item.get('JLRTBZCL') else '--'
        djdjlhz = f"{round(item.get('DJDJLHZ', 0), 2)}%" if item.get('DJDJLHZ') else '--'
        bvps = round(item.get('PARENT_BVPS', 0), 2) if item.get('PARENT_BVPS') else '--'
        roe = f"{round(item.get('WEIGHTAVG_ROE', 0), 2)}%" if item.get('WEIGHTAVG_ROE') else '--'
        notice_date = item.get('NOTICE_DATE', '--')[:10] if item.get('NOTICE_DATE') else '--'
        markdown += f"| {report_date} | {basic_eps} | {income_str} | {income_sq_str} | {ystz} | {djdyshz} | {profit_str} | {profit_sq_str} | {jlrtbzcl} | {djdjlhz} | {bvps} | {roe} | {notice_date} |\n"
    return markdown

async def get_performance_forecast_markdown(stock_code, page_size=15):
    """获取业绩预告明细并转换为markdown"""
    forecast_data = await get_performance_forecast(stock_code, page_size)
    if not forecast_data:
        return ""
    markdown = """## 业绩预告明细
| 截止日期 | 预测指标 | 业绩变动 | 预测数值(元) | 业绩变动同比 | 业绩变动环比 | 业绩变动原因 | 预告类型 | 上年同期值(元) | 公告日期 |
|----------|---------|---------|------------|------------|------------|------------|---------|--------------|----------|
"""
    for item in forecast_data:
        report_date = item.get('REPORT_DATE', '--')[:10] if item.get('REPORT_DATE') else '--'
        predict_finance = item.get('PREDICT_FINANCE', '--')
        predict_content = item.get('PREDICT_CONTENT', '--')
        amt_lower = item.get('PREDICT_AMT_LOWER')
        amt_upper = item.get('PREDICT_AMT_UPPER')
        if predict_finance == '每股收益':
            predict_value = f"{amt_lower}～{amt_upper}" if amt_lower and amt_upper else '--'
        else:
            predict_value = f"{convert_amount_unit(amt_lower)}～{convert_amount_unit(amt_upper)}" if amt_lower and amt_upper else '--'
        add_lower = item.get('ADD_AMP_LOWER')
        add_upper = item.get('ADD_AMP_UPPER')
        add_amp = f"{round(add_lower, 2)}%～{round(add_upper, 2)}%" if add_lower is not None and add_upper is not None else '-'
        ratio_lower = item.get('PREDICT_RATIO_LOWER')
        ratio_upper = item.get('PREDICT_RATIO_UPPER')
        predict_ratio = f"{round(ratio_lower, 2)}%～{round(ratio_upper, 2)}%" if ratio_lower is not None and ratio_upper is not None else '-'
        change_reason = item.get('CHANGE_REASON_EXPLAIN', '--')
        predict_type = item.get('PREDICT_TYPE', '--')
        preyear = item.get('PREYEAR_SAME_PERIOD')
        if predict_finance == '每股收益':
            preyear_str = str(preyear) if preyear else '--'
        else:
            preyear_str = convert_amount_unit(preyear) if preyear else '--'
        notice_date = item.get('NOTICE_DATE', '--')[:10] if item.get('NOTICE_DATE') else '--'
        markdown += f"| {report_date} | {predict_finance} | {predict_content} | {predict_value} | {add_amp} | {predict_ratio} | {change_reason} | {predict_type} | {preyear_str} | {notice_date} |\n"
    return markdown

async def get_org_holder_markdown(stock_code, page_size=8):
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
        markdown += f"""## {report_date} 机构持仓明细

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


async def get_stock_markdown(secid="0.002371", stock_name=None):
    """获取股票数据并返回格式化的markdown"""
    try:
        stock_code = secid.split('.')[-1]
        realtime_data = await get_stock_realtime(secid)
        fund_flow_data = await get_main_fund_flow(secid)

        markdown = (""
                    f"# 使用欧奈尔CAN SLIM规则分析一下<{stock_code} {stock_name}>，是否符合买入条件：基于模型的最终判断，稳健买入价格区间：基于技术形态（如杯柄形态、突破点）给出的建议\n"
                    "# 1.分析涉及当日交易信息、主力当日资金流向、实时成交分布、股票基本信息、业绩报表、业绩预告、高管减持、机构持仓变化等数据必须严格使用已提供的【东方财富数据集】\n"
                    #f"# 从网络搜索 <{stock_code} {stock_name}> 行业动态、未来销售预测、欧美同级别产品限制政策等公开内容，网络数据必须备注来源\n"
                    "# 2.参考【东方财富数据集】中A股市场中业务相关性最高的上市公司的交易信息\n"
                    f"# 3.同时针对股票<{stock_code} {stock_name}>执行深度行业调研，要求如下：\n"
                    "## 3.1.**行业动态**： 检索近 6 个月内该行业的核心技术变革、重大投融资事件及市场格局变化。\n"
                    "## 3.2.**销售预测**： 搜集权威机构（如券商研报、咨询公司）对该企业或所属细分赛道的未来3-5年营收增速、出货量或市场份额的预测数据。\n"
                    "## 3.3.**政策环境**： 重点调研欧美市场对同类产品的准入门槛、关税政策、环保指令或技术性贸易壁垒（如反倾销、出口管制）。\n"
                    "## 3.4.**数据规范**： 所有的核心事实、数据点必须紧跟 [来源链接/机构名称]。\n"
                    "# 4.呈现形式：请以表格或分级标题的形式输出。\n"
                    "# 5.必须在明细结论中备注数据来源。\n"
                    "# 6.以下是【东方财富数据集】：\n")
        
        markdown += f"## <{stock_code} {stock_name}> - 当日交易信息\n" + format_realtime_markdown(realtime_data).replace("## 当日交易信息", "") + "\n"
        markdown += f"## <{stock_code} {stock_name}> - 主力当日资金流向\n" + format_fund_flow_markdown(fund_flow_data).replace("## 主力当日资金流向", "") + "\n\n"
        markdown += f"## <{stock_code} {stock_name}> - 实时成交分布\n" + format_trade_distribution_markdown(fund_flow_data).replace("## 实时成交分布", "") + "\n\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 股票基本信息\n" + (await get_stock_base_info(secid)).replace("## 股票基本信息", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 股票基本信息错误\n\n获取失败: {str(e)}\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 历史资金流向\n" + (await get_fund_flow_history_markdown(secid)).replace("## 历史资金流向", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 历史资金流向错误\n\n获取失败: {str(e)}\n"

        try:
            markdown += f"## <{stock_code} {stock_name}> - 业绩报表明细\n" + (await get_financial_report_markdown(stock_code)).replace("## 业绩报表明细", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 业绩报表明细错误\n\n获取失败: {str(e)}\n"

        # try:
        #     markdown += f"## <{stock_code} {stock_name}> - 业绩报表明细\n" + (await get_financial_fast_report_markdown(stock_code)).replace("## 业绩快报明细", "") + "\n\n"
        # except Exception as e:
        #     markdown += f"## 业绩快报明细错误\n\n获取失败: {str(e)}\n\n"


        try:
            markdown += f"## <{stock_code} {stock_name}> - 业绩预告明细\n" + (await get_performance_forecast_markdown(stock_code)).replace("## 业绩预告明细", "") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 业绩预告明细错误\n\n获取失败: {str(e)}\n"

        try:
            org_md = await get_org_holder_markdown(stock_code)
            markdown += org_md.replace("##", f"## <{stock_code} {stock_name}> -") + "\n"
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 机构持仓明细错误\n\n获取失败: {str(e)}\n\n"

        try:
            increase_markdown = await get_shareholder_increase(stock_code)
            if increase_markdown:
                markdown += increase_markdown.replace("##", f"## <{stock_code} {stock_name}> -")
        except Exception as e:
            markdown += f"## <{stock_code} {stock_name}> - 股东增减持明细\n: {str(e)}"

        return markdown
    except Exception as e:
        return f"# 错误\n\n获取股票数据失败: {str(e)}"


def normalize_stock_code(code):
    """自动添加市场前缀: SH结尾添加1., SZ结尾添加0."""
    code = code.strip()
    if code.endswith('.SH'):
        return f"1.{code.split('.')[0]}"
    elif code.endswith('.SZ'):
        return f"0.{code.split('.')[0]}"
    return code

async def get_similar_companies_data(stock_name, stock_code, similar_company_num = 5):
    """获取相似公司的资金流向数据"""
    #secucode = f"{stock_code.split('.')[-1]}.SZ" if stock_code.startswith(('0', '3')) else f"{stock_code.split('.')[-1]}.SH"
    industry_data = await get_industry_market_data(stock_code, similar_company_num)

    similar_prompt = f"\n**以下是A股市场中和<{stock_code} {stock_name}>业务相关性最高的{similar_company_num}家上市公司的资金流向数据**\n"
    for company in industry_data:
        code = company.get('SECUCODE')
        name = company.get('CORRE_SECURITY_NAME')
        
        similar_secid = normalize_stock_code(f"{code}")
        try:
            fund_flow = await get_main_fund_flow(similar_secid)
            fund_flow_md = f"## <{code} {name}>：\n#" + format_fund_flow_markdown(fund_flow) + "\n\n"
            fund_flow_md += f"## <{code} {name}>: \n#" + format_trade_distribution_markdown(fund_flow)
            similar_prompt += fund_flow_md + "\n\n"
        except Exception as e:
            print(f"  <{code} {name}> 主力当日资金流向: 获取失败 - {str(e)}\n")
        
        try:
            history_md = await get_fund_flow_history_markdown(similar_secid, 20)
            history_md = f"## <{code} {name}>：\n#" + history_md
            similar_prompt += history_md + "\n\n"
        except Exception as e:
            print(f"  <{code} {name}> 历史资金流向: 获取失败 - {str(e)}")
    
    return similar_prompt

async def main():
    """
    目前不持有该股票，结合已提供的数据和你的分析，本周我该如何操作
    """
    stock_name = "三花智控"
    stock_code = get_stock_code(stock_name)
    similar_company_num = 5

    similar_prompt = await get_similar_companies_data(stock_name, stock_code, similar_company_num)

    main_stock_result = await get_stock_markdown(normalize_stock_code(stock_code), stock_name)
    main_stock_result += similar_prompt
    print("\n\n")
    print(main_stock_result)


if __name__ == "__main__":
    asyncio.run(main())
