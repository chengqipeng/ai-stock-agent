import aiohttp
import asyncio
from common.utils.amount_utils import convert_amount_unit


MAX_RECENT_PERIODS = 24
SHARE_FIELDS = ['TOTAL_SHARES', 'LIMITED_SHARES', 'UNLIMITED_SHARES', 'LISTED_A_SHARES', 'FREE_SHARES', 'LIMITED_A_SHARES']

EQUITY_INDICATORS = [
    ('变动日期', 'END_DATE'),
    ('总股本(股)', 'TOTAL_SHARES'),
    ('流通受限股份(股)', 'LIMITED_SHARES'),
    ('已流通股份(股)', 'UNLIMITED_SHARES'),
    ('已上市流通A股(股)', 'LISTED_A_SHARES'),
    ('流通股(股)', 'FREE_SHARES'),
    ('限售A股(股)', 'LIMITED_A_SHARES'),
    ('变动原因', 'CHANGE_REASON'),
]


async def get_equity_data_to_json(secucode="002371.SZ", indicator_keys=None):
    """将股本结构数据转换为JSON格式"""
    data_list = await get_equity_structure_data(secucode, page_size=MAX_RECENT_PERIODS)
    if not data_list:
        return []
    
    indicators = EQUITY_INDICATORS if indicator_keys is None else [(n, k) for n, k in EQUITY_INDICATORS if k in indicator_keys]
    
    result = []
    for d in data_list:
        period_data = {}
        for name, key in indicators:
            val = d.get(key)
            if val is None:
                period_data[name] = None
            elif isinstance(val, (int, float)):
                period_data[name] = int(val) if key in SHARE_FIELDS else val
            else:
                period_data[name] = str(val).split()[0] if key == 'END_DATE' else str(val)
        result.append(period_data)
    
    return result


async def get_equity_data_to_markdown(secucode="002371.SZ", indicator_keys=None):
    """将股本结构数据转换为Markdown格式"""
    data_list = await get_equity_structure_data(secucode, page_size=MAX_RECENT_PERIODS)
    if not data_list:
        return "暂无股本结构数据"
    
    md = "## 股本结构变动\n\n"
    md += "| 指标 | " + " | ".join([d.get('END_DATE', '').split()[0] for d in data_list]) + " |\n"
    md += "|" + "---|" * (len(data_list) + 1) + "\n"
    
    indicators = EQUITY_INDICATORS if indicator_keys is None else [(n, k) for n, k in EQUITY_INDICATORS if k in indicator_keys]
    
    for name, key in indicators:
        row = f"| {name} | "
        values = []
        for d in data_list:
            val = d.get(key)
            if val is None:
                values.append("-")
            elif isinstance(val, (int, float)):
                values.append(f"{int(val)}" if key in SHARE_FIELDS else str(val))
            else:
                values.append(str(val))
        row += " | ".join(values) + " |\n"
        md += row
    
    return md


async def get_equity_structure_data(secucode="002371.SZ", page_size=20, page_number=1):
    """获取股本结构数据"""
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    
    params = {
        "reportName": "RPT_F10_EH_EQUITY",
        "columns": "SECUCODE,SECURITY_CODE,END_DATE,TOTAL_SHARES,LIMITED_SHARES,LIMITED_OTHARS,LIMITED_DOMESTIC_NATURAL,LIMITED_STATE_LEGAL,LIMITED_OVERSEAS_NOSTATE,LIMITED_OVERSEAS_NATURAL,UNLIMITED_SHARES,LISTED_A_SHARES,B_FREE_SHARE,H_FREE_SHARE,FREE_SHARES,LIMITED_A_SHARES,NON_FREE_SHARES,LIMITED_B_SHARES,OTHER_FREE_SHARES,LIMITED_STATE_SHARES,LIMITED_DOMESTIC_NOSTATE,LOCK_SHARES,LIMITED_FOREIGN_SHARES,LIMITED_H_SHARES,SPONSOR_SHARES,STATE_SPONSOR_SHARES,SPONSOR_SOCIAL_SHARES,RAISE_SHARES,RAISE_STATE_SHARES,RAISE_DOMESTIC_SHARES,RAISE_OVERSEAS_SHARES,CHANGE_REASON",
        "quoteColumns": "",
        "filter": f'(SECUCODE="{secucode}")',
        "pageNumber": str(page_number),
        "pageSize": str(page_size),
        "sortTypes": "-1",
        "sortColumns": "END_DATE",
        "source": "HSF10",
        "client": "PC",
        "v": "01777784686238697"
    }
    
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Origin": "https://emweb.securities.eastmoney.com",
        "Pragma": "no-cache",
        "Referer": "https://emweb.securities.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=headers) as response:
            data = await response.json(content_type=None)
            if data.get("result") and data["result"].get("data"):
                return data["result"]["data"]
            else:
                raise Exception(f"未获取到证券代码 {secucode} 的股本结构数据")


if __name__ == "__main__":
    async def main():
        markdown = await get_equity_data_to_markdown("002371.SZ")
        print(markdown)
        print("\n" + "="*50 + "\n")
        
        json_data = await get_equity_data_to_json("002371.SZ")
        print(json_data)
    
    asyncio.run(main())
