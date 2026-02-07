from common.utils.amount_utils import convert_amount_unit
from .common_utils import EASTMONEY_PUSH_API_URL, EASTMONEY_PUSH2HIS_API_URL, fetch_eastmoney_api


async def get_main_fund_flow(secids="0.002371"):
    """获取主力资金流向数据"""
    url = f"{EASTMONEY_PUSH_API_URL}/ulist.np/get"
    params = {
        "fltt": "2",
        "secids": secids,
        "fields": "f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f64,f65,f70,f71,f76,f77,f82,f83,f164,f166,f168,f170,f172,f252,f253,f254,f255,f256,f124,f6,f278,f279,f280,f281,f282",
        "ut": "b2884a393a59ad64002292a3e90d46a5"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data") and data["data"].get("diff"):
        result = []
        for stock in data["data"]["diff"]:
            amount = stock.get('f6', 1)
            super_ratio = round(stock.get('f66', 0) / amount * 100, 2) if amount else 0
            big_ratio = round(stock.get('f72', 0) / amount * 100, 2) if amount else 0
            mid_ratio = round(stock.get('f78', 0) / amount * 100, 2) if amount else 0
            small_ratio = round(stock.get('f84', 0) / amount * 100, 2) if amount else 0
            stock_info = {
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


async def get_fund_flow_history(secid="0.002371"):
    """获取资金流向历史数据"""
    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/fflow/daykline/get"
    params = {
        "lmt": "0",
        "klt": "101",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "secid": secid
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data") and data["data"].get("klines"):
        klines = data["data"]["klines"]
        klines.reverse()
        return klines
    else:
        raise Exception(f"未获取到股票 {secid} 的资金流向历史数据")


async def get_main_fund_flow_markdown(secids="0.002371"):
    """获取主力资金流向并转换为markdown"""
    fund_flow_data = await get_main_fund_flow(secids)
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


async def get_trade_distribution_markdown(secids="0.002371"):
    """获取实时成交分布并转换为markdown"""
    fund_flow_data = await get_main_fund_flow(secids)
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


async def get_fund_flow_history_markdown(secid="0.002371", page_size=60):
    """获取资金流向历史数据并转换为markdown"""
    klines = await get_fund_flow_history(secid)
    markdown = f"""## 历史资金流向
| 日期 | 收盘价 | 涨跌幅 | 主力净流入净额 | 主力净流入净占比 | 超大单净流入净额 | 超大单净流入净占比 | 大单净流入净额 | 大单净流入净占比 | 中单净流入净额 | 中单净流入占比 | 小单净流入净额 | 小单净流入净占比 |
|-----|-------|-------|--------------|---------------|----------------|-----------------|-------------|----------------|-------------|--------------|--------------|---------------|
"""
    for kline in klines[:page_size]:
        fields = kline.split(',')
        if len(fields) >= 15:
            date = fields[0]
            close_price = round(float(fields[11]), 2) if fields[11] != '-' else '--'
            change_pct = f"{round(float(fields[12]), 2)}%" if fields[12] != '-' else "--"
            super_net = float(fields[5]) if fields[5] != '-' else 0
            super_pct = f"{round(float(fields[10]), 2)}%" if fields[10] != '-' else "--"
            super_net_str = convert_amount_unit(super_net)
            big_net = float(fields[4]) if fields[4] != '-' else 0
            big_net_str = convert_amount_unit(big_net)
            big_pct = f"{round(float(fields[9]), 2)}%" if fields[9] != '-' else "--"
            mid_net = float(fields[3]) if fields[3] != '-' else 0
            mid_net_str = convert_amount_unit(mid_net)
            mid_pct = f"{round(float(fields[8]), 2)}%" if fields[8] != '-' else "--"
            small_net = float(fields[2]) if fields[2] != '-' else 0
            small_net_str = convert_amount_unit(small_net)
            small_pct = f"{round(float(fields[7]), 2)}%" if fields[7] != '-' else "--"
            main_net = super_net + big_net
            main_net_str = convert_amount_unit(main_net)
            main_pct = f"{round(float(fields[6]), 2)}%" if fields[6] != '-' else "--"
            markdown += f"| {date} | {close_price} | {change_pct} | {main_net_str} | {main_pct} | {super_net_str} | {super_pct} | {big_net_str} | {big_pct} | {mid_net_str} | {mid_pct} | {small_net_str} | {small_pct} |\n"
    return markdown


async def get_stock_kline(secid="0.002371", lmt=120):
    """获取股票K线数据"""
    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/kline/get"
    params = {
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "end": "20500101",
        "lmt": str(lmt)
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data") and data["data"].get("klines"):
        return data["data"]["klines"]
    else:
        raise Exception(f"未获取到股票 {secid} 的K线数据")
