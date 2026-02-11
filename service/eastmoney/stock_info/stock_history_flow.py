import asyncio

from common.utils.amount_utils import convert_amount_unit
from common.http.http_utils import fetch_eastmoney_api, EASTMONEY_PUSH2HIS_API_URL

async def get_fund_flow_history(secid="0.002371"):
    """获取资金流向历史数据"""
    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/fflow/daykline/get"
    params = {
        "lmt": "150",
        "klt": "101",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "secid": secid,
        "_": 1715330901
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data") and data["data"].get("klines"):
        klines = data["data"]["klines"]
        klines.reverse()
        return klines
    else:
        raise Exception(f"未获取到股票 {secid} 的资金流向历史数据")

async def get_fund_flow_history_markdown(secid="0.002371", stock_code=None, stock_name=None, page_size = 120):
    """获取资金流向历史数据并转换为markdown"""
    klines = await get_fund_flow_history(secid)
    kline_max_min_map = await get_stock_history_kline_max_min(secid)
    if not stock_code:
        stock_code = secid.split('.')[-1]
    header = f"## <{stock_code} {stock_name}> - 历史资金流向" if stock_name else "## 历史资金流向"
    markdown = f"""{header}
| 日期 | 收盘价 | 涨跌幅 | 主力净流入净额 | 主力净流入净占比 | 超大单净流入净额 | 超大单净流入净占比 | 大单净流入净额 | 大单净流入净占比 | 中单净流入净额 | 中单净流入占比 | 小单净流入净额 | 小单净流入净占比 | 当日最高价 | 当日最低价 |
|-----|-------|-------|--------------|---------------|----------------|-----------------|-------------|----------------|-------------|--------------|--------------|---------------|----------|--------------|
"""
    for kline in klines[:page_size]:
        fields = kline.split(',')
        if len(fields) >= 15:
            date = fields[0]
            kline_max_min_item = kline_max_min_map[date]
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
            markdown += f"| {date} | {close_price} | {change_pct} | {main_net_str} | {main_pct} | {super_net_str} | {super_pct} | {big_net_str} | {big_pct} | {mid_net_str} | {mid_pct} | {small_net_str} | {small_pct} | {kline_max_min_item['high_price']} | {kline_max_min_item['low_price']} |\n"
    return markdown + "\n"


async def get_stock_history_kline_max_min(secid="0.002371"):
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
        "smplmt": "460",
        "lmt": "130"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data") and data["data"].get("klines"):
        klines = data["data"]["klines"]
        result = {}
        for kline in klines:
            fields = kline.split(',')
            date = fields[0]
            high_price = float(fields[2])
            low_price = float(fields[3])
            result[date] = {"high_price": high_price, "low_price": low_price}
        return result
    else:
        raise Exception(f"未获取到股票 {secid} 的K线数据")


if __name__ == "__main__":
    asyncio.run(get_fund_flow_history_markdown(secid="0.002371", stock_code="002371", stock_name="北方华创"))

