import aiohttp
import json
import re
from common.utils.amount_utils import convert_amount_unit


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
