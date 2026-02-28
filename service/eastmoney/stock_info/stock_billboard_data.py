"""
龙虎榜数据接口 - 东方财富
获取个股龙虎榜上榜记录及买卖席位明细
"""
import json
import asyncio

from common.http.http_utils import fetch_eastmoney_api, EASTMONEY_API_URL
from common.utils.amount_utils import convert_amount_unit
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.utils.stock_info_utils import StockInfo


async def get_billboard_records(stock_info: StockInfo, days: int = 30, page_size: int = 50) -> list[dict]:
    """获取个股龙虎榜上榜记录（近N天内的所有上榜日）

    Args:
        stock_info: 股票信息
        days: 查询最近多少天的记录，默认30天
        page_size: 每页数量

    Returns:
        上榜记录列表，按日期倒序
    """
    cache_path = get_cache_path("billboard_records", stock_info.stock_code)
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data

    from datetime import datetime, timedelta
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    params = {
        "sortColumns": "SECURITY_CODE,TRADE_DATE",
        "sortTypes": "1,-1",
        "pageSize": page_size,
        "pageNumber": 1,
        "reportName": "RPT_DAILYBILLBOARD_DETAILSNEW",
        "columns": "SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,"
                   "CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,"
                   "BILLBOARD_SELL_AMT,BILLBOARD_DEAL_AMT,ACCUM_AMOUNT,"
                   "DEAL_NET_RATIO,DEAL_AMOUNT_RATIO,TURNOVERRATE,FREE_MARKET_CAP,"
                   "EXPLANATION,D1_CLOSE_ADJCHRATE,D2_CLOSE_ADJCHRATE,"
                   "D5_CLOSE_ADJCHRATE,D10_CLOSE_ADJCHRATE,SECURITY_TYPE_CODE",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(SECURITY_CODE=\"{stock_info.stock_code}\")"
                  f"(TRADE_DATE>='{start_date}')(TRADE_DATE<='{end_date}')",
    }

    data = await fetch_eastmoney_api(
        EASTMONEY_API_URL,
        params,
        referer="https://data.eastmoney.com/stock/tradedetail.html",
    )

    records = []
    if data.get("result") and data["result"].get("data"):
        records = data["result"]["data"]
        save_cache(cache_path, records)

    return records


async def get_billboard_buy_seats(stock_info: StockInfo, trade_date: str, page_size: int = 50) -> list[dict]:
    """获取龙虎榜某日买入席位明细

    Args:
        stock_info: 股票信息
        trade_date: 交易日期 YYYY-MM-DD
        page_size: 每页数量

    Returns:
        买入席位列表
    """
    cache_key = f"billboard_buy_{trade_date}"
    cache_path = get_cache_path(cache_key, stock_info.stock_code)
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data

    params = {
        "reportName": "RPT_BILLBOARD_DAILYDETAILSBUY",
        "columns": "ALL",
        "filter": f"(TRADE_DATE='{trade_date}')(SECURITY_CODE=\"{stock_info.stock_code}\")",
        "pageNumber": 1,
        "pageSize": page_size,
        "sortTypes": "-1",
        "sortColumns": "BUY",
        "source": "WEB",
        "client": "WEB",
    }

    data = await fetch_eastmoney_api(
        EASTMONEY_API_URL,
        params,
        referer=f"https://data.eastmoney.com/stock/lhb/{stock_info.stock_code}.html",
    )

    records = []
    if data.get("result") and data["result"].get("data"):
        records = data["result"]["data"]
        save_cache(cache_path, records)

    return records


async def get_billboard_sell_seats(stock_info: StockInfo, trade_date: str, page_size: int = 50) -> list[dict]:
    """获取龙虎榜某日卖出席位明细

    Args:
        stock_info: 股票信息
        trade_date: 交易日期 YYYY-MM-DD
        page_size: 每页数量

    Returns:
        卖出席位列表
    """
    cache_key = f"billboard_sell_{trade_date}"
    cache_path = get_cache_path(cache_key, stock_info.stock_code)
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data

    params = {
        "reportName": "RPT_BILLBOARD_DAILYDETAILSSELL",
        "columns": "ALL",
        "filter": f"(TRADE_DATE='{trade_date}')(SECURITY_CODE=\"{stock_info.stock_code}\")",
        "pageNumber": 1,
        "pageSize": page_size,
        "sortTypes": "-1",
        "sortColumns": "SELL",
        "source": "WEB",
        "client": "WEB",
    }

    data = await fetch_eastmoney_api(
        EASTMONEY_API_URL,
        params,
        referer=f"https://data.eastmoney.com/stock/lhb/{stock_info.stock_code}.html",
    )

    records = []
    if data.get("result") and data["result"].get("data"):
        records = data["result"]["data"]
        save_cache(cache_path, records)

    return records


async def get_billboard_json(stock_info: StockInfo, days: int = 30) -> list[dict]:
    """获取龙虎榜完整数据（上榜记录 + 买卖席位明细），转为中文JSON

    Args:
        stock_info: 股票信息
        days: 查询最近多少天

    Returns:
        结构化的龙虎榜数据列表，每条包含上榜概况和买卖席位
    """
    records = await get_billboard_records(stock_info, days)
    if not records:
        return []

    result = []
    for rec in records:
        trade_date_raw = rec.get("TRADE_DATE", "")
        trade_date = trade_date_raw[:10] if trade_date_raw else ""
        if not trade_date:
            continue

        # 并发获取买卖席位
        buy_seats, sell_seats = await asyncio.gather(
            get_billboard_buy_seats(stock_info, trade_date),
            get_billboard_sell_seats(stock_info, trade_date),
        )

        # 格式化上榜概况
        entry = {
            "上榜日期": trade_date,
            "上榜原因": rec.get("EXPLANATION", rec.get("EXPLAIN", "")),
            "收盘价": rec.get("CLOSE_PRICE"),
            "涨跌幅(%)": round(rec.get("CHANGE_RATE", 0) or 0, 2),
            "龙虎榜净买额": convert_amount_unit(rec.get("BILLBOARD_NET_AMT")),
            "龙虎榜买入额": convert_amount_unit(rec.get("BILLBOARD_BUY_AMT")),
            "龙虎榜卖出额": convert_amount_unit(rec.get("BILLBOARD_SELL_AMT")),
            "龙虎榜成交额": convert_amount_unit(rec.get("BILLBOARD_DEAL_AMT")),
            "当日总成交额": convert_amount_unit(rec.get("ACCUM_AMOUNT")),
            "龙虎榜净买占比(%)": round(rec.get("DEAL_NET_RATIO", 0) or 0, 2),
            "龙虎榜成交占比(%)": round(rec.get("DEAL_AMOUNT_RATIO", 0) or 0, 2),
            "换手率(%)": round(rec.get("TURNOVERRATE", 0) or 0, 2),
            "流通市值": convert_amount_unit(rec.get("FREE_MARKET_CAP")),
            "次日涨跌(%)": round(rec.get("D1_CLOSE_ADJCHRATE", 0) or 0, 2) if rec.get("D1_CLOSE_ADJCHRATE") is not None else None,
            "2日涨跌(%)": round(rec.get("D2_CLOSE_ADJCHRATE", 0) or 0, 2) if rec.get("D2_CLOSE_ADJCHRATE") is not None else None,
            "5日涨跌(%)": round(rec.get("D5_CLOSE_ADJCHRATE", 0) or 0, 2) if rec.get("D5_CLOSE_ADJCHRATE") is not None else None,
            "10日涨跌(%)": round(rec.get("D10_CLOSE_ADJCHRATE", 0) or 0, 2) if rec.get("D10_CLOSE_ADJCHRATE") is not None else None,
        }

        # 格式化买入席位
        buy_list = []
        for s in buy_seats:
            buy_list.append({
                "席位名称": s.get("OPERATEDEPT_NAME", ""),
                "买入额": convert_amount_unit(s.get("BUY")),
                "卖出额": convert_amount_unit(s.get("SELL")),
                "净额": convert_amount_unit(s.get("NET")),
                "类型": _seat_type(s.get("OPERATEDEPT_NAME", "")),
            })
        entry["买入席位"] = buy_list

        # 格式化卖出席位
        sell_list = []
        for s in sell_seats:
            sell_list.append({
                "席位名称": s.get("OPERATEDEPT_NAME", ""),
                "买入额": convert_amount_unit(s.get("BUY")),
                "卖出额": convert_amount_unit(s.get("SELL")),
                "净额": convert_amount_unit(s.get("NET")),
                "类型": _seat_type(s.get("OPERATEDEPT_NAME", "")),
            })
        entry["卖出席位"] = sell_list

        result.append(entry)

    return result


def _seat_type(name: str) -> str:
    """根据席位名称判断类型：机构专用 / 沪股通/深股通 / 游资"""
    if not name:
        return "未知"
    if "机构专用" in name:
        return "机构"
    if "沪股通" in name or "深股通" in name or "港股通" in name:
        return "北向资金"
    return "游资"


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("生益科技")
        print(f"=== {stock_info.stock_name}（{stock_info.stock_code_normalize}）龙虎榜数据 ===\n")

        # 1. 测试上榜记录
        print("【1】近30日上榜记录：")
        records = await get_billboard_records(stock_info, days=30)
        if records:
            for r in records:
                date = r.get("TRADE_DATE", "")[:10]
                reason = r.get("EXPLANATION", r.get("EXPLAIN", ""))
                change = r.get("CHANGE_RATE", 0) or 0
                net = r.get("BILLBOARD_NET_AMT", 0) or 0
                print(f"  {date} | 涨跌幅:{change:+.2f}% | 净买额:{convert_amount_unit(net)} | 原因:{reason}")
        else:
            print("  近30日无龙虎榜记录")

        print()

        # 2. 测试完整数据（含席位）
        print("【2】完整龙虎榜数据（含买卖席位）：")
        full_data = await get_billboard_json(stock_info, days=30)
        if full_data:
            print(json.dumps(full_data, ensure_ascii=False, indent=2))
        else:
            print("  近30日无龙虎榜记录")

    asyncio.run(main())
