"""融资融券数据模块。

通过东方财富API获取个股融资融券余额数据。
接口：datacenter-web.eastmoney.com/api/data/v1/get
报表：RPTA_WEB_RZRQ_GGMX（个股融资融券明细）
"""

import logging
import time
import random

from common.http.http_utils import fetch_eastmoney_api, EASTMONEY_API_URL
from common.utils.amount_utils import convert_amount_unit
from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

# ── 缓存（2小时TTL） ──
_margin_cache: dict[str, dict] = {}
_CACHE_TTL = 7200


async def get_margin_trading_data(stock_info: StockInfo, page_size: int = 50) -> list[dict]:
    """获取个股融资融券余额明细数据。

    Args:
        stock_info: 股票信息
        page_size: 返回数据条数（按日期降序），默认50条

    Returns:
        list[dict]: 融资融券逐日数据，中文key，按日期降序
    """
    cache_key = f"{stock_info.stock_code}_{page_size}"
    now = time.time()

    cached = _margin_cache.get(cache_key)
    if cached and (now - cached['ts']) < _CACHE_TTL:
        logger.info("命中融资融券缓存 [%s]", stock_info.stock_name)
        return cached['data']

    # 使用scode过滤（纯数字代码，如600183），与东方财富网页端一致
    callback_name = f"datatable{random.randint(1000000, 9999999)}"
    params = {
        "callback": callback_name,
        "reportName": "RPTA_WEB_RZRQ_GGMX",
        "columns": "ALL",
        "source": "WEB",
        "sortColumns": "date",
        "sortTypes": "-1",
        "pageNumber": "1",
        "pageSize": str(page_size),
        "filter": f'(scode="{stock_info.stock_code}")',
        "pageNo": "1",
    }

    try:
        data = await fetch_eastmoney_api(
            EASTMONEY_API_URL, params,
            referer=f"https://data.eastmoney.com/rzrq/stock/{stock_info.stock_code}.html",
        )

        raw_items = []
        if data.get("result") and data["result"].get("data"):
            raw_items = data["result"]["data"]
        elif data.get("data") and isinstance(data["data"], list):
            raw_items = data["data"]

        field_map = {
            "DATE": "交易日期",
            "SCODE": "股票代码",
            "SECNAME": "股票名称",
            "RZYE": "融资余额(元)",
            "RZMRE": "融资买入额(元)",
            "RZCHE": "融资偿还额(元)",
            "RZJME": "融资净买入(元)",
            "RQYE": "融券余额(元)",
            "RQYL": "融券余量(股)",
            "RQMCL": "融券卖出量(股)",
            "RQCHL": "融券偿还量(股)",
            "RQJMG": "融券净卖出(股)",
            "RZRQYE": "融资融券余额(元)",
            "RZRQYECZ": "融资融券余额差值(元)",
        }

        items = []
        for raw in raw_items:
            item = {}
            for en_key, cn_key in field_map.items():
                val = raw.get(en_key)
                if en_key == "DATE" and val:
                    val = str(val)[:10]
                item[cn_key] = val
            items.append(item)

        _margin_cache[cache_key] = {'data': items, 'ts': now}
        logger.info("获取融资融券数据成功 [%s]，共%d条", stock_info.stock_name, len(items))
        return items

    except Exception as e:
        logger.warning("获取融资融券数据失败 [%s]: %s", stock_info.stock_name, e)
        if cached:
            return cached['data']
        return []


async def get_margin_trading_json(stock_info: StockInfo, page_size: int = 50) -> list[dict]:
    """获取融资融券数据并格式化金额为可读单位。

    Args:
        stock_info: 股票信息
        page_size: 返回数据条数

    Returns:
        list[dict]: 格式化后的融资融券数据
    """
    items = await get_margin_trading_data(stock_info, page_size)
    if not items:
        return []

    amount_keys = [
        "融资余额(元)", "融资买入额(元)", "融资偿还额(元)", "融资净买入(元)",
        "融券余额(元)", "融资融券余额(元)", "融资融券余额差值(元)",
    ]

    result = []
    for item in items:
        formatted = {}
        for k, v in item.items():
            if k in amount_keys and v is not None:
                formatted[k] = convert_amount_unit(v)
            else:
                formatted[k] = v
        result.append(formatted)

    return result


if __name__ == "__main__":
    import asyncio
    import json

    async def main():
        # 测试600183（生益科技），对比 https://data.eastmoney.com/rzrq/stock/600183.html
        stock_info = StockInfo(
            secid="1.600183",
            stock_code="600183",
            stock_code_normalize="600183.SH",
            stock_name="生益科技",
        )

        logger.info("=== %s（%s）融资融券数据 ===", stock_info.stock_name, stock_info.stock_code)
        logger.info("对比页面: https://data.eastmoney.com/rzrq/stock/600183.html\n")

        # 获取原始数据（未格式化），用于精确数值对比
        raw_data = await get_margin_trading_data(stock_info, page_size=5)
        if not raw_data:
            logger.info("未获取到数据")
            return

        # 打印表头
        logger.info("%s %s %s %s %s %s %s %s %s",
                    '交易日期'.ljust(12), '融资余额'.rjust(16), '融资买入额'.rjust(14), '融资偿还额'.rjust(14),
                    '融资净买入'.rjust(14), '融券余量(股)'.rjust(12), '融券卖出量'.rjust(10), '融券偿还量'.rjust(10),
                    '融资融券余额'.rjust(16))
        logger.info("-" * 140)

        for item in raw_data:
            rzye = item.get("融资余额(元)")
            rzmre = item.get("融资买入额(元)")
            rzche = item.get("融资偿还额(元)")
            rzjme = item.get("融资净买入(元)")
            rqyl = item.get("融券余量(股)")
            rqmcl = item.get("融券卖出量(股)")
            rqchl = item.get("融券偿还量(股)")
            rzrqye = item.get("融资融券余额(元)")

            def _fmt(v, width=14):
                s = convert_amount_unit(v) if v is not None else "--"
                return str(s).rjust(width)

            logger.info("%s %s %s %s %s %s %s %s %s",
                        str(item.get('交易日期', '') or '').ljust(12),
                        _fmt(rzye, 16),
                        _fmt(rzmre, 14),
                        _fmt(rzche, 14),
                        _fmt(rzjme, 14),
                        str(rqyl if rqyl is not None else '--').rjust(12),
                        str(rqmcl if rqmcl is not None else '--').rjust(10),
                        str(rqchl if rqchl is not None else '--').rjust(10),
                        _fmt(rzrqye, 16))

        logger.info("共获取 %d 条记录", len(raw_data))
        logger.info("\n请对比以上数据与东方财富网页 https://data.eastmoney.com/rzrq/stock/600183.html 中的表格是否一致")
        logger.info("重点核对：交易日期、融资余额、融资买入额、融券余量、融资融券余额\n")

        # 同时输出JSON格式方便详细对比
        formatted_data = await get_margin_trading_json(stock_info, page_size=5)
        logger.info("=== JSON格式（前5条） ===")
        logger.info(json.dumps(formatted_data, ensure_ascii=False, indent=2))

    asyncio.run(main())
