import re
import json
import asyncio
import logging
import aiohttp
from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

_HEADERS = {
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


async def get_stock_time_kline_10jqka(stock_info: StockInfo, limit: int = None, max_retries: int = 3) -> list[dict]:
    """
    从同花顺获取当日分时数据，返回列表（由旧到新）。
    每条记录包含：time, close_price, trading_amount, avg_price, trading_volume, change_percent
    """
    code = stock_info.stock_code
    url = f"https://d.10jqka.com.cn/v6/time/hs_{code}/defer/last.js"

    text = ""
    for attempt in range(1, max_retries + 1):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(url, headers=_HEADERS) as resp:
                    text = await resp.text()
            break
        except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
            if attempt < max_retries:
                wait = 1.5 * attempt
                logger.warning("[%s] 分时数据第%d次请求失败，%.1f秒后重试: %s", code, attempt, wait, e)
                await asyncio.sleep(wait)
            else:
                logger.error("[%s] 分时数据重试%d次后仍失败: %s", code, max_retries, e)
                return []

    json_text = re.sub(r"^\w+\(", "", text)
    json_text = re.sub(r"\);?\s*$", "", json_text)
    data = json.loads(json_text)

    # 响应结构：{"hs_{code}": {"pre": "488.88", "data": "0930,481.12,38975531,481.120,81010;...", ...}}
    inner = data.get(f"hs_{code}", {})
    if not inner:
        logger.error("[%s] 分时数据响应中缺少 hs_%s，data keys=%s", code, code, list(data.keys()))
        return []
    pre_close = float(inner.get("pre", 0) or 0)
    data_str = inner.get("data", "")
    trade_date = inner.get("date", "")  # 分时数据对应的交易日期
    # 格式化日期: "20260311" → "2026-03-11"
    if trade_date and len(trade_date) == 8:
        trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    name = stock_info.stock_name
    if not pre_close:
        logger.error("[%s] 分时数据昨收价为空或为0，inner keys=%s", code, list(inner.keys()))
    if not data_str:
        logger.error("[%s] 分时数据 data 字段为空", code)
        return []

    # ---- 第一遍：解析原始行，空值字段记为 None ----
    _FIELD_KEYS = ("time", "close_price", "trading_amount", "avg_price", "trading_volume")
    raw_rows: list[dict | None] = []
    for row in data_str.split(";"):
        parts = row.split(",")
        if len(parts) < 5:
            raw_rows.append(None)
            continue
        raw_rows.append({
            "time":            parts[0] or None,
            "close_price":     parts[1] or None,
            "trading_amount":  parts[2] or None,
            "avg_price":       parts[3] or None,
            "trading_volume":  parts[4] or None,
        })

    # ---- 第二遍：空值字段用上一条填充，没有上一条则用下一条 ----
    for i, cur in enumerate(raw_rows):
        if cur is None:
            continue
        empty_keys = [k for k in _FIELD_KEYS if cur[k] is None]
        if not empty_keys:
            continue
        # 尝试从上一条有效行取值
        prev = next((raw_rows[j] for j in range(i - 1, -1, -1) if raw_rows[j] is not None), None)
        for k in list(empty_keys):
            if prev and prev[k] is not None:
                cur[k] = prev[k]
                empty_keys.remove(k)
        # 仍有空值则尝试从下一条有效行取值
        if empty_keys:
            nxt = next((raw_rows[j] for j in range(i + 1, len(raw_rows)) if raw_rows[j] is not None), None)
            for k in list(empty_keys):
                if nxt and nxt[k] is not None:
                    cur[k] = nxt[k]
                    empty_keys.remove(k)
        # 填充后仍有空值，标记为丢弃
        if empty_keys:
            logger.warning(
                "[%s %s] 分时数据字段 %s 无法填充，日期=%s，已丢弃",
                code, name, empty_keys, trade_date,
            )
            raw_rows[i] = None

    # ---- 第三遍：转换为最终结果 ----
    result = []
    for r in raw_rows:
        if r is None:
            continue
        price = float(r["close_price"])
        change_pct = round((price - pre_close) / pre_close * 100, 2) if pre_close else None
        result.append({
            "time":            f"{r['time'][:2]}:{r['time'][2:]}",
            "close_price":     price,
            "trading_amount":  float(r["trading_amount"]),
            "avg_price":       float(r["avg_price"]),
            "trading_volume":  int(float(r["trading_volume"])),
            "change_percent":  change_pct,
        })

    final = result if limit is None else result[-limit:]
    # 将API返回的真实交易日期附加到结果上，供调用方使用
    if final and trade_date:
        for item in final:
            item["_trade_date"] = trade_date
    return final


async def get_stock_time_kline_cn_10jqka(stock_info: StockInfo, limit: int = None) -> list[dict]:
    """获取当日分时数据，返回中文key列表"""
    rows = await get_stock_time_kline_10jqka(stock_info, limit)
    return [{
        "时间":   r["time"],
        "价格":   r["close_price"],
        "成交额":  r["trading_amount"],
        "均价":   r["avg_price"],
        "成交量":  r["trading_volume"],
        "涨跌幅":  r["change_percent"],
    } for r in rows]


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("生益电子")
        klines = await get_stock_time_kline_cn_10jqka(stock_info)
        logger.info(json.dumps(klines[:50], ensure_ascii=False))

    asyncio.run(main())
