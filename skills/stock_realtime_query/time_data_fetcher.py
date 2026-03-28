"""
分时数据实时抓取 — 数据源：同花顺 d.10jqka.com.cn

独立封装，不依赖主项目 service 层。
"""

import asyncio
import json
import logging
import re
import aiohttp

logger = logging.getLogger(__name__)

_HEADERS = {
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/145.0.0.0 Safari/537.36"),
}


async def fetch_time_data(
    stock_code_normalize: str,
    limit: int | None = None,
    max_retries: int = 3,
) -> list[dict]:
    """
    实时抓取当日分时数据。

    Args:
        stock_code_normalize: 标准化代码，如 "600519.SH"
        limit: 只返回最后 N 条（None=全部）
        max_retries: 最大重试次数

    Returns:
        list[dict]，每条包含 time, close_price, trading_amount, avg_price,
        trading_volume, change_percent, _trade_date
    """
    code = stock_code_normalize.split(".")[0]
    url = f"https://d.10jqka.com.cn/v6/time/hs_{code}/defer/last.js"

    text = ""
    for attempt in range(1, max_retries + 1):
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            ) as session:
                async with session.get(url, headers=_HEADERS) as resp:
                    text = await resp.text()
            break
        except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError,
                asyncio.TimeoutError) as e:
            if attempt < max_retries:
                await asyncio.sleep(1.5 * attempt)
            else:
                logger.error("[%s] 分时数据重试%d次后仍失败: %s", code, max_retries, e)
                return []

    # 去掉 JSONP 包裹
    json_text = re.sub(r"^\w+\(", "", text)
    json_text = re.sub(r"\);?\s*$", "", json_text)
    data = json.loads(json_text)

    inner = data.get(f"hs_{code}", {})
    if not inner:
        logger.error("[%s] 分时数据响应中缺少 hs_%s", code, code)
        return []

    pre_close = float(inner.get("pre", 0) or 0)
    data_str = inner.get("data", "")
    trade_date = inner.get("date", "")
    if trade_date and len(trade_date) == 8:
        trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"

    if not data_str:
        logger.error("[%s] 分时数据 data 字段为空", code)
        return []

    # 第一遍：解析原始行
    _KEYS = ("time", "close_price", "trading_amount", "avg_price", "trading_volume")
    raw: list[dict | None] = []
    for row in data_str.split(";"):
        parts = row.split(",")
        if len(parts) < 5:
            raw.append(None)
            continue
        raw.append({
            "time": parts[0] or None,
            "close_price": parts[1] or None,
            "trading_amount": parts[2] or None,
            "avg_price": parts[3] or None,
            "trading_volume": parts[4] or None,
        })

    # 第二遍：空值填充（前后行补齐）
    for i, cur in enumerate(raw):
        if cur is None:
            continue
        empty = [k for k in _KEYS if cur[k] is None]
        if not empty:
            continue
        prev = next((raw[j] for j in range(i - 1, -1, -1) if raw[j] is not None), None)
        for k in list(empty):
            if prev and prev[k] is not None:
                cur[k] = prev[k]
                empty.remove(k)
        if empty:
            nxt = next((raw[j] for j in range(i + 1, len(raw)) if raw[j] is not None), None)
            for k in list(empty):
                if nxt and nxt[k] is not None:
                    cur[k] = nxt[k]
                    empty.remove(k)
        if empty:
            raw[i] = None

    # 第三遍：转换为最终结果
    result = []
    for r in raw:
        if r is None:
            continue
        price = float(r["close_price"])
        chg = round((price - pre_close) / pre_close * 100, 2) if pre_close else None
        item = {
            "time": f"{r['time'][:2]}:{r['time'][2:]}",
            "close_price": price,
            "trading_amount": float(r["trading_amount"]),
            "avg_price": float(r["avg_price"]),
            "trading_volume": int(float(r["trading_volume"])),
            "change_percent": chg,
        }
        if trade_date:
            item["_trade_date"] = trade_date
        result.append(item)

    return result if limit is None else result[-limit:]
