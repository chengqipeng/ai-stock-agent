"""
五档盘口数据实时抓取 — 数据源：新浪财经 hq.sinajs.cn

独立封装，不依赖主项目 service 层。
"""

import logging
import re
import aiohttp

logger = logging.getLogger(__name__)

_SINA_HQ_URL = "https://hq.sinajs.cn"
_HEADERS = {
    "Referer": "https://finance.sina.com.cn/",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
}


def _to_sina_symbol(stock_code_normalize: str) -> str:
    """600519.SH → sh600519"""
    code, market = stock_code_normalize.split(".")
    return f"{'sh' if market == 'SH' else 'sz'}{code}"


def _convert_amount(amount_yuan: float) -> str:
    """元 → 自动转换为 亿/万"""
    if abs(amount_yuan) >= 1e8:
        return f"{round(amount_yuan / 1e8, 4)}亿"
    elif abs(amount_yuan) >= 1e4:
        return f"{round(amount_yuan / 1e4, 4)}万"
    return str(amount_yuan)


def _parse_fields(fields: list[str]) -> dict:
    """解析新浪行情字段列表。"""
    def _f(idx):
        try:
            return float(fields[idx])
        except (ValueError, TypeError, IndexError):
            return 0.0

    def _vol(idx):
        try:
            return int(float(fields[idx])) // 100
        except (ValueError, TypeError, IndexError):
            return 0

    trade_date = fields[30].strip() if len(fields) > 30 else ""

    result = {
        "current_price": _f(3),
        "open_price": _f(1),
        "prev_close": _f(2),
        "high_price": _f(4),
        "low_price": _f(5),
        "volume": _vol(8),
        "amount": _convert_amount(_f(9)),
    }
    if trade_date:
        result["_trade_date"] = trade_date

    for i in range(5):
        n = i + 1
        result[f"buy{n}_price"] = _f(11 + i * 2)
        result[f"buy{n}_vol"] = _vol(10 + i * 2)
    for i in range(5):
        n = i + 1
        result[f"sell{n}_price"] = _f(21 + i * 2)
        result[f"sell{n}_vol"] = _vol(20 + i * 2)

    return result


async def fetch_order_book(stock_code_normalize: str) -> dict | None:
    """
    实时抓取五档盘口数据。

    Args:
        stock_code_normalize: 标准化代码，如 "600519.SH" / "000001.SZ"

    Returns:
        dict 包含 current_price, buy1~5, sell1~5 等字段，失败返回 None
    """
    symbol = _to_sina_symbol(stock_code_normalize)
    url = f"{_SINA_HQ_URL}/list={symbol}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                text = await resp.text(encoding="gbk")

        match = re.search(r'"(.+)"', text)
        if not match:
            logger.warning("[%s] 新浪盘口接口返回异常: %s", stock_code_normalize, text[:200])
            return None

        fields = match.group(1).split(",")
        if len(fields) < 32:
            logger.warning("[%s] 新浪盘口字段不足: %d", stock_code_normalize, len(fields))
            return None

        return _parse_fields(fields)

    except Exception as e:
        logger.error("[%s] 获取盘口数据异常: %s", stock_code_normalize, e)
        return None
