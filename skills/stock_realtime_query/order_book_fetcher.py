"""
五档盘口数据实时抓取 — 数据源：腾讯财经 qt.gtimg.cn

独立封装，不依赖主项目 service 层。
包含五档买卖盘、外盘、内盘数据。
"""

import logging
import aiohttp

logger = logging.getLogger(__name__)

_QT_URL = "https://qt.gtimg.cn/q="
_HEADERS = {
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/145.0.0.0 Safari/537.36"),
}


def _to_qt_symbol(stock_code_normalize: str) -> str:
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
    """
    解析腾讯行情接口字段列表（以 ~ 分隔）。

    字段索引：
    0: 未知, 1: 名称, 2: 代码, 3: 当前价, 4: 昨收, 5: 今开
    6: 成交量(手), 7: 外盘(手), 8: 内盘(手)
    9,10: 买一价,量  11,12: 买二  13,14: 买三  15,16: 买四  17,18: 买五
    19,20: 卖一价,量  21,22: 卖二  23,24: 卖三  25,26: 卖四  27,28: 卖五
    30: 时间(YYYYMMDDHHmmss), 33: 最高, 34: 最低, 37: 成交额(万)
    """
    def _f(idx):
        try:
            return float(fields[idx])
        except (ValueError, TypeError, IndexError):
            return 0.0

    def _i(idx):
        try:
            return int(float(fields[idx]))
        except (ValueError, TypeError, IndexError):
            return 0

    trade_date = ""
    if len(fields) > 30 and fields[30]:
        dt_str = fields[30].strip()
        if len(dt_str) >= 8:
            trade_date = f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]}"

    amount_yuan = _f(37) * 10000  # 万 → 元

    result = {
        "current_price": _f(3),
        "open_price": _f(5),
        "prev_close": _f(4),
        "high_price": _f(33),
        "low_price": _f(34),
        "volume": _i(6),
        "amount": _convert_amount(amount_yuan),
        "outer_vol": _i(7),
        "inner_vol": _i(8),
    }
    if trade_date:
        result["_trade_date"] = trade_date

    for i in range(5):
        n = i + 1
        result[f"buy{n}_price"] = _f(9 + i * 2)
        result[f"buy{n}_vol"] = _i(10 + i * 2)
    for i in range(5):
        n = i + 1
        result[f"sell{n}_price"] = _f(19 + i * 2)
        result[f"sell{n}_vol"] = _i(20 + i * 2)

    return result


async def fetch_order_book(stock_code_normalize: str) -> dict | None:
    """
    实时抓取五档盘口数据（含外盘、内盘）。

    Args:
        stock_code_normalize: 标准化代码，如 "600519.SH" / "000001.SZ"

    Returns:
        dict 包含 current_price, buy1~5, sell1~5, outer_vol, inner_vol 等字段，
        失败返回 None
    """
    symbol = _to_qt_symbol(stock_code_normalize)
    url = f"{_QT_URL}{symbol}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                text = await resp.text(encoding="gbk")

        if '="' not in text:
            logger.warning("[%s] 腾讯盘口接口返回异常: %s", stock_code_normalize, text[:200])
            return None

        content = text.split('="')[1].rstrip('";').rstrip('"')
        fields = content.split("~")
        if len(fields) < 35:
            logger.warning("[%s] 腾讯盘口字段不足: %d", stock_code_normalize, len(fields))
            return None

        return _parse_fields(fields)

    except Exception as e:
        logger.error("[%s] 获取盘口数据异常: %s", stock_code_normalize, e)
        return None
