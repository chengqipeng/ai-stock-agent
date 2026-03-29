"""
新浪财经数据接口 —— 用于交叉验证

提供以下数据的新浪财经版本，与数据库中已有数据进行对比：
1. 日K线数据（近30天）
2. 财报数据（最新季度）
3. 最高最低价（近30天）
4. 分时数据（当天）
5. 盘口数据（当天）
6. 资金流向（近30天）
"""
import asyncio
import json
import logging
import re
from datetime import datetime, date, timedelta

import aiohttp

from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

_HEADERS = {
    "Referer": "https://finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}

_SINA_KLINE_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
_SINA_HQ_URL = "https://hq.sinajs.cn"


def _build_sina_symbol(stock_info: StockInfo) -> str:
    code, market = stock_info.stock_code_normalize.split('.')
    prefix = 'sh' if market == 'SH' else 'sz'
    return f"{prefix}{code}"


# ─────────── 1. 日K线（近30天） ───────────

async def fetch_sina_kline(stock_info: StockInfo, limit: int = 60) -> list[dict]:
    """从新浪获取日K线数据，返回标准化字典列表"""
    symbol = _build_sina_symbol(stock_info)
    params = {"symbol": symbol, "scale": "240", "ma": "no", "datalen": str(limit)}
    async with aiohttp.ClientSession() as session:
        async with session.get(_SINA_KLINE_URL, params=params, headers=_HEADERS,
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
            text = await resp.text()
            if not text or not text.strip():
                logger.debug("[sina_kline] %s 返回空响应", symbol)
                return []
            try:
                raw_list = json.loads(text)
            except json.JSONDecodeError:
                logger.debug("[sina_kline] %s JSON解析失败, 响应前100字符: %s", symbol, text[:100])
                return []

    result = []
    prev_close = None
    for item in raw_list:
        o, c, h, l_ = float(item["open"]), float(item["close"]), float(item["high"]), float(item["low"])
        vol = int(item["volume"]) // 100  # 股→手
        change_pct = round((c - prev_close) / prev_close * 100, 2) if prev_close else None
        result.append({
            "date": item["day"],
            "open_price": o, "close_price": c, "high_price": h, "low_price": l_,
            "trading_volume": vol, "change_percent": change_pct,
        })
        prev_close = c
    return result


# ─────────── 2. 实时行情（盘口 + 当日OHLCV） ───────────

async def fetch_sina_realtime(stock_info: StockInfo) -> dict:
    """从新浪实时行情接口获取当日数据（含盘口五档）"""
    symbol = _build_sina_symbol(stock_info)
    url = f"{_SINA_HQ_URL}/list={symbol}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_HEADERS,
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            text = await resp.text(encoding='gbk')

    match = re.search(r'"(.+)"', text)
    if not match:
        return {}
    f = match.group(1).split(',')
    if len(f) < 32:
        return {}

    def _f(i):
        try: return float(f[i])
        except: return 0.0
    def _v(i):
        try: return int(float(f[i])) // 100
        except: return 0

    return {
        "name": f[0],
        "open_price": _f(1), "prev_close": _f(2), "current_price": _f(3),
        "high_price": _f(4), "low_price": _f(5),
        "volume": _v(8), "amount": _f(9),
        "buy1_vol": _v(10), "buy1_price": _f(11),
        "buy2_vol": _v(12), "buy2_price": _f(13),
        "buy3_vol": _v(14), "buy3_price": _f(15),
        "buy4_vol": _v(16), "buy4_price": _f(17),
        "buy5_vol": _v(18), "buy5_price": _f(19),
        "sell1_vol": _v(20), "sell1_price": _f(21),
        "sell2_vol": _v(22), "sell2_price": _f(23),
        "sell3_vol": _v(24), "sell3_price": _f(25),
        "sell4_vol": _v(26), "sell4_price": _f(27),
        "sell5_vol": _v(28), "sell5_price": _f(29),
        "date": f[30] if len(f) > 30 else "",
        "time": f[31] if len(f) > 31 else "",
    }


# ─────────── 3. 批量实时行情 ───────────

async def fetch_sina_realtime_batch(stock_infos: list[StockInfo]) -> dict[str, dict]:
    """批量获取新浪实时行情（一次请求最多获取多只股票）"""
    if not stock_infos:
        return {}

    symbols = [_build_sina_symbol(si) for si in stock_infos]
    url = f"{_SINA_HQ_URL}/list={','.join(symbols)}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_HEADERS,
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
            text = await resp.text(encoding='gbk')

    result = {}
    for line in text.strip().split('\n'):
        m = re.match(r'var hq_str_(\w+)="(.+)"', line.strip())
        if not m:
            continue
        sym = m.group(1)
        f = m.group(2).split(',')
        if len(f) < 32:
            continue

        # 从 symbol 反推 stock_code_normalize
        if sym.startswith('sh'):
            code_norm = sym[2:] + '.SH'
        else:
            code_norm = sym[2:] + '.SZ'

        def _f(i):
            try: return float(f[i])
            except: return 0.0
        def _v(i):
            try: return int(float(f[i])) // 100
            except: return 0

        result[code_norm] = {
            "name": f[0],
            "open_price": _f(1), "prev_close": _f(2), "current_price": _f(3),
            "high_price": _f(4), "low_price": _f(5),
            "volume": _v(8), "amount": _f(9),
            "buy1_price": _f(11), "buy1_vol": _v(10),
            "sell1_price": _f(21), "sell1_vol": _v(20),
            "date": f[30] if len(f) > 30 else "",
        }
    return result
