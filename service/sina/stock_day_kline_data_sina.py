"""新浪财经日K线数据模块。

通过新浪财经K线数据接口获取股票历史日K线数据。
接口：https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData
参数：
  - symbol: 股票代码，格式 sh600183（上海）/ sz000001（深圳）
  - scale:  K线周期（分钟），5/15/30/60/240（日线）
  - ma:     均线参数，如 "5,10,20,30" 或 "no"
  - datalen: 返回数据条数

返回JSON数组，每条记录包含：day, open, high, low, close, volume
当 ma 不为 "no" 时，额外返回 ma_price{N}, ma_volume{N}

来源页面：https://finance.sina.com.cn/realstock/company/sh600183/nc.shtml
"""

import json
import logging
import aiohttp
import asyncio

from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

_SINA_KLINE_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"

_HEADERS = {
    "Referer": "https://finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
}


def _build_sina_symbol(stock_info: StockInfo) -> str:
    """将 StockInfo 转换为新浪K线接口的股票代码格式（sh600183 / sz000001）"""
    code, market = stock_info.stock_code_normalize.split('.')
    prefix = 'sh' if market == 'SH' else 'sz'
    return f"{prefix}{code}"


async def _fetch_kline_raw(symbol: str, scale: int = 240, ma: str = "no", datalen: int = 400) -> list[dict]:
    """请求新浪K线接口，返回原始JSON列表。

    Args:
        symbol:  新浪格式股票代码，如 sh600183
        scale:   K线周期（分钟）：5, 15, 30, 60, 240（日线）
        ma:      均线参数，"no" 或 "5,10,20,30"
        datalen: 返回数据条数

    Returns:
        原始JSON数组，每条包含 day/open/high/low/close/volume 等字段
    """
    params = {
        "symbol": symbol,
        "scale": str(scale),
        "ma": ma,
        "datalen": str(datalen),
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(_SINA_KLINE_URL, params=params, headers=_HEADERS,
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            text = await resp.text()
            return json.loads(text)


async def get_stock_day_kline_sina(stock_info: StockInfo, limit: int = 400) -> list[dict]:
    """从新浪财经获取日K线数据，返回标准化字典列表（由旧到新）。

    每条记录包含：date, open_price, close_price, high_price, low_price, trading_volume
    与同花顺接口 get_stock_day_kline_10jqka 输出格式对齐。
    """
    symbol = _build_sina_symbol(stock_info)
    raw_list = await _fetch_kline_raw(symbol, scale=240, ma="no", datalen=limit)

    result = []
    prev_close = None
    for item in raw_list:
        open_p = float(item["open"])
        close_p = float(item["close"])
        high_p = float(item["high"])
        low_p = float(item["low"])
        volume = int(item["volume"]) // 100  # 股 -> 手

        amplitude = round((high_p - low_p) / prev_close * 100, 2) if prev_close else None
        change_pct = round((close_p - prev_close) / prev_close * 100, 2) if prev_close else None
        change_amt = round(close_p - prev_close, 2) if prev_close else None

        result.append({
            "date": item["day"],
            "open_price": open_p,
            "close_price": close_p,
            "high_price": high_p,
            "low_price": low_p,
            "trading_volume": volume,
            "trading_amount": None,       # 新浪接口不返回成交额
            "amplitude": amplitude,
            "change_percent": change_pct,
            "change_amount": change_amt,
            "change_hand": None,          # 新浪接口不返回换手率
        })
        prev_close = close_p

    return result


async def get_stock_day_kline_as_str_sina(stock_info: StockInfo, limit: int = 400) -> list[str]:
    """返回逗号分隔字符串列表，格式与 get_stock_day_kline_as_str_10jqka 一致：
    date,open_price,close_price,high_price,low_price,trading_volume,trading_amount,amplitude,change_percent,change_amount,change_hand
    """
    klines = await get_stock_day_kline_sina(stock_info, limit)
    def _fmt(v):
        return "" if v is None else str(v)

    return [
        ','.join([
            k["date"], str(k["open_price"]), str(k["close_price"]), str(k["high_price"]), str(k["low_price"]),
            str(round(k["trading_volume"], 2)),
            _fmt(k.get("trading_amount")),
            _fmt(k.get("amplitude")), _fmt(k.get("change_percent")), _fmt(k.get("change_amount")),
            _fmt(k.get("change_hand")),
        ])
        for k in klines
    ]


async def get_stock_day_kline_cn_sina(stock_info: StockInfo, limit: int = 400) -> list[dict]:
    """获取日K线数据，返回中文key，与 get_stock_day_kline_cn_10jqka 格式一致"""
    klines = await get_stock_day_kline_sina(stock_info, limit)
    return [{
        "日期":       k["date"],
        "开盘价":     k["open_price"],
        "收盘价":     k["close_price"],
        "最高价":     k["high_price"],
        "最低价":     k["low_price"],
        "成交量（手）": k["trading_volume"],
        "成交额":     k.get("trading_amount"),
        "振幅(%)":    k.get("amplitude"),
        "涨跌幅(%)":  k.get("change_percent"),
        "涨跌额":     k.get("change_amount"),
        "换手率(%)":  k.get("change_hand"),
    } for k in klines]


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("生益科技")
        logger.info("=== %s（%s）新浪日K线 ===\n", stock_info.stock_name, stock_info.stock_code_normalize)

        # 标准化格式
        klines = await get_stock_day_kline_sina(stock_info, limit=10)
        logger.info("最近 %d 条日K线：", len(klines))
        for k in klines:
            logger.info("  %s  开:%s  收:%s  高:%s  低:%s  量:%s手  涨跌:%s%%",
                        k['date'], k['open_price'], k['close_price'],
                        k['high_price'], k['low_price'], k['trading_volume'],
                        k['change_percent'])

        # 字符串格式
        logger.info("\n字符串格式：")
        str_klines = await get_stock_day_kline_as_str_sina(stock_info, limit=5)
        for s in str_klines:
            logger.info("  %s", s)

    asyncio.run(main())
