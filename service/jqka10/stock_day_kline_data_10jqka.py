import re
import json
import asyncio
import logging
import aiohttp
from datetime import date, timedelta
from chinese_calendar import is_workday
from common.utils.stock_info_utils import StockInfo
from common.constants.stocks_data import INDEX_CODES_FULL

def _jqka_symbol(stock_code_normalize: str) -> str:
    """
    根据标准化代码返回同花顺 v6/line 接口使用的标识符。
    规则：
      - 深圳 399xxx 指数                      → zs_{code}
      - 深圳其他指数 (899xxx/93xxxx 等)        → 120_{code}
      - 上证指数 000001.SH                    → 16_1A0001
      - 其他上海指数 (000xxx.SH)               → 16_1B{后4位}
      - 普通股票                               → hs_{code}
    """
    code = stock_code_normalize.split(".")[0]
    if stock_code_normalize in INDEX_CODES_FULL:
        suffix = stock_code_normalize.split(".")[-1]
        if suffix == "SZ":
            if code.startswith("399"):
                return f"zs_{code}"
            return f"120_{code}"
        # 上海指数：16_1A{后4位} 或 16_1B{后4位}
        short = code[2:]  # 000300 → 0300, 000001 → 0001
        if code == "000001":
            return f"16_1A{short}"
        return f"16_1B{short}"
    return f"hs_{code}"

logger = logging.getLogger(__name__)


# --------------- 数据校验 ---------------
_DAY_KLINE_REQUIRED_FIELDS = ("date", "open_price", "close_price", "high_price", "low_price", "trading_volume")


def _validate_raw_response(data: dict, stock_code: str, label: str = "日K") -> bool:
    """校验同花顺原始响应是否包含必要字段，异常时记录错误日志"""
    missing = [f for f in ("priceFactor", "sortYear", "dates", "price", "volumn") if not data.get(f)]
    if missing:
        logger.error("[%s] %s 原始响应缺少关键字段 %s，data keys=%s", stock_code, label, missing, list(data.keys()))
        return False
    return True


def _validate_kline_record(record: dict, stock_code: str, label: str = "日K") -> bool:
    """校验单条K线记录的关键字段是否为空或 None"""
    empty_fields = [f for f in _DAY_KLINE_REQUIRED_FIELDS if record.get(f) is None or record.get(f) == ""]
    if empty_fields:
        logger.error("[%s] %s 数据异常：日期=%s 存在空值字段 %s，record=%s",
                     stock_code, label, record.get("date", "N/A"), empty_fields, record)
        return False
    return True

def _fix_close_price_boundary(record: dict, stock_code: str) -> None:
    """
    修正收盘价越界：当 close_price 略微超出 high/low 范围且误差不超过 1% 时，
    将 close_price 钳位到 high_price 或 low_price。
    例如：close=3476.931 > high=3476.93 → close=high
         close=2678.66  < low=2685.04  → close=low
    """
    close = record.get("close_price")
    high = record.get("high_price")
    low = record.get("low_price")
    if close is None or high is None or low is None:
        return

    if close > high and high != 0:
        deviation = abs(close - high) / high
        if deviation <= 0.01:
            logger.debug("[%s] %s close_price %.4f > high_price %.4f (偏差%.4f%%)，修正为 high_price",
                         stock_code, record.get("date", ""), close, high, deviation * 100)
            record["close_price"] = high

    if close < low and low != 0:
        deviation = abs(low - close) / low
        if deviation <= 0.01:
            logger.debug("[%s] %s close_price %.4f < low_price %.4f (偏差%.4f%%)，修正为 low_price",
                         stock_code, record.get("date", ""), close, low, deviation * 100)
            record["close_price"] = low




_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


def _build_dates(start: str, sort_year: list, dates_str: str) -> list[str]:
    """将 sortYear + dates 还原为完整日期列表 YYYY-MM-DD"""
    mmdd_list = dates_str.split(",")
    result = []
    idx = 0
    for year, count in sort_year:
        for _ in range(count):
            if idx >= len(mmdd_list):
                break
            mmdd = mmdd_list[idx]
            # 同花顺周K的 dates 可能是 "MM-DD" 格式（含连字符），日K是 "MMDD"
            if "-" in mmdd:
                result.append(f"{year}-{mmdd}")
            else:
                result.append(f"{year}-{mmdd[:2]}-{mmdd[2:]}")
            idx += 1
    return result


def _decode_prices(price_str: str, price_factor: int) -> list[tuple]:
    """
    解码同花顺价格数据，每4个数字一组：
      chunk = [prev_close*pf, (open-prev_close)*pf, (high-prev_close)*pf, (open-low)*pf]
    对齐东方财富价格体系：
      open  = (chunk[0]+chunk[1]) / pf
      close = 由last.js覆盖，占位用(chunk[0]+chunk[1]-chunk[3])/pf
      high  = (chunk[0]+chunk[2]) / pf
      low   =  chunk[0] / pf
    """
    nums = list(map(int, price_str.split(",")))
    records = []
    for i in range(0, len(nums), 4):
        chunk = nums[i:i + 4]
        if len(chunk) < 4:
            break
        prev    = chunk[0]
        open_i  = prev + chunk[1]
        high_i  = prev + chunk[2]
        close_i = open_i - chunk[3]  # 占位，会被last.js覆盖
        records.append((
            round(open_i  / price_factor, 2),
            round(close_i / price_factor, 2),
            round(high_i  / price_factor, 2),
            round(prev    / price_factor, 2),
        ))
    return records


async def _fetch_raw(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_HEADERS) as resp:
            status = resp.status
            text = await resp.text()
    if status != 200:
        if status == 404:
            logger.debug("[_fetch_raw] HTTP 404, url=%s", url)
        else:
            logger.warning("[_fetch_raw] HTTP %d, url=%s, body=%s", status, url, text[:500])
        raise aiohttp.ClientResponseError(
            request_info=None, history=None,
            status=status, message=f"HTTP {status}: {text[:200]}"
        )
    if not text or not text.strip():
        logger.warning("[_fetch_raw] 空响应, url=%s", url)
        raise ValueError(f"接口返回空响应: {url}")
    json_text = re.sub(r"^\w+\(", "", text)
    json_text = re.sub(r"\);?\s*$", "", json_text)
    if not json_text.strip():
        logger.warning("[_fetch_raw] JSONP解包后为空, url=%s, raw=%s", url, text[:500])
        raise ValueError(f"JSONP解包后为空: {url}")
    try:
        return json.loads(json_text, strict=False)
    except json.JSONDecodeError as e:
        logger.warning("[_fetch_raw] JSON解析失败, url=%s, error=%s, body=%s", url, e, text[:500])
        raise


def _build_nofq_map(year_data_list: list[dict]) -> dict[str, dict]:
    """从年份分段不复权数据构建 {YYYYMMDD: {close, amount, turnover}} 映射"""
    result = {}
    for year_data in year_data_list:
        for row in year_data.get("data", "").strip().split(";"):
            parts = row.split(",")
            if len(parts) >= 8 and parts[4]:
                result[parts[0]] = {
                    "close":    float(parts[4]),
                    "amount":   float(parts[6]) if parts[6] else None,
                    "turnover": float(parts[7]) if parts[7] else None,
                }
    return result


def _latest_trading_day() -> date:
    """返回最近一个交易日（含今天）"""
    d = date.today()
    while d.weekday() >= 5 or not is_workday(d):
        d -= timedelta(days=1)
    return d


async def _get_today_kline(stock_code_normalize: str) -> dict | None:
    """从实时数据获取最近交易日K线，若数据不完整则返回 None"""
    symbol = _jqka_symbol(stock_code_normalize)
    stock_code = stock_code_normalize.split(".")[0]
    try:
        url_today = f"https://d.10jqka.com.cn/v6/line/{symbol}/01/defer/today.js"
        url_prev  = f"https://d.10jqka.com.cn/v6/time/{symbol}/last.js"

        async with aiohttp.ClientSession(headers=_HEADERS) as session:
            resp_today, resp_prev = await asyncio.gather(
                session.get(url_today),
                session.get(url_prev),
            )
            text_today = await resp_today.text()
            text_prev  = await resp_prev.text()

        # 解析 today
        match = re.search(r"\((\{.*\})\)", text_today, re.DOTALL)
        if not match:
            return None
        today_data = json.loads(match.group(1))
        item = today_data.get(symbol, {})

        # 解析 prev_close
        prev_close = None
        match_prev = re.search(r"\((.+)\)", text_prev, re.DOTALL)
        if match_prev:
            prev_data = json.loads(match_prev.group(1))
            pre = prev_data.get(symbol, {}).get("pre")
            prev_close = float(pre) if pre else None

        trade_date = item.get("1", "")
        if len(trade_date) == 8:
            trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
        close_p = item.get("11")
        if not trade_date or not close_p:
            return None
        close_p = float(close_p)
        volume = int(item.get("13", 0)) // 100  # 股 -> 手
        amplitude    = round((float(item.get("8", close_p)) - float(item.get("9", close_p))) / prev_close * 100, 2) if prev_close else None
        change_pct   = round((close_p - prev_close) / prev_close * 100, 2) if prev_close else None
        change_amt   = round(close_p - prev_close, 2) if prev_close else None
        return {
            "date":           trade_date,
            "open_price":     float(item.get("7", close_p)),
            "close_price":    close_p,
            "high_price":     float(item.get("8", close_p)),
            "low_price":      float(item.get("9", close_p)),
            "trading_volume": volume,
            "trading_amount": float(item["19"]) if item.get("19") else None,
            "amplitude":      amplitude,
            "change_percent": change_pct,
            "change_amount":  change_amt,
            "change_hand":    float(item["1968584"]) if item.get("1968584") else None,
        }
    except Exception as e:
        logging.getLogger(__name__).exception("_get_today_kline failed for %s: %s", stock_code, e)
        return None


async def get_stock_day_kline_10jqka(stock_info: StockInfo, limit: int = 400) -> list[dict]:
    """
    从同花顺获取日K线数据，返回最近 limit 条记录（由旧到新排列）。
    每条记录包含：date, open_price, close_price, high_price, low_price,
                 trading_volume（手）, trading_amount, change_hand（换手率%）
    """
    symbol = _jqka_symbol(stock_info.stock_code_normalize)
    code = stock_info.stock_code

    # 需要覆盖的年份：从 (当前年 - limit/243向上取整) 到当前年
    import math
    current_year = date.today().year
    years_needed = math.ceil(limit / 243) + 1
    years = [current_year - i for i in range(years_needed)]

    fetch_tasks = [_fetch_raw(f"https://d.10jqka.com.cn/v6/line/{symbol}/01/all.js")]
    for y in years:
        fetch_tasks.append(_fetch_raw(f"https://d.10jqka.com.cn/v6/line/{symbol}/01/{y}.js"))

    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    data = results[0]
    if isinstance(data, Exception):
        raise data
    year_data_list = [r for r in results[1:] if isinstance(r, dict)]
    nofq_map = _build_nofq_map(year_data_list)

    if not _validate_raw_response(data, code):
        logger.warning("[%s] 日K原始数据校验失败，返回空列表", code)
        return []

    price_factor = data.get("priceFactor", 100)
    sort_year = data.get("sortYear", [])
    dates = _build_dates(data.get("start", ""), sort_year, data.get("dates", ""))
    prices = _decode_prices(data.get("price", ""), price_factor)
    volumes = [int(v) // 100 for v in data.get("volumn", "").split(",") if v]

    n = min(len(dates), len(prices), len(volumes))
    if n == 0:
        logger.error("[%s] 日K解析后数据为空：dates=%d, prices=%d, volumes=%d", code, len(dates), len(prices), len(volumes))
        return []
    start = max(0, n - limit)

    result = []
    anomaly_count = 0
    for i in range(start, n):
        open_p, close_p, high_p, low_p = prices[i]
        nofq = nofq_map.get(dates[i].replace("-", ""), {})
        actual_close = nofq.get("close", close_p)
        prev_close = result[-1]["close_price"] if result else None
        amplitude   = round((high_p - low_p) / prev_close * 100, 2) if prev_close else None
        change_pct  = round((actual_close - prev_close) / prev_close * 100, 2) if prev_close else None
        change_amt  = round(actual_close - prev_close, 2) if prev_close else None
        record = {
            "date":           dates[i],
            "open_price":     open_p,
            "close_price":    actual_close,
            "high_price":     high_p,
            "low_price":      low_p,
            "trading_volume": volumes[i],
            "trading_amount": nofq.get("amount"),
            "amplitude":      amplitude,
            "change_percent": change_pct,
            "change_amount":  change_amt,
            "change_hand":    nofq.get("turnover"),
        }
        _fix_close_price_boundary(record, code)
        # 2024年前的历史数据，数值类字段为空则补0
        if dates[i] < "2024":
            for _fk in ("open_price", "close_price", "high_price", "low_price",
                         "trading_volume", "trading_amount", "amplitude",
                         "change_percent", "change_amount", "change_hand"):
                if record.get(_fk) is None:
                    record[_fk] = 0
        if not _validate_kline_record(record, code):
            anomaly_count += 1
        result.append(record)

    if anomaly_count > 0:
        logger.warning("[%s] 日K共 %d/%d 条记录存在异常数据", code, anomaly_count, len(result))

    latest_trading_day = _latest_trading_day().strftime("%Y-%m-%d")
    today_kline = await _get_today_kline(stock_info.stock_code_normalize)
    if today_kline and today_kline["date"] == latest_trading_day:
        if result and result[-1]["date"] == latest_trading_day:
            result[-1] = today_kline
        else:
            result.append(today_kline)
            if len(result) > limit:
                result = result[-limit:]

    return result


async def get_stock_day_kline_as_str_10jqka(stock_info: StockInfo, limit: int = 400) -> list[str]:
    """
    返回与 get_stock_day_range_kline_by_db_cache 格式一致的逗号分隔字符串列表：
    date,open_price,close_price,high_price,low_price,trading_volume,trading_amount,amplitude,change_percent,change_amount,change_hand
    """
    klines = await get_stock_day_kline_10jqka(stock_info, limit)
    return [
        ','.join(str(v) for v in (
            k["date"], k["open_price"], k["close_price"], k["high_price"], k["low_price"],
            round(k["trading_volume"], 2),
            k["trading_amount"] if k.get("trading_amount") is not None else "",
            k.get("amplitude", ""), k.get("change_percent", ""), k.get("change_amount", ""),
            k.get("change_hand", ""),
        ))
        for k in klines
    ]


async def get_stock_day_kline_cn_10jqka(stock_info: StockInfo, limit: int = 400) -> list[dict]:
    """获取日K线数据，返回中文key，与 get_stock_day_kline_cn 格式一致"""
    klines = await get_stock_day_kline_10jqka(stock_info, limit)
    return [{
        "日期":       k["date"],
        "开盘价":     k["open_price"],
        "收盘价":     k["close_price"],
        "最高价":     k["high_price"],
        "最低价":     k["low_price"],
        "成交量（手）": k["trading_volume"],
        "成交额":     k.get("trading_amount"),
        "振幅(%)": k.get("amplitude"),
        "涨跌幅(%)": k.get("change_percent"),
        "涨跌额":     k.get("change_amount"),
        "换手率(%)": k.get("change_hand"),
    } for k in klines]


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("三孚股份")
        klines = await _get_today_kline(stock_info.stock_code_normalize)
        logger.info(json.dumps(klines, ensure_ascii=False))

    asyncio.run(main())
