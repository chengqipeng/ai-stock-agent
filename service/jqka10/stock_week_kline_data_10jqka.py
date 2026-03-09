import re
import json
import math
import asyncio
import logging
import aiohttp
from html import unescape
from datetime import date
from common.utils.cache_utils import get_cache_path, load_cache, save_cache
from common.utils.stock_info_utils import StockInfo
from service.jqka10.stock_day_kline_data_10jqka import _build_dates

logger = logging.getLogger(__name__)

_WEEK_KLINE_REQUIRED_FIELDS = ("date", "open_price", "close_price", "high_price", "low_price", "trading_volume")

_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


def _decode_week_prices(price_str: str, price_factor: int) -> list[tuple]:
    """
    同花顺周K线价格解码，对齐东方财富价格体系。
    chunk = [prev_close*pf, (open-prev_close)*pf, (high-prev_close)*pf, (open-low)*pf]
    映射（实测验证）：
      open  = (chunk[0] + chunk[1]) / pf  与东方财富open完全一致
      high  = (chunk[0] + chunk[2]) / pf  与东方财富high完全一致
      low   =  chunk[0] / pf              与东方财富low完全一致
      close = 优先用不复权年份数据（与东方财富完全一致），历史数据用open-chunk[3]近似
    """
    nums = list(map(int, price_str.split(",")))
    records = []
    for i in range(0, len(nums), 4):
        chunk = nums[i:i + 4]
        if len(chunk) < 4:
            break
        prev  = chunk[0]
        open_ = prev + chunk[1]
        high  = prev + chunk[2]
        close = open_ - chunk[3]  # 历史数据近似值，会被不复权数据覆盖
        records.append((
            round(open_  / price_factor, 2),
            round(close  / price_factor, 2),
            round(high   / price_factor, 2),
            round(prev   / price_factor, 2),
        ))
    return records


async def _fetch_raw(url: str, cache_key: str = None, code: str = None) -> dict:
    if cache_key and code:
        cache_path = get_cache_path(cache_key, code)
        data = load_cache(cache_path)
        if data:
            return data
    else:
        cache_path = None

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        async with session.get(url, headers=_HEADERS) as resp:
            text = unescape(await resp.text())
    json_text = re.sub(r"^\w+\(", "", text)
    json_text = re.sub(r"\);?\s*$", "", json_text).strip()
    if not json_text:
        return {}
    data = json.loads(json_text)
    if cache_path:
        save_cache(cache_path, data)
    return data


def _build_nofq_map(year_data_list: list[dict]) -> dict[str, dict]:
    """
    从年份分段不复权日K数据构建 {YYYYMMDD: {close, amount, turnover}} 映射。
    与 stock_day_kline_data_10jqka._build_nofq_map 保持一致。
    """
    result = {}
    for year_data in year_data_list:
        for row in year_data.get("data", "").strip().split(";"):
            parts = row.split(",")
            if len(parts) >= 8 and parts[4]:
                result[parts[0]] = {
                    "close":    float(parts[4]),
                    "amount":   float(parts[6]) if parts[6] else 0,
                    "turnover": float(parts[7]) if len(parts) > 7 and parts[7] else None,
                }
    return result


async def get_stock_week_kline_10jqka(stock_info: StockInfo, limit: int = 200) -> list[dict]:
    """
    从同花顺获取周K线数据，返回最近 limit 条记录（由旧到新排列）。
    close 优先用不复权年份数据（与东方财富完全一致），历史数据用前复权近似。
    """
    market = "hs"
    code = stock_info.stock_code

    # 需要覆盖的年份：周K limit=200 约4年，按每年52周估算
    current_year = date.today().year
    years_needed = math.ceil(limit / 52) + 1
    years = [current_year - i for i in range(years_needed)]

    # 并发获取：周K all.js + 各年份日K不复权数据
    fetch_tasks = [
        _fetch_raw(f"https://d.10jqka.com.cn/v6/line/{market}_{code}/11/all.js", "week_kline_10jqka", code)
    ]
    for y in years:
        fetch_tasks.append(
            _fetch_raw(f"https://d.10jqka.com.cn/v6/line/{market}_{code}/01/{y}.js")
        )

    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    week_data = results[0] if isinstance(results[0], dict) else {}
    year_data_list = [r for r in results[1:] if isinstance(r, dict)]
    nofq_map = _build_nofq_map(year_data_list)

    # 校验周K原始响应
    if not week_data:
        logger.error("[%s] 周K原始响应为空", code)
        return []
    missing = [f for f in ("priceFactor", "sortYear", "dates", "price", "volumn") if not week_data.get(f)]
    if missing:
        logger.error("[%s] 周K原始响应缺少关键字段 %s，data keys=%s", code, missing, list(week_data.keys()))
        return []

    price_factor = week_data.get("priceFactor", 100)
    dates = _build_dates(week_data.get("start", ""), week_data.get("sortYear", []), week_data.get("dates", ""))
    prices = _decode_week_prices(week_data.get("price", ""), price_factor)
    volumes = [int(v) // 100 for v in week_data.get("volumn", "").split(",") if v]

    # nofq_map 的 key 是 YYYYMMDD，dates 是 YYYY-MM-DD，统一用 YYYYMMDD 比较
    nofq_dates_sorted = sorted(nofq_map.keys())

    n = min(len(dates), len(prices), len(volumes))
    if n == 0:
        logger.error("[%s] 周K解析后数据为空：dates=%d, prices=%d, volumes=%d", code, len(dates), len(prices), len(volumes))
        return []
    start = max(0, n - limit)

    # 为第一条记录计算 prev_close：取 start-1 周的收盘价
    first_prev_close = None
    if start > 0:
        prev_idx = start - 1
        prev_wk_date_raw = dates[prev_idx].replace("-", "")
        prev_prev_wk_raw = dates[prev_idx - 1].replace("-", "") if prev_idx > 0 else "00000000"
        prev_wk_nofq_days = [d for d in nofq_dates_sorted if prev_prev_wk_raw < d <= prev_wk_date_raw]
        if prev_wk_nofq_days:
            first_prev_close = nofq_map[prev_wk_nofq_days[-1]]["close"]
        else:
            first_prev_close = prices[prev_idx][1]

    result = []
    anomaly_count = 0
    for i in range(start, n):
        week_date_raw = dates[i].replace("-", "")  # YYYYMMDD
        prev_week_date_raw = dates[i - 1].replace("-", "") if i > 0 else "00000000"
        # 周K日期是该周最后一个交易日，找 prev_week_date < d <= week_date 的日K
        week_nofq_days = [d for d in nofq_dates_sorted if prev_week_date_raw < d <= week_date_raw]
        if week_nofq_days:
            close          = nofq_map[week_nofq_days[-1]]["close"]
            trading_amount = sum(nofq_map[d]["amount"] for d in week_nofq_days)
            change_hands   = [nofq_map[d]["turnover"] for d in week_nofq_days if nofq_map[d]["turnover"] is not None]
            change_hand    = round(sum(change_hands), 2) if change_hands else None
        else:
            close          = prices[i][1]
            trading_amount = None
            change_hand    = None
        if result:
            prev_close = result[-1]["close_price"]
        elif first_prev_close is not None:
            prev_close = first_prev_close
        else:
            prev_close = None
        if prev_close:
            amplitude      = round((prices[i][2] - prices[i][3]) / prev_close * 100, 2)
            change_percent = round((close - prev_close) / prev_close * 100, 2)
            change_amount  = round(close - prev_close, 2)
        else:
            amplitude = change_percent = change_amount = None
        record = {
            "date":           dates[i],
            "open_price":     prices[i][0],
            "close_price":    close,
            "high_price":     prices[i][2],
            "low_price":      prices[i][3],
            "trading_volume": volumes[i],
            "trading_amount": trading_amount,
            "amplitude":      amplitude,
            "change_percent": change_percent,
            "change_amount":  change_amount,
            "change_hand":    change_hand,
        }
        # 校验关键字段
        empty_fields = [f for f in _WEEK_KLINE_REQUIRED_FIELDS if record.get(f) is None or record.get(f) == ""]
        if empty_fields:
            logger.error("[%s] 周K数据异常：日期=%s 存在空值字段 %s，record=%s",
                         code, record.get("date", "N/A"), empty_fields, record)
            anomaly_count += 1
        result.append(record)

    if anomaly_count > 0:
        logger.warning("[%s] 周K共 %d/%d 条记录存在异常数据", code, anomaly_count, len(result))

    return result


async def get_stock_week_kline_list_10jqka(stock_info: StockInfo, limit: int = 200) -> list[dict]:
    """返回与 get_stock_month_kline_list 格式一致的周K线数据"""
    klines = await get_stock_week_kline_10jqka(stock_info, limit)
    result = []
    for k in klines:
        d = k["date"]
        # date 已经是 YYYY-MM-DD 格式，直接使用
        result.append({
            "日期":      d,
            "开盘":      k["open_price"],
            "收盘":      k["close_price"],
            "最高":      k["high_price"],
            "最低":      k["low_price"],
            "成交量":    k["trading_volume"],
            "成交额":    k["trading_amount"],
            "振幅(%)": k["amplitude"],
            "涨跌幅(%)": k["change_percent"],
            "涨跌额":    k["change_amount"],
            "换手率(%)": k["change_hand"],
        })
    return result


if __name__ == "__main__":
    import json
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        klines = await get_stock_week_kline_list_10jqka(stock_info, limit=10)
        logger.info(json.dumps(klines, ensure_ascii=False))

    asyncio.run(main())
