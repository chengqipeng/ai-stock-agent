"""
从同花顺概念板块详情页抓取板块日K线数据并存入数据库。

板块详情页: https://q.10jqka.com.cn/gn/detail/code/{board_code}/
K线数据源:  https://d.10jqka.com.cn/v6/line/bk_{board_index_code}/01/all.js

板块代码映射:
  - board_code (30xxxx): 概念板块详情页URL中的代码
  - board_index_code (885xxx/886xxx): K线API使用的指数代码
  - 映射关系通过详情页 <input id="clid" value="886108"> 获取

Usage:
    # 抓取单个板块的日K线
    python -m service.jqka10.concept_board_kline_10jqka 309264

    # 抓取所有板块的日K线（跳过已有数据的板块）
    python -m service.jqka10.concept_board_kline_10jqka --all

    # 强制重新抓取所有板块
    python -m service.jqka10.concept_board_kline_10jqka --all --force

    # 仅增量更新（只拉取最新日期之后的数据）
    python -m service.jqka10.concept_board_kline_10jqka --all --incremental
"""
import asyncio
import json
import logging
import math
import random
import re
import sys
import time
import urllib.request
from datetime import date
from typing import Optional

import aiohttp
import yarl

logger = logging.getLogger(__name__)

_HEADERS_HTML = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/145.0.0.0 Safari/537.36",
}

_HEADERS_API = {
    "Accept": "*/*",
    "Referer": "https://q.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/145.0.0.0 Safari/537.36",
}


# ── 板块代码映射：从详情页获取 board_index_code ──

_CLID_RE = re.compile(r'id="clid"\s+value=[\'"](\d+)[\'"]')


def fetch_board_index_code(board_code: str, retries: int = 2) -> Optional[str]:
    """
    从概念板块详情页获取板块指数代码(885xxx/886xxx)。

    Args:
        board_code: 板块代码(30xxxx)

    Returns:
        板块指数代码，如 "886108"，失败返回 None
    """
    url = f"http://q.10jqka.com.cn/gn/detail/code/{board_code}/"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=_HEADERS_HTML)
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
                try:
                    html = raw.decode("gbk")
                except UnicodeDecodeError:
                    html = raw.decode("gb2312", errors="replace")

            m = _CLID_RE.search(html)
            if m:
                return m.group(1)
            logger.warning("[概念板块K线] 未找到clid board=%s", board_code)
            return None
        except Exception as e:
            logger.warning("[概念板块K线] 获取clid异常 board=%s attempt=%d: %s",
                           board_code, attempt, e)
            if attempt < retries:
                time.sleep(1 + random.uniform(0, 1))
    return None


# ── K线数据解码 ──

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
            if "-" in mmdd:
                result.append(f"{year}-{mmdd}")
            else:
                result.append(f"{year}-{mmdd[:2]}-{mmdd[2:]}")
            idx += 1
    return result


def _decode_prices(price_str: str, price_factor: int) -> list[tuple]:
    """
    解码同花顺价格数据，每4个数字一组。
    返回: [(open, close_placeholder, high, low), ...]
    """
    nums = list(map(int, price_str.split(",")))
    records = []
    for i in range(0, len(nums), 4):
        chunk = nums[i:i + 4]
        if len(chunk) < 4:
            break
        prev = chunk[0]
        open_i = prev + chunk[1]
        high_i = prev + chunk[2]
        close_i = open_i - chunk[3]
        records.append((
            round(open_i / price_factor, 2),
            round(close_i / price_factor, 2),
            round(high_i / price_factor, 2),
            round(prev / price_factor, 2),
        ))
    return records


def _build_nofq_map(year_data_list: list[dict]) -> dict[str, dict]:
    """从年份分段数据构建 {YYYYMMDD: {close, amount, turnover}} 映射"""
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


async def _fetch_raw(url: str) -> dict:
    """请求同花顺 JSONP 接口并解析为 dict"""
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers=_HEADERS_API) as resp:
            status = resp.status
            text = await resp.text()
    if status != 200:
        if status == 404:
            logger.debug("[概念板块K线] HTTP 404, url=%s", url)
        else:
            logger.warning("[概念板块K线] HTTP %d, url=%s", status, url)
        raise aiohttp.ClientResponseError(
            request_info=aiohttp.RequestInfo(
                url=yarl.URL(url), method="GET",
                headers={}, real_url=yarl.URL(url),
            ),
            history=(), status=status,
            message=f"HTTP {status}: {text[:200]}",
        )
    if not text or not text.strip():
        raise ValueError(f"接口返回空响应: {url}")
    json_text = re.sub(r"^\w+\(", "", text)
    json_text = re.sub(r"\);?\s*$", "", json_text)
    if not json_text.strip():
        raise ValueError(f"JSONP解包后为空: {url}")
    return json.loads(json_text, strict=False)


async def fetch_board_kline(board_index_code: str, limit: int = 800) -> list[dict]:
    """
    从同花顺获取概念板块日K线数据。

    Args:
        board_index_code: 板块指数代码(885xxx/886xxx)
        limit: 最多返回条数

    Returns:
        由旧到新排列的K线列表
    """
    symbol = f"bk_{board_index_code}"

    current_year = date.today().year
    years_needed = math.ceil(limit / 243) + 1
    years = [current_year - i for i in range(years_needed)]

    # 并发请求 all.js + 各年份数据
    fetch_tasks = [_fetch_raw(f"https://d.10jqka.com.cn/v6/line/{symbol}/01/all.js")]
    for y in years:
        fetch_tasks.append(
            _fetch_raw(f"https://d.10jqka.com.cn/v6/line/{symbol}/01/{y}.js")
        )

    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    data = results[0]
    if isinstance(data, Exception):
        logger.error("[概念板块K线] all.js 请求失败 index=%s: %s",
                     board_index_code, data)
        raise data

    year_data_list = [r for r in results[1:] if isinstance(r, dict)]
    nofq_map = _build_nofq_map(year_data_list)

    # 校验必要字段
    required = ("priceFactor", "sortYear", "dates", "price", "volumn")
    missing = [f for f in required if not data.get(f)]
    if missing:
        logger.error("[概念板块K线] index=%s 原始数据缺失字段: %s",
                     board_index_code, missing)
        return []

    price_factor = data.get("priceFactor", 100)
    sort_year = data.get("sortYear", [])
    dates = _build_dates(data.get("start", ""), sort_year, data.get("dates", ""))
    prices = _decode_prices(data.get("price", ""), price_factor)
    volumes = [int(v) // 100 for v in data.get("volumn", "").split(",") if v]

    n = min(len(dates), len(prices), len(volumes))
    if n == 0:
        logger.error("[概念板块K线] index=%s 解析后数据为空", board_index_code)
        return []

    start = max(0, n - limit)
    result = []
    for i in range(start, n):
        open_p, close_p, high_p, low_p = prices[i]
        nofq = nofq_map.get(dates[i].replace("-", ""), {})
        actual_close = nofq.get("close", close_p)

        prev_close = result[-1]["close_price"] if result else None
        amplitude = round((high_p - low_p) / prev_close * 100, 2) if prev_close else 0
        change_pct = round((actual_close - prev_close) / prev_close * 100, 2) if prev_close else 0
        change_amt = round(actual_close - prev_close, 2) if prev_close else 0

        result.append({
            "date":           dates[i],
            "open_price":     open_p,
            "close_price":    actual_close,
            "high_price":     high_p,
            "low_price":      low_p,
            "trading_volume": volumes[i],
            "trading_amount": nofq.get("amount"),
            "change_percent": change_pct,
            "change_amount":  change_amt,
            "amplitude":      amplitude,
            "change_hand":    nofq.get("turnover"),
        })

    board_name = data.get("name", board_index_code)
    logger.info("[概念板块K线] %s(%s) 获取 %d 条K线 (%s ~ %s)",
                board_name, board_index_code, len(result),
                result[0]["date"] if result else "N/A",
                result[-1]["date"] if result else "N/A")
    return result


def _resolve_index_code(board_code: str, board: dict = None) -> Optional[str]:
    """
    获取板块的指数代码，优先从数据库读取，没有则从网页抓取并回写。

    Args:
        board_code: 板块代码(30xxxx)
        board: 数据库中的板块记录（可选，避免重复查询）

    Returns:
        板块指数代码(885xxx/886xxx)，失败返回 None
    """
    from dao.stock_concept_board_dao import update_board_index_code

    # 优先使用已有的 index_code
    if board and board.get("board_index_code"):
        return board["board_index_code"]

    # 从网页抓取
    index_code = fetch_board_index_code(board_code)
    if index_code:
        update_board_index_code(board_code, index_code)
        logger.info("[概念板块K线] board=%s -> index_code=%s (已回写数据库)",
                    board_code, index_code)
    return index_code


async def fetch_and_save_board_kline(board_code: str, board: dict = None,
                                     limit: int = 800) -> int:
    """抓取单个板块的日K线并写入数据库。"""
    from dao.concept_board_kline_dao import batch_upsert_klines

    index_code = _resolve_index_code(board_code, board)
    if not index_code:
        logger.error("[概念板块K线] board=%s 无法获取指数代码", board_code)
        return 0

    klines = await fetch_board_kline(index_code, limit=limit)
    if not klines:
        return 0
    return batch_upsert_klines(board_code, klines, board_index_code=index_code)


async def fetch_and_save_all_boards_kline(
    limit: int = 800,
    delay: float = 0.5,
    force: bool = False,
    incremental: bool = False,
) -> dict:
    """
    遍历数据库中所有概念板块，抓取每个板块的日K线并写入。

    Args:
        limit: 每个板块最多拉取的K线条数
        delay: 板块间延迟（秒）
        force: 是否强制重新抓取已有数据的板块
        incremental: 增量模式，仅拉取最新日期之后的数据

    Returns:
        {"total_boards": N, "success": N, "skipped": N, "failed": N, "total_klines": N}
    """
    from dao.stock_concept_board_dao import get_all_concept_boards
    from dao.concept_board_kline_dao import (
        batch_upsert_klines, get_kline_count, get_latest_date,
    )

    boards = get_all_concept_boards()
    total = len(boards)
    success = 0
    skipped = 0
    failed = 0
    total_klines = 0

    print(f"[概念板块K线] 共 {total} 个板块待处理 "
          f"(force={force}, incremental={incremental})")

    for i, board in enumerate(boards):
        board_code = board["board_code"]
        board_name = board["board_name"]

        # 跳过已有数据的板块（除非 force 或 incremental）
        if not force and not incremental:
            existing = get_kline_count(board_code)
            if existing > 0:
                skipped += 1
                total_klines += existing
                print(f"  [{i+1}/{total}] {board_code} {board_name} -> "
                      f"已有{existing}条K线, 跳过")
                continue

        try:
            # 获取指数代码
            index_code = _resolve_index_code(board_code, board)
            if not index_code:
                failed += 1
                print(f"  [{i+1}/{total}] {board_code} {board_name} -> "
                      f"无法获取指数代码")
                continue

            klines = await fetch_board_kline(index_code, limit=limit)
            if not klines:
                failed += 1
                print(f"  [{i+1}/{total}] {board_code} {board_name} -> 无数据")
                continue

            # 增量模式：只保留最新日期之后的数据
            if incremental:
                latest = get_latest_date(board_code)
                if latest:
                    klines = [k for k in klines if k["date"] > latest]
                    if not klines:
                        skipped += 1
                        print(f"  [{i+1}/{total}] {board_code} {board_name} -> "
                              f"已是最新({latest}), 跳过")
                        continue

            batch_upsert_klines(board_code, klines, board_index_code=index_code)
            success += 1
            total_klines += len(klines)
            print(f"  [{i+1}/{total}] {board_code} {board_name}({index_code}) -> "
                  f"{len(klines)}条K线 ({klines[0]['date']}~{klines[-1]['date']})")

        except Exception as e:
            failed += 1
            logger.error("[概念板块K线] board=%s 抓取异常: %s", board_code, e)
            print(f"  [{i+1}/{total}] {board_code} {board_name} -> 异常: {e}")

        if i < total - 1:
            time.sleep(delay + random.uniform(0, 0.2))

    return {
        "total_boards": total,
        "success": success,
        "skipped": skipped,
        "failed": failed,
        "total_klines": total_klines,
    }


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    args = sys.argv[1:]
    is_all = "--all" in args
    is_force = "--force" in args
    is_incremental = "--incremental" in args

    if is_all:
        result = asyncio.run(
            fetch_and_save_all_boards_kline(
                force=is_force,
                incremental=is_incremental,
            )
        )
        print(f"\n完成: 成功{result['success']}个, 跳过{result['skipped']}个, "
              f"失败{result['failed']}个 / 共{result['total_boards']}个板块, "
              f"累计{result['total_klines']}条K线")
    else:
        board_code = next((a for a in args if not a.startswith("--")), "309264")
        print(f"抓取板块 {board_code} 的日K线...")

        # 获取指数代码
        index_code = fetch_board_index_code(board_code)
        if not index_code:
            print(f"无法获取板块 {board_code} 的指数代码")
            return
        print(f"板块指数代码: {index_code}")

        klines = asyncio.run(fetch_board_kline(index_code))
        print(f"\n共 {len(klines)} 条K线:")
        if klines:
            print(f"  日期范围: {klines[0]['date']} ~ {klines[-1]['date']}")
            print(f"\n最近5条:")
            for k in klines[-5:]:
                print(f"  {k['date']}  开:{k['open_price']}  收:{k['close_price']}  "
                      f"高:{k['high_price']}  低:{k['low_price']}  "
                      f"量:{k['trading_volume']}  涨跌:{k['change_percent']}%")

            print(f"\n写入数据库...")
            count = asyncio.run(fetch_and_save_board_kline(board_code))
            print(f"完成，写入 {count} 条记录")


if __name__ == "__main__":
    main()
