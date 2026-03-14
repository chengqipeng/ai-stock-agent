"""
从同花顺概念板块详情页抓取板块内所有成分股。

详情页: http://q.10jqka.com.cn/gn/detail/code/{board_code}/
分页URL: http://q.10jqka.com.cn/gn/detail/field/{field}/order/{order}/page/{page}/code/{board_code}

使用 curl_cffi 模拟浏览器 TLS 指纹绕过反爬（与 stock_fund_flow_10jqka.py 一致），
通过 AsyncSession 复用连接和 cookie，可突破 urllib 方式的5页 SSR 限制。

策略：
  1. 先用默认排序 desc 遍历全部页面获取成分股
  2. 如果仍未覆盖全部（对比 total_pages*10），再用 asc 方向补充
  3. 如果还不够，用额外排序字段（换手率、成交额等）补充

Usage:
    # 抓取单个板块的成分股
    python -m service.jqka10.concept_board_stocks_10jqka 309264

    # 抓取所有板块的成分股（跳过已有数据的板块）
    python -m service.jqka10.concept_board_stocks_10jqka --all

    # 强制重新抓取所有板块
    python -m service.jqka10.concept_board_stocks_10jqka --force
"""
import asyncio
import logging
import random
import re
import sys
import time
from typing import Optional

from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

IMPERSONATE = "chrome131"

# 匹配股票行
_STOCK_RE = re.compile(
    r'<td><a\s+href="http://stockpage\.10jqka\.com\.cn/(\d{6})/"'
    r'\s+target="_blank">\d{6}</a></td>\s*'
    r'<td><a\s+href="http://stockpage\.10jqka\.com\.cn/\d{6}"'
    r'\s+target="_blank">([^<]+)</a></td>',
    re.DOTALL,
)

# 分页信息
_PAGE_INFO_RE = re.compile(r'class="page_info">(\d+)/(\d+)</span>')
_LAST_PAGE_RE = re.compile(r'class="changePage"\s+page="(\d+)"[^>]*>尾页</a>')

# 额外排序字段，用于补充覆盖
_EXTRA_SORT_FIELDS = [
    "1968584",  # 换手率
    "3475914",  # 成交额
    "19",       # 最新价
    "7",        # 涨跌
]


def _clean_html(raw_bytes: bytes) -> Optional[str]:
    """GBK 解码，返回 None 表示空页面。"""
    if len(raw_bytes) < 500:
        return None
    try:
        return raw_bytes.decode("gbk")
    except UnicodeDecodeError:
        return raw_bytes.decode("gb2312", errors="replace")


def _get_total_pages(html: str) -> int:
    """从HTML中提取总页数。"""
    m = _PAGE_INFO_RE.search(html)
    if m:
        return int(m.group(2))
    m = _LAST_PAGE_RE.search(html)
    if m:
        return int(m.group(1))
    return 1


def _parse_stocks(html: str) -> list[tuple[str, str]]:
    """从HTML中解析股票列表，返回 [(code, name), ...]。"""
    results = []
    seen = set()
    for m in _STOCK_RE.finditer(html):
        code = m.group(1).strip()
        name = m.group(2).strip()
        if code not in seen:
            seen.add(code)
            results.append((code, name))
    return results


def _build_url(board_code: str, page: int = 1, order: str = "desc",
               field: str = "") -> str:
    """构建板块详情页URL。"""
    if page == 1 and order == "desc" and not field:
        return f"http://q.10jqka.com.cn/gn/detail/code/{board_code}/"
    if field:
        return (f"http://q.10jqka.com.cn/gn/detail/field/{field}/"
                f"order/{order}/page/{page}/code/{board_code}")
    return (f"http://q.10jqka.com.cn/gn/detail/order/{order}/"
            f"page/{page}/code/{board_code}")


async def _async_fetch_page(session: AsyncSession, board_code: str,
                            page: int, order: str = "desc",
                            field: str = "", retries: int = 2) -> Optional[str]:
    """使用 curl_cffi AsyncSession 获取板块详情某一页的HTML。"""
    url = _build_url(board_code, page, order, field)
    for attempt in range(retries + 1):
        try:
            resp = await session.get(url, timeout=20)
            resp.raise_for_status()
            return _clean_html(resp.content)
        except Exception as e:
            logger.warning("[板块成分股] 请求异常 board=%s page=%d order=%s "
                           "field=%s attempt=%d: %s",
                           board_code, page, order, field, attempt, e)
            if attempt < retries:
                await asyncio.sleep(1.5 + random.uniform(0, 1))
    return None


async def _fetch_direction_pages(session: AsyncSession, board_code: str,
                                 total_pages: int, order: str,
                                 all_stocks: dict, field: str = "",
                                 delay: float = 0.2) -> int:
    """
    按指定方向遍历所有页面，合并到 all_stocks。
    返回本轮新增数量。
    """
    added = 0
    for page in range(1, total_pages + 1):
        if page > 1:
            await asyncio.sleep(delay + random.uniform(0, 0.1))
        html = await _async_fetch_page(session, board_code, page, order, field)
        if not html:
            # 连续空页面说明已到末尾
            break
        page_stocks = _parse_stocks(html)
        if not page_stocks:
            break
        for code, name in page_stocks:
            if code not in all_stocks:
                all_stocks[code] = name
                added += 1
    return added


async def async_fetch_board_stocks(board_code: str,
                                   delay: float = 0.2) -> list[dict]:
    """
    异步抓取某个概念板块的全部成分股。

    使用 curl_cffi AsyncSession 模拟浏览器 TLS 指纹，
    复用 session 的 cookie 以突破 SSR 分页限制。

    策略：
      1. 先访问首页获取 total_pages 和 cookie
      2. desc 方向遍历全部页面
      3. 如果还不够，asc 方向补充
      4. 如果仍不够，用额外排序字段补充

    Returns:
        [{"stock_code": "300143", "stock_name": "盈康生命"}, ...]
    """
    all_stocks: dict[str, str] = {}  # code -> name

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        # 1. 获取首页，确定总页数
        first_html = await _async_fetch_page(session, board_code, 1, "desc")
        if not first_html:
            logger.error("[板块成分股] 首页获取失败 board=%s", board_code)
            return []

        total_pages = _get_total_pages(first_html)
        expected_total = total_pages * 10  # 每页10只的估算

        # 解析首页
        for code, name in _parse_stocks(first_html):
            all_stocks[code] = name

        # 2. desc 方向剩余页
        for page in range(2, total_pages + 1):
            await asyncio.sleep(delay + random.uniform(0, 0.1))
            html = await _async_fetch_page(session, board_code, page, "desc")
            if not html:
                break
            page_stocks = _parse_stocks(html)
            if not page_stocks:
                break
            for code, name in page_stocks:
                all_stocks[code] = name

        logger.info("[板块成分股] board=%s desc完成 获取=%d/%d",
                    board_code, len(all_stocks), expected_total)

        # 3. 如果 desc 没覆盖全，用 asc 补充
        if len(all_stocks) < expected_total:
            added = await _fetch_direction_pages(
                session, board_code, total_pages, "asc", all_stocks,
                delay=delay)
            logger.info("[板块成分股] board=%s asc补充 新增=%d 累计=%d/%d",
                        board_code, added, len(all_stocks), expected_total)

        # 4. 如果还不够，用额外排序字段补充
        if len(all_stocks) < expected_total:
            for field in _EXTRA_SORT_FIELDS:
                if len(all_stocks) >= expected_total:
                    break
                for direction in ("desc", "asc"):
                    if len(all_stocks) >= expected_total:
                        break
                    added = await _fetch_direction_pages(
                        session, board_code, total_pages, direction,
                        all_stocks, field=field, delay=delay)
                    logger.debug("[板块成分股] board=%s field=%s %s "
                                 "新增=%d 累计=%d/%d",
                                 board_code, field, direction,
                                 added, len(all_stocks), expected_total)

    result = [{"stock_code": c, "stock_name": n} for c, n in all_stocks.items()]
    logger.info("[板块成分股] board=%s 总页数=%d 预期≈%d只 获取=%d只",
                board_code, total_pages, expected_total, len(result))
    return result


def fetch_board_stocks(board_code: str, delay: float = 0.2) -> list[dict]:
    """
    同步接口：抓取某个概念板块的成分股。
    内部调用 async_fetch_board_stocks。
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # 已在事件循环中，创建新线程运行
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run,
                                 async_fetch_board_stocks(board_code, delay))
            return future.result()
    else:
        return asyncio.run(async_fetch_board_stocks(board_code, delay))


def fetch_and_save_board_stocks(board_code: str, board_name: str = "",
                                delay: float = 0.2) -> int:
    """抓取板块成分股并写入数据库。"""
    from dao.stock_concept_board_dao import batch_upsert_board_stocks

    stocks = fetch_board_stocks(board_code, delay=delay)
    if not stocks:
        return 0
    return batch_upsert_board_stocks(board_code, board_name, stocks)


def fetch_and_save_all_boards_stocks(delay_page: float = 0.2,
                                     delay_board: float = 0.8,
                                     force: bool = False) -> dict:
    """
    遍历数据库中所有概念板块，抓取每个板块的成分股并写入。

    Args:
        delay_page: 页间延迟（秒）
        delay_board: 板块间延迟（秒）
        force: 是否强制重新抓取已有数据的板块

    Returns:
        {"total_boards": N, "success": N, "skipped": N, "total_stocks": N}
    """
    from dao.stock_concept_board_dao import (
        get_all_concept_boards, batch_upsert_board_stocks, get_board_stock_count
    )

    boards = get_all_concept_boards()
    total = len(boards)
    success = 0
    skipped = 0
    total_stocks = 0
    failed = 0

    print(f"[板块成分股] 共 {total} 个板块待处理 (force={force})")

    for i, board in enumerate(boards):
        board_code = board["board_code"]
        board_name = board["board_name"]

        # 跳过已有数据的板块（除非 force）
        if not force:
            existing = get_board_stock_count(board_code)
            if existing > 0:
                skipped += 1
                total_stocks += existing
                print(f"  [{i+1}/{total}] {board_code} {board_name} -> "
                      f"已有{existing}只, 跳过")
                continue

        stocks = fetch_board_stocks(board_code, delay=delay_page)
        if stocks:
            batch_upsert_board_stocks(board_code, board_name, stocks)
            success += 1
            total_stocks += len(stocks)
            print(f"  [{i+1}/{total}] {board_code} {board_name} -> "
                  f"{len(stocks)}只成分股")
        else:
            failed += 1
            print(f"  [{i+1}/{total}] {board_code} {board_name} -> 抓取失败")

        if i < total - 1:
            time.sleep(delay_board + random.uniform(0, 0.3))

    return {"total_boards": total, "success": success, "skipped": skipped,
            "failed": failed, "total_stocks": total_stocks}


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    if len(sys.argv) > 1 and sys.argv[1] in ("--all", "--force"):
        force = "--force" in sys.argv
        result = fetch_and_save_all_boards_stocks(force=force)
        print(f"\n完成: 新抓取{result['success']}个, 跳过{result['skipped']}个, "
              f"失败{result['failed']}个 / 共{result['total_boards']}个板块, "
              f"累计{result['total_stocks']}只成分股")
    else:
        board_code = sys.argv[1] if len(sys.argv) > 1 else "309264"
        print(f"抓取板块 {board_code} 的成分股...")
        stocks = fetch_board_stocks(board_code)
        print(f"\n共 {len(stocks)} 只成分股:")
        for i, s in enumerate(stocks):
            print(f"  {i+1}. {s['stock_code']} {s['stock_name']}")

        if stocks:
            print(f"\n写入数据库...")
            count = fetch_and_save_board_stocks(board_code)
            print(f"完成，写入 {count} 条记录")


if __name__ == "__main__":
    main()
