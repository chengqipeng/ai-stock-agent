"""
从同花顺概念板块详情页抓取板块内所有成分股。

详情页: http://q.10jqka.com.cn/gn/detail/code/{board_code}/
分页URL: http://q.10jqka.com.cn/gn/detail/order/{order}/page/{page}/code/{board_code}

注意: 同花顺服务端仅渲染前5页数据（每页10只），超出部分需要JS执行的ajax请求。
本模块通过 desc + asc 两种排序各取5页，最多可获取100只成分股。
对于成分股超过100只的板块，获取的是涨跌幅最高和最低的各50只。

Usage:
    # 抓取单个板块的成分股
    python -m service.jqka10.concept_board_stocks_10jqka 309264

    # 抓取所有板块的成分股
    python -m service.jqka10.concept_board_stocks_10jqka --all
"""
import logging
import random
import re
import sys
import time
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/145.0.0.0 Safari/537.36",
}

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

# 服务端渲染最大页数
_MAX_SSR_PAGES = 5


def _fetch_page(board_code: str, page: int, order: str = "desc",
                retries: int = 2) -> Optional[str]:
    """获取板块详情某一页的HTML。"""
    if page == 1 and order == "desc":
        url = f"http://q.10jqka.com.cn/gn/detail/code/{board_code}/"
    else:
        url = (f"http://q.10jqka.com.cn/gn/detail/order/{order}/"
               f"page/{page}/code/{board_code}")

    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
                if len(raw) < 500:
                    return None  # 空页面
                try:
                    return raw.decode("gbk")
                except UnicodeDecodeError:
                    return raw.decode("gb2312", errors="replace")
        except Exception as e:
            logger.warning("[板块成分股] 请求异常 board=%s page=%d order=%s attempt=%d: %s",
                           board_code, page, order, attempt, e)
            if attempt < retries:
                time.sleep(1.5 + random.uniform(0, 1))
    return None


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


def fetch_board_stocks(board_code: str, delay: float = 0.2) -> list[dict]:
    """
    抓取某个概念板块的成分股。

    通过 desc（涨幅从高到低）和 asc（涨幅从低到高）两种排序各取5页，
    合并去重后返回。对于成分股≤100只的板块可获取全部数据。

    Args:
        board_code: 板块代码，如 "309264"
        delay: 请求间隔（秒）

    Returns:
        [{"stock_code": "300143", "stock_name": "盈康生命"}, ...]
    """
    all_stocks = {}  # code -> name

    # 先获取首页，确定总页数
    first_html = _fetch_page(board_code, 1, "desc")
    if not first_html:
        logger.error("[板块成分股] 首页获取失败 board=%s", board_code)
        return []

    total_pages = _get_total_pages(first_html)
    max_pages = min(total_pages, _MAX_SSR_PAGES)

    # 解析首页数据
    for code, name in _parse_stocks(first_html):
        all_stocks[code] = name

    # desc 方向剩余页
    for page in range(2, max_pages + 1):
        time.sleep(delay + random.uniform(0, 0.1))
        html = _fetch_page(board_code, page, "desc")
        if not html:
            break
        for code, name in _parse_stocks(html):
            all_stocks[code] = name

    # 如果总页数超过 SSR 限制，用 asc 方向补充
    if total_pages > _MAX_SSR_PAGES:
        for page in range(1, max_pages + 1):
            time.sleep(delay + random.uniform(0, 0.1))
            html = _fetch_page(board_code, page, "asc")
            if not html:
                continue
            for code, name in _parse_stocks(html):
                all_stocks[code] = name

    result = [{"stock_code": c, "stock_name": n} for c, n in all_stocks.items()]
    logger.info("[板块成分股] board=%s 总页数=%d 获取=%d只",
                board_code, total_pages, len(result))
    return result


def fetch_and_save_board_stocks(board_code: str, board_name: str = "",
                                delay: float = 0.2) -> int:
    """抓取板块成分股并写入数据库。"""
    from dao.stock_concept_board_dao import batch_upsert_board_stocks

    stocks = fetch_board_stocks(board_code, delay=delay)
    if not stocks:
        return 0
    return batch_upsert_board_stocks(board_code, board_name, stocks)


def fetch_and_save_all_boards_stocks(delay_page: float = 0.2,
                                     delay_board: float = 0.8) -> dict:
    """
    遍历数据库中所有概念板块，抓取每个板块的成分股并写入。

    Returns:
        {"total_boards": N, "success": N, "total_stocks": N}
    """
    from dao.stock_concept_board_dao import (
        get_all_concept_boards, batch_upsert_board_stocks
    )

    boards = get_all_concept_boards()
    total = len(boards)
    success = 0
    total_stocks = 0

    print(f"[板块成分股] 共 {total} 个板块待抓取")

    for i, board in enumerate(boards):
        board_code = board["board_code"]
        board_name = board["board_name"]

        stocks = fetch_board_stocks(board_code, delay=delay_page)
        if stocks:
            count = batch_upsert_board_stocks(board_code, board_name, stocks)
            success += 1
            total_stocks += len(stocks)
            print(f"  [{i+1}/{total}] {board_code} {board_name} -> "
                  f"{len(stocks)}只成分股, 写入{count}条")
        else:
            print(f"  [{i+1}/{total}] {board_code} {board_name} -> 无数据")

        if i < total - 1:
            time.sleep(delay_board + random.uniform(0, 0.3))

    return {"total_boards": total, "success": success, "total_stocks": total_stocks}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if len(sys.argv) > 1 and sys.argv[1] == "--all":
        result = fetch_and_save_all_boards_stocks()
        print(f"\n完成: {result['success']}/{result['total_boards']}个板块, "
              f"共{result['total_stocks']}只成分股")
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
