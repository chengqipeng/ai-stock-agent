"""
从同花顺概念板块列表页抓取所有概念板块信息。

数据源: http://q.10jqka.com.cn/gn/
主页包含按字母分组的全部概念板块（cate_group），无需分页。

每个概念板块包含: 板块代码、板块名称、板块URL。

Usage:
    python -m service.jqka10.concept_board_list_10jqka
"""
import logging
import random
import re
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

# 匹配概念板块链接: <a href="http://q.10jqka.com.cn/gn/detail/code/308007/" ...>概念名</a>
_BOARD_RE = re.compile(
    r'<a[^>]+href="[^"]*?/gn/detail/code/(\d+)/"[^>]*>([^<]+)</a>',
    re.DOTALL,
)


def fetch_all_concept_boards(retries: int = 3) -> list[dict]:
    """
    抓取同花顺所有概念板块列表。

    主页 http://q.10jqka.com.cn/gn/ 包含按字母分组的全部概念板块，
    无需分页即可获取完整列表。

    Returns:
        [{"board_code": "308007", "board_name": "人工智能", "board_url": "..."}, ...]
    """
    url = "http://q.10jqka.com.cn/gn/"

    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                try:
                    html = raw.decode("gbk")
                except UnicodeDecodeError:
                    html = raw.decode("gb2312", errors="replace")

            boards = []
            seen = set()
            for m in _BOARD_RE.finditer(html):
                board_code = m.group(1).strip()
                board_name = m.group(2).strip()
                if board_code not in seen and board_name:
                    seen.add(board_code)
                    boards.append({
                        "board_code": board_code,
                        "board_name": board_name,
                        "board_url": f"http://q.10jqka.com.cn/gn/detail/code/{board_code}/",
                    })

            logger.info("[概念板块列表] 共解析到 %d 个概念板块", len(boards))
            return boards

        except Exception as e:
            logger.warning("[概念板块列表] 请求异常 attempt=%d: %s", attempt, e)
            if attempt < retries:
                time.sleep(2 + random.uniform(0, 1))

    logger.error("[概念板块列表] 所有重试均失败")
    return []


def fetch_and_save_concept_boards() -> int:
    """
    抓取所有概念板块并存入数据库。

    Returns:
        成功写入的记录数
    """
    from dao.stock_concept_board_dao import batch_upsert_concept_boards

    boards = fetch_all_concept_boards()
    if not boards:
        print("[概念板块列表] 未获取到任何板块数据")
        return 0

    count = batch_upsert_concept_boards(boards)
    print(f"[概念板块列表] 成功写入 {count} 条概念板块记录")
    return count


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    boards = fetch_all_concept_boards()
    print(f"\n共获取 {len(boards)} 个概念板块:")
    for i, b in enumerate(boards):
        print(f"  {i+1}. [{b['board_code']}] {b['board_name']}")

    if boards:
        print(f"\n正在写入数据库...")
        count = fetch_and_save_concept_boards()
        print(f"完成，共写入 {count} 条记录")


if __name__ == "__main__":
    main()
