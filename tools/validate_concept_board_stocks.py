"""
概念板块成分股数量验证工具

对比数据库中每个概念板块的成分股数量与同花顺网站实际数量是否一致。

逻辑：
  1. 从数据库查询所有概念板块及其成分股数量
  2. 对每个板块，请求同花顺首页获取 total_pages，推算预期成分股数 ≈ total_pages * 10
  3. 同时解析首页实际股票数，用于辅助判断
  4. 对比 DB 数量 vs 同花顺预期数量，输出差异报告

Usage:
    python -m tools.validate_concept_board_stocks
    python -m tools.validate_concept_board_stocks --top 20
    python -m tools.validate_concept_board_stocks --board 308007
"""
import argparse
import asyncio
import logging
import random
import re
import sys
import time
from pathlib import Path

# 项目根目录
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import aiohttp
from dao import get_connection

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger(__name__)

# ── 同花顺页面解析 ──

_PAGE_INFO_RE = re.compile(r'class="page_info">(\d+)/(\d+)</span>')
_LAST_PAGE_RE = re.compile(r'class="changePage"\s+page="(\d+)"[^>]*>尾页</a>')
_STOCK_RE = re.compile(
    r'<td><a\s+href="http://stockpage\.10jqka\.com\.cn/(\d{6})/"'
    r'\s+target="_blank">\d{6}</a></td>\s*'
    r'<td><a\s+href="http://stockpage\.10jqka\.com\.cn/\d{6}"'
    r'\s+target="_blank">([^<]+)</a></td>',
    re.DOTALL,
)


def _get_total_pages(html: str) -> int:
    m = _PAGE_INFO_RE.search(html)
    if m:
        return int(m.group(2))
    m = _LAST_PAGE_RE.search(html)
    if m:
        return int(m.group(1))
    return 1


def _count_stocks_on_page(html: str) -> int:
    seen = set()
    for m in _STOCK_RE.finditer(html):
        seen.add(m.group(1))
    return len(seen)


def _clean_html(raw_bytes: bytes) -> str | None:
    if len(raw_bytes) < 500:
        return None
    try:
        return raw_bytes.decode("gbk")
    except UnicodeDecodeError:
        return raw_bytes.decode("gb2312", errors="replace")


async def _fetch_board_page_info(session: aiohttp.ClientSession,
                                  board_code: str) -> dict:
    """请求同花顺板块首页，返回 total_pages 和首页股票数。"""
    url = f"https://q.10jqka.com.cn/gn/detail/code/{board_code}/"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status in (403, 401):
                return {"error": f"HTTP {resp.status}", "total_pages": 0,
                        "first_page_stocks": 0}
            resp.raise_for_status()
            raw = await resp.read()
            html = _clean_html(raw)
            if not html:
                return {"error": "empty/short response", "total_pages": 0,
                        "first_page_stocks": 0}
            total_pages = _get_total_pages(html)
            first_page_count = _count_stocks_on_page(html)
            return {"total_pages": total_pages,
                    "first_page_stocks": first_page_count,
                    "error": None}
    except Exception as e:
        return {"error": str(e)[:120], "total_pages": 0,
                "first_page_stocks": 0}


def _get_db_board_stock_counts() -> list[dict]:
    """从数据库查询每个概念板块及其成分股数量。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT b.board_code, b.board_name,
                   COALESCE(s.cnt, 0) AS db_stock_count
            FROM stock_concept_board b
            LEFT JOIN (
                SELECT board_code, COUNT(*) AS cnt
                FROM stock_concept_board_stock
                GROUP BY board_code
            ) s ON b.board_code = s.board_code
            ORDER BY b.board_code
        """)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


async def validate(board_codes: list[str] | None = None,
                   top_n: int | None = None,
                   delay: float = 0.5) -> dict:
    """
    执行验证，返回结果摘要。

    Args:
        board_codes: 指定板块代码列表，None 表示全部
        top_n: 只检查前 N 个板块
        delay: 请求间隔（秒）
    """
    boards = _get_db_board_stock_counts()
    if board_codes:
        code_set = set(board_codes)
        boards = [b for b in boards if b["board_code"] in code_set]
    if top_n:
        boards = boards[:top_n]

    total = len(boards)
    print(f"\n{'='*72}")
    print(f"  概念板块成分股数量验证  (共 {total} 个板块)")
    print(f"{'='*72}\n")

    headers = {
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"),
        "Referer": "https://q.10jqka.com.cn/gn/",
    }

    matched = []
    mismatched = []
    errors = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for i, board in enumerate(boards):
            code = board["board_code"]
            name = board["board_name"]
            db_count = board["db_stock_count"]

            info = await _fetch_board_page_info(session, code)

            if info["error"]:
                errors.append({**board, **info})
                print(f"  [{i+1}/{total}] {code} {name:12s}  "
                      f"DB={db_count:4d}  ⚠ 请求失败: {info['error']}")
            else:
                tp = info["total_pages"]
                # 最后一页可能不满10只，用 (tp-1)*10 + first_page 作为下界估算
                # 但最准确的是 tp*10 作为上界
                expected_max = tp * 10
                # 如果只有1页，精确数就是首页股票数
                if tp == 1:
                    expected = info["first_page_stocks"]
                else:
                    expected = expected_max  # 上界估算

                diff = db_count - expected
                entry = {**board, **info, "expected": expected, "diff": diff}

                if tp == 1:
                    # 单页板块，精确比较
                    if db_count == expected:
                        matched.append(entry)
                        tag = "✓"
                    else:
                        mismatched.append(entry)
                        tag = "✗"
                else:
                    # 多页板块，DB数量应在 (expected-9) ~ expected 之间
                    # 因为最后一页可能不满10只
                    if (expected - 9) <= db_count <= expected:
                        matched.append(entry)
                        tag = "≈"
                    elif db_count > expected:
                        # DB比预期多，可能同花顺减少了成分股
                        mismatched.append(entry)
                        tag = "✗"
                    else:
                        mismatched.append(entry)
                        tag = "✗"

                status = (f"  [{i+1}/{total}] {code} {name:12s}  "
                          f"DB={db_count:4d}  同花顺≈{expected:4d} "
                          f"(pages={tp})")
                if tag == "✗":
                    print(f"{status}  {tag} 差异={diff:+d}")
                else:
                    print(f"{status}  {tag}")

            # 请求间隔
            if i < total - 1:
                await asyncio.sleep(delay + random.uniform(0, 0.2))

    # ── 汇总报告 ──
    print(f"\n{'='*72}")
    print(f"  验证结果汇总")
    print(f"{'='*72}")
    print(f"  总板块数:   {total}")
    print(f"  匹配/近似:  {len(matched)}")
    print(f"  数量不一致: {len(mismatched)}")
    print(f"  请求失败:   {len(errors)}")

    if mismatched:
        # 按差异绝对值排序
        mismatched.sort(key=lambda x: abs(x["diff"]), reverse=True)
        print(f"\n  ── 数量不一致的板块 (共{len(mismatched)}个) ──")
        print(f"  {'板块代码':10s} {'板块名称':14s} {'DB数量':>6s} "
              f"{'同花顺':>6s} {'差异':>6s} {'页数':>4s}")
        print(f"  {'-'*56}")
        for m in mismatched:
            print(f"  {m['board_code']:10s} {m['board_name']:14s} "
                  f"{m['db_stock_count']:6d} {m['expected']:6d} "
                  f"{m['diff']:+6d} {m['total_pages']:4d}")

    if errors:
        print(f"\n  ── 请求失败的板块 (共{len(errors)}个) ──")
        for e in errors[:10]:
            print(f"  {e['board_code']} {e['board_name']}: {e['error']}")
        if len(errors) > 10:
            print(f"  ... 还有 {len(errors)-10} 个")

    print()
    return {
        "total": total,
        "matched": len(matched),
        "mismatched": len(mismatched),
        "errors": len(errors),
        "mismatch_details": mismatched,
        "error_details": errors,
    }


def main():
    parser = argparse.ArgumentParser(description="验证概念板块成分股数量")
    parser.add_argument("--board", type=str, help="指定板块代码(逗号分隔)")
    parser.add_argument("--top", type=int, help="只检查前N个板块")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="请求间隔秒数(默认0.5)")
    args = parser.parse_args()

    board_codes = args.board.split(",") if args.board else None
    result = asyncio.run(validate(board_codes, args.top, args.delay))

    if result["mismatched"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
