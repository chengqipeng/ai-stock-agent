"""
从同花顺概念板块详情页抓取板块内所有成分股。

详情页: https://q.10jqka.com.cn/gn/detail/code/{board_code}/
分页URL: https://q.10jqka.com.cn/gn/detail/field/{field}/order/{order}/page/{page}/code/{board_code}
AJAX: 上述URL + /ajax/1/  (需要 hexin-v 请求头 + 登录cookie)

同花顺 q.10jqka.com.cn 服务端 SSR 仅渲染前5页（每页10只），超出返回空页面。
本模块采用两阶段策略:
  阶段1: curl_cffi SSR 获取 desc+asc 各5页 (最多100只，无需登录)
  阶段2: subprocess curl AJAX 获取全部页面 (需要登录cookie中的 hexin-v)

cookie 配置文件: common/files/10jqka_cookies.json
  - 关键字段: v (hexin-v, JS生成, 会过期需定期更新)
  - 更新方法: 浏览器登录后从开发者工具复制cookie

Usage:
    python -m service.jqka10.concept_board_stocks_10jqka 308832
    python -m service.jqka10.concept_board_stocks_10jqka --all
    python -m service.jqka10.concept_board_stocks_10jqka --force
"""
import asyncio
import json
import logging
import os
import random
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

IMPERSONATE = "chrome131"

# Cookie 配置文件路径
_COOKIE_FILE = (Path(__file__).resolve().parent.parent.parent
                / "common" / "files" / "10jqka_cookies.json")

# 匹配股票行 (SSR 完整页面)
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


# ── Cookie 管理 ──────────────────────────────────────────

def _load_cookies() -> dict[str, str]:
    """从 JSON 配置文件加载同花顺登录 cookie。"""
    cookie_file = os.environ.get("JQKA_COOKIE_FILE", str(_COOKIE_FILE))
    path = Path(cookie_file)
    if not path.exists():
        logger.warning("[板块成分股] cookie文件不存在: %s", path)
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        cookies = {k: v for k, v in data.items()
                   if not k.startswith("_") and isinstance(v, str)}
        logger.info("[板块成分股] 已加载 %d 个cookie", len(cookies))
        return cookies
    except Exception as e:
        logger.error("[板块成分股] 加载cookie失败: %s", e)
        return {}


def _build_cookie_string(cookies: dict[str, str]) -> str:
    """将 cookie dict 转为 curl -b 格式的字符串。"""
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


def _generate_hexin_v() -> Optional[str]:
    """
    使用 DrissionPage headless Chrome 自动生成 hexin-v cookie。
    chameleon.js 在浏览器中执行后会设置 v cookie。
    需要强制 IPv4 解析以避免 Nginx IPv6 封锁。
    每次调用会删除旧 v cookie 并随机选择 IPv4 地址以避免被封。
    """
    try:
        import socket
        import time as _time
        from DrissionPage import ChromiumPage, ChromiumOptions

        # 获取所有 IPv4 地址并随机选择
        results = socket.getaddrinfo("q.10jqka.com.cn", 443, socket.AF_INET)
        ips = list(set(r[4][0] for r in results))
        if not ips:
            logger.warning("[板块成分股] 无法解析 q.10jqka.com.cn IPv4")
            return None
        ipv4 = random.choice(ips)

        co = ChromiumOptions()
        co.headless()
        co.set_argument("--no-sandbox")
        co.set_argument("--disable-gpu")
        co.set_argument(f"--host-resolver-rules=MAP q.10jqka.com.cn {ipv4}")
        co.set_argument("--ignore-certificate-errors")

        page = ChromiumPage(co)
        # 删除旧 v cookie 以确保 chameleon.js 生成新值
        try:
            page.set.cookies.remove("v", domain=".10jqka.com.cn")
        except Exception:
            pass
        page.get("https://q.10jqka.com.cn/gn/")
        _time.sleep(4)

        v_cookie = None
        for c in page.cookies():
            if c["name"] == "v" and len(c["value"]) > 10:
                v_cookie = c["value"]
                break
        page.quit()

        if v_cookie:
            logger.info("[板块成分股] 自动生成 hexin-v: %s... (ip=%s)",
                        v_cookie[:20], ipv4)
        else:
            logger.warning("[板块成分股] 自动生成 hexin-v 失败 (ip=%s)", ipv4)
        return v_cookie

    except ImportError:
        logger.debug("[板块成分股] DrissionPage 未安装，无法自动生成 hexin-v")
        return None
    except Exception as e:
        logger.warning("[板块成分股] 自动生成 hexin-v 异常: %s", e)
        return None



# ── HTML 解析 ────────────────────────────────────────────

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


# ── 阶段1: curl_cffi SSR (无需登录, 最多5页/方向) ────────

def _build_ssr_url(board_code: str, page: int = 1,
                   order: str = "desc") -> str:
    """构建 SSR 页面 URL (不含 field 参数)。"""
    if page == 1 and order == "desc":
        return f"https://q.10jqka.com.cn/gn/detail/code/{board_code}/"
    return (f"https://q.10jqka.com.cn/gn/detail/order/{order}/"
            f"page/{page}/code/{board_code}")


async def _ssr_fetch_page(session: AsyncSession, board_code: str,
                          page: int, order: str = "desc",
                          retries: int = 2) -> Optional[str]:
    """curl_cffi SSR 获取单页。"""
    url = _build_ssr_url(board_code, page, order)
    for attempt in range(retries + 1):
        try:
            resp = await session.get(url, timeout=20)
            if resp.status_code in (403, 401):
                return None
            resp.raise_for_status()
            return _clean_html(resp.content)
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(1.0 + random.uniform(0, 0.5))
            else:
                logger.warning("[板块成分股] SSR异常 board=%s page=%d: %s",
                               board_code, page, e)
    return None


async def _ssr_fetch_all(board_code: str,
                         delay: float = 0.3) -> tuple[dict, int]:
    """
    阶段1: 用 curl_cffi SSR 获取 desc+asc 各5页。
    返回 (all_stocks_dict, total_pages)。
    """
    all_stocks: dict[str, str] = {}
    total_pages = 1

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        # desc pages 1-5
        for page in range(1, _MAX_SSR_PAGES + 1):
            html = await _ssr_fetch_page(session, board_code, page, "desc")
            if not html:
                break
            if page == 1:
                total_pages = _get_total_pages(html)
            for code, name in _parse_stocks(html):
                all_stocks[code] = name
            if page >= min(total_pages, _MAX_SSR_PAGES):
                break
            await asyncio.sleep(delay)

        if total_pages <= _MAX_SSR_PAGES:
            return all_stocks, total_pages

        # asc pages 1-5
        for page in range(1, _MAX_SSR_PAGES + 1):
            html = await _ssr_fetch_page(session, board_code, page, "asc")
            if not html:
                break
            for code, name in _parse_stocks(html):
                all_stocks[code] = name
            await asyncio.sleep(delay)

    return all_stocks, total_pages


# ── 阶段2: subprocess curl AJAX (需要登录cookie) ────────

def _curl_ajax_fetch(board_code: str, page: int, order: str,
                     cookie_str: str, hexin_v: str,
                     field: str = "199112") -> Optional[str]:
    """
    用系统 curl 发送 AJAX 请求获取单页。
    需要 hexin-v 请求头和登录 cookie。
    """
    url = (f"https://q.10jqka.com.cn/gn/detail/field/{field}/"
           f"order/{order}/page/{page}/ajax/1/code/{board_code}")
    cmd = [
        "curl", "-s", "--max-time", "20", url,
        "-H", "Accept: text/html, */*; q=0.01",
        "-H", f"Referer: https://q.10jqka.com.cn/gn/detail/code/{board_code}/",
        "-H", "X-Requested-With: XMLHttpRequest",
        "-H", f"hexin-v: {hexin_v}",
        "-b", cookie_str,
        "-H", ("User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/146.0.0.0 Safari/537.36"),
        "-H", "Sec-Fetch-Dest: empty",
        "-H", "Sec-Fetch-Mode: cors",
        "-H", "Sec-Fetch-Site: same-origin",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=25)
        raw = result.stdout
        if len(raw) < 500:
            return None
        try:
            return raw.decode("gbk")
        except UnicodeDecodeError:
            return raw.decode("gb2312", errors="replace")
    except Exception as e:
        logger.warning("[板块成分股] curl异常 board=%s page=%d: %s",
                       board_code, page, e)
        return None


def _ajax_fetch_all_pages(board_code: str, total_pages: int,
                          all_stocks: dict[str, str],
                          cookies: dict[str, str],
                          start_page: int = 0,
                          delay: float = 0.3) -> int:
    """
    阶段2: 用 subprocess curl AJAX 获取页面。
    直接修改 all_stocks dict。

    hexin-v 通常在 3-8 次请求后过期，过期后返回断点页码，
    由调用方重新生成 v 后继续。

    Args:
        start_page: 从哪一页开始（0 表示自动从 SSR 之后开始）

    Returns:
        下次应该从哪一页继续（v 过期时返回断点页码，全部完成返回 total_pages+1）
    """
    hexin_v = cookies.get("v", "")
    if not hexin_v:
        logger.warning("[板块成分股] cookie中缺少v(hexin-v)，无法获取AJAX数据")
        return start_page or (_MAX_SSR_PAGES + 1)

    cookie_str = _build_cookie_string(cookies)
    expected = total_pages * 10

    if start_page <= 0:
        start_page = _MAX_SSR_PAGES + 1

    for page in range(start_page, total_pages + 1):
        if len(all_stocks) >= expected:
            return total_pages + 1
        html = _curl_ajax_fetch(board_code, page, "desc",
                                cookie_str, hexin_v)
        if not html:
            logger.info("[板块成分股] AJAX page %d 无数据，v可能已过期", page)
            return page  # 返回当前页作为下次起点
        new = 0
        for code, name in _parse_stocks(html):
            if code not in all_stocks:
                all_stocks[code] = name
                new += 1
        if new > 0:
            logger.info("[板块成分股] AJAX page %d: +%d new, total=%d/%d",
                        page, new, len(all_stocks), expected)
        time.sleep(delay + random.uniform(0, 0.1))

    return total_pages + 1  # 全部完成


# ── 主入口 ───────────────────────────────────────────────

async def async_fetch_board_stocks(board_code: str,
                                   delay: float = 0.3) -> list[dict]:
    """
    异步抓取某个概念板块的全部成分股。

    两阶段策略:
      1. curl_cffi SSR: desc+asc 各5页 (最多100只, 无需登录)
      2. subprocess curl AJAX: 全部页面 (需要 hexin-v cookie)

    Returns:
        [{"stock_code": "300143", "stock_name": "盈康生命"}, ...]
    """
    # 阶段1: SSR
    all_stocks, total_pages = await _ssr_fetch_all(board_code, delay)
    expected = total_pages * 10

    logger.info("[板块成分股] SSR阶段 board=%s: %d只/%d页(≈%d只)",
                board_code, len(all_stocks), total_pages, expected)

    # 阶段2: AJAX (仅当 SSR 未覆盖全部时)
    if total_pages > _MAX_SSR_PAGES and len(all_stocks) < expected:
        cookies = _load_cookies()

        # hexin-v 每次只能用几页就过期，需要循环: 生成v -> 用到过期 -> 重新生成
        max_v_retries = 8  # v 每次约3-8页，8次足够覆盖60+页
        next_page = _MAX_SSR_PAGES + 1
        last_v = None

        for v_attempt in range(max_v_retries):
            if len(all_stocks) >= expected or next_page > total_pages:
                break

            # 生成新的 hexin-v
            auto_v = _generate_hexin_v()
            if not auto_v:
                logger.warning("[板块成分股] 无法生成hexin-v (attempt %d/%d)",
                               v_attempt + 1, max_v_retries)
                break

            # 如果生成了和上次相同的 v，跳过（会立即过期）
            if auto_v == last_v:
                logger.info("[板块成分股] 生成了相同的v，跳过 (attempt %d/%d)",
                            v_attempt + 1, max_v_retries)
                continue
            last_v = auto_v
            cookies["v"] = auto_v

            before = len(all_stocks)
            next_page = _ajax_fetch_all_pages(
                board_code, total_pages, all_stocks, cookies,
                start_page=next_page, delay=delay)
            gained = len(all_stocks) - before

            logger.info("[板块成分股] v_attempt %d/%d: +%d stocks, total=%d/%d, next_page=%d",
                        v_attempt + 1, max_v_retries, gained,
                        len(all_stocks), expected, next_page)

            if gained == 0:
                # v 可能立即过期，再试一次
                continue

    result = [{"stock_code": c, "stock_name": n}
              for c, n in all_stocks.items()]
    coverage = len(result) / max(expected, 1) * 100
    logger.info("[板块成分股] board=%s 总页数=%d 预期≈%d只 获取=%d只 覆盖率=%.1f%%",
                board_code, total_pages, expected, len(result), coverage)
    return result


def fetch_board_stocks(board_code: str, delay: float = 0.3) -> list[dict]:
    """同步接口：抓取某个概念板块的成分股。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run,
                                 async_fetch_board_stocks(board_code, delay))
            return future.result()
    else:
        return asyncio.run(async_fetch_board_stocks(board_code, delay))


def fetch_and_save_board_stocks(board_code: str, board_name: str = "",
                                delay: float = 0.3,
                                stocks: list[dict] | None = None) -> int:
    """抓取板块成分股并写入数据库。可传入已获取的 stocks 避免重复抓取。"""
    from dao.stock_concept_board_dao import batch_upsert_board_stocks
    if stocks is None:
        stocks = fetch_board_stocks(board_code, delay=delay)
    if not stocks:
        return 0
    return batch_upsert_board_stocks(board_code, board_name, stocks)


def fetch_and_save_all_boards_stocks(delay_page: float = 0.3,
                                     delay_board: float = 0.8,
                                     force: bool = False,
                                     incomplete_only: bool = False) -> dict:
    """
    遍历数据库中所有概念板块，抓取每个板块的成分股并写入。

    Args:
        force: 强制重新抓取所有板块
        incomplete_only: 只补全正好100只成分股的板块（很可能被SSR截断）
    """
    from dao.stock_concept_board_dao import (
        get_all_concept_boards, batch_upsert_board_stocks, get_board_stock_count
    )

    boards = get_all_concept_boards()
    total = len(boards)
    success = skipped = total_stocks = failed = 0

    mode = "force" if force else ("incomplete" if incomplete_only else "normal")
    print(f"[板块成分股] 共 {total} 个板块待处理 (mode={mode})")

    for i, board in enumerate(boards):
        board_code = board["board_code"]
        board_name = board["board_name"]

        existing = get_board_stock_count(board_code)

        if not force:
            if incomplete_only:
                # 只处理正好100只的板块（被SSR截断的）
                if existing != 100:
                    skipped += 1
                    total_stocks += existing
                    continue
                print(f"  [{i+1}/{total}] {board_code} {board_name} -> "
                      f"当前{existing}只, 补全中...")
            else:
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
            gained = len(stocks) - existing if existing else len(stocks)
            print(f"  [{i+1}/{total}] {board_code} {board_name} -> "
                  f"{len(stocks)}只成分股"
                  f"{f' (+{gained}新增)' if gained > 0 else ''}")
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

    args = sys.argv[1:]
    if "--all" in args or "--force" in args or "--incomplete" in args:
        force = "--force" in args
        incomplete = "--incomplete" in args
        result = fetch_and_save_all_boards_stocks(
            force=force, incomplete_only=incomplete)
        print(f"\n完成: 新抓取{result['success']}个, 跳过{result['skipped']}个, "
              f"失败{result['failed']}个 / 共{result['total_boards']}个板块, "
              f"累计{result['total_stocks']}只成分股")
    else:
        board_code = args[0] if args else "308832"
        print(f"抓取板块 {board_code} 的成分股...")
        stocks = fetch_board_stocks(board_code)
        print(f"\n共 {len(stocks)} 只成分股:")
        for i, s in enumerate(stocks):
            print(f"  {i+1}. {s['stock_code']} {s['stock_name']}")

        if stocks:
            print(f"\n写入数据库...")
            count = fetch_and_save_board_stocks(board_code, stocks=stocks)
            print(f"完成，写入 {count} 条记录")

# # 只补全正好100只的板块（约161个）
# python -m service.jqka10.concept_board_stocks_10jqka --incomplete
#
# # 强制全部重新抓取
# python -m service.jqka10.concept_board_stocks_10jqka --force
#
# # 只抓取没有数据的板块
# python -m service.jqka10.concept_board_stocks_10jqka --all

if __name__ == "__main__":
    main()
