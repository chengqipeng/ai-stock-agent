"""
从同花顺概念板块详情页抓取板块内所有成分股。

三阶段策略:
  阶段1: curl_cffi SSR 多字段轮换 desc+asc 各5页 (最多500只，无需登录)
  阶段2: subprocess curl AJAX 获取全部页面 (需要 hexin-v cookie)
  阶段3: curl_cffi AJAX 回退 (当 Playwright 生成 hexin-v 失败时)

Usage:
    python -m service.jqka10.concept_board_stocks_10jqka 308832
    python -m service.jqka10.concept_board_stocks_10jqka --all
    python -m service.jqka10.concept_board_stocks_10jqka --force
"""
import asyncio
import concurrent.futures
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

_COOKIE_FILE = (Path(__file__).resolve().parent.parent.parent
                / "common" / "files" / "10jqka_cookies.json")

_STOCK_RE = re.compile(
    r'<td><a\s+href="http://stockpage\.10jqka\.com\.cn/(\d{6})/"'
    r'\s+target="_blank">\d{6}</a></td>\s*'
    r'<td><a\s+href="http://stockpage\.10jqka\.com\.cn/\d{6}"'
    r'\s+target="_blank">([^<]+)</a></td>',
    re.DOTALL,
)

_PAGE_INFO_RE = re.compile(r'class="page_info">(\d+)/(\d+)</span>')
_LAST_PAGE_RE = re.compile(r'class="changePage"\s+page="(\d+)"[^>]*>尾页</a>')

_MAX_SSR_PAGES = 5

# SSR 多字段轮换: 不同排序字段获取不同的股票子集
_SSR_FIELDS = [
    ("199112", "涨跌幅"),
    ("19913",  "成交量"),
    ("1968584", "换手率"),
    ("3475914", "总市值"),
    ("3541450", "流通市值"),
]


# ── Cookie 管理 ──────────────────────────────────────────

def _load_cookies() -> dict[str, str]:
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
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


# ── hexin-v 生成器 ───────────────────────────────────────

class _HexinVGenerator:
    _UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
           "AppleWebKit/537.36 (KHTML, like Gecko) "
           "Chrome/146.0.0.0 Safari/537.36")

    _URLS = [
        "https://q.10jqka.com.cn/gn/",
        "https://q.10jqka.com.cn/thshy/",
        "https://q.10jqka.com.cn/",
    ]

    def __init__(self):
        self._browser = None
        self._playwright = None
        self._pw_context_manager = None

    def _ensure_browser(self) -> bool:
        if self._browser is not None:
            try:
                if self._browser.is_connected():
                    return True
            except Exception:
                pass
            self._cleanup_sync()
        try:
            from playwright.sync_api import sync_playwright
            self._pw_context_manager = sync_playwright()
            self._playwright = self._pw_context_manager.start()
            self._browser = self._playwright.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            logger.info("[hexin-v] Playwright Chrome 启动成功")
            return True
        except ImportError:
            logger.debug("[hexin-v] Playwright 未安装")
            return False
        except Exception as e:
            logger.warning("[hexin-v] Playwright Chrome 启动失败: %s", e)
            self._cleanup_sync()
            return False

    def generate(self) -> Optional[str]:
        for attempt in range(2):
            if not self._ensure_browser():
                return None
            ctx = None
            try:
                ctx = self._browser.new_context(user_agent=self._UA)
                page = ctx.new_page()
                page.add_init_script(
                    'Object.defineProperty(navigator, "webdriver", '
                    '{get: () => undefined})')
                url = self._URLS[attempt % len(self._URLS)]
                page.goto(url, timeout=20000)
                page.wait_for_timeout(5000)
                for c in ctx.cookies():
                    if c["name"] == "v" and len(c["value"]) > 10:
                        logger.info("[hexin-v] 生成成功 (url=%s): %s...",
                                    url, c["value"][:20])
                        return c["value"]
                logger.warning("[hexin-v] 未找到v cookie (attempt %d, url=%s)",
                               attempt + 1, url)
            except Exception as e:
                logger.warning("[hexin-v] 生成异常 (attempt %d): %s",
                               attempt + 1, e)
            finally:
                if ctx:
                    try:
                        ctx.close()
                    except Exception:
                        pass
            self._cleanup_sync()
        return None

    def _cleanup_sync(self):
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._pw_context_manager:
                self._pw_context_manager.__exit__(None, None, None)
        except Exception:
            pass
        self._browser = None
        self._playwright = None
        self._pw_context_manager = None

    def quit(self):
        self._cleanup_sync()


_v_generator = _HexinVGenerator()


def _generate_hexin_v() -> Optional[str]:
    """用 ThreadPoolExecutor 隔离 Playwright Sync API，避免 asyncio 冲突。"""
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_v_generator.generate)
            return future.result(timeout=60)
    except concurrent.futures.TimeoutError:
        logger.warning("[hexin-v] 生成超时(60s)")
        return None
    except Exception as e:
        logger.warning("[hexin-v] 生成失败: %s", e)
        return None


# ── HTML 解析 ────────────────────────────────────────────

def _clean_html(raw_bytes: bytes) -> Optional[str]:
    if len(raw_bytes) < 500:
        return None
    try:
        return raw_bytes.decode("gbk")
    except UnicodeDecodeError:
        return raw_bytes.decode("gb2312", errors="replace")


def _get_total_pages(html: str) -> int:
    m = _PAGE_INFO_RE.search(html)
    if m:
        return int(m.group(2))
    m = _LAST_PAGE_RE.search(html)
    if m:
        return int(m.group(1))
    return 1


def _parse_stocks(html: str) -> list[tuple[str, str]]:
    results = []
    seen = set()
    for m in _STOCK_RE.finditer(html):
        code = m.group(1).strip()
        name = m.group(2).strip()
        if code not in seen:
            seen.add(code)
            results.append((code, name))
    return results


# ── 阶段1: curl_cffi SSR 多字段轮换 ─────────────────────

def _build_ssr_url(board_code: str, page: int = 1,
                   order: str = "desc", field: str = "") -> str:
    if page == 1 and order == "desc" and not field:
        return f"https://q.10jqka.com.cn/gn/detail/code/{board_code}/"
    parts = "https://q.10jqka.com.cn/gn/detail"
    if field:
        parts += f"/field/{field}"
    parts += f"/order/{order}/page/{page}/code/{board_code}"
    return parts


async def _ssr_fetch_page(session: AsyncSession, board_code: str,
                          page: int, order: str = "desc",
                          field: str = "",
                          retries: int = 2) -> Optional[str]:
    url = _build_ssr_url(board_code, page, order, field)
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
    """阶段1: 多字段轮换 SSR。每个字段 desc+asc 各5页。"""
    all_stocks: dict[str, str] = {}
    total_pages = 1

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        for field_idx, (field_code, field_name) in enumerate(_SSR_FIELDS):
            before = len(all_stocks)
            f = field_code if field_idx > 0 else ""

            for page in range(1, _MAX_SSR_PAGES + 1):
                html = await _ssr_fetch_page(
                    session, board_code, page, "desc", f)
                if not html:
                    break
                if page == 1 and field_idx == 0:
                    total_pages = _get_total_pages(html)
                for code, name in _parse_stocks(html):
                    all_stocks[code] = name
                if page >= min(total_pages, _MAX_SSR_PAGES):
                    break
                await asyncio.sleep(delay)

            if total_pages <= _MAX_SSR_PAGES:
                break

            for page in range(1, _MAX_SSR_PAGES + 1):
                html = await _ssr_fetch_page(
                    session, board_code, page, "asc", f)
                if not html:
                    break
                for code, name in _parse_stocks(html):
                    all_stocks[code] = name
                await asyncio.sleep(delay)

            gained = len(all_stocks) - before
            logger.info("[板块成分股] SSR字段[%s] board=%s: +%d只, 累计%d只",
                        field_name, board_code, gained, len(all_stocks))

    return all_stocks, total_pages


# ── 阶段2: subprocess curl AJAX ─────────────────────────

def _curl_ajax_fetch(board_code: str, page: int, order: str,
                     cookie_str: str, hexin_v: str,
                     field: str = "199112") -> Optional[str]:
    url = (f"https://q.10jqka.com.cn/gn/detail/field/{field}/"
           f"order/{order}/page/{page}/ajax/1/code/{board_code}")
    referer = f"https://q.10jqka.com.cn/gn/detail/code/{board_code}/"
    ua = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
          "AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/146.0.0.0 Safari/537.36")
    cmd = [
        "curl", "-s", "--max-time", "20", url,
        "-H", "Accept: text/html, */*; q=0.01",
        "-H", f"Referer: {referer}",
        "-H", "X-Requested-With: XMLHttpRequest",
        "-H", f"hexin-v: {hexin_v}",
        "-b", cookie_str,
        "-H", f"User-Agent: {ua}",
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
    """阶段2: subprocess curl AJAX。返回下次起始页码。"""
    hexin_v = cookies.get("v", "")
    if not hexin_v:
        logger.warning("[板块成分股] cookie中缺少v(hexin-v)")
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
            return page
        new = 0
        for code, name in _parse_stocks(html):
            if code not in all_stocks:
                all_stocks[code] = name
                new += 1
        if new > 0:
            logger.info("[板块成分股] AJAX page %d: +%d new, total=%d/%d",
                        page, new, len(all_stocks), expected)
        time.sleep(delay + random.uniform(0, 0.1))

    return total_pages + 1


# ── 阶段3: curl_cffi AJAX 回退 ──────────────────────────

async def _cffi_ajax_fetch_page(session: AsyncSession, board_code: str,
                                page: int, cookies: dict[str, str],
                                hexin_v: str,
                                field: str = "199112") -> Optional[str]:
    url = (f"https://q.10jqka.com.cn/gn/detail/field/{field}/"
           f"order/desc/page/{page}/ajax/1/code/{board_code}")
    headers = {
        "Accept": "text/html, */*; q=0.01",
        "Referer": f"https://q.10jqka.com.cn/gn/detail/code/{board_code}/",
        "X-Requested-With": "XMLHttpRequest",
        "hexin-v": hexin_v,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    try:
        resp = await session.get(url, headers=headers,
                                 cookies=cookies, timeout=20)
        if resp.status_code in (403, 401):
            return None
        resp.raise_for_status()
        return _clean_html(resp.content)
    except Exception as e:
        logger.warning("[板块成分股] cffi-ajax异常 board=%s page=%d: %s",
                       board_code, page, e)
        return None


async def _cffi_ajax_fallback(board_code: str, total_pages: int,
                              all_stocks: dict[str, str],
                              cookies: dict[str, str],
                              delay: float = 0.5) -> None:
    """阶段3: 当 Playwright hexin-v 完全失败时，用 curl_cffi TLS 指纹模拟。"""
    hexin_v = cookies.get("v", "")
    if not hexin_v:
        logger.info("[板块成分股] cffi-ajax回退: cookie中无v值，跳过")
        return

    expected = total_pages * 10
    logger.info("[板块成分股] 尝试cffi-ajax回退 board=%s (v=%s...)",
                board_code, hexin_v[:20] if hexin_v else "empty")

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        consecutive_empty = 0
        for page in range(1, total_pages + 1):
            if len(all_stocks) >= expected:
                break
            html = await _cffi_ajax_fetch_page(
                session, board_code, page, cookies, hexin_v)
            if not html:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("[板块成分股] cffi-ajax连续%d页空数据，停止",
                                consecutive_empty)
                    break
                continue
            consecutive_empty = 0
            for code, name in _parse_stocks(html):
                all_stocks[code] = name
            await asyncio.sleep(delay)

    logger.info("[板块成分股] cffi-ajax回退完成 board=%s: 累计%d只",
                board_code, len(all_stocks))


# ── 主入口 ───────────────────────────────────────────────

async def async_fetch_board_stocks(board_code: str,
                                   delay: float = 0.3) -> list[dict]:
    """
    异步抓取某个概念板块的全部成分股。三阶段策略。
    Returns: [{"stock_code": "300143", "stock_name": "盈康生命"}, ...]
    """
    # 阶段1: SSR 多字段轮换
    all_stocks, total_pages = await _ssr_fetch_all(board_code, delay)
    expected = total_pages * 10

    logger.info("[板块成分股] SSR阶段 board=%s: %d只/%d页(≈%d只)",
                board_code, len(all_stocks), total_pages, expected)

    # 阶段2+3: AJAX
    hexin_v_ok = False
    if total_pages > _MAX_SSR_PAGES and len(all_stocks) < expected:
        cookies = _load_cookies()

        remaining_pages = total_pages - _MAX_SSR_PAGES
        max_v_retries = max(8, remaining_pages // 3 + 2)
        next_page = _MAX_SSR_PAGES + 1
        last_v = None
        consecutive_fails = 0

        for v_attempt in range(max_v_retries):
            if len(all_stocks) >= expected or next_page > total_pages:
                break
            if consecutive_fails >= 3:
                logger.warning("[板块成分股] 连续%d次生成hexin-v失败，尝试回退",
                               consecutive_fails)
                break

            auto_v = _generate_hexin_v()
            if not auto_v:
                consecutive_fails += 1
                logger.warning(
                    "[板块成分股] 无法生成hexin-v "
                    "(attempt %d/%d, consecutive_fails=%d)",
                    v_attempt + 1, max_v_retries, consecutive_fails)
                time.sleep(2)
                continue

            if auto_v == last_v:
                logger.info("[板块成分股] 生成了相同的v，跳过")
                continue

            hexin_v_ok = True
            consecutive_fails = 0
            last_v = auto_v
            cookies["v"] = auto_v

            before = len(all_stocks)
            next_page = _ajax_fetch_all_pages(
                board_code, total_pages, all_stocks, cookies,
                start_page=next_page, delay=delay)
            gained = len(all_stocks) - before
            logger.info(
                "[板块成分股] v_attempt %d/%d: +%d stocks, "
                "total=%d/%d, next_page=%d",
                v_attempt + 1, max_v_retries, gained,
                len(all_stocks), expected, next_page)

        # 阶段3: cffi-ajax 回退
        if not hexin_v_ok and len(all_stocks) < expected:
            logger.info("[板块成分股] Playwright hexin-v 全部失败，启动cffi-ajax回退")
            await _cffi_ajax_fallback(
                board_code, total_pages, all_stocks, cookies, delay=0.5)

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
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run,
                                 async_fetch_board_stocks(board_code, delay))
            return future.result()
    else:
        return asyncio.run(async_fetch_board_stocks(board_code, delay))


def fetch_and_save_board_stocks(board_code: str, board_name: str = "",
                                delay: float = 0.3,
                                stocks: list[dict] | None = None) -> int:
    """抓取板块成分股并写入数据库。"""
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
    """遍历数据库中所有概念板块，抓取每个板块的成分股并写入。"""
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


def _cleanup():
    _v_generator.quit()


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    args = sys.argv[1:]
    try:
        if "--all" in args or "--force" in args or "--incomplete" in args:
            force = "--force" in args
            incomplete = "--incomplete" in args
            result = fetch_and_save_all_boards_stocks(
                force=force, incomplete_only=incomplete)
            print(f"\n完成: 新抓取{result['success']}个, "
                  f"跳过{result['skipped']}个, "
                  f"失败{result['failed']}个 / "
                  f"共{result['total_boards']}个板块, "
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
    finally:
        _cleanup()


if __name__ == "__main__":
    main()
