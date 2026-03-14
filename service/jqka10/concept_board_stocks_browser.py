"""
基于 browser_web 的概念板块成分股抓取器。

利用 BrowserManager 的真实 Chrome 实例 + 反检测注入 + 登录 cookie 注入，
直接在浏览器中翻页抓取全部成分股。

核心流程:
  1. 启动真实 Chrome (BrowserManager, 含 stealth 反检测)
  2. 访问 10jqka 主页，让 chameleon.js 生成 hexin-v cookie
  3. 注入 10jqka_cookies.json 中的登录 cookie (user/ticket/sess_tk 等)
  4. 访问板块详情页，逐页点击翻页抓取

数据源: https://q.10jqka.com.cn/gn/detail/code/{board_code}/

Usage:
    python -m service.jqka10.concept_board_stocks_browser 300008
    python -m service.jqka10.concept_board_stocks_browser --all
    python -m service.jqka10.concept_board_stocks_browser --incomplete
    python -m service.jqka10.concept_board_stocks_browser 300008 --no-headless
"""
import json
import logging
import random
import re
import sys
import time
from pathlib import Path
from typing import Optional

from browser_web.browser_manager import BrowserManager, BrowserProfile

logger = logging.getLogger(__name__)

_COOKIE_FILE = (Path(__file__).resolve().parent.parent.parent
                / "common" / "files" / "10jqka_cookies.json")

# 匹配股票行
_STOCK_RE = re.compile(
    r'<td><a\s+href="http://stockpage\.10jqka\.com\.cn/(\d{6})/"'
    r'\s+target="_blank">\d{6}</a></td>\s*'
    r'<td><a\s+href="http://stockpage\.10jqka\.com\.cn/\d{6}"'
    r'\s+target="_blank">([^<]+)</a></td>',
    re.DOTALL,
)
_PAGE_INFO_RE = re.compile(r'class="page_info">(\d+)/(\d+)</span>')


def _parse_stocks_from_html(html: str) -> list[tuple[str, str]]:
    """从 HTML 中解析股票列表 [(code, name), ...]"""
    results, seen = [], set()
    for m in _STOCK_RE.finditer(html):
        code, name = m.group(1).strip(), m.group(2).strip()
        if code not in seen:
            seen.add(code)
            results.append((code, name))
    return results


def _get_total_pages(html: str) -> int:
    m = _PAGE_INFO_RE.search(html)
    return int(m.group(2)) if m else 1


def _load_login_cookies() -> list[dict]:
    """从 10jqka_cookies.json 加载登录 cookie，转为 Playwright 格式。"""
    if not _COOKIE_FILE.exists():
        logger.warning("[browser-scraper] cookie 文件不存在: %s", _COOKIE_FILE)
        return []
    try:
        with open(_COOKIE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error("[browser-scraper] 加载 cookie 失败: %s", e)
        return []

    cookies = []
    for k, v in data.items():
        if k.startswith("_") or not isinstance(v, str) or not v:
            continue
        cookies.append({
            "name": k, "value": v,
            "domain": ".10jqka.com.cn",
            "path": "/",
        })
    logger.info("[browser-scraper] 加载 %d 个登录 cookie", len(cookies))
    return cookies


class ConceptBoardBrowserScraper:
    """
    使用 BrowserManager 真实 Chrome + 登录 cookie 抓取概念板块成分股。

    with ConceptBoardBrowserScraper() as scraper:
        stocks = scraper.fetch_board_stocks("300008")
    """

    def __init__(self, headless: bool = True, cdp_port: int = 18801):
        self._manager = BrowserManager(BrowserProfile(
            name="concept-scraper",
            cdp_port=cdp_port,
            headless=headless,
        ))
        self._started = False

    def start(self):
        if self._started:
            return
        self._manager.start()
        self._started = True

        page = self._manager.get_active_page()

        # 1) 先访问主页，让 chameleon.js 生成 hexin-v
        page.goto("https://q.10jqka.com.cn/gn/", timeout=20000)
        page.wait_for_timeout(3000)

        # 2) 注入登录 cookie
        login_cookies = _load_login_cookies()
        if login_cookies:
            page.context.add_cookies(login_cookies)
            logger.info("[browser-scraper] 登录 cookie 已注入")

        # 3) 刷新页面使 cookie 生效
        page.reload(timeout=15000)
        page.wait_for_timeout(2000)
        logger.info("[browser-scraper] 浏览器已启动，登录态已就绪")

    def stop(self):
        if self._started:
            self._manager.stop()
            self._started = False

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.stop()

    def fetch_board_stocks(self, board_code: str,
                           delay: float = 0.5) -> list[dict]:
        """
        抓取某个概念板块的全部成分股。

        Returns:
            [{"stock_code": "300143", "stock_name": "盈康生命"}, ...]
        """
        if not self._started:
            raise RuntimeError("Scraper not started. Call start() first.")

        page = self._manager.get_active_page()
        url = f"https://q.10jqka.com.cn/gn/detail/code/{board_code}/"

        try:
            page.goto(url, timeout=20000)
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            page.wait_for_timeout(3000)

        try:
            page.wait_for_selector("table.m-table", timeout=8000)
        except Exception:
            logger.warning("[browser-scraper] board=%s 表格未加载", board_code)

        all_stocks: dict[str, str] = {}
        html = page.content()
        total_pages = _get_total_pages(html)

        for code, name in _parse_stocks_from_html(html):
            all_stocks[code] = name

        logger.info("[browser-scraper] board=%s 第1页: %d只, 共%d页",
                    board_code, len(all_stocks), total_pages)

        # 连续无新增计数，超过阈值则停止
        empty_streak = 0
        max_empty = 3

        for pg in range(2, total_pages + 1):
            time.sleep(delay + random.uniform(0, 0.2))

            if not self._navigate_to_page(page, pg, board_code):
                logger.warning("[browser-scraper] board=%s 翻到第%d页失败",
                               board_code, pg)
                break

            html = page.content()
            before = len(all_stocks)
            for code, name in _parse_stocks_from_html(html):
                all_stocks[code] = name
            gained = len(all_stocks) - before

            if gained == 0:
                empty_streak += 1
                if empty_streak >= max_empty:
                    logger.warning(
                        "[browser-scraper] board=%s 连续%d页无新增，停止",
                        board_code, max_empty)
                    break
            else:
                empty_streak = 0

            logger.info("[browser-scraper] board=%s 第%d/%d页: +%d只, 累计%d只",
                        board_code, pg, total_pages, gained, len(all_stocks))

        expected = total_pages * 10
        coverage = len(all_stocks) / max(expected, 1) * 100
        logger.info("[browser-scraper] board=%s 完成: %d只/%d页 覆盖率=%.1f%%",
                    board_code, len(all_stocks), total_pages, coverage)

        return [{"stock_code": c, "stock_name": n}
                for c, n in all_stocks.items()]

    def _navigate_to_page(self, page, target_page: int,
                          board_code: str) -> bool:
        """
        翻页导航。

        同花顺翻页是 AJAX 局部刷新，点击"下一页"后 JS 替换表格 HTML。
        优先用 JS 模拟点击翻页链接，失败则直接 URL 导航。
        """
        # 方式1: 用 JS 触发翻页（模拟页面自身的翻页行为）
        try:
            clicked = page.evaluate(f"""
                () => {{
                    // 找到"下一页"按钮并点击
                    const links = document.querySelectorAll('a.changePage');
                    for (const a of links) {{
                        if (a.getAttribute('page') === '{target_page}'
                            || a.title === '下一页') {{
                            a.click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            """)
            if clicked:
                # 等待 AJAX 完成：page_info 文本更新
                try:
                    page.wait_for_function(
                        f'document.querySelector(".page_info") && '
                        f'document.querySelector(".page_info")'
                        f'.textContent.startsWith("{target_page}/")',
                        timeout=8000,
                    )
                except Exception:
                    # 即使超时也继续，可能内容已更新
                    page.wait_for_timeout(2000)
                return True
        except Exception as e:
            logger.debug("[browser-scraper] JS 翻页失败: %s", e)

        # 方式2: 直接 URL 导航（完整页面加载，较慢但可靠）
        try:
            page_url = (f"https://q.10jqka.com.cn/gn/detail/"
                        f"order/desc/page/{target_page}/code/{board_code}")
            page.goto(page_url, timeout=15000)
            page.wait_for_timeout(2000)
            return True
        except Exception as e:
            logger.warning("[browser-scraper] URL 导航失败: %s", e)
            return False


# ── 便捷函数 ─────────────────────────────────────────────

def fetch_board_stocks_via_browser(board_code: str,
                                   headless: bool = True,
                                   delay: float = 0.5) -> list[dict]:
    """单次抓取（自动管理浏览器生命周期）。批量请用 Scraper 类复用浏览器。"""
    with ConceptBoardBrowserScraper(headless=headless) as scraper:
        return scraper.fetch_board_stocks(board_code, delay=delay)


def fetch_and_save_board_stocks_via_browser(
    board_code: str, board_name: str = "",
    headless: bool = True, delay: float = 0.5,
) -> int:
    """抓取并写入数据库。"""
    from dao.stock_concept_board_dao import batch_upsert_board_stocks
    stocks = fetch_board_stocks_via_browser(board_code, headless, delay)
    if not stocks:
        return 0
    return batch_upsert_board_stocks(board_code, board_name, stocks)


def fetch_and_save_all_boards_via_browser(
    delay_page: float = 0.5, delay_board: float = 1.0,
    headless: bool = True, force: bool = False,
    incomplete_only: bool = False,
) -> dict:
    """遍历所有概念板块，用浏览器抓取成分股并写入数据库。"""
    from dao.stock_concept_board_dao import (
        get_all_concept_boards, batch_upsert_board_stocks, get_board_stock_count,
    )

    boards = get_all_concept_boards()
    total = len(boards)
    success = skipped = total_stocks = failed = 0
    mode = "force" if force else ("incomplete" if incomplete_only else "normal")
    print(f"[browser-scraper] 共 {total} 个板块 (mode={mode})")

    with ConceptBoardBrowserScraper(headless=headless) as scraper:
        for i, board in enumerate(boards):
            board_code = board["board_code"]
            board_name = board["board_name"]
            existing = get_board_stock_count(board_code)

            if not force:
                if incomplete_only and existing != 100:
                    skipped += 1; total_stocks += existing; continue
                elif not incomplete_only and existing > 0:
                    skipped += 1; total_stocks += existing; continue

            try:
                stocks = scraper.fetch_board_stocks(board_code, delay=delay_page)
                if stocks:
                    batch_upsert_board_stocks(board_code, board_name, stocks)
                    success += 1; total_stocks += len(stocks)
                    gained = len(stocks) - existing if existing else len(stocks)
                    print(f"  [{i+1}/{total}] {board_code} {board_name} "
                          f"-> {len(stocks)}只"
                          f"{f' (+{gained}新增)' if gained > 0 else ''}")
                else:
                    failed += 1
                    print(f"  [{i+1}/{total}] {board_code} {board_name} -> 失败")
            except Exception as e:
                failed += 1
                logger.error("[browser-scraper] %s %s: %s", board_code, board_name, e)

            if i < total - 1:
                time.sleep(delay_board + random.uniform(0, 0.3))

    return {"total_boards": total, "success": success, "skipped": skipped,
            "failed": failed, "total_stocks": total_stocks}


# ── CLI ──────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    args = sys.argv[1:]
    headless = "--no-headless" not in args

    if "--all" in args or "--force" in args or "--incomplete" in args:
        result = fetch_and_save_all_boards_via_browser(
            headless=headless,
            force="--force" in args,
            incomplete_only="--incomplete" in args,
        )
        print(f"\n完成: 新抓取{result['success']}个, "
              f"跳过{result['skipped']}个, "
              f"失败{result['failed']}个 / "
              f"共{result['total_boards']}个板块, "
              f"累计{result['total_stocks']}只成分股")
    else:
        board_code = next((a for a in args if a.isdigit()), "300008")
        print(f"抓取板块 {board_code} 的成分股 "
              f"(headless={'是' if headless else '否'})...")
        stocks = fetch_board_stocks_via_browser(board_code, headless=headless)
        print(f"\n共 {len(stocks)} 只成分股:")
        for i, s in enumerate(stocks):
            print(f"  {i+1}. {s['stock_code']} {s['stock_name']}")

        if stocks:
            print(f"\n写入数据库...")
            from dao.stock_concept_board_dao import batch_upsert_board_stocks
            count = batch_upsert_board_stocks(board_code, "", stocks)
            print(f"完成，写入 {count} 条记录")


if __name__ == "__main__":
    main()
