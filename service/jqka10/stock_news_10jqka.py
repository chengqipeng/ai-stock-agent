"""
同花顺个股新闻公告抓取模块

数据来源：https://stockpage.10jqka.com.cn/{code}/#xwgg
四个维度：
  1. 公司新闻 (news)     — stat="f10_spqk_gsxw"
  2. 公司公告 (notice)   — stat="f10_spqk_gsgg"
  3. 行业资讯 (industry) — stat="f10_spqk_hyzx"
  4. 研究报告 (report)   — stat="f10_spqk_yjbg"

HTML 结构：
  <ul class="news_list stat" stat="f10_spqk_gsxw">
    <li class="clearfix">
      <span class="news_title fl"><a href="...">标题</a></span>
      <span class="news_date"><em>03-27 14:38</em></span>
    </li>
  </ul>

使用 curl_cffi 模拟浏览器 TLS 指纹绕过反爬。
"""

import asyncio
import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Literal

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

IMPERSONATE = "chrome131"
_CST = ZoneInfo("Asia/Shanghai")

NewsType = Literal["news", "notice", "industry", "report"]

TYPE_LABEL: dict[NewsType, str] = {
    "news": "公司新闻",
    "notice": "公司公告",
    "industry": "行业资讯",
    "report": "研究报告",
}

# stat 属性 → news_type 映射
_STAT_MAP: dict[str, NewsType] = {
    "f10_spqk_gsxw": "news",
    "f10_spqk_gsgg": "notice",
    "f10_spqk_hyzx": "industry",
    "f10_spqk_yjbg": "report",
}


def _normalize_date(date_str: str) -> tuple[str, str]:
    """将日期字符串标准化为 (publish_date, publish_time)

    输入格式：
      - "03-27 14:38"  → ("2026-03-27", "14:38")
      - "2026-03-28"   → ("2026-03-28", "")
      - "03-23 16:25"  → ("2026-03-23", "16:25")
    """
    date_str = date_str.strip()
    if not date_str:
        return "", ""

    # 格式: YYYY-MM-DD
    m = re.match(r'^(\d{4}-\d{2}-\d{2})$', date_str)
    if m:
        return m.group(1), ""

    # 格式: MM-DD HH:MM
    m = re.match(r'^(\d{2}-\d{2})\s+(\d{2}:\d{2})$', date_str)
    if m:
        year = datetime.now(_CST).year
        return f"{year}-{m.group(1)}", m.group(2)

    # 格式: YYYY-MM-DD HH:MM
    m = re.match(r'^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})$', date_str)
    if m:
        return m.group(1), m.group(2)

    return date_str, ""


def _parse_stockpage_html(html: str, stock_code: str) -> dict[str, list[dict]]:
    """解析同花顺个股主页 HTML，提取四类新闻数据"""
    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, list[dict]] = {
        "news": [], "notice": [], "industry": [], "report": [],
    }

    # 查找所有 <ul> 标签，通过 stat 属性识别新闻板块
    for stat_val, news_type in _STAT_MAP.items():
        ul = soup.find("ul", attrs={"stat": stat_val})
        if not ul:
            logger.debug("[%s] 未找到 stat=%s 的新闻列表", stock_code, stat_val)
            continue

        items = []
        for li in ul.find_all("li", class_="clearfix"):
            # 提取标题和链接
            title_span = li.find("span", class_="news_title")
            if not title_span:
                continue
            a_tag = title_span.find("a")
            if not a_tag:
                continue

            title = a_tag.get_text(strip=True)
            href = a_tag.get("href", "")

            if not title or len(title) < 2:
                continue

            # 确保 URL 完整
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://stockpage.10jqka.com.cn" + href

            # 提取日期
            date_span = li.find("span", class_="news_date")
            date_text = ""
            if date_span:
                em = date_span.find("em")
                date_text = em.get_text(strip=True) if em else date_span.get_text(strip=True)

            publish_date, publish_time = _normalize_date(date_text)

            # 如果没有解析到日期，尝试从 URL 中提取
            if not publish_date:
                url_date = re.search(r'/(\d{4})(\d{2})(\d{2})/', href)
                if url_date:
                    publish_date = f"{url_date.group(1)}-{url_date.group(2)}-{url_date.group(3)}"

            items.append({
                "news_type": news_type,
                "title": title,
                "url": href,
                "publish_date": publish_date,
                "publish_time": publish_time,
                "source": "同花顺",
            })

        result[news_type] = items
        logger.debug("[%s] 解析 %s: %d 条", stock_code, TYPE_LABEL[news_type], len(items))

    return result


def _extract_code(stock_code_normalize: str) -> str:
    """002371.SZ → 002371"""
    return stock_code_normalize.split(".")[0]


async def fetch_stock_news(stock_code_normalize: str, session: AsyncSession = None) -> dict[str, list[dict]]:
    """抓取个股主页的新闻公告数据

    Args:
        stock_code_normalize: 标准化代码如 002371.SZ
        session: 可选的复用 session

    Returns:
        {"news": [...], "notice": [...], "industry": [...], "report": [...]}
    """
    code = _extract_code(stock_code_normalize)
    url = f"https://stockpage.10jqka.com.cn/{code}/"

    async def _do_fetch(s: AsyncSession) -> str:
        resp = await s.get(url, timeout=20)
        resp.raise_for_status()
        return resp.text

    try:
        if session:
            html = await _do_fetch(session)
        else:
            async with AsyncSession(impersonate=IMPERSONATE) as s:
                html = await _do_fetch(s)

        result = _parse_stockpage_html(html, stock_code_normalize)
        total = sum(len(v) for v in result.values())
        logger.info("[%s] 抓取新闻公告完成，共 %d 条", stock_code_normalize, total)
        return result

    except Exception as e:
        logger.error("[%s] 抓取新闻公告失败: %s", stock_code_normalize, e)
        return {"news": [], "notice": [], "industry": [], "report": []}


async def fetch_stock_news_all_types(stock_code_normalize: str) -> list[dict]:
    """抓取个股所有类型新闻，返回扁平列表"""
    result = await fetch_stock_news(stock_code_normalize)
    all_items = []
    for items in result.values():
        all_items.extend(items)
    return all_items


# ── 测试入口 ──────────────────────────────────────────────

if __name__ == "__main__":
    import json

    async def main():
        code = "002371.SZ"
        result = await fetch_stock_news(code)
        for news_type, items in result.items():
            print(f"\n{'=' * 60}")
            print(f"  {TYPE_LABEL.get(news_type, news_type)} ({len(items)} 条)")
            print(f"{'=' * 60}")
            for item in items:
                print(f"  {json.dumps(item, ensure_ascii=False)}")

    asyncio.run(main())
