"""网页抓取工具模块。

提供简单易用的网页内容抓取功能。
"""

import asyncio
import logging
import random
import re
from typing import List
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)


# 常量定义
DEFAULT_TIMEOUT = 30
BUSINESS_TIMEOUT = 60
CLASHX_PROXY = "http://127.0.0.1:7890"
IMPERSONATE = "chrome131"

# 已知付费墙 / 强反爬网站域名，直接跳过抓取以节省时间
_SKIP_DOMAINS = {
    "barrons.com", "wsj.com", "ft.com", "nytimes.com",
    "bloomberg.com", "economist.com", "theathletic.com",
    "washingtonpost.com", "telegraph.co.uk", "thetimes.co.uk",
}

# 多组 UA，随机选取降低指纹一致性
_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]

def _build_headers() -> dict:
    """构建随机化的请求头。"""
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": random.choice(["en-US,en;q=0.9", "en-US,en;q=0.9,zh-CN;q=0.8", "en;q=0.9"]),
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.google.com/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }


def _is_skip_domain(url: str) -> bool:
    """判断 URL 是否属于应跳过的付费墙/强反爬域名。"""
    try:
        host = urlparse(url).hostname or ""
        return any(host == d or host.endswith("." + d) for d in _SKIP_DOMAINS)
    except Exception:
        return False





async def extract_main_content(url: str, use_proxy: bool = False, timeout: int = DEFAULT_TIMEOUT) -> str:
    """提取网页正文内容。

    Args:
        url: 目标网页URL
        use_proxy: 是否使用ClashX代理
        timeout: 请求超时时间（秒）

    Returns:
        str: 网页正文内容
    """
    if _is_skip_domain(url):
        logger.info("extract_main_content 跳过付费墙/强反爬网站: %s", url)
        return ""

    text = await _fetch_html(url, use_proxy=use_proxy, timeout=timeout)
    if not text and not use_proxy:
        logger.info("extract_main_content 直连失败，尝试代理重试: %s", url)
        text = await _fetch_html(url, use_proxy=True, timeout=timeout)
    if not text:
        return ""

    try:
        return _parse_html_content(text)
    except Exception as e:
        logger.error("extract_main_content 解析失败 [%s]: %s", url, e)
        return ""




async def _fetch_html(url: str, use_proxy: bool = False, timeout: int = DEFAULT_TIMEOUT) -> str:
    """请求网页并返回 HTML 文本，失败返回空字符串。"""
    proxy = CLASHX_PROXY if use_proxy else None
    try:
        async with AsyncSession(impersonate=IMPERSONATE) as session:
            response = await session.get(url, proxy=proxy, timeout=timeout, headers=_build_headers())
            if response.status_code in (401, 403):
                logger.warning("extract_main_content %d: %s (proxy=%s)", response.status_code, url, use_proxy)
                return ""
            response.raise_for_status()
            return response.text
    except Exception as e:
        logger.error("extract_main_content 请求失败 [%s] (proxy=%s): %s", url, use_proxy, e)
        return ""



def _parse_html_content(text: str) -> str:
    """从 HTML 文本中提取正文内容。"""
    soup = BeautifulSoup(text, "html.parser")

    # 移除不需要的标签
    for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()

    # 尝试找到主要内容区域
    main_content = (
        soup.find("main") or
        soup.find("article") or
        soup.find("div", class_=re.compile(r"content|main|article|post|detail|body", re.I)) or
        soup.find("div", id=re.compile(r"content|main|article|post|detail|body", re.I)) or
        soup.find("section", class_=re.compile(r"content|main|article|post|detail", re.I)) or
        soup.find("div", class_=lambda x: x and any(word in x.lower() for word in ["text", "news", "story"]))
    )

    if main_content:
        content_text = main_content.get_text(strip=True, separator="\n")
    else:
        body = soup.find("body")
        content_text = body.get_text(strip=True, separator="\n") if body else soup.get_text(strip=True)

    # 过滤元信息和去重
    lines = content_text.split("\n")
    filtered_lines = []
    seen_lines = set()

    for line in lines:
        line = line.strip()
        if not line or line in seen_lines:
            continue
        if re.match(r'^[《》""\w\s]+官方账号$', line):
            continue
        if re.match(r'^\d{2}\.\d{2}$', line):
            continue
        if re.match(r'^\d{2}:\d{2}$', line):
            continue
        if line in ["关注", "分享", "点赞", "评论", "转发", "收藏"]:
            continue
        if len(line) < 10 and re.match(r'^[\w\s《》""]+$', line):
            continue

        seen_lines.add(line)
        filtered_lines.append(line)

    return "\n".join(filtered_lines)



# ── 常见中文日期时间正则 ──
_DATETIME_PATTERNS = [
    # 2026-02-28 15:30:00 / 2026-02-28 15:30
    (re.compile(r'(\d{4}[-/]\d{1,2}[-/]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)'), '%Y-%m-%d %H:%M'),
    # 2026年02月28日 15:30
    (re.compile(r'(\d{4}年\d{1,2}月\d{1,2}日\s*\d{1,2}:\d{2}(?::\d{2})?)'), '%Y年%m月%d日 %H:%M'),
    # 02-28 15:30 (当年)
    (re.compile(r'(?<!\d)(\d{1,2}[-/]\d{1,2}\s+\d{1,2}:\d{2})(?::\d{2})?'), '%m-%d %H:%M'),
    # 2月28日 15:30
    (re.compile(r'(\d{1,2}月\d{1,2}日\s*\d{1,2}:\d{2})'), '%m月%d日 %H:%M'),
]

# 用于从 meta / time 标签提取 ISO 格式
_ISO_DATETIME_RE = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}')


def extract_publish_datetime(html_text: str) -> str | None:
    """从网页 HTML 中提取发布日期时间字符串。

    优先从 meta 标签 / <time> 标签获取精确时间，
    回退到正文中正则匹配常见日期时间格式。

    Returns:
        格式化后的 'YYYY-MM-DD HH:MM' 字符串，提取失败返回 None。
    """
    from datetime import datetime as _dt

    soup = BeautifulSoup(html_text, "html.parser")

    # 1) meta 标签（article:published_time / datePublished / pubdate 等）
    for attr in ['article:published_time', 'datePublished', 'pubdate',
                 'publishdate', 'og:article:published_time']:
        tag = soup.find('meta', attrs={'property': attr}) or soup.find('meta', attrs={'name': attr})
        if tag and tag.get('content'):
            m = _ISO_DATETIME_RE.search(tag['content'])
            if m:
                try:
                    dt = _dt.fromisoformat(m.group().replace('T', ' '))
                    return dt.strftime('%Y-%m-%d %H:%M')
                except ValueError as e:
                    logger.debug("meta标签日期解析失败 [%s]: %s", tag['content'], e)

    # 2) <time> 标签的 datetime 属性
    time_tag = soup.find('time', attrs={'datetime': True})
    if time_tag:
        m = _ISO_DATETIME_RE.search(time_tag['datetime'])
        if m:
            try:
                dt = _dt.fromisoformat(m.group().replace('T', ' '))
                return dt.strftime('%Y-%m-%d %H:%M')
            except ValueError as e:
                logger.debug("time标签日期解析失败 [%s]: %s", time_tag['datetime'], e)

    # 3) 正文正则匹配
    body = soup.find('body')
    text = body.get_text(separator=' ', strip=True)[:3000] if body else ''
    for pattern, fmt in _DATETIME_PATTERNS:
        match = pattern.search(text)
        if match:
            raw = match.group(1).replace('/', '-')
            try:
                dt = _dt.strptime(raw, fmt)
                # 补全年份
                if dt.year == 1900:
                    dt = dt.replace(year=_dt.now().year)
                return dt.strftime('%Y-%m-%d %H:%M')
            except ValueError as e:
                logger.debug("正文日期解析失败 [%s]: %s", raw, e)
                continue

    return None
async def extract_titles(url: str, tag: str = "h2", use_proxy: bool = True) -> List[str]:
    """提取网页中指定标签的文本内容。

    Args:
        url: 目标网页URL
        tag: HTML标签名称，默认为h2
        use_proxy: 是否使用ClashX代理

    Returns:
        List[str]: 提取的文本内容列表
    """
    proxy = CLASHX_PROXY if use_proxy else None

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        response = await session.get(url, proxy=proxy, timeout=DEFAULT_TIMEOUT, headers=_build_headers())
        response.raise_for_status()
        text = response.text

    soup = BeautifulSoup(text, "html.parser")
    elements = soup.find_all(tag)

    return [elem.get_text().strip() for elem in elements if elem.get_text().strip()]

async def extract_content_with_datetime(url: str, use_proxy: bool = False, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """提取网页正文内容和发布时间。

    Returns:
        dict: {'content': str, 'publish_time': str|None}
              publish_time 格式为 'YYYY-MM-DD HH:MM'，提取失败为 None。
    """
    proxy = CLASHX_PROXY if use_proxy else None

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        response = await session.get(url, proxy=proxy, timeout=timeout, headers=_build_headers())
        if response.status_code == 403:
            return {'content': '', 'publish_time': None}
        response.raise_for_status()
        raw_html = response.text

    # 提取发布时间
    publish_time = extract_publish_datetime(raw_html)

    # 提取正文（复用已有逻辑）
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()

    main_content = (
        soup.find("main") or
        soup.find("article") or
        soup.find("div", class_=re.compile(r"content|main|article|post|detail|body", re.I)) or
        soup.find("div", id=re.compile(r"content|main|article|post|detail|body", re.I)) or
        soup.find("section", class_=re.compile(r"content|main|article|post|detail", re.I)) or
        soup.find("div", class_=lambda x: x and any(word in x.lower() for word in ["text", "news", "story"]))
    )

    if main_content:
        content_text = main_content.get_text(strip=True, separator="\n")
    else:
        body = soup.find("body")
        content_text = body.get_text(strip=True, separator="\n") if body else soup.get_text(strip=True)

    lines = content_text.split("\n")
    filtered_lines = []
    seen_lines = set()
    for line in lines:
        line = line.strip()
        if not line or line in seen_lines:
            continue
        if re.match(r'^[《》""\w\s]+官方账号$', line):
            continue
        if re.match(r'^\d{2}\.\d{2}$', line):
            continue
        if re.match(r'^\d{2}:\d{2}$', line):
            continue
        if line in ["关注", "分享", "点赞", "评论", "转发", "收藏"]:
            continue
        if len(line) < 10 and re.match(r'^[\w\s《》""]+$', line):
            continue
        seen_lines.add(line)
        filtered_lines.append(line)

    return {
        'content': "\n".join(filtered_lines),
        'publish_time': publish_time,
    }



# 使用示例
if __name__ == "__main__":
    async def main():
        url_1 = "https://finance.yahoo.com/news/tsmc-lifts-2026-capex-outlook-154053660.html"
        url_2 = "https://www.bitget.com/news/detail/12560605204670"
        url_3 = "https://electronics360.globalspec.com/article/23198/semi-chip-manufacturing-equipment-to-hit-record-high-in-2025"
        url = "https://www.bitget.com/news/detail/12560605204670"
        content = await extract_main_content(url_3)
        logger.info(content)

    asyncio.run(main())
