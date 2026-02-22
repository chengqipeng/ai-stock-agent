"""网页抓取工具模块。

提供简单易用的网页内容抓取功能。
"""

import asyncio
import re
from typing import List

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession


# 常量定义
DEFAULT_TIMEOUT = 30
BUSINESS_TIMEOUT = 60
CLASHX_PROXY = "http://127.0.0.1:7890"
IMPERSONATE = "chrome120"
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


async def extract_main_content(url: str, use_proxy: bool = False, timeout: int = DEFAULT_TIMEOUT) -> str:
    """提取网页正文内容。

    Args:
        url: 目标网页URL
        use_proxy: 是否使用ClashX代理
        timeout: 请求超时时间（秒）

    Returns:
        str: 网页正文内容
    """
    proxy = CLASHX_PROXY if use_proxy else None

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        response = await session.get(url, proxy=proxy, timeout=timeout, headers=BROWSER_HEADERS)
        if response.status_code == 403:
            return ""
        response.raise_for_status()
        text = response.text

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
        response = await session.get(url, proxy=proxy, timeout=DEFAULT_TIMEOUT, headers=BROWSER_HEADERS)
        response.raise_for_status()
        text = response.text

    soup = BeautifulSoup(text, "html.parser")
    elements = soup.find_all(tag)

    return [elem.get_text().strip() for elem in elements if elem.get_text().strip()]


# 使用示例
if __name__ == "__main__":
    async def main():
        url_1 = "https://finance.yahoo.com/news/tsmc-lifts-2026-capex-outlook-154053660.html"
        url_2 = "https://www.bitget.com/news/detail/12560605204670"
        url_3 = "https://electronics360.globalspec.com/article/23198/semi-chip-manufacturing-equipment-to-hit-record-high-in-2025"
        url = "https://www.bitget.com/news/detail/12560605204670"
        content = await extract_main_content(url_3)
        print(content)

    asyncio.run(main())
