"""网页抓取工具模块。

提供简单易用的网页内容抓取功能。
"""

import asyncio
import re
from typing import List, Optional

import aiohttp
from bs4 import BeautifulSoup


# 常量定义
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
DEFAULT_TIMEOUT = 15
CLASHX_PROXY = "http://127.0.0.1:7890"
MAX_HEADER_SIZE = 32768

async def extract_main_content(url: str, use_proxy: bool = True) -> Optional[str]:
    """提取网页正文内容。

    Args:
        url: 目标网页URL
        use_proxy: 是否使用ClashX代理

    Returns:
        Optional[str]: 网页正文内容，失败时返回None
    """
    try:
        return await _extract_main_content(url, use_proxy)
    except Exception as e:
        print(f"extract_main_content error for {url}: {e}")
        return None


async def _extract_main_content(url: str, use_proxy: bool = True) -> str:
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    connector = aiohttp.TCPConnector(limit_per_host=100)

    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
        max_line_size=MAX_HEADER_SIZE,
        max_field_size=MAX_HEADER_SIZE
    ) as session:
        kwargs = {}
        if use_proxy:
            kwargs["proxy"] = CLASHX_PROXY

        async with session.get(url, **kwargs) as response:
            response.raise_for_status()
            text = await response.text()

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
        # 过滤媒体信息、时间戳、关注按钮等
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

    Raises:
        aiohttp.ClientError: 网络请求异常
    """
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    connector = aiohttp.TCPConnector(limit_per_host=100)

    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
        max_line_size=MAX_HEADER_SIZE,
        max_field_size=MAX_HEADER_SIZE
    ) as session:
        kwargs = {}
        if use_proxy:
            kwargs["proxy"] = CLASHX_PROXY

        async with session.get(url, **kwargs) as response:
            response.raise_for_status()
            text = await response.text()

    soup = BeautifulSoup(text, "html.parser")
    elements = soup.find_all(tag)

    return [elem.get_text().strip() for elem in elements if elem.get_text().strip()]


# 使用示例
if __name__ == "__main__":
    async def main():
        url = "https://finance.yahoo.com/news/tsmc-lifts-2026-capex-outlook-154053660.html"
        content = await extract_main_content(url)
        print(content)

    asyncio.run(main())