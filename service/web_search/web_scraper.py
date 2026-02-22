"""网页抓取工具模块。

提供简单易用的网页内容抓取功能。
"""

import re
from typing import List

import requests
from bs4 import BeautifulSoup


# 常量定义
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
DEFAULT_TIMEOUT = 30

def extract_main_content(url: str) -> str:
    """提取网页正文内容。

    Args:
        url: 目标网页URL

    Returns:
        str: 网页正文内容
    """
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    response = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    
    # 处理编码问题
    response.encoding = response.apparent_encoding or 'utf-8'
    
    soup = BeautifulSoup(response.text, "html.parser")
    
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
        text = main_content.get_text(strip=True, separator="\n")
    else:
        body = soup.find("body")
        text = body.get_text(strip=True, separator="\n") if body else soup.get_text(strip=True)
    
    # 过滤元信息和去重
    lines = text.split("\n")
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

def extract_titles(url: str, tag: str = "h2") -> List[str]:
    """提取网页中指定标签的文本内容。

    Args:
        url: 目标网页URL
        tag: HTML标签名称，默认为h2

    Returns:
        List[str]: 提取的文本内容列表

    Raises:
        requests.RequestException: 网络请求异常
    """
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    response = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    elements = soup.find_all(tag)

    return [elem.get_text().strip() for elem in elements if elem.get_text().strip()]


# 使用示例
if __name__ == "__main__":
    url = "https://finance.sina.cn/tech/2026-02-22/detail-inhnqzxa1637041.d.html"
    url_1 = "https://cj.sina.cn/articles/view/1960136440/74d54ef802701iiqw"
    content = extract_main_content(url_1)
    print(content)
    pass