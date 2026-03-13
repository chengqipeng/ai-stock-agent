"""
从同花顺个股页面抓取「涉及概念」板块数据（同步单线程版本）。

URL 模式: https://stockpage.10jqka.com.cn/{6位代码}/
概念数据位于 <dt>涉及概念：</dt> 后的 <dd title="..."> 中，
title 属性包含完整概念列表（逗号分隔），页面显示文本被截断。

概念顺序即为同花顺给出的相关性排序（越靠前越相关）。

Usage:
    python -m service.jqka10.stock_concept_boards_10jqka
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
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/145.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Upgrade-Insecure-Requests": "1",
}

# 匹配 <dt>涉及概念：</dt> 后面的 <dd title="...">
_CONCEPT_RE = re.compile(
    r'涉及概念[：:]\s*</dt>\s*<dd\s+title="([^"]+)"',
    re.DOTALL,
)


def fetch_concept_boards(stock_code: str, retries: int = 2) -> Optional[list[str]]:
    """
    抓取单只股票的概念板块列表（同步）。

    Args:
        stock_code: 6位代码或 "002371.SZ" 格式
        retries: 重试次数

    Returns:
        概念列表（按同花顺相关性排序），失败返回 None
    """
    code = stock_code.split(".")[0] if "." in stock_code else stock_code
    url = f"https://stockpage.10jqka.com.cn/{code}/"

    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                html = resp.read().decode("utf-8", errors="replace")

            m = _CONCEPT_RE.search(html)
            if not m:
                logger.debug("[10jqka概念] 未找到概念数据, code=%s", code)
                return []

            raw = m.group(1).strip()
            concepts = [c.strip() for c in raw.split("，") if c.strip()]
            return concepts

        except Exception as e:
            logger.warning("[10jqka概念] 请求异常 code=%s attempt=%d: %s", code, attempt, e)
            if attempt < retries:
                time.sleep(1 + random.uniform(0, 1))
            else:
                return None


def batch_fetch_concept_boards(
    stock_codes: list[str],
    delay: float = 0.15,
) -> dict[str, list[str]]:
    """
    单线程顺序批量抓取概念板块。

    Args:
        stock_codes: 股票代码列表（支持 "002371" 或 "002371.SZ" 格式）
        delay: 每个请求间的间隔（秒）

    Returns:
        {stock_code: [概念1, 概念2, ...]}，失败的股票不包含在结果中
    """
    results: dict[str, list[str]] = {}
    total = len(stock_codes)

    for i, code in enumerate(stock_codes):
        concepts = fetch_concept_boards(code)
        if concepts is not None:
            results[code] = concepts
            print(f"  [10jqka概念] {i + 1}/{total} {code} -> {len(concepts)}个概念: "
                  f"{', '.join(concepts[:5])}{'...' if len(concepts) > 5 else ''}")
        else:
            print(f"  [10jqka概念] {i + 1}/{total} {code} -> 获取失败")

        time.sleep(delay + random.uniform(0, 0.1))

    return results


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    test_codes = ["002371", "600519", "000001", "300750", "688981"]
    results = batch_fetch_concept_boards(test_codes)
    for code, concepts in results.items():
        print(f"\n{code}: ({len(concepts)}个概念)")
        for i, c in enumerate(concepts):
            marker = " ★" if i < 3 else ""
            print(f"  {i+1}. {c}{marker}")


if __name__ == "__main__":
    main()
