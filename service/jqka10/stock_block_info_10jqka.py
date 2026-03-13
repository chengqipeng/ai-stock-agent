"""
从同花顺获取个股所属板块信息（行业板块 + 概念板块 + 风格板块等）。

接口1: https://d.10jqka.com.cn/v4/stockblock/hs_{6位代码}/last.js
  返回 JSONP，包含该股票所属的全部板块及实时行情。

接口2: https://basic.10jqka.com.cn/api/stockph/conceptdetail/{6位代码}/
  返回 JSON，包含 App F10 概念详情页数据，含"最相关"等标签。
  这是同花顺 App F10 页面「公司概念」的数据源。

板块ID前缀规则：
  881xxx = 同花顺行业板块（一级/二级）
  884xxx = 同花顺细分行业（三级）
  885xxx / 886xxx = 概念板块
  882xxx = 地域板块
  883xxx = 风格/指数板块（如沪深300、大盘股等）

Usage:
    python -m service.jqka10.stock_block_info_10jqka
"""
import json
import logging
import random
import re
import time
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://stockpage.10jqka.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/145.0.0.0 Safari/537.36",
}

# JSONP 提取正则
_JSONP_RE = re.compile(r'\((\{.*\})\)', re.DOTALL)

# 板块分类规则
BLOCK_TYPE_INDUSTRY = "行业"
BLOCK_TYPE_CONCEPT = "概念"
BLOCK_TYPE_REGION = "地域"
BLOCK_TYPE_STYLE = "风格"


def _classify_block(block_id: int) -> str:
    """根据板块ID前缀判断板块类型。"""
    prefix = block_id // 1000
    if prefix in (881, 884):
        return BLOCK_TYPE_INDUSTRY
    elif prefix in (885, 886):
        return BLOCK_TYPE_CONCEPT
    elif prefix == 882:
        return BLOCK_TYPE_REGION
    else:
        return BLOCK_TYPE_STYLE

# 移动端 F10 概念详情接口 Headers
_MOBILE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://basic.10jqka.com.cn/mobile/",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                  "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                  "Mobile/15E148 IHexin/12.0.0",
}


def fetch_concept_detail(stock_code: str, retries: int = 2) -> Optional[dict]:
    """
    获取同花顺 App F10 概念详情数据（含"走势最相关"标签）。

    数据源: https://basic.10jqka.com.cn/api/stockph/conceptdetail/{code}/
    这是同花顺 App F10 页面「公司概念」的实际数据接口。

    Args:
        stock_code: 6位代码或 "002371.SZ" 格式
        retries: 重试次数

    Returns:
        {
            "code": "002371",
            "concepts": [
                {
                    "title": "中芯国际概念",
                    "labels": ["最相关"],
                    "cid": 308690,
                    "short": "公司刻蚀机...",
                    "leaders": [{"type": "lingzhang", "title": "领涨龙头", "stocks": [...]}, ...]
                }, ...
            ],
            "most_related": ["中芯国际概念"],   # 带"最相关"标签的概念
            "all_concept_names": ["中芯国际概念", "国家大基金持股", ...],
        }
        失败返回 None
    """
    code = stock_code.split(".")[0] if "." in stock_code else stock_code
    url = f"https://basic.10jqka.com.cn/api/stockph/conceptdetail/{code}/"

    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=_MOBILE_HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode("utf-8", errors="replace")

            data = json.loads(raw)
            if data.get("errorcode") != "0":
                logger.debug("[10jqka概念详情] 接口返回错误, code=%s, msg=%s",
                             code, data.get("errormsg", ""))
                return None

            items = data.get("data", [])
            if not items:
                return {"code": code, "concepts": [], "most_related": [],
                        "all_concept_names": []}

            concepts = []
            most_related = []
            all_names = []

            for item in items:
                title = item.get("title", "")
                labels = [lb.get("name", "") for lb in item.get("label", [])
                          if lb.get("name")]
                leaders = []
                for ld in item.get("leader", []):
                    stocks = [{"code": s.get("code", ""), "name": s.get("name", "").strip()}
                              for s in ld.get("data", [])]
                    if stocks:
                        leaders.append({
                            "type": ld.get("type", ""),
                            "title": ld.get("title", ""),
                            "stocks": stocks,
                        })

                concepts.append({
                    "title": title,
                    "labels": labels,
                    "cid": item.get("cid", 0),
                    "short": item.get("short", ""),
                    "leaders": leaders,
                })
                all_names.append(title)
                if "最相关" in labels:
                    most_related.append(title)

            return {
                "code": code,
                "concepts": concepts,
                "most_related": most_related,
                "all_concept_names": all_names,
            }

        except Exception as e:
            logger.warning("[10jqka概念详情] 请求异常 code=%s attempt=%d: %s",
                           code, attempt, e)
            if attempt < retries:
                time.sleep(1 + random.uniform(0, 1))

    return None


def batch_fetch_concept_detail(
    stock_codes: list[str],
    delay: float = 0.2,
) -> dict[str, dict]:
    """
    批量获取 F10 概念详情（含"走势最相关"标签）。

    Returns:
        {stock_code: concept_detail_dict}
    """
    results = {}
    total = len(stock_codes)

    for i, code in enumerate(stock_codes):
        detail = fetch_concept_detail(code)
        if detail:
            results[code] = detail
            related = detail["most_related"]
            related_str = ", ".join(related) if related else "无"
            print(f"  [F10概念] {i+1}/{total} {code} -> "
                  f"共{len(detail['concepts'])}个概念, "
                  f"最相关: {related_str}")
        else:
            print(f"  [F10概念] {i+1}/{total} {code} -> 获取失败")

        time.sleep(delay + random.uniform(0, 0.1))

    return results




def fetch_stock_blocks(stock_code: str, retries: int = 2) -> Optional[dict]:
    """
    获取单只股票的所属板块信息。

    Args:
        stock_code: 6位代码或 "002371.SZ" 格式
        retries: 重试次数

    Returns:
        {
            "code": "002371",
            "all_blocks": [{"id": 881121, "name": "半导体", "type": "行业", "price": "13485.282", "change_pct": "-1.476"}, ...],
            "industry_blocks": [{"id": 881121, "name": "半导体", ...}, ...],
            "concept_blocks": [{"id": 885756, "name": "芯片概念", ...}, ...],
            "top_industry": "半导体",          # 第一个行业板块（最相关行业）
            "top_concept": "中芯国际概念",      # 第一个概念板块（最相关概念）
        }
        失败返回 None
    """
    code = stock_code.split(".")[0] if "." in stock_code else stock_code
    url = f"https://d.10jqka.com.cn/v4/stockblock/hs_{code}/last.js"

    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode("utf-8", errors="replace")

            m = _JSONP_RE.search(raw)
            if not m:
                logger.debug("[10jqka板块] 未解析到JSONP数据, code=%s", code)
                return None

            data = json.loads(m.group(1))
            items = data.get("items", [])

            all_blocks = []
            industry_blocks = []
            concept_blocks = []

            for item in items:
                block_id = item.get("id", 0)
                block_name = item.get("name", "")
                block_type = _classify_block(block_id)
                block_info = {
                    "id": block_id,
                    "name": block_name,
                    "type": block_type,
                    "price": item.get("10", ""),
                    "change_pct": item.get("199112", ""),
                }
                all_blocks.append(block_info)
                if block_type == BLOCK_TYPE_INDUSTRY:
                    industry_blocks.append(block_info)
                elif block_type == BLOCK_TYPE_CONCEPT:
                    concept_blocks.append(block_info)

            return {
                "code": code,
                "all_blocks": all_blocks,
                "industry_blocks": industry_blocks,
                "concept_blocks": concept_blocks,
                "top_industry": industry_blocks[0]["name"] if industry_blocks else "",
                "top_concept": concept_blocks[0]["name"] if concept_blocks else "",
            }

        except Exception as e:
            logger.warning("[10jqka板块] 请求异常 code=%s attempt=%d: %s", code, attempt, e)
            if attempt < retries:
                time.sleep(1 + random.uniform(0, 1))

    return None


def batch_fetch_stock_blocks(
    stock_codes: list[str],
    delay: float = 0.15,
) -> dict[str, dict]:
    """
    批量获取个股板块信息。

    Returns:
        {stock_code: block_info_dict}
    """
    results = {}
    total = len(stock_codes)

    for i, code in enumerate(stock_codes):
        info = fetch_stock_blocks(code)
        if info:
            results[code] = info
            ind = info["top_industry"] or "-"
            con = info["top_concept"] or "-"
            print(f"  [10jqka板块] {i+1}/{total} {code} -> 行业:{ind} | 概念:{con} | "
                  f"共{len(info['all_blocks'])}个板块")
        else:
            print(f"  [10jqka板块] {i+1}/{total} {code} -> 获取失败")

        time.sleep(delay + random.uniform(0, 0.1))

    return results


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    test_codes = ["002371", "600519", "000001", "300750", "688981"]

    # 1. 板块信息（v4/stockblock 接口）
    print("=" * 60)
    print("【板块信息】v4/stockblock 接口")
    print("=" * 60)
    results = batch_fetch_stock_blocks(test_codes)
    for code, info in results.items():
        print(f"\n{code} | 行业: {info['top_industry']} | 概念(首个): {info['top_concept']}")
        print(f"  行业板块({len(info['industry_blocks'])}个):")
        for b in info["industry_blocks"]:
            print(f"    {b['id']} {b['name']} ({b['change_pct']}%)")
        print(f"  概念板块({len(info['concept_blocks'])}个):")
        for b in info["concept_blocks"]:
            print(f"    {b['id']} {b['name']} ({b['change_pct']}%)")

    # 2. F10 概念详情（含"走势最相关"标签）
    print(f"\n{'=' * 60}")
    print("【F10概念详情】conceptdetail 接口（含走势最相关标签）")
    print("=" * 60)
    details = batch_fetch_concept_detail(test_codes)
    for code, detail in details.items():
        related = detail["most_related"]
        related_str = ", ".join(related) if related else "无"
        print(f"\n{code} | 走势最相关: {related_str}")
        for i, c in enumerate(detail["concepts"]):
            label_str = f" 【{'、'.join(c['labels'])}】" if c["labels"] else ""
            print(f"  {i+1}. {c['title']}{label_str}")



if __name__ == "__main__":
    main()
