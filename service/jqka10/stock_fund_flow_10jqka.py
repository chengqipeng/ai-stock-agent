"""
同花顺资金流向数据模块

数据来源：https://data.10jqka.com.cn/funds/hyzjl/
包含四个维度：
  1. 行业资金流 (hyzjl) — 行业板块资金流入流出
  2. 概念资金流 (gnzjl) — 概念板块资金流入流出
  3. 个股资金流 (ggzjl) — 个股资金流入流出排行
  4. 大单追踪   (ddzz)  — 大单买入卖出追踪

主页直接返回 SSR HTML（GBK 编码），使用 BeautifulSoup 解析表格。
翻页通过 page 参数拼接 URL 实现。
使用 curl_cffi 模拟浏览器 TLS 指纹绕过反爬。
"""

import asyncio
import logging
import re
from typing import Literal

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

BASE_URL = "https://data.10jqka.com.cn/funds"
IMPERSONATE = "chrome131"

# ── 类型 & 字段定义 ──────────────────────────────────────

FundFlowType = Literal["hyzjl", "gnzjl", "ggzjl", "ddzz"]


# 行业资金流字段（11列）
INDUSTRY_FIELDS = [
    "rank", "industry", "industry_index", "change_pct",
    "inflow", "outflow", "net_flow",
    "company_count", "leading_stock", "leading_change_pct", "leading_price",
]

# 概念资金流字段（11列）
CONCEPT_FIELDS = [
    "rank", "concept", "concept_index", "change_pct",
    "inflow", "outflow", "net_flow",
    "company_count", "leading_stock", "leading_change_pct", "leading_price",
]

# 个股资金流字段（10列）
STOCK_FIELDS = [
    "rank", "stock_code", "stock_name", "latest_price", "change_pct",
    "turnover_rate", "flow_strength",
    "inflow", "outflow", "net_flow",
]

# 大单追踪字段（10列）
BIG_ORDER_FIELDS = [
    "time", "stock_code", "stock_name", "price", "volume",
    "amount", "direction", "change_pct", "turnover_rate", "detail",
]

FIELD_CN_MAP = {
    # 行业/概念
    "rank": "排名", "industry": "行业", "concept": "概念",
    "industry_index": "行业指数", "concept_index": "概念指数",
    "change_pct": "涨跌幅", "inflow": "流入资金",
    "outflow": "流出资金", "net_flow": "净额",
    "company_count": "公司家数", "leading_stock": "领涨股",
    "leading_change_pct": "领涨股涨跌幅", "leading_price": "当前价(元)",
    # 个股
    "stock_code": "代码", "stock_name": "名称",
    "latest_price": "最新价", "turnover_rate": "换手率",
    "flow_strength": "资金强度",
    # 大单追踪
    "time": "时间", "price": "成交价", "volume": "成交量(手)",
    "amount": "成交额(万)", "direction": "买卖方向", "detail": "详细",
}

FIELD_KEYS_MAP: dict[FundFlowType, list[str]] = {
    "hyzjl": INDUSTRY_FIELDS,
    "gnzjl": CONCEPT_FIELDS,
    "ggzjl": STOCK_FIELDS,
    "ddzz": BIG_ORDER_FIELDS,
}

DEFAULT_SORT_FIELD: dict[FundFlowType, str] = {
    "hyzjl": "zdf",
    "gnzjl": "zdf",
    "ggzjl": "zdf",
    "ddzz": "zdf",
}

TYPE_LABEL: dict[FundFlowType, str] = {
    "hyzjl": "行业资金流",
    "gnzjl": "概念资金流",
    "ggzjl": "个股资金流",
    "ddzz": "大单追踪",
}


# ── 核心请求 & 解析 ──────────────────────────────────────

def _clean_html(raw_bytes: bytes) -> str:
    """GBK 解码 + 移除 IE 条件注释（会导致 html.parser 解析失败）"""
    text = raw_bytes.decode("gbk", errors="replace")
    text = re.sub(r"<!--\[if.*?\]-->", "", text, flags=re.DOTALL)
    text = re.sub(r"<!\[endif\]-->", "", text)
    return text


def _clean_text(td) -> str:
    """提取 td 内文本，优先取 <a> 标签"""
    a_tag = td.find("a")
    if a_tag:
        return a_tag.get_text(strip=True)
    return td.get_text(strip=True)


def _parse_total_pages(soup: BeautifulSoup) -> int:
    """从 <span class="page_info">1/5</span> 解析总页数"""
    page_info = soup.find("span", class_="page_info")
    if page_info:
        m = re.search(r"(\d+)/(\d+)", page_info.get_text())
        if m:
            return int(m.group(2))
    return 1


def _parse_table(html: str, field_keys: list[str]) -> tuple[list[dict], int]:
    """解析同花顺资金流向 HTML 表格，返回 (rows, total_pages)"""
    soup = BeautifulSoup(html, "html.parser")
    total_pages = _parse_total_pages(soup)

    table = soup.find("table")
    if not table:
        return [], total_pages

    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        values = [_clean_text(td) for td in tds]
        if len(values) < len(field_keys):
            values.extend([""] * (len(field_keys) - len(values)))
        row = dict(zip(field_keys, values[: len(field_keys)]))
        rows.append(row)

    return rows, total_pages


async def _fetch_page(
    flow_type: FundFlowType,
    page: int = 1,
    field: str | None = None,
    order: str = "desc",
    session: AsyncSession | None = None,
) -> str:
    """
    请求同花顺资金流向页面。

    第1页直接访问主页 URL，翻页通过 field/order/page 路径参数。
    注意：不使用 ajax/1/free/1 后缀，因为 AJAX 端点需要 JS 生成的 hexin-v cookie。
    直接请求完整页面（SSR）可以绕过此限制。
    返回清理后的 HTML 文本。
    """
    sort_field = field or DEFAULT_SORT_FIELD.get(flow_type, "zdf")

    if page == 1:
        url = f"{BASE_URL}/{flow_type}/"
    else:
        url = f"{BASE_URL}/{flow_type}/field/{sort_field}/order/{order}/page/{page}/"

    async def _do_fetch(s: AsyncSession) -> str:
        resp = await s.get(url, timeout=15)
        resp.raise_for_status()
        return _clean_html(resp.content)

    if session:
        return await _do_fetch(session)
    async with AsyncSession(impersonate=IMPERSONATE) as s:
        return await _do_fetch(s)


async def fetch_fund_flow(
    flow_type: FundFlowType,
    page: int = 1,
    field: str | None = None,
    order: str = "desc",
) -> dict:
    """
    获取同花顺资金流向数据（单页）。

    Args:
        flow_type: 数据类型 hyzjl/gnzjl/ggzjl/ddzz
        page: 页码，从1开始
        field: 排序字段，默认 zdf（涨跌幅）
        order: 排序方向 desc/asc

    Returns:
        {"type": "行业资金流", "page": 1, "total_pages": 2, "data": [...]}
    """
    field_keys = FIELD_KEYS_MAP[flow_type]
    html = await _fetch_page(flow_type, page, field, order)
    rows, total_pages = _parse_table(html, field_keys)

    logger.info("[%s] 第%d/%d页，获取%d条记录", TYPE_LABEL[flow_type], page, total_pages, len(rows))

    return {
        "type": TYPE_LABEL[flow_type],
        "page": page,
        "total_pages": total_pages,
        "data": rows,
    }


async def fetch_fund_flow_all_pages(
    flow_type: FundFlowType,
    field: str | None = None,
    order: str = "desc",
    max_pages: int = 0,
) -> list[dict]:
    """
    获取同花顺资金流向全部页数据。

    翻页请求需要先访问主页获取 cookie，因此复用同一个 session。
    第1页从主页 SSR 获取，后续页通过 AJAX URL 获取。
    """
    async with AsyncSession(impersonate=IMPERSONATE) as session:
        # 第1页：直接访问主页
        field_keys = FIELD_KEYS_MAP[flow_type]
        html = await _fetch_page(flow_type, 1, field, order, session=session)
        rows, total_pages = _parse_table(html, field_keys)
        all_rows = list(rows)

        logger.info("[%s] 第1/%d页，获取%d条记录", TYPE_LABEL[flow_type], total_pages, len(rows))

        if max_pages > 0:
            total_pages = min(total_pages, max_pages)

        # 后续页：复用 session
        for p in range(2, total_pages + 1):
            try:
                page_html = await _fetch_page(flow_type, p, field, order, session=session)
                page_rows, _ = _parse_table(page_html, field_keys)
                all_rows.extend(page_rows)
                logger.info("[%s] 第%d/%d页，获取%d条记录", TYPE_LABEL[flow_type], p, total_pages, len(page_rows))
            except Exception as e:
                logger.error("[%s] 第%d页请求异常: %s", TYPE_LABEL[flow_type], p, e)

    logger.info("[%s] 共获取%d条记录（%d页）", TYPE_LABEL[flow_type], len(all_rows), total_pages)
    return all_rows


# ── 便捷接口 ─────────────────────────────────────────────

async def get_industry_fund_flow(page: int = 1, field: str = "zdf", order: str = "desc") -> dict:
    """获取行业资金流向（单页）"""
    return await fetch_fund_flow("hyzjl", page, field, order)


async def get_concept_fund_flow(page: int = 1, field: str = "zdf", order: str = "desc") -> dict:
    """获取概念资金流向（单页）"""
    return await fetch_fund_flow("gnzjl", page, field, order)


async def get_stock_fund_flow(page: int = 1, field: str = "zdf", order: str = "desc") -> dict:
    """获取个股资金流向（单页）"""
    return await fetch_fund_flow("ggzjl", page, field, order)


async def get_big_order_tracking(page: int = 1, field: str = "zdf", order: str = "desc") -> dict:
    """获取大单追踪（单页）"""
    return await fetch_fund_flow("ddzz", page, field, order)


async def get_industry_fund_flow_all(max_pages: int = 0) -> list[dict]:
    """获取行业资金流向（全部页）"""
    return await fetch_fund_flow_all_pages("hyzjl", max_pages=max_pages)


async def get_concept_fund_flow_all(max_pages: int = 0) -> list[dict]:
    """获取概念资金流向（全部页）"""
    return await fetch_fund_flow_all_pages("gnzjl", max_pages=max_pages)


async def get_stock_fund_flow_all(max_pages: int = 0) -> list[dict]:
    """获取个股资金流向（全部页）"""
    return await fetch_fund_flow_all_pages("ggzjl", max_pages=max_pages)


async def get_big_order_tracking_all(max_pages: int = 0) -> list[dict]:
    """获取大单追踪（全部页）"""
    return await fetch_fund_flow_all_pages("ddzz", max_pages=max_pages)


def to_cn_rows(rows: list[dict], flow_type: FundFlowType) -> list[dict]:
    """将英文 key 的数据行转为中文 key"""
    return [
        {FIELD_CN_MAP.get(k, k): v for k, v in row.items()}
        for row in rows
    ]


# ── 测试入口 ──────────────────────────────────────────────

if __name__ == "__main__":
    import json

    async def main():
        for label, func in [
            ("行业资金流", get_industry_fund_flow),
            ("概念资金流", get_concept_fund_flow),
            ("个股资金流", get_stock_fund_flow),
            ("大单追踪", get_big_order_tracking),
        ]:
            print(f"\n{'=' * 70}")
            print(f"  {label}（第1页前5条）")
            print(f"{'=' * 70}")
            try:
                result = await func()
                print(f"  总页数: {result['total_pages']}, 本页: {len(result['data'])}条")
                for row in result["data"][:5]:
                    print(f"  {json.dumps(row, ensure_ascii=False)}")
            except Exception as e:
                print(f"  请求失败: {e}")

    asyncio.run(main())
