"""
同花顺龙虎榜数据模块

数据来源：https://data.10jqka.com.cn/market/lhb/
页面 SSR 渲染龙虎榜数据表格，包含：
  排名、代码、名称、收盘价、涨跌幅、龙虎榜净买额、龙虎榜买入额、龙虎榜卖出额、
  龙虎榜成交额、市场总成交额、净买额占总成交比、成交额占总成交比、
  换手率、流通市值、上榜原因

使用 curl_cffi 模拟浏览器 TLS 指纹绕过反爬，GBK 编码解析。
"""

import asyncio
import logging
import re
from datetime import date

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

IMPERSONATE = "chrome131"
BASE_URL = "https://data.10jqka.com.cn/market/lhb"

# 龙虎榜表格字段
LHB_FIELDS = [
    "rank", "stock_code", "stock_name", "close_price", "change_pct",
    "net_buy_amount", "buy_amount", "sell_amount",
    "lhb_turnover", "market_turnover", "net_buy_ratio", "turnover_ratio",
    "turnover_rate", "circulating_market_cap", "reason",
]

FIELD_CN_MAP = {
    "rank": "排名", "stock_code": "代码", "stock_name": "名称",
    "close_price": "收盘价", "change_pct": "涨跌幅",
    "net_buy_amount": "龙虎榜净买额", "buy_amount": "龙虎榜买入额",
    "sell_amount": "龙虎榜卖出额", "lhb_turnover": "龙虎榜成交额",
    "market_turnover": "市场总成交额", "net_buy_ratio": "净买额占总成交比",
    "turnover_ratio": "成交额占总成交比", "turnover_rate": "换手率",
    "circulating_market_cap": "流通市值", "reason": "上榜原因",
}


def _clean_html(raw_bytes: bytes) -> str:
    """GBK 解码 + 移除 IE 条件注释"""
    text = raw_bytes.decode("gbk", errors="replace")
    text = re.sub(r"<!--\[if.*?\]-->", "", text, flags=re.DOTALL)
    text = re.sub(r"<!\[endif\]-->", "", text)
    return text


def _clean_text(td) -> str:
    a_tag = td.find("a")
    if a_tag:
        return a_tag.get_text(strip=True)
    return td.get_text(strip=True)


def _parse_total_pages(soup: BeautifulSoup) -> int:
    page_info = soup.find("span", class_="page_info")
    if page_info:
        m = re.search(r"(\d+)/(\d+)", page_info.get_text())
        if m:
            return int(m.group(2))
    return 1


def _parse_lhb_table(html: str) -> tuple[list[dict], int]:
    """解析龙虎榜 HTML 表格，返回 (rows, total_pages)"""
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
        if len(values) < len(LHB_FIELDS):
            values.extend([""] * (len(LHB_FIELDS) - len(values)))
        row = dict(zip(LHB_FIELDS, values[:len(LHB_FIELDS)]))
        rows.append(row)

    return rows, total_pages


async def fetch_dragon_tiger(
    trade_date: str | None = None,
    page: int = 1,
) -> dict:
    """
    获取龙虎榜数据（单页）。

    Args:
        trade_date: 交易日期，格式 YYYY-MM-DD，默认当天
        page: 页码

    Returns:
        {"date": "2025-01-01", "page": 1, "total_pages": 3, "data": [...]}
    """
    if trade_date is None:
        trade_date = date.today().isoformat()

    date_compact = trade_date.replace("-", "")

    if page == 1:
        url = f"{BASE_URL}/scode/all/stype/all/date/{date_compact}/"
    else:
        url = f"{BASE_URL}/scode/all/stype/all/date/{date_compact}/field/lhbJme/order/desc/page/{page}/"

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        resp = await session.get(url, timeout=15)
        resp.raise_for_status()
        html = _clean_html(resp.content)

    rows, total_pages = _parse_lhb_table(html)
    logger.info("[龙虎榜] 日期=%s 第%d/%d页，获取%d条", trade_date, page, total_pages, len(rows))

    return {
        "date": trade_date,
        "page": page,
        "total_pages": total_pages,
        "data": rows,
    }


async def fetch_dragon_tiger_all_pages(
    trade_date: str | None = None,
    max_pages: int = 0,
) -> list[dict]:
    """获取龙虎榜全部页数据"""
    if trade_date is None:
        trade_date = date.today().isoformat()

    date_compact = trade_date.replace("-", "")

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        # 第1页
        url = f"{BASE_URL}/scode/all/stype/all/date/{date_compact}/"
        resp = await session.get(url, timeout=15)
        resp.raise_for_status()
        html = _clean_html(resp.content)
        rows, total_pages = _parse_lhb_table(html)
        all_rows = list(rows)

        logger.info("[龙虎榜] 日期=%s 第1/%d页，获取%d条", trade_date, total_pages, len(rows))

        if max_pages > 0:
            total_pages = min(total_pages, max_pages)

        for p in range(2, total_pages + 1):
            try:
                page_url = f"{BASE_URL}/scode/all/stype/all/date/{date_compact}/field/lhbJme/order/desc/page/{p}/"
                page_resp = await session.get(page_url, timeout=15)
                page_resp.raise_for_status()
                page_html = _clean_html(page_resp.content)
                page_rows, _ = _parse_lhb_table(page_html)
                all_rows.extend(page_rows)
                logger.info("[龙虎榜] 日期=%s 第%d/%d页，获取%d条", trade_date, p, total_pages, len(page_rows))
            except Exception as e:
                logger.error("[龙虎榜] 第%d页请求异常: %s", p, e)

    logger.info("[龙虎榜] 日期=%s 共获取%d条（%d页）", trade_date, len(all_rows), total_pages)
    return all_rows


def to_cn_rows(rows: list[dict]) -> list[dict]:
    """将英文 key 转为中文 key"""
    return [{FIELD_CN_MAP.get(k, k): v for k, v in row.items()} for row in rows]


if __name__ == "__main__":
    import json

    async def main():
        result = await fetch_dragon_tiger()
        print(f"总页数: {result['total_pages']}, 本页: {len(result['data'])}条")
        for row in result["data"][:5]:
            print(json.dumps(row, ensure_ascii=False))

    asyncio.run(main())
