"""
同花顺龙虎榜数据模块

数据来源：https://data.10jqka.com.cn/market/longhu/
页面 SSR 渲染龙虎榜数据，每只上榜股票一个 div.stockcont 块，包含：
  股票名称、代码、上榜原因、成交额、合计买入额、合计卖出额

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
BASE_URL = "https://data.10jqka.com.cn/market/longhu"


def _clean_html(raw_bytes: bytes) -> str:
    """GBK 解码 + 移除 IE 条件注释"""
    text = raw_bytes.decode("gbk", errors="replace")
    text = re.sub(r"<!--\[if.*?\]-->", "", text, flags=re.DOTALL)
    text = re.sub(r"<!\[endif\]-->", "", text)
    return text


def _parse_longhu_page(html: str) -> list[dict]:
    """
    解析龙虎榜页面，提取每只股票的摘要信息。
    返回: [{"stock_code": "301638", "stock_name": "南网数字",
            "reason": "...", "turnover": "12.45亿元",
            "buy_amount": "6.13亿元", "sell_amount": "6.33亿元"}, ...]
    """
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.find_all("div", class_="stockcont")

    results = []
    seen = set()  # 同一只股票可能因多个原因多次上榜，合并去重

    for block in blocks:
        p = block.find("p")
        if not p:
            continue
        title_text = p.get_text(strip=True)

        m = re.match(r"(.+?)\((\d+)\)明细[：:](.+)", title_text)
        if not m:
            continue

        stock_name = m.group(1).strip()
        stock_code = m.group(2).strip()
        reason = m.group(3).strip()

        # 成交额、买入、卖出
        cell = block.find("div", class_="cell-cont")
        cell_text = cell.get_text() if cell else ""

        turnover = ""
        buy_amount = ""
        sell_amount = ""

        tm = re.search(r"成交额[：:]([^\s]+)", cell_text)
        if tm:
            turnover = tm.group(1)
        bm = re.search(r"合计买入[：:]([^\s]+)", cell_text)
        if bm:
            buy_amount = bm.group(1)
        sm = re.search(r"合计卖出[：:]([^\s]+)", cell_text)
        if sm:
            sell_amount = sm.group(1)

        # 同一只股票多次上榜时，reason 拼接，金额取第一次出现的
        if stock_code in seen:
            for r in results:
                if r["stock_code"] == stock_code:
                    if reason not in r["reason"]:
                        r["reason"] += "；" + reason
                    break
        else:
            seen.add(stock_code)
            results.append({
                "stock_code": stock_code,
                "stock_name": stock_name,
                "reason": reason,
                "turnover": turnover,
                "buy_amount": buy_amount,
                "sell_amount": sell_amount,
            })

    return results


async def fetch_dragon_tiger_all_pages(
    trade_date: str | None = None,
) -> list[dict]:
    """
    获取指定日期的龙虎榜数据。

    Args:
        trade_date: 交易日期 YYYY-MM-DD，默认当天

    Returns:
        [{"stock_code", "stock_name", "reason", "turnover", "buy_amount", "sell_amount"}, ...]
    """
    if trade_date is None:
        trade_date = date.today().isoformat()

    date_compact = trade_date.replace("-", "")
    url = f"{BASE_URL}/scode/all/stype/all/date/{date_compact}/"

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        resp = await session.get(url, timeout=15)
        resp.raise_for_status()
        html = _clean_html(resp.content)

    rows = _parse_longhu_page(html)
    logger.info("[龙虎榜] 日期=%s 获取%d只股票", trade_date, len(rows))
    return rows


async def fetch_dragon_tiger(
    trade_date: str | None = None,
    page: int = 1,
) -> dict:
    """兼容旧接口，返回 {"date", "page", "total_pages", "data"}"""
    rows = await fetch_dragon_tiger_all_pages(trade_date)
    return {
        "date": trade_date or date.today().isoformat(),
        "page": 1,
        "total_pages": 1,
        "data": rows,
    }


def to_cn_rows(rows: list[dict]) -> list[dict]:
    """将英文 key 转为中文 key"""
    cn_map = {
        "stock_code": "代码", "stock_name": "名称",
        "reason": "上榜原因", "turnover": "成交额",
        "buy_amount": "买入额", "sell_amount": "卖出额",
    }
    return [{cn_map.get(k, k): v for k, v in row.items()} for row in rows]


if __name__ == "__main__":
    import json

    async def main():
        rows = await fetch_dragon_tiger_all_pages("2026-03-07")
        print(f"共 {len(rows)} 只股票")
        for row in rows[:10]:
            print(json.dumps(row, ensure_ascii=False))

    asyncio.run(main())
