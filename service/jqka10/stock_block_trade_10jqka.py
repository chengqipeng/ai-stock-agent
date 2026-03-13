"""
同花顺大宗交易数据模块

数据来源：https://stockpage.10jqka.com.cn/{stock_code}/
页面 SSR 渲染大宗交易表格，包含：
  交易日期、成交价(元)、成交金额(万元)、成交量(万股)、溢价率、买入营业部、卖出营业部

使用 curl_cffi 模拟浏览器 TLS 指纹绕过反爬，UTF-8 编码解析。
"""

import asyncio
import logging
import re

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

IMPERSONATE = "chrome131"


def _clean_html(raw_bytes: bytes) -> str:
    """UTF-8 解码 + 移除 IE 条件注释"""
    text = raw_bytes.decode("utf-8", errors="replace")
    text = re.sub(r"<!--\[if.*?\]-->", "", text, flags=re.DOTALL)
    text = re.sub(r"<!\[endif\]-->", "", text)
    return text


def _parse_float(text: str) -> float | None:
    text = text.strip()
    if not text or text == "--":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_pct(text: str) -> float | None:
    """将 '-6.72%' 转为 -6.72"""
    text = text.strip().replace("%", "")
    if not text or text == "--":
        return None
    try:
        return round(float(text), 2)
    except ValueError:
        return None


def _parse_block_trade_table(html: str) -> list[dict]:
    """
    解析个股页面中的大宗交易表格。

    页面结构：id="dzjy" 的 div 内包含一个 table，表头7列：
    交易日期、成交价(元)、成交金额(万元)、成交量(万股)、溢价率、买入营业部、卖出营业部

    Returns:
        list[dict]: 按日期降序排列的大宗交易记录
    """
    soup = BeautifulSoup(html, "html.parser")

    # 通过 id="dzjy" 定位大宗交易区块
    dzjy_div = soup.find(id="dzjy")
    if not dzjy_div:
        return []

    table = dzjy_div.find("table")
    if not table:
        return []

    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        values = [td.get_text(strip=True) for td in tds]
        trade_date = values[0].strip()

        # 跳过非日期行
        if not re.match(r"\d{4}-\d{2}-\d{2}", trade_date):
            continue

        rows.append({
            "trade_date": trade_date,
            "price": _parse_float(values[1]),
            "amount": _parse_float(values[2]),       # 万元
            "volume": _parse_float(values[3]),        # 万股
            "premium_rate": _parse_pct(values[4]),    # %
            "buyer": values[5].strip(),
            "seller": values[6].strip(),
        })

    return rows


async def get_block_trade_10jqka(stock_info: StockInfo) -> list[dict]:
    """获取个股大宗交易数据（同花顺页面解析）。

    Args:
        stock_info: 股票信息

    Returns:
        list[dict]: 大宗交易记录列表，按日期降序
    """
    code = stock_info.stock_code.split(".")[0]
    url = f"https://stockpage.10jqka.com.cn/{code}/"

    async with AsyncSession(impersonate=IMPERSONATE) as session:
        resp = await session.get(url, timeout=15)
        resp.raise_for_status()
        html = _clean_html(resp.content)

    rows = _parse_block_trade_table(html)
    logger.info("[%s] 大宗交易获取 %d 条记录", stock_info.stock_code, len(rows))
    return rows


if __name__ == "__main__":
    import json
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        rows = await get_block_trade_10jqka(stock_info)
        print(f"股票: {stock_info.stock_name}({stock_info.stock_code})")
        print(f"大宗交易记录: {len(rows)} 条")
        for row in rows:
            print(json.dumps(row, ensure_ascii=False))

    asyncio.run(main())
