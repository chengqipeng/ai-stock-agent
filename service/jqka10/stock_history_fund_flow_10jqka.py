"""
同花顺个股历史资金流向数据模块

数据来源：https://stockpage.10jqka.com.cn/{stock_code}/funds/
页面 SSR 渲染历史资金数据一览表格，包含：
  日期、收盘价、涨跌幅、资金净流入、5日主力净额、
  大单(主力)净额/净占比、中单净额/净占比、小单净额/净占比

接口与 service/eastmoney/stock_info/stock_history_flow.py 保持一致。
使用 curl_cffi 模拟浏览器 TLS 指纹绕过反爬，UTF-8 编码解析。

注意：同花顺页面的"大单(主力)"等同于东方财富的"主力"（超大单+大单），
     同花顺不区分超大单和大单，因此 super_net/super_pct 固定为 0/None，
     big_net/big_pct 也固定为 0/None，main_net = 页面"大单(主力)"值。
"""

import asyncio
import logging
import re

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from common.utils.amount_utils import convert_amount_unit
from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

IMPERSONATE = "chrome131"


# ── HTML 表格列定义（页面原始11列） ─────────────────────────

_RAW_FIELDS = [
    "date", "close_price", "change_pct",
    "net_flow", "main_net_5day",
    "big_net", "big_net_pct",
    "mid_net", "mid_net_pct",
    "small_net", "small_net_pct",
]


# ── 解析工具 ──────────────────────────────────────────────

def _clean_html(raw_bytes: bytes) -> str:
    """UTF-8 解码 + 移除 IE 条件注释"""
    text = raw_bytes.decode("utf-8", errors="replace")
    text = re.sub(r"<!--\[if.*?\]-->", "", text, flags=re.DOTALL)
    text = re.sub(r"<!\[endif\]-->", "", text)
    return text


def _format_date(raw: str) -> str:
    """将 YYYYMMDD 转为 YYYY-MM-DD"""
    raw = raw.strip()
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def _parse_pct(text: str) -> float | None:
    """将 '-3.00%' 转为 -3.0"""
    text = text.strip().replace("%", "")
    if not text or text == "--":
        return None
    try:
        return round(float(text), 2)
    except ValueError:
        return None


def _parse_float(text: str) -> float | None:
    text = text.strip()
    if not text or text == "--":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _wan_to_yuan(val: float | None) -> float:
    """万元 → 元"""
    if val is None:
        return 0
    return round(val * 10000, 2)


def _parse_fund_flow_table(html: str) -> list[dict]:
    """
    解析个股历史资金流向 HTML 表格。
    页面包含两个 class="m_table_3" 的表格（日线/周线），取第一个。
    前两行是表头，数据行从第3行开始，每行11个 <td>。
    返回原始解析结果列表（万元单位，百分比为 float）。
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="m_table_3")
    if not table:
        logger.warning("未找到 class=m_table_3 的资金流向历史表格")
        return []

    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        values = [td.get_text(strip=True) for td in tds]
        if not any(v for v in values) or len(values) < len(_RAW_FIELDS):
            continue

        rows.append({
            "date":         _format_date(values[0]),
            "close_price":  _parse_float(values[1]),
            "change_pct":   _parse_pct(values[2]),
            "net_flow":     _parse_float(values[3]),      # 万元
            "main_net_5day": _parse_float(values[4]),     # 万元
            "big_net":      _parse_float(values[5]),      # 万元（大单=主力）
            "big_net_pct":  _parse_pct(values[6]),
            "mid_net":      _parse_float(values[7]),      # 万元
            "mid_net_pct":  _parse_pct(values[8]),
            "small_net":    _parse_float(values[9]),      # 万元
            "small_net_pct": _parse_pct(values[10]),
        })
    return rows


# ── 网络请求 ──────────────────────────────────────────────

async def _fetch_page(stock_code: str, session: AsyncSession | None = None) -> list[dict]:
    """请求同花顺个股资金流向页面并解析"""
    code = stock_code.split(".")[0]
    url = f"https://stockpage.10jqka.com.cn/{code}/funds/"

    async def _do(s: AsyncSession) -> list[dict]:
        resp = await s.get(url, timeout=15)
        resp.raise_for_status()
        return _parse_fund_flow_table(_clean_html(resp.content))

    if session:
        return await _do(session)
    async with AsyncSession(impersonate=IMPERSONATE) as s:
        return await _do(s)


# ── 对外接口（与 stock_history_flow.py 保持一致） ─────────

async def get_fund_flow_history(stock_info: StockInfo) -> list[dict]:
    """获取资金流向历史数据（原始解析结果，万元单位，按日期倒序）"""
    rows = await _fetch_page(stock_info.stock_code)
    logger.info("[%s] 历史资金流向获取 %d 条记录", stock_info.stock_code, len(rows))
    return rows


async def get_fund_flow_history_json(
    stock_info: StockInfo,
    fields: list[str] | None = None,
    page_size: int = 120,
) -> dict:
    """获取资金流向历史数据并转换为JSON格式

    与 eastmoney stock_history_flow.get_fund_flow_history_json 接口一致。
    金额单位统一为元（页面万元 × 10000）。

    注意：同花顺"大单(主力)" = 东方财富"主力"（超大单+大单），
         因此 super_net/big_net 无法拆分，均置为 0，main_net 取页面大单值。

    Args:
        stock_info: 股票信息
        fields: 可选字段列表，None 表示返回所有字段
        page_size: 返回数据条数
    """
    raw_rows = await get_fund_flow_history(stock_info)
    result = []
    for row in raw_rows[:page_size]:
        # 页面"大单(主力)" = 东方财富"主力"
        main_net_yuan = _wan_to_yuan(row["big_net"])
        mid_net_yuan = _wan_to_yuan(row["mid_net"])
        small_net_yuan = _wan_to_yuan(row["small_net"])

        all_data = {
            "date":          row["date"],
            "close_price":   row["close_price"],
            "change_pct":    row["change_pct"],
            "main_net":      main_net_yuan,
            "main_net_str":  convert_amount_unit(main_net_yuan),
            "main_pct":      row["big_net_pct"],
            "super_net":     0,
            "super_net_str": convert_amount_unit(0),
            "super_pct":     None,
            "big_net":       0,
            "big_net_str":   convert_amount_unit(0),
            "big_pct":       None,
            "mid_net":       mid_net_yuan,
            "mid_net_str":   convert_amount_unit(mid_net_yuan),
            "mid_pct":       row["mid_net_pct"],
            "small_net":     small_net_yuan,
            "small_net_str": convert_amount_unit(small_net_yuan),
            "small_pct":     row["small_net_pct"],
        }

        if fields:
            result.append({k: v for k, v in all_data.items() if k in fields})
        else:
            result.append(all_data)

    return {
        "stock_name": stock_info.stock_name,
        "stock_code": stock_info.stock_code_normalize,
        "data": result,
    }


async def get_fund_flow_history_json_cn(
    stock_info: StockInfo,
    fields: list[str] | None = None,
    page_size: int = 120,
) -> dict:
    """获取资金流向历史数据并转换为中文key的JSON格式

    Args:
        stock_info: 股票信息
        fields: 可选字段列表（中文），None 表示返回所有字段
        page_size: 返回数据条数
    """
    en_to_cn = {
        "date": "日期", "close_price": "收盘价", "change_pct": "涨跌幅",
        "main_net": "主力净流入净额", "main_net_str": "主力净流入净额(文本)", "main_pct": "主力净流入净占比",
        "super_net": "超大单净流入净额", "super_net_str": "超大单净流入净额(文本)", "super_pct": "超大单净流入净占比",
        "big_net": "大单净流入净额", "big_net_str": "大单净流入净额(文本)", "big_pct": "大单净流入净占比",
        "mid_net": "中单净流入净额", "mid_net_str": "中单净流入净额(文本)", "mid_pct": "中单净流入占比",
        "small_net": "小单净流入净额", "small_net_str": "小单净流入净额(文本)", "small_pct": "小单净流入净占比",
    }
    cn_to_en = {v: k for k, v in en_to_cn.items()}

    en_fields = [cn_to_en.get(f, f) for f in fields] if fields else None
    result = await get_fund_flow_history_json(stock_info, en_fields, page_size)

    cn_data = []
    for item in result["data"]:
        cn_data.append({en_to_cn.get(k, k): v for k, v in item.items()})

    return {
        "股票名称": result["stock_name"],
        "股票代码": result["stock_code"],
        "数据": cn_data,
    }


# ── 测试入口 ──────────────────────────────────────────────

if __name__ == "__main__":
    import json
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("生益科技")
        result = await get_fund_flow_history_json(stock_info, page_size=3)
        print(f"股票: {result['stock_name']}({result['stock_code']})")
        for item in result["data"]:
            print(json.dumps(item, ensure_ascii=False))

    asyncio.run(main())
