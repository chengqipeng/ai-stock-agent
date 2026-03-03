"""
同花顺财报数据抓取模块

从 basic.10jqka.com.cn 获取上市公司财务报表数据，包括：
- 利润表（benefit）：营业收入、净利润、扣非净利润等
- 资产负债表（debt）：总资产、总负债、净资产等
- 现金流量表（cash）：经营/投资/筹资现金流等
- 主要财务指标（main）：每股收益、ROE、毛利率等

接口地址：
  https://basic.10jqka.com.cn/api/stock/finance/{stock_code}_{report_type}.json

数据结构：
  {
    "flashData": "{...}",       # JSON 字符串
    "fieldflashData": "{...}"   # 同比数据
  }

  flashData 解析后：
  {
    "title": ["科目\\时间", ["*净利润", "元", 2, false, true], ...],
    "report": [["2024-12-31", "2024-06-30", ...], ["893.35亿", ...], ...],
    "simple": [...],   # 简化版
    "year": [...],     # 按年度
    "report_yoy": [...],  # 同比
    ...
  }

  title[0] 是表头标识，title[i] (i>=1) 格式为 [字段名, 单位, 缩进, 是否分组标题, 是否数据行]
  report[0] 是报告期日期列表，report[i] 是 title[i] 对应的数据列表
"""

import json
import asyncio
import logging
from typing import Literal

import aiohttp

from common.utils.stock_info_utils import StockInfo

logger = logging.getLogger(__name__)

_FINANCE_API_BASE = "https://basic.10jqka.com.cn/api/stock/finance"

# 报表类型：benefit=利润表, debt=资产负债表, cash=现金流量表, main=主要财务指标
ReportType = Literal["benefit", "debt", "cash", "main"]

_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://basic.10jqka.com.cn/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

_REPORT_TYPE_CN = {
    "benefit": "利润表",
    "debt": "资产负债表",
    "cash": "现金流量表",
    "main": "主要财务指标",
}


def _parse_value(val_str: str):
    """
    将同花顺返回的值字符串转为数值。
    示例：'893.35亿' -> 89335000000.0, '32.60亿' -> 3260000000.0
           '52.08%' -> 52.08, '51.53' -> 51.53, '--' -> None
    """
    if not val_str or val_str.strip() in ("", "--", "不适用", "False", "True"):
        return None
    s = val_str.strip()
    try:
        if s.endswith("亿"):
            return round(float(s[:-1]) * 1e8, 2)
        if s.endswith("万"):
            return round(float(s[:-1]) * 1e4, 2)
        if s.endswith("%"):
            return float(s[:-1])
        return float(s)
    except (ValueError, TypeError):
        return s


async def _fetch_finance_json(
    stock_code: str,
    report_type: ReportType,
    max_retries: int = 3,
) -> dict:
    """
    请求同花顺财务数据 JSON 接口。

    URL: https://basic.10jqka.com.cn/api/stock/finance/{stock_code}_{report_type}.json
    """
    url = f"{_FINANCE_API_BASE}/{stock_code}_{report_type}.json"
    headers = {**_HEADERS, "Referer": f"https://basic.10jqka.com.cn/{stock_code}/finance.shtml"}

    for attempt in range(1, max_retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "[finance_10jqka] HTTP %d, code=%s, type=%s",
                            resp.status, stock_code, report_type,
                        )
                        raise aiohttp.ClientResponseError(
                            request_info=resp.request_info,
                            history=resp.history,
                            status=resp.status,
                            message=f"HTTP {resp.status}",
                        )
                    text = await resp.text()
                    raw = json.loads(text)
                    flash_str = raw.get("flashData", "{}")
                    flash = json.loads(flash_str) if isinstance(flash_str, str) else flash_str
                    return flash

        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            if attempt < max_retries:
                wait = 2 ** attempt
                logger.warning(
                    "[finance_10jqka] 第%d次请求失败 code=%s type=%s: %s，%ds后重试",
                    attempt, stock_code, report_type, e, wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    "[finance_10jqka] 重试%d次后仍失败 code=%s type=%s: %s",
                    max_retries, stock_code, report_type, e,
                )
                raise


def _extract_field_name(title_item) -> str | None:
    """从 title 项中提取字段名。"""
    if isinstance(title_item, str):
        return None  # 表头标识行，跳过
    if isinstance(title_item, list) and len(title_item) >= 2:
        name = title_item[0]
        # 去掉前缀 * 和序号前缀（如 "一、"、"二、"）
        name = name.lstrip("*")
        return name
    return None


def _is_data_row(title_item) -> bool:
    """判断 title 项是否为数据行（非分组标题）。"""
    if isinstance(title_item, list) and len(title_item) >= 5:
        return title_item[4] is True  # 第5个元素表示是否为数据行
    return False


def _parse_report_data(
    flash: dict,
    data_key: str = "report",
    include_headers: bool = False,
) -> list[dict]:
    """
    将 flashData 解析为结构化的字典列表。

    Args:
        flash: 解析后的 flashData
        data_key: 数据键名，"report"=按报告期, "year"=按年度, "simple"=简化版
        include_headers: 是否包含分组标题行

    Returns:
        list[dict]: 每个 dict 代表一个报告期的数据，key 为中文字段名
    """
    titles = flash.get("title", [])
    data = flash.get(data_key, [])

    if not titles or not data or len(data) < 2:
        return []

    # data[0] 是日期/年份列表
    dates = data[0]
    if not dates:
        return []

    results = []
    for col_idx, date_val in enumerate(dates):
        record = {"报告期": str(date_val)}
        for row_idx in range(1, len(titles)):
            if row_idx >= len(data):
                break
            field_name = _extract_field_name(titles[row_idx])
            if field_name is None:
                continue
            if not include_headers and not _is_data_row(titles[row_idx]):
                continue
            values = data[row_idx]
            if col_idx < len(values):
                record[field_name] = _parse_value(str(values[col_idx]))
            else:
                record[field_name] = None
        results.append(record)

    return results


async def get_stock_finance_report(
    stock_info: StockInfo,
    report_type: ReportType = "main",
    data_key: str = "report",
) -> list[dict]:
    """
    获取股票财报数据。

    Args:
        stock_info: 股票信息对象
        report_type: 报表类型
            - "benefit" : 利润表
            - "debt"    : 资产负债表
            - "cash"    : 现金流量表
            - "main"    : 主要财务指标（默认）
        data_key: 数据维度
            - "report"  : 按报告期（默认）
            - "year"    : 按年度
            - "simple"  : 简化版

    Returns:
        list[dict]: 各期财报数据列表，按报告期倒序排列。
        每条记录包含中文字段名，数值已转换（亿→元，%→数值）。

    Example:
        >>> stock_info = get_stock_info_by_name("贵州茅台")
        >>> data = await get_stock_finance_report(stock_info, "benefit")
        >>> print(data[0]["报告期"], data[0]["营业收入"])
        2025-09-30 128454000000.0
    """
    flash = await _fetch_finance_json(stock_info.stock_code, report_type)
    return _parse_report_data(flash, data_key)


async def get_stock_income(stock_info: StockInfo, data_key: str = "report") -> list[dict]:
    """获取利润表数据"""
    return await get_stock_finance_report(stock_info, "benefit", data_key)


async def get_stock_balance(stock_info: StockInfo, data_key: str = "report") -> list[dict]:
    """获取资产负债表数据"""
    return await get_stock_finance_report(stock_info, "debt", data_key)


async def get_stock_cashflow(stock_info: StockInfo, data_key: str = "report") -> list[dict]:
    """获取现金流量表数据"""
    return await get_stock_finance_report(stock_info, "cash", data_key)


async def get_stock_finance_indicators(stock_info: StockInfo, data_key: str = "report") -> list[dict]:
    """获取主要财务指标"""
    return await get_stock_finance_report(stock_info, "main", data_key)


async def get_stock_all_finance_data(stock_info: StockInfo) -> dict[str, list[dict]]:
    """
    一次性并发获取全部四张财务报表数据。

    Returns:
        dict: {
            "benefit": [...],   # 利润表
            "debt": [...],      # 资产负债表
            "cash": [...],      # 现金流量表
            "main": [...]       # 主要财务指标
        }
    """
    benefit, debt, cash, main = await asyncio.gather(
        get_stock_income(stock_info),
        get_stock_balance(stock_info),
        get_stock_cashflow(stock_info),
        get_stock_finance_indicators(stock_info),
    )
    return {"benefit": benefit, "debt": debt, "cash": cash, "main": main}


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("贵州茅台")
        if not stock_info:
            print("未找到股票信息")
            return

        print(f"=== {stock_info.stock_name} ({stock_info.stock_code_normalize}) ===\n")

        # 获取主要财务指标
        indicators = await get_stock_finance_indicators(stock_info)
        if indicators:
            print("【主要财务指标】")
            for item in indicators[:3]:
                print(json.dumps(item, ensure_ascii=False, indent=2))
            print()

        # 获取利润表
        income = await get_stock_income(stock_info)
        if income:
            print("【利润表（最近3期）】")
            for item in income[:3]:
                print(json.dumps(item, ensure_ascii=False, indent=2))
            print()

        # 获取资产负债表
        balance = await get_stock_balance(stock_info)
        if balance:
            print("【资产负债表（最近1期）】")
            print(json.dumps(balance[0], ensure_ascii=False, indent=2))

    asyncio.run(main())
