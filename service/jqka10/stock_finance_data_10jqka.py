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
           '1.03万亿' -> 1030000000000.0
           '9852.29万' -> 98522900.0
           '52.08%' -> 52.08, '51.53' -> 51.53, '--' -> None
    """
    if not val_str or val_str.strip() in ("", "--", "不适用", "False", "True"):
        return None
    s = val_str.strip()
    try:
        if s.endswith("万亿"):
            return round(float(s[:-2]) * 1e12, 2)
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
    session: aiohttp.ClientSession | None = None,
) -> dict:
    """
    请求同花顺财务数据 JSON 接口。

    URL: https://basic.10jqka.com.cn/api/stock/finance/{stock_code}_{report_type}.json

    Args:
        session: 可选的外部 session，传入时复用连接，不传则内部创建。
    """
    url = f"{_FINANCE_API_BASE}/{stock_code}_{report_type}.json"
    headers = {**_HEADERS, "Referer": f"https://basic.10jqka.com.cn/{stock_code}/finance.shtml"}

    async def _do_fetch(s: aiohttp.ClientSession) -> dict:
        for attempt in range(1, max_retries + 1):
            try:
                async with s.get(
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

    if session is not None:
        return await _do_fetch(session)
    else:
        async with aiohttp.ClientSession() as s:
            return await _do_fetch(s)


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
    """获取利润表数据（单独调用，如需多张报表请用 get_stock_all_finance_data）"""
    return await get_stock_finance_report(stock_info, "benefit", data_key)


async def get_stock_balance(stock_info: StockInfo, data_key: str = "report") -> list[dict]:
    """获取资产负债表数据（单独调用，如需多张报表请用 get_stock_all_finance_data）"""
    return await get_stock_finance_report(stock_info, "debt", data_key)


async def get_stock_cashflow(stock_info: StockInfo, data_key: str = "report") -> list[dict]:
    """获取现金流量表数据（单独调用，如需多张报表请用 get_stock_all_finance_data）"""
    return await get_stock_finance_report(stock_info, "cash", data_key)


async def get_stock_finance_indicators(stock_info: StockInfo, data_key: str = "report") -> list[dict]:
    """获取主要财务指标（单独调用，如需多张报表请用 get_stock_all_finance_data）"""
    return await get_stock_finance_report(stock_info, "main", data_key)


async def get_stock_all_finance_data(
    stock_info: StockInfo,
    data_key: str = "report",
) -> dict[str, list[dict]]:
    """
    一次性获取全部四张财务报表数据（统一接口）。

    共享同一个 aiohttp.ClientSession 并发请求四种报表，
    避免多次创建连接，减少底层调用开销。

    Args:
        stock_info: 股票信息对象
        data_key: 数据维度 "report"(按报告期) / "year"(按年度) / "simple"(简化版)

    Returns:
        dict: {
            "benefit": [...],   # 利润表
            "debt": [...],      # 资产负债表
            "cash": [...],      # 现金流量表
            "main": [...]       # 主要财务指标
        }
    """
    report_types: list[ReportType] = ["benefit", "debt", "cash", "main"]
    async with aiohttp.ClientSession() as session:
        flashes = await asyncio.gather(
            *(
                _fetch_finance_json(stock_info.stock_code, rt, session=session)
                for rt in report_types
            )
        )
    return {
        rt: _parse_report_data(flash, data_key)
        for rt, flash in zip(report_types, flashes)
    }

# ─────────────────── 与 stock_financial_main 对齐的输出格式 ───────────────────

MAX_RECENT_PERIODS = 20

# 10jqka 中文字段 → eastmoney 英文 key 的映射
# 来源: main=主要财务指标, benefit=利润表, cash=现金流量表
_FIELD_MAP_MAIN = {
    "基本每股收益": "EPSJB",
    "稀释每股收益": "EPSXS",
    "每股净资产": "BPS",
    "每股公积金": "MGZBGJ",
    "每股未分配利润": "MGWFPLR",
    "每股经营现金流": "MGJYXJJE",
    "营业总收入": "TOTALOPERATEREVE",
    "毛利润": "MLR",
    "归属净利润": "PARENTNETPROFIT",
    "净利润": "PARENTNETPROFIT",  # main 表中的"净利润"实际是归母净利润
    "扣非净利润": "KCFJCXSYJLR",
    "营业总收入同比增长": "TOTALOPERATEREVETZ",
    "营业总收入同比增长率": "TOTALOPERATEREVETZ",
    "归属净利润同比增长": "PARENTNETPROFITTZ",
    "净利润同比增长率": "PARENTNETPROFITTZ",
    "扣非净利润同比增长": "KCFJCXSYJLRTZ",
    "扣非净利润同比增长率": "KCFJCXSYJLRTZ",
    "营业总收入滚动环比增长": "YYZSRGDHBZC",
    "归属净利润滚动环比增长": "NETPROFITRPHBZC",
    "扣非净利润滚动环比增长": "KFJLRGDHBZC",
    "净资产收益率": "ROEJQ",
    "加权净资产收益率": "ROEJQ",
    "净资产收益率-摊薄": "ROEJQ",  # 10jqka 有时用摊薄版
    "总资产收益率": "ZZCJLL",
    "销售毛利率": "XSMLL",
    "毛利率": "XSMLL",
    "销售净利率": "XSJLL",
    "净利率": "XSJLL",
    "实际税率": "TAXRATE",
    "预收账款/营业收入": "YSZKYYSR",
    "销售净现金流/营业收入": "XSJXLYYSR",
    "经营现金流/营业收入": "JYXJLYYSR",
    "每股资本公积金": "MGZBGJ",
    "流动比率": "LD",
    "速动比率": "SD",
    "现金流量比率": "XJLLB",
    "资产负债率": "ZCFZL",
    "权益乘数": "QYCS",
    "权益系数": "QYCS",
    "产权比率": "CQBL",
    "总资产周转天数": "ZZCZZTS",
    "存货周转天数": "CHZZTS",
    "应收账款周转天数": "YSZKZZTS",
    "总资产周转率": "TOAZZL",
    "存货周转率": "CHZZL",
    "应收账款周转率": "YSZKZZL",
}

# 金额类字段（输出时用 convert_amount_unit 格式化）
_AMOUNT_KEYS = {
    "TOTALOPERATEREVE", "PARENTNETPROFIT", "KCFJCXSYJLR", "MLR",
    "SINGLE_QUARTER_REVENUE", "SINGLE_QUARTER_KCFJCXSYJLR",
    "SINGLE_QUARTER_PARENTNETPROFIT",
}

# 与 stock_financial_main.FINANCIAL_INDICATORS 完全一致的输出指标列表
_OUTPUT_INDICATORS = [
    ("报告日期", "REPORT_DATE"),
    ("基本每股收益(元)", "EPSJB"),
    ("扣非每股收益(元)", "EPSKCJB"),
    ("稀释每股收益(元)", "EPSXS"),
    ("每股净资产(元)", "BPS"),
    ("每股公积金(元)", "MGZBGJ"),
    ("每股未分配利润(元)", "MGWFPLR"),
    ("每股经营现金流(元)", "MGJYXJJE"),
    ("营业总收入(元)", "TOTALOPERATEREVE"),
    ("单季度营业收入(元)", "SINGLE_QUARTER_REVENUE"),
    ("毛利润(元)", "MLR"),
    ("归母净利润(元)", "PARENTNETPROFIT"),
    ("单季归母净利润(元)", "SINGLE_QUARTER_PARENTNETPROFIT"),
    ("扣非净利润(元)", "KCFJCXSYJLR"),
    ("单季扣非净利润(元)", "SINGLE_QUARTER_KCFJCXSYJLR"),
    ("营业总收入同比增长(%)", "TOTALOPERATEREVETZ"),
    ("归属净利润同比增长(%)", "PARENTNETPROFITTZ"),
    ("扣非净利润同比增长(%)", "KCFJCXSYJLRTZ"),
    ("单季营业收入同比增长(%)", "SINGLE_QUARTER_REVENUETZ"),
    ("单季归母净利润同比增长(%)", "SINGLE_QUARTER_PARENTNETPROFITTZ"),
    ("单季扣非净利润同比增长(%)", "SINGLE_QUARTER_KCFJCXSYJLRTZ"),
    ("营业总收入环比增长(%)", "YYZSRGDHBZC"),
    ("归属净利润环比增长(%)", "NETPROFITRPHBZC"),
    ("扣非净利润环比增长(%)", "KFJLRGDHBZC"),
    ("净资产收益率(加权)(%)", "ROEJQ"),
    ("净资产收益率(扣非/加权)(%)", "ROEKCJQ"),
    ("净资产收益率_1(扣非/加权)(%)", "ROEKCJQ_1"),
    ("总资产收益率(加权)(%)", "ZZCJLL"),
    ("毛利率(%)", "XSMLL"),
    ("净利率(%)", "XSJLL"),
    ("预收账款/营业收入", "YSZKYYSR"),
    ("销售净现金流/营业收入", "XSJXLYYSR"),
    ("经营现金流/营业收入", "JYXJLYYSR"),
    ("实际税率(%)", "TAXRATE"),
    ("流动比率", "LD"),
    ("速动比率", "SD"),
    ("现金流量比率", "XJLLB"),
    ("资产负债率(%)", "ZCFZL"),
    ("权益系数", "QYCS"),
    ("产权比率", "CQBL"),
    ("总资产周转天数(天)", "ZZCZZTS"),
    ("存货周转天数(天)", "CHZZTS"),
    ("应收账款周转天数(天)", "YSZKZZTS"),
    ("总资产周转率(次)", "TOAZZL"),
    ("存货周转率(次)", "CHZZL"),
    ("应收账款周转率(次)", "YSZKZZL"),
]

# 报告期日期 → 报告期名称
_REPORT_DATE_NAME_MAP = {
    "03-31": "一季报",
    "06-30": "中报",
    "09-30": "三季报",
    "12-31": "年报",
}


def _date_to_report_name(date_str: str) -> str:
    """将 '2024-12-31' 转为 '2024年报' 格式"""
    if len(date_str) >= 10:
        year = date_str[:4]
        suffix = _REPORT_DATE_NAME_MAP.get(date_str[5:10], "")
        return f"{year}{suffix}" if suffix else date_str
    return date_str


def _map_raw_to_standard(raw_records: list[dict]) -> list[dict]:
    """
    将 10jqka 原始中文字段映射为 eastmoney 标准英文 key 的 dict 列表。
    """
    result = []
    for rec in raw_records:
        mapped = {}
        date_str = rec.get("报告期", "")
        mapped["REPORT_DATE"] = date_str
        mapped["REPORT_DATE_NAME"] = _date_to_report_name(date_str)
        for cn_name, val in rec.items():
            if cn_name == "报告期":
                continue
            en_key = _FIELD_MAP_MAIN.get(cn_name)
            if en_key and en_key not in mapped:
                mapped[en_key] = val
        result.append(mapped)
    return result


def _calculate_single_quarter(data_list: list[dict], src_key: str, dst_key: str):
    """通用单季度计算：从累计值推算单季值"""
    for i, d in enumerate(data_list):
        report_name = d.get("REPORT_DATE_NAME", "")
        cumulative = d.get(src_key)
        if cumulative is None or not isinstance(cumulative, (int, float)):
            d[dst_key] = None
            continue
        year = report_name[:4]
        if "一季报" in report_name:
            d[dst_key] = cumulative
        elif "中报" in report_name:
            prev = next(
                (data_list[j].get(src_key) for j in range(i + 1, len(data_list))
                 if "一季报" in data_list[j].get("REPORT_DATE_NAME", "")
                 and data_list[j].get("REPORT_DATE_NAME", "")[:4] == year),
                None,
            )
            d[dst_key] = cumulative - prev if isinstance(prev, (int, float)) else None
        elif "三季报" in report_name:
            prev = next(
                (data_list[j].get(src_key) for j in range(i + 1, len(data_list))
                 if "中报" in data_list[j].get("REPORT_DATE_NAME", "")
                 and data_list[j].get("REPORT_DATE_NAME", "")[:4] == year),
                None,
            )
            d[dst_key] = cumulative - prev if isinstance(prev, (int, float)) else None
        elif "年报" in report_name:
            prev = next(
                (data_list[j].get(src_key) for j in range(i + 1, len(data_list))
                 if "三季报" in data_list[j].get("REPORT_DATE_NAME", "")
                 and data_list[j].get("REPORT_DATE_NAME", "")[:4] == year),
                None,
            )
            d[dst_key] = cumulative - prev if isinstance(prev, (int, float)) else None
        else:
            d[dst_key] = None


def _calculate_single_quarter_yoy(data_list: list[dict], sq_key: str, dst_key: str):
    """计算单季度同比增长率"""
    for i, d in enumerate(data_list):
        report_name = d.get("REPORT_DATE_NAME", "")
        sq_val = d.get(sq_key)
        year = report_name[:4]
        if not year.isdigit() or sq_val is None:
            d[dst_key] = None
            continue
        prev_year = str(int(year) - 1)
        prev_sq = None
        for j in range(i + 1, len(data_list)):
            pn = data_list[j].get("REPORT_DATE_NAME", "")
            if pn[:4] == prev_year and pn[4:] == report_name[4:]:
                prev_sq = data_list[j].get(sq_key)
                break
        if prev_sq is not None and prev_sq != 0:
            d[dst_key] = round((sq_val - prev_sq) / abs(prev_sq) * 100, 4)
        else:
            d[dst_key] = None


def _calculate_epskcjb(data_list: list[dict]):
    """计算扣非每股收益 = 基本每股收益 × (扣非净利润 / 归母净利润)"""
    for d in data_list:
        eps = d.get("EPSJB")
        net = d.get("PARENTNETPROFIT")
        kcf = d.get("KCFJCXSYJLR")
        if eps is not None and net is not None and kcf is not None and net != 0:
            d.setdefault("EPSKCJB", round(eps * (kcf / net), 4))
        else:
            d.setdefault("EPSKCJB", None)


def _calculate_roe_kcjq(data_list: list[dict]):
    """计算净资产收益率(扣非/加权) = ROE加权 × (扣非净利润 / 归母净利润)"""
    for d in data_list:
        roe = d.get("ROEJQ")
        kcf = d.get("KCFJCXSYJLR")
        net = d.get("PARENTNETPROFIT")
        if roe is not None and kcf is not None and net is not None and net != 0:
            val = round(roe * (kcf / net), 4)
            d["ROEKCJQ_1"] = val
            d["ROEKCJQ"] = val
        else:
            d["ROEKCJQ_1"] = None
            d.setdefault("ROEKCJQ", None)


def _enrich_computed_fields(data_list: list[dict]):
    """统一计算所有衍生字段（单季度值、同比增长、扣非每股收益、扣非ROE）"""
    _calculate_single_quarter(data_list, "TOTALOPERATEREVE", "SINGLE_QUARTER_REVENUE")
    _calculate_single_quarter(data_list, "PARENTNETPROFIT", "SINGLE_QUARTER_PARENTNETPROFIT")
    _calculate_single_quarter(data_list, "KCFJCXSYJLR", "SINGLE_QUARTER_KCFJCXSYJLR")
    _calculate_single_quarter_yoy(data_list, "SINGLE_QUARTER_REVENUE", "SINGLE_QUARTER_REVENUETZ")
    _calculate_single_quarter_yoy(data_list, "SINGLE_QUARTER_PARENTNETPROFIT", "SINGLE_QUARTER_PARENTNETPROFITTZ")
    _calculate_single_quarter_yoy(data_list, "SINGLE_QUARTER_KCFJCXSYJLR", "SINGLE_QUARTER_KCFJCXSYJLRTZ")
    _calculate_epskcjb(data_list)
    _calculate_roe_kcjq(data_list)


async def _prepare_standard_data(
    stock_info: StockInfo,
    indicator_keys: list[str] | None = None,
) -> tuple[list[dict], list[tuple[str, str]]]:
    """
    内部公共方法：获取并合并 main + benefit 数据，计算衍生字段。

    Returns:
        (data_list, indicators): 标准化后的数据列表 和 需要输出的指标列表
    """
    all_data = await get_stock_all_finance_data(stock_info)
    main_records = _map_raw_to_standard(all_data.get("main", []))
    benefit_records = _map_raw_to_standard(all_data.get("benefit", []))

    benefit_by_date = {r["REPORT_DATE"]: r for r in benefit_records}
    for rec in main_records:
        b = benefit_by_date.get(rec["REPORT_DATE"], {})
        for k, v in b.items():
            if k not in rec or rec[k] is None:
                rec[k] = v

    recent = main_records[:MAX_RECENT_PERIODS]
    _enrich_computed_fields(recent)

    indicators = (
        _OUTPUT_INDICATORS if indicator_keys is None
        else [(n, k) for n, k in _OUTPUT_INDICATORS if k in indicator_keys]
    )
    return recent, indicators


async def get_financial_raw_data(
    stock_info: StockInfo,
    indicator_keys: list[str] | None = None,
) -> list[dict]:
    """
    获取原始财务数据，字段用英文命名，数值保持原始精度（不做单位转换）。

    返回的每条记录包含:
      - REPORT_DATE_NAME: 报告期名称（如 "2024年报"）
      - REPORT_DATE: 报告日期（如 "2024-12-31"）
      - 以及 _OUTPUT_INDICATORS 中定义的各英文字段

    Args:
        stock_info: 股票信息对象
        indicator_keys: 需要的指标英文 key 列表，None 表示全部

    Returns:
        list[dict]: 英文字段名的原始数值数据列表
    """
    recent, indicators = await _prepare_standard_data(stock_info, indicator_keys)

    result = []
    for d in recent:
        record = {
            "REPORT_DATE_NAME": d.get("REPORT_DATE_NAME", ""),
            "REPORT_DATE": d.get("REPORT_DATE", ""),
        }
        for _name, key in indicators:
            val = d.get(key)
            if val is None:
                record[key] = None
            elif isinstance(val, (int, float)):
                record[key] = round(val, 4)
            else:
                val_str = str(val)
                record[key] = val_str[:10] if val_str else None
        result.append(record)

    return result


async def get_financial_data_to_json(
    stock_info: StockInfo,
    indicator_keys: list[str] | None = None,
) -> list[dict]:
    """
    获取财务数据并转换为与 stock_financial_main.get_financial_data_to_json 一致的 JSON 格式。

    数据来源为同花顺，输出格式与东方财富完全对齐，可作为替代数据源。

    Args:
        stock_info: 股票信息对象
        indicator_keys: 需要的指标英文 key 列表，None 表示全部

    Returns:
        list[dict]: 每条记录包含 "报告期"、"报告日期" 及各指标中文名字段。
    """
    from common.utils.amount_utils import convert_amount_unit

    recent, indicators = await _prepare_standard_data(stock_info, indicator_keys)

    result = []
    for d in recent:
        period = {
            "报告期": d.get("REPORT_DATE_NAME", ""),
            "报告日期": d.get("REPORT_DATE", ""),
        }
        for name, key in indicators:
            val = d.get(key)
            if val is None:
                period[name] = None
            elif isinstance(val, (int, float)):
                if key in _AMOUNT_KEYS:
                    period[name] = convert_amount_unit(val)
                else:
                    period[name] = round(val, 4)
            else:
                val_str = str(val)
                period[name] = val_str[:10] if val_str else None
        result.append(period)

    return result


async def get_financial_data_to_markdown(
    stock_info: StockInfo,
    indicator_keys: list[str] | None = None,
) -> str:
    """
    获取财务数据并转换为与 stock_financial_main.get_financial_data_to_markdown 一致的 Markdown 格式。

    Args:
        stock_info: 股票信息对象
        indicator_keys: 需要的指标英文 key 列表，None 表示全部

    Returns:
        str: Markdown 表格字符串
    """
    from common.utils.amount_utils import convert_amount_unit

    recent, indicators = await _prepare_standard_data(stock_info, indicator_keys)
    if not recent:
        return "暂无财务数据"

    md = "## 主要财务指标\n\n"
    md += "| 指标 | " + " | ".join(d.get("REPORT_DATE_NAME", "") for d in recent) + " |\n"
    md += "|" + "---|" * (len(recent) + 1) + "\n"

    for name, key in indicators:
        row = f"| {name} | "
        values = []
        for d in recent:
            val = d.get(key)
            if val is None:
                values.append("-")
            elif isinstance(val, (int, float)):
                if key in _AMOUNT_KEYS:
                    values.append(convert_amount_unit(val))
                else:
                    values.append(f"{val:.4f}")
            else:
                values.append(str(val))
        row += " | ".join(values) + " |\n"
        md += row

    return md


def get_financial_data_from_db(
    stock_info: StockInfo,
    limit: int | None = None,
    indicator_keys: list[str] | None = None,
) -> list[dict]:
    """
    从本地数据库查询已存储的财报数据。

    数据由 kline_data_scheduler 批量拉取后写入，
    格式与 get_financial_data_to_json 返回值一致。

    优先从 DB 读取，避免重复请求远程接口。

    Args:
        stock_info: 股票信息对象
        limit: 返回的最大记录数，None 表示全部
        indicator_keys: 需要过滤的指标英文 key 列表，None 表示返回全部字段

    Returns:
        list[dict]: 按报告期倒序排列的财报数据，每条记录包含
                    "报告期"、"报告日期" 及各指标中文名字段。
                    数据库中无数据时返回空列表。
    """
    from dao.stock_finance_dao import get_finance_from_db

    records = get_finance_from_db(stock_info.stock_code_normalize, limit=limit)
    if not records or indicator_keys is None:
        return records

    # 按 indicator_keys 过滤字段
    allowed_names = {"报告期", "报告日期"}
    for _cn, en_key in _OUTPUT_INDICATORS:
        if en_key in indicator_keys:
            allowed_names.add(_cn)

    return [
        {k: v for k, v in rec.items() if k in allowed_names}
        for rec in records
    ]


if __name__ == "__main__":
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name("飞荣达")
        if not stock_info:
            logger.info("未找到股票信息")
            return

        logger.info(f"=== {stock_info.stock_name} ({stock_info.stock_code_normalize}) ===\n")

        # 标准 JSON 格式输出（与 stock_financial_main 一致）
        json_data = await get_financial_data_to_json(stock_info)
        logger.info("【标准JSON格式（前3期）】")
        for item in json_data[:3]:
            logger.info(json.dumps(item, ensure_ascii=False, indent=2))

        # 标准 Markdown 格式输出
        logger.info("\n【标准Markdown格式】")
        md = await get_financial_data_to_markdown(stock_info)
        logger.info(md)

    asyncio.run(main())
