"""
美股指数数据接口 — 数据来源：东方财富 quote.eastmoney.com/gb/zsNDX.html

提供以下功能：
1. 美股指数日K线数据（纳斯达克/道琼斯/标普500等）
2. 美股指数当日实时行情
3. 美洲主要指数列表
4. 中国概念股涨幅榜
5. 知名美股涨幅榜
6. 互联网中国涨幅榜
"""

import asyncio
import json
import logging
import time
from datetime import datetime

from common.http.http_utils import (
    EASTMONEY_PUSH2_API_URL,
    EASTMONEY_PUSH2HIS_API_URL,
    EASTMONEY_PUSH_API_URL,
    fetch_eastmoney_api,
)
logger = logging.getLogger(__name__)

# ─────────── 美股指数 secid 映射 ───────────
# 东方财富美股/全球指数 market 前缀为 100
US_INDEX_MAP = {
    "NDX":  {"secid": "100.NDX",  "name": "纳斯达克100"},
    "DJIA": {"secid": "100.DJIA", "name": "道琼斯"},
    "SPX":  {"secid": "100.SPX",  "name": "标普500"},
}

# ─────────── 板块 fs 代码 ───────────
FS_GLOBAL_INDICES = "m:100"                     # 全球指数（含美洲/欧洲/亚洲/澳洲）
FS_CHINA_CONCEPT = "m:105+t:3,m:106+t:3"       # 中国概念股（NASDAQ+NYSE）
FS_FAMOUS_US = "m:105+t:1,m:106+t:1"           # 知名美股
FS_INTERNET_CHINA = "m:105+t:4,m:106+t:4"      # 互联网中国

# ─────────── 全球指数按地区分类 ───────────
# 美洲指数代码
AMERICAS_INDEX_CODES = {"NDX", "NDX100", "DJIA", "SPX", "TSX", "BVSP", "MXX"}
# 欧洲指数代码
EUROPE_INDEX_CODES = {
    "FTSE", "GDAXI", "FCHI", "AEX", "BFX", "IBEX", "MIB", "PSI20", "ATX",
    "SSMI", "OMXC20", "OMXSPI", "OSEBX", "HEX", "HEX25", "ISEQ", "ASE",
    "PX", "WIG", "RTS", "SX5E", "ASX", "AXX", "MCX", "NMX", "ICEXI",
}
# 亚洲指数代码
ASIA_INDEX_CODES = {
    "N225", "KS11", "KOSPI200", "HSI", "HSCEI", "TWII", "STI", "FSTAS",
    "FSTM", "KLSE", "SET", "JKSE", "PSI", "SENSEX", "VNINDEX", "CSEALL",
    "KSE100", "FISAULMU", "XIN9", "HSAHP", "HKEXT100", "HKEXTN", "HKEXTT",
}
# 澳洲指数代码
AUSTRALIA_INDEX_CODES = {"AORD", "AS51", "NZ50"}

REFERER = "https://quote.eastmoney.com/gb/zsNDX.html"


# ═══════════════════════════════════════════════════════════════
# 1. 美股指数日K线数据
# ═══════════════════════════════════════════════════════════════

async def get_us_index_day_kline(index_code: str = "NDX", limit: int = 120) -> list[dict]:
    """获取美股指数日K线数据

    Args:
        index_code: 指数代码，支持 NDX / DJIA / SPX / IXIC
        limit: 返回K线条数，默认120

    Returns:
        list[dict]: 由新到旧排列的K线数据列表，每条包含：
            日期, 开盘价, 收盘价, 最高价, 最低价, 成交量, 成交额,
            振幅(%), 涨跌幅(%), 涨跌额, 换手率(%)
    """
    index_info = US_INDEX_MAP.get(index_code)
    if not index_info:
        raise ValueError(f"不支持的美股指数代码: {index_code}，可选: {list(US_INDEX_MAP.keys())}")

    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/kline/get"
    params = {
        "secid": index_info["secid"],
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",       # 日K
        "fqt": "1",
        "end": datetime.now().strftime("%Y%m%d"),
        "lmt": limit,
        "cb": "quote_jp1",
        "_": int(time.time() * 1000),
    }

    data = await fetch_eastmoney_api(url, params, referer=REFERER)
    klines_raw = data.get("data", {}).get("klines", [])

    result = []
    for kline in klines_raw:
        fields = kline.split(",")
        result.append({
            "日期":       fields[0],
            "开盘价":     _safe_float(fields[1]),
            "收盘价":     _safe_float(fields[2]),
            "最高价":     _safe_float(fields[3]),
            "最低价":     _safe_float(fields[4]),
            "成交量":     _safe_float(fields[5]),
            "成交额":     fields[6],
            "振幅(%)":    _safe_float(fields[7]),
            "涨跌幅(%)":  _safe_float(fields[8]),
            "涨跌额":     _safe_float(fields[9]),
            "换手率(%)":  _safe_float(fields[10]) if len(fields) > 10 else None,
        })

    result.reverse()  # 由新到旧
    return result


# ═══════════════════════════════════════════════════════════════
# 2. 美股指数当日实时行情
# ═══════════════════════════════════════════════════════════════

async def get_us_index_realtime(index_code: str = "NDX") -> dict:
    """获取美股指数当日实时行情（对应页面顶部的今开/昨收/最高/最低/振幅/成交量等）

    Args:
        index_code: 指数代码，支持 NDX / DJIA / SPX / IXIC

    Returns:
        dict: 包含指数名称、最新价、涨跌幅、涨跌额、今开、昨收、最高、最低、振幅、成交量等
    """
    index_info = US_INDEX_MAP.get(index_code)
    if not index_info:
        raise ValueError(f"不支持的美股指数代码: {index_code}，可选: {list(US_INDEX_MAP.keys())}")

    url = f"{EASTMONEY_PUSH_API_URL}/stock/get"
    params = {
        "fltt": "2",
        "invt": "2",
        "secid": index_info["secid"],
        "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f59,f60,f107,f152,f168,f169,f170,f171",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
    }

    data = await fetch_eastmoney_api(url, params, referer=REFERER)
    d = data.get("data")
    if not d:
        raise Exception(f"未获取到美股指数 {index_code} 的实时数据")

    return {
        "指数名称": index_info["name"],
        "指数代码": index_code,
        "最新价":   d.get("f43"),
        "涨跌额":   d.get("f169"),
        "涨跌幅(%)": d.get("f170"),
        "今开":     d.get("f44"),
        "昨收":     d.get("f60"),
        "最高":     d.get("f45"),
        "最低":     d.get("f46"),
        "振幅(%)":  d.get("f171"),
        "成交量":   d.get("f47"),
        "成交额":   d.get("f48"),
        "换手率(%)": d.get("f168"),
    }


async def get_us_index_realtime_all() -> list[dict]:
    """批量获取所有美股主要指数的实时行情"""
    tasks = [get_us_index_realtime(code) for code in US_INDEX_MAP]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if isinstance(r, dict)]


# ═══════════════════════════════════════════════════════════════
# 3. 美洲主要指数列表
# ═══════════════════════════════════════════════════════════════

async def get_americas_main_indices(page: int = 1, page_size: int = 30) -> list[dict]:
    """获取美洲主要指数列表（对应页面"美洲主要指数"/"美洲其他指数"板块）

    Returns:
        list[dict]: 每条包含 代码、名称、最新价、涨跌幅(%)、涨跌额等
    """
    return await _fetch_global_indices_by_region(AMERICAS_INDEX_CODES)


# ═══════════════════════════════════════════════════════════════
# 4. 中国概念股涨幅榜
# ═══════════════════════════════════════════════════════════════

async def get_china_concept_stock_ranking(page: int = 1, page_size: int = 20) -> list[dict]:
    """获取中国概念股涨幅榜（对应页面"中国概念股涨幅榜"板块）

    Returns:
        list[dict]: 按涨跌幅降序排列，每条包含 代码、名称、最新价、涨跌幅(%)、涨跌额
    """
    return await _fetch_stock_rank_list(FS_CHINA_CONCEPT, page, page_size)


# ═══════════════════════════════════════════════════════════════
# 5. 知名美股涨幅榜
# ═══════════════════════════════════════════════════════════════

async def get_famous_us_stock_ranking(page: int = 1, page_size: int = 20) -> list[dict]:
    """获取知名美股涨幅榜（对应页面"知名美股涨幅榜"板块）

    Returns:
        list[dict]: 按涨跌幅降序排列
    """
    return await _fetch_stock_rank_list(FS_FAMOUS_US, page, page_size)


# ═══════════════════════════════════════════════════════════════
# 6. 互联网中国涨幅榜
# ═══════════════════════════════════════════════════════════════

async def get_internet_china_stock_ranking(page: int = 1, page_size: int = 20) -> list[dict]:
    """获取互联网中国涨幅榜（对应页面"互联网中国涨幅榜"板块）

    Returns:
        list[dict]: 按涨跌幅降序排列
    """
    return await _fetch_stock_rank_list(FS_INTERNET_CHINA, page, page_size)


# ═══════════════════════════════════════════════════════════════
# 7. 其他地区指数（澳洲/欧洲/亚洲）
# ═══════════════════════════════════════════════════════════════

async def get_australia_indices() -> list[dict]:
    """获取澳洲指数列表"""
    return await _fetch_global_indices_by_region(AUSTRALIA_INDEX_CODES)


async def get_europe_indices() -> list[dict]:
    """获取欧洲指数列表"""
    return await _fetch_global_indices_by_region(EUROPE_INDEX_CODES)


async def get_asia_indices() -> list[dict]:
    """获取亚洲指数列表"""
    return await _fetch_global_indices_by_region(ASIA_INDEX_CODES)


# ═══════════════════════════════════════════════════════════════
# 内部工具函数
# ═══════════════════════════════════════════════════════════════

def _safe_float(v: str):
    """安全转换为 float，失败返回 None"""
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


async def _fetch_global_indices_by_region(
    region_codes: set[str],
) -> list[dict]:
    """从全球指数列表中按地区代码集合过滤

    先请求 m:100 获取全部全球指数，再按 region_codes 过滤。

    Args:
        region_codes: 该地区的指数代码集合

    Returns:
        list[dict]: 按涨跌幅降序排列
    """
    url = f"{EASTMONEY_PUSH2_API_URL}/clist/get"
    params = {
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fs": FS_GLOBAL_INDICES,
        "fields": "f2,f3,f4,f5,f6,f12,f13,f14,f15,f16,f17,f18",
        "pn": "1",
        "pz": "100",
        "po": "1",
        "fid": "f3",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "dect": "1",
    }

    data = await fetch_eastmoney_api(url, params, referer=REFERER)
    items = (data.get("data") or {}).get("diff", [])

    result = []
    for item in items:
        code = item.get("f12", "")
        if code not in region_codes:
            continue
        result.append({
            "代码":       code,
            "名称":       item.get("f14", ""),
            "最新价":     item.get("f2"),
            "涨跌幅(%)":  item.get("f3"),
            "涨跌额":     item.get("f4"),
            "成交量":     item.get("f5"),
            "成交额":     item.get("f6"),
            "今开":       item.get("f17"),
            "昨收":       item.get("f18"),
            "最高":       item.get("f15"),
            "最低":       item.get("f16"),
        })

    return result


async def _fetch_stock_rank_list(
    fs_code: str,
    page: int = 1,
    page_size: int = 20,
) -> list[dict]:
    """通用的涨幅榜/指数列表请求方法

    使用 push2.eastmoney.com/api/qt/clist/get 接口，
    按涨跌幅降序排列。

    Args:
        fs_code: 板块代码，如 m:105+t:3（中国概念股）
        page: 页码
        page_size: 每页条数

    Returns:
        list[dict]: 每条包含 代码、名称、最新价、涨跌幅(%)、涨跌额、今开、昨收、最高、最低、成交量、成交额
    """
    url = f"{EASTMONEY_PUSH2_API_URL}/clist/get"
    params = {
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fs": fs_code,
        "fields": "f2,f3,f4,f5,f6,f12,f13,f14,f15,f16,f17,f18",
        "pn": str(page),
        "pz": str(page_size),
        "po": "1",          # 降序
        "fid": "f3",         # 按涨跌幅排序
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "dect": "1",
    }

    data = await fetch_eastmoney_api(url, params, referer=REFERER)
    items = data.get("data", {}).get("diff", [])
    if not items:
        logger.warning("_fetch_stock_rank_list 未获取到数据: fs=%s", fs_code)
        return []

    result = []
    for item in items:
        result.append({
            "代码":       item.get("f12", ""),
            "名称":       item.get("f14", ""),
            "最新价":     item.get("f2"),
            "涨跌幅(%)":  item.get("f3"),
            "涨跌额":     item.get("f4"),
            "成交量":     item.get("f5"),
            "成交额":     item.get("f6"),
            "今开":       item.get("f17"),
            "昨收":       item.get("f18"),
            "最高":       item.get("f15"),
            "最低":       item.get("f16"),
        })

    return result


# ═══════════════════════════════════════════════════════════════
# 测试入口
# ═══════════════════════════════════════════════════════════════

async def main():
    """测试所有接口"""
    print("=" * 60)
    print("1. 纳斯达克日K线（最近5条）")
    print("=" * 60)
    klines = await get_us_index_day_kline("NDX", limit=5)
    print(json.dumps(klines, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("2. 纳斯达克实时行情")
    print("=" * 60)
    realtime = await get_us_index_realtime("NDX")
    print(json.dumps(realtime, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("3. 所有美股主要指数实时行情")
    print("=" * 60)
    all_realtime = await get_us_index_realtime_all()
    print(json.dumps(all_realtime, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("4. 美洲主要指数列表")
    print("=" * 60)
    americas = await get_americas_main_indices()
    print(json.dumps(americas, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("5. 中国概念股涨幅榜（前10）")
    print("=" * 60)
    china_concept = await get_china_concept_stock_ranking(page_size=10)
    print(json.dumps(china_concept, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("6. 知名美股涨幅榜（前10）")
    print("=" * 60)
    famous_us = await get_famous_us_stock_ranking(page_size=10)
    print(json.dumps(famous_us, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("7. 互联网中国涨幅榜（前10）")
    print("=" * 60)
    internet_china = await get_internet_china_stock_ranking(page_size=10)
    print(json.dumps(internet_china, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("8. 欧洲指数")
    print("=" * 60)
    europe = await get_europe_indices()
    print(json.dumps(europe[:5], ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("9. 亚洲指数")
    print("=" * 60)
    asia = await get_asia_indices()
    print(json.dumps(asia[:5], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    from common.logger import setup_logging
    setup_logging()
    asyncio.run(main())
