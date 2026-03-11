"""
美股半导体/电子个股日K线数据接口 — 数据来源：东方财富

抓取与A股半导体板块深度关联的美股龙头公司日K线数据，
用于计算美股→A股板块的隔夜信号和相关性分析。

覆盖细分领域：
- 芯片设计: NVDA, AMD, QCOM, AVGO, MRVL, TXN, INTC
- 半导体设备: AMAT, LRCX, KLAC, ASML
- 半导体材料: ENTG
- 晶圆代工/封测: TSM
- 消费电子/EMS: AAPL
- PCB/连接器: APH
- 光通信/光模块: COHR
- 存储: MU, WDC
- 半导体ETF(跟踪费城半导体指数): SOXX

Usage:
    python -m service.eastmoney.indices.us_stock_kline
"""

import asyncio
import json
import logging
import time
from datetime import datetime

from common.http.http_utils import (
    EASTMONEY_PUSH2HIS_API_URL,
    EASTMONEY_PUSH2_API_URL,
    fetch_eastmoney_api,
)

logger = logging.getLogger(__name__)

REFERER = "https://quote.eastmoney.com/us/NVDA.html"

# ═══════════════════════════════════════════════════════════════
# 美股半导体/电子龙头 — secid 映射
# market: 105=NASDAQ, 106=NYSE, 100=全球指数
# ═══════════════════════════════════════════════════════════════

US_SEMI_STOCK_MAP = {
    # ── 芯片设计 ──
    "NVDA":  {"secid": "105.NVDA",  "name": "英伟达",       "sector": "芯片设计"},
    "AMD":   {"secid": "105.AMD",   "name": "超威半导体",   "sector": "芯片设计"},
    "QCOM":  {"secid": "105.QCOM",  "name": "高通",         "sector": "芯片设计"},
    "AVGO":  {"secid": "105.AVGO",  "name": "博通",         "sector": "芯片设计"},
    "MRVL":  {"secid": "105.MRVL",  "name": "迈威尔科技",   "sector": "芯片设计"},
    "TXN":   {"secid": "105.TXN",   "name": "德州仪器",     "sector": "芯片设计"},
    "INTC":  {"secid": "105.INTC",  "name": "英特尔",       "sector": "芯片设计"},
    # ── 半导体设备 ──
    "AMAT":  {"secid": "105.AMAT",  "name": "应用材料",     "sector": "半导体设备"},
    "LRCX":  {"secid": "105.LRCX",  "name": "拉姆研究",     "sector": "半导体设备"},
    "KLAC":  {"secid": "105.KLAC",  "name": "科磊",         "sector": "半导体设备"},
    "ASML":  {"secid": "105.ASML",  "name": "阿斯麦",       "sector": "半导体设备"},
    # ── 半导体材料 ──
    "ENTG":  {"secid": "105.ENTG",  "name": "英特格",       "sector": "半导体材料"},
    # ── 晶圆代工 ──
    "TSM":   {"secid": "106.TSM",   "name": "台积电",       "sector": "晶圆代工"},
    # ── 消费电子 ──
    "AAPL":  {"secid": "105.AAPL",  "name": "苹果",         "sector": "消费电子"},
    # ── PCB/连接器 ──
    "APH":   {"secid": "106.APH",   "name": "安费诺",       "sector": "连接器"},
    # ── 光通信 ──
    "COHR":  {"secid": "106.COHR",  "name": "Coherent Corp", "sector": "光通信"},
    # ── 存储 ──
    "MU":    {"secid": "105.MU",    "name": "美光科技",     "sector": "存储"},
    "WDC":   {"secid": "105.WDC",   "name": "西部数据",     "sector": "存储"},
}

# 半导体指数/ETF（SOXX 跟踪费城半导体指数，可作为替代）
US_SEMI_INDEX_MAP = {
    "SOXX": {"secid": "105.SOXX", "name": "半导体ETF-iShares(SOXX)"},
}


def _safe_float(v):
    """安全转换浮点数"""
    if v is None or v == "" or v == "-":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _parse_klines(klines_raw: list[str]) -> list[dict]:
    """解析东方财富K线原始数据为标准字典列表（由新到旧）"""
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
# 1. 美股个股日K线
# ═══════════════════════════════════════════════════════════════

async def get_us_stock_day_kline(
    stock_code: str = "NVDA",
    limit: int = 120,
) -> list[dict]:
    """获取美股个股日K线数据

    Args:
        stock_code: 股票代码，如 NVDA / AMD / AAPL 等
        limit: 返回K线条数，默认120（约半年）

    Returns:
        list[dict]: 由新到旧排列的K线数据列表
    """
    stock_info = US_SEMI_STOCK_MAP.get(stock_code)
    if not stock_info:
        raise ValueError(
            f"不支持的美股代码: {stock_code}，"
            f"可选: {list(US_SEMI_STOCK_MAP.keys())}"
        )

    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/kline/get"
    params = {
        "secid": stock_info["secid"],
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
    return _parse_klines(klines_raw)


async def get_us_stock_day_kline_batch(
    stock_codes: list[str] = None,
    limit: int = 120,
    delay: float = 0.3,
) -> dict[str, list[dict]]:
    """批量获取多只美股个股日K线

    Args:
        stock_codes: 股票代码列表，默认全部
        limit: 每只股票返回的K线条数
        delay: 每次请求间隔（秒），避免被限流

    Returns:
        dict: {stock_code: [kline_list]}
    """
    if stock_codes is None:
        stock_codes = list(US_SEMI_STOCK_MAP.keys())

    results = {}
    for code in stock_codes:
        try:
            klines = await get_us_stock_day_kline(code, limit=limit)
            results[code] = klines
            logger.info(
                "获取 %s(%s) K线 %d 条",
                code, US_SEMI_STOCK_MAP[code]["name"], len(klines),
            )
        except Exception as e:
            logger.error("获取 %s K线失败: %s", code, e)
            results[code] = []
        if delay > 0:
            await asyncio.sleep(delay)

    return results


# ═══════════════════════════════════════════════════════════════
# 2. 费城半导体指数日K线
# ═══════════════════════════════════════════════════════════════

async def get_sox_index_day_kline(limit: int = 120) -> list[dict]:
    """获取半导体ETF(SOXX)日K线数据，跟踪费城半导体指数

    Returns:
        list[dict]: 由新到旧排列的K线数据列表
    """
    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/kline/get"
    params = {
        "secid": US_SEMI_INDEX_MAP["SOXX"]["secid"],
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "end": datetime.now().strftime("%Y%m%d"),
        "lmt": limit,
        "cb": "quote_jp1",
        "_": int(time.time() * 1000),
    }

    data = await fetch_eastmoney_api(url, params, referer=REFERER)
    klines_raw = data.get("data", {}).get("klines", [])
    return _parse_klines(klines_raw)


# ═══════════════════════════════════════════════════════════════
# 3. 美股个股实时行情
# ═══════════════════════════════════════════════════════════════

async def get_us_stock_realtime(stock_code: str = "NVDA") -> dict:
    """获取美股个股当日实时行情

    Args:
        stock_code: 股票代码

    Returns:
        dict: 包含名称、最新价、涨跌幅、成交量等
    """
    stock_info = US_SEMI_STOCK_MAP.get(stock_code)
    if not stock_info:
        raise ValueError(f"不支持的美股代码: {stock_code}")

    url = f"{EASTMONEY_PUSH2_API_URL}/stock/get"
    params = {
        "fltt": "2",
        "invt": "2",
        "secid": stock_info["secid"],
        "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f59,f60,f107,f152,f168,f169,f170,f171",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
    }

    data = await fetch_eastmoney_api(url, params, referer=REFERER)
    d = data.get("data")
    if not d:
        return {}

    return {
        "代码":       stock_code,
        "名称":       stock_info["name"],
        "细分领域":   stock_info["sector"],
        "最新价":     d.get("f43"),
        "涨跌额":     d.get("f169"),
        "涨跌幅(%)":  d.get("f170"),
        "今开":       d.get("f44"),
        "昨收":       d.get("f60"),
        "最高":       d.get("f45"),
        "最低":       d.get("f46"),
        "振幅(%)":    d.get("f171"),
        "成交量":     d.get("f47"),
        "成交额":     d.get("f48"),
        "换手率(%)":  d.get("f168"),
    }


async def get_us_stock_realtime_batch(
    stock_codes: list[str] = None,
) -> list[dict]:
    """批量获取美股个股实时行情

    Args:
        stock_codes: 股票代码列表，默认全部

    Returns:
        list[dict]: 实时行情列表
    """
    if stock_codes is None:
        stock_codes = list(US_SEMI_STOCK_MAP.keys())

    tasks = [get_us_stock_realtime(code) for code in stock_codes]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    valid = []
    for code, r in zip(stock_codes, results):
        if isinstance(r, Exception):
            logger.error("获取 %s 实时行情失败: %s", code, r)
        elif r:
            valid.append(r)
    return valid


# ═══════════════════════════════════════════════════════════════
# 测试入口
# ═══════════════════════════════════════════════════════════════

async def main():
    """测试所有接口"""
    print("=" * 60)
    print("1. 英伟达(NVDA) 日K线（最近5条）")
    print("=" * 60)
    klines = await get_us_stock_day_kline("NVDA", limit=5)
    print(json.dumps(klines, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("2. 半导体ETF(SOXX) 日K线（最近5条）")
    print("=" * 60)
    sox = await get_sox_index_day_kline(limit=5)
    print(json.dumps(sox, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("3. 英伟达(NVDA) 实时行情")
    print("=" * 60)
    realtime = await get_us_stock_realtime("NVDA")
    print(json.dumps(realtime, ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("4. 批量获取所有美股半导体龙头K线（最近3条）")
    print("=" * 60)
    batch = await get_us_stock_day_kline_batch(limit=3, delay=0.2)
    for code, data in batch.items():
        info = US_SEMI_STOCK_MAP[code]
        latest = data[0] if data else {}
        print(f"  {code:6s} {info['name']:10s} | "
              f"最新: {latest.get('收盘价', '-'):>10} | "
              f"涨跌: {latest.get('涨跌幅(%)', '-'):>6}%")

    print("\n" + "=" * 60)
    print("5. 批量实时行情")
    print("=" * 60)
    all_rt = await get_us_stock_realtime_batch()
    for item in all_rt:
        print(f"  {item['代码']:6s} {item['名称']:10s} | "
              f"最新: {item.get('最新价', '-'):>10} | "
              f"涨跌: {item.get('涨跌幅(%)', '-'):>6}%")


if __name__ == "__main__":
    from common.logger import setup_logging
    setup_logging()
    asyncio.run(main())
