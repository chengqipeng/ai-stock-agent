"""
新股同步服务：将概念板块抓取到的成分股同步到系统中。

功能:
  1. 检查 stocks_data.py STOCKS 列表，缺失则追加
  2. 检查 stock_score_list.md，缺失则追加（打分：0）
  3. 拉取日线K线数据并写入数据库

Usage:
    from service.jqka10.stock_sync_service import sync_new_stocks
    added = await sync_new_stocks(stocks_list)
"""
import asyncio
import logging
import os
import re
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_STOCKS_DATA_FILE = _PROJECT_ROOT / "common" / "constants" / "stocks_data.py"
_SCORE_LIST_FILE = _PROJECT_ROOT / "data_results" / "stock_to_score_list" / "stock_score_list.md"

# 指数映射规则
_INDEX_MAPPING = {
    "300": (["399001.SZ", "399006.SZ"], ["深证成指", "创业板指"]),
    "301": (["399001.SZ", "399006.SZ"], ["深证成指", "创业板指"]),
    "000": (["399001.SZ"], ["深证成指"]),
    "001": (["399001.SZ"], ["深证成指"]),
    "002": (["399001.SZ"], ["深证成指"]),
    "003": (["399001.SZ"], ["深证成指"]),
    "600": (["000001.SH"], ["上证指数"]),
    "601": (["000001.SH"], ["上证指数"]),
    "603": (["000001.SH"], ["上证指数"]),
    "605": (["000001.SH"], ["上证指数"]),
    "688": (["000001.SH", "000680.SH"], ["上证指数", "科创综指"]),
    "689": (["000001.SH", "000680.SH"], ["上证指数", "科创综指"]),
    "920": (["899050.SZ"], ["北证50"]),
    "430": (["899050.SZ"], ["北证50"]),
    "830": (["899050.SZ"], ["北证50"]),
    "831": (["899050.SZ"], ["北证50"]),
    "832": (["899050.SZ"], ["北证50"]),
    "833": (["899050.SZ"], ["北证50"]),
    "834": (["899050.SZ"], ["北证50"]),
    "835": (["899050.SZ"], ["北证50"]),
    "836": (["899050.SZ"], ["北证50"]),
    "837": (["899050.SZ"], ["北证50"]),
    "838": (["899050.SZ"], ["北证50"]),
    "839": (["899050.SZ"], ["北证50"]),
    "870": (["899050.SZ"], ["北证50"]),
    "871": (["899050.SZ"], ["北证50"]),
    "872": (["899050.SZ"], ["北证50"]),
    "873": (["899050.SZ"], ["北证50"]),
}


def _normalize_stock_code(raw_code: str) -> str | None:
    """
    将6位纯数字代码转为标准化代码 (如 300143 -> 300143.SZ)。
    """
    code = raw_code.strip()
    if len(code) != 6 or not code.isdigit():
        return None
    prefix3 = code[:3]
    if prefix3 in ("300", "301", "000", "001", "002", "003"):
        return f"{code}.SZ"
    if prefix3 in ("600", "601", "603", "605", "688", "689"):
        return f"{code}.SH"
    if prefix3 in ("920", "430", "830", "831", "832", "833", "834", "835",
                    "836", "837", "838", "839", "870", "871", "872", "873"):
        return f"{code}.BJ"
    # 未知前缀，尝试按首位判断
    if code[0] in ("0", "3"):
        return f"{code}.SZ"
    if code[0] == "6":
        return f"{code}.SH"
    if code[0] in ("4", "8", "9"):
        return f"{code}.BJ"
    return None


def _get_index_mapping(code_normalize: str) -> tuple[list[str], list[str]]:
    """根据标准化代码返回 (indices_stock_codes, indices_stock_names)。"""
    prefix3 = code_normalize[:3]
    if prefix3 in _INDEX_MAPPING:
        return _INDEX_MAPPING[prefix3]
    # fallback
    if code_normalize.endswith(".SZ"):
        return (["399001.SZ"], ["深证成指"])
    if code_normalize.endswith(".SH"):
        return (["000001.SH"], ["上证指数"])
    if code_normalize.endswith(".BJ"):
        return (["899050.SZ"], ["北证50"])
    return ([], [])


def _load_existing_stock_codes() -> set[str]:
    """从 stocks_data.py 的 STOCK_DICT 加载已有代码集合。"""
    from common.constants.stocks_data import STOCK_DICT
    return set(STOCK_DICT.keys())


def _load_score_list_codes() -> set[str]:
    """从 stock_score_list.md 加载已有代码集合。"""
    codes = set()
    if not _SCORE_LIST_FILE.exists():
        return codes
    pattern = re.compile(r'\(([^)]+)\)')
    for line in _SCORE_LIST_FILE.read_text(encoding='utf-8').splitlines():
        m = pattern.search(line)
        if m:
            codes.add(m.group(1))
    return codes


def _append_to_stocks_data(entries: list[dict]) -> int:
    """
    向 stocks_data.py 的 STOCKS 列表末尾追加新股票条目。
    entries: [{"code": "300143.SZ", "name": "盈康生命",
               "indices_stock_codes": [...], "indices_stock_names": [...]}]
    返回实际追加数量。
    """
    if not entries:
        return 0

    content = _STOCKS_DATA_FILE.read_text(encoding='utf-8')

    # 找到 STOCKS 列表的结束位置: 最后一个 "\n]" 之前（在 ALL_STOCKS 之前）
    # 定位 "ALL_STOCKS = STOCKS + MAIN_STOCK" 行之前的 "]"
    marker = "\nALL_STOCKS = STOCKS + MAIN_STOCK"
    marker_pos = content.find(marker)
    if marker_pos < 0:
        logger.error("stocks_data.py 中未找到 ALL_STOCKS 标记，无法追加")
        return 0

    # 从 marker_pos 往前找最近的 "]"
    bracket_pos = content.rfind("]", 0, marker_pos)
    if bracket_pos < 0:
        logger.error("stocks_data.py 中未找到 STOCKS 列表结束括号")
        return 0

    # 构建新条目文本
    new_entries_text = ""
    for e in entries:
        codes_str = ",\n      ".join(f'"{c}"' for c in e["indices_stock_codes"])
        names_str = ",\n      ".join(f'"{n}"' for n in e["indices_stock_names"])
        new_entries_text += f""",
  {{
    "code": "{e['code']}",
    "name": "{e['name']}",
    "indices_stock_codes": [
      {codes_str}
    ],
    "indices_stock_names": [
      {names_str}
    ]
  }}"""

    # 在 "]" 之前插入
    new_content = content[:bracket_pos] + new_entries_text + "\n" + content[bracket_pos:]
    _STOCKS_DATA_FILE.write_text(new_content, encoding='utf-8')
    return len(entries)


def _append_to_score_list(entries: list[dict]) -> int:
    """
    向 stock_score_list.md 末尾追加新股票。
    entries: [{"code": "300143.SZ", "name": "盈康生命"}]
    """
    if not entries:
        return 0
    lines = []
    for e in entries:
        lines.append(f"{e['name']} ({e['code']}) - 打分：0")
    with open(_SCORE_LIST_FILE, 'a', encoding='utf-8') as f:
        f.write("\n".join(lines) + "\n")
    return len(entries)


async def _fetch_and_save_kline(code_normalize: str, stock_name: str,
                                 limit: int = 400) -> bool:
    """拉取单只股票的日线K线并写入数据库，返回是否成功。"""
    from common.utils.stock_info_utils import StockInfo
    from service.jqka10.stock_day_kline_data_10jqka import (
        get_stock_day_kline_as_str_10jqka,
    )
    from dao.stock_kline_dao import (
        parse_kline_data, create_kline_table,
        batch_insert_or_update_kline_data, get_latest_db_date,
    )
    from common.http.http_utils import get_connection

    stock_code = code_normalize.split('.')[0]
    market_suffix = code_normalize.split('.')[1]
    market_prefix = "0" if market_suffix == "SZ" else "1"
    secid = f"{market_prefix}.{stock_code}"

    indices_codes, indices_names = _get_index_mapping(code_normalize)
    stock_info = StockInfo(
        secid=secid,
        stock_code=stock_code,
        stock_code_normalize=code_normalize,
        stock_name=stock_name,
        indices_stock_code=indices_codes[-1] if indices_codes else None,
        indices_stock_name=indices_names[-1] if indices_names else None,
    )

    # 如果数据库已有数据，跳过
    latest = get_latest_db_date(code_normalize)
    if latest:
        logger.debug("[新股K线] %s %s 已有数据(最新%s)，跳过", code_normalize, stock_name, latest)
        return True

    try:
        klines_str = await get_stock_day_kline_as_str_10jqka(stock_info, limit)
    except Exception as e:
        logger.error("[新股K线] %s %s 拉取失败: %s", code_normalize, stock_name, str(e)[:200])
        return False

    if not klines_str:
        logger.warning("[新股K线] %s %s 返回空数据", code_normalize, stock_name)
        return False

    # 写入数据库
    conn = get_connection()
    cursor = conn.cursor()
    try:
        create_kline_table(cursor)
        parsed_list = []
        for ks in klines_str:
            try:
                parsed_list.append(parse_kline_data(ks))
            except Exception as e:
                logger.error("[新股K线] 解析失败 %s: %s", code_normalize, e)
        batch_insert_or_update_kline_data(cursor, code_normalize, parsed_list)
        conn.commit()
        logger.info("[新股K线] %s %s 写入 %d 条K线", code_normalize, stock_name, len(parsed_list))
        return True
    except Exception as e:
        logger.error("[新股K线] %s %s 写入DB失败: %s", code_normalize, stock_name, e)
        return False
    finally:
        cursor.close()
        conn.close()


async def sync_new_stocks(stocks: list[dict],
                           fetch_kline: bool = True,
                           kline_limit: int = 400,
                           kline_delay: float = 2.0) -> dict:
    """
    同步新股到系统中。

    Args:
        stocks: [{"stock_code": "300143", "stock_name": "盈康生命"}, ...]
        fetch_kline: 是否拉取K线数据
        kline_limit: K线拉取条数
        kline_delay: K线拉取间隔（秒），避免被封

    Returns:
        {"added_to_stocks_data": n, "added_to_score_list": n,
         "kline_success": n, "kline_failed": n, "total_new": n}
    """
    if not stocks:
        return {"added_to_stocks_data": 0, "added_to_score_list": 0,
                "kline_success": 0, "kline_failed": 0, "total_new": 0}

    # 加载已有数据
    existing_codes = _load_existing_stock_codes()
    score_codes = _load_score_list_codes()

    # 找出新股
    new_for_stocks_data = []
    new_for_score_list = []
    all_new_stocks = []  # 需要拉K线的

    for s in stocks:
        raw_code = s.get("stock_code", "")
        name = s.get("stock_name", "")
        code_norm = _normalize_stock_code(raw_code)
        if not code_norm:
            continue

        is_new = False
        if code_norm not in existing_codes:
            indices_codes, indices_names = _get_index_mapping(code_norm)
            new_for_stocks_data.append({
                "code": code_norm,
                "name": name,
                "indices_stock_codes": indices_codes,
                "indices_stock_names": indices_names,
            })
            existing_codes.add(code_norm)  # 防止同批次重复
            is_new = True

        if code_norm not in score_codes:
            new_for_score_list.append({"code": code_norm, "name": name})
            score_codes.add(code_norm)
            is_new = True

        if is_new:
            all_new_stocks.append({"code": code_norm, "name": name})

    # 写入 stocks_data.py
    added_sd = _append_to_stocks_data(new_for_stocks_data)
    if added_sd > 0:
        logger.info("[新股同步] 向 stocks_data.py 追加 %d 只新股", added_sd)

    # 写入 score_list.md
    added_sl = _append_to_score_list(new_for_score_list)
    if added_sl > 0:
        logger.info("[新股同步] 向 stock_score_list.md 追加 %d 只新股", added_sl)

    # 拉取K线
    kline_ok = kline_fail = 0
    if fetch_kline and all_new_stocks:
        logger.info("[新股同步] 开始拉取 %d 只新股的K线数据...", len(all_new_stocks))
        for i, ns in enumerate(all_new_stocks):
            ok = await _fetch_and_save_kline(ns["code"], ns["name"], kline_limit)
            if ok:
                kline_ok += 1
            else:
                kline_fail += 1
            if i < len(all_new_stocks) - 1:
                await asyncio.sleep(kline_delay)
        logger.info("[新股同步] K线拉取完成: 成功%d, 失败%d", kline_ok, kline_fail)

    total_new = len(all_new_stocks)
    if total_new > 0:
        print(f"  [新股同步] 发现 {total_new} 只新股: "
              f"stocks_data+{added_sd}, score_list+{added_sl}, "
              f"K线成功{kline_ok}/失败{kline_fail}")

    return {
        "added_to_stocks_data": added_sd,
        "added_to_score_list": added_sl,
        "kline_success": kline_ok,
        "kline_failed": kline_fail,
        "total_new": total_new,
    }


def sync_new_stocks_sync(stocks: list[dict], **kwargs) -> dict:
    """sync_new_stocks 的同步包装。"""
    return asyncio.run(sync_new_stocks(stocks, **kwargs))
