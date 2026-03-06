import asyncio
import logging
import threading
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import re

from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from service.jqka10.stock_day_kline_data_10jqka import get_stock_day_kline_as_str_10jqka
from service.jqka10.stock_realtime_10jqka import get_today_kline_as_str
from service.jqka10.stock_finance_data_10jqka import get_financial_data_to_json as get_finance_data
from common.utils.stock_info_utils import get_stock_info_by_code
from common.constants.stocks_data import MAIN_STOCK
from dao import get_connection
from dao.stock_kline_dao import (
    get_missing_trading_days, get_latest_db_date,
    create_kline_table, parse_kline_data, batch_insert_or_update_kline_data, insert_suspension_day,
)
from dao.stock_finance_dao import (
    create_finance_table, batch_upsert_finance_data,
    get_finance_latest_updated_at,
)

_CST = ZoneInfo("Asia/Shanghai")

logger = logging.getLogger(__name__)

# 指数代码集合，用于判断是否为指数类股票
_INDEX_CODES = {s['code'] for s in MAIN_STOCK}


# ─────────────────── K线采集流水线 ───────────────────

async def _process_single_kline(stock_code, stock_name, limit, counter):
    """处理单只股票的K线数据拉取和存储"""
    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        counter['failed'] += 1
        return

    t_start = asyncio.get_event_loop().time()
    missing_days = get_missing_trading_days(stock_code)
    latest_db_date = get_latest_db_date(stock_code)
    t_dao = asyncio.get_event_loop().time()

    if not missing_days:
        logger.info("[K线 总%d 成功%d 失败%d 当前:%s] 最新数据日期是%s，无需拉取 dao耗时%.2fs",
                    counter['total'], counter['success'], counter['failed'], stock_name, latest_db_date, t_dao-t_start)
        counter['success'] += 1
        return

    earliest_missing = missing_days[-1]
    today_cst = datetime.now(_CST).date()
    fetch_limit = (today_cst - earliest_missing).days + 5 if latest_db_date else limit

    klines = None
    t0 = asyncio.get_event_loop().time()

    # 仅缺最新一天时，直接从同花顺实时接口获取
    if len(missing_days) == 1 and missing_days[0] == today_cst:
        try:
            pure_code = stock_code.split('.')[0]
            kline_str = await get_today_kline_as_str(pure_code)
            klines = [kline_str] if kline_str else []
            elapsed = asyncio.get_event_loop().time() - t0
        except Exception as e:
            logger.error("[K线 %s] 实时K线获取失败: %s", stock_name, e)
            counter['failed'] += 1
            return
    else:
        _RETRYABLE_KEYWORDS = ('Server disconnected', 'Connection closed abruptly',
                               'Expecting value', '空响应', 'JSONP解包后为空',
                               'JSON解析失败', 'ClientResponseError')
        is_index = stock_code in _INDEX_CODES
        max_attempts = 1 if is_index else 10
        for attempt in range(1, max_attempts + 1):
            try:
                klines = await get_stock_day_kline_as_str_10jqka(stock_info, fetch_limit)
                elapsed = asyncio.get_event_loop().time() - t0
                break
            except Exception as e:
                if is_index:
                    logger.warning("[K线 %s] 指数类股票拉取失败，跳过: %s", stock_name, str(e)[:200])
                    counter['failed'] += 1
                    return
                err_msg = str(e)
                is_retryable = any(kw in err_msg for kw in _RETRYABLE_KEYWORDS)
                if is_retryable and attempt < max_attempts:
                    wait = min(10 * attempt, 60)
                    logger.warning("[K线 %s] 请求异常(%s)，第%d次重试，等待%d秒",
                                   stock_name, err_msg[:200], attempt, wait)
                    await asyncio.sleep(wait)
                else:
                    logger.error("[K线 %s] 获取失败(重试%d次): %s", stock_name, attempt, e)
                    counter['failed'] += 1
                    return

    if klines is None:
        counter['failed'] += 1
        return

    # 校验K线核心字段
    _REQUIRED_FIELD_NAMES = ("date", "open_price", "close_price", "high_price", "low_price", "trading_volume")
    _REQUIRED_FIELD_INDICES = (0, 1, 2, 3, 4, 5)
    bad_lines = []
    for idx, kline_str in enumerate(klines):
        fields = kline_str.split(",")
        for fi, fname in zip(_REQUIRED_FIELD_INDICES, _REQUIRED_FIELD_NAMES):
            if fi >= len(fields) or not fields[fi] or fields[fi].strip() == "" or fields[fi] == "None":
                bad_lines.append(f"第{idx+1}条 字段[{fname}]为空: {kline_str[:120]}")
    if bad_lines:
        err_detail = "; ".join(bad_lines[:5])
        logger.error("[K线 %s %s] 数据存在空值，共%d条异常: %s", stock_code, stock_name, len(bad_lines), err_detail)
        counter['failed'] += 1
        return

    # 写入数据库
    conn = get_connection()
    cursor = conn.cursor()
    t_db_start = asyncio.get_event_loop().time()
    create_kline_table(cursor)
    saved_dates = set()
    parsed_list = []
    for kline_str in klines:
        try:
            kline_data = parse_kline_data(kline_str)
            parsed_list.append(kline_data)
            saved_dates.add(date.fromisoformat(kline_data['date']))
        except Exception as e:
            logger.error("解析K线数据失败 %s: %s", stock_code, e)
    batch_insert_or_update_kline_data(cursor, stock_code, parsed_list)
    for d in missing_days:
        if d not in saved_dates:
            insert_suspension_day(cursor, stock_code, d)
    conn.commit()
    cursor.close()
    conn.close()
    t_db_end = asyncio.get_event_loop().time()

    counter['success'] += 1
    logger.info("[K线 总%d 成功%d 失败%d 当前:%s] 完成，%d条 网络%.2fs dao%.2fs 写db%.2fs",
                counter['total'], counter['success'], counter['failed'], stock_name,
                len(klines), elapsed, t_dao-t_start, t_db_end-t_db_start)


# ─────────────────── 财报采集流水线 ───────────────────

async def _process_single_finance(stock_code, stock_name, counter):
    """处理单只股票的财报数据拉取和存储"""
    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        counter['failed'] += 1
        return

    # 今天已拉取过则跳过
    today_str = datetime.now(_CST).strftime("%Y-%m-%d")
    latest_updated = get_finance_latest_updated_at(stock_code)
    if latest_updated and latest_updated[:10] >= today_str:
        counter['success'] += 1
        logger.info("[财报 总%d 成功%d 失败%d 当前:%s] 今日已更新(%s)，跳过",
                    counter['total'], counter['success'], counter['failed'], stock_name, latest_updated)
        return

    t0 = asyncio.get_event_loop().time()
    try:
        records = await get_finance_data(stock_info)
    except Exception as e:
        logger.warning("[财报 %s] 获取失败: %s", stock_name, e)
        counter['failed'] += 1
        return
    elapsed = asyncio.get_event_loop().time() - t0

    if not records:
        logger.warning("[财报 %s] 返回空数据", stock_name)
        counter['failed'] += 1
        return

    # 写入数据库
    t_db = asyncio.get_event_loop().time()
    conn = get_connection()
    cursor = conn.cursor()
    create_finance_table(cursor)
    batch_upsert_finance_data(cursor, stock_code, records)
    conn.commit()
    cursor.close()
    conn.close()
    t_db_end = asyncio.get_event_loop().time()

    counter['success'] += 1
    logger.info("[财报 总%d 成功%d 失败%d 当前:%s] 完成，%d条 网络%.2fs 写db%.2fs",
                counter['total'], counter['success'], counter['failed'], stock_name,
                len(records), elapsed, t_db_end-t_db)


# ─────────────────── 公共工具 ───────────────────

def load_stocks_from_score_list() -> list[dict]:
    score_list_path = Path(__file__).parent.parent.parent / "data_results/stock_to_score_list/stock_score_list.md"
    stocks = []
    pattern = re.compile(r'^(.+?)\s+\(([^)]+)\)')
    for line in score_list_path.read_text(encoding='utf-8').splitlines():
        m = pattern.match(line.strip())
        if m:
            stocks.append({'name': m.group(1), 'code': m.group(2)})
    return stocks


def _build_stock_list() -> list[dict]:
    """构建完整的股票列表（score_list + MAIN_STOCK 去重）"""
    stocks = load_stocks_from_score_list()
    main_codes = {s['code'] for s in stocks}
    stocks += [s for s in MAIN_STOCK if s['code'] not in main_codes]
    return stocks


# ─────────────────── 独立运行入口 ───────────────────

async def run_kline_job(limit=800, max_concurrent=1, counter=None):
    """独立运行K线采集任务"""
    stocks = _build_stock_list()
    logger.info("[K线] 开始采集，共 %d 只股票", len(stocks))

    semaphore = asyncio.Semaphore(max_concurrent)
    if counter is None:
        counter = {'total': len(stocks), 'success': 0, 'failed': 0}
    else:
        counter['total'] = len(stocks)

    async def task(stock):
        async with semaphore:
            await _process_single_kline(stock['code'], stock['name'], limit, counter)

    await asyncio.gather(*[task(s) for s in stocks], return_exceptions=True)
    logger.info("[K线] 采集完成，总%d 成功%d 失败%d", counter['total'], counter['success'], counter['failed'])
    return counter


async def run_finance_job(max_concurrent=3, counter=None):
    """独立运行财报采集任务"""
    stocks = _build_stock_list()
    logger.info("[财报] 开始采集，共 %d 只股票", len(stocks))

    semaphore = asyncio.Semaphore(max_concurrent)
    if counter is None:
        counter = {'total': len(stocks), 'success': 0, 'failed': 0}
    else:
        counter['total'] = len(stocks)

    async def task(stock):
        async with semaphore:
            await _process_single_finance(stock['code'], stock['name'], counter)

    await asyncio.gather(*[task(s) for s in stocks], return_exceptions=True)
    logger.info("[财报] 采集完成，总%d 成功%d 失败%d", counter['total'], counter['success'], counter['failed'])
    return counter


def run_stock_klines_job(limit=800, max_concurrent=1):
    """
    在两个独立线程中分别运行K线和财报采集流水线。

    每条流水线拥有独立的线程、事件循环、计数器、信号量和错误处理，
    任何一条流水线的异常或阻塞都不会影响另一条。
    K线默认串行（max_concurrent=1），财报默认3并发。
    """
    logger.info("=" * 60)
    logger.info("  启动数据采集（K线 + 财报 独立线程）")
    logger.info("=" * 60)

    results = {}

    def _run_kline():
        try:
            results['kline'] = asyncio.run(run_kline_job(limit=limit, max_concurrent=max_concurrent))
        except Exception as e:
            logger.error("[K线线程] 异常退出: %s", e)
            results['kline'] = {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}

    def _run_finance():
        try:
            results['finance'] = asyncio.run(run_finance_job(max_concurrent=3))
        except Exception as e:
            logger.error("[财报线程] 异常退出: %s", e)
            results['finance'] = {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}

    t_kline = threading.Thread(target=_run_kline, name="Thread-Kline", daemon=True)
    t_finance = threading.Thread(target=_run_finance, name="Thread-Finance", daemon=True)

    t_kline.start()
    t_finance.start()

    t_kline.join()
    t_finance.join()

    kline_counter = results.get('kline', {})
    finance_counter = results.get('finance', {})

    logger.info("\n" + "=" * 60)
    logger.info("  全部完成")
    logger.info("  K线: 总%d 成功%d 失败%d", kline_counter.get('total',0), kline_counter.get('success',0), kline_counter.get('failed',0))
    logger.info("  财报: 总%d 成功%d 失败%d", finance_counter.get('total',0), finance_counter.get('success',0), finance_counter.get('failed',0))
    logger.info("=" * 60)
    return kline_counter, finance_counter


if __name__ == "__main__":
    run_stock_klines_job()
