import asyncio
import logging
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
from dao.stock_kline_dao import (
    get_db_path_for_stock, get_missing_trading_days, get_latest_db_date,
    create_kline_table, parse_kline_data, batch_insert_or_update_kline_data, insert_suspension_day,
    _open_conn
)
from dao.stock_finance_dao import (
    get_finance_table_name, create_finance_table, batch_upsert_finance_data,
)

_CST = ZoneInfo("Asia/Shanghai")

logger = logging.getLogger(__name__)


async def _fetch_klines(stock_info, stock_code, stock_name, missing_days, fetch_limit, counter):
    """拉取K线数据，返回 (klines, elapsed) 或 None"""
    today_cst = datetime.now(_CST).date()
    t0 = asyncio.get_event_loop().time()

    # 仅缺最新一天时，直接从同花顺实时接口获取
    if len(missing_days) == 1 and missing_days[0] == today_cst:
        try:
            pure_code = stock_code.split('.')[0]
            kline_str = await get_today_kline_as_str(pure_code)
            klines = [kline_str] if kline_str else []
            return klines, asyncio.get_event_loop().time() - t0
        except Exception as e:
            logger.error("[%s] 实时K线获取失败: %s", stock_name, e)
            return None, 0

    _RETRYABLE_KEYWORDS = ('Server disconnected', 'Connection closed abruptly',
                           'Expecting value', '空响应', 'JSONP解包后为空',
                           'JSON解析失败', 'ClientResponseError')
    for attempt in range(1, 11):
        try:
            klines = await get_stock_day_kline_as_str_10jqka(stock_info, fetch_limit)
            return klines, asyncio.get_event_loop().time() - t0
        except Exception as e:
            err_msg = str(e)
            is_retryable = any(kw in err_msg for kw in _RETRYABLE_KEYWORDS)
            if is_retryable and attempt < 10:
                wait = min(10 * attempt, 60)
                logger.warning("[%s] K线请求异常(%s)，第%d次重试，等待%d秒",
                               stock_name, err_msg[:200], attempt, wait)
                await asyncio.sleep(wait)
            else:
                logger.error("[%s] 获取K线失败(重试%d次): %s", stock_name, attempt, e)
                return None, 0
    return None, 0


async def _fetch_finance(stock_info, stock_name):
    """拉取财报数据，返回 (records, elapsed) 或 (None, 0)"""
    t0 = asyncio.get_event_loop().time()
    try:
        records = await get_finance_data(stock_info)
        return records, asyncio.get_event_loop().time() - t0
    except Exception as e:
        logger.warning("[%s] 财报数据获取失败: %s", stock_name, e)
        return None, asyncio.get_event_loop().time() - t0


async def process_stock_klines(stock_code, stock_name, db_path, limit, counter):
    """处理单个股票的K线数据和财报数据（并行拉取）"""
    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        counter['failed'] += 1
        return

    t_start = asyncio.get_event_loop().time()
    missing_days = get_missing_trading_days(db_path, stock_code)
    latest_db_date = get_latest_db_date(db_path, stock_code)
    t_dao = asyncio.get_event_loop().time()

    if not missing_days:
        # K线无需更新，但仍需拉取财报数据
        finance_records, finance_elapsed = await _fetch_finance(stock_info, stock_name)
        if finance_records:
            conn = _open_conn(db_path)
            cursor = conn.cursor()
            fin_table = get_finance_table_name(stock_code)
            create_finance_table(cursor, fin_table)
            batch_upsert_finance_data(cursor, fin_table, finance_records)
            conn.commit()
            conn.close()
        fin_info = f" 财报{len(finance_records or [])}条/{finance_elapsed:.2f}s"
        print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 最新数据日期是{latest_db_date}，无需拉取K线{fin_info} dao耗时{t_dao-t_start:.2f}s")
        counter['success'] += 1
        return

    earliest_missing = missing_days[-1]
    today_cst = datetime.now(_CST).date()
    fetch_limit = (today_cst - earliest_missing).days + 5 if latest_db_date else limit

    # 并行拉取K线和财报数据
    (klines, kline_elapsed), (finance_records, finance_elapsed) = await asyncio.gather(
        _fetch_klines(stock_info, stock_code, stock_name, missing_days, fetch_limit, counter),
        _fetch_finance(stock_info, stock_name),
    )

    if klines is None:
        counter['failed'] += 1
        return

    # 校验每条K线核心字段不能为空（date, open, close, high, low, volume）
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
        logger.error("[%s %s] K线数据存在空值，共%d条异常: %s", stock_code, stock_name, len(bad_lines), err_detail)
        counter['failed'] += 1
        return

    # 写入数据库：K线 + 财报
    conn = _open_conn(db_path)
    cursor = conn.cursor()
    t_db_start = asyncio.get_event_loop().time()

    # 写K线
    table_name = f"kline_{stock_code.replace('.', '_')}"
    create_kline_table(cursor, table_name)
    saved_dates = set()
    parsed_list = []
    for kline_str in klines:
        try:
            kline_data = parse_kline_data(kline_str)
            parsed_list.append(kline_data)
            saved_dates.add(date.fromisoformat(kline_data['date']))
        except Exception as e:
            logger.error("解析K线数据失败 %s: %s", stock_code, e)
    batch_insert_or_update_kline_data(cursor, table_name, parsed_list)
    for d in missing_days:
        if d not in saved_dates:
            insert_suspension_day(cursor, table_name, d)

    # 写财报
    fin_count = 0
    if finance_records:
        fin_table = get_finance_table_name(stock_code)
        create_finance_table(cursor, fin_table)
        batch_upsert_finance_data(cursor, fin_table, finance_records)
        fin_count = len(finance_records)

    conn.commit()
    conn.close()
    t_db_end = asyncio.get_event_loop().time()

    counter['success'] += 1
    print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 完成，K线{len(klines)}条/{kline_elapsed:.2f}s 财报{fin_count}条/{finance_elapsed:.2f}s dao{t_dao-t_start:.2f}s 写db{t_db_end-t_db_start:.2f}s")


def load_stocks_from_score_list() -> list[dict]:
    score_list_path = Path(__file__).parent.parent.parent / "data_results/stock_to_score_list/stock_score_list.md"
    stocks = []
    pattern = re.compile(r'^(.+?)\s+\(([^)]+)\)')
    for line in score_list_path.read_text(encoding='utf-8').splitlines():
        m = pattern.match(line.strip())
        if m:
            stocks.append({'name': m.group(1), 'code': m.group(2)})
    return stocks


async def run_stock_klines_job(limit=800, max_concurrent=1):
    """运行股票K线数据采集任务"""
    db_dir = Path(__file__).parent.parent.parent / "data_results/sql_lite"
    db_dir.mkdir(parents=True, exist_ok=True)

    stocks = load_stocks_from_score_list()
    main_codes = {s['code'] for s in stocks}
    stocks += [s for s in MAIN_STOCK if s['code'] not in main_codes]
    print(f"开始采集股票K线数据，共 {len(stocks)} 只股票")
    print(f"数据库目录: {db_dir}")

    semaphore = asyncio.Semaphore(max_concurrent)
    counter = {'total': len(stocks), 'success': 0, 'failed': 0}

    async def process_with_semaphore(stock):
        async with semaphore:
            db_path = get_db_path_for_stock(stock['code'], db_dir)
            print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock['name']}] 开始查询")
            await process_stock_klines(stock['code'], stock['name'], str(db_path), limit, counter)

    await asyncio.gather(*[process_with_semaphore(stock) for stock in stocks], return_exceptions=True)
    print(f"采集完成，总{counter['total']} 成功{counter['success']} 失败{counter['failed']}")


if __name__ == "__main__":
    asyncio.run(run_stock_klines_job())
