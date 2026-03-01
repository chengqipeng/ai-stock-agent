import asyncio
import logging
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import re

from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from service.jqka10.stock_day_kline_data_10jqka import get_stock_day_kline_as_str_10jqka
from service.jqka10.stock_realtime_10jqka import get_today_kline_as_str
from common.utils.stock_info_utils import get_stock_info_by_code
from common.constants.stocks_data import MAIN_STOCK
from dao.stock_kline_dao import (
    get_db_path_for_stock, get_missing_trading_days, get_latest_db_date,
    create_kline_table, parse_kline_data, batch_insert_or_update_kline_data, insert_suspension_day,
    _open_conn
)

_CST = ZoneInfo("Asia/Shanghai")

logger = logging.getLogger(__name__)


async def process_stock_klines(stock_code, stock_name, db_path, limit, counter):
    """处理单个股票的K线数据"""
    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        counter['failed'] += 1
        return

    t_start = asyncio.get_event_loop().time()
    missing_days = get_missing_trading_days(db_path, stock_code)
    latest_db_date = get_latest_db_date(db_path, stock_code)
    t_dao = asyncio.get_event_loop().time()
    if not missing_days:
        print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 最新数据日期是{latest_db_date}，无需拉取数据 dao耗时{t_dao-t_start:.2f}s")
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
            logger.error("[总%d 成功%d 失败%d 当前:%s] 实时K线获取失败: %s", counter['total'], counter['success'], counter['failed'], stock_name, e)
            counter['failed'] += 1
            return
    else:
        _RETRYABLE_KEYWORDS = ('Server disconnected', 'Connection closed abruptly',
                               'Expecting value', '空响应', 'JSONP解包后为空',
                               'JSON解析失败', 'ClientResponseError')
        for attempt in range(1, 11):
            try:
                klines = await get_stock_day_kline_as_str_10jqka(stock_info, fetch_limit)
                # klines = await get_stock_day_range_kline(stock_info, fetch_limit)
                elapsed = asyncio.get_event_loop().time() - t0
                break
            except Exception as e:
                err_msg = str(e)
                is_retryable = any(kw in err_msg for kw in _RETRYABLE_KEYWORDS)
                if is_retryable and attempt < 10:
                    wait = min(10 * attempt, 60)
                    logger.warning("[总%d 成功%d 失败%d 当前:%s] 请求异常(%s: %s)，第%d次重试，等待%d秒",
                                   counter['total'], counter['success'], counter['failed'],
                                   stock_name, e.__class__.__name__, err_msg[:200], attempt, wait)
                    await asyncio.sleep(wait)
                else:
                    logger.error("[总%d 成功%d 失败%d 当前:%s] 获取K线失败(重试%d次): %s",
                                 counter['total'], counter['success'], counter['failed'],
                                 stock_name, attempt, e)
                    counter['failed'] += 1
                    return

    if klines is None:
        counter['failed'] += 1
        return

    conn = _open_conn(db_path)
    cursor = conn.cursor()
    t_db_start = asyncio.get_event_loop().time()
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
    conn.commit()
    conn.close()
    t_db_end = asyncio.get_event_loop().time()

    counter['success'] += 1
    print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 完成，本次查询{len(klines)}条，网络{elapsed:.2f}s dao{t_dao-t_start:.2f}s 写db{t_db_end-t_db_start:.2f}s")
    # await asyncio.sleep(1)


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
