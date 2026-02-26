import asyncio
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import re
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from common.utils.stock_info_utils import get_stock_info_by_code
from dao.stock_kline_dao import (
    get_db_path_for_stock, get_missing_trading_days, get_latest_db_date,
    create_kline_table, parse_kline_data, insert_or_update_kline_data, insert_suspension_day
)
import sqlite3

_CST = ZoneInfo("Asia/Shanghai")


async def process_stock_klines(stock_code, stock_name, db_path, limit, counter):
    """处理单个股票的K线数据"""
    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        counter['failed'] += 1
        return

    missing_days = get_missing_trading_days(db_path, stock_code)
    if not missing_days:
        latest_db_date = get_latest_db_date(db_path, stock_code)
        print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 最新数据日期是{latest_db_date}，无需拉取数据")
        counter['success'] += 1
        return

    latest_db_date = get_latest_db_date(db_path, stock_code)
    earliest_missing = missing_days[-1]
    today_cst = datetime.now(_CST).date()
    fetch_limit = (today_cst - earliest_missing).days + 5 if latest_db_date else limit

    klines = None
    for attempt in range(1, 11):
        try:
            t0 = asyncio.get_event_loop().time()
            klines = await get_stock_day_range_kline(stock_info, fetch_limit)
            elapsed = asyncio.get_event_loop().time() - t0
            break
        except Exception as e:
            if ('Server disconnected' in str(e) or 'Connection closed abruptly' in str(e)) and attempt < 10:
                print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 连接中断({e.__class__.__name__})，第{attempt}次重试，等待10秒")
                await asyncio.sleep(10)
            else:
                print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 获取K线失败: {e}")
                counter['failed'] += 1
                return

    if not klines:
        counter['failed'] += 1
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    table_name = f"kline_{stock_code.replace('.', '_')}"
    create_kline_table(cursor, table_name)
    saved_dates = set()
    for kline_str in klines:
        try:
            kline_data = parse_kline_data(kline_str)
            insert_or_update_kline_data(cursor, table_name, kline_data)
            saved_dates.add(date.fromisoformat(kline_data['date']))
        except Exception as e:
            print(f"解析K线数据失败 {stock_code}: {e}")
    for d in missing_days:
        if d not in saved_dates:
            insert_suspension_day(cursor, table_name, d)
    conn.commit()
    conn.close()

    counter['success'] += 1
    print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 完成，本次查询{len(klines)}条，耗时{elapsed:.2f}s")
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
