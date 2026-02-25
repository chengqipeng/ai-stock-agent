import asyncio
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from chinese_calendar import is_workday
from common.constants.stocks_data import STOCKS
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from common.utils.stock_info_utils import get_stock_info_by_code


def get_missing_trading_days(db_path, stock_code, n=20):
    """
    返回过去n天内需要拉取K线数据的交易日列表。

    今天的处理逻辑（今天必须是A股交易日）：
      - 盘前（< 09:30）：不拉取今天，今天数据尚未产生
      - 盘中（09:30 ~ 15:00）：强制拉取今天，即使数据库已有今天记录也覆盖更新（实时数据持续变化）
      - 收盘后（> 15:00）：今天已在数据库则跳过，不在则拉取

    历史交易日：数据库中缺失的交易日均纳入拉取列表。

    返回值按日期降序排列（最新日期在前）。
    """
    from datetime import datetime, time as dtime
    today = date.today()
    now = datetime.now().time()
    in_trading = dtime(9, 30) <= now <= dtime(15, 0)
    after_close = now > dtime(15, 0)

    trading_days = set()
    for i in range(n):
        d = today - timedelta(days=i)
        if d.weekday() < 5 and is_workday(d):
            trading_days.add(d)

    if not trading_days:
        return []

    table_name = f"kline_{stock_code.replace('.', '_')}"
    start = min(trading_days).isoformat()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT date FROM {table_name} WHERE date >= ?", (start,))
        existing = {date.fromisoformat(row[0]) for row in cursor.fetchall()}
        conn.close()
    except sqlite3.OperationalError:
        existing = set()

    missing = trading_days - existing
    # 盘中今天强制包含（即使已在库，也要更新实时数据）
    if in_trading and today in trading_days:
        missing.add(today)
    # 盘前或非交易日，不拉取今天
    if not in_trading and not after_close:
        missing.discard(today)

    return sorted(missing, reverse=True)


def get_latest_db_date(db_path, stock_code):
    """获取数据库中该股票最新K线日期"""
    table_name = f"kline_{stock_code.replace('.', '_')}"
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(date) FROM {table_name}")
        row = cursor.fetchone()
        conn.close()
        if row and row[0]:
            return date.fromisoformat(row[0])
    except sqlite3.OperationalError:
        pass
    return None


def create_kline_table(cursor, table_name):
    """创建K线数据表"""
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            open_price REAL,
            close_price REAL,
            high_price REAL,
            low_price REAL,
            trading_volume REAL,
            trading_amount REAL,
            amplitude REAL,
            change_percent REAL,
            change_amount REAL,
            change_hand REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date)
        )
    ''')


def parse_kline_data(kline_str):
    """解析K线数据字符串"""
    fields = kline_str.split(',')
    return {
        'date': fields[0],
        'open_price': float(fields[1]),
        'close_price': float(fields[2]),
        'high_price': float(fields[3]),
        'low_price': float(fields[4]),
        'trading_volume': float(fields[5]),
        'trading_amount': float(fields[6]),
        'amplitude': float(fields[7]),
        'change_percent': float(fields[8]),
        'change_amount': float(fields[9]),
        'change_hand': float(fields[10])
    }


def insert_or_update_kline_data(cursor, table_name, kline_data):
    """插入或更新K线数据"""
    cursor.execute(f'''
        INSERT OR REPLACE INTO {table_name} 
        (date, open_price, close_price, high_price, low_price, trading_volume, 
         trading_amount, amplitude, change_percent, change_amount, change_hand, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ''', (
        kline_data['date'],
        kline_data['open_price'],
        kline_data['close_price'],
        kline_data['high_price'],
        kline_data['low_price'],
        kline_data['trading_volume'],
        kline_data['trading_amount'],
        kline_data['amplitude'],
        kline_data['change_percent'],
        kline_data['change_amount'],
        kline_data['change_hand']
    ))



async def process_stock_klines(stock_code, stock_name, db_path, limit, counter):
    """处理单个股票的K线数据"""
    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        counter['failed'] += 1
        return

    # 判断过去20天交易日中数据库是否有缺失
    missing_days = get_missing_trading_days(db_path, stock_code)
    if not missing_days:
        latest_db_date = get_latest_db_date(db_path, stock_code)
        print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 最新数据日期是{latest_db_date}，无需拉取数据")
        counter['success'] += 1
        return

    latest_db_date = get_latest_db_date(db_path, stock_code)
    # fetch_limit 基于最早缺失日期到今天的自然日数，+5作为缓冲确保覆盖所有缺失交易日
    earliest_missing = missing_days[-1]  # missing_days 降序，最后一个是最早的
    fetch_limit = (date.today() - earliest_missing).days + 5 if latest_db_date else limit

    klines = None
    for attempt in range(1, 11):
        try:
            klines = await get_stock_day_range_kline(stock_info, fetch_limit)
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
    # 停牌日处理：缺失日期在API返回数据中也不存在，说明是停牌日，插入占位记录避免重复拉取
    for d in missing_days:
        if d not in saved_dates:
            cursor.execute(f'''
                INSERT OR IGNORE INTO {table_name}
                (date, open_price, close_price, high_price, low_price, trading_volume,
                 trading_amount, amplitude, change_percent, change_amount, change_hand)
                VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            ''', (d.isoformat(),))
    conn.commit()
    conn.close()

    counter['success'] += 1
    print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock_name}] 完成，本次查询{len(klines)}条")
    await asyncio.sleep(3)


async def run_stock_klines_job(limit=800, max_concurrent=1):
    """运行股票K线数据采集任务"""
    # 创建数据库目录
    db_dir = Path("data_results/sql_lite")
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "stock_klines.db"
    
    print(f"开始采集股票K线数据，共 {len(STOCKS)} 只股票")
    print(f"数据库路径: {db_path}")
    
    semaphore = asyncio.Semaphore(max_concurrent)
    counter = {'total': len(STOCKS), 'success': 0, 'failed': 0}

    async def process_with_semaphore(stock):
        async with semaphore:
            print(f"[总{counter['total']} 成功{counter['success']} 失败{counter['failed']} 当前:{stock['name']}] 开始查询")
            await process_stock_klines(stock['code'], stock['name'], str(db_path), limit, counter)

    await asyncio.gather(*[process_with_semaphore(stock) for stock in STOCKS], return_exceptions=True)
    print(f"采集完成，总{counter['total']} 成功{counter['success']} 失败{counter['failed']}")


if __name__ == "__main__":
    asyncio.run(run_stock_klines_job())