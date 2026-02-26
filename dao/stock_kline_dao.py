import sqlite3
from datetime import date, timedelta, datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
from chinese_calendar import is_workday

_CST = ZoneInfo("Asia/Shanghai")
_DB_DIR = Path(__file__).parent.parent / "data_results/sql_lite"


def get_db_path_for_stock(stock_code: str, db_dir: Path = None) -> Path:
    db_dir = db_dir or _DB_DIR
    code_num = stock_code.split('.')[0]
    exchange = stock_code.split('.')[-1].upper()
    if exchange == 'SH':
        return db_dir / 'stock_klines_sh.db'
    if exchange == 'SZ':
        prefix = int(code_num[:3])
        if prefix >= 300:
            return db_dir / 'stock_klines_sz_cyb.db'
        if prefix < 1:
            return db_dir / 'stock_klines_sz_000.db'
        if prefix < 2:
            return db_dir / 'stock_klines_sz_001.db'
        return db_dir / 'stock_klines_sz_002.db'
    return db_dir / 'stock_klines_other.db'


def create_kline_table(cursor, table_name):
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
    now_cst = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(f'''
        INSERT OR REPLACE INTO {table_name} 
        (date, open_price, close_price, high_price, low_price, trading_volume, 
         trading_amount, amplitude, change_percent, change_amount, change_hand, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        kline_data['change_hand'],
        now_cst
    ))


def insert_suspension_day(cursor, table_name, d: date):
    """插入停牌日占位记录"""
    cursor.execute(f'''
        INSERT OR IGNORE INTO {table_name}
        (date, open_price, close_price, high_price, low_price, trading_volume,
         trading_amount, amplitude, change_percent, change_amount, change_hand)
        VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    ''', (d.isoformat(),))


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
    now_cst = datetime.now(_CST)
    today = now_cst.date()
    now = now_cst.time()
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
    if today in trading_days and (in_trading or after_close):
        if in_trading:
            missing.add(today)
        elif after_close and today not in missing:
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute(f"SELECT updated_at FROM {table_name} WHERE date=?", (today.isoformat(),))
                row = cursor.fetchone()
                conn.close()
                if row and row[0] < f"{today.isoformat()} 15:00:00":
                    missing.add(today)
            except sqlite3.OperationalError:
                pass
    if not in_trading and not after_close:
        missing.discard(today)

    return sorted(missing, reverse=True)


def get_kline_data(stock_code: str, start_date: str = None, end_date: str = None, limit: int = None) -> list[dict]:
    """
    查询股票K线数据

    Args:
        stock_code: 股票代码，如 "300812.SZ"
        start_date: 开始日期，如 "2024-01-01"（可选）
        end_date: 结束日期，如 "2024-12-31"（可选）
        limit: 返回条数限制（可选）

    Returns:
        list[dict]: K线数据列表，按日期升序排列
    """
    table_name = f"kline_{stock_code.replace('.', '_')}"
    db_path = get_db_path_for_stock(stock_code)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    sql = f"SELECT date, open_price, close_price, high_price, low_price, trading_volume, trading_amount, amplitude, change_percent, change_amount, change_hand FROM {table_name} WHERE 1=1"
    params = []

    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND date <= ?"
        params.append(end_date)

    sql += " ORDER BY date DESC"

    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    try:
        cursor.execute(sql, params)
        rows = [dict(row) for row in cursor.fetchall()]
        rows.reverse()
    except sqlite3.OperationalError:
        rows = []
    finally:
        conn.close()

    return rows
