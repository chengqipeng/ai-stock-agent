import logging
import re
import sqlite3
from datetime import date, timedelta, datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
from chinese_calendar import is_workday

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")
_DB_DIR = Path(__file__).parent.parent / "data_results/sql_lite"


def get_db_path_for_stock(stock_code: str, db_dir: Path = None) -> Path:
    db_dir = db_dir or _DB_DIR
    safe_code = stock_code.replace('.', '_')
    return db_dir / f'stock_{safe_code}.db'


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
    cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(date)')


def _to_float(v):
    return float(v) if v and v != 'None' else 0.0


def parse_kline_data(kline_str):
    fields = kline_str.split(',')
    return {
        'date': fields[0],
        'open_price': _to_float(fields[1]),
        'close_price': _to_float(fields[2]),
        'high_price': _to_float(fields[3]),
        'low_price': _to_float(fields[4]),
        'trading_volume': _to_float(fields[5]),
        'trading_amount': _to_float(fields[6]),
        'amplitude': _to_float(fields[7]),
        'change_percent': _to_float(fields[8]),
        'change_amount': _to_float(fields[9]),
        'change_hand': _to_float(fields[10])
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


def batch_insert_or_update_kline_data(cursor, table_name, kline_data_list):
    now_cst = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    cursor.executemany(f'''
        INSERT OR REPLACE INTO {table_name}
        (date, open_price, close_price, high_price, low_price, trading_volume,
         trading_amount, amplitude, change_percent, change_amount, change_hand, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', [
        (d['date'], d['open_price'], d['close_price'], d['high_price'], d['low_price'],
         d['trading_volume'], d['trading_amount'], d['amplitude'], d['change_percent'],
         d['change_amount'], d['change_hand'], now_cst)
        for d in kline_data_list
    ])


def insert_suspension_day(cursor, table_name, d: date):
    """插入停牌日占位记录"""
    cursor.execute(f'''
        INSERT OR IGNORE INTO {table_name}
        (date, open_price, close_price, high_price, low_price, trading_volume,
         trading_amount, amplitude, change_percent, change_amount, change_hand)
        VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    ''', (d.isoformat(),))


def _open_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn


def get_latest_db_date(db_path, stock_code):
    """获取数据库中该股票最新K线日期"""
    table_name = f"kline_{stock_code.replace('.', '_')}"
    try:
        conn = _open_conn(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(date) FROM {table_name}")
        row = cursor.fetchone()
        conn.close()
        if row and row[0]:
            return date.fromisoformat(row[0])
    except sqlite3.OperationalError as e:
        logger.warning("get_latest_db_date 查询失败 [%s]: %s", stock_code, e)
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
    today_iso = today.isoformat()
    try:
        conn = _open_conn(db_path)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT date, updated_at FROM {table_name} WHERE date >= ?", (start,)
        )
        rows = cursor.fetchall()
        conn.close()
        existing = {date.fromisoformat(r[0]) for r in rows}
        today_updated_at = next((r[1] for r in rows if r[0] == today_iso), None)
    except sqlite3.OperationalError as e:
        logger.warning("get_missing_trading_days 查询失败 [%s]: %s", stock_code, e)
        existing = set()
        today_updated_at = None

    missing = trading_days - existing
    if today in trading_days and (in_trading or after_close):
        if in_trading:
            missing.add(today)
        elif after_close and today not in missing:
            if today_updated_at and today_updated_at < f"{today_iso} 15:00:00":
                missing.add(today)
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
    conn = _open_conn(db_path)
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
    except sqlite3.OperationalError as e:
        logger.warning("get_kline_data 查询失败 [%s]: %s", stock_code, e)
        rows = []
    finally:
        conn.close()

    return rows


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_trading_day(d: date) -> bool:
    try:
        return d.weekday() < 5 and is_workday(d)
    except Exception as e:
        logger.warning("_is_trading_day 判断失败 [%s]: %s", d, e)
        return True


def check_db(db_path: Path) -> list[dict]:
    """
    检测 stock_*.db 数据异常，返回异常列表。
    同时会删除非交易日数据。
    """
    stock_code = db_path.stem.removeprefix("stock_").replace("_", ".")
    table_name = f"kline_{db_path.stem.removeprefix('stock_')}"
    issues = []

    def issue(row_date, anomaly_type, detail):
        issues.append({"stock_code": stock_code, "date": row_date, "type": anomaly_type, "detail": detail,
                       "legacy": bool(row_date and str(row_date) < "2025-07-01")})

    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
    except Exception as e:
        return [{"stock_code": stock_code, "date": None, "type": "DB_OPEN_ERROR", "detail": str(e)}]

    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
    except sqlite3.OperationalError as e:
        logger.warning("check_db 表不存在 [%s]: %s", stock_code, e)
        conn.close()
        return [{"stock_code": stock_code, "date": None, "type": "TABLE_MISSING", "detail": f"表 {table_name} 不存在"}]

    if count == 0:
        conn.close()
        return [{"stock_code": stock_code, "date": None, "type": "TABLE_EMPTY", "detail": "表为空，无任何数据"}]

    cur.execute(
        f"SELECT date, open_price, close_price, high_price, low_price, "
        f"trading_volume, trading_amount, change_percent FROM {table_name} ORDER BY date ASC"
    )
    rows = cur.fetchall()

    seen_dates = {}
    prev_date = None

    for row in rows:
        d = row["date"]
        op, cp, hp, lp = row["open_price"], row["close_price"], row["high_price"], row["low_price"]
        vol, amt, chg = row["trading_volume"], row["trading_amount"], row["change_percent"]

        # 1. 日期格式
        if not _DATE_RE.match(str(d)):
            issue(d, "INVALID_DATE_FORMAT", f"日期格式异常: {d}")
            continue

        # 非交易日数据：立即删除
        try:
            row_date = date.fromisoformat(d)
            if not _is_trading_day(row_date):
                cur.execute(f"DELETE FROM {table_name} WHERE date = ?", (d,))
                conn.commit()
                logger.warning("[%s] 删除非交易日数据: date=%s", stock_code, d)
                continue
        except ValueError as e:
            logger.debug("[%s] 日期解析失败: date=%s, %s", stock_code, d, e)

        # 2. 日期重复
        if d in seen_dates:
            issue(d, "DUPLICATE_DATE", "日期重复出现")
        seen_dates[d] = True

        is_suspension = (cp == 0 and op == 0 and hp == 0 and lp == 0 and vol == 0 and amt == 0)

        if is_suspension:
            if chg != 0:
                issue(d, "SUSPENSION_NONZERO_FIELD", f"停牌占位记录中 change_percent={chg} 非零")
        else:
            for field, val in [("close_price", cp), ("open_price", op), ("high_price", hp), ("low_price", lp)]:
                if val is not None and val <= 0:
                    issue(d, "PRICE_NON_POSITIVE", f"{field}={val} 不合法（应 > 0）")

            if hp is not None and lp is not None and hp < lp:
                issue(d, "PRICE_HIGH_LESS_THAN_LOW", f"high_price={hp} < low_price={lp}")
            if cp is not None and hp is not None and cp > hp:
                issue(d, "PRICE_CLOSE_ABOVE_HIGH", f"close_price={cp} > high_price={hp}")
            if cp is not None and lp is not None and cp < lp:
                issue(d, "PRICE_CLOSE_BELOW_LOW", f"close_price={cp} < low_price={lp}")
            if op is not None and hp is not None and op > hp:
                issue(d, "PRICE_OPEN_ABOVE_HIGH", f"open_price={op} > high_price={hp}")
            if op is not None and lp is not None and op < lp:
                issue(d, "PRICE_OPEN_BELOW_LOW", f"open_price={op} < low_price={lp}")

            if vol is not None and vol < 0:
                issue(d, "NEGATIVE_VOLUME", f"trading_volume={vol} < 0")
            if amt is not None and amt < 0:
                issue(d, "NEGATIVE_AMOUNT", f"trading_amount={amt} < 0")

            if chg is not None and abs(chg) > 21:
                issue(d, "ABNORMAL_CHANGE_PERCENT", f"change_percent={chg}% 超过±21%")

        if prev_date is not None:
            try:
                d0 = date.fromisoformat(prev_date)
                d1 = date.fromisoformat(d)
                gap_days = (d1 - d0).days
                if gap_days > 1:
                    pass  # 缺失交易日检测（当前仅预留）
            except (ValueError, Exception) as e:
                logger.debug("[%s] 缺失交易日检测异常: %s", stock_code, e)
        prev_date = d

    conn.close()
    return issues


def kline_to_dao_record(k: dict) -> dict:
    """将 get_stock_day_kline_10jqka 返回的记录转换为 dao 层所需格式"""
    return {
        "date":           k["date"],
        "open_price":     k["open_price"],
        "close_price":    k["close_price"],
        "high_price":     k["high_price"],
        "low_price":      k["low_price"],
        "trading_volume": k["trading_volume"],
        "trading_amount": k.get("trading_amount") or 0.0,
        "amplitude":      0.0,
        "change_percent": 0.0,
        "change_amount":  0.0,
        "change_hand":    k.get("change_hand") or 0.0,
    }


def save_kline_to_db(stock_code_normalize: str, klines: list[dict]) -> None:
    """将重新拉取的 K 线数据覆盖写入数据库"""
    db_path = get_db_path_for_stock(stock_code_normalize)
    table_name = f"kline_{stock_code_normalize.replace('.', '_')}"
    records = [kline_to_dao_record(k) for k in klines]
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        cur = conn.cursor()
        create_kline_table(cur, table_name)
        batch_insert_or_update_kline_data(cur, table_name, records)
        conn.commit()
    finally:
        conn.close()


if __name__ == '__main__':
    stock_code = '600183.SH'
    db_path = get_db_path_for_stock(stock_code)
    print('DB path:', db_path)
    print('Latest date:', get_latest_db_date(db_path, stock_code))
    print('Missing trading days:', get_missing_trading_days(db_path, stock_code))
    rows = get_kline_data(stock_code, limit=5)
    for row in rows:
        print(row)
