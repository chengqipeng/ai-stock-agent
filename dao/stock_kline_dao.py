"""K线数据 DAO — MySQL 单表版，所有股票共用 stock_kline 表"""
import logging
import re
from datetime import date, timedelta, datetime, time as dtime
from zoneinfo import ZoneInfo
from chinese_calendar import is_workday

from dao import get_connection

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")

TABLE_NAME = "stock_kline"



def _get_table_name(stock_code: str) -> str:
    """兼容旧调用，统一返回单表名"""
    return TABLE_NAME


def create_kline_table(cursor=None, table_name: str = None):
    """创建统一K线表（幂等），table_name 参数仅为兼容旧调用，实际忽略"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(20) NOT NULL,
            `date` VARCHAR(20) NOT NULL,
            open_price DOUBLE,
            close_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            trading_volume DOUBLE,
            trading_amount DOUBLE,
            amplitude DOUBLE,
            change_percent DOUBLE,
            change_amount DOUBLE,
            change_hand DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_date (stock_code, `date`),
            INDEX idx_stock_code (stock_code),
            INDEX idx_date (`date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(ddl)
    if own:
        conn.commit()
        cursor.close()
        conn.close()



# ─────────────────── 解析工具 ───────────────────

def _to_float(v):
    return float(v) if v and v != 'None' else 0.0


def parse_kline_data(kline_str: str) -> dict:
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
        'change_hand': _to_float(fields[10]),
    }


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


# ─────────────────── 写入 ───────────────────

_UPSERT_SQL = f"""
    INSERT INTO {TABLE_NAME}
    (stock_code, `date`, open_price, close_price, high_price, low_price,
     trading_volume, trading_amount, amplitude, change_percent, change_amount, change_hand, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        open_price=VALUES(open_price), close_price=VALUES(close_price),
        high_price=VALUES(high_price), low_price=VALUES(low_price),
        trading_volume=VALUES(trading_volume), trading_amount=VALUES(trading_amount),
        amplitude=VALUES(amplitude), change_percent=VALUES(change_percent),
        change_amount=VALUES(change_amount), change_hand=VALUES(change_hand),
        updated_at=VALUES(updated_at)
"""


def insert_or_update_kline_data(cursor, stock_code: str, kline_data: dict):
    now_cst = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(_UPSERT_SQL, (
        stock_code,
        kline_data['date'], kline_data['open_price'], kline_data['close_price'],
        kline_data['high_price'], kline_data['low_price'], kline_data['trading_volume'],
        kline_data['trading_amount'], kline_data['amplitude'], kline_data['change_percent'],
        kline_data['change_amount'], kline_data['change_hand'], now_cst,
    ))


def batch_insert_or_update_kline_data(cursor, stock_code: str, kline_data_list: list[dict]):
    now_cst = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    cursor.executemany(_UPSERT_SQL, [
        (stock_code,
         d['date'], d['open_price'], d['close_price'], d['high_price'], d['low_price'],
         d['trading_volume'], d['trading_amount'], d['amplitude'], d['change_percent'],
         d['change_amount'], d['change_hand'], now_cst)
        for d in kline_data_list
    ])


def insert_suspension_day(cursor, stock_code: str, d: date):
    """插入停牌日占位记录"""
    cursor.execute(f"""
        INSERT IGNORE INTO {TABLE_NAME}
        (stock_code, `date`, open_price, close_price, high_price, low_price,
         trading_volume, trading_amount, amplitude, change_percent, change_amount, change_hand)
        VALUES (%s, %s, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    """, (stock_code, d.isoformat()))


# ─────────────────── 查询 ───────────────────

def get_latest_db_date(stock_code: str) -> date | None:
    """获取数据库中该股票最新K线日期"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT MAX(`date`) FROM {TABLE_NAME} WHERE stock_code = %s", (stock_code,)
        )
        row = cursor.fetchone()
        if row and row[0]:
            return date.fromisoformat(str(row[0]))
    except Exception as e:
        logger.warning("get_latest_db_date 查询失败 [%s]: %s", stock_code, e)
    finally:
        cursor.close()
        conn.close()
    return None


def get_missing_trading_days(stock_code: str, n: int = 20) -> list[date]:
    """
    返回过去n天内需要拉取K线数据的交易日列表。

    今天的处理逻辑（今天必须是A股交易日）：
      - 盘前（< 09:30）：不拉取今天
      - 盘中（09:30 ~ 15:00）：强制拉取今天
      - 收盘后（> 15:00）：今天已在数据库则跳过，不在则拉取

    返回值按日期降序排列。
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

    start = min(trading_days).isoformat()
    today_iso = today.isoformat()

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT `date`, updated_at FROM {TABLE_NAME} WHERE stock_code = %s AND `date` >= %s",
            (stock_code, start),
        )
        rows = cursor.fetchall()
        existing = {date.fromisoformat(str(r[0])) for r in rows}
        today_updated_at = next((str(r[1]) for r in rows if str(r[0]) == today_iso), None)
    except Exception as e:
        logger.warning("get_missing_trading_days 查询失败 [%s]: %s", stock_code, e)
        existing = set()
        today_updated_at = None
    finally:
        cursor.close()
        conn.close()

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
    """查询股票K线数据，按日期升序。"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()

    sql = (f"SELECT `date`, open_price, close_price, high_price, low_price, "
           f"trading_volume, trading_amount, amplitude, change_percent, change_amount, change_hand "
           f"FROM {TABLE_NAME} WHERE stock_code = %s")
    params: list = [stock_code]

    if start_date:
        sql += " AND `date` >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND `date` <= %s"
        params.append(end_date)

    sql += " ORDER BY `date` DESC"

    if limit:
        sql += " LIMIT %s"
        params.append(limit)

    try:
        cursor.execute(sql, params)
        rows = list(cursor.fetchall())
        rows.reverse()
    except Exception as e:
        logger.warning("get_kline_data 查询失败 [%s]: %s", stock_code, e)
        rows = []
    finally:
        cursor.close()
        conn.close()

    return rows


# ─────────────────── 所有股票代码 ───────────────────

def get_all_stock_codes() -> list[str]:
    """从 stock_kline 表中查询所有不同的 stock_code"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT DISTINCT stock_code FROM {TABLE_NAME}")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


# ─────────────────── 数据检测 & 修复 ───────────────────

def check_db(stock_code: str) -> list[dict]:
    """
    检测指定股票在 stock_kline 表中的数据异常，返回异常列表。
    每条异常: {"type": str, "date": str, "detail": str, "legacy": bool}
    """
    conn = get_connection()
    cursor = conn.cursor()
    issues: list[dict] = []
    try:
        cursor.execute(
            f"SELECT `date`, open_price, close_price, high_price, low_price, "
            f"trading_volume, trading_amount, amplitude, change_percent, change_amount, change_hand "
            f"FROM {TABLE_NAME} WHERE stock_code = %s ORDER BY `date`",
            (stock_code,),
        )
        rows = cursor.fetchall()
        if not rows:
            issues.append({"type": "empty", "date": "-", "detail": "该股票无K线数据", "legacy": False})
            return issues

        seen_dates: set[str] = set()
        for r in rows:
            d, op, cp, hp, lp, vol, amt = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
            chg_pct = r[8]
            d_str = str(d)

            # 日期重复
            if d_str in seen_dates:
                issues.append({"type": "dup_date", "date": d_str, "detail": "日期重复", "legacy": False})
            seen_dates.add(d_str)

            # 停牌占位记录跳过价格检测
            if op == 0 and cp == 0 and hp == 0 and lp == 0 and vol == 0:
                continue

            # 价格 <= 0
            for name, val in [("open", op), ("close", cp), ("high", hp), ("low", lp)]:
                if val is not None and val <= 0:
                    issues.append({"type": "price_le0", "date": d_str,
                                   "detail": f"{name}_price={val}", "legacy": False})

            # 价格关系
            if hp is not None and lp is not None and hp < lp:
                issues.append({"type": "high_lt_low", "date": d_str,
                               "detail": f"high={hp} < low={lp}", "legacy": False})

            # 涨跌幅 > 21%
            if chg_pct is not None and abs(chg_pct) > 21:
                issues.append({"type": "chg_pct", "date": d_str,
                               "detail": f"change_percent={chg_pct}", "legacy": False})

            # 成交量/金额 < 0
            if vol is not None and vol < 0:
                issues.append({"type": "neg_vol", "date": d_str,
                               "detail": f"trading_volume={vol}", "legacy": False})
            if amt is not None and amt < 0:
                issues.append({"type": "neg_amt", "date": d_str,
                               "detail": f"trading_amount={amt}", "legacy": False})
    finally:
        cursor.close()
        conn.close()
    return issues


def save_kline_to_db(stock_code: str, kline_list: list[dict]):
    """将K线数据列表写入数据库（upsert），用于修复场景"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        create_kline_table(cursor)
        records = [kline_to_dao_record(k) for k in kline_list]
        batch_insert_or_update_kline_data(cursor, stock_code, records)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
