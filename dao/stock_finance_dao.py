"""
财报数据 DAO

将 get_financial_data_to_json 返回的财报数据存储到每只股票的 SQLite 数据库中。
表名: finance_{stock_code}  (如 finance_600519_SH)
每条记录对应一个报告期，以 report_date 为唯一键。
"""
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")
_DB_DIR = Path(__file__).parent.parent / "data_results/sql_lite"


def _open_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def get_finance_table_name(stock_code: str) -> str:
    return f"finance_{stock_code.replace('.', '_')}"


def create_finance_table(cursor: sqlite3.Cursor, table_name: str):
    """创建财报数据表"""
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,
            report_period_name TEXT,
            data_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(report_date)
        )
    """)
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_report_date ON {table_name}(report_date)"
    )


def batch_upsert_finance_data(
    cursor: sqlite3.Cursor,
    table_name: str,
    records: list[dict],
):
    """
    批量插入或更新财报数据。

    Args:
        cursor: 数据库游标
        table_name: 表名
        records: get_financial_data_to_json 返回的 list[dict]，
                 每条包含 "报告期"、"报告日期" 及各指标字段。
    """
    now_cst = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for rec in records:
        report_date = (rec.get("报告日期") or "")[:10]
        if not report_date:
            continue
        report_name = rec.get("报告期", "")
        rows.append((report_date, report_name, json.dumps(rec, ensure_ascii=False), now_cst))

    if not rows:
        return

    cursor.executemany(
        f"""
        INSERT OR REPLACE INTO {table_name}
        (report_date, report_period_name, data_json, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )


def save_finance_to_db(stock_code: str, records: list[dict], db_path: str | None = None):
    """
    将财报数据保存到股票对应的 SQLite 数据库。

    Args:
        stock_code: 标准化股票代码（如 "600519.SH"）
        records: get_financial_data_to_json 返回的数据
        db_path: 数据库路径，None 则使用默认路径
    """
    if not records:
        return

    if db_path is None:
        safe_code = stock_code.replace(".", "_")
        db_path = str(_DB_DIR / f"stock_{safe_code}.db")

    table_name = get_finance_table_name(stock_code)
    conn = _open_conn(db_path)
    cursor = conn.cursor()
    try:
        create_finance_table(cursor, table_name)
        batch_upsert_finance_data(cursor, table_name, records)
        conn.commit()
    finally:
        conn.close()


def get_finance_from_db(
    stock_code: str,
    db_path: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    """
    从数据库读取财报数据。

    Returns:
        list[dict]: 按 report_date 倒序排列的财报记录
    """
    if db_path is None:
        safe_code = stock_code.replace(".", "_")
        db_path = str(_DB_DIR / f"stock_{safe_code}.db")

    table_name = get_finance_table_name(stock_code)
    conn = _open_conn(db_path)
    try:
        cursor = conn.cursor()
        sql = f"SELECT data_json FROM {table_name} ORDER BY report_date DESC"
        if limit:
            sql += f" LIMIT {limit}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        return [json.loads(row[0]) for row in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()

def get_finance_latest_updated_at(
    stock_code: str,
    db_path: str | None = None,
) -> str | None:
    """
    获取财报表中最新的 updated_at 时间戳。

    Returns:
        如 "2025-07-15 10:30:00"，表不存在或无数据返回 None
    """
    if db_path is None:
        safe_code = stock_code.replace(".", "_")
        db_path = str(_DB_DIR / f"stock_{safe_code}.db")

    table_name = get_finance_table_name(stock_code)
    conn = _open_conn(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(updated_at) FROM {table_name}")
        row = cursor.fetchone()
        return row[0] if row and row[0] else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


