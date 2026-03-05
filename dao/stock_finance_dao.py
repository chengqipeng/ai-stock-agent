"""
财报数据 DAO — MySQL 单表版，所有股票共用 stock_finance 表
"""
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")

TABLE_NAME = "stock_finance"


def get_finance_table_name(stock_code: str) -> str:
    """兼容旧调用，统一返回单表名"""
    return TABLE_NAME


def create_finance_table(cursor=None, table_name: str = None):
    """创建统一财报表（幂等），table_name 参数仅为兼容旧调用，实际忽略"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(20) NOT NULL,
            report_date VARCHAR(20) NOT NULL,
            report_period_name VARCHAR(50),
            data_json LONGTEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_report (stock_code, report_date),
            INDEX idx_stock_code (stock_code),
            INDEX idx_report_date (report_date)
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


# ─────────────────── 写入 ───────────────────

_UPSERT_SQL = f"""
    INSERT INTO {TABLE_NAME}
    (stock_code, report_date, report_period_name, data_json, updated_at)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        report_period_name = VALUES(report_period_name),
        data_json = VALUES(data_json),
        updated_at = VALUES(updated_at)
"""


def batch_upsert_finance_data(cursor, stock_code: str, records: list[dict]):
    """批量插入或更新财报数据。stock_code 为股票代码（如 600519.SH）。
    兼容旧调用：如果 stock_code 看起来像表名（finance_xxx），自动提取真实代码。
    """
    # 兼容旧调用方传入 table_name 的情况
    if stock_code.startswith("finance_"):
        code = stock_code.removeprefix("finance_")
        parts = code.rsplit("_", 1)
        stock_code = f"{parts[0]}.{parts[1]}" if len(parts) == 2 else code

    now_cst = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for rec in records:
        report_date = (rec.get("报告日期") or "")[:10]
        if not report_date:
            continue
        report_name = rec.get("报告期", "")
        rows.append((stock_code, report_date, report_name,
                      json.dumps(rec, ensure_ascii=False), now_cst))

    if not rows:
        return

    cursor.executemany(_UPSERT_SQL, rows)


def save_finance_to_db(stock_code: str, records: list[dict]):
    """将财报数据保存到 MySQL。"""
    if not records:
        return
    conn = get_connection()
    cursor = conn.cursor()
    try:
        create_finance_table(cursor)
        batch_upsert_finance_data(cursor, stock_code, records)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# ─────────────────── 查询 ───────────────────

def get_finance_from_db(stock_code: str, limit: int | None = None) -> list[dict]:
    """从数据库读取财报数据，按 report_date 倒序。"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        sql = f"SELECT data_json FROM {TABLE_NAME} WHERE stock_code = %s ORDER BY report_date DESC"
        params: list = [stock_code]
        if limit:
            sql += " LIMIT %s"
            params.append(limit)
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        return [json.loads(row[0]) for row in rows]
    except Exception:
        return []
    finally:
        cursor.close()
        conn.close()


def get_finance_latest_updated_at(stock_code: str) -> str | None:
    """获取该股票财报数据中最新的 updated_at 时间戳。"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT MAX(updated_at) FROM {TABLE_NAME} WHERE stock_code = %s",
            (stock_code,),
        )
        row = cursor.fetchone()
        return str(row[0]) if row and row[0] else None
    except Exception:
        return None
    finally:
        cursor.close()
        conn.close()
