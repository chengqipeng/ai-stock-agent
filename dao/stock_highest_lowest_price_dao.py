"""股票历史最高最低价 DAO — MySQL 版"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")

TABLE_NAME = "stock_highest_lowest_price"

_UPSERT_SQL = f"""
    INSERT INTO {TABLE_NAME}
    (stock_code, stock_name, highest_price, highest_date, lowest_price, lowest_date, update_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        stock_name=VALUES(stock_name),
        highest_price = CASE
            WHEN VALUES(highest_price) > highest_price OR VALUES(highest_date) > highest_date
            THEN VALUES(highest_price) ELSE highest_price END,
        highest_date = CASE
            WHEN VALUES(highest_price) > highest_price OR VALUES(highest_date) > highest_date
            THEN VALUES(highest_date) ELSE highest_date END,
        lowest_price = CASE
            WHEN VALUES(lowest_price) < lowest_price OR VALUES(lowest_date) > lowest_date
            THEN VALUES(lowest_price) ELSE lowest_price END,
        lowest_date = CASE
            WHEN VALUES(lowest_price) < lowest_price OR VALUES(lowest_date) > lowest_date
            THEN VALUES(lowest_date) ELSE lowest_date END,
        update_time=VALUES(update_time)
"""


def save_price_record(record: dict):
    """保存单条最高最低价记录（upsert），如果有更高价或更新时间则覆盖"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(_UPSERT_SQL, (
            record["code"], record["name"],
            record["highest_price"], record["highest_date"],
            record["lowest_price"], record["lowest_date"],
            record["update_time"],
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("save_price_record 失败 [%s]: %s", record.get("code"), e)
        raise
    finally:
        cursor.close()
        conn.close()


def get_today_processed_codes(today: str) -> set:
    """获取今日已处理的股票代码集合"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT stock_code FROM {TABLE_NAME} WHERE update_time LIKE %s",
            (f"{today}%",),
        )
        return {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()
        conn.close()


def get_all_price_records() -> list[dict]:
    """查询所有最高最低价记录"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT stock_code AS code, stock_name AS name,
                   highest_price, highest_date, lowest_price, lowest_date, update_time
            FROM {TABLE_NAME}
        """)
        return list(cursor.fetchall())
    finally:
        cursor.close()
        conn.close()


def get_new_high_low_count_from_db(start_date: str) -> dict:
    """直接在数据库中统计创新高/新低数量"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN highest_date >= %s THEN 1 ELSE 0 END) AS new_high,
                SUM(CASE WHEN lowest_date >= %s THEN 1 ELSE 0 END) AS new_low
            FROM {TABLE_NAME}
        """, (start_date, start_date))
        row = cursor.fetchone()
        return {"total": row[0], "new_high": row[1] or 0, "new_low": row[2] or 0}
    finally:
        cursor.close()
        conn.close()


def get_candidates_by_high_date(start_date: str, top_n: int = 10) -> list[dict]:
    """查询指定日期后创新高的股票，按粗略涨幅降序取前N"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT stock_code AS code, stock_name AS name,
                   highest_price, highest_date, lowest_price, lowest_date
            FROM {TABLE_NAME}
            WHERE highest_date >= %s
              AND highest_price IS NOT NULL AND lowest_price IS NOT NULL
              AND lowest_price > 0
            ORDER BY (highest_price - lowest_price) / lowest_price DESC
            LIMIT %s
        """, (start_date, top_n))
        return list(cursor.fetchall())
    finally:
        cursor.close()
        conn.close()
