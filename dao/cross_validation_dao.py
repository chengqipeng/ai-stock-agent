"""
数据交叉验证结果 DAO
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)
_CST = ZoneInfo("Asia/Shanghai")


def create_cross_validation_table():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_cross_validation (
                id INT AUTO_INCREMENT PRIMARY KEY,
                run_date DATE NOT NULL COMMENT '验证执行日期',
                category VARCHAR(30) NOT NULL COMMENT '验证类别: kline/finance/price/time_data/order_book/fund_flow',
                stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                check_date DATE COMMENT '被验证的数据日期',
                field_name VARCHAR(50) NOT NULL COMMENT '字段名',
                db_value VARCHAR(100) COMMENT '数据库值',
                sina_value VARCHAR(100) COMMENT '新浪值',
                diff_pct DECIMAL(10,4) COMMENT '差异百分比',
                match_status VARCHAR(10) NOT NULL COMMENT 'match/mismatch/missing',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_run_date (run_date),
                INDEX idx_category (category),
                INDEX idx_stock_code (stock_code),
                INDEX idx_match_status (match_status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_cross_validation_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                run_date DATE NOT NULL COMMENT '验证执行日期',
                category VARCHAR(30) NOT NULL COMMENT '验证类别',
                total_checks INT DEFAULT 0 COMMENT '总检查数',
                match_count INT DEFAULT 0 COMMENT '匹配数',
                mismatch_count INT DEFAULT 0 COMMENT '不匹配数',
                missing_count INT DEFAULT 0 COMMENT '缺失数',
                match_rate DECIMAL(6,2) COMMENT '匹配率(%)',
                sample_stocks INT DEFAULT 0 COMMENT '抽样股票数',
                detail TEXT COMMENT '详细信息',
                started_at DATETIME COMMENT '开始时间',
                finished_at DATETIME COMMENT '结束时间',
                duration_seconds INT COMMENT '耗时(秒)',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_run_date (run_date),
                INDEX idx_category (category),
                UNIQUE KEY uk_run_category (run_date, category)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def batch_insert_validation_details(rows: list[dict]):
    """批量插入验证明细"""
    if not rows:
        return
    conn = get_connection()
    cursor = conn.cursor()
    try:
        sql = ("INSERT INTO data_cross_validation "
               "(run_date, category, stock_code, check_date, field_name, "
               "db_value, sina_value, diff_pct, match_status) "
               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        values = [(r["run_date"], r["category"], r["stock_code"], r.get("check_date"),
                   r["field_name"], str(r.get("db_value", ""))[:100],
                   str(r.get("sina_value", ""))[:100],
                   r.get("diff_pct"), r["match_status"]) for r in rows]
        cursor.executemany(sql, values)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def upsert_validation_summary(row: dict):
    """插入或更新验证汇总（含重试，防止远程连接断开）"""
    import time

    sql = ("INSERT INTO data_cross_validation_summary "
           "(run_date, category, total_checks, match_count, mismatch_count, "
           "missing_count, match_rate, sample_stocks, detail, started_at, "
           "finished_at, duration_seconds) "
           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
           "ON DUPLICATE KEY UPDATE "
           "total_checks=VALUES(total_checks), match_count=VALUES(match_count), "
           "mismatch_count=VALUES(mismatch_count), missing_count=VALUES(missing_count), "
           "match_rate=VALUES(match_rate), sample_stocks=VALUES(sample_stocks), "
           "detail=VALUES(detail), started_at=VALUES(started_at), "
           "finished_at=VALUES(finished_at), duration_seconds=VALUES(duration_seconds)")
    params = (
        row["run_date"], row["category"], row.get("total_checks", 0),
        row.get("match_count", 0), row.get("mismatch_count", 0),
        row.get("missing_count", 0), row.get("match_rate"),
        row.get("sample_stocks", 0), row.get("detail"),
        row.get("started_at"), row.get("finished_at"),
        row.get("duration_seconds"),
    )

    last_err = None
    for attempt in range(3):
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
            return
        except Exception as e:
            last_err = e
            logger.warning("[upsert_validation_summary] 第%d次执行失败: %s", attempt + 1, e)
            try:
                conn.rollback()
            except Exception:
                pass
            if attempt < 2:
                time.sleep(1)
        finally:
            cursor.close()
            conn.close()

    raise last_err


def get_latest_summary(run_date: str = None) -> list[dict]:
    """获取最新一次验证汇总"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        if run_date:
            cursor.execute(
                "SELECT * FROM data_cross_validation_summary WHERE run_date = %s ORDER BY category",
                (run_date,))
        else:
            cursor.execute(
                "SELECT run_date FROM data_cross_validation_summary ORDER BY run_date DESC LIMIT 1")
            row = cursor.fetchone()
            if not row:
                return []
            cursor.execute(
                "SELECT * FROM data_cross_validation_summary WHERE run_date = %s ORDER BY category",
                (row["run_date"],))
        rows = list(cursor.fetchall())
        for r in rows:
            for k in ('started_at', 'finished_at', 'created_at', 'run_date'):
                if r.get(k) and isinstance(r[k], (datetime,)):
                    r[k] = r[k].strftime("%Y-%m-%d %H:%M:%S")
                elif r.get(k) and hasattr(r[k], 'isoformat'):
                    r[k] = r[k].isoformat()
        return rows
    finally:
        cursor.close()
        conn.close()


def get_validation_details(run_date: str, category: str = None,
                           match_status: str = None, limit: int = 200) -> list[dict]:
    """获取验证明细"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        where = ["run_date = %s"]
        params = [run_date]
        if category:
            where.append("category = %s")
            params.append(category)
        if match_status:
            where.append("match_status = %s")
            params.append(match_status)
        sql = (f"SELECT * FROM data_cross_validation WHERE {' AND '.join(where)} "
               f"ORDER BY category, stock_code, check_date LIMIT %s")
        params.append(limit)
        cursor.execute(sql, params)
        rows = list(cursor.fetchall())
        for r in rows:
            for k in ('run_date', 'check_date', 'created_at'):
                if r.get(k) and hasattr(r[k], 'isoformat'):
                    r[k] = r[k].isoformat()
                elif r.get(k) and isinstance(r[k], datetime):
                    r[k] = r[k].strftime("%Y-%m-%d %H:%M:%S")
        return rows
    finally:
        cursor.close()
        conn.close()
