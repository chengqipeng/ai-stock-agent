"""
调度任务执行日志 DAO
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)
_CST = ZoneInfo("Asia/Shanghai")


def create_scheduler_log_table():
    """创建调度任务执行日志表"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scheduler_execution_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                job_name VARCHAR(50) NOT NULL COMMENT '任务名称',
                status VARCHAR(20) NOT NULL COMMENT 'running/success/failed/partial',
                total_count INT DEFAULT 0 COMMENT '总数',
                success_count INT DEFAULT 0 COMMENT '成功数',
                failed_count INT DEFAULT 0 COMMENT '失败数',
                skipped_count INT DEFAULT 0 COMMENT '跳过数(断点续传)',
                detail MEDIUMTEXT COMMENT '详细信息',
                started_at DATETIME NOT NULL COMMENT '开始时间',
                finished_at DATETIME COMMENT '结束时间',
                duration_seconds INT COMMENT '耗时(秒)',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_job_name (job_name),
                INDEX idx_started_at (started_at),
                INDEX idx_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        # 兼容已有表：将 detail 列升级为 MEDIUMTEXT
        cursor.execute(
            "ALTER TABLE scheduler_execution_log MODIFY COLUMN detail MEDIUMTEXT COMMENT '详细信息'"
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def insert_log(job_name: str, started_at: datetime) -> int:
    """插入一条执行日志，返回 log_id"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        create_scheduler_log_table()
        cursor.execute(
            "INSERT INTO scheduler_execution_log (job_name, status, started_at) VALUES (%s, %s, %s)",
            (job_name, "running", started_at),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        cursor.close()
        conn.close()


def update_log(log_id: int, status: str, total_count: int = 0,
               success_count: int = 0, failed_count: int = 0,
               skipped_count: int = 0, detail: str = None):
    """更新执行日志"""
    now = datetime.now(_CST)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # 先查 started_at 计算耗时
        cursor.execute("SELECT started_at FROM scheduler_execution_log WHERE id = %s", (log_id,))
        row = cursor.fetchone()
        duration = None
        if row and row[0]:
            started = row[0]
            if isinstance(started, datetime):
                if started.tzinfo is None:
                    started = started.replace(tzinfo=_CST)
                duration = int((now - started).total_seconds())

        cursor.execute(
            "UPDATE scheduler_execution_log SET status=%s, total_count=%s, success_count=%s, "
            "failed_count=%s, skipped_count=%s, detail=%s, finished_at=%s, duration_seconds=%s "
            "WHERE id=%s",
            (status, total_count, success_count, failed_count, skipped_count,
             detail, now, duration, log_id),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def get_logs(job_name: str = None, limit: int = 100, offset: int = 0) -> tuple[list[dict], int]:
    """查询执行日志，返回 (列表, 总数)"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        create_scheduler_log_table()
        where = ""
        params = []
        if job_name:
            where = "WHERE job_name = %s"
            params.append(job_name)

        cursor.execute(f"SELECT COUNT(*) AS cnt FROM scheduler_execution_log {where}", params)
        total = cursor.fetchone()['cnt']

        cursor.execute(
            f"SELECT * FROM scheduler_execution_log {where} ORDER BY id DESC LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        rows = list(cursor.fetchall())
        # datetime 转字符串
        for r in rows:
            for k in ('started_at', 'finished_at', 'created_at'):
                if r.get(k) and isinstance(r[k], datetime):
                    r[k] = r[k].strftime("%Y-%m-%d %H:%M:%S")
        return rows, total
    finally:
        cursor.close()
        conn.close()
