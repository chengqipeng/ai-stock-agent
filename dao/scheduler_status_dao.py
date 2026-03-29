"""
调度任务状态持久化 DAO — scheduler_job_status 表

统一存储所有调度任务的状态和明细维度，替代各调度器的 JSON 文件持久化。

表结构：
  - job_id: 调度任务标识（如 kline, fund_flow, news, db_check 等）
  - dim_key: 维度标识（_main 表示主记录，其他如 kline, finance, time_data 等）
  - last_run_date: 最近执行日期
  - last_run_time: 最近执行时间
  - last_success: 是否成功
  - total: 总数
  - success: 成功数
  - failed: 失败数
  - skipped: 跳过数
  - extra_json: 扩展字段（JSON），存储各维度特有的数据
  - error: 错误信息
"""
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)
_CST = ZoneInfo("Asia/Shanghai")

TABLE = "scheduler_job_status"


def create_table():
    """创建调度状态表（幂等）"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                job_id VARCHAR(40) NOT NULL COMMENT '调度任务ID',
                dim_key VARCHAR(40) NOT NULL DEFAULT '_main' COMMENT '维度标识',
                last_run_date VARCHAR(20) COMMENT '最近执行日期',
                last_run_time VARCHAR(30) COMMENT '最近执行时间',
                last_success TINYINT COMMENT '是否成功 1/0',
                total INT DEFAULT 0,
                success INT DEFAULT 0,
                failed INT DEFAULT 0,
                skipped INT DEFAULT 0,
                extra_json TEXT COMMENT '扩展JSON数据',
                error TEXT COMMENT '错误信息',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_job_dim (job_id, dim_key),
                INDEX idx_job_id (job_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
    except Exception as e:
        logger.error("创建 %s 表失败: %s", TABLE, e)
    finally:
        cur.close()
        conn.close()


def save_job_status(job_id: str, dims: dict):
    """
    保存调度任务状态（upsert）。

    Args:
        job_id: 调度任务ID，如 'kline', 'news', 'db_check'
        dims: {
            '_main': {'last_run_date': '...', 'last_run_time': '...', 'last_success': True, 'error': None},
            'kline': {'total': 5000, 'success': 4998, 'failed': 2},
            'finance': {'total': 5000, 'success': 4990, 'failed': 10},
            ...
        }
        每个 dim 可包含: last_run_date, last_run_time, last_success, total, success, failed, skipped, extra_json, error
    """
    if not dims:
        return

    conn = get_connection()
    cur = conn.cursor()
    try:
        sql = f"""
            INSERT INTO {TABLE}
                (job_id, dim_key, last_run_date, last_run_time, last_success,
                 total, success, failed, skipped, extra_json, error)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_run_date=VALUES(last_run_date), last_run_time=VALUES(last_run_time),
                last_success=VALUES(last_success), total=VALUES(total),
                success=VALUES(success), failed=VALUES(failed), skipped=VALUES(skipped),
                extra_json=VALUES(extra_json), error=VALUES(error)
        """
        for dim_key, d in dims.items():
            extra = d.get("extra_json")
            if extra and not isinstance(extra, str):
                extra = json.dumps(extra, ensure_ascii=False)
            cur.execute(sql, (
                job_id, dim_key,
                d.get("last_run_date"), d.get("last_run_time"),
                1 if d.get("last_success") else (0 if d.get("last_success") is False else None),
                d.get("total", 0), d.get("success", 0),
                d.get("failed", 0), d.get("skipped", 0),
                extra, d.get("error"),
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("save_job_status(%s) 失败: %s", job_id, e)
    finally:
        cur.close()
        conn.close()


def load_job_status(job_id: str) -> dict:
    """
    加载调度任务状态。

    Returns:
        {
            '_main': {'last_run_date': '...', 'last_run_time': '...', 'last_success': True, ...},
            'kline': {'total': 5000, 'success': 4998, 'failed': 2, ...},
            ...
        }
        如果无记录返回空 dict。
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT dim_key, last_run_date, last_run_time, last_success, "
            f"total, success, failed, skipped, extra_json, error "
            f"FROM {TABLE} WHERE job_id = %s",
            (job_id,),
        )
        result = {}
        for row in cur.fetchall():
            dk = row.pop("dim_key")
            # last_success: 1->True, 0->False, None->None
            ls = row.get("last_success")
            row["last_success"] = True if ls == 1 else (False if ls == 0 else None)
            # 解析 extra_json
            ej = row.get("extra_json")
            if ej:
                try:
                    row["extra_json"] = json.loads(ej)
                except (json.JSONDecodeError, TypeError):
                    pass
            result[dk] = row
        return result
    except Exception as e:
        logger.error("load_job_status(%s) 失败: %s", job_id, e)
        return {}
    finally:
        cur.close()
        conn.close()


def load_all_job_status() -> dict:
    """
    加载所有调度任务状态。

    Returns:
        {
            'kline': {'_main': {...}, 'kline': {...}, 'finance': {...}},
            'news': {'_main': {...}, ...},
            ...
        }
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT job_id, dim_key, last_run_date, last_run_time, last_success, "
            f"total, success, failed, skipped, extra_json, error "
            f"FROM {TABLE} ORDER BY job_id, dim_key"
        )
        result = {}
        for row in cur.fetchall():
            jid = row.pop("job_id")
            dk = row.pop("dim_key")
            ls = row.get("last_success")
            row["last_success"] = True if ls == 1 else (False if ls == 0 else None)
            ej = row.get("extra_json")
            if ej:
                try:
                    row["extra_json"] = json.loads(ej)
                except (json.JSONDecodeError, TypeError):
                    pass
            result.setdefault(jid, {})[dk] = row
        return result
    except Exception as e:
        logger.error("load_all_job_status 失败: %s", e)
        return {}
    finally:
        cur.close()
        conn.close()
