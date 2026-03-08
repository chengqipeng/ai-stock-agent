"""
技术面打分结果 DAO — MySQL 版
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")

# 布林线反弹相关列（用于自动迁移）
_BOLL_COLUMNS = [
    ("boll_score", "INT DEFAULT 0"),
    ("boll_signal", "TINYINT DEFAULT 0"),
    ("boll_detail", "TEXT"),
    ("mid_bounce_score", "INT DEFAULT 0"),
    ("mid_bounce_signal", "TINYINT DEFAULT 0"),
    ("mid_bounce_detail", "TEXT"),
]


def create_technical_score_table(conn=None):
    """创建技术面打分结果表（兼容旧表自动迁移）"""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_batch_technical_score (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id INT NOT NULL,
            stock_name VARCHAR(100) NOT NULL,
            stock_code VARCHAR(20) NOT NULL,
            total_score INT NOT NULL,
            macd_score INT NOT NULL,
            macd_detail TEXT,
            kdj_score INT NOT NULL,
            kdj_detail TEXT,
            vol_score INT NOT NULL,
            vol_detail TEXT,
            trend_score INT NOT NULL,
            trend_detail TEXT,
            boll_score INT DEFAULT 0,
            boll_signal TINYINT DEFAULT 0,
            boll_detail TEXT,
            mid_bounce_score INT DEFAULT 0,
            mid_bounce_signal TINYINT DEFAULT 0,
            mid_bounce_detail TEXT,
            close_price DOUBLE,
            score_date VARCHAR(20),
            created_at VARCHAR(30) NOT NULL,
            UNIQUE KEY uk_batch_code_date (batch_id, stock_code, score_date),
            INDEX idx_ts_batch (batch_id),
            INDEX idx_ts_code (stock_code),
            INDEX idx_ts_total (total_score),
            INDEX idx_ts_date (score_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    # 兼容旧表：自动迁移缺失列
    try:
        cursor.execute("SELECT batch_id FROM stock_batch_technical_score LIMIT 1")
    except Exception:
        conn.rollback()
        logger.info("检测到旧表缺少 batch_id 列，开始迁移...")
        cursor.execute("ALTER TABLE stock_batch_technical_score ADD COLUMN batch_id INT NOT NULL DEFAULT 0 AFTER id")
        try:
            cursor.execute("ALTER TABLE stock_batch_technical_score DROP INDEX uk_code_date")
        except Exception:
            conn.rollback()
        try:
            cursor.execute("ALTER TABLE stock_batch_technical_score ADD UNIQUE KEY uk_batch_code_date (batch_id, stock_code, score_date)")
        except Exception:
            conn.rollback()
        try:
            cursor.execute("ALTER TABLE stock_batch_technical_score ADD INDEX idx_ts_batch (batch_id)")
        except Exception:
            conn.rollback()
        logger.info("旧表迁移完成，已添加 batch_id 列")

    # 兼容旧表：自动添加布林线反弹列
    for col_name, col_def in _BOLL_COLUMNS:
        try:
            cursor.execute(f"SELECT {col_name} FROM stock_batch_technical_score LIMIT 1")
        except Exception:
            conn.rollback()
            logger.info("添加缺失列 %s ...", col_name)
            cursor.execute(f"ALTER TABLE stock_batch_technical_score ADD COLUMN {col_name} {col_def}")

    conn.commit()
    if own_conn:
        cursor.close()
        conn.close()
    else:
        cursor.close()


def save_score_results(results: list[dict], batch_id: int):
    """批量保存打分结果到数据库（带批次号、含布林线反弹数据）"""
    conn = get_connection()
    create_technical_score_table(conn)
    now = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    cursor = conn.cursor()
    rows = [
        (
            batch_id,
            r["name"], r["code"], r["total"],
            r["macd_score"], r["macd_detail"],
            r["kdj_score"], r["kdj_detail"],
            r["vol_score"], r["vol_detail"],
            r["trend_score"], r["trend_detail"],
            r.get("boll_score", 0), 1 if r.get("boll_signal") else 0, r.get("boll_detail", ""),
            r.get("mid_bounce_score", 0), 1 if r.get("mid_bounce_signal") else 0, r.get("mid_bounce_detail", ""),
            r.get("close"), r.get("date"),
            now,
        )
        for r in results
    ]
    cursor.executemany("""
        INSERT INTO stock_batch_technical_score
        (batch_id, stock_name, stock_code, total_score,
         macd_score, macd_detail, kdj_score, kdj_detail,
         vol_score, vol_detail, trend_score, trend_detail,
         boll_score, boll_signal, boll_detail,
         mid_bounce_score, mid_bounce_signal, mid_bounce_detail,
         close_price, score_date, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            stock_name=VALUES(stock_name), total_score=VALUES(total_score),
            macd_score=VALUES(macd_score), macd_detail=VALUES(macd_detail),
            kdj_score=VALUES(kdj_score), kdj_detail=VALUES(kdj_detail),
            vol_score=VALUES(vol_score), vol_detail=VALUES(vol_detail),
            trend_score=VALUES(trend_score), trend_detail=VALUES(trend_detail),
            boll_score=VALUES(boll_score), boll_signal=VALUES(boll_signal), boll_detail=VALUES(boll_detail),
            mid_bounce_score=VALUES(mid_bounce_score), mid_bounce_signal=VALUES(mid_bounce_signal), mid_bounce_detail=VALUES(mid_bounce_detail),
            close_price=VALUES(close_price), created_at=VALUES(created_at)
    """, rows)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("已保存 %d 条打分结果到数据库 (batch_id=%d)", len(rows), batch_id)


def get_continuous_analysis_batches() -> list[dict]:
    """获取所有标记为持续分析的批次"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM stock_batch_list_info WHERE is_continuous_analysis = 1 ORDER BY sort_order ASC, id ASC"
        )
        return list(cursor.fetchall())
    finally:
        cursor.close()
        conn.close()


def get_batch_stock_list(batch_id: int) -> list[dict]:
    """获取批次中的股票列表（stock_code, stock_name）"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT stock_code, stock_name FROM stock_analysis_detail WHERE batch_id = %s",
            (batch_id,),
        )
        return list(cursor.fetchall())
    finally:
        cursor.close()
        conn.close()


def get_qualified_scores(min_score: int = 50, score_date: str = None,
                         batch_id: int = None) -> list[dict]:
    """查询达标的打分结果"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    sql = "SELECT * FROM stock_batch_technical_score WHERE total_score >= %s"
    params: list = [min_score]
    if batch_id:
        sql += " AND batch_id = %s"
        params.append(batch_id)
    if score_date:
        sql += " AND score_date = %s"
        params.append(score_date)
    sql += " ORDER BY total_score DESC"
    try:
        cursor.execute(sql, params)
        rows = list(cursor.fetchall())
    except Exception as e:
        logger.warning("查询打分结果失败: %s", e)
        rows = []
    finally:
        cursor.close()
        conn.close()
    return rows


def get_score_by_code(stock_code: str) -> list[dict]:
    """查询某只股票的历史打分记录"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM stock_batch_technical_score WHERE stock_code = %s ORDER BY score_date DESC",
            (stock_code,),
        )
        rows = list(cursor.fetchall())
    except Exception as e:
        logger.warning("查询股票打分失败 [%s]: %s", stock_code, e)
        rows = []
    finally:
        cursor.close()
        conn.close()
    return rows


def get_latest_score_date() -> str | None:
    """获取最新一次打分的日期"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(score_date) FROM stock_batch_technical_score")
        row = cursor.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None
    finally:
        cursor.close()
        conn.close()


def get_latest_technical_scores_for_batch(batch_id: int) -> dict:
    """获取批次中每只股票的最新一次技术打分，返回 {stock_code: row_dict}"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT t.* FROM stock_batch_technical_score t
            INNER JOIN (
                SELECT stock_code, MAX(score_date) AS max_date
                FROM stock_batch_technical_score
                WHERE batch_id = %s
                GROUP BY stock_code
            ) latest ON t.stock_code = latest.stock_code AND t.score_date = latest.max_date
            WHERE t.batch_id = %s
        """, (batch_id, batch_id))
        rows = cursor.fetchall()
        return {r['stock_code']: r for r in rows}
    except Exception as e:
        logger.warning("查询批次最新技术打分失败 [batch_id=%s]: %s", batch_id, e)
        return {}
    finally:
        cursor.close()
        conn.close()


def get_technical_score_history(batch_id: int, stock_code: str) -> list[dict]:
    """获取某只股票在某批次下的所有技术打分记录，按时间倒序"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM stock_batch_technical_score WHERE batch_id = %s AND stock_code = %s ORDER BY score_date DESC",
            (batch_id, stock_code),
        )
        return list(cursor.fetchall())
    except Exception as e:
        logger.warning("查询技术打分历史失败 [batch_id=%s, %s]: %s", batch_id, stock_code, e)
        return []
    finally:
        cursor.close()
        conn.close()

def get_today_scored_codes(batch_id: int) -> set[str]:
    """获取今日已完成技术打分的股票代码集合（按 created_at 判断）"""
    today_str = datetime.now(_CST).strftime("%Y-%m-%d")
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT DISTINCT stock_code FROM stock_batch_technical_score "
            "WHERE batch_id = %s AND created_at >= %s",
            (batch_id, today_str),
        )
        return {row['stock_code'] for row in cursor.fetchall()}
    except Exception as e:
        logger.warning("查询今日已打分股票失败 [batch_id=%s]: %s", batch_id, e)
        return set()
    finally:
        cursor.close()
        conn.close()

