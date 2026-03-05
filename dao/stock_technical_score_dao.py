"""
技术面打分结果 DAO — MySQL 版
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")


def create_technical_score_table(conn=None):
    """创建技术面打分结果表"""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_batch_technical_score (
            id INT AUTO_INCREMENT PRIMARY KEY,
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
            close_price DOUBLE,
            score_date VARCHAR(20),
            created_at VARCHAR(30) NOT NULL,
            UNIQUE KEY uk_code_date (stock_code, score_date),
            INDEX idx_ts_code (stock_code),
            INDEX idx_ts_total (total_score),
            INDEX idx_ts_date (score_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    conn.commit()
    if own_conn:
        cursor.close()
        conn.close()
    else:
        cursor.close()


def save_score_results(results: list[dict]):
    """批量保存打分结果到数据库"""
    conn = get_connection()
    create_technical_score_table(conn)
    now = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    cursor = conn.cursor()
    rows = [
        (
            r["name"], r["code"], r["total"],
            r["macd_score"], r["macd_detail"],
            r["kdj_score"], r["kdj_detail"],
            r["vol_score"], r["vol_detail"],
            r["trend_score"], r["trend_detail"],
            r.get("close"), r.get("date"),
            now,
        )
        for r in results
    ]
    cursor.executemany("""
        INSERT INTO stock_batch_technical_score
        (stock_name, stock_code, total_score,
         macd_score, macd_detail, kdj_score, kdj_detail,
         vol_score, vol_detail, trend_score, trend_detail,
         close_price, score_date, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            stock_name=VALUES(stock_name), total_score=VALUES(total_score),
            macd_score=VALUES(macd_score), macd_detail=VALUES(macd_detail),
            kdj_score=VALUES(kdj_score), kdj_detail=VALUES(kdj_detail),
            vol_score=VALUES(vol_score), vol_detail=VALUES(vol_detail),
            trend_score=VALUES(trend_score), trend_detail=VALUES(trend_detail),
            close_price=VALUES(close_price), created_at=VALUES(created_at)
    """, rows)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("已保存 %d 条打分结果到数据库", len(rows))


def get_qualified_scores(min_score: int = 50, score_date: str = None) -> list[dict]:
    """查询达标的打分结果"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    sql = "SELECT * FROM stock_batch_technical_score WHERE total_score >= %s"
    params: list = [min_score]
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
