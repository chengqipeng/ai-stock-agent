"""
技术面打分结果 DAO — 存储 batch_technical_score 的打分结果到 SQLite
"""
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")
_DB_PATH = Path(__file__).parent.parent / "data_results/sql_lite/batch_analysis.db"


def _open_conn(db_path: Path = None) -> sqlite3.Connection:
    db_path = db_path or _DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def create_technical_score_table(conn: sqlite3.Connection = None):
    """创建技术面打分结果表"""
    own_conn = conn is None
    if own_conn:
        conn = _open_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS technical_score (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_name TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            total_score INTEGER NOT NULL,
            macd_score INTEGER NOT NULL,
            macd_detail TEXT,
            kdj_score INTEGER NOT NULL,
            kdj_detail TEXT,
            vol_score INTEGER NOT NULL,
            vol_detail TEXT,
            trend_score INTEGER NOT NULL,
            trend_detail TEXT,
            close_price REAL,
            score_date TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(stock_code, score_date)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ts_code ON technical_score(stock_code)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ts_total ON technical_score(total_score DESC)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ts_date ON technical_score(score_date)
    """)
    conn.commit()
    if own_conn:
        conn.close()


def save_score_results(results: list[dict], db_path: Path = None):
    """
    批量保存打分结果到数据库

    Args:
        results: analyze_stock 返回的 dict 列表，每个包含:
            name, code, total, macd_score, macd_detail,
            kdj_score, kdj_detail, vol_score, vol_detail,
            trend_score, trend_detail, close, date
        db_path: 数据库路径，默认 batch_analysis.db
    """
    conn = _open_conn(db_path)
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
        INSERT OR REPLACE INTO technical_score
        (stock_name, stock_code, total_score,
         macd_score, macd_detail, kdj_score, kdj_detail,
         vol_score, vol_detail, trend_score, trend_detail,
         close_price, score_date, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    logger.info("已保存 %d 条打分结果到数据库", len(rows))


def get_qualified_scores(min_score: int = 50, score_date: str = None,
                         db_path: Path = None) -> list[dict]:
    """查询达标的打分结果"""
    conn = _open_conn(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    sql = "SELECT * FROM technical_score WHERE total_score >= ?"
    params: list = [min_score]
    if score_date:
        sql += " AND score_date = ?"
        params.append(score_date)
    sql += " ORDER BY total_score DESC"
    try:
        cursor.execute(sql, params)
        rows = [dict(r) for r in cursor.fetchall()]
    except sqlite3.OperationalError as e:
        logger.warning("查询打分结果失败: %s", e)
        rows = []
    finally:
        conn.close()
    return rows


def get_score_by_code(stock_code: str, db_path: Path = None) -> list[dict]:
    """查询某只股票的历史打分记录"""
    conn = _open_conn(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM technical_score WHERE stock_code = ? ORDER BY score_date DESC",
            (stock_code,),
        )
        rows = [dict(r) for r in cursor.fetchall()]
    except sqlite3.OperationalError as e:
        logger.warning("查询股票打分失败 [%s]: %s", stock_code, e)
        rows = []
    finally:
        conn.close()
    return rows


def get_latest_score_date(db_path: Path = None) -> str | None:
    """获取最新一次打分的日期"""
    conn = _open_conn(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(score_date) FROM technical_score")
        row = cursor.fetchone()
        return row[0] if row and row[0] else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()
