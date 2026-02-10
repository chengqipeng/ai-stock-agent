import sqlite3
import os
from typing import List, Dict

DB_DIR = "data_results/sql_lite"
DB_PATH = os.path.join(DB_DIR, "stock_history.db")

def init_db():
    """初始化数据库"""
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analysis_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_type TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON analysis_history(timestamp DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_name ON analysis_history(stock_name)")
    conn.commit()
    conn.close()

def insert_history(analysis_type: str, stock_name: str, stock_code: str, timestamp: str, content: str):
    """插入历史记录"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO analysis_history (analysis_type, stock_name, stock_code, timestamp, content)
        VALUES (?, ?, ?, ?, ?)
    """, (analysis_type, stock_name, stock_code, timestamp, content))
    conn.commit()
    conn.close()

def get_all_history() -> List[Dict]:
    """获取所有历史记录"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, analysis_type, stock_name, stock_code, timestamp, LENGTH(content) as content_size
        FROM analysis_history
        ORDER BY timestamp DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_history_content(history_id: int) -> str:
    """根据ID获取历史记录内容"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT content FROM analysis_history WHERE id = ?", (history_id,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None
