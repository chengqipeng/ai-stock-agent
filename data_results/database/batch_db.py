import sqlite3
import os
from typing import List, Dict
from datetime import datetime

DB_DIR = "data_results/sql_lite"
DB_PATH = os.path.join(DB_DIR, "stock_history.db")

def init_batch_tables():
    """初始化批次相关表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 批次表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS batch_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_name TEXT NOT NULL,
            total_count INTEGER NOT NULL,
            completed_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 批次股票记录表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS batch_stock_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            stock_name TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            prompt TEXT,
            result TEXT,
            reason TEXT,
            score INTEGER,
            technical_prompt TEXT,
            technical_result TEXT,
            technical_score INTEGER,
            technical_reason TEXT,
            error_message TEXT,
            status TEXT DEFAULT 'pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            completed_at DATETIME,
            FOREIGN KEY (batch_id) REFERENCES batch_records(id)
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_batch_id ON batch_stock_records(batch_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_batch_status ON batch_records(status)")
    
    # 添加error_message字段（如果不存在）
    try:
        cursor.execute("SELECT error_message FROM batch_stock_records LIMIT 1")
    except sqlite3.OperationalError:
        cursor.execute("ALTER TABLE batch_stock_records ADD COLUMN error_message TEXT")
    
    conn.commit()
    conn.close()

def create_batch(batch_name: str, total_count: int) -> int:
    """创建批次记录"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO batch_records (batch_name, total_count)
        VALUES (?, ?)
    """, (batch_name, total_count))
    batch_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return batch_id

def add_batch_stock(batch_id: int, stock_name: str, stock_code: str):
    """添加批次股票记录"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO batch_stock_records (batch_id, stock_name, stock_code)
        VALUES (?, ?, ?)
    """, (batch_id, stock_name, stock_code))
    conn.commit()
    conn.close()

def update_batch_stock(batch_id: int, stock_code: str, prompt: str, result: str, score: int, reason: str = "", technical_prompt: str = "", technical_result: str = "", technical_score: int = 0, technical_reason: str = "", error_message: str = "", is_deep_thinking: int = 0):
    """更新批次股票记录"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    status = 'failed' if error_message else 'completed'
    cursor.execute("""
        UPDATE batch_stock_records
        SET prompt = ?, result = ?, score = ?, reason = ?, technical_prompt = ?, technical_result = ?, technical_score = ?, technical_reason = ?, error_message = ?, is_deep_thinking = ?, status = ?, completed_at = ?
        WHERE batch_id = ? AND stock_code = ?
    """, (prompt, result, score, reason, technical_prompt, technical_result, technical_score, technical_reason, error_message, is_deep_thinking, status, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), batch_id, stock_code))
    
    # 更新批次完成数量
    cursor.execute("""
        UPDATE batch_records
        SET completed_count = (
            SELECT COUNT(*) FROM batch_stock_records
            WHERE batch_id = ? AND status = 'completed'
        )
        WHERE id = ?
    """, (batch_id, batch_id))
    
    # 检查是否全部完成
    cursor.execute("""
        SELECT total_count, completed_count FROM batch_records WHERE id = ?
    """, (batch_id,))
    row = cursor.fetchone()
    if row and row[0] == row[1]:
        cursor.execute("""
            UPDATE batch_records SET status = 'completed' WHERE id = ?
        """, (batch_id,))
    
    conn.commit()
    conn.close()

def get_all_batches() -> List[Dict]:
    """获取所有批次"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            br.id, 
            br.batch_name, 
            br.total_count, 
            br.completed_count,
            (SELECT COUNT(*) FROM batch_stock_records WHERE batch_id = br.id AND status = 'completed') as success_count,
            br.status, 
            br.created_at
        FROM batch_records br
        ORDER BY br.created_at DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_batch_stocks(batch_id: int) -> List[Dict]:
    """获取批次下的所有股票记录，按分数倒序"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, stock_name, stock_code, score, reason, technical_score, technical_reason, error_message, is_deep_thinking, status, completed_at
        FROM batch_stock_records
        WHERE batch_id = ?
        ORDER BY score DESC, completed_at DESC
    """, (batch_id,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_batch_stock_detail(stock_id: int) -> Dict:
    """获取批次股票详细信息"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM batch_stock_records WHERE id = ?
    """, (stock_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_batch_progress(batch_id: int) -> Dict:
    """获取批次进度"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT total_count, completed_count, status
        FROM batch_records
        WHERE id = ?
    """, (batch_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def clear_all_batches():
    """清空所有批次记录"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM batch_stock_records")
    cursor.execute("DELETE FROM batch_records")
    conn.commit()
    conn.close()
