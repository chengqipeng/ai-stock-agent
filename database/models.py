"""数据库模型定义"""
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any
import json


class DatabaseManager:
    def __init__(self, db_path: str = "batch_analysis.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """初始化数据库表"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 批次信息表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS batch_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_name TEXT NOT NULL,
                    total_count INTEGER NOT NULL,
                    success_count INTEGER DEFAULT 0,
                    completed_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 明细信息表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_analysis_detail (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL,
                    
                    -- CAN SLIM各维度打分
                    c_score INTEGER,
                    c_prompt TEXT,
                    c_summary TEXT,
                    
                    a_score INTEGER,
                    a_prompt TEXT,
                    a_summary TEXT,
                    
                    n_score INTEGER,
                    n_prompt TEXT,
                    n_summary TEXT,
                    
                    s_score INTEGER,
                    s_prompt TEXT,
                    s_summary TEXT,
                    
                    l_score INTEGER,
                    l_prompt TEXT,
                    l_summary TEXT,
                    
                    i_score INTEGER,
                    i_prompt TEXT,
                    i_summary TEXT,
                    
                    m_score INTEGER,
                    m_prompt TEXT,
                    m_summary TEXT,
                    
                    -- 整体分析
                    overall_analysis TEXT,
                    
                    -- K线分析
                    kline_score INTEGER,
                    kline_prompt TEXT,
                    
                    -- 状态信息
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    is_deep_thinking INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    
                    FOREIGN KEY (batch_id) REFERENCES batch_info (id)
                )
            """)
            
            conn.commit()
    
    def create_batch(self, stock_codes: List[str]) -> int:
        """创建新批次"""
        batch_name = f"批次_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 创建批次记录
            cursor.execute("""
                INSERT INTO batch_info (batch_name, total_count)
                VALUES (?, ?)
            """, (batch_name, len(stock_codes)))
            
            batch_id = cursor.lastrowid
            
            # 创建股票明细记录
            for stock_code in stock_codes:
                # 解析股票代码和名称
                if " (" in stock_code and stock_code.endswith(")"):
                    stock_name = stock_code.split(" (")[0]
                    code = stock_code.split(" (")[1].rstrip(")")
                else:
                    stock_name = stock_code
                    code = stock_code
                
                cursor.execute("""
                    INSERT INTO stock_analysis_detail (batch_id, stock_code, stock_name)
                    VALUES (?, ?, ?)
                """, (batch_id, code, stock_name))
            
            conn.commit()
            return batch_id
    
    def get_batches(self) -> List[Dict[str, Any]]:
        """获取所有批次"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM batch_info 
                ORDER BY created_at DESC
            """)
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_batch_stocks(self, batch_id: int) -> List[Dict[str, Any]]:
        """获取批次中的股票列表"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM stock_analysis_detail 
                WHERE batch_id = ?
                ORDER BY id
            """, (batch_id,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_stock_detail(self, stock_id: int) -> Optional[Dict[str, Any]]:
        """获取股票详细信息"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM stock_analysis_detail 
                WHERE id = ?
            """, (stock_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def update_stock_dimension_score(self, stock_id: int, dimension: str, 
                                   score: int, prompt: str, summary: str = None):
        """更新股票维度打分"""
        dimension = dimension.lower()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute(f"""
                UPDATE stock_analysis_detail 
                SET {dimension}_score = ?, {dimension}_prompt = ?, {dimension}_summary = ?
                WHERE id = ?
            """, (score, prompt, summary, stock_id))
            
            conn.commit()
    
    def update_stock_kline(self, stock_id: int, score: int, prompt: str):
        """更新K线分析"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE stock_analysis_detail 
                SET kline_score = ?, kline_prompt = ?
                WHERE id = ?
            """, (score, prompt, stock_id))
            
            conn.commit()
    
    def update_stock_overall_analysis(self, stock_id: int, analysis: str):
        """更新整体分析"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE stock_analysis_detail 
                SET overall_analysis = ?
                WHERE id = ?
            """, (analysis, stock_id))
            
            conn.commit()
    
    def update_stock_status(self, stock_id: int, status: str, 
                          error_message: str = None, is_deep_thinking: bool = False):
        """更新股票状态"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            completed_at = datetime.now().isoformat() if status == 'completed' else None
            
            cursor.execute("""
                UPDATE stock_analysis_detail 
                SET status = ?, error_message = ?, is_deep_thinking = ?, completed_at = ?
                WHERE id = ?
            """, (status, error_message, 1 if is_deep_thinking else 0, completed_at, stock_id))
            
            conn.commit()
    
    def update_batch_progress(self, batch_id: int):
        """更新批次进度"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 统计完成情况
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as success,
                    SUM(CASE WHEN status IN ('completed', 'failed') THEN 1 ELSE 0 END) as completed
                FROM stock_analysis_detail 
                WHERE batch_id = ?
            """, (batch_id,))
            
            result = cursor.fetchone()
            total, success, completed = result
            
            # 更新批次状态
            batch_status = 'completed' if completed == total else 'processing'
            
            cursor.execute("""
                UPDATE batch_info 
                SET success_count = ?, completed_count = ?, status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (success, completed, batch_status, batch_id))
            
            conn.commit()
    
    def delete_batch(self, batch_id: int):
        """删除批次及其所有明细"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM stock_analysis_detail WHERE batch_id = ?", (batch_id,))
            cursor.execute("DELETE FROM batch_info WHERE id = ?", (batch_id,))
            
            conn.commit()
    
    def clear_all_batches(self):
        """清空所有批次数据"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM stock_analysis_detail")
            cursor.execute("DELETE FROM batch_info")
            
            conn.commit()


# 全局数据库管理器实例
db_manager = DatabaseManager()