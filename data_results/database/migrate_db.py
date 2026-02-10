import sqlite3
import os

DB_PATH = "data_results/sql_lite/stock_history.db"

def migrate_database():
    """迁移数据库：从文件存储迁移到内容存储"""
    if not os.path.exists(DB_PATH):
        print("数据库不存在，无需迁移")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 检查是否有旧表结构
    cursor.execute("PRAGMA table_info(analysis_history)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'content' in columns:
        print("数据库已是新结构，无需迁移")
        conn.close()
        return
    
    print("开始迁移数据库...")
    
    # 创建新表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analysis_history_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_type TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 迁移数据（从文件读取内容）
    cursor.execute("SELECT id, analysis_type, stock_name, stock_code, timestamp, file_path FROM analysis_history")
    rows = cursor.fetchall()
    
    migrated = 0
    for row in rows:
        record_id, analysis_type, stock_name, stock_code, timestamp, file_path = row
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                cursor.execute("""
                    INSERT INTO analysis_history_new (analysis_type, stock_name, stock_code, timestamp, content)
                    VALUES (?, ?, ?, ?, ?)
                """, (analysis_type, stock_name, stock_code, timestamp, content))
                migrated += 1
        except Exception as e:
            print(f"迁移记录 {record_id} 失败: {e}")
    
    # 删除旧表，重命名新表
    cursor.execute("DROP TABLE analysis_history")
    cursor.execute("ALTER TABLE analysis_history_new RENAME TO analysis_history")
    
    # 创建索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON analysis_history(timestamp DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_name ON analysis_history(stock_name)")
    
    conn.commit()
    conn.close()
    
    print(f"迁移完成！成功迁移 {migrated} 条记录")

if __name__ == "__main__":
    migrate_database()
