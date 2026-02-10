#!/usr/bin/env python3
"""添加深度思考字段到数据库"""
import sqlite3
import os

DB_DIR = "data_results/sql_lite"
DB_PATH = os.path.join(DB_DIR, "stock_history.db")

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("ALTER TABLE batch_stock_records ADD COLUMN is_deep_thinking INTEGER DEFAULT 0")
        print("✓ 添加 is_deep_thinking 字段")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("- is_deep_thinking 字段已存在")
        else:
            print(f"✗ 添加 is_deep_thinking 字段失败: {e}")
    
    conn.commit()
    conn.close()
    print("\n数据库迁移完成！")

if __name__ == "__main__":
    migrate()
