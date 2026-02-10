#!/usr/bin/env python3
"""数据库迁移脚本 - 添加新字段"""
import sqlite3
import os

DB_DIR = "data_results/sql_lite"
DB_PATH = os.path.join(DB_DIR, "stock_history.db")

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 检查并添加reason字段
    try:
        cursor.execute("ALTER TABLE batch_stock_records ADD COLUMN reason TEXT")
        print("✓ 添加 reason 字段")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("- reason 字段已存在")
        else:
            print(f"✗ 添加 reason 字段失败: {e}")
    
    # 检查并添加technical_prompt字段
    try:
        cursor.execute("ALTER TABLE batch_stock_records ADD COLUMN technical_prompt TEXT")
        print("✓ 添加 technical_prompt 字段")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("- technical_prompt 字段已存在")
        else:
            print(f"✗ 添加 technical_prompt 字段失败: {e}")
    
    # 检查并添加technical_result字段
    try:
        cursor.execute("ALTER TABLE batch_stock_records ADD COLUMN technical_result TEXT")
        print("✓ 添加 technical_result 字段")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("- technical_result 字段已存在")
        else:
            print(f"✗ 添加 technical_result 字段失败: {e}")
    
    # 检查并添加technical_score字段
    try:
        cursor.execute("ALTER TABLE batch_stock_records ADD COLUMN technical_score INTEGER")
        print("✓ 添加 technical_score 字段")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("- technical_score 字段已存在")
        else:
            print(f"✗ 添加 technical_score 字段失败: {e}")
    
    # 检查并添加technical_reason字段
    try:
        cursor.execute("ALTER TABLE batch_stock_records ADD COLUMN technical_reason TEXT")
        print("✓ 添加 technical_reason 字段")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("- technical_reason 字段已存在")
        else:
            print(f"✗ 添加 technical_reason 字段失败: {e}")
    
    conn.commit()
    conn.close()
    print("\n数据库迁移完成！")

if __name__ == "__main__":
    migrate()
