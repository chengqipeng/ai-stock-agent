"""数据库初始化"""
from database.models import db_manager

if __name__ == "__main__":
    print("初始化数据库...")
    db_manager.init_database()
    print("数据库初始化完成！")