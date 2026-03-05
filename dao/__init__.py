"""
DAO 层公共 MySQL 连接管理
"""
import pymysql
from pymysql.cursors import DictCursor

_MYSQL_CONFIG = {
    "host": "106.14.194.144",
    "port": 3306,
    "user": "root",
    "password": "MySql@888888",
    "database": "stock_db",
    "charset": "utf8mb4",
    "autocommit": False,
}


def get_connection(use_dict_cursor: bool = False) -> pymysql.connections.Connection:
    """获取 MySQL 连接"""
    cursor_class = DictCursor if use_dict_cursor else pymysql.cursors.Cursor
    return pymysql.connect(**_MYSQL_CONFIG, cursorclass=cursor_class)
