"""
DAO 层公共 MySQL 连接管理 —— 基于 DBUtils.PooledDB 连接池

连接池会复用空闲连接，避免高并发场景下频繁创建/销毁连接导致
MySQL 报 "Lost connection to MySQL server during query" 错误。
"""
import logging
import pymysql
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB

logger = logging.getLogger(__name__)

_MYSQL_CONFIG = {
    "host": "106.14.194.144",
    "port": 3306,
    "user": "root",
    "password": "MySql@888888",
    "database": "stock_db",
    "charset": "utf8mb4",
    "autocommit": False,
}

# ── 连接池（全局单例） ──
# mincached  : 启动时预建的空闲连接数
# maxcached  : 池中最多保留的空闲连接数
# maxconnections : 允许的最大连接数（0 = 不限制）
# blocking   : 达到上限时阻塞等待，而非抛异常
_pool = PooledDB(
    creator=pymysql,
    mincached=2,
    maxcached=5,
    maxconnections=10,
    blocking=True,
    **_MYSQL_CONFIG,
)

# DictCursor 版本的池（查询需要返回 dict 时使用）
_pool_dict = PooledDB(
    creator=pymysql,
    mincached=1,
    maxcached=3,
    maxconnections=5,
    blocking=True,
    cursorclass=DictCursor,
    **_MYSQL_CONFIG,
)


def get_connection(use_dict_cursor: bool = False) -> pymysql.connections.Connection:
    """从连接池获取 MySQL 连接。

    用法与之前完全一致：
        conn = get_connection()
        ...
        conn.close()   # 归还到池，而非真正关闭

    返回的连接对象调用 close() 时会自动归还连接池，不会真正断开。
    """
    pool = _pool_dict if use_dict_cursor else _pool
    return pool.connection()
