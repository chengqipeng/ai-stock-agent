"""大单追踪数据 DAO — stock_big_order 表"""
import logging
from dao import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_big_order"


def create_big_order_table(cursor=None):
    """创建大单追踪数据表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            trade_date VARCHAR(20) NOT NULL COMMENT '交易日期',
            `time` VARCHAR(20) COMMENT '成交时间',
            stock_code VARCHAR(20) NOT NULL COMMENT '股票代码(6位)',
            stock_name VARCHAR(100) COMMENT '股票名称',
            price VARCHAR(50) COMMENT '成交价',
            volume VARCHAR(50) COMMENT '成交量(手)',
            amount VARCHAR(50) COMMENT '成交额(万)',
            direction VARCHAR(20) COMMENT '买卖方向: 买入/卖出',
            change_pct VARCHAR(50) COMMENT '涨跌幅',
            turnover_rate VARCHAR(50) COMMENT '换手率',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_trade_date (trade_date),
            INDEX idx_stock_code (stock_code),
            INDEX idx_date_code (trade_date, stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(ddl)
    if own:
        conn.commit()
        cursor.close()
        conn.close()


def batch_insert_big_orders(trade_date: str, data_list: list[dict], cursor=None):
    """
    批量写入大单追踪数据。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        data_list: fetch_fund_flow_all_pages("ddzz") 返回的原始数据列表
    """
    if not data_list:
        return 0

    sql = f"""
        INSERT INTO {TABLE_NAME}
            (trade_date, `time`, stock_code, stock_name, price, volume,
             amount, direction, change_pct, turnover_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    rows = [
        (trade_date, d.get("time", ""), d.get("stock_code", ""),
         d.get("stock_name", ""), d.get("price", ""),
         d.get("volume", ""), d.get("amount", ""),
         d.get("direction", ""), d.get("change_pct", ""),
         d.get("turnover_rate", ""))
        for d in data_list
    ]
    cursor.executemany(sql, rows)
    count = cursor.rowcount

    if own:
        conn.commit()
        cursor.close()
        conn.close()

    return count


def get_big_orders_by_stock(stock_code: str, limit: int = 100, cursor=None) -> list[dict]:
    """
    查询某只股票的大单追踪数据（按日期+时间倒序）。
    stock_code: 6位纯数字代码（如 600519）
    """
    sql = (f"SELECT trade_date, `time`, stock_code, stock_name, price, volume, "
           f"amount, direction, change_pct, turnover_rate "
           f"FROM {TABLE_NAME} WHERE stock_code = %s "
           f"ORDER BY trade_date DESC, `time` DESC LIMIT %s")
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (stock_code, limit))
    result = cursor.fetchall()
    if own:
        cursor.close()
        conn.close()
    return result


def has_big_orders(trade_date: str) -> bool:
    """检查某天是否已有大单追踪数据"""
    sql = f"SELECT 1 FROM {TABLE_NAME} WHERE trade_date = %s LIMIT 1"
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, (trade_date,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        conn.close()


def get_big_orders_by_date(trade_date: str, limit: int = 200, cursor=None) -> list[dict]:
    """查询某天的大单追踪数据"""
    sql = (f"SELECT trade_date, `time`, stock_code, stock_name, price, volume, "
           f"amount, direction, change_pct, turnover_rate "
           f"FROM {TABLE_NAME} WHERE trade_date = %s "
           f"ORDER BY `time` DESC LIMIT %s")
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (trade_date, limit))
    result = cursor.fetchall()
    if own:
        cursor.close()
        conn.close()
    return result
