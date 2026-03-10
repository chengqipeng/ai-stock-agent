"""分时数据 DAO — stock_time_data 表"""
import logging
from dao import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_time_data"


def create_time_data_table(cursor=None):
    """创建分时数据表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(20) NOT NULL,
            trade_date VARCHAR(20) NOT NULL,
            `time` VARCHAR(10) NOT NULL,
            close_price DOUBLE,
            trading_amount DOUBLE,
            avg_price DOUBLE,
            trading_volume BIGINT,
            change_percent DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_date_time (stock_code, trade_date, `time`),
            INDEX idx_stock_code (stock_code),
            INDEX idx_trade_date (trade_date)
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


def batch_upsert_time_data(stock_code: str, trade_date: str, data_list: list[dict], cursor=None):
    """
    批量写入分时数据（ON DUPLICATE KEY UPDATE）。

    Args:
        stock_code: 股票代码
        trade_date: 交易日期 YYYY-MM-DD
        data_list: [{"time": "09:30", "close_price": ..., "trading_amount": ..., "avg_price": ..., "trading_volume": ..., "change_percent": ...}]
    """
    if not data_list:
        return 0

    sql = f"""
        INSERT INTO {TABLE_NAME}
            (stock_code, trade_date, `time`, close_price, trading_amount, avg_price, trading_volume, change_percent)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            close_price = VALUES(close_price),
            trading_amount = VALUES(trading_amount),
            avg_price = VALUES(avg_price),
            trading_volume = VALUES(trading_volume),
            change_percent = VALUES(change_percent)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    rows = [
        (stock_code, trade_date, d["time"], d.get("close_price"), d.get("trading_amount"),
         d.get("avg_price"), d.get("trading_volume"), d.get("change_percent"))
        for d in data_list
    ]
    cursor.executemany(sql, rows)
    count = cursor.rowcount

    if own:
        conn.commit()
        cursor.close()
        conn.close()

    return count


def get_time_data(stock_code: str, trade_date: str, cursor=None) -> list[dict]:
    """查询某只股票某天的分时数据"""
    sql = f"""
        SELECT `time`, close_price, trading_amount, avg_price, trading_volume, change_percent
        FROM {TABLE_NAME}
        WHERE stock_code = %s AND trade_date = %s
        ORDER BY `time`
    """
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (stock_code, trade_date))
    result = cursor.fetchall()
    if own:
        cursor.close()
        conn.close()
    return result
