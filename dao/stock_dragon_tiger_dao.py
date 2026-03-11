"""龙虎榜数据 DAO — stock_dragon_tiger 表"""
import logging
from dao import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_dragon_tiger"


def create_dragon_tiger_table(cursor=None):
    """创建龙虎榜数据表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            trade_date VARCHAR(20) NOT NULL,
            stock_code VARCHAR(20) NOT NULL,
            stock_name VARCHAR(100),
            reason VARCHAR(500),
            turnover VARCHAR(50),
            buy_amount VARCHAR(50),
            sell_amount VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_date_code (trade_date, stock_code),
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


def batch_upsert_dragon_tiger(trade_date: str, data_list: list[dict], cursor=None):
    """
    批量写入龙虎榜数据。

    Args:
        trade_date: 交易日期 YYYY-MM-DD
        data_list: [{"stock_code", "stock_name", "reason", "turnover", "buy_amount", "sell_amount"}]
    """
    if not data_list:
        return 0

    sql = f"""
        INSERT INTO {TABLE_NAME}
            (trade_date, stock_code, stock_name, reason, turnover, buy_amount, sell_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            stock_name=VALUES(stock_name), reason=VALUES(reason),
            turnover=VALUES(turnover), buy_amount=VALUES(buy_amount),
            sell_amount=VALUES(sell_amount)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    rows = [
        (trade_date, d.get("stock_code", ""), d.get("stock_name", ""),
         d.get("reason", ""), d.get("turnover", ""),
         d.get("buy_amount", ""), d.get("sell_amount", ""))
        for d in data_list
    ]
    cursor.executemany(sql, rows)
    count = cursor.rowcount

    if own:
        conn.commit()
        cursor.close()
        conn.close()

    return count


def get_dragon_tiger_by_date(trade_date: str, cursor=None) -> list[dict]:
    """查询某天的龙虎榜数据"""
    sql = f"SELECT * FROM {TABLE_NAME} WHERE trade_date = %s ORDER BY id"
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (trade_date,))
    result = cursor.fetchall()
    if own:
        cursor.close()
        conn.close()
    return result


def has_dragon_tiger(trade_date: str) -> bool:
    """检查某天是否已有龙虎榜数据"""
    sql = f"SELECT 1 FROM {TABLE_NAME} WHERE trade_date = %s LIMIT 1"
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, (trade_date,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        conn.close()

