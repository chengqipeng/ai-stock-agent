"""盘口数据 DAO — stock_order_book 表"""
import logging
from dao import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_order_book"


def create_order_book_table(cursor=None):
    """创建盘口数据表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(20) NOT NULL,
            trade_date VARCHAR(20) NOT NULL,
            current_price DOUBLE,
            open_price DOUBLE,
            prev_close DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            volume BIGINT COMMENT '成交量（手）',
            amount VARCHAR(50) COMMENT '成交额',
            buy1_price DOUBLE, buy1_vol INT,
            buy2_price DOUBLE, buy2_vol INT,
            buy3_price DOUBLE, buy3_vol INT,
            buy4_price DOUBLE, buy4_vol INT,
            buy5_price DOUBLE, buy5_vol INT,
            sell1_price DOUBLE, sell1_vol INT,
            sell2_price DOUBLE, sell2_vol INT,
            sell3_price DOUBLE, sell3_vol INT,
            sell4_price DOUBLE, sell4_vol INT,
            sell5_price DOUBLE, sell5_vol INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_date (stock_code, trade_date),
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


def upsert_order_book(stock_code: str, trade_date: str, data: dict, cursor=None):
    """
    写入或更新盘口数据。

    Args:
        stock_code: 股票代码
        trade_date: 交易日期 YYYY-MM-DD
        data: 盘口数据字典
    """
    sql = f"""
        INSERT INTO {TABLE_NAME}
            (stock_code, trade_date, current_price, open_price, prev_close, high_price, low_price,
             volume, amount,
             buy1_price, buy1_vol, buy2_price, buy2_vol, buy3_price, buy3_vol,
             buy4_price, buy4_vol, buy5_price, buy5_vol,
             sell1_price, sell1_vol, sell2_price, sell2_vol, sell3_price, sell3_vol,
             sell4_price, sell4_vol, sell5_price, sell5_vol)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            current_price=VALUES(current_price), open_price=VALUES(open_price),
            prev_close=VALUES(prev_close), high_price=VALUES(high_price), low_price=VALUES(low_price),
            volume=VALUES(volume), amount=VALUES(amount),
            buy1_price=VALUES(buy1_price), buy1_vol=VALUES(buy1_vol),
            buy2_price=VALUES(buy2_price), buy2_vol=VALUES(buy2_vol),
            buy3_price=VALUES(buy3_price), buy3_vol=VALUES(buy3_vol),
            buy4_price=VALUES(buy4_price), buy4_vol=VALUES(buy4_vol),
            buy5_price=VALUES(buy5_price), buy5_vol=VALUES(buy5_vol),
            sell1_price=VALUES(sell1_price), sell1_vol=VALUES(sell1_vol),
            sell2_price=VALUES(sell2_price), sell2_vol=VALUES(sell2_vol),
            sell3_price=VALUES(sell3_price), sell3_vol=VALUES(sell3_vol),
            sell4_price=VALUES(sell4_price), sell4_vol=VALUES(sell4_vol),
            sell5_price=VALUES(sell5_price), sell5_vol=VALUES(sell5_vol)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    cursor.execute(sql, (
        stock_code, trade_date,
        data.get("current_price"), data.get("open_price"), data.get("prev_close"),
        data.get("high_price"), data.get("low_price"),
        data.get("volume"), data.get("amount"),
        data.get("buy1_price"), data.get("buy1_vol"),
        data.get("buy2_price"), data.get("buy2_vol"),
        data.get("buy3_price"), data.get("buy3_vol"),
        data.get("buy4_price"), data.get("buy4_vol"),
        data.get("buy5_price"), data.get("buy5_vol"),
        data.get("sell1_price"), data.get("sell1_vol"),
        data.get("sell2_price"), data.get("sell2_vol"),
        data.get("sell3_price"), data.get("sell3_vol"),
        data.get("sell4_price"), data.get("sell4_vol"),
        data.get("sell5_price"), data.get("sell5_vol"),
    ))

    if own:
        conn.commit()
        cursor.close()
        conn.close()


def get_order_book(stock_code: str, trade_date: str, cursor=None) -> dict | None:
    """查询某只股票某天的盘口数据"""
    sql = f"SELECT * FROM {TABLE_NAME} WHERE stock_code = %s AND trade_date = %s"
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (stock_code, trade_date))
    result = cursor.fetchone()
    if own:
        cursor.close()
        conn.close()
    return result


def has_order_book(stock_code: str, trade_date: str) -> bool:
    """检查某只股票某天是否已有有效的盘口数据（prev_close > 0 表示数据有效）"""
    sql = (f"SELECT 1 FROM {TABLE_NAME} "
           f"WHERE stock_code = %s AND trade_date = %s AND prev_close > 0 LIMIT 1")
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, (stock_code, trade_date))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        conn.close()

