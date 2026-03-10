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
            `rank` INT,
            stock_code VARCHAR(20) NOT NULL,
            stock_name VARCHAR(100),
            close_price VARCHAR(50),
            change_pct VARCHAR(50),
            net_buy_amount VARCHAR(50),
            buy_amount VARCHAR(50),
            sell_amount VARCHAR(50),
            lhb_turnover VARCHAR(50),
            market_turnover VARCHAR(50),
            net_buy_ratio VARCHAR(50),
            turnover_ratio VARCHAR(50),
            turnover_rate VARCHAR(50),
            circulating_market_cap VARCHAR(50),
            reason VARCHAR(500),
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
        data_list: 龙虎榜数据列表
    """
    if not data_list:
        return 0

    sql = f"""
        INSERT INTO {TABLE_NAME}
            (trade_date, `rank`, stock_code, stock_name, close_price, change_pct,
             net_buy_amount, buy_amount, sell_amount, lhb_turnover, market_turnover,
             net_buy_ratio, turnover_ratio, turnover_rate, circulating_market_cap, reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `rank`=VALUES(`rank`), stock_name=VALUES(stock_name),
            close_price=VALUES(close_price), change_pct=VALUES(change_pct),
            net_buy_amount=VALUES(net_buy_amount), buy_amount=VALUES(buy_amount),
            sell_amount=VALUES(sell_amount), lhb_turnover=VALUES(lhb_turnover),
            market_turnover=VALUES(market_turnover), net_buy_ratio=VALUES(net_buy_ratio),
            turnover_ratio=VALUES(turnover_ratio), turnover_rate=VALUES(turnover_rate),
            circulating_market_cap=VALUES(circulating_market_cap), reason=VALUES(reason)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    def _rank(v):
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0

    rows = [
        (trade_date, _rank(d.get("rank")), d.get("stock_code", ""), d.get("stock_name", ""),
         d.get("close_price", ""), d.get("change_pct", ""),
         d.get("net_buy_amount", ""), d.get("buy_amount", ""), d.get("sell_amount", ""),
         d.get("lhb_turnover", ""), d.get("market_turnover", ""),
         d.get("net_buy_ratio", ""), d.get("turnover_ratio", ""),
         d.get("turnover_rate", ""), d.get("circulating_market_cap", ""), d.get("reason", ""))
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
    sql = f"SELECT * FROM {TABLE_NAME} WHERE trade_date = %s ORDER BY `rank`"
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
