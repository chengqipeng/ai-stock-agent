"""历史资金流向数据 DAO — stock_fund_flow 表"""
import logging
from dao import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_fund_flow"


def create_fund_flow_table(cursor=None):
    """创建历史资金流向数据表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(20) NOT NULL,
            `date` VARCHAR(20) NOT NULL,
            close_price DOUBLE,
            change_pct DOUBLE,
            net_flow DOUBLE COMMENT '资金净流入(万元)',
            main_net_5day DOUBLE COMMENT '5日主力净额(万元)',
            big_net DOUBLE COMMENT '大单(主力)净额(万元)',
            big_net_pct DOUBLE COMMENT '大单净占比(%)',
            mid_net DOUBLE COMMENT '中单净额(万元)',
            mid_net_pct DOUBLE COMMENT '中单净占比(%)',
            small_net DOUBLE COMMENT '小单净额(万元)',
            small_net_pct DOUBLE COMMENT '小单净占比(%)',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_date (stock_code, `date`),
            INDEX idx_stock_code (stock_code),
            INDEX idx_date (`date`)
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


def batch_upsert_fund_flow(stock_code: str, data_list: list[dict], cursor=None):
    """
    批量写入资金流向数据（upsert）。

    Args:
        stock_code: 股票代码（如 600183.SH）
        data_list: get_fund_flow_history 返回的原始数据列表（万元单位）
    """
    if not data_list:
        return 0

    sql = f"""
        INSERT INTO {TABLE_NAME}
            (stock_code, `date`, close_price, change_pct,
             net_flow, main_net_5day,
             big_net, big_net_pct, mid_net, mid_net_pct,
             small_net, small_net_pct)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            close_price=VALUES(close_price), change_pct=VALUES(change_pct),
            net_flow=VALUES(net_flow), main_net_5day=VALUES(main_net_5day),
            big_net=VALUES(big_net), big_net_pct=VALUES(big_net_pct),
            mid_net=VALUES(mid_net), mid_net_pct=VALUES(mid_net_pct),
            small_net=VALUES(small_net), small_net_pct=VALUES(small_net_pct)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    rows = [
        (stock_code, d.get("date", ""), d.get("close_price"),
         d.get("change_pct"), d.get("net_flow"), d.get("main_net_5day"),
         d.get("big_net"), d.get("big_net_pct"),
         d.get("mid_net"), d.get("mid_net_pct"),
         d.get("small_net"), d.get("small_net_pct"))
        for d in data_list
    ]
    cursor.executemany(sql, rows)
    count = cursor.rowcount

    if own:
        conn.commit()
        cursor.close()
        conn.close()

    return count


def get_fund_flow_by_code(stock_code: str, limit: int = 120, cursor=None) -> list[dict]:
    """查询某只股票的历史资金流向数据（按日期倒序）"""
    sql = f"SELECT * FROM {TABLE_NAME} WHERE stock_code = %s ORDER BY `date` DESC LIMIT %s"
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


def get_fund_flow_latest_date(stock_code: str, cursor=None) -> str | None:
    """查询某只股票资金流向的最新日期"""
    sql = f"SELECT MAX(`date`) FROM {TABLE_NAME} WHERE stock_code = %s"
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(sql, (stock_code,))
    row = cursor.fetchone()
    if own:
        cursor.close()
        conn.close()
    return row[0] if row else None

def get_fund_flow_count(stock_code: str, cursor=None) -> int:
    """查询某只股票的历史资金流向记录条数"""
    sql = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE stock_code = %s"
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(sql, (stock_code,))
    row = cursor.fetchone()
    if own:
        cursor.close()
        conn.close()
    return row[0] if row else 0


