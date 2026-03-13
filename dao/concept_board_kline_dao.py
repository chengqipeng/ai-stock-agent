"""概念板块日K线 DAO — concept_board_kline 表"""
import logging
from dao import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "concept_board_kline"


def create_table(cursor=None):
    """创建概念板块日K线表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            board_code VARCHAR(20) NOT NULL COMMENT '板块代码(30xxxx)',
            board_index_code VARCHAR(20) COMMENT '板块指数代码(885xxx/886xxx)',
            `date` VARCHAR(20) NOT NULL COMMENT '交易日期',
            open_price DOUBLE COMMENT '开盘价',
            close_price DOUBLE COMMENT '收盘价',
            high_price DOUBLE COMMENT '最高价',
            low_price DOUBLE COMMENT '最低价',
            trading_volume DOUBLE COMMENT '成交量(手)',
            trading_amount DOUBLE COMMENT '成交额',
            change_percent DOUBLE COMMENT '涨跌幅(%)',
            change_amount DOUBLE COMMENT '涨跌额',
            amplitude DOUBLE COMMENT '振幅(%)',
            change_hand DOUBLE COMMENT '换手率(%)',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_board_date (board_code, `date`),
            INDEX idx_board_code (board_code),
            INDEX idx_board_index_code (board_index_code),
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


def batch_upsert_klines(board_code: str, klines: list[dict],
                        board_index_code: str = None, cursor=None) -> int:
    """
    批量写入概念板块日K线数据（upsert）。

    Args:
        board_code: 板块代码(30xxxx)
        klines: [{"date": "2025-03-10", "open_price": 1000.0, ...}, ...]
        board_index_code: 板块指数代码(885xxx/886xxx)

    Returns:
        affected rows count
    """
    if not klines:
        return 0

    create_table(cursor)

    sql = f"""
        INSERT INTO {TABLE_NAME}
            (board_code, board_index_code, `date`, open_price, close_price,
             high_price, low_price, trading_volume, trading_amount,
             change_percent, change_amount, amplitude, change_hand)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            board_index_code=VALUES(board_index_code),
            open_price=VALUES(open_price),
            close_price=VALUES(close_price),
            high_price=VALUES(high_price),
            low_price=VALUES(low_price),
            trading_volume=VALUES(trading_volume),
            trading_amount=VALUES(trading_amount),
            change_percent=VALUES(change_percent),
            change_amount=VALUES(change_amount),
            amplitude=VALUES(amplitude),
            change_hand=VALUES(change_hand)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    rows = [
        (board_code, board_index_code, k["date"],
         k.get("open_price"), k.get("close_price"),
         k.get("high_price"), k.get("low_price"),
         k.get("trading_volume"), k.get("trading_amount"),
         k.get("change_percent"), k.get("change_amount"),
         k.get("amplitude"), k.get("change_hand"))
        for k in klines
    ]
    cursor.executemany(sql, rows)
    count = cursor.rowcount

    if own:
        conn.commit()
        cursor.close()
        conn.close()

    logger.info("[概念板块K线DAO] board=%s 写入 %d 条K线", board_code, count)
    return count


def get_klines_by_board(board_code: str, limit: int = 400, cursor=None) -> list[dict]:
    """查询某个板块的日K线数据（由旧到新）"""
    sql = (f"SELECT * FROM {TABLE_NAME} WHERE board_code = %s "
           f"ORDER BY `date` DESC LIMIT %s")
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (board_code, limit))
    result = list(reversed(cursor.fetchall()))
    if own:
        cursor.close()
        conn.close()
    return result


def get_latest_date(board_code: str, cursor=None) -> str | None:
    """查询某个板块最新的K线日期"""
    sql = f"SELECT MAX(`date`) FROM {TABLE_NAME} WHERE board_code = %s"
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(sql, (board_code,))
    row = cursor.fetchone()
    if own:
        cursor.close()
        conn.close()
    return row[0] if row and row[0] else None


def get_kline_count(board_code: str = None, cursor=None) -> int:
    """查询K线记录数，可按板块筛选"""
    if board_code:
        sql = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE board_code = %s"
        args = (board_code,)
    else:
        sql = f"SELECT COUNT(*) FROM {TABLE_NAME}"
        args = ()
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(sql, args)
    row = cursor.fetchone()
    if own:
        cursor.close()
        conn.close()
    return row[0] if row else 0
