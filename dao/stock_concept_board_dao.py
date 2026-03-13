"""概念板块 DAO — stock_concept_board 表"""
import logging
from dao import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_concept_board"


def create_concept_board_table(cursor=None):
    """创建概念板块表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            board_code VARCHAR(20) NOT NULL COMMENT '板块代码',
            board_name VARCHAR(100) NOT NULL COMMENT '板块名称',
            board_url VARCHAR(500) COMMENT '板块详情URL',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_board_code (board_code),
            INDEX idx_board_name (board_name)
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


def batch_upsert_concept_boards(boards: list[dict], cursor=None) -> int:
    """
    批量写入概念板块数据（upsert）。

    Args:
        boards: [{"board_code": "308007", "board_name": "人工智能", "board_url": "..."}, ...]

    Returns:
        affected rows count
    """
    if not boards:
        return 0

    # 先确保表存在
    create_concept_board_table(cursor)

    sql = f"""
        INSERT INTO {TABLE_NAME} (board_code, board_name, board_url)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            board_name=VALUES(board_name),
            board_url=VALUES(board_url)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    rows = [(b["board_code"], b["board_name"], b.get("board_url", "")) for b in boards]
    cursor.executemany(sql, rows)
    count = cursor.rowcount

    if own:
        conn.commit()
        cursor.close()
        conn.close()

    logger.info("[概念板块DAO] 写入 %d 条记录", count)
    return count


def get_all_concept_boards(cursor=None) -> list[dict]:
    """查询所有概念板块"""
    sql = f"SELECT * FROM {TABLE_NAME} ORDER BY board_code"
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    if own:
        cursor.close()
        conn.close()
    return result


def get_concept_board_count(cursor=None) -> int:
    """查询概念板块总数"""
    sql = f"SELECT COUNT(*) FROM {TABLE_NAME}"
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    if own:
        cursor.close()
        conn.close()
    return row[0] if row else 0


# ── stock_concept_board_stock（概念板块成分股） ──

STOCK_TABLE = "stock_concept_board_stock"


def create_board_stock_table(cursor=None):
    """创建概念板块成分股表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {STOCK_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            board_code VARCHAR(20) NOT NULL COMMENT '板块代码',
            board_name VARCHAR(100) NOT NULL COMMENT '板块名称',
            stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
            stock_name VARCHAR(100) NOT NULL COMMENT '股票名称',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_board_stock (board_code, stock_code),
            INDEX idx_board_code (board_code),
            INDEX idx_stock_code (stock_code),
            INDEX idx_board_name (board_name)
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


def batch_upsert_board_stocks(board_code: str, board_name: str,
                              stocks: list[dict], cursor=None) -> int:
    """
    批量写入板块成分股（upsert）。

    Args:
        board_code: 板块代码
        board_name: 板块名称
        stocks: [{"stock_code": "300143", "stock_name": "盈康生命"}, ...]

    Returns:
        affected rows count
    """
    if not stocks:
        return 0

    create_board_stock_table(cursor)

    sql = f"""
        INSERT INTO {STOCK_TABLE} (board_code, board_name, stock_code, stock_name)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            board_name=VALUES(board_name),
            stock_name=VALUES(stock_name)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    rows = [(board_code, board_name, s["stock_code"], s["stock_name"]) for s in stocks]
    cursor.executemany(sql, rows)
    count = cursor.rowcount

    if own:
        conn.commit()
        cursor.close()
        conn.close()

    logger.info("[板块成分股DAO] board=%s 写入 %d 条", board_code, count)
    return count


def get_stocks_by_board(board_code: str, cursor=None) -> list[dict]:
    """查询某个板块的所有成分股"""
    sql = f"SELECT * FROM {STOCK_TABLE} WHERE board_code = %s ORDER BY stock_code"
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (board_code,))
    result = cursor.fetchall()
    if own:
        cursor.close()
        conn.close()
    return result


def get_boards_by_stock(stock_code: str, cursor=None) -> list[dict]:
    """查询某只股票所属的所有概念板块"""
    sql = f"SELECT * FROM {STOCK_TABLE} WHERE stock_code = %s ORDER BY board_code"
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (stock_code,))
    result = cursor.fetchall()
    if own:
        cursor.close()
        conn.close()
    return result


def get_board_stock_count(board_code: str = None, cursor=None) -> int:
    """查询成分股记录数，可按板块筛选"""
    if board_code:
        sql = f"SELECT COUNT(*) FROM {STOCK_TABLE} WHERE board_code = %s"
        args = (board_code,)
    else:
        sql = f"SELECT COUNT(*) FROM {STOCK_TABLE}"
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
