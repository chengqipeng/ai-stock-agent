"""
个股概念板块强弱势评分 DAO — stock_concept_strength 表

存储每只股票在其所属概念板块中的强弱势评分（0-100）。
一只股票可能属于多个概念板块，每个板块一条记录。
"""
import logging
from dao import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_concept_strength"


def ensure_table(cursor=None):
    """创建表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
            stock_name VARCHAR(100) NOT NULL COMMENT '股票名称',
            board_code VARCHAR(20) NOT NULL COMMENT '板块代码',
            board_name VARCHAR(100) NOT NULL COMMENT '板块名称',
            strength_score DOUBLE NOT NULL COMMENT '强弱势评分(0-100)',
            strength_level VARCHAR(20) NOT NULL COMMENT '强势/中性/弱势',
            total_return DOUBLE COMMENT '个股区间涨跌幅%',
            excess_5d DOUBLE COMMENT '5日超额收益%',
            excess_20d DOUBLE COMMENT '20日超额收益%',
            excess_total DOUBLE COMMENT '全区间超额收益%',
            win_rate DOUBLE COMMENT '跑赢板块天数占比',
            rank_in_board INT COMMENT '板块内排名',
            board_total_stocks INT COMMENT '板块成分股总数',
            trade_days INT COMMENT '分析交易日数',
            analysis_days INT DEFAULT 60 COMMENT '分析参数天数',
            score_date VARCHAR(20) COMMENT '评分日期',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_stock_board (stock_code, board_code),
            INDEX idx_stock_code (stock_code),
            INDEX idx_board_code (board_code),
            INDEX idx_strength_score (strength_score),
            INDEX idx_strength_level (strength_level),
            INDEX idx_score_date (score_date)
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


def batch_upsert_strength(rows: list[dict], cursor=None) -> int:
    """
    批量写入个股板块强弱势评分（upsert）。

    Args:
        rows: [{stock_code, stock_name, board_code, board_name,
                strength_score, strength_level, total_return,
                excess_5d, excess_20d, excess_total, win_rate,
                rank_in_board, board_total_stocks, trade_days,
                analysis_days, score_date}, ...]
    Returns:
        affected rows count
    """
    if not rows:
        return 0

    ensure_table(cursor)

    sql = f"""
        INSERT INTO {TABLE_NAME}
            (stock_code, stock_name, board_code, board_name,
             strength_score, strength_level, total_return,
             excess_5d, excess_20d, excess_total, win_rate,
             rank_in_board, board_total_stocks, trade_days,
             analysis_days, score_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            stock_name=VALUES(stock_name),
            board_name=VALUES(board_name),
            strength_score=VALUES(strength_score),
            strength_level=VALUES(strength_level),
            total_return=VALUES(total_return),
            excess_5d=VALUES(excess_5d),
            excess_20d=VALUES(excess_20d),
            excess_total=VALUES(excess_total),
            win_rate=VALUES(win_rate),
            rank_in_board=VALUES(rank_in_board),
            board_total_stocks=VALUES(board_total_stocks),
            trade_days=VALUES(trade_days),
            analysis_days=VALUES(analysis_days),
            score_date=VALUES(score_date)
    """

    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()

    params = [
        (r["stock_code"], r["stock_name"], r["board_code"], r["board_name"],
         r["strength_score"], r["strength_level"], r.get("total_return"),
         r.get("excess_5d"), r.get("excess_20d"), r.get("excess_total"),
         r.get("win_rate"), r.get("rank_in_board"), r.get("board_total_stocks"),
         r.get("trade_days"), r.get("analysis_days", 60), r.get("score_date"))
        for r in rows
    ]
    cursor.executemany(sql, params)
    count = cursor.rowcount

    if own:
        conn.commit()
        cursor.close()
        conn.close()

    logger.info("[概念强弱DAO] 写入 %d 条评分记录", count)
    if count == 0 and rows:
        board_info = {(r["board_code"], r["board_name"]) for r in rows}
        for bc, bn in board_info:
            logger.warning("[概念强弱DAO] 写入0条 板块: %s(%s), 传入rows=%d", bn, bc, len(rows))
    return count


def get_stock_strength(stock_code: str, cursor=None) -> list[dict]:
    """查询某只股票在所有概念板块中的强弱势评分"""
    sql = (f"SELECT * FROM {TABLE_NAME} WHERE stock_code = %s "
           f"ORDER BY strength_score DESC")
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


def get_board_strength_ranking(board_code: str, cursor=None) -> list[dict]:
    """查询某个板块内所有个股的强弱势排名"""
    sql = (f"SELECT * FROM {TABLE_NAME} WHERE board_code = %s "
           f"ORDER BY strength_score DESC")
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


def get_strongest_stocks(limit: int = 50, cursor=None) -> list[dict]:
    """
    获取全市场概念板块强势股排名。
    对每只股票取其所有板块中的最高分作为代表分。
    """
    sql = f"""
        SELECT stock_code, stock_name,
               MAX(strength_score) AS max_score,
               GROUP_CONCAT(
                   CONCAT(board_name, ':', ROUND(strength_score,1))
                   ORDER BY strength_score DESC SEPARATOR ' | '
               ) AS board_scores,
               COUNT(*) AS board_count
        FROM {TABLE_NAME}
        WHERE score_date = (SELECT MAX(score_date) FROM {TABLE_NAME})
        GROUP BY stock_code, stock_name
        ORDER BY max_score DESC
        LIMIT %s
    """
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql, (limit,))
    result = cursor.fetchall()
    if own:
        cursor.close()
        conn.close()
    return result


def get_score_stats(cursor=None) -> dict:
    """获取评分统计信息"""
    sql = f"""
        SELECT COUNT(*) AS total_records,
               COUNT(DISTINCT stock_code) AS total_stocks,
               COUNT(DISTINCT board_code) AS total_boards,
               MAX(score_date) AS latest_date,
               ROUND(AVG(strength_score), 2) AS avg_score
        FROM {TABLE_NAME}
    """
    own = cursor is None
    if own:
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchone()
    if own:
        cursor.close()
        conn.close()
    return result or {}
