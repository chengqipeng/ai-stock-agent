"""
周预测 DAO — 管理 stock_weekly_prediction 和 stock_weekly_prediction_history 表
"""
import logging
from datetime import datetime

from dao import get_connection

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# 建表 DDL
# ═══════════════════════════════════════════════════════════

_CREATE_PREDICTION_TABLE = """
CREATE TABLE IF NOT EXISTS stock_weekly_prediction (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码(如002230.SZ)',
    stock_name VARCHAR(100) COMMENT '股票名称',
    predict_date VARCHAR(20) NOT NULL COMMENT '预测执行日期(基于哪天的数据)',
    iso_year INT NOT NULL COMMENT 'ISO年',
    iso_week INT NOT NULL COMMENT 'ISO周',
    pred_direction VARCHAR(4) NOT NULL COMMENT '预测方向: UP/DOWN',
    confidence VARCHAR(10) NOT NULL COMMENT '置信度: high/medium/low',
    strategy VARCHAR(30) NOT NULL COMMENT '使用策略',
    reason VARCHAR(200) COMMENT '预测理由',
    d3_chg DOUBLE COMMENT '前3天复合涨跌幅(%)',
    d4_chg DOUBLE COMMENT '前4天复合涨跌幅(%)',
    is_suspended TINYINT DEFAULT 0 COMMENT '是否停牌',
    week_day_count INT COMMENT '本周已有交易天数',
    board_momentum DOUBLE COMMENT '板块动量信号',
    concept_consensus DOUBLE COMMENT '概念板块一致性',
    fund_flow_signal DOUBLE COMMENT '资金流信号',
    market_d3_chg DOUBLE COMMENT '大盘前3天涨跌(%)',
    market_d4_chg DOUBLE COMMENT '大盘前4天涨跌(%)',
    concept_boards VARCHAR(500) COMMENT '所属概念板块(逗号分隔)',
    backtest_accuracy DOUBLE COMMENT '回测全样本准确率(%)',
    backtest_lowo_accuracy DOUBLE COMMENT '回测LOWO准确率(%)',
    backtest_weeks INT COMMENT '回测覆盖周数',
    backtest_samples INT COMMENT '回测样本数(该股票有效周数)',
    backtest_start_date VARCHAR(20) COMMENT '回测起始日期',
    backtest_end_date VARCHAR(20) COMMENT '回测截止日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_stock_code (stock_code),
    INDEX idx_predict_date (predict_date),
    INDEX idx_iso_week (iso_year, iso_week),
    INDEX idx_direction (pred_direction),
    INDEX idx_confidence (confidence)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='周预测最新结果(每只股票保留最新一条)'
"""

_CREATE_HISTORY_TABLE = """
CREATE TABLE IF NOT EXISTS stock_weekly_prediction_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    stock_name VARCHAR(100) COMMENT '股票名称',
    predict_date VARCHAR(20) NOT NULL COMMENT '预测执行日期',
    iso_year INT NOT NULL COMMENT 'ISO年',
    iso_week INT NOT NULL COMMENT 'ISO周',
    pred_direction VARCHAR(4) NOT NULL COMMENT '预测方向: UP/DOWN',
    confidence VARCHAR(10) NOT NULL COMMENT '置信度',
    strategy VARCHAR(30) NOT NULL COMMENT '使用策略',
    reason VARCHAR(200) COMMENT '预测理由',
    d3_chg DOUBLE COMMENT '前3天复合涨跌幅(%)',
    d4_chg DOUBLE COMMENT '前4天复合涨跌幅(%)',
    is_suspended TINYINT DEFAULT 0 COMMENT '是否停牌',
    week_day_count INT COMMENT '本周已有交易天数',
    board_momentum DOUBLE COMMENT '板块动量信号',
    concept_consensus DOUBLE COMMENT '概念板块一致性',
    fund_flow_signal DOUBLE COMMENT '资金流信号',
    market_d3_chg DOUBLE COMMENT '大盘前3天涨跌(%)',
    market_d4_chg DOUBLE COMMENT '大盘前4天涨跌(%)',
    concept_boards VARCHAR(500) COMMENT '所属概念板块',
    actual_direction VARCHAR(4) COMMENT '实际方向(周结束后回填): UP/DOWN/NULL',
    actual_weekly_chg DOUBLE COMMENT '实际全周涨跌幅(%)(周结束后回填)',
    is_correct TINYINT COMMENT '预测是否正确(周结束后回填)',
    backtest_accuracy DOUBLE COMMENT '回测准确率(%)',
    backtest_lowo_accuracy DOUBLE COMMENT '回测LOWO准确率(%)',
    backtest_weeks INT COMMENT '回测覆盖周数',
    backtest_samples INT COMMENT '回测样本数',
    backtest_start_date VARCHAR(20) COMMENT '回测起始日期',
    backtest_end_date VARCHAR(20) COMMENT '回测截止日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_stock_week (stock_code, iso_year, iso_week),
    INDEX idx_predict_date (predict_date),
    INDEX idx_iso_week (iso_year, iso_week),
    INDEX idx_direction (pred_direction),
    INDEX idx_is_correct (is_correct)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='周预测历史记录(每只股票每周一条)'
"""


def ensure_tables():
    """确保预测表存在，并迁移新增列。"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(_CREATE_PREDICTION_TABLE)
        cur.execute(_CREATE_HISTORY_TABLE)

        # 迁移：为已有表添加回测元数据列
        _bt_cols = [
            ("backtest_weeks", "INT COMMENT '回测覆盖周数'"),
            ("backtest_samples", "INT COMMENT '回测样本数'"),
            ("backtest_start_date", "VARCHAR(20) COMMENT '回测起始日期'"),
            ("backtest_end_date", "VARCHAR(20) COMMENT '回测截止日期'"),
        ]
        for tbl in ("stock_weekly_prediction", "stock_weekly_prediction_history"):
            for col_name, col_def in _bt_cols:
                try:
                    cur.execute(
                        "SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s",
                        (tbl, col_name))
                    if cur.fetchone()[0] == 0:
                        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col_name} {col_def} AFTER backtest_lowo_accuracy")
                        logger.info("  已添加列: %s.%s", tbl, col_name)
                except Exception as e:
                    logger.warning("  添加列失败 %s.%s: %s", tbl, col_name, e)

        conn.commit()
        logger.info("周预测表已就绪")
    finally:
        cur.close()
        conn.close()


def upsert_latest_prediction(prediction: dict):
    """插入或更新最新预测（stock_weekly_prediction 表，每只股票仅保留最新一条）。"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO stock_weekly_prediction
                (stock_code, stock_name, predict_date, iso_year, iso_week,
                 pred_direction, confidence, strategy, reason,
                 d3_chg, d4_chg, is_suspended, week_day_count,
                 board_momentum, concept_consensus, fund_flow_signal,
                 market_d3_chg, market_d4_chg, concept_boards,
                 backtest_accuracy, backtest_lowo_accuracy,
                 backtest_weeks, backtest_samples, backtest_start_date, backtest_end_date)
            VALUES
                (%(stock_code)s, %(stock_name)s, %(predict_date)s,
                 %(iso_year)s, %(iso_week)s,
                 %(pred_direction)s, %(confidence)s, %(strategy)s, %(reason)s,
                 %(d3_chg)s, %(d4_chg)s, %(is_suspended)s, %(week_day_count)s,
                 %(board_momentum)s, %(concept_consensus)s, %(fund_flow_signal)s,
                 %(market_d3_chg)s, %(market_d4_chg)s, %(concept_boards)s,
                 %(backtest_accuracy)s, %(backtest_lowo_accuracy)s,
                 %(backtest_weeks)s, %(backtest_samples)s, %(backtest_start_date)s, %(backtest_end_date)s)
            ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                predict_date = VALUES(predict_date),
                iso_year = VALUES(iso_year),
                iso_week = VALUES(iso_week),
                pred_direction = VALUES(pred_direction),
                confidence = VALUES(confidence),
                strategy = VALUES(strategy),
                reason = VALUES(reason),
                d3_chg = VALUES(d3_chg),
                d4_chg = VALUES(d4_chg),
                is_suspended = VALUES(is_suspended),
                week_day_count = VALUES(week_day_count),
                board_momentum = VALUES(board_momentum),
                concept_consensus = VALUES(concept_consensus),
                fund_flow_signal = VALUES(fund_flow_signal),
                market_d3_chg = VALUES(market_d3_chg),
                market_d4_chg = VALUES(market_d4_chg),
                concept_boards = VALUES(concept_boards),
                backtest_accuracy = VALUES(backtest_accuracy),
                backtest_lowo_accuracy = VALUES(backtest_lowo_accuracy),
                backtest_weeks = VALUES(backtest_weeks),
                backtest_samples = VALUES(backtest_samples),
                backtest_start_date = VALUES(backtest_start_date),
                backtest_end_date = VALUES(backtest_end_date)
        """, prediction)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def batch_upsert_latest_predictions(predictions: list[dict]):
    """批量插入或更新最新预测。"""
    if not predictions:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany("""
            INSERT INTO stock_weekly_prediction
                (stock_code, stock_name, predict_date, iso_year, iso_week,
                 pred_direction, confidence, strategy, reason,
                 d3_chg, d4_chg, is_suspended, week_day_count,
                 board_momentum, concept_consensus, fund_flow_signal,
                 market_d3_chg, market_d4_chg, concept_boards,
                 backtest_accuracy, backtest_lowo_accuracy,
                 backtest_weeks, backtest_samples, backtest_start_date, backtest_end_date)
            VALUES
                (%(stock_code)s, %(stock_name)s, %(predict_date)s,
                 %(iso_year)s, %(iso_week)s,
                 %(pred_direction)s, %(confidence)s, %(strategy)s, %(reason)s,
                 %(d3_chg)s, %(d4_chg)s, %(is_suspended)s, %(week_day_count)s,
                 %(board_momentum)s, %(concept_consensus)s, %(fund_flow_signal)s,
                 %(market_d3_chg)s, %(market_d4_chg)s, %(concept_boards)s,
                 %(backtest_accuracy)s, %(backtest_lowo_accuracy)s,
                 %(backtest_weeks)s, %(backtest_samples)s, %(backtest_start_date)s, %(backtest_end_date)s)
            ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                predict_date = VALUES(predict_date),
                iso_year = VALUES(iso_year),
                iso_week = VALUES(iso_week),
                pred_direction = VALUES(pred_direction),
                confidence = VALUES(confidence),
                strategy = VALUES(strategy),
                reason = VALUES(reason),
                d3_chg = VALUES(d3_chg),
                d4_chg = VALUES(d4_chg),
                is_suspended = VALUES(is_suspended),
                week_day_count = VALUES(week_day_count),
                board_momentum = VALUES(board_momentum),
                concept_consensus = VALUES(concept_consensus),
                fund_flow_signal = VALUES(fund_flow_signal),
                market_d3_chg = VALUES(market_d3_chg),
                market_d4_chg = VALUES(market_d4_chg),
                concept_boards = VALUES(concept_boards),
                backtest_accuracy = VALUES(backtest_accuracy),
                backtest_lowo_accuracy = VALUES(backtest_lowo_accuracy),
                backtest_weeks = VALUES(backtest_weeks),
                backtest_samples = VALUES(backtest_samples),
                backtest_start_date = VALUES(backtest_start_date),
                backtest_end_date = VALUES(backtest_end_date)
        """, predictions)
        conn.commit()
        logger.info("批量更新最新预测: %d 条", len(predictions))
    finally:
        cur.close()
        conn.close()


def batch_insert_history(predictions: list[dict]):
    """批量插入历史预测记录（每只股票每周一条，重复则更新）。"""
    if not predictions:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany("""
            INSERT INTO stock_weekly_prediction_history
                (stock_code, stock_name, predict_date, iso_year, iso_week,
                 pred_direction, confidence, strategy, reason,
                 d3_chg, d4_chg, is_suspended, week_day_count,
                 board_momentum, concept_consensus, fund_flow_signal,
                 market_d3_chg, market_d4_chg, concept_boards,
                 backtest_accuracy, backtest_lowo_accuracy,
                 backtest_weeks, backtest_samples, backtest_start_date, backtest_end_date)
            VALUES
                (%(stock_code)s, %(stock_name)s, %(predict_date)s,
                 %(iso_year)s, %(iso_week)s,
                 %(pred_direction)s, %(confidence)s, %(strategy)s, %(reason)s,
                 %(d3_chg)s, %(d4_chg)s, %(is_suspended)s, %(week_day_count)s,
                 %(board_momentum)s, %(concept_consensus)s, %(fund_flow_signal)s,
                 %(market_d3_chg)s, %(market_d4_chg)s, %(concept_boards)s,
                 %(backtest_accuracy)s, %(backtest_lowo_accuracy)s,
                 %(backtest_weeks)s, %(backtest_samples)s, %(backtest_start_date)s, %(backtest_end_date)s)
            ON DUPLICATE KEY UPDATE
                predict_date = VALUES(predict_date),
                pred_direction = VALUES(pred_direction),
                confidence = VALUES(confidence),
                strategy = VALUES(strategy),
                reason = VALUES(reason),
                d3_chg = VALUES(d3_chg),
                d4_chg = VALUES(d4_chg),
                is_suspended = VALUES(is_suspended),
                week_day_count = VALUES(week_day_count),
                board_momentum = VALUES(board_momentum),
                concept_consensus = VALUES(concept_consensus),
                fund_flow_signal = VALUES(fund_flow_signal),
                market_d3_chg = VALUES(market_d3_chg),
                market_d4_chg = VALUES(market_d4_chg),
                concept_boards = VALUES(concept_boards),
                backtest_accuracy = VALUES(backtest_accuracy),
                backtest_lowo_accuracy = VALUES(backtest_lowo_accuracy),
                backtest_weeks = VALUES(backtest_weeks),
                backtest_samples = VALUES(backtest_samples),
                backtest_start_date = VALUES(backtest_start_date),
                backtest_end_date = VALUES(backtest_end_date)
        """, predictions)
        conn.commit()
        logger.info("批量插入历史预测: %d 条", len(predictions))
    finally:
        cur.close()
        conn.close()


def backfill_actual_results(iso_year: int, iso_week: int,
                            results: list[dict]):
    """周结束后回填实际结果。

    results: [{'stock_code': ..., 'actual_direction': 'UP'/'DOWN',
               'actual_weekly_chg': float, 'is_correct': 0/1}, ...]
    """
    if not results:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany("""
            UPDATE stock_weekly_prediction_history
            SET actual_direction = %(actual_direction)s,
                actual_weekly_chg = %(actual_weekly_chg)s,
                is_correct = %(is_correct)s
            WHERE stock_code = %(stock_code)s
              AND iso_year = %s AND iso_week = %s
        """.replace('%s', str(iso_year), 1).replace('%s', str(iso_week), 1),
        results)
        conn.commit()
        logger.info("回填实际结果: %d 条 (Y%d-W%02d)", len(results), iso_year, iso_week)
    finally:
        cur.close()
        conn.close()


def get_latest_predictions(limit: int = 100) -> list[dict]:
    """获取最新预测结果。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM stock_weekly_prediction
            ORDER BY predict_date DESC, stock_code
            LIMIT %s
        """, (limit,))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_prediction_history(stock_code: str, limit: int = 30) -> list[dict]:
    """获取某只股票的预测历史。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM stock_weekly_prediction_history
            WHERE stock_code = %s
            ORDER BY iso_year DESC, iso_week DESC
            LIMIT %s
        """, (stock_code, limit))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_week_predictions(iso_year: int, iso_week: int) -> list[dict]:
    """获取某一周的所有预测。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM stock_weekly_prediction_history
            WHERE iso_year = %s AND iso_week = %s
            ORDER BY stock_code
        """, (iso_year, iso_week))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def get_prediction_accuracy_stats(iso_year: int = None, iso_week: int = None) -> dict:
    """获取预测准确率统计（已回填的历史数据）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        where = "WHERE is_correct IS NOT NULL"
        params = []
        if iso_year and iso_week:
            where += " AND iso_year = %s AND iso_week = %s"
            params = [iso_year, iso_week]

        cur.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(is_correct) as correct,
                ROUND(SUM(is_correct)/COUNT(*)*100, 1) as accuracy,
                SUM(CASE WHEN confidence='high' AND is_correct=1 THEN 1 ELSE 0 END) as high_correct,
                SUM(CASE WHEN confidence='high' THEN 1 ELSE 0 END) as high_total,
                SUM(CASE WHEN confidence='medium' AND is_correct=1 THEN 1 ELSE 0 END) as med_correct,
                SUM(CASE WHEN confidence='medium' THEN 1 ELSE 0 END) as med_total,
                SUM(CASE WHEN confidence='low' AND is_correct=1 THEN 1 ELSE 0 END) as low_correct,
                SUM(CASE WHEN confidence='low' THEN 1 ELSE 0 END) as low_total
            FROM stock_weekly_prediction_history
            {where}
        """, params)
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def get_latest_predictions_page(direction: str = None, confidence: str = None,
                                keyword: str = None, sort_by: str = 'stock_code',
                                sort_dir: str = 'asc',
                                limit: int = 50, offset: int = 0) -> tuple[list[dict], int]:
    """分页查询最新预测结果，支持筛选和排序。返回 (rows, total_count)。
    自动排除指数代码（如000001.SH等非个股代码）。
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 排除指数：只保留个股代码（6开头.SH, 0/3开头.SZ，且非399xxx、000001.SH）
        where_parts = [
            "("
            "  (stock_code LIKE %s)"
            "  OR (stock_code LIKE %s)"
            "  OR (stock_code LIKE %s)"
            ")",
            "stock_code NOT LIKE %s",
            "stock_code != %s",
        ]
        params = ['6%.SH', '0%.SZ', '3%.SZ', '399%', '000001.SH']
        if direction:
            where_parts.append("pred_direction = %s")
            params.append(direction)
        if confidence:
            where_parts.append("confidence = %s")
            params.append(confidence)
        if keyword:
            where_parts.append("(stock_code LIKE %s OR stock_name LIKE %s)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # 安全排序字段白名单
        allowed_sorts = {
            'stock_code', 'stock_name', 'pred_direction', 'confidence',
            'd3_chg', 'd4_chg', 'strategy', 'predict_date', 'backtest_accuracy',
        }
        if sort_by not in allowed_sorts:
            sort_by = 'stock_code'
        order_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        cur.execute(f"SELECT COUNT(*) as cnt FROM stock_weekly_prediction {where_sql}", params)
        total = cur.fetchone()['cnt']

        cur.execute(f"""
            SELECT stock_code, stock_name, predict_date, iso_year, iso_week,
                   pred_direction, confidence, strategy, reason,
                   d3_chg, d4_chg, is_suspended, week_day_count,
                   backtest_accuracy, backtest_lowo_accuracy,
                   backtest_weeks, backtest_samples, backtest_start_date, backtest_end_date,
                   concept_boards
            FROM stock_weekly_prediction
            {where_sql}
            ORDER BY {sort_by} {order_dir}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()
        return rows, total
    finally:
        cur.close()
        conn.close()


def get_prediction_summary() -> dict:
    """获取最新一批预测的汇总统计（排除指数）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                MAX(predict_date) as predict_date,
                MAX(iso_year) as iso_year,
                MAX(iso_week) as iso_week,
                SUM(pred_direction = 'UP') as up_count,
                SUM(pred_direction = 'DOWN') as down_count,
                SUM(confidence = 'high') as high_count,
                SUM(confidence = 'medium') as medium_count,
                SUM(confidence = 'low') as low_count,
                SUM(is_suspended = 1) as suspended_count,
                ROUND(AVG(backtest_accuracy), 1) as avg_backtest_accuracy,
                ROUND(AVG(backtest_lowo_accuracy), 1) as avg_lowo_accuracy,
                SUM(strategy = 'd4_strong') as d4_strong_count,
                SUM(strategy = 'd4_medium') as d4_medium_count,
                SUM(strategy = 'd4_fuzzy') as d4_fuzzy_count,
                SUM(strategy = 'd3_strong') as d3_strong_count,
                SUM(strategy = 'd3_medium') as d3_medium_count,
                SUM(strategy = 'd3_fuzzy') as d3_fuzzy_count,
                SUM(strategy = 'suspended') as suspended_strategy_count
            FROM stock_weekly_prediction
            WHERE (stock_code LIKE '6%.SH' OR stock_code LIKE '0%.SZ' OR stock_code LIKE '3%.SZ')
              AND stock_code NOT LIKE '399%'
              AND stock_code != '000001.SH'
        """)
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()
