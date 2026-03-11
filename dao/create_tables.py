"""
在 MySQL 中创建所有业务表。
用法: python -m dao.create_tables
"""
import logging

from dao import get_connection

logger = logging.getLogger(__name__)


_TABLES = [
    # ── stock_kline（所有股票K线合并单表） ──
    """
    CREATE TABLE IF NOT EXISTS stock_kline (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        `date` VARCHAR(20) NOT NULL,
        open_price DOUBLE,
        close_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        trading_volume DOUBLE,
        trading_amount DOUBLE,
        amplitude DOUBLE,
        change_percent DOUBLE,
        change_amount DOUBLE,
        change_hand DOUBLE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_code_date (stock_code, `date`),
        INDEX idx_stock_code (stock_code),
        INDEX idx_date (`date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_finance（所有股票财报合并单表） ──
    """
    CREATE TABLE IF NOT EXISTS stock_finance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        report_date VARCHAR(20) NOT NULL,
        report_period_name VARCHAR(50),
        data_json LONGTEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_code_report (stock_code, report_date),
        INDEX idx_stock_code (stock_code),
        INDEX idx_report_date (report_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_batch_list_info ──
    """
    CREATE TABLE IF NOT EXISTS stock_batch_list_info (
        id INT AUTO_INCREMENT PRIMARY KEY,
        batch_name VARCHAR(255) NOT NULL,
        total_count INT NOT NULL,
        success_count INT DEFAULT 0,
        completed_count INT DEFAULT 0,
        status VARCHAR(50) DEFAULT 'pending',
        is_pinned TINYINT DEFAULT 0,
        is_continuous_analysis TINYINT DEFAULT 0,
        sort_order INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_continuous (is_continuous_analysis)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_analysis_detail ──
    """
    CREATE TABLE IF NOT EXISTS stock_analysis_detail (
        id INT AUTO_INCREMENT PRIMARY KEY,
        batch_id INT NOT NULL,
        stock_code VARCHAR(20) NOT NULL,
        stock_name VARCHAR(100) NOT NULL,

        c_score INT, c_prompt LONGTEXT, c_summary LONGTEXT, c_score_prompt LONGTEXT,
        a_score INT, a_prompt LONGTEXT, a_summary LONGTEXT, a_score_prompt LONGTEXT,
        n_score INT, n_prompt LONGTEXT, n_summary LONGTEXT, n_score_prompt LONGTEXT,
        s_score INT, s_prompt LONGTEXT, s_summary LONGTEXT, s_score_prompt LONGTEXT,
        l_score INT, l_prompt LONGTEXT, l_summary LONGTEXT, l_score_prompt LONGTEXT,
        i_score INT, i_prompt LONGTEXT, i_summary LONGTEXT, i_score_prompt LONGTEXT,
        m_score INT, m_prompt LONGTEXT, m_summary LONGTEXT, m_score_prompt LONGTEXT,

        overall_analysis LONGTEXT,
        overall_prompt LONGTEXT,
        overall_grade VARCHAR(20),

        kline_score VARCHAR(50),
        kline_prompt LONGTEXT,
        kline_score_prompt LONGTEXT,
        kline_summary LONGTEXT,
        kline_hold_score VARCHAR(50),
        kline_hold_prompt LONGTEXT,
        kline_total_score INT,

        c_deep_score DOUBLE, c_deep_prompt LONGTEXT, c_deep_summary LONGTEXT, c_deep_score_prompt LONGTEXT,
        a_deep_score DOUBLE, a_deep_prompt LONGTEXT, a_deep_summary LONGTEXT, a_deep_score_prompt LONGTEXT,
        n_deep_score DOUBLE, n_deep_prompt LONGTEXT, n_deep_summary LONGTEXT, n_deep_score_prompt LONGTEXT,
        s_deep_score DOUBLE, s_deep_prompt LONGTEXT, s_deep_summary LONGTEXT, s_deep_score_prompt LONGTEXT,
        l_deep_score DOUBLE, l_deep_prompt LONGTEXT, l_deep_summary LONGTEXT, l_deep_score_prompt LONGTEXT,
        i_deep_score DOUBLE, i_deep_prompt LONGTEXT, i_deep_summary LONGTEXT, i_deep_score_prompt LONGTEXT,
        m_deep_score DOUBLE, m_deep_prompt LONGTEXT, m_deep_summary LONGTEXT, m_deep_score_prompt LONGTEXT,

        data_issues LONGTEXT,
        change_pct DOUBLE,
        high_price_120 DOUBLE,
        high_price_date_120 VARCHAR(20),
        latest_price DOUBLE,

        status VARCHAR(50) DEFAULT 'pending',
        error_message LONGTEXT,
        is_deep_thinking TINYINT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP NULL,

        INDEX idx_batch_id (batch_id),
        INDEX idx_ad_stock_code (stock_code),
        INDEX idx_ad_status (status),
        FOREIGN KEY (batch_id) REFERENCES stock_batch_list_info (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_deep_analysis_history ──
    """
    CREATE TABLE IF NOT EXISTS stock_deep_analysis_history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        batch_id INT NOT NULL,
        stock_id INT NOT NULL,
        stock_name VARCHAR(100) NOT NULL,
        stock_code VARCHAR(20) NOT NULL,
        is_deep_thinking TINYINT DEFAULT 0,
        c_score DOUBLE, c_result LONGTEXT, c_summary LONGTEXT,
        a_score DOUBLE, a_result LONGTEXT, a_summary LONGTEXT,
        n_score DOUBLE, n_result LONGTEXT, n_summary LONGTEXT,
        s_score DOUBLE, s_result LONGTEXT, s_summary LONGTEXT,
        l_score DOUBLE, l_result LONGTEXT, l_summary LONGTEXT,
        i_score DOUBLE, i_result LONGTEXT, i_summary LONGTEXT,
        m_score DOUBLE, m_result LONGTEXT, m_summary LONGTEXT,
        overall_analysis LONGTEXT,
        overall_prompt LONGTEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_dah_batch (batch_id),
        INDEX idx_dah_stock_id (stock_id),
        INDEX idx_dah_stock_name (stock_name),
        INDEX idx_dah_stock_code (stock_code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_dim_analysis_history ──
    """
    CREATE TABLE IF NOT EXISTS stock_dim_analysis_history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        execution_id VARCHAR(100),
        batch_id INT NOT NULL,
        stock_id INT NOT NULL,
        stock_name VARCHAR(100) NOT NULL,
        stock_code VARCHAR(20) NOT NULL,
        dimension VARCHAR(10) NOT NULL,
        is_deep_thinking TINYINT DEFAULT 0,
        score DOUBLE,
        result LONGTEXT,
        summary LONGTEXT,
        overall_grade VARCHAR(20),
        status VARCHAR(50) DEFAULT 'done',
        error_message LONGTEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_stock_name (stock_name),
        INDEX idx_execution_id (execution_id),
        INDEX idx_dim_batch_id (batch_id),
        INDEX idx_dim_stock_id (stock_id),
        INDEX idx_dim_stock_code (stock_code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_highest_lowest_price ──
    """
    CREATE TABLE IF NOT EXISTS stock_highest_lowest_price (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        stock_name VARCHAR(100) NOT NULL,
        highest_price DOUBLE,
        highest_date VARCHAR(20),
        lowest_price DOUBLE,
        lowest_date VARCHAR(20),
        update_time VARCHAR(30),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_code (stock_code),
        INDEX idx_highest_date (highest_date),
        INDEX idx_lowest_date (lowest_date),
        INDEX idx_update_time (update_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_batch_technical_score ──
    """
    CREATE TABLE IF NOT EXISTS stock_batch_technical_score (
        id INT AUTO_INCREMENT PRIMARY KEY,
        batch_id INT NOT NULL,
        stock_name VARCHAR(100) NOT NULL,
        stock_code VARCHAR(20) NOT NULL,
        total_score INT NOT NULL,
        macd_score INT NOT NULL,
        macd_detail TEXT,
        kdj_score INT NOT NULL,
        kdj_detail TEXT,
        vol_score INT NOT NULL,
        vol_detail TEXT,
        trend_score INT NOT NULL,
        trend_detail TEXT,
        boll_score INT DEFAULT 0,
        boll_signal TINYINT DEFAULT 0,
        boll_detail TEXT,
        mid_bounce_score INT DEFAULT 0,
        mid_bounce_signal TINYINT DEFAULT 0,
        mid_bounce_detail TEXT,
        close_price DOUBLE,
        score_date VARCHAR(20),
        created_at VARCHAR(30) NOT NULL,
        UNIQUE KEY uk_batch_code_date (batch_id, stock_code, score_date),
        INDEX idx_ts_batch (batch_id),
        INDEX idx_ts_code (stock_code),
        INDEX idx_ts_total (total_score),
        INDEX idx_ts_date (score_date),
        INDEX idx_ts_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # ── stock_time_data（分时数据） ──
    """
    CREATE TABLE IF NOT EXISTS stock_time_data (
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
    """,

    # ── stock_order_book（盘口数据） ──
    """
    CREATE TABLE IF NOT EXISTS stock_order_book (
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
    """,

    # ── stock_dragon_tiger（龙虎榜数据） ──
    """
    CREATE TABLE IF NOT EXISTS stock_dragon_tiger (
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
    """,

    # ── stock_fund_flow（历史资金流向数据） ──
    """
    CREATE TABLE IF NOT EXISTS stock_fund_flow (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL,
        `date` VARCHAR(20) NOT NULL,
        close_price DOUBLE,
        change_pct DOUBLE,
        net_flow DOUBLE COMMENT '资金净流入(万元)',
        main_net_5day DOUBLE COMMENT '5日主力净额(万元)',
        big_net DOUBLE COMMENT '大单(主力)净额(万元)',
        big_net_pct DOUBLE COMMENT '大单净占比',
        mid_net DOUBLE COMMENT '中单净额(万元)',
        mid_net_pct DOUBLE COMMENT '中单净占比',
        small_net DOUBLE COMMENT '小单净额(万元)',
        small_net_pct DOUBLE COMMENT '小单净占比',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_code_date (stock_code, `date`),
        INDEX idx_stock_code (stock_code),
        INDEX idx_date (`date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_kline_screening_history（K线初筛历史记录，每次分析都产生新记录） ──
    """
    CREATE TABLE IF NOT EXISTS stock_kline_screening_history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        batch_id INT NOT NULL,
        stock_id INT NOT NULL,
        stock_name VARCHAR(100) NOT NULL,
        stock_code VARCHAR(20) NOT NULL,
        screen_date VARCHAR(20) NOT NULL,
        kline_score VARCHAR(50),
        kline_hold_score VARCHAR(50),
        kline_total_score INT,
        kline_prompt LONGTEXT,
        kline_hold_prompt LONGTEXT,
        data_issues LONGTEXT,
        next_day_prediction JSON,
        next_week_prediction JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_ksh_batch (batch_id),
        INDEX idx_ksh_date (screen_date),
        INDEX idx_ksh_stock_id (stock_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def create_all_tables():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        for ddl in _TABLES:
            cursor.execute(ddl)
        conn.commit()
        logger.info("所有表创建完成 ✓")

        # 为已存在的表补充缺失索引
        _migrate_indexes(cursor)
        conn.commit()
        logger.info("索引迁移完成 ✓")

        # 列出已创建的表
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        logger.info("当前数据库共 %d 张表:", len(tables))
        for t in tables:
            logger.info("  - %s", t)
    finally:
        cursor.close()
        conn.close()


# ── 索引迁移：为已存在的表安全添加缺失索引 ──
_INDEX_MIGRATIONS = [
    # (表名, 索引名, 索引定义)
    ("stock_batch_list_info", "idx_continuous", "INDEX idx_continuous (is_continuous_analysis)"),
    ("stock_analysis_detail", "idx_ad_stock_code", "INDEX idx_ad_stock_code (stock_code)"),
    ("stock_analysis_detail", "idx_ad_status", "INDEX idx_ad_status (status)"),
    ("stock_deep_analysis_history", "idx_dah_batch", "INDEX idx_dah_batch (batch_id)"),
    ("stock_deep_analysis_history", "idx_dah_stock_id", "INDEX idx_dah_stock_id (stock_id)"),
    ("stock_deep_analysis_history", "idx_dah_stock_name", "INDEX idx_dah_stock_name (stock_name)"),
    ("stock_deep_analysis_history", "idx_dah_stock_code", "INDEX idx_dah_stock_code (stock_code)"),
    ("stock_dim_analysis_history", "idx_dim_batch_id", "INDEX idx_dim_batch_id (batch_id)"),
    ("stock_dim_analysis_history", "idx_dim_stock_id", "INDEX idx_dim_stock_id (stock_id)"),
    ("stock_dim_analysis_history", "idx_dim_stock_code", "INDEX idx_dim_stock_code (stock_code)"),
    ("stock_highest_lowest_price", "idx_update_time", "INDEX idx_update_time (update_time)"),
    ("stock_batch_technical_score", "idx_ts_created", "INDEX idx_ts_created (created_at)"),
    ("stock_kline_screening_history", "idx_ksh_stock_id", "INDEX idx_ksh_stock_id (stock_id)"),
]


def _migrate_indexes(cursor):
    """安全地为已存在的表添加缺失索引，索引已存在则跳过"""
    for table, idx_name, idx_def in _INDEX_MIGRATIONS:
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.statistics "
                "WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s",
                (table, idx_name),
            )
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"ALTER TABLE {table} ADD {idx_def}")
                logger.info("  已添加索引: %s.%s", table, idx_name)
            else:
                logger.debug("  索引已存在: %s.%s", table, idx_name)
        except Exception as e:
            logger.warning("  添加索引失败 %s.%s: %s", table, idx_name, e)


if __name__ == "__main__":
    create_all_tables()
