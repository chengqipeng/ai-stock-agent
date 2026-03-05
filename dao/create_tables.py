"""
在 MySQL 中创建所有业务表。
用法: python -m dao.create_tables
"""
from dao import get_connection


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
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        INDEX idx_execution_id (execution_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_batch_technical_score ──
    """
    CREATE TABLE IF NOT EXISTS stock_batch_technical_score (
        id INT AUTO_INCREMENT PRIMARY KEY,
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
        close_price DOUBLE,
        score_date VARCHAR(20),
        created_at VARCHAR(30) NOT NULL,
        UNIQUE KEY uk_code_date (stock_code, score_date),
        INDEX idx_ts_code (stock_code),
        INDEX idx_ts_total (total_score),
        INDEX idx_ts_date (score_date)
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
        print("所有表创建完成 ✓")

        # 列出已创建的表
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"当前数据库共 {len(tables)} 张表:")
        for t in tables:
            print(f"  - {t}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_all_tables()
