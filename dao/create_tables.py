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

    # ── stock_concept_board（概念板块列表） ──
    """
    CREATE TABLE IF NOT EXISTS stock_concept_board (
        id INT AUTO_INCREMENT PRIMARY KEY,
        board_code VARCHAR(20) NOT NULL COMMENT '板块代码',
        board_name VARCHAR(100) NOT NULL COMMENT '板块名称',
        board_url VARCHAR(500) COMMENT '板块详情URL',
        board_index_code VARCHAR(20) COMMENT '板块指数代码(885xxx/886xxx)',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_board_code (board_code),
        INDEX idx_board_name (board_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_concept_board_stock（概念板块成分股） ──
    """
    CREATE TABLE IF NOT EXISTS stock_concept_board_stock (
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
    """,

    # ── concept_board_kline（概念板块日K线） ──
    """
    CREATE TABLE IF NOT EXISTS concept_board_kline (
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
    """,

    # ── stock_concept_strength（个股概念板块强弱势评分） ──
    """
    CREATE TABLE IF NOT EXISTS stock_concept_strength (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
        stock_name VARCHAR(100) NOT NULL COMMENT '股票名称',
        board_code VARCHAR(20) NOT NULL COMMENT '板块代码',
        board_name VARCHAR(100) NOT NULL COMMENT '板块名称',
        strength_score DOUBLE NOT NULL COMMENT '强弱势评分(0-100)',
        strength_level VARCHAR(20) NOT NULL COMMENT '强势/中性/弱势',
        total_return DOUBLE COMMENT '个股区间涨跌幅',
        excess_5d DOUBLE COMMENT '5日超额收益',
        excess_20d DOUBLE COMMENT '20日超额收益',
        excess_total DOUBLE COMMENT '全区间超额收益',
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

    # ── stock_weekly_prediction（周预测最新结果） ──
    """
    CREATE TABLE IF NOT EXISTS stock_weekly_prediction (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
        stock_name VARCHAR(100) COMMENT '股票名称',
        predict_date VARCHAR(20) NOT NULL COMMENT '预测执行日期',
        iso_year INT NOT NULL COMMENT 'ISO年',
        iso_week INT NOT NULL COMMENT 'ISO周',
        pred_direction VARCHAR(4) NOT NULL COMMENT 'UP/DOWN',
        confidence VARCHAR(10) NOT NULL COMMENT 'high/medium/low',
        strategy VARCHAR(30) NOT NULL COMMENT '使用策略',
        reason VARCHAR(200) COMMENT '预测理由',
        d3_chg DOUBLE COMMENT '前3天复合涨跌幅',
        d3_date_range VARCHAR(50) COMMENT 'd3计算区间',
        d4_chg DOUBLE COMMENT '前4天复合涨跌幅',
        d4_date_range VARCHAR(50) COMMENT 'd4计算区间',
        is_suspended TINYINT DEFAULT 0 COMMENT '是否停牌',
        week_day_count INT COMMENT '本周已有交易天数',
        board_momentum DOUBLE COMMENT '板块动量',
        concept_consensus DOUBLE COMMENT '概念一致性',
        fund_flow_signal DOUBLE COMMENT '资金流信号',
        market_d3_chg DOUBLE COMMENT '大盘前3天涨跌',
        market_d4_chg DOUBLE COMMENT '大盘前4天涨跌',
        concept_boards VARCHAR(500) COMMENT '所属概念板块',
        backtest_accuracy DOUBLE COMMENT '回测准确率',
        backtest_lowo_accuracy DOUBLE COMMENT '回测LOWO准确率',
        backtest_weeks INT COMMENT '回测覆盖周数',
        backtest_samples INT COMMENT '回测样本数',
        backtest_start_date VARCHAR(20) COMMENT '回测起始日期',
        backtest_end_date VARCHAR(20) COMMENT '回测截止日期',
        suggested_buy_date VARCHAR(20) COMMENT '建议买入日期',
        suggested_buy_price DOUBLE COMMENT '建议买入价格',
        suggested_buy_reason VARCHAR(200) COMMENT '买入建议理由',
        pred_weekly_chg DOUBLE COMMENT '预测本周涨跌幅(%)',
        pred_chg_low DOUBLE COMMENT '预测涨跌幅下限(%)',
        pred_chg_high DOUBLE COMMENT '预测涨跌幅上限(%)',
        pred_chg_mae DOUBLE COMMENT '涨跌幅预测MAE-平均绝对误差(%)',
        pred_chg_hit_rate DOUBLE COMMENT '涨跌幅区间命中率(%)',
        pred_chg_samples INT COMMENT '涨跌幅预测回测样本数',
        week_realized_chg DOUBLE COMMENT '本周已实现涨跌幅(%)',
        pred_remaining_chg DOUBLE COMMENT '本周剩余天数预测涨跌幅(%)',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_stock_code (stock_code),
        INDEX idx_predict_date (predict_date),
        INDEX idx_iso_week (iso_year, iso_week),
        INDEX idx_direction (pred_direction),
        INDEX idx_confidence (confidence)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_weekly_prediction_history（周预测历史记录） ──
    """
    CREATE TABLE IF NOT EXISTS stock_weekly_prediction_history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
        stock_name VARCHAR(100) COMMENT '股票名称',
        predict_date VARCHAR(20) NOT NULL COMMENT '预测执行日期',
        iso_year INT NOT NULL COMMENT 'ISO年',
        iso_week INT NOT NULL COMMENT 'ISO周',
        pred_direction VARCHAR(4) NOT NULL COMMENT 'UP/DOWN',
        confidence VARCHAR(10) NOT NULL COMMENT '置信度',
        strategy VARCHAR(30) NOT NULL COMMENT '使用策略',
        reason VARCHAR(200) COMMENT '预测理由',
        d3_chg DOUBLE COMMENT '前3天复合涨跌幅',
        d3_date_range VARCHAR(50) COMMENT 'd3计算区间',
        d4_chg DOUBLE COMMENT '前4天复合涨跌幅',
        d4_date_range VARCHAR(50) COMMENT 'd4计算区间',
        is_suspended TINYINT DEFAULT 0 COMMENT '是否停牌',
        week_day_count INT COMMENT '本周已有交易天数',
        board_momentum DOUBLE COMMENT '板块动量',
        concept_consensus DOUBLE COMMENT '概念一致性',
        fund_flow_signal DOUBLE COMMENT '资金流信号',
        market_d3_chg DOUBLE COMMENT '大盘前3天涨跌',
        market_d4_chg DOUBLE COMMENT '大盘前4天涨跌',
        concept_boards VARCHAR(500) COMMENT '所属概念板块',
        actual_direction VARCHAR(4) COMMENT '实际方向(回填)',
        actual_weekly_chg DOUBLE COMMENT '实际全周涨跌幅(回填)',
        is_correct TINYINT COMMENT '是否正确(回填)',
        backtest_accuracy DOUBLE COMMENT '回测准确率',
        backtest_lowo_accuracy DOUBLE COMMENT '回测LOWO准确率',
        backtest_weeks INT COMMENT '回测覆盖周数',
        backtest_samples INT COMMENT '回测样本数',
        backtest_start_date VARCHAR(20) COMMENT '回测起始日期',
        backtest_end_date VARCHAR(20) COMMENT '回测截止日期',
        suggested_buy_date VARCHAR(20) COMMENT '建议买入日期',
        suggested_buy_price DOUBLE COMMENT '建议买入价格',
        suggested_buy_reason VARCHAR(200) COMMENT '买入建议理由',
        pred_weekly_chg DOUBLE COMMENT '预测本周涨跌幅(%)',
        pred_chg_low DOUBLE COMMENT '预测涨跌幅下限(%)',
        pred_chg_high DOUBLE COMMENT '预测涨跌幅上限(%)',
        pred_chg_mae DOUBLE COMMENT '涨跌幅预测MAE-平均绝对误差(%)',
        pred_chg_hit_rate DOUBLE COMMENT '涨跌幅区间命中率(%)',
        pred_chg_samples INT COMMENT '涨跌幅预测回测样本数',
        week_realized_chg DOUBLE COMMENT '本周已实现涨跌幅(%)',
        pred_remaining_chg DOUBLE COMMENT '本周剩余天数预测涨跌幅(%)',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_stock_week (stock_code, iso_year, iso_week),
        INDEX idx_predict_date (predict_date),
        INDEX idx_iso_week (iso_year, iso_week),
        INDEX idx_direction (pred_direction),
        INDEX idx_is_correct (is_correct)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── stock_news（股票新闻公告） ──
    """
    CREATE TABLE IF NOT EXISTS stock_news (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
        news_type VARCHAR(20) NOT NULL COMMENT '类型: news/notice/industry/report',
        title VARCHAR(500) NOT NULL COMMENT '标题',
        url VARCHAR(1000) COMMENT '链接地址',
        publish_date VARCHAR(20) COMMENT '发布日期',
        publish_time VARCHAR(30) COMMENT '发布时间(含时分)',
        source VARCHAR(100) COMMENT '来源',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_code_type_title_date (stock_code, news_type, title(200), publish_date),
        INDEX idx_stock_code (stock_code),
        INDEX idx_news_type (news_type),
        INDEX idx_publish_date (publish_date)
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
    ("stock_kline_screening_history", "idx_ksh_stock_id", "INDEX idx_ksh_stock_id (stock_id)"),
]


def _migrate_indexes(cursor):
    """安全地为已存在的表添加缺失索引和列"""
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

    # 为 stock_concept_board 添加 board_index_code 列（如果不存在）
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'stock_concept_board' "
            "AND column_name = 'board_index_code'"
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute(
                "ALTER TABLE stock_concept_board ADD COLUMN "
                "board_index_code VARCHAR(20) COMMENT '板块指数代码(885xxx/886xxx)' "
                "AFTER board_url"
            )
            logger.info("  已添加列: stock_concept_board.board_index_code")
    except Exception as e:
        logger.warning("  添加列失败 stock_concept_board.board_index_code: %s", e)


if __name__ == "__main__":
    create_all_tables()
