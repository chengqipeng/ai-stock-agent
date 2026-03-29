"""
周预测 DAO — 管理 stock_weekly_prediction 和 stock_weekly_prediction_history 表
"""
import logging
from datetime import datetime, timedelta

from dao import get_connection

logger = logging.getLogger(__name__)


def _is_target_week_finished(iso_year: int, iso_week: int) -> bool:
    """判断目标周是否已结束（周五已过）。"""
    if not iso_year or not iso_week:
        return False
    try:
        mon = datetime.strptime(f'{iso_year}-W{iso_week:02d}-1', '%G-W%V-%u')
        fri = mon + timedelta(days=4)
        return datetime.now().date() > fri.date()
    except (ValueError, TypeError):
        return False


def _mask_unfinished_actual(rows: list[dict], actual_fields: list[str],
                            iso_year: int, iso_week: int):
    """如果目标周尚未结束，将 actual 字段强制置为 None（防止显示错误回填数据）。"""
    if _is_target_week_finished(iso_year, iso_week):
        return
    for r in rows:
        for f in actual_fields:
            r[f] = None

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
    d3_date_range VARCHAR(50) COMMENT 'd3计算区间(如2025-03-10~2025-03-12)',
    d4_chg DOUBLE COMMENT '前4天复合涨跌幅(%)',
    d4_date_range VARCHAR(50) COMMENT 'd4计算区间(如2025-03-10~2025-03-13)',
    is_suspended TINYINT DEFAULT 0 COMMENT '是否停牌',
    week_day_count INT COMMENT '本周已有交易天数',
    board_momentum DOUBLE COMMENT '板块动量信号',
    concept_consensus DOUBLE COMMENT '概念板块一致性',
    fund_flow_signal DOUBLE COMMENT '资金流信号',
    market_d3_chg DOUBLE COMMENT '大盘前3天涨跌(%)',
    market_d4_chg DOUBLE COMMENT '大盘前4天涨跌(%)',
    market_index VARCHAR(20) COMMENT '对应大盘指数代码',
    concept_boards VARCHAR(500) COMMENT '所属概念板块(逗号分隔)',
    backtest_accuracy DOUBLE COMMENT '回测全样本准确率(%)',
    backtest_lowo_accuracy DOUBLE COMMENT '回测LOWO准确率(%)',
    backtest_weeks INT COMMENT '回测覆盖周数',
    backtest_samples INT COMMENT '回测样本数(该股票有效周数)',
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
    vol_ratio DOUBLE COMMENT '本周均量/20日均量',
    vol_price_corr DOUBLE COMMENT '量价相关性[-1,1]',
    vol_trend VARCHAR(20) COMMENT '量能趋势: expanding/shrinking/normal',
    fund_flow_trend VARCHAR(20) COMMENT '资金流趋势: inflow/outflow/neutral',
    big_net_sum DOUBLE COMMENT '本周大单净额合计(万元)',
    main_net_5day DOUBLE COMMENT '最新5日主力净额(万元)',
    finance_score DOUBLE COMMENT '财务综合评分[-1,1]',
    revenue_yoy DOUBLE COMMENT '营收同比增长率(%)',
    profit_yoy DOUBLE COMMENT '净利润同比增长率(%)',
    roe DOUBLE COMMENT 'ROE(%)',
    nw_pred_direction VARCHAR(4) COMMENT '下周预测方向: UP/DOWN',
    nw_confidence VARCHAR(10) COMMENT '下周预测置信度',
    nw_strategy VARCHAR(30) COMMENT '下周预测策略',
    nw_reason VARCHAR(200) COMMENT '下周预测理由',
    nw_composite_score DOUBLE COMMENT '下周预测综合评分',
    nw_this_week_chg DOUBLE COMMENT '本周涨跌幅(%)',
    nw_iso_year INT COMMENT '下周ISO年',
    nw_iso_week INT COMMENT '下周ISO周',
    nw_date_range VARCHAR(50) COMMENT '下周日期范围',
    nw_pred_chg DOUBLE COMMENT '下周预测涨跌幅(%)',
    nw_pred_chg_low DOUBLE COMMENT '下周预测涨跌幅下限(%)',
    nw_pred_chg_high DOUBLE COMMENT '下周预测涨跌幅上限(%)',
    nw_pred_chg_mae DOUBLE COMMENT '下周涨跌幅MAE(%)',
    nw_pred_chg_hit_rate DOUBLE COMMENT '下周涨跌幅命中率(%)',
    nw_pred_chg_samples INT COMMENT '下周涨跌幅样本数',
    nw_backtest_accuracy DOUBLE COMMENT '下周预测回测准确率(%)',
    nw_backtest_samples INT COMMENT '下周预测回测样本数',
    nm_pred_direction VARCHAR(4) COMMENT '下月预测方向: UP/DOWN',
    nm_confidence VARCHAR(10) COMMENT '下月预测置信度',
    nm_strategy VARCHAR(30) COMMENT '下月预测策略',
    nm_reason VARCHAR(200) COMMENT '下月预测理由',
    nm_composite_score DOUBLE COMMENT '下月预测综合评分',
    nm_this_month_chg DOUBLE COMMENT '本月涨跌幅(%)',
    nm_target_year INT COMMENT '预测目标年',
    nm_target_month INT COMMENT '预测目标月',
    nm_date_range VARCHAR(50) COMMENT '下月日期范围',
    nm_backtest_accuracy DOUBLE COMMENT '下月预测回测准确率(%)',
    nm_backtest_samples INT COMMENT '下月预测回测样本数',
    nm_dim_scores VARCHAR(500) COMMENT '下月预测各维度评分(JSON)',
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
    d3_date_range VARCHAR(50) COMMENT 'd3计算区间',
    d4_chg DOUBLE COMMENT '前4天复合涨跌幅(%)',
    d4_date_range VARCHAR(50) COMMENT 'd4计算区间',
    is_suspended TINYINT DEFAULT 0 COMMENT '是否停牌',
    week_day_count INT COMMENT '本周已有交易天数',
    board_momentum DOUBLE COMMENT '板块动量信号',
    concept_consensus DOUBLE COMMENT '概念板块一致性',
    fund_flow_signal DOUBLE COMMENT '资金流信号',
    market_d3_chg DOUBLE COMMENT '大盘前3天涨跌(%)',
    market_d4_chg DOUBLE COMMENT '大盘前4天涨跌(%)',
    market_index VARCHAR(20) COMMENT '对应大盘指数代码',
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
    vol_ratio DOUBLE COMMENT '本周均量/20日均量',
    vol_price_corr DOUBLE COMMENT '量价相关性[-1,1]',
    vol_trend VARCHAR(20) COMMENT '量能趋势: expanding/shrinking/normal',
    fund_flow_trend VARCHAR(20) COMMENT '资金流趋势: inflow/outflow/neutral',
    big_net_sum DOUBLE COMMENT '本周大单净额合计(万元)',
    main_net_5day DOUBLE COMMENT '最新5日主力净额(万元)',
    finance_score DOUBLE COMMENT '财务综合评分[-1,1]',
    revenue_yoy DOUBLE COMMENT '营收同比增长率(%)',
    profit_yoy DOUBLE COMMENT '净利润同比增长率(%)',
    roe DOUBLE COMMENT 'ROE(%)',
    nw_pred_direction VARCHAR(4) COMMENT '下周预测方向: UP/DOWN',
    nw_confidence VARCHAR(10) COMMENT '下周预测置信度',
    nw_strategy VARCHAR(30) COMMENT '下周预测策略',
    nw_reason VARCHAR(200) COMMENT '下周预测理由',
    nw_composite_score DOUBLE COMMENT '下周预测综合评分',
    nw_this_week_chg DOUBLE COMMENT '本周涨跌幅(%)',
    nw_iso_year INT COMMENT '下周ISO年',
    nw_iso_week INT COMMENT '下周ISO周',
    nw_date_range VARCHAR(50) COMMENT '下周日期范围',
    nw_pred_chg DOUBLE COMMENT '下周预测涨跌幅(%)',
    nw_pred_chg_low DOUBLE COMMENT '下周预测涨跌幅下限(%)',
    nw_pred_chg_high DOUBLE COMMENT '下周预测涨跌幅上限(%)',
    nw_pred_chg_mae DOUBLE COMMENT '下周涨跌幅MAE(%)',
    nw_pred_chg_hit_rate DOUBLE COMMENT '下周涨跌幅命中率(%)',
    nw_pred_chg_samples INT COMMENT '下周涨跌幅样本数',
    nw_backtest_accuracy DOUBLE COMMENT '下周预测回测准确率(%)',
    nw_backtest_samples INT COMMENT '下周预测回测样本数',
    nm_pred_direction VARCHAR(4) COMMENT '下月预测方向: UP/DOWN',
    nm_confidence VARCHAR(10) COMMENT '下月预测置信度',
    nm_strategy VARCHAR(30) COMMENT '下月预测策略',
    nm_reason VARCHAR(200) COMMENT '下月预测理由',
    nm_composite_score DOUBLE COMMENT '下月预测综合评分',
    nm_this_month_chg DOUBLE COMMENT '本月涨跌幅(%)',
    nm_target_year INT COMMENT '预测目标年',
    nm_target_month INT COMMENT '预测目标月',
    nm_date_range VARCHAR(50) COMMENT '下月日期范围',
    nm_backtest_accuracy DOUBLE COMMENT '下月预测回测准确率(%)',
    nm_backtest_samples INT COMMENT '下月预测回测样本数',
    nm_dim_scores VARCHAR(500) COMMENT '下月预测各维度评分(JSON)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_stock_week (stock_code, iso_year, iso_week),
    INDEX idx_predict_date (predict_date),
    INDEX idx_iso_week (iso_year, iso_week),
    INDEX idx_direction (pred_direction),
    INDEX idx_is_correct (is_correct)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='周预测历史记录(每只股票每周一条)'
"""

# ── 共享列列表（用于动态生成 INSERT/UPDATE SQL） ──
_PREDICTION_COLUMNS = [
    'stock_code', 'stock_name', 'predict_date', 'iso_year', 'iso_week',
    'pred_direction', 'confidence', 'strategy', 'reason',
    'd3_chg', 'd3_date_range', 'd4_chg', 'd4_date_range',
    'is_suspended', 'week_day_count',
    'board_momentum', 'concept_consensus', 'fund_flow_signal',
    'market_d3_chg', 'market_d4_chg', 'market_index', 'concept_boards',
    'backtest_accuracy', 'backtest_lowo_accuracy',
    'backtest_weeks', 'backtest_samples', 'backtest_start_date', 'backtest_end_date',
    'suggested_buy_date', 'suggested_buy_price', 'suggested_buy_reason',
    'pred_weekly_chg', 'pred_chg_low', 'pred_chg_high',
    'pred_chg_mae', 'pred_chg_hit_rate', 'pred_chg_samples',
    'week_realized_chg', 'pred_remaining_chg',
    'vol_ratio', 'vol_price_corr', 'vol_trend',
    'fund_flow_trend', 'big_net_sum', 'main_net_5day',
    'finance_score', 'revenue_yoy', 'profit_yoy', 'roe',
    'nw_pred_direction', 'nw_confidence', 'nw_strategy', 'nw_reason',
    'nw_composite_score', 'nw_this_week_chg',
    'nw_iso_year', 'nw_iso_week', 'nw_date_range',
    'nw_pred_chg', 'nw_pred_chg_low', 'nw_pred_chg_high',
    'nw_pred_chg_mae', 'nw_pred_chg_hit_rate', 'nw_pred_chg_samples',
    'nw_backtest_accuracy', 'nw_backtest_samples',
    # 月度预测 nm_* 列
    'nm_pred_direction', 'nm_confidence', 'nm_strategy', 'nm_reason',
    'nm_composite_score', 'nm_this_month_chg',
    'nm_target_year', 'nm_target_month', 'nm_date_range',
    'nm_backtest_accuracy', 'nm_backtest_samples',
    'nm_dim_scores',
    # V20量价超跌反弹预测 v20_* 列
    'v20_pred_direction', 'v20_confidence', 'v20_rule_name', 'v20_reason',
    'v20_backtest_acc', 'v20_matched_count', 'v20_matched_rules',
    'v20_pos', 'v20_vr5', 'v20_ma20d', 'v20_cdn',
    'v20_actual_direction', 'v20_actual_5d_chg', 'v20_is_correct',
    # V30情绪因子预测 v30_* 列
    'v30_pred_direction', 'v30_confidence', 'v30_strategy', 'v30_reason',
    'v30_composite_score', 'v30_sent_agree', 'v30_tech_agree', 'v30_mkt_ret_20d',
    'v30_actual_direction', 'v30_actual_5d_chg', 'v30_is_correct',
]

# 不参与 ON DUPLICATE KEY UPDATE 的列（主键/唯一键 + 回填的实际结果字段）
_SKIP_UPDATE_COLS = {
    'stock_code',
    # 回填的实际结果字段：这些由验证服务单独回填，预测时不应覆盖
    'actual_direction', 'actual_weekly_chg', 'is_correct',
    'nw_actual_direction', 'nw_actual_weekly_chg', 'nw_is_correct',
    'v20_actual_direction', 'v20_actual_5d_chg', 'v20_is_correct',
    'v30_actual_direction', 'v30_actual_5d_chg', 'v30_is_correct',
}


def _build_upsert_sql(table: str) -> str:
    """动态生成 INSERT ... ON DUPLICATE KEY UPDATE SQL。"""
    cols_str = ', '.join(_PREDICTION_COLUMNS)
    vals_str = ', '.join(f'%({c})s' for c in _PREDICTION_COLUMNS)
    update_parts = [f'{c} = VALUES({c})' for c in _PREDICTION_COLUMNS if c not in _SKIP_UPDATE_COLS]
    update_str = ',\n                '.join(update_parts)
    return f"""
        INSERT INTO {table} ({cols_str})
        VALUES ({vals_str})
        ON DUPLICATE KEY UPDATE
                {update_str}
    """


def ensure_tables():
    """确保预测表存在，并迁移新增列。"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(_CREATE_PREDICTION_TABLE)
        cur.execute(_CREATE_HISTORY_TABLE)

        # 迁移：为已有表添加新增列
        _migrate_cols = [
            ("backtest_weeks", "INT COMMENT '回测覆盖周数'", "backtest_lowo_accuracy"),
            ("backtest_samples", "INT COMMENT '回测样本数'", "backtest_weeks"),
            ("backtest_start_date", "VARCHAR(20) COMMENT '回测起始日期'", "backtest_samples"),
            ("backtest_end_date", "VARCHAR(20) COMMENT '回测截止日期'", "backtest_start_date"),
            ("d3_date_range", "VARCHAR(50) COMMENT 'd3计算区间'", "d3_chg"),
            ("d4_date_range", "VARCHAR(50) COMMENT 'd4计算区间'", "d4_chg"),
            ("suggested_buy_date", "VARCHAR(20) COMMENT '建议买入日期'", "backtest_end_date"),
            ("suggested_buy_price", "DOUBLE COMMENT '建议买入价格'", "suggested_buy_date"),
            ("suggested_buy_reason", "VARCHAR(200) COMMENT '买入建议理由'", "suggested_buy_price"),
            ("pred_weekly_chg", "DOUBLE COMMENT '预测本周涨跌幅(%)'", "suggested_buy_reason"),
            ("pred_chg_low", "DOUBLE COMMENT '预测涨跌幅下限(%)'", "pred_weekly_chg"),
            ("pred_chg_high", "DOUBLE COMMENT '预测涨跌幅上限(%)'", "pred_chg_low"),
            ("pred_chg_mae", "DOUBLE COMMENT '涨跌幅预测MAE-平均绝对误差(%)'", "pred_chg_high"),
            ("pred_chg_hit_rate", "DOUBLE COMMENT '涨跌幅区间命中率(%)'", "pred_chg_mae"),
            ("pred_chg_samples", "INT COMMENT '涨跌幅预测回测样本数'", "pred_chg_hit_rate"),
            ("week_realized_chg", "DOUBLE COMMENT '本周已实现涨跌幅(%)'", "pred_chg_samples"),
            ("pred_remaining_chg", "DOUBLE COMMENT '本周剩余天数预测涨跌幅(%)'", "week_realized_chg"),
            ("nw_pred_direction", "VARCHAR(4) COMMENT '下周预测方向: UP/DOWN'", "pred_remaining_chg"),
            ("nw_confidence", "VARCHAR(10) COMMENT '下周预测置信度'", "nw_pred_direction"),
            ("nw_strategy", "VARCHAR(30) COMMENT '下周预测策略'", "nw_confidence"),
            ("nw_reason", "VARCHAR(200) COMMENT '下周预测理由'", "nw_strategy"),
            ("nw_composite_score", "DOUBLE COMMENT '下周预测综合评分'", "nw_reason"),
            ("nw_this_week_chg", "DOUBLE COMMENT '本周涨跌幅(%)'", "nw_composite_score"),
            ("nw_iso_year", "INT COMMENT '下周ISO年'", "nw_this_week_chg"),
            ("nw_iso_week", "INT COMMENT '下周ISO周'", "nw_iso_year"),
            ("nw_date_range", "VARCHAR(50) COMMENT '下周日期范围'", "nw_iso_week"),
            ("nw_pred_chg", "DOUBLE COMMENT '下周预测涨跌幅(%)'", "nw_date_range"),
            ("nw_pred_chg_low", "DOUBLE COMMENT '下周预测涨跌幅下限(%)'", "nw_pred_chg"),
            ("nw_pred_chg_high", "DOUBLE COMMENT '下周预测涨跌幅上限(%)'", "nw_pred_chg_low"),
            ("nw_pred_chg_mae", "DOUBLE COMMENT '下周涨跌幅MAE(%)'", "nw_pred_chg_high"),
            ("nw_pred_chg_hit_rate", "DOUBLE COMMENT '下周涨跌幅命中率(%)'", "nw_pred_chg_mae"),
            ("nw_pred_chg_samples", "INT COMMENT '下周涨跌幅样本数'", "nw_pred_chg_hit_rate"),
            ("nw_backtest_accuracy", "DOUBLE COMMENT '下周预测回测准确率(%)'", "nw_pred_chg_samples"),
            ("nw_backtest_samples", "INT COMMENT '下周预测回测样本数'", "nw_backtest_accuracy"),
            ("nw_actual_direction", "VARCHAR(4) COMMENT '下周实际方向: UP/DOWN'", "nw_backtest_samples"),
            ("nw_actual_weekly_chg", "DOUBLE COMMENT '下周实际涨跌幅(%)'", "nw_actual_direction"),
            ("nw_is_correct", "TINYINT COMMENT '下周预测是否正确'", "nw_actual_weekly_chg"),
            # 月度预测 nm_* 列
            ("nm_pred_direction", "VARCHAR(4) COMMENT '下月预测方向: UP'", "nw_backtest_samples"),
            ("nm_confidence", "VARCHAR(10) COMMENT '下月预测置信度'", "nm_pred_direction"),
            ("nm_strategy", "VARCHAR(30) COMMENT '下月预测策略'", "nm_confidence"),
            ("nm_reason", "VARCHAR(200) COMMENT '下月预测理由'", "nm_strategy"),
            ("nm_composite_score", "DOUBLE COMMENT '下月预测综合评分'", "nm_reason"),
            ("nm_this_month_chg", "DOUBLE COMMENT '本月涨跌幅(%)'", "nm_composite_score"),
            ("nm_target_year", "INT COMMENT '预测目标年'", "nm_this_month_chg"),
            ("nm_target_month", "INT COMMENT '预测目标月'", "nm_target_year"),
            ("nm_date_range", "VARCHAR(50) COMMENT '下月日期范围'", "nm_target_month"),
            ("nm_backtest_accuracy", "DOUBLE COMMENT '下月预测回测准确率(%)'", "nm_date_range"),
            ("nm_backtest_samples", "INT COMMENT '下月预测回测样本数'", "nm_backtest_accuracy"),
            ("nm_dim_scores", "VARCHAR(500) COMMENT '下月预测各维度评分(JSON)'", "nm_backtest_samples"),
            ("market_index", "VARCHAR(20) COMMENT '对应大盘指数代码'", "market_d4_chg"),
            ("vol_ratio", "DOUBLE COMMENT '本周均量/20日均量'", "pred_remaining_chg"),
            ("vol_price_corr", "DOUBLE COMMENT '量价相关性[-1,1]'", "vol_ratio"),
            ("vol_trend", "VARCHAR(20) COMMENT '量能趋势'", "vol_price_corr"),
            ("fund_flow_trend", "VARCHAR(20) COMMENT '资金流趋势'", "vol_trend"),
            ("big_net_sum", "DOUBLE COMMENT '本周大单净额合计(万元)'", "fund_flow_trend"),
            ("main_net_5day", "DOUBLE COMMENT '最新5日主力净额(万元)'", "big_net_sum"),
            ("finance_score", "DOUBLE COMMENT '财务综合评分[-1,1]'", "main_net_5day"),
            ("revenue_yoy", "DOUBLE COMMENT '营收同比增长率(%)'", "finance_score"),
            ("profit_yoy", "DOUBLE COMMENT '净利润同比增长率(%)'", "revenue_yoy"),
            ("roe", "DOUBLE COMMENT 'ROE(%)'", "profit_yoy"),
            # V20量价超跌反弹预测 v20_* 列
            ("v20_pred_direction", "VARCHAR(4) COMMENT 'V20量价预测方向: UP'", "nm_dim_scores"),
            ("v20_confidence", "VARCHAR(10) COMMENT 'V20预测置信度: high/medium'", "v20_pred_direction"),
            ("v20_rule_name", "VARCHAR(30) COMMENT 'V20命中主规则名'", "v20_confidence"),
            ("v20_reason", "VARCHAR(200) COMMENT 'V20预测理由'", "v20_rule_name"),
            ("v20_backtest_acc", "DOUBLE COMMENT 'V20规则回测准确率(%)'", "v20_reason"),
            ("v20_matched_count", "INT COMMENT 'V20命中规则数量'", "v20_backtest_acc"),
            ("v20_matched_rules", "VARCHAR(100) COMMENT 'V20所有命中规则(逗号分隔)'", "v20_matched_count"),
            ("v20_pos", "DOUBLE COMMENT 'V20特征:60日位置'", "v20_matched_rules"),
            ("v20_vr5", "DOUBLE COMMENT 'V20特征:5日量比'", "v20_pos"),
            ("v20_ma20d", "DOUBLE COMMENT 'V20特征:MA20偏离度(%)'", "v20_vr5"),
            ("v20_cdn", "INT COMMENT 'V20特征:连跌天数'", "v20_ma20d"),
            ("v20_actual_direction", "VARCHAR(4) COMMENT 'V20实际5日方向(回填): UP/DOWN'", "v20_cdn"),
            ("v20_actual_5d_chg", "DOUBLE COMMENT 'V20实际5日涨跌幅(%)(回填)'", "v20_actual_direction"),
            ("v20_is_correct", "TINYINT COMMENT 'V20预测是否正确(回填)'", "v20_actual_5d_chg"),
            # V30情绪因子预测 v30_* 列
            ("v30_pred_direction", "VARCHAR(4) COMMENT 'V30情绪预测方向: UP'", "v20_is_correct"),
            ("v30_confidence", "VARCHAR(10) COMMENT 'V30预测置信度: high/medium/low'", "v30_pred_direction"),
            ("v30_strategy", "VARCHAR(30) COMMENT 'V30预测策略名'", "v30_confidence"),
            ("v30_reason", "VARCHAR(200) COMMENT 'V30预测理由'", "v30_strategy"),
            ("v30_composite_score", "DOUBLE COMMENT 'V30综合评分'", "v30_reason"),
            ("v30_sent_agree", "INT COMMENT 'V30情绪因子看涨数'", "v30_composite_score"),
            ("v30_tech_agree", "INT COMMENT 'V30技术因子看涨数'", "v30_sent_agree"),
            ("v30_mkt_ret_20d", "DOUBLE COMMENT 'V30大盘20日涨幅(%)'", "v30_tech_agree"),
            ("v30_actual_direction", "VARCHAR(4) COMMENT 'V30实际5日方向(回填): UP/DOWN'", "v30_mkt_ret_20d"),
            ("v30_actual_5d_chg", "DOUBLE COMMENT 'V30实际5日涨跌幅(%)(回填)'", "v30_actual_direction"),
            ("v30_is_correct", "TINYINT COMMENT 'V30预测是否正确(回填)'", "v30_actual_5d_chg"),
        ]
        for tbl in ("stock_weekly_prediction", "stock_weekly_prediction_history"):
            for col_name, col_def, after_col in _migrate_cols:
                try:
                    cur.execute(
                        "SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s",
                        (tbl, col_name))
                    if cur.fetchone()[0] == 0:
                        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col_name} {col_def} AFTER {after_col}")
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
    _ensure_all_columns([prediction])
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(_build_upsert_sql("stock_weekly_prediction"), prediction)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _ensure_all_columns(predictions: list[dict]) -> list[dict]:
    """确保每条预测记录包含 _PREDICTION_COLUMNS 中的所有键，缺失的填 None。"""
    for p in predictions:
        for col in _PREDICTION_COLUMNS:
            if col not in p:
                p[col] = None
    return predictions


def batch_upsert_latest_predictions(predictions: list[dict]):
    """批量插入或更新最新预测。"""
    if not predictions:
        return
    _ensure_all_columns(predictions)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany(_build_upsert_sql("stock_weekly_prediction"), predictions)
        conn.commit()
        logger.info("批量更新最新预测: %d 条", len(predictions))
    finally:
        cur.close()
        conn.close()


def batch_insert_history(predictions: list[dict]):
    """批量插入历史预测记录（每只股票每周一条，重复则更新）。"""
    if not predictions:
        return
    _ensure_all_columns(predictions)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany(_build_upsert_sql("stock_weekly_prediction_history"), predictions)
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


def backfill_nw_actual_results(iso_year: int, iso_week: int,
                               results: list[dict]):
    """回填下周预测的实际结果到预测周(W)的记录上。

    iso_year/iso_week: 做出预测的那一周(W)
    results: [{'stock_code': ..., 'nw_actual_direction': 'UP'/'DOWN',
               'nw_actual_weekly_chg': float, 'nw_is_correct': 0/1}, ...]
    """
    if not results:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany("""
            UPDATE stock_weekly_prediction_history
            SET nw_actual_direction = %(nw_actual_direction)s,
                nw_actual_weekly_chg = %(nw_actual_weekly_chg)s,
                nw_is_correct = %(nw_is_correct)s
            WHERE stock_code = %(stock_code)s
              AND iso_year = %s AND iso_week = %s
        """.replace('%s', str(iso_year), 1).replace('%s', str(iso_week), 1),
        results)
        conn.commit()
        logger.info("回填下周预测实际结果: %d 条 (Y%d-W%02d)", len(results), iso_year, iso_week)
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
                                keyword: str = None, keywords: list[str] = None,
                                nw_direction: str = None,
                                v20_direction: str = None,
                                v30_direction: str = None,
                                sort_by: str = 'stock_code',
                                sort_dir: str = 'asc',
                                limit: int = 50, offset: int = 0,
                                monthly_only: bool = False) -> tuple[list[dict], int]:
    """分页查询最新预测结果，支持筛选和排序。返回 (rows, total_count)。
    自动排除指数代码（如000001.SH等非个股代码）。
    keywords: 多关键词列表，任一匹配即命中（OR逻辑）。
    keyword: 兼容旧的单关键词参数。
    nw_direction: 下周预测方向筛选，支持 UP/DOWN/UNCERTAIN/HAS_SIGNAL。
    monthly_only: 仅返回有月度预测(nm_pred_direction IS NOT NULL)的行。
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 排除指数：只保留个股代码（6开头.SH, 0/3开头.SZ，且非399xxx、000001.SH）
        where_parts = [
            "("
            "  (p.stock_code LIKE %s)"
            "  OR (p.stock_code LIKE %s)"
            "  OR (p.stock_code LIKE %s)"
            ")",
            "p.stock_code NOT LIKE %s",
            "p.stock_code != %s",
        ]
        params = ['6%.SH', '0%.SZ', '3%.SZ', '399%', '000001.SH']
        if monthly_only:
            where_parts.append("p.nm_pred_direction IS NOT NULL")
        if direction:
            where_parts.append("p.pred_direction = %s")
            params.append(direction)
        if confidence:
            where_parts.append("p.confidence = %s")
            params.append(confidence)
        # 下周预测方向筛选
        if nw_direction:
            if nw_direction == 'UNCERTAIN':
                where_parts.append("p.nw_pred_direction IS NULL")
            elif nw_direction == 'HAS_SIGNAL':
                where_parts.append("p.nw_pred_direction IS NOT NULL")
            else:
                where_parts.append("p.nw_pred_direction = %s")
                params.append(nw_direction)
        # V20量价预测方向筛选
        if v20_direction:
            if v20_direction == 'NO_SIGNAL':
                where_parts.append("p.v20_pred_direction IS NULL")
            else:
                where_parts.append("p.v20_pred_direction = %s")
                params.append(v20_direction)
        # V30情绪预测方向筛选
        if v30_direction:
            if v30_direction == 'NO_SIGNAL':
                where_parts.append("p.v30_pred_direction IS NULL")
            else:
                where_parts.append("p.v30_pred_direction = %s")
                params.append(v30_direction)
        # 多关键词搜索（OR逻辑）
        search_terms = keywords or ([keyword] if keyword else None)
        if search_terms:
            or_clauses = []
            for term in search_terms:
                or_clauses.append("(p.stock_code LIKE %s OR p.stock_name LIKE %s)")
                params.extend([f"%{term}%", f"%{term}%"])
            where_parts.append("(" + " OR ".join(or_clauses) + ")")

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # 安全排序字段白名单
        allowed_sorts = {
            'stock_code', 'stock_name', 'pred_direction', 'confidence',
            'd3_chg', 'd4_chg', 'strategy', 'predict_date', 'backtest_accuracy',
            'suggested_buy_date', 'suggested_buy_price', 'pred_weekly_chg',
            'week_realized_chg', 'pred_remaining_chg',
            'nw_pred_direction', 'nw_pred_chg', 'nw_backtest_accuracy',
            'nm_pred_direction', 'nm_composite_score', 'nm_backtest_accuracy',
        }
        if sort_by not in allowed_sorts:
            sort_by = 'stock_code'
        order_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        cur.execute(f"SELECT COUNT(*) as cnt FROM stock_weekly_prediction p {where_sql}", params)
        total = cur.fetchone()['cnt']

        cur.execute(f"""
            SELECT p.stock_code, p.stock_name, p.predict_date, p.iso_year, p.iso_week,
                   p.pred_direction, p.confidence, p.strategy, p.reason,
                   p.d3_chg, p.d3_date_range, p.d4_chg, p.d4_date_range,
                   p.is_suspended, p.week_day_count,
                   p.backtest_accuracy, p.backtest_lowo_accuracy,
                   p.backtest_weeks, p.backtest_samples, p.backtest_start_date, p.backtest_end_date,
                   p.suggested_buy_date, p.suggested_buy_price, p.suggested_buy_reason,
                   p.pred_weekly_chg, p.pred_chg_low, p.pred_chg_high,
                   p.pred_chg_mae, p.pred_chg_hit_rate, p.pred_chg_samples,
                   p.week_realized_chg, p.pred_remaining_chg,
                   p.nw_pred_direction, p.nw_confidence, p.nw_strategy, p.nw_reason,
                   p.nw_composite_score, p.nw_this_week_chg,
                   p.nw_iso_year, p.nw_iso_week, p.nw_date_range,
                   p.nw_pred_chg, p.nw_pred_chg_low, p.nw_pred_chg_high,
                   p.nw_pred_chg_mae, p.nw_pred_chg_hit_rate, p.nw_pred_chg_samples,
                   p.nw_backtest_accuracy, p.nw_backtest_samples,
                   p.nm_pred_direction, p.nm_confidence, p.nm_strategy, p.nm_reason,
                   p.nm_composite_score, p.nm_this_month_chg,
                   p.nm_target_year, p.nm_target_month, p.nm_date_range,
                   p.nm_backtest_accuracy, p.nm_backtest_samples, p.nm_dim_scores,
                   p.concept_boards,
                   p.fund_flow_signal, p.finance_score, p.board_momentum, p.vol_trend,
                   p.v20_pred_direction, p.v20_confidence, p.v20_rule_name, p.v20_reason,
                   p.v20_backtest_acc, p.v20_matched_count, p.v20_matched_rules,
                   p.v20_pos, p.v20_vr5, p.v20_ma20d, p.v20_cdn,
                   p.v30_pred_direction, p.v30_confidence, p.v30_strategy, p.v30_reason,
                   p.v30_composite_score, p.v30_sent_agree, p.v30_tech_agree, p.v30_mkt_ret_20d
            FROM stock_weekly_prediction p
            {where_sql}
            ORDER BY {sort_by} {order_dir}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()
        return rows, total
    finally:
        cur.close()
        conn.close()


def get_prediction_verification(iso_year: int = None, iso_week: int = None,
                                keyword: str = None, keywords: list[str] = None,
                                direction_filter: str = None,
                                result_filter: str = None,
                                sort_by: str = 'stock_code',
                                sort_dir: str = 'asc',
                                limit: int = 50, offset: int = 0) -> tuple[list[dict], int, dict]:
    """获取本周预测验证数据：预测方向 vs 实际结果。

    iso_year/iso_week 为目标周（即要验证的那一周），与多模型叠加逻辑一致。
    本周预测的目标周就是 iso_year/iso_week 本身（预测当周涨跌）。
    如果不指定，自动取最近有验证数据的目标周。

    返回 (rows, total_count, summary)。
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        _stock_filter = (
            "(h.stock_code LIKE '6%%.SH' OR h.stock_code LIKE '0%%.SZ' OR h.stock_code LIKE '3%%.SZ')"
            " AND h.stock_code NOT LIKE '399%%'"
            " AND h.stock_code != '000001.SH'"
        )

        # 确定目标周：使用 nw_iso_year/nw_iso_week 对齐，与多模型叠加一致
        if not iso_year or not iso_week:
            cur.execute(f"""
                SELECT DISTINCT nw_iso_year, nw_iso_week
                FROM stock_weekly_prediction_history
                WHERE {_stock_filter}
                  AND nw_iso_year IS NOT NULL AND nw_iso_week IS NOT NULL
                ORDER BY nw_iso_year DESC, nw_iso_week DESC
                LIMIT 2
            """)
            weeks = cur.fetchall()
            if not weeks:
                return [], 0, {}
            # 取第一个目标周（最新的目标周）
            iso_year = weeks[0]['nw_iso_year']
            iso_week = weeks[0]['nw_iso_week']

        # 本周预测验证：目标周 = iso_year/iso_week（预测当周涨跌）
        # 所以直接用 iso_year/iso_week 匹配
        where_parts = [
            "h.iso_year = %s",
            "h.iso_week = %s",
            "(h.stock_code LIKE '6%%.SH' OR h.stock_code LIKE '0%%.SZ' OR h.stock_code LIKE '3%%.SZ')",
            "h.stock_code NOT LIKE '399%%'",
            "h.stock_code != '000001.SH'",
        ]
        params = [iso_year, iso_week]

        # 目标周未结束时，忽略结果筛选（DB中可能有脏数据）
        _week_finished = _is_target_week_finished(iso_year, iso_week)

        if direction_filter:
            where_parts.append("h.pred_direction = %s")
            params.append(direction_filter)

        if _week_finished:
            if result_filter == 'correct':
                where_parts.append("h.is_correct = 1")
            elif result_filter == 'wrong':
                where_parts.append("h.is_correct = 0")
            elif result_filter == 'pending':
                where_parts.append("h.is_correct IS NULL")

        search_terms = keywords or ([keyword] if keyword else None)
        if search_terms:
            or_clauses = []
            for term in search_terms:
                or_clauses.append("(h.stock_code LIKE %s OR h.stock_name LIKE %s)")
                params.extend([f"%{term}%", f"%{term}%"])
            where_parts.append("(" + " OR ".join(or_clauses) + ")")

        where_sql = "WHERE " + " AND ".join(where_parts)

        # 排序白名单
        allowed_sorts = {
            'stock_code', 'stock_name', 'pred_direction', 'confidence',
            'strategy', 'actual_weekly_chg', 'is_correct', 'pred_weekly_chg',
            'backtest_accuracy', 'nw_pred_direction', 'predict_date',
        }
        if sort_by not in allowed_sorts:
            sort_by = 'stock_code'
        order_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        # 总数
        cur.execute(f"SELECT COUNT(*) as cnt FROM stock_weekly_prediction_history h {where_sql}", params)
        total = cur.fetchone()['cnt']

        # 数据
        cur.execute(f"""
            SELECT h.stock_code, h.stock_name, h.predict_date, h.iso_year, h.iso_week,
                   h.pred_direction, h.confidence, h.strategy, h.reason,
                   h.d3_chg, h.d4_chg, h.pred_weekly_chg, h.pred_chg_low, h.pred_chg_high,
                   h.backtest_accuracy, h.backtest_samples,
                   h.actual_direction, h.actual_weekly_chg, h.is_correct,
                   h.nw_pred_direction, h.nw_confidence, h.nw_strategy,
                   h.nw_pred_chg, h.nw_date_range, h.nw_backtest_accuracy,
                   h.concept_boards,
                   h.fund_flow_signal, h.finance_score, h.board_momentum, h.vol_trend
            FROM stock_weekly_prediction_history h
            {where_sql}
            ORDER BY {sort_by} {order_dir}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        # 如果目标周尚未结束，屏蔽 actual 字段
        _tw_actual_fields = ['actual_direction', 'actual_weekly_chg', 'is_correct']
        _mask_unfinished_actual(rows, _tw_actual_fields, iso_year, iso_week)

        # 汇总统计
        cur.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(h.is_correct = 1) as correct,
                SUM(h.is_correct = 0) as wrong,
                SUM(h.is_correct IS NULL) as pending,
                ROUND(SUM(h.is_correct = 1) / NULLIF(SUM(h.is_correct IS NOT NULL), 0) * 100, 1) as accuracy,
                SUM(h.pred_direction = 'UP') as pred_up,
                SUM(h.pred_direction = 'DOWN') as pred_down,
                SUM(h.confidence = 'high' AND h.is_correct = 1) as high_correct,
                SUM(h.confidence = 'high' AND h.is_correct IS NOT NULL) as high_total,
                SUM(h.confidence = 'medium' AND h.is_correct = 1) as med_correct,
                SUM(h.confidence = 'medium' AND h.is_correct IS NOT NULL) as med_total,
                SUM(h.confidence = 'low' AND h.is_correct = 1) as low_correct,
                SUM(h.confidence = 'low' AND h.is_correct IS NOT NULL) as low_total,
                ROUND(AVG(h.actual_weekly_chg), 2) as avg_actual_chg,
                ROUND(AVG(h.pred_weekly_chg), 2) as avg_pred_chg,
                ROUND(AVG(h.backtest_accuracy), 1) as avg_backtest_accuracy,
                MAX(h.predict_date) as predict_date,
                SUM(h.nw_pred_direction IS NOT NULL AND h.nw_pred_direction != '') as nw_total,
                h.iso_year, h.iso_week
            FROM stock_weekly_prediction_history h
            {where_sql}
        """, params)
        summary = cur.fetchone() or {}
        summary['iso_year'] = iso_year
        summary['iso_week'] = iso_week

        # 目标周未结束时，汇总中的 actual 统计也要屏蔽
        if not _is_target_week_finished(iso_year, iso_week):
            total_cnt = summary.get('total') or 0
            summary['correct'] = 0
            summary['wrong'] = 0
            summary['pending'] = total_cnt
            summary['accuracy'] = None
            summary['high_correct'] = 0
            summary['high_total'] = 0
            summary['med_correct'] = 0
            summary['med_total'] = 0
            summary['low_correct'] = 0
            summary['low_total'] = 0
            summary['avg_actual_chg'] = None

        return rows, total, summary
    finally:
        cur.close()
        conn.close()



def get_nw_prediction_verification(iso_year: int = None, iso_week: int = None,
                                   keyword: str = None, keywords: list[str] = None,
                                   direction_filter: str = None,
                                   result_filter: str = None,
                                   sort_by: str = 'stock_code',
                                   sort_dir: str = 'asc',
                                   limit: int = 50, offset: int = 0) -> tuple[list[dict], int, dict]:
    """获取"下周预测"验证数据。

    iso_year/iso_week 为目标周（即预测要验证的那一周），与多模型叠加逻辑一致。
    使用 nw_iso_year/nw_iso_week 匹配目标周。
    例如选择 W14，则查找 nw_iso_year/nw_iso_week = W14 的记录（即 W13 发起的预测）。

    返回 (rows, total_count, summary)。
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        _stock_filter = (
            "(h.stock_code LIKE '6%%.SH' OR h.stock_code LIKE '0%%.SZ' OR h.stock_code LIKE '3%%.SZ')"
            " AND h.stock_code NOT LIKE '399%%'"
            " AND h.stock_code != '000001.SH'"
        )

        if not iso_year or not iso_week:
            cur.execute(f"""
                SELECT DISTINCT nw_iso_year, nw_iso_week
                FROM stock_weekly_prediction_history
                WHERE {_stock_filter}
                  AND nw_pred_direction IS NOT NULL AND nw_pred_direction != ''
                  AND nw_iso_year IS NOT NULL AND nw_iso_week IS NOT NULL
                ORDER BY nw_iso_year DESC, nw_iso_week DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                return [], 0, {}
            iso_year = row['nw_iso_year']
            iso_week = row['nw_iso_week']

        # 按目标周筛选（nw_iso_year/nw_iso_week = 预测验证的那一周）
        where_parts = [
            "h.nw_iso_year = %s",
            "h.nw_iso_week = %s",
            "(h.stock_code LIKE '6%%.SH' OR h.stock_code LIKE '0%%.SZ' OR h.stock_code LIKE '3%%.SZ')",
            "h.stock_code NOT LIKE '399%%'",
            "h.stock_code != '000001.SH'",
            "h.nw_pred_direction IS NOT NULL",
            "h.nw_pred_direction != ''",
        ]
        params = [iso_year, iso_week]

        # 目标周未结束时，忽略结果筛选（DB中可能有脏数据）
        _week_finished = _is_target_week_finished(iso_year, iso_week)

        if direction_filter:
            where_parts.append("h.nw_pred_direction = %s")
            params.append(direction_filter)

        if _week_finished:
            if result_filter == 'correct':
                where_parts.append("h.nw_is_correct = 1")
            elif result_filter == 'wrong':
                where_parts.append("h.nw_is_correct = 0")
            elif result_filter == 'pending':
                where_parts.append("h.nw_is_correct IS NULL")

        search_terms = keywords or ([keyword] if keyword else None)
        if search_terms:
            or_clauses = []
            for term in search_terms:
                or_clauses.append("(h.stock_code LIKE %s OR h.stock_name LIKE %s)")
                params.extend([f"%{term}%", f"%{term}%"])
            where_parts.append("(" + " OR ".join(or_clauses) + ")")

        where_sql = "WHERE " + " AND ".join(where_parts)

        # 排序白名单
        allowed_sorts = {
            'stock_code', 'stock_name', 'nw_pred_direction', 'nw_confidence',
            'nw_pred_chg', 'nw_backtest_accuracy', 'nw_actual_weekly_chg',
            'predict_date',
        }
        safe_sort = sort_by if sort_by in allowed_sorts else 'stock_code'
        # 兼容前端传 actual_weekly_chg
        if sort_by == 'actual_weekly_chg':
            safe_sort = 'nw_actual_weekly_chg'
        sort_col = f"h.{safe_sort}"
        order_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        from_sql = "FROM stock_weekly_prediction_history h"

        # 总数
        cur.execute(f"SELECT COUNT(*) as cnt {from_sql} {where_sql}", params)
        total = cur.fetchone()['cnt']

        # 数据
        cur.execute(f"""
            SELECT h.stock_code, h.stock_name, h.predict_date,
                   h.iso_year, h.iso_week,
                   h.nw_pred_direction, h.nw_confidence, h.nw_strategy, h.nw_reason,
                   h.nw_pred_chg, h.nw_pred_chg_low, h.nw_pred_chg_high,
                   h.nw_date_range, h.nw_backtest_accuracy, h.nw_backtest_samples,
                   h.nw_iso_year, h.nw_iso_week,
                   h.pred_direction as tw_pred_direction,
                   h.actual_weekly_chg as tw_actual_chg,
                   h.nw_actual_direction as nw_actual_direction,
                   h.nw_actual_weekly_chg as nw_actual_chg,
                   h.nw_is_correct as nw_is_correct,
                   h.concept_boards,
                   h.pred_direction, h.confidence,
                   h.fund_flow_signal, h.finance_score, h.board_momentum, h.vol_trend
            {from_sql}
            {where_sql}
            ORDER BY {sort_col} {order_dir}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        # 如果目标周尚未结束，屏蔽 actual 字段
        _nw_actual_fields = ['nw_actual_direction', 'nw_actual_chg', 'nw_is_correct']
        _mask_unfinished_actual(rows, _nw_actual_fields, iso_year, iso_week)

        # 汇总统计
        cur.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(h.nw_is_correct = 1) as correct,
                SUM(h.nw_is_correct = 0) as wrong,
                SUM(h.nw_is_correct IS NULL) as pending,
                ROUND(
                    SUM(h.nw_is_correct = 1)
                    / NULLIF(SUM(h.nw_is_correct IS NOT NULL), 0) * 100, 1
                ) as accuracy,
                SUM(h.nw_pred_direction = 'UP') as pred_up,
                SUM(h.nw_pred_direction = 'DOWN') as pred_down,
                SUM(h.nw_confidence = 'high' AND h.nw_is_correct = 1) as high_correct,
                SUM(h.nw_confidence = 'high' AND h.nw_is_correct IS NOT NULL) as high_total,
                SUM(h.nw_confidence = 'reference' AND h.nw_is_correct = 1) as ref_correct,
                SUM(h.nw_confidence = 'reference' AND h.nw_is_correct IS NOT NULL) as ref_total,
                ROUND(AVG(h.nw_actual_weekly_chg), 2) as avg_actual_chg,
                ROUND(AVG(h.nw_pred_chg), 2) as avg_pred_chg,
                ROUND(AVG(h.nw_backtest_accuracy), 1) as avg_backtest_accuracy,
                MAX(h.predict_date) as predict_date,
                MAX(h.nw_date_range) as nw_date_range
            {from_sql}
            {where_sql}
        """, params)
        summary = cur.fetchone() or {}
        summary['iso_year'] = iso_year
        summary['iso_week'] = iso_week

        # 目标周未结束时，汇总中的 actual 统计也要屏蔽
        if not _is_target_week_finished(iso_year, iso_week):
            total_cnt = summary.get('total') or 0
            summary['correct'] = 0
            summary['wrong'] = 0
            summary['pending'] = total_cnt
            summary['accuracy'] = None
            summary['high_correct'] = 0
            summary['high_total'] = 0
            summary['ref_correct'] = 0
            summary['ref_total'] = 0
            summary['avg_actual_chg'] = None

        return rows, total, summary
    finally:
        cur.close()
        conn.close()



def get_v20_prediction_verification(iso_year: int = None, iso_week: int = None,
                                    keywords: list[str] = None,
                                    direction_filter: str = None,
                                    result_filter: str = None,
                                    sort_by: str = 'stock_code',
                                    sort_dir: str = 'asc',
                                    limit: int = 50, offset: int = 0):
    """获取V20量价超跌反弹预测验证数据。

    iso_year/iso_week 为目标周（即预测要验证的那一周），与多模型叠加逻辑一致。
    使用 nw_iso_year/nw_iso_week 匹配目标周。

    返回 (rows, total_count, summary)。
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        from_sql = "FROM stock_weekly_prediction_history h"
        where_parts = [
            "h.v20_pred_direction IS NOT NULL",
            "h.v20_pred_direction != ''",
            "h.nw_iso_year IS NOT NULL",
            "h.nw_iso_week IS NOT NULL",
        ]
        params = []

        if iso_year and iso_week:
            where_parts.append("h.nw_iso_year = %s AND h.nw_iso_week = %s")
            params += [iso_year, iso_week]

        if keywords:
            kw_parts = []
            for kw in keywords:
                kw_parts.append("(h.stock_code LIKE %s OR h.stock_name LIKE %s)")
                params += [f'%{kw}%', f'%{kw}%']
            where_parts.append('(' + ' OR '.join(kw_parts) + ')')

        if direction_filter:
            where_parts.append("h.v20_pred_direction = %s")
            params.append(direction_filter)

        # 目标周未结束时，忽略结果筛选（DB中可能有脏数据）
        _week_finished = _is_target_week_finished(iso_year, iso_week)
        if _week_finished:
            if result_filter == 'correct':
                where_parts.append("h.v20_is_correct = 1")
            elif result_filter == 'wrong':
                where_parts.append("h.v20_is_correct = 0")
            elif result_filter == 'pending':
                where_parts.append("h.v20_is_correct IS NULL")

        where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        # total
        cur.execute(f"SELECT COUNT(*) as cnt {from_sql} {where_sql}", params)
        total = cur.fetchone()['cnt']

        # sort
        allowed_sorts = {
            'stock_code', 'stock_name', 'v20_pred_direction', 'v20_confidence',
            'v20_rule_name', 'v20_backtest_acc', 'v20_actual_5d_chg', 'v20_is_correct',
            'predict_date',
        }
        safe_sort = sort_by if sort_by in allowed_sorts else 'stock_code'
        order_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        cur.execute(f"""
            SELECT h.stock_code, h.stock_name, h.predict_date, h.iso_year, h.iso_week,
                   h.v20_pred_direction, h.v20_confidence, h.v20_rule_name, h.v20_reason,
                   h.v20_backtest_acc, h.v20_matched_count, h.v20_matched_rules,
                   h.v20_pos, h.v20_vr5, h.v20_ma20d, h.v20_cdn,
                   h.v20_actual_direction, h.v20_actual_5d_chg, h.v20_is_correct,
                   h.concept_boards
            {from_sql}
            {where_sql}
            ORDER BY h.{safe_sort} {order_dir}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        # 如果目标周尚未结束，屏蔽 actual 字段（防止显示错误回填数据）
        _v20_actual_fields = ['v20_actual_direction', 'v20_actual_5d_chg', 'v20_is_correct']
        _mask_unfinished_actual(rows, _v20_actual_fields, iso_year, iso_week)

        cur.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(h.v20_is_correct = 1) as correct,
                SUM(h.v20_is_correct = 0) as wrong,
                SUM(h.v20_is_correct IS NULL) as pending,
                ROUND(SUM(h.v20_is_correct = 1) / NULLIF(SUM(h.v20_is_correct IS NOT NULL), 0) * 100, 1) as accuracy,
                SUM(h.v20_pred_direction = 'UP') as pred_up,
                SUM(h.v20_confidence = 'high' AND h.v20_is_correct = 1) as high_correct,
                SUM(h.v20_confidence = 'high' AND h.v20_is_correct IS NOT NULL) as high_total,
                SUM(h.v20_confidence = 'medium' AND h.v20_is_correct = 1) as med_correct,
                SUM(h.v20_confidence = 'medium' AND h.v20_is_correct IS NOT NULL) as med_total,
                ROUND(AVG(h.v20_actual_5d_chg), 2) as avg_actual_chg,
                ROUND(AVG(h.v20_backtest_acc), 1) as avg_backtest_acc,
                MAX(h.predict_date) as predict_date
            {from_sql}
            {where_sql}
        """, params)
        summary = cur.fetchone() or {}
        summary['iso_year'] = iso_year
        summary['iso_week'] = iso_week

        # 目标周未结束时，汇总中的 actual 统计也要屏蔽
        if not _is_target_week_finished(iso_year, iso_week):
            total_cnt = summary.get('total') or 0
            summary['correct'] = 0
            summary['wrong'] = 0
            summary['pending'] = total_cnt
            summary['accuracy'] = None
            summary['high_correct'] = 0
            summary['high_total'] = 0
            summary['med_correct'] = 0
            summary['med_total'] = 0
            summary['avg_actual_chg'] = None

        return rows, total, summary
    finally:
        cur.close()
        conn.close()


def backfill_v20_actual_results(results: list[dict]):
    """回填V20量价超跌反弹预测的实际5日结果。

    results: [{'stock_code': ..., 'iso_year': ..., 'iso_week': ...,
               'v20_actual_direction': 'UP'/'DOWN',
               'v20_actual_5d_chg': float, 'v20_is_correct': 0/1}, ...]
    """
    if not results:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany("""
            UPDATE stock_weekly_prediction_history
            SET v20_actual_direction = %(v20_actual_direction)s,
                v20_actual_5d_chg = %(v20_actual_5d_chg)s,
                v20_is_correct = %(v20_is_correct)s
            WHERE stock_code = %(stock_code)s
              AND iso_year = %(iso_year)s AND iso_week = %(iso_week)s
        """, results)
        conn.commit()
        logger.info("V20回填实际结果: %d 条", len(results))
    finally:
        cur.close()
        conn.close()


def backfill_v30_actual_results(results: list[dict]):
    """回填V30情绪因子预测的实际5日结果。

    results: [{'stock_code': ..., 'iso_year': ..., 'iso_week': ...,
               'v30_actual_direction': 'UP'/'DOWN',
               'v30_actual_5d_chg': float, 'v30_is_correct': 0/1}, ...]
    """
    if not results:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.executemany("""
            UPDATE stock_weekly_prediction_history
            SET v30_actual_direction = %(v30_actual_direction)s,
                v30_actual_5d_chg = %(v30_actual_5d_chg)s,
                v30_is_correct = %(v30_is_correct)s
            WHERE stock_code = %(stock_code)s
              AND iso_year = %(iso_year)s AND iso_week = %(iso_week)s
        """, results)
        conn.commit()
        logger.info("V30回填实际结果: %d 条", len(results))
    finally:
        cur.close()
        conn.close()


def get_v30_prediction_verification(iso_year: int = None, iso_week: int = None,
                                    keyword: str = None, keywords: list[str] = None,
                                    direction_filter: str = None,
                                    result_filter: str = None,
                                    sort_by: str = 'stock_code',
                                    sort_dir: str = 'asc',
                                    limit: int = 50, offset: int = 0) -> tuple[list[dict], int, dict]:
    """获取V30情绪因子5日预测验证数据。

    iso_year/iso_week 为目标周（即预测要验证的那一周），与多模型叠加逻辑一致。
    使用 nw_iso_year/nw_iso_week 匹配目标周。

    返回 (rows, total_count, summary)。
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        _stock_filter = (
            "(h.stock_code LIKE '6%%.SH' OR h.stock_code LIKE '0%%.SZ' OR h.stock_code LIKE '3%%.SZ')"
            " AND h.stock_code NOT LIKE '399%%'"
            " AND h.stock_code != '000001.SH'"
        )

        if not iso_year or not iso_week:
            cur.execute(f"""
                SELECT DISTINCT nw_iso_year, nw_iso_week
                FROM stock_weekly_prediction_history
                WHERE {_stock_filter}
                  AND v30_pred_direction IS NOT NULL
                  AND nw_iso_year IS NOT NULL AND nw_iso_week IS NOT NULL
                ORDER BY nw_iso_year DESC, nw_iso_week DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                return [], 0, {}
            iso_year = row['nw_iso_year']
            iso_week = row['nw_iso_week']

        where_parts = [
            "h.nw_iso_year = %s",
            "h.nw_iso_week = %s",
            "(h.stock_code LIKE '6%%.SH' OR h.stock_code LIKE '0%%.SZ' OR h.stock_code LIKE '3%%.SZ')",
            "h.stock_code NOT LIKE '399%%'",
            "h.stock_code != '000001.SH'",
            "h.v30_pred_direction IS NOT NULL",
            "h.v30_pred_direction != ''",
        ]
        params = [iso_year, iso_week]

        if direction_filter:
            where_parts.append("h.v30_pred_direction = %s")
            params.append(direction_filter)

        # 目标周未结束时，忽略结果筛选（DB中可能有脏数据）
        _week_finished = _is_target_week_finished(iso_year, iso_week)
        if _week_finished:
            if result_filter == 'correct':
                where_parts.append("h.v30_is_correct = 1")
            elif result_filter == 'wrong':
                where_parts.append("h.v30_is_correct = 0")
            elif result_filter == 'pending':
                where_parts.append("h.v30_is_correct IS NULL")

        search_terms = keywords or ([keyword] if keyword else None)
        if search_terms:
            or_clauses = []
            for term in search_terms:
                or_clauses.append("(h.stock_code LIKE %s OR h.stock_name LIKE %s)")
                params.extend([f"%{term}%", f"%{term}%"])
            where_parts.append("(" + " OR ".join(or_clauses) + ")")

        where_sql = "WHERE " + " AND ".join(where_parts)

        allowed_sorts = {
            'stock_code', 'stock_name', 'v30_pred_direction', 'v30_confidence',
            'v30_strategy', 'v30_composite_score', 'v30_actual_5d_chg', 'v30_is_correct',
            'v30_sent_agree', 'v30_tech_agree', 'predict_date',
        }
        safe_sort = sort_by if sort_by in allowed_sorts else 'stock_code'
        order_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        from_sql = "FROM stock_weekly_prediction_history h"

        cur.execute(f"SELECT COUNT(*) as cnt {from_sql} {where_sql}", params)
        total = cur.fetchone()['cnt']

        cur.execute(f"""
            SELECT h.stock_code, h.stock_name, h.predict_date,
                   h.iso_year, h.iso_week,
                   h.v30_pred_direction, h.v30_confidence, h.v30_strategy, h.v30_reason,
                   h.v30_composite_score, h.v30_sent_agree, h.v30_tech_agree, h.v30_mkt_ret_20d,
                   h.v30_actual_direction, h.v30_actual_5d_chg, h.v30_is_correct,
                   h.pred_direction as tw_pred_direction,
                   h.actual_weekly_chg as tw_actual_chg,
                   h.concept_boards,
                   h.pred_direction, h.confidence, h.nw_pred_direction,
                   h.fund_flow_signal, h.finance_score, h.board_momentum, h.vol_trend
            {from_sql}
            {where_sql}
            ORDER BY h.{safe_sort} {order_dir}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        # 如果目标周尚未结束，屏蔽 actual 字段（防止显示错误回填数据）
        _v30_actual_fields = ['v30_actual_direction', 'v30_actual_5d_chg', 'v30_is_correct']
        _mask_unfinished_actual(rows, _v30_actual_fields, iso_year, iso_week)

        cur.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(h.v30_is_correct = 1) as correct,
                SUM(h.v30_is_correct = 0) as wrong,
                SUM(h.v30_is_correct IS NULL) as pending,
                ROUND(SUM(h.v30_is_correct = 1) / NULLIF(SUM(h.v30_is_correct IS NOT NULL), 0) * 100, 1) as accuracy,
                SUM(h.v30_pred_direction = 'UP') as pred_up,
                SUM(h.v30_confidence = 'high' AND h.v30_is_correct = 1) as high_correct,
                SUM(h.v30_confidence = 'high' AND h.v30_is_correct IS NOT NULL) as high_total,
                SUM(h.v30_confidence = 'medium' AND h.v30_is_correct = 1) as med_correct,
                SUM(h.v30_confidence = 'medium' AND h.v30_is_correct IS NOT NULL) as med_total,
                SUM(h.v30_confidence = 'low' AND h.v30_is_correct = 1) as low_correct,
                SUM(h.v30_confidence = 'low' AND h.v30_is_correct IS NOT NULL) as low_total,
                ROUND(AVG(h.v30_actual_5d_chg), 2) as avg_actual_chg,
                ROUND(AVG(h.v30_composite_score), 2) as avg_composite_score,
                MAX(h.predict_date) as predict_date
            {from_sql}
            {where_sql}
        """, params)
        summary = cur.fetchone() or {}
        summary['iso_year'] = iso_year
        summary['iso_week'] = iso_week

        # 目标周未结束时，汇总中的 actual 统计也要屏蔽
        if not _is_target_week_finished(iso_year, iso_week):
            total_cnt = summary.get('total') or 0
            summary['correct'] = 0
            summary['wrong'] = 0
            summary['pending'] = total_cnt
            summary['accuracy'] = None
            summary['high_correct'] = 0
            summary['high_total'] = 0
            summary['med_correct'] = 0
            summary['med_total'] = 0
            summary['low_correct'] = 0
            summary['low_total'] = 0
            summary['avg_actual_chg'] = None

        return rows, total, summary
    finally:
        cur.close()
        conn.close()


def get_available_prediction_weeks(limit: int = 20) -> list[dict]:
    """获取有预测记录的周列表（用于周选择器）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT iso_year, iso_week, COUNT(*) as stock_count,
                   MAX(predict_date) as predict_date,
                   SUM(is_correct IS NOT NULL) as verified_count,
                   SUM(is_correct = 1) as correct_count,
                   ROUND(SUM(is_correct = 1) / NULLIF(SUM(is_correct IS NOT NULL), 0) * 100, 1) as accuracy
            FROM stock_weekly_prediction_history
            WHERE (stock_code LIKE '6%%.SH' OR stock_code LIKE '0%%.SZ' OR stock_code LIKE '3%%.SZ')
              AND stock_code NOT LIKE '399%%'
              AND stock_code != '000001.SH'
            GROUP BY iso_year, iso_week
            ORDER BY iso_year DESC, iso_week DESC
            LIMIT %s
        """, (limit,))
        return cur.fetchall()
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
                ROUND(AVG(pred_chg_mae), 2) as avg_pred_chg_mae,
                ROUND(AVG(pred_chg_hit_rate), 1) as avg_pred_chg_hit_rate,
                SUM(pred_weekly_chg IS NOT NULL) as pred_chg_count,
                SUM(strategy = 'd4_strong') as d4_strong_count,
                SUM(strategy = 'd4_medium') as d4_medium_count,
                SUM(strategy = 'd4_fuzzy') as d4_fuzzy_count,
                SUM(strategy = 'd3_strong') as d3_strong_count,
                SUM(strategy = 'd3_medium') as d3_medium_count,
                SUM(strategy = 'd3_fuzzy') as d3_fuzzy_count,
                SUM(strategy = 'suspended') as suspended_strategy_count,
                SUM(nw_pred_direction IS NOT NULL) as nw_total,
                SUM(nw_pred_direction = 'UP') as nw_up_count,
                SUM(nw_pred_direction = 'DOWN') as nw_down_count,
                SUM(nw_pred_direction IS NULL AND nw_date_range IS NOT NULL) as nw_uncertain_count,
                SUM(nw_confidence = 'reference') as nw_reference_count,
                SUM(nw_confidence = 'high') as nw_high_count,
                ROUND(AVG(CASE WHEN nw_backtest_accuracy IS NOT NULL AND nw_confidence = 'high' THEN nw_backtest_accuracy END), 1) as nw_high_avg_accuracy,
                ROUND(AVG(CASE WHEN nw_backtest_accuracy IS NOT NULL AND nw_confidence = 'reference' THEN nw_backtest_accuracy END), 1) as nw_ref_avg_accuracy,
                ROUND(AVG(CASE WHEN nw_backtest_accuracy IS NOT NULL THEN nw_backtest_accuracy END), 1) as nw_avg_backtest_accuracy,
                ROUND(AVG(CASE WHEN nw_pred_chg_mae IS NOT NULL THEN nw_pred_chg_mae END), 2) as nw_avg_pred_chg_mae,
                ROUND(AVG(CASE WHEN nw_pred_chg_hit_rate IS NOT NULL THEN nw_pred_chg_hit_rate END), 1) as nw_avg_pred_chg_hit_rate,
                SUM(nw_pred_chg IS NOT NULL) as nw_pred_chg_count,
                MAX(nw_date_range) as nw_date_range,
                SUM(nm_pred_direction IS NOT NULL) as nm_total,
                SUM(nm_pred_direction = 'UP') as nm_up_count,
                ROUND(AVG(CASE WHEN nm_backtest_accuracy IS NOT NULL THEN nm_backtest_accuracy END), 1) as nm_avg_backtest_accuracy,
                ROUND(AVG(CASE WHEN nm_backtest_samples IS NOT NULL THEN nm_backtest_samples END), 0) as nm_avg_backtest_samples,
                MAX(nm_date_range) as nm_date_range,
                MAX(nm_target_year) as nm_target_year,
                MAX(nm_target_month) as nm_target_month
            FROM stock_weekly_prediction
            WHERE (stock_code LIKE '6%.SH' OR stock_code LIKE '0%.SZ' OR stock_code LIKE '3%.SZ')
              AND stock_code NOT LIKE '399%'
              AND stock_code != '000001.SH'
        """)
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def get_weekly_predictions_by_codes(stock_codes: list[str]) -> dict[str, dict]:
    """根据股票代码列表批量获取最新周预测数据，返回 {stock_code: {pred_direction, confidence, nw_pred_direction, nw_confidence, ...}}"""
    if not stock_codes:
        return {}
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        placeholders = ','.join(['%s'] * len(stock_codes))
        cur.execute(f"""
            SELECT stock_code, pred_direction, confidence, strategy,
                   nw_pred_direction, nw_confidence, nw_strategy, nw_reason,
                   nw_pred_chg, nw_backtest_accuracy,
                   fund_flow_signal, finance_score,
                   board_momentum, vol_trend
            FROM stock_weekly_prediction
            WHERE stock_code IN ({placeholders})
        """, stock_codes)
        rows = cur.fetchall()
        return {r['stock_code']: r for r in rows}
    finally:
        cur.close()
        conn.close()
