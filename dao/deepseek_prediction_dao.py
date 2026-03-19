"""
DeepSeek 周预测 DAO — 管理 deepseek_weekly_prediction 表
"""
import logging
from datetime import datetime

from dao import get_connection

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS deepseek_weekly_prediction (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码(如600519.SH)',
    stock_name VARCHAR(100) COMMENT '股票名称',
    predict_date VARCHAR(20) NOT NULL COMMENT '预测执行日期',
    iso_year INT NOT NULL COMMENT '预测基于的ISO年',
    iso_week INT NOT NULL COMMENT '预测基于的ISO周',
    target_iso_year INT NOT NULL COMMENT '预测目标ISO年(下周)',
    target_iso_week INT NOT NULL COMMENT '预测目标ISO周(下周)',
    target_date_range VARCHAR(50) COMMENT '预测目标周日期范围',
    pred_direction VARCHAR(12) NOT NULL COMMENT '预测方向: DOWN/UNCERTAIN',
    confidence DOUBLE COMMENT 'LLM置信度(0~1)',
    justification VARCHAR(200) COMMENT 'LLM预测理由',
    prefilter_pass TINYINT DEFAULT 0 COMMENT '是否通过预过滤(1=通过,0=被过滤)',
    prefilter_reason VARCHAR(200) COMMENT '预过滤原因',
    this_week_chg DOUBLE COMMENT '本周涨跌幅(%)',
    market_chg DOUBLE COMMENT '大盘本周涨跌幅(%)',
    price_pos_60 DOUBLE COMMENT '60日价格位置(0~1)',
    vol_ratio DOUBLE COMMENT '量比',
    prev_week_chg DOUBLE COMMENT '前一周涨跌幅(%)',
    last_day_chg DOUBLE COMMENT '最后一天涨跌幅(%)',
    consec_up INT COMMENT '连涨天数',
    actual_direction VARCHAR(4) COMMENT '实际方向(回填): UP/DOWN',
    actual_chg DOUBLE COMMENT '实际下周涨跌幅(%)(回填)',
    is_correct TINYINT COMMENT '预测是否正确(回填)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_stock_week (stock_code, iso_year, iso_week),
    INDEX idx_target_week (target_iso_year, target_iso_week),
    INDEX idx_direction (pred_direction),
    INDEX idx_predict_date (predict_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DeepSeek周预测历史记录'
"""


def ensure_table():
    """确保表存在。"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(_CREATE_TABLE)
        conn.commit()
        logger.info("deepseek_weekly_prediction 表已就绪")
    except Exception as e:
        logger.warning("创建 deepseek_weekly_prediction 表失败: %s", e)
    finally:
        conn.close()


def batch_insert_predictions(predictions: list[dict]):
    """批量插入预测记录。"""
    if not predictions:
        return
    conn = get_connection()
    cur = conn.cursor()
    cols = [
        'stock_code', 'stock_name', 'predict_date',
        'iso_year', 'iso_week', 'target_iso_year', 'target_iso_week',
        'target_date_range', 'pred_direction', 'confidence', 'justification',
        'prefilter_pass', 'prefilter_reason',
        'this_week_chg', 'market_chg', 'price_pos_60', 'vol_ratio',
        'prev_week_chg', 'last_day_chg', 'consec_up',
    ]
    cols_str = ', '.join(cols)
    vals_str = ', '.join(f'%({c})s' for c in cols)
    sql = f"INSERT INTO deepseek_weekly_prediction ({cols_str}) VALUES ({vals_str})"
    try:
        rows = []
        for p in predictions:
            row = {c: p.get(c) for c in cols}
            rows.append(row)
        cur.executemany(sql, rows)
        conn.commit()
        logger.info("插入 %d 条 DeepSeek 预测记录", len(rows))
    except Exception as e:
        conn.rollback()
        logger.error("插入 DeepSeek 预测失败: %s", e)
        raise
    finally:
        conn.close()


def get_predictions_by_week(iso_year: int, iso_week: int) -> list[dict]:
    """获取某周的所有预测。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM deepseek_weekly_prediction "
            "WHERE iso_year = %s AND iso_week = %s ORDER BY stock_code",
            (iso_year, iso_week)
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_prediction_history(stock_code: str, limit: int = 20) -> list[dict]:
    """获取某只股票的预测历史。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM deepseek_weekly_prediction "
            "WHERE stock_code = %s ORDER BY predict_date DESC LIMIT %s",
            (stock_code, limit)
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_latest_predictions(limit: int = 200) -> list[dict]:
    """获取最新一批预测（按predict_date最新）。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT predict_date FROM deepseek_weekly_prediction "
            "ORDER BY predict_date DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return []
        latest_date = row['predict_date']
        cur.execute(
            "SELECT * FROM deepseek_weekly_prediction "
            "WHERE predict_date = %s ORDER BY stock_code LIMIT %s",
            (latest_date, limit)
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_accuracy_stats() -> dict:
    """获取预测准确率统计。"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN pred_direction = 'DOWN' THEN 1 ELSE 0 END) as down_count,
                SUM(CASE WHEN pred_direction = 'DOWN' AND is_correct = 1 THEN 1 ELSE 0 END) as down_correct,
                SUM(CASE WHEN pred_direction = 'DOWN' AND is_correct IS NOT NULL THEN 1 ELSE 0 END) as down_verified
            FROM deepseek_weekly_prediction
        """)
        row = cur.fetchone()
        if not row or row['down_verified'] == 0:
            return {'total': row['total'] if row else 0, 'accuracy': None}
        return {
            'total': row['total'],
            'down_count': row['down_count'],
            'down_verified': row['down_verified'],
            'down_correct': row['down_correct'],
            'accuracy': round(row['down_correct'] / row['down_verified'] * 100, 1),
        }
    finally:
        conn.close()
