"""
海外市场数据 DAO — 美股指数日K线 + 全球指数行情 + 美股/中概股涨幅榜

表设计：
1. us_index_kline          — 美股指数日K线（按日期 upsert）
2. global_index_realtime   — 全球指数当日行情快照（按 trade_date + index_code upsert）
3. us_stock_ranking        — 涨幅榜快照（中国概念股/知名美股/互联网中国，按 trade_date + category + stock_code upsert）
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)
_CST = ZoneInfo("Asia/Shanghai")

# ═══════════════════════════════════════════════════════════════
# DDL
# ═══════════════════════════════════════════════════════════════

DDL_US_INDEX_KLINE = """
CREATE TABLE IF NOT EXISTS us_index_kline (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    index_code    VARCHAR(20)    NOT NULL COMMENT '指数代码 NDX/DJIA/SPX',
    trade_date    DATE           NOT NULL COMMENT '交易日期',
    open_price    DECIMAL(16,4)  DEFAULT NULL,
    close_price   DECIMAL(16,4)  DEFAULT NULL,
    high_price    DECIMAL(16,4)  DEFAULT NULL,
    low_price     DECIMAL(16,4)  DEFAULT NULL,
    volume        BIGINT         DEFAULT NULL COMMENT '成交量',
    amount        VARCHAR(30)    DEFAULT NULL COMMENT '成交额',
    amplitude     DECIMAL(10,4)  DEFAULT NULL COMMENT '振幅(%)',
    change_pct    DECIMAL(10,4)  DEFAULT NULL COMMENT '涨跌幅(%)',
    change_amt    DECIMAL(16,4)  DEFAULT NULL COMMENT '涨跌额',
    turnover      DECIMAL(10,4)  DEFAULT NULL COMMENT '换手率(%)',
    updated_at    DATETIME       NOT NULL,
    UNIQUE KEY uk_code_date (index_code, trade_date),
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='美股指数日K线'
"""

DDL_GLOBAL_INDEX_REALTIME = """
CREATE TABLE IF NOT EXISTS global_index_realtime (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    index_code    VARCHAR(20)    NOT NULL COMMENT '指数代码',
    index_name    VARCHAR(60)    DEFAULT NULL COMMENT '指数名称',
    region        VARCHAR(20)    NOT NULL COMMENT '地区: americas/europe/asia/australia',
    trade_date    DATE           NOT NULL COMMENT '交易日期',
    latest_price  DECIMAL(16,4)  DEFAULT NULL,
    change_pct    DECIMAL(10,4)  DEFAULT NULL COMMENT '涨跌幅(%)',
    change_amt    DECIMAL(16,4)  DEFAULT NULL COMMENT '涨跌额',
    volume        BIGINT         DEFAULT NULL,
    amount        VARCHAR(30)    DEFAULT NULL,
    open_price    DECIMAL(16,4)  DEFAULT NULL,
    prev_close    DECIMAL(16,4)  DEFAULT NULL,
    high_price    DECIMAL(16,4)  DEFAULT NULL,
    low_price     DECIMAL(16,4)  DEFAULT NULL,
    updated_at    DATETIME       NOT NULL,
    UNIQUE KEY uk_code_date (index_code, trade_date),
    INDEX idx_region_date (region, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全球指数当日行情'
"""

DDL_US_STOCK_RANKING = """
CREATE TABLE IF NOT EXISTS us_stock_ranking (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date    DATE           NOT NULL COMMENT '交易日期',
    category      VARCHAR(30)    NOT NULL COMMENT '分类: china_concept/famous_us/internet_china',
    stock_code    VARCHAR(20)    NOT NULL COMMENT '股票代码',
    stock_name    VARCHAR(80)    DEFAULT NULL,
    latest_price  DECIMAL(16,4)  DEFAULT NULL,
    change_pct    DECIMAL(10,4)  DEFAULT NULL COMMENT '涨跌幅(%)',
    change_amt    DECIMAL(16,4)  DEFAULT NULL COMMENT '涨跌额',
    volume        BIGINT         DEFAULT NULL,
    amount        DECIMAL(20,2)  DEFAULT NULL,
    open_price    DECIMAL(16,4)  DEFAULT NULL,
    prev_close    DECIMAL(16,4)  DEFAULT NULL,
    high_price    DECIMAL(16,4)  DEFAULT NULL,
    low_price     DECIMAL(16,4)  DEFAULT NULL,
    rank_order    INT            DEFAULT NULL COMMENT '排名序号',
    updated_at    DATETIME       NOT NULL,
    UNIQUE KEY uk_date_cat_code (trade_date, category, stock_code),
    INDEX idx_category_date (category, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='美股/中概股涨幅榜'
"""


# ═══════════════════════════════════════════════════════════════
# 建表
# ═══════════════════════════════════════════════════════════════

def create_us_market_tables(cursor=None):
    """创建海外市场相关的所有表"""
    own_conn = cursor is None
    conn = None
    if own_conn:
        conn = get_connection()
        cursor = conn.cursor()
    try:
        cursor.execute(DDL_US_INDEX_KLINE)
        cursor.execute(DDL_GLOBAL_INDEX_REALTIME)
        cursor.execute(DDL_US_STOCK_RANKING)
        if own_conn:
            conn.commit()
        logger.info("海外市场表创建/检查完成 ✓")
    finally:
        if own_conn:
            cursor.close()
            conn.close()


# ═══════════════════════════════════════════════════════════════
# 写入 — 美股指数日K线（按日期 upsert）
# ═══════════════════════════════════════════════════════════════

_UPSERT_KLINE_SQL = """
INSERT INTO us_index_kline
    (index_code, trade_date, open_price, close_price, high_price, low_price,
     volume, amount, amplitude, change_pct, change_amt, turnover, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    open_price  = VALUES(open_price),
    close_price = VALUES(close_price),
    high_price  = VALUES(high_price),
    low_price   = VALUES(low_price),
    volume      = VALUES(volume),
    amount      = VALUES(amount),
    amplitude   = VALUES(amplitude),
    change_pct  = VALUES(change_pct),
    change_amt  = VALUES(change_amt),
    turnover    = VALUES(turnover),
    updated_at  = VALUES(updated_at)
"""


def batch_upsert_index_kline(cursor, index_code: str, kline_list: list[dict]):
    """批量写入美股指数日K线数据（按 index_code + trade_date 覆盖）"""
    now = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for k in kline_list:
        rows.append((
            index_code,
            k.get("日期"),
            k.get("开盘价"),
            k.get("收盘价"),
            k.get("最高价"),
            k.get("最低价"),
            int(k["成交量"]) if k.get("成交量") is not None else None,
            k.get("成交额"),
            k.get("振幅(%)"),
            k.get("涨跌幅(%)"),
            k.get("涨跌额"),
            k.get("换手率(%)"),
            now,
        ))
    if rows:
        cursor.executemany(_UPSERT_KLINE_SQL, rows)
