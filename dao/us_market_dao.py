"""
海外市场数据 DAO — 美股指数日K线 + 全球指数行情 + 美股/中概股涨幅榜 + 美股个股日K线

表设计：
1. us_index_kline          — 美股指数日K线（按日期 upsert）
2. global_index_realtime   — 全球指数当日行情快照（按 trade_date + index_code upsert）
3. us_stock_ranking        — 涨幅榜快照（中国概念股/知名美股/互联网中国，按 trade_date + category + stock_code upsert）
4. us_stock_kline          — 美股半导体龙头个股日K线（按 stock_code + trade_date upsert）
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

DDL_US_STOCK_KLINE = """
CREATE TABLE IF NOT EXISTS us_stock_kline (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_code    VARCHAR(20)    NOT NULL COMMENT '股票代码 NVDA/AMD/AAPL等',
    stock_name    VARCHAR(60)    DEFAULT NULL COMMENT '股票名称',
    sector        VARCHAR(30)    DEFAULT NULL COMMENT '细分领域: 芯片设计/半导体设备/存储等',
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
    UNIQUE KEY uk_code_date (stock_code, trade_date),
    INDEX idx_trade_date (trade_date),
    INDEX idx_sector_date (sector, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='美股半导体龙头个股日K线'
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
        cursor.execute(DDL_US_STOCK_KLINE)
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


# ═══════════════════════════════════════════════════════════════
# 写入 — 全球指数当日行情（按日期 upsert）
# ═══════════════════════════════════════════════════════════════

_UPSERT_REALTIME_SQL = """
INSERT INTO global_index_realtime
    (index_code, index_name, region, trade_date, latest_price, change_pct, change_amt,
     volume, amount, open_price, prev_close, high_price, low_price, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    index_name   = VALUES(index_name),
    region       = VALUES(region),
    latest_price = VALUES(latest_price),
    change_pct   = VALUES(change_pct),
    change_amt   = VALUES(change_amt),
    volume       = VALUES(volume),
    amount       = VALUES(amount),
    open_price   = VALUES(open_price),
    prev_close   = VALUES(prev_close),
    high_price   = VALUES(high_price),
    low_price    = VALUES(low_price),
    updated_at   = VALUES(updated_at)
"""


def batch_upsert_global_index_realtime(cursor, region: str, trade_date: str, items: list[dict]):
    """批量写入全球指数行情（按 index_code + trade_date 覆盖）"""
    now = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for item in items:
        vol = item.get("成交量")
        rows.append((
            item.get("代码") or item.get("指数代码", ""),
            item.get("名称") or item.get("指数名称", ""),
            region,
            trade_date,
            item.get("最新价"),
            item.get("涨跌幅(%)"),
            item.get("涨跌额"),
            int(vol) if vol is not None and vol != "-" else None,
            str(item.get("成交额", "")) if item.get("成交额") != "-" else None,
            item.get("今开") or item.get("open_price"),
            item.get("昨收") or item.get("prev_close"),
            item.get("最高") or item.get("high_price"),
            item.get("最低") or item.get("low_price"),
            now,
        ))
    if rows:
        cursor.executemany(_UPSERT_REALTIME_SQL, rows)


# ═══════════════════════════════════════════════════════════════
# 写入 — 涨幅榜快照（按日期 + 分类 upsert）
# ═══════════════════════════════════════════════════════════════

_UPSERT_RANKING_SQL = """
INSERT INTO us_stock_ranking
    (trade_date, category, stock_code, stock_name, latest_price, change_pct, change_amt,
     volume, amount, open_price, prev_close, high_price, low_price, rank_order, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    stock_name   = VALUES(stock_name),
    latest_price = VALUES(latest_price),
    change_pct   = VALUES(change_pct),
    change_amt   = VALUES(change_amt),
    volume       = VALUES(volume),
    amount       = VALUES(amount),
    open_price   = VALUES(open_price),
    prev_close   = VALUES(prev_close),
    high_price   = VALUES(high_price),
    low_price    = VALUES(low_price),
    rank_order   = VALUES(rank_order),
    updated_at   = VALUES(updated_at)
"""


def batch_upsert_stock_ranking(cursor, category: str, trade_date: str, items: list[dict]):
    """批量写入涨幅榜数据（按 trade_date + category + stock_code 覆盖）"""
    now = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for idx, item in enumerate(items, 1):
        rows.append((
            trade_date,
            category,
            item.get("代码", ""),
            item.get("名称", ""),
            item.get("最新价"),
            item.get("涨跌幅(%)"),
            item.get("涨跌额"),
            item.get("成交量"),
            item.get("成交额"),
            item.get("今开"),
            item.get("昨收"),
            item.get("最高"),
            item.get("最低"),
            idx,
            now,
        ))
    if rows:
        cursor.executemany(_UPSERT_RANKING_SQL, rows)


# ═══════════════════════════════════════════════════════════════
# 查询接口
# ═══════════════════════════════════════════════════════════════

def get_us_index_kline(index_code: str, limit: int = 120) -> list[dict]:
    """查询美股指数日K线（由新到旧）"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT trade_date, open_price, close_price, high_price, low_price, "
            "       volume, amount, amplitude, change_pct, change_amt, turnover "
            "FROM us_index_kline "
            "WHERE index_code = %s ORDER BY trade_date DESC LIMIT %s",
            (index_code, limit),
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def get_global_index_realtime(trade_date: str, region: str = None) -> list[dict]:
    """查询全球指数当日行情，可按地区过滤"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        if region:
            cursor.execute(
                "SELECT index_code, index_name, region, trade_date, latest_price, "
                "       change_pct, change_amt, volume, amount, open_price, prev_close, "
                "       high_price, low_price "
                "FROM global_index_realtime "
                "WHERE trade_date = %s AND region = %s ORDER BY change_pct DESC",
                (trade_date, region),
            )
        else:
            cursor.execute(
                "SELECT index_code, index_name, region, trade_date, latest_price, "
                "       change_pct, change_amt, volume, amount, open_price, prev_close, "
                "       high_price, low_price "
                "FROM global_index_realtime "
                "WHERE trade_date = %s ORDER BY change_pct DESC",
                (trade_date,),
            )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def get_us_stock_ranking(trade_date: str, category: str, limit: int = 50) -> list[dict]:
    """查询涨幅榜数据"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT stock_code, stock_name, latest_price, change_pct, change_amt, "
            "       volume, amount, open_price, prev_close, high_price, low_price, rank_order "
            "FROM us_stock_ranking "
            "WHERE trade_date = %s AND category = %s ORDER BY rank_order ASC LIMIT %s",
            (trade_date, category, limit),
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def get_latest_trade_date_for_index(index_code: str = "NDX") -> str | None:
    """获取指定指数最新的交易日期"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT MAX(trade_date) FROM us_index_kline WHERE index_code = %s",
            (index_code,),
        )
        row = cursor.fetchone()
        return str(row[0]) if row and row[0] else None
    finally:
        cursor.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════
# 写入 — 美股个股日K线（按 stock_code + trade_date upsert）
# ═══════════════════════════════════════════════════════════════

_UPSERT_STOCK_KLINE_SQL = """
INSERT INTO us_stock_kline
    (stock_code, stock_name, sector, trade_date, open_price, close_price,
     high_price, low_price, volume, amount, amplitude, change_pct,
     change_amt, turnover, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    stock_name  = VALUES(stock_name),
    sector      = VALUES(sector),
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


def batch_upsert_stock_kline(cursor, stock_code: str, stock_name: str,
                             sector: str, kline_list: list[dict]):
    """批量写入美股个股日K线数据（按 stock_code + trade_date 覆盖）"""
    now = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for k in kline_list:
        rows.append((
            stock_code,
            stock_name,
            sector,
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
        cursor.executemany(_UPSERT_STOCK_KLINE_SQL, rows)


def get_us_stock_kline_range(
    stock_code: str,
    start_date: str = None,
    end_date: str = None,
    limit: int = 120,
) -> list[dict]:
    """查询美股个股日K线（按日期升序）"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        sql = (
            "SELECT stock_code, stock_name, sector, trade_date, "
            "       open_price, close_price, high_price, low_price, "
            "       volume, amount, amplitude, change_pct, change_amt, turnover "
            "FROM us_stock_kline "
            "WHERE stock_code = %s"
        )
        params = [stock_code]
        if start_date:
            sql += " AND trade_date >= %s"
            params.append(start_date)
        if end_date:
            sql += " AND trade_date <= %s"
            params.append(end_date)
        sql += " ORDER BY trade_date ASC LIMIT %s"
        params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def get_us_stock_latest_date(stock_code: str) -> str | None:
    """获取指定美股个股最新的交易日期"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT MAX(trade_date) FROM us_stock_kline WHERE stock_code = %s",
            (stock_code,),
        )
        row = cursor.fetchone()
        return str(row[0]) if row and row[0] else None
    finally:
        cursor.close()
        conn.close()


def get_us_stock_sector_avg_change(trade_date: str, sector: str = None) -> list[dict]:
    """查询指定日期各细分领域的平均涨跌幅

    Args:
        trade_date: 交易日期
        sector: 可选，指定细分领域

    Returns:
        list[dict]: [{sector, avg_change_pct, stock_count}]
    """
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        if sector:
            cursor.execute(
                "SELECT sector, "
                "       ROUND(AVG(change_pct), 4) AS avg_change_pct, "
                "       COUNT(*) AS stock_count "
                "FROM us_stock_kline "
                "WHERE trade_date = %s AND sector = %s "
                "GROUP BY sector",
                (trade_date, sector),
            )
        else:
            cursor.execute(
                "SELECT sector, "
                "       ROUND(AVG(change_pct), 4) AS avg_change_pct, "
                "       COUNT(*) AS stock_count "
                "FROM us_stock_kline "
                "WHERE trade_date = %s "
                "GROUP BY sector ORDER BY avg_change_pct DESC",
                (trade_date,),
            )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()
