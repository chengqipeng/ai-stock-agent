"""
股票新闻公告 DAO — MySQL 单表版

存储四类信息：公司新闻(news)、公司公告(notice)、行业资讯(industry)、研究报告(report)
含正文内容字段 content。
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")

TABLE_NAME = "stock_news"


def create_news_table(cursor=None):
    """创建统一新闻公告表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
            news_type VARCHAR(20) NOT NULL COMMENT '类型: news/notice/industry/report',
            title VARCHAR(500) NOT NULL COMMENT '标题',
            url VARCHAR(1000) COMMENT '链接地址',
            publish_date VARCHAR(20) COMMENT '发布日期',
            publish_time VARCHAR(30) COMMENT '发布时间(含时分)',
            source VARCHAR(100) COMMENT '来源',
            content MEDIUMTEXT COMMENT '正文内容',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_code_type_title_date (stock_code, news_type, title(200), publish_date),
            INDEX idx_stock_code (stock_code),
            INDEX idx_news_type (news_type),
            INDEX idx_publish_date (publish_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(ddl)
    # 兼容已有表：添加 content 列
    try:
        cursor.execute(
            f"ALTER TABLE {TABLE_NAME} ADD COLUMN "
            f"content MEDIUMTEXT COMMENT '正文内容' AFTER source"
        )
    except Exception:
        pass  # 列已存在
    if own:
        conn.commit()
        cursor.close()
        conn.close()


# ─────────────────── 写入 ───────────────────

_UPSERT_SQL = f"""
    INSERT INTO {TABLE_NAME}
    (stock_code, news_type, title, url, publish_date, publish_time, source, content, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        url = VALUES(url),
        publish_time = VALUES(publish_time),
        source = VALUES(source),
        content = IF(VALUES(content) IS NOT NULL AND VALUES(content) != '', VALUES(content), content),
        updated_at = VALUES(updated_at)
"""


def batch_upsert_news(stock_code: str, news_list: list[dict]):
    """批量插入/更新新闻记录

    Args:
        stock_code: 股票代码如 002371.SZ
        news_list: [{"news_type", "title", "url", "publish_date", "publish_time", "source", "content"}, ...]
    """
    if not news_list:
        return 0
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now(_CST)
    count = 0
    try:
        for item in news_list:
            cursor.execute(_UPSERT_SQL, (
                stock_code,
                item.get("news_type", ""),
                item.get("title", ""),
                item.get("url", ""),
                item.get("publish_date", ""),
                item.get("publish_time", ""),
                item.get("source", ""),
                item.get("content", ""),
                now,
            ))
            count += 1
        conn.commit()
        logger.debug("[%s] 写入 %d 条新闻记录", stock_code, count)
    except Exception as e:
        conn.rollback()
        logger.error("[%s] 写入新闻失败: %s", stock_code, e)
        raise
    finally:
        cursor.close()
        conn.close()
    return count


# ─────────────────── 查询 ───────────────────

def get_news_by_stock(stock_code: str, news_type: str = None, limit: int = 50) -> list[dict]:
    """查询某只股票的新闻"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        if news_type:
            sql = (f"SELECT * FROM {TABLE_NAME} "
                   f"WHERE stock_code = %s AND news_type = %s "
                   f"ORDER BY publish_date DESC, publish_time DESC LIMIT %s")
            cursor.execute(sql, (stock_code, news_type, limit))
        else:
            sql = (f"SELECT * FROM {TABLE_NAME} "
                   f"WHERE stock_code = %s "
                   f"ORDER BY publish_date DESC, publish_time DESC LIMIT %s")
            cursor.execute(sql, (stock_code, limit))
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def get_latest_news_date(stock_code: str, news_type: str = None) -> str | None:
    """获取某只股票最新的新闻日期"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if news_type:
            cursor.execute(
                f"SELECT MAX(publish_date) FROM {TABLE_NAME} "
                f"WHERE stock_code = %s AND news_type = %s",
                (stock_code, news_type),
            )
        else:
            cursor.execute(
                f"SELECT MAX(publish_date) FROM {TABLE_NAME} WHERE stock_code = %s",
                (stock_code,),
            )
        row = cursor.fetchone()
        return str(row[0]) if row and row[0] else None
    finally:
        cursor.close()
        conn.close()
