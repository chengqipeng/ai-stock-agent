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
    # 兼容已有表：添加 content_status 列
    try:
        cursor.execute(
            f"ALTER TABLE {TABLE_NAME} ADD COLUMN "
            f"content_status VARCHAR(20) DEFAULT 'pending' "
            f"COMMENT 'pending/fetching/done/skip/failed' AFTER content"
        )
    except Exception:
        pass  # 列已存在
    # 为 content_status 添加索引（加速 worker 查询待处理记录）
    try:
        cursor.execute(
            f"ALTER TABLE {TABLE_NAME} ADD INDEX idx_content_status (content_status)"
        )
    except Exception:
        pass  # 索引已存在
    # 迁移：已有 content 的记录标记为 done，无 content 且有 url 的标记为 pending
    try:
        cursor.execute(
            f"UPDATE {TABLE_NAME} SET content_status = 'done' "
            f"WHERE content IS NOT NULL AND content != '' AND "
            f"(content_status IS NULL OR content_status = 'pending')"
        )
        cursor.execute(
            f"UPDATE {TABLE_NAME} SET content_status = 'skip' "
            f"WHERE (content IS NULL OR content = '') AND "
            f"(url IS NULL OR url = '' OR url NOT LIKE '%%10jqka.com.cn%%') AND "
            f"content_status IS NULL"
        )
    except Exception:
        pass
    if own:
        conn.commit()
        cursor.close()
        conn.close()


# ─────────────────── 写入 ───────────────────

_UPSERT_SQL = f"""
    INSERT INTO {TABLE_NAME}
    (stock_code, news_type, title, url, publish_date, publish_time, source, content, content_status, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        url = VALUES(url),
        publish_time = VALUES(publish_time),
        source = VALUES(source),
        content = IF(VALUES(content) IS NOT NULL AND VALUES(content) != '', VALUES(content), content),
        content_status = IF(VALUES(content) IS NOT NULL AND VALUES(content) != '', 'done',
                           IF(content IS NOT NULL AND content != '', content_status, VALUES(content_status))),
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
            content = item.get("content", "")
            # 有内容 → done；无内容但有URL → pending；无URL → skip
            if content:
                status = "done"
            elif item.get("url") and "10jqka.com.cn" in item.get("url", ""):
                status = "pending"
            else:
                status = "skip"
            cursor.execute(_UPSERT_SQL, (
                stock_code,
                item.get("news_type", ""),
                item.get("title", ""),
                item.get("url", ""),
                item.get("publish_date", ""),
                item.get("publish_time", ""),
                item.get("source", ""),
                content,
                status,
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


# ─────────────────── 内容拉取 Worker 专用 ───────────────────

def get_pending_content_records(limit: int = 50) -> list[dict]:
    """获取待拉取正文的新闻记录（content_status='pending'）"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        sql = (f"SELECT id, stock_code, news_type, title, url "
               f"FROM {TABLE_NAME} "
               f"WHERE content_status = 'pending' "
               f"ORDER BY id DESC LIMIT %s")
        cursor.execute(sql, (limit,))
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def update_content_status(record_id: int, status: str, content: str = ""):
    """更新单条记录的正文内容和状态"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if content:
            cursor.execute(
                f"UPDATE {TABLE_NAME} SET content = %s, content_status = %s, "
                f"updated_at = %s WHERE id = %s",
                (content, status, datetime.now(_CST), record_id),
            )
        else:
            cursor.execute(
                f"UPDATE {TABLE_NAME} SET content_status = %s, "
                f"updated_at = %s WHERE id = %s",
                (status, datetime.now(_CST), record_id),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("更新内容状态失败 [id=%d]: %s", record_id, e)
    finally:
        cursor.close()
        conn.close()


def mark_as_fetching(record_ids: list[int]):
    """批量标记为 fetching 状态（防止多 worker 重复拉取）"""
    if not record_ids:
        return
    conn = get_connection()
    cursor = conn.cursor()
    try:
        placeholders = ",".join(["%s"] * len(record_ids))
        cursor.execute(
            f"UPDATE {TABLE_NAME} SET content_status = 'fetching' "
            f"WHERE id IN ({placeholders}) AND content_status = 'pending'",
            record_ids,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("批量标记 fetching 失败: %s", e)
    finally:
        cursor.close()
        conn.close()


def get_content_status_stats() -> dict:
    """统计各 content_status 的数量（利用 idx_content_status 索引）"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT content_status, COUNT(*) AS cnt FROM {TABLE_NAME} "
            f"GROUP BY content_status"
        )
        rows = cursor.fetchall()
        return {r["content_status"] or "null": r["cnt"] for r in rows}
    finally:
        cursor.close()
        conn.close()


def get_news_content_summary() -> dict:
    """快速获取新闻总数和待抓取数（轻量查询，供首页轮询使用）"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) AS total, "
            f"SUM(content_status = 'pending') AS pending, "
            f"SUM(content_status = 'fetching') AS fetching, "
            f"SUM(content_status = 'done') AS done, "
            f"SUM(content_status = 'failed') AS failed "
            f"FROM {TABLE_NAME}"
        )
        row = cursor.fetchone()
        return {
            "total": row[0] or 0,
            "pending": int(row[1] or 0),
            "fetching": int(row[2] or 0),
            "done": int(row[3] or 0),
            "failed": int(row[4] or 0),
        }
    finally:
        cursor.close()
        conn.close()


def reset_stale_fetching(timeout_minutes: int = 30):
    """将超时的 fetching 状态重置为 pending（防止 worker 崩溃后卡死）"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"UPDATE {TABLE_NAME} SET content_status = 'pending' "
            f"WHERE content_status = 'fetching' "
            f"AND updated_at < DATE_SUB(NOW(), INTERVAL %s MINUTE)",
            (timeout_minutes,),
        )
        affected = cursor.rowcount
        conn.commit()
        if affected > 0:
            logger.info("[内容Worker] 重置 %d 条超时 fetching 记录", affected)
    except Exception as e:
        conn.rollback()
        logger.error("重置超时 fetching 失败: %s", e)
    finally:
        cursor.close()
        conn.close()
