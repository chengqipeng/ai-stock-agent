"""低估值股票筛选记录 DAO — stock_undervalued_pick 表"""
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dao import get_connection

logger = logging.getLogger(__name__)
_CST = ZoneInfo("Asia/Shanghai")
TABLE_NAME = "stock_undervalued_pick"


def create_undervalued_table(cursor=None):
    """创建低估值筛选记录表（幂等）"""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            pick_date VARCHAR(20) NOT NULL COMMENT '筛选日期(每天一条)',
            sector_filter VARCHAR(50) NOT NULL DEFAULT 'tech_mfg' COMMENT '行业筛选条件',
            mcap_min DOUBLE DEFAULT 80 COMMENT '最小市值(亿)',
            mcap_max DOUBLE DEFAULT 500 COMMENT '最大市值(亿)',
            total_screened INT COMMENT '筛选池总数',
            total_picked INT COMMENT '入选数量',
            picks_json MEDIUMTEXT COMMENT '入选股票列表JSON',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_date_sector (pick_date, sector_filter),
            INDEX idx_pick_date (pick_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    own = cursor is None
    if own:
        conn = get_connection()
        cursor = conn.cursor()
    cursor.execute(ddl)
    if own:
        conn.commit()
        cursor.close()
        conn.close()


def upsert_pick(pick_date: str, sector_filter: str, mcap_min: float,
                mcap_max: float, total_screened: int, total_picked: int,
                picks: list[dict]):
    """写入或更新当天的筛选记录（同一天同一筛选条件只保留一条）"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME}
                (pick_date, sector_filter, mcap_min, mcap_max,
                 total_screened, total_picked, picks_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                mcap_min=VALUES(mcap_min), mcap_max=VALUES(mcap_max),
                total_screened=VALUES(total_screened),
                total_picked=VALUES(total_picked),
                picks_json=VALUES(picks_json)
        """, (pick_date, sector_filter, mcap_min, mcap_max,
              total_screened, total_picked,
              json.dumps(picks, ensure_ascii=False)))
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def get_pick_history(limit: int = 30) -> list[dict]:
    """获取历史筛选记录列表（不含 picks_json 大字段）"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT id, pick_date, sector_filter, mcap_min, mcap_max, "
            f"total_screened, total_picked, created_at "
            f"FROM {TABLE_NAME} ORDER BY pick_date DESC LIMIT %s", (limit,))
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def get_pick_detail(pick_id: int) -> dict | None:
    """获取某条筛选记录的完整数据"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT * FROM {TABLE_NAME} WHERE id = %s", (pick_id,))
        row = cursor.fetchone()
        if row and row.get("picks_json"):
            row["picks"] = json.loads(row["picks_json"])
            del row["picks_json"]
        return row
    finally:
        cursor.close()
        conn.close()


def get_pick_by_date(pick_date: str, sector_filter: str = "tech_mfg") -> dict | None:
    """按日期查询"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT * FROM {TABLE_NAME} WHERE pick_date=%s AND sector_filter=%s",
            (pick_date, sector_filter))
        row = cursor.fetchone()
        if row and row.get("picks_json"):
            row["picks"] = json.loads(row["picks_json"])
            del row["picks_json"]
        return row
    finally:
        cursor.close()
        conn.close()
