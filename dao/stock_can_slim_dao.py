"""数据库模型定义 — MySQL 版"""
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from dao import get_connection

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.init_database()

    def init_database(self):
        """初始化数据库表"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            # 批次信息表
            cursor.execute("""
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
            """)

            # 明细信息表
            cursor.execute("""
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
            """)

            # 深度分析历史记录表
            cursor.execute("""
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
            """)

            # 维度级历史记录表
            cursor.execute("""
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
            """)

            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def create_batch(self, stock_codes: List[str]) -> int:
        """创建新批次"""
        now = datetime.now()
        batch_name = f"批次_{now.strftime('%Y%m%d_%H%M%S')}"
        now_iso = now.isoformat()

        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO stock_batch_list_info (batch_name, total_count) VALUES (%s, %s)",
                (batch_name, len(stock_codes)),
            )
            batch_id = cursor.lastrowid

            for stock_code in stock_codes:
                if " (" in stock_code and stock_code.endswith(")"):
                    stock_name = stock_code.split(" (")[0]
                    code = stock_code.split(" (")[1].rstrip(")")
                else:
                    stock_name = stock_code
                    code = stock_code
                cursor.execute(
                    "INSERT INTO stock_analysis_detail (batch_id, stock_code, stock_name, created_at) VALUES (%s, %s, %s, %s)",
                    (batch_id, code, stock_name, now_iso),
                )

            conn.commit()
            return batch_id
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def add_stocks_to_batch(self, batch_id: int, stock_codes: List[str]) -> int:
        """向已有批次中添加股票，返回实际新增数量"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT stock_code FROM stock_analysis_detail WHERE batch_id = %s", (batch_id,))
            existing_codes = {row[0] for row in cursor.fetchall()}

            added = 0
            now = datetime.now().isoformat()
            for stock_code in stock_codes:
                if " (" in stock_code and stock_code.endswith(")"):
                    stock_name = stock_code.split(" (")[0]
                    code = stock_code.split(" (")[1].rstrip(")")
                else:
                    stock_name = stock_code
                    code = stock_code

                if code in existing_codes:
                    continue

                cursor.execute(
                    "INSERT INTO stock_analysis_detail (batch_id, stock_code, stock_name, created_at) VALUES (%s, %s, %s, %s)",
                    (batch_id, code, stock_name, now),
                )
                existing_codes.add(code)
                added += 1

            if added > 0:
                cursor.execute("UPDATE stock_batch_list_info SET total_count = total_count + %s WHERE id = %s", (added, batch_id))

            conn.commit()
            return added
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def get_batches(self) -> List[Dict[str, Any]]:
        """获取所有批次，置顶优先，再按创建时间倒序"""
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM stock_batch_list_info ORDER BY is_pinned DESC, created_at DESC")
            return list(cursor.fetchall())
        finally:
            cursor.close()
            conn.close()

    def rename_batch(self, batch_id: int, new_name: str) -> bool:
        """重命名批次，名称不能重复"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM stock_batch_list_info WHERE batch_name = %s AND id != %s", (new_name, batch_id))
            if cursor.fetchone():
                return False
            cursor.execute("UPDATE stock_batch_list_info SET batch_name = %s WHERE id = %s", (new_name, batch_id))
            conn.commit()
            return True
        finally:
            cursor.close()
            conn.close()

    def toggle_pin_batch(self, batch_id: int) -> bool:
        """切换批次置顶状态，返回新的置顶状态"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT is_pinned FROM stock_batch_list_info WHERE id = %s", (batch_id,))
            row = cursor.fetchone()
            if not row:
                return False
            new_val = 0 if row[0] else 1
            cursor.execute("UPDATE stock_batch_list_info SET is_pinned = %s WHERE id = %s", (new_val, batch_id))
            conn.commit()
            return bool(new_val)
        finally:
            cursor.close()
            conn.close()

    def get_batch_stocks(self, batch_id: int) -> List[Dict[str, Any]]:
        """获取批次中的股票列表，有深度分析的优先，按最新深度分析时间倒序"""
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT s.*,
                    MAX(h.created_at) as last_deep_at
                FROM stock_analysis_detail s
                LEFT JOIN stock_dim_analysis_history h ON h.stock_id = s.id
                WHERE s.batch_id = %s
                GROUP BY s.id
                ORDER BY last_deep_at IS NULL, last_deep_at DESC, s.id ASC
            """, (batch_id,))
            return list(cursor.fetchall())
        finally:
            cursor.close()
            conn.close()

    def get_stock_detail(self, stock_id: int) -> Optional[Dict[str, Any]]:
        """获取股票详细信息"""
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM stock_analysis_detail WHERE id = %s", (stock_id,))
            return cursor.fetchone()
        finally:
            cursor.close()
            conn.close()

    def update_stock_dimension_score(self, stock_id: int, dimension: str,
                                     score: int, prompt: str, summary: str = None, score_prompt: str = None):
        """更新股票维度打分"""
        dimension = dimension.lower()
        conn = get_connection()
        cursor = conn.cursor()
        try:
            if score_prompt:
                cursor.execute(f"""
                    UPDATE stock_analysis_detail
                    SET {dimension}_score = %s, {dimension}_prompt = %s, {dimension}_summary = %s, {dimension}_score_prompt = %s
                    WHERE id = %s
                """, (score, prompt, summary, score_prompt, stock_id))
            else:
                cursor.execute(f"""
                    UPDATE stock_analysis_detail
                    SET {dimension}_score = %s, {dimension}_prompt = %s, {dimension}_summary = %s
                    WHERE id = %s
                """, (score, prompt, summary, stock_id))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def update_stock_dimension_deep_analysis(self, stock_id: int, dimension: str,
                                              score: float, prompt: str, summary: str = None, score_prompt: str = None):
        """更新股票维度深度分析"""
        dimension = dimension.lower()
        conn = get_connection()
        cursor = conn.cursor()
        try:
            if score_prompt:
                cursor.execute(f"""
                    UPDATE stock_analysis_detail
                    SET {dimension}_deep_score = %s, {dimension}_deep_prompt = %s, {dimension}_deep_summary = %s, {dimension}_deep_score_prompt = %s
                    WHERE id = %s
                """, (score, prompt, summary, score_prompt, stock_id))
            else:
                cursor.execute(f"""
                    UPDATE stock_analysis_detail
                    SET {dimension}_deep_score = %s, {dimension}_deep_prompt = %s, {dimension}_deep_summary = %s
                    WHERE id = %s
                """, (score, prompt, summary, stock_id))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def update_stock_prescreening_data(self, stock_id: int, change_pct: float, high_price: float, high_price_date: str, latest_price: float = None):
        """更新涨跌初筛数据"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE stock_analysis_detail
                SET change_pct = %s, high_price_120 = %s, high_price_date_120 = %s, latest_price = %s
                WHERE id = %s
            """, (change_pct, high_price, high_price_date, latest_price, stock_id))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def update_stock_kline(self, stock_id: int, score: int, prompt: str):
        """更新K线分析"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE stock_analysis_detail SET kline_score = %s, kline_prompt = %s WHERE id = %s
            """, (score, prompt, stock_id))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def update_stock_kline_scores(self, stock_id: int, total_score: int):
        """更新K线综合评分总分"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE stock_analysis_detail SET kline_total_score = %s WHERE id = %s", (total_score, stock_id))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def update_stock_overall_analysis(self, stock_id: int, analysis: str, prompt: str = None, grade: str = None):
        """更新整体分析"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            if prompt is not None:
                cursor.execute("""
                    UPDATE stock_analysis_detail
                    SET overall_analysis = %s, overall_prompt = %s, overall_grade = %s
                    WHERE id = %s
                """, (analysis, prompt, grade, stock_id))
            else:
                cursor.execute("""
                    UPDATE stock_analysis_detail SET overall_analysis = %s, overall_grade = %s WHERE id = %s
                """, (analysis, grade, stock_id))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def update_stock_status(self, stock_id: int, status: str,
                            error_message: str = None, is_deep_thinking: bool = False):
        """更新股票状态"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            completed_at = datetime.now().isoformat() if status == 'completed' else None
            cursor.execute("""
                UPDATE stock_analysis_detail
                SET status = %s, error_message = %s, is_deep_thinking = %s, completed_at = %s
                WHERE id = %s
            """, (status, error_message, 1 if is_deep_thinking else 0, completed_at, stock_id))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def update_batch_progress(self, batch_id: int):
        """更新批次进度"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as success,
                    SUM(CASE WHEN status IN ('completed', 'failed') THEN 1 ELSE 0 END) as completed
                FROM stock_analysis_detail
                WHERE batch_id = %s
            """, (batch_id,))
            result = cursor.fetchone()
            total, success, completed = result
            batch_status = 'completed' if completed == total else 'processing'
            cursor.execute("""
                UPDATE stock_batch_list_info
                SET success_count = %s, completed_count = %s, status = %s
                WHERE id = %s
            """, (success, completed, batch_status, batch_id))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def delete_batch(self, batch_id: int):
        """删除批次及其所有明细"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM stock_analysis_detail WHERE batch_id = %s", (batch_id,))
            cursor.execute("DELETE FROM stock_batch_list_info WHERE id = %s", (batch_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def clear_all_batches(self):
        """清空所有批次数据"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM stock_analysis_detail")
            cursor.execute("DELETE FROM stock_batch_list_info")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def add_dim_analysis_history(self, batch_id: int, stock_id: int, stock_name: str, stock_code: str,
                                 dimension: str, is_deep_thinking: bool, execution_id: str = None,
                                 score: float = None, result: str = None, summary: str = None,
                                 status: str = 'done', error_message: str = None):
        """写入单个维度历史记录"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO stock_dim_analysis_history
                (execution_id, batch_id, stock_id, stock_name, stock_code, dimension, is_deep_thinking,
                 score, result, summary, status, error_message)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (execution_id, batch_id, stock_id, stock_name, stock_code, dimension.upper(),
                  1 if is_deep_thinking else 0, score, result, summary, status, error_message))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def update_dim_history_overall_grade(self, execution_id: str, overall_grade: str):
        """按 execution_id 更新整体评级"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE stock_dim_analysis_history SET overall_grade = %s WHERE execution_id = %s",
                (overall_grade, execution_id),
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def get_stock_dim_analysis_history(self, stock_name: str) -> List[Dict[str, Any]]:
        """按股票名称查询维度历史记录，按时间倒序"""
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, execution_id, batch_id, stock_id, stock_name, stock_code, dimension,
                    is_deep_thinking, score, summary, status, error_message, overall_grade, created_at
                FROM stock_dim_analysis_history
                WHERE stock_name = %s
                ORDER BY created_at DESC
            """, (stock_name,))
            return list(cursor.fetchall())
        finally:
            cursor.close()
            conn.close()

    def clear_stock_dim_analysis_history(self, stock_name: str):
        """清空指定股票的维度历史记录"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM stock_dim_analysis_history WHERE stock_name = %s", (stock_name,))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def add_deep_analysis_history(self, batch_id: int, stock_id: int, stock_name: str, stock_code: str,
                                   is_deep_thinking: bool, dim_results: dict, overall_analysis: str, overall_prompt: str):
        """写入一条深度分析历史记录"""
        dims = ['c', 'a', 'n', 's', 'l', 'i', 'm']
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO stock_deep_analysis_history
                (batch_id, stock_id, stock_name, stock_code, is_deep_thinking,
                 c_score, c_result, c_summary, a_score, a_result, a_summary,
                 n_score, n_result, n_summary, s_score, s_result, s_summary,
                 l_score, l_result, l_summary, i_score, i_result, i_summary,
                 m_score, m_result, m_summary, overall_analysis, overall_prompt)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                batch_id, stock_id, stock_name, stock_code, 1 if is_deep_thinking else 0,
                *[v for d in dims for v in (dim_results.get(d, {}).get('score'), dim_results.get(d, {}).get('result'), dim_results.get(d, {}).get('summary'))],
                overall_analysis, overall_prompt,
            ))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def get_stock_deep_analysis_history(self, stock_name: str) -> List[Dict[str, Any]]:
        """按股票名称查询历史深度分析记录，按时间倒序"""
        conn = get_connection(use_dict_cursor=True)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, batch_id, stock_id, stock_name, stock_code, is_deep_thinking,
                    c_score, a_score, n_score, s_score, l_score, i_score, m_score,
                    overall_analysis, created_at
                FROM stock_deep_analysis_history
                WHERE stock_name = %s
                ORDER BY created_at DESC
            """, (stock_name,))
            return list(cursor.fetchall())
        finally:
            cursor.close()
            conn.close()


# 全局数据库管理器实例
db_manager = DatabaseManager()
