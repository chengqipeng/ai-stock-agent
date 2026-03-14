"""
概念板块相对大盘强弱势评分服务

将每个概念板块的K线走势与大盘指数（上证指数）对比，
计算综合强弱势评分（0-100分），分数越高代表越强势。

评分维度：
1. 超额收益得分（30分）：板块区间涨跌幅 - 大盘区间涨跌幅
2. 短期动量得分（25分）：近5日超额收益趋势
3. 中期趋势得分（20分）：近20日超额收益趋势
4. 胜率得分（15分）：跑赢大盘的交易日占比
5. 回撤控制得分（10分）：相对大盘的最大回撤控制

结果写入 stock_concept_board 表的 market_strength_score 等字段。
"""
import logging
from datetime import date, timedelta
from decimal import Decimal

from dao import get_connection

logger = logging.getLogger(__name__)


def _to_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _compound_return(daily_pcts: list[float]) -> float:
    """复合收益率（百分比输入输出）"""
    product = 1.0
    for p in daily_pcts:
        product *= (1 + p / 100)
    return (product - 1) * 100


def _max_drawdown(cum_returns: list[float]) -> float:
    """计算最大回撤（百分比），输入为累计收益率序列"""
    if not cum_returns:
        return 0.0
    peak = cum_returns[0]
    max_dd = 0.0
    for v in cum_returns:
        if v > peak:
            peak = v
        dd = (peak - v) / (100 + peak) * 100 if (100 + peak) > 0 else 0
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _sigmoid_score(x: float, center: float = 0, scale: float = 1) -> float:
    """S型映射到 0~1，center 为中点，scale 控制斜率"""
    import math
    try:
        return 1.0 / (1.0 + math.exp(-(x - center) / scale))
    except OverflowError:
        return 0.0 if x < center else 1.0


def compute_board_market_strength(board_code: str, days: int = 60) -> dict | None:
    """
    计算单个概念板块相对大盘的强弱势评分。

    Returns:
        {
            "board_code": str,
            "board_name": str,
            "score": float,           # 综合评分 0-100
            "excess_return": float,    # 区间超额收益%
            "board_return": float,     # 板块区间涨跌幅%
            "market_return": float,    # 大盘区间涨跌幅%
            "excess_5d": float,        # 近5日超额收益%
            "excess_20d": float,       # 近20日超额收益%
            "win_rate": float,         # 跑赢大盘天数占比
            "trade_days": int,         # 有效交易日数
            "detail_scores": {         # 各维度得分明细
                "excess_return_score": float,
                "short_momentum_score": float,
                "mid_trend_score": float,
                "win_rate_score": float,
                "drawdown_score": float,
            }
        }
        失败返回 None
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 1. 板块信息
        cur.execute(
            "SELECT board_code, board_name FROM stock_concept_board "
            "WHERE board_code = %s", (board_code,)
        )
        board = cur.fetchone()
        if not board:
            return None

        # 2. 板块K线（最近 days 个交易日）
        cur.execute(
            "SELECT `date`, change_percent, close_price FROM concept_board_kline "
            "WHERE board_code = %s ORDER BY `date` DESC LIMIT %s",
            (board_code, days),
        )
        board_rows = cur.fetchall()
        if not board_rows or len(board_rows) < 5:
            return None

        # 转为升序
        board_rows = list(reversed(board_rows))
        trade_dates = [r["date"] for r in board_rows]
        start_date = trade_dates[0]
        end_date = trade_dates[-1]

        board_daily = {r["date"]: _to_float(r["change_percent"]) for r in board_rows}

        # 3. 大盘指数K线（上证指数 000001.SH）
        cur.execute(
            "SELECT `date`, change_percent, close_price FROM stock_kline "
            "WHERE stock_code = '000001.SH' AND `date` >= %s AND `date` <= %s "
            "ORDER BY `date` ASC",
            (start_date, end_date),
        )
        market_rows = cur.fetchall()
        if not market_rows or len(market_rows) < 5:
            return None

        market_daily = {r["date"]: _to_float(r["change_percent"]) for r in market_rows}

        # 4. 对齐日期，计算每日超额收益
        aligned_dates = [d for d in trade_dates if d in market_daily and d in board_daily]
        if len(aligned_dates) < 5:
            return None

        daily_excess = []
        board_returns = []
        market_returns = []
        for d in aligned_dates:
            b_ret = board_daily[d]
            m_ret = market_daily[d]
            board_returns.append(b_ret)
            market_returns.append(m_ret)
            daily_excess.append(b_ret - m_ret)

        n = len(aligned_dates)

        # ── 维度1: 超额收益得分（30分）──
        board_total = _compound_return(board_returns)
        market_total = _compound_return(market_returns)
        excess_total = board_total - market_total

        # 超额收益映射：[-20%, +20%] -> [0, 30]
        excess_return_score = _sigmoid_score(excess_total, center=0, scale=8) * 30

        # ── 维度2: 短期动量得分（25分）──
        recent_5 = daily_excess[-min(5, n):]
        excess_5d = sum(recent_5)
        # 近5日超额映射：[-5%, +5%] -> [0, 25]
        short_momentum_score = _sigmoid_score(excess_5d, center=0, scale=2) * 25

        # ── 维度3: 中期趋势得分（20分）──
        recent_20 = daily_excess[-min(20, n):]
        excess_20d = sum(recent_20)
        # 近20日超额映射：[-10%, +10%] -> [0, 20]
        mid_trend_score = _sigmoid_score(excess_20d, center=0, scale=4) * 20

        # ── 维度4: 胜率得分（15分）──
        win_days = sum(1 for e in daily_excess if e > 0)
        win_rate = win_days / n if n > 0 else 0.5
        # 胜率映射：[0.3, 0.7] -> [0, 15]
        win_rate_score = max(0, min(15, (win_rate - 0.3) / 0.4 * 15))

        # ── 维度5: 回撤控制得分（10分）──
        # 计算板块和大盘的累计超额收益序列
        cum_excess = []
        running = 0.0
        for e in daily_excess:
            running += e
            cum_excess.append(running)

        # 超额收益的最大回撤
        if cum_excess:
            peak = cum_excess[0]
            max_dd = 0.0
            for v in cum_excess:
                if v > peak:
                    peak = v
                dd = peak - v
                if dd > max_dd:
                    max_dd = dd
        else:
            max_dd = 0.0

        # 回撤越小越好：max_dd [0, 15] -> score [10, 0]
        drawdown_score = max(0, min(10, 10 - max_dd / 15 * 10))

        # ── 综合评分 ──
        total_score = (excess_return_score + short_momentum_score
                       + mid_trend_score + win_rate_score + drawdown_score)
        total_score = round(max(0, min(100, total_score)), 1)

        return {
            "board_code": board_code,
            "board_name": board["board_name"],
            "score": total_score,
            "excess_return": round(excess_total, 2),
            "board_return": round(board_total, 2),
            "market_return": round(market_total, 2),
            "excess_5d": round(excess_5d, 2),
            "excess_20d": round(excess_20d, 2),
            "win_rate": round(win_rate, 4),
            "trade_days": n,
            "detail_scores": {
                "excess_return_score": round(excess_return_score, 1),
                "short_momentum_score": round(short_momentum_score, 1),
                "mid_trend_score": round(mid_trend_score, 1),
                "win_rate_score": round(win_rate_score, 1),
                "drawdown_score": round(drawdown_score, 1),
            },
        }
    except Exception as e:
        logger.error("板块强弱评分失败 board=%s: %s", board_code, e, exc_info=True)
        return None
    finally:
        cur.close()
        conn.close()


def compute_and_save_all_boards(days: int = 60, progress_callback=None) -> dict:
    """
    批量计算所有概念板块的大盘强弱势评分，并写入数据库。

    Args:
        days: 分析天数
        progress_callback: 可选回调函数，签名 (total, success, failed) -> None，
                           每处理完一个板块调用一次，用于实时更新进度。

    Returns:
        {"total": N, "success": N, "failed": N, "results": [...]}
    """
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 确保列存在
        _ensure_columns(cur)
        conn.commit()

        # 获取所有板块
        cur.execute("SELECT board_code, board_name FROM stock_concept_board ORDER BY board_code")
        boards = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    total = len(boards)
    success = 0
    failed = 0
    results = []

    if progress_callback:
        progress_callback(total, 0, 0)

    for i, board in enumerate(boards):
        board_code = board["board_code"]
        board_name = board["board_name"]

        result = compute_board_market_strength(board_code, days=days)
        if result:
            _update_board_score(board_code, result)
            success += 1
            results.append(result)
        else:
            failed += 1

        if progress_callback:
            progress_callback(total, success, failed)

        if (i + 1) % 50 == 0 or i == total - 1:
            logger.info("[板块强弱] 进度 %d/%d, 成功=%d, 失败=%d",
                        i + 1, total, success, failed)

    logger.info("[板块强弱] 完成: 共%d个板块, 成功=%d, 失败=%d", total, success, failed)
    return {"total": total, "success": success, "failed": failed, "results": results}


def _ensure_columns(cur):
    """确保 stock_concept_board 表有强弱评分相关列"""
    columns_to_add = [
        ("market_strength_score", "DOUBLE DEFAULT NULL COMMENT '大盘强弱评分(0-100)'"),
        ("market_excess_return", "DOUBLE DEFAULT NULL COMMENT '相对大盘超额收益%'"),
        ("market_board_return", "DOUBLE DEFAULT NULL COMMENT '板块区间涨跌幅%'"),
        ("market_strength_detail", "JSON DEFAULT NULL COMMENT '评分明细JSON'"),
        ("market_strength_updated", "TIMESTAMP NULL COMMENT '评分更新时间'"),
    ]
    for col_name, col_def in columns_to_add:
        try:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'stock_concept_board' "
                "AND column_name = %s", (col_name,)
            )
            if cur.fetchone()["cnt"] == 0:
                cur.execute(
                    f"ALTER TABLE stock_concept_board ADD COLUMN {col_name} {col_def}"
                )
                logger.info("已添加列: stock_concept_board.%s", col_name)
        except Exception as e:
            logger.warning("添加列失败 %s: %s", col_name, e)


def _update_board_score(board_code: str, result: dict):
    """将评分结果写入数据库"""
    import json
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        detail_json = json.dumps({
            "excess_return": result["excess_return"],
            "board_return": result["board_return"],
            "market_return": result["market_return"],
            "excess_5d": result["excess_5d"],
            "excess_20d": result["excess_20d"],
            "win_rate": result["win_rate"],
            "trade_days": result["trade_days"],
            "detail_scores": result["detail_scores"],
        }, ensure_ascii=False)

        cur.execute(
            "UPDATE stock_concept_board SET "
            "market_strength_score = %s, "
            "market_excess_return = %s, "
            "market_board_return = %s, "
            "market_strength_detail = %s, "
            "market_strength_updated = NOW() "
            "WHERE board_code = %s",
            (result["score"], result["excess_return"],
             result["board_return"], detail_json, board_code),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("更新板块评分失败 board=%s: %s", board_code, e)
    finally:
        cur.close()
        conn.close()


def get_all_board_strength_ranking(limit: int = 200) -> list[dict]:
    """
    获取所有板块的强弱势排名（按评分降序）。

    Returns:
        [{"board_code", "board_name", "score", "excess_return",
          "board_return", "detail", "updated"}, ...]
    """
    import json
    import datetime as _dt
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT b.board_code, b.board_name, "
            "b.market_strength_score, b.market_excess_return, "
            "b.market_board_return, b.market_strength_detail, "
            "b.market_strength_updated, "
            "IFNULL(s.stock_count, 0) AS stock_count "
            "FROM stock_concept_board b "
            "LEFT JOIN ("
            "  SELECT board_code, COUNT(*) AS stock_count "
            "  FROM stock_concept_board_stock GROUP BY board_code"
            ") s ON b.board_code = s.board_code "
            "WHERE b.market_strength_score IS NOT NULL "
            "ORDER BY b.market_strength_score DESC "
            "LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
        results = []
        for r in rows:
            detail = r.get("market_strength_detail")
            if isinstance(detail, str):
                try:
                    detail = json.loads(detail)
                except Exception:
                    detail = {}
            elif detail is None:
                detail = {}

            updated = r.get("market_strength_updated")
            if isinstance(updated, (_dt.datetime, _dt.date)):
                updated = updated.isoformat()

            results.append({
                "board_code": r["board_code"],
                "board_name": r["board_name"],
                "score": float(r["market_strength_score"]) if r["market_strength_score"] else 0,
                "excess_return": float(r["market_excess_return"]) if r["market_excess_return"] else 0,
                "board_return": float(r["market_board_return"]) if r["market_board_return"] else 0,
                "stock_count": r["stock_count"],
                "detail": detail,
                "updated": updated,
            })
        return results
    except Exception as e:
        logger.error("获取板块强弱排名失败: %s", e, exc_info=True)
        return []
    finally:
        cur.close()
        conn.close()
