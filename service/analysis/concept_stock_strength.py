"""
概念板块个股强弱势分析服务

对比个股K线与所属概念板块K线，计算相对强弱指标：
- 超额收益率（个股涨跌幅 - 板块涨跌幅）
- 累计超额收益
- 相对强弱评分（综合短中长期）
- 强弱势分类（强势/中性/弱势）
"""
import logging
from datetime import date, timedelta
from decimal import Decimal

from dao import get_connection

logger = logging.getLogger(__name__)


def _to_float(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _normalize_stock_code(code: str) -> str:
    """将6位纯数字股票代码转换为带市场后缀的格式（如 000001 -> 000001.SZ）"""
    if "." in code:
        return code
    if code.startswith("6"):
        return f"{code}.SH"
    elif code.startswith("0") or code.startswith("3"):
        return f"{code}.SZ"
    elif code.startswith("4") or code.startswith("8") or code.startswith("9"):
        return f"{code}.BJ"
    return f"{code}.SZ"


def analyze_board_stock_strength(
    board_code: str,
    days: int = 60,
) -> dict:
    """
    分析某个概念板块内所有成分股的相对强弱势。

    计算逻辑：
    1. 取板块K线和所有成分股K线（最近 days 个交易日）
    2. 对齐日期，计算每日超额收益 = 个股涨跌幅 - 板块涨跌幅
    3. 分短期(5日)、中期(20日)、长期(days日) 计算累计超额收益
    4. 综合评分 = 短期*0.4 + 中期*0.35 + 长期*0.25
    5. 按评分排序，分为强势/中性/弱势

    Returns:
        {
            "board": {"board_code", "board_name"},
            "period": {"days", "start_date", "end_date", "trade_days"},
            "board_performance": {"total_return", "avg_daily_return"},
            "stocks": [
                {
                    "stock_code", "stock_name",
                    "total_return",          # 个股区间涨跌幅
                    "excess_5d",             # 5日超额收益
                    "excess_20d",            # 20日超额收益
                    "excess_total",          # 全区间超额收益
                    "strength_score",        # 综合强弱评分
                    "strength_level",        # 强势/中性/弱势
                    "win_rate",              # 跑赢板块天数占比
                    "daily_excess": [...]    # 每日超额收益序列
                }, ...
            ]
        }
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
            return {"success": False, "error": "板块不存在"}

        # 2. 板块K线
        cur.execute(
            "SELECT `date`, change_percent FROM concept_board_kline "
            "WHERE board_code = %s ORDER BY `date` DESC LIMIT %s",
            (board_code, days),
        )
        board_klines = {
            r["date"]: _to_float(r["change_percent"])
            for r in cur.fetchall()
            if r["change_percent"] is not None
        }
        if not board_klines:
            return {"success": False, "error": "板块无K线数据"}

        trade_dates = sorted(board_klines.keys())
        start_date = trade_dates[0]
        end_date = trade_dates[-1]

        # 3. 成分股列表
        cur.execute(
            "SELECT stock_code, stock_name FROM stock_concept_board_stock "
            "WHERE board_code = %s", (board_code,)
        )
        members = cur.fetchall()
        if not members:
            return {"success": False, "error": "板块无成分股"}

        # 4. 批量查询成分股K线
        codes = [m["stock_code"] for m in members]
        name_map = {m["stock_code"]: m["stock_name"] for m in members}

        # stock_concept_board_stock 中的 stock_code 是6位纯数字，
        # 而 stock_kline 中的 stock_code 带市场后缀（如 000001.SZ），需要转换
        normalized_codes = [_normalize_stock_code(c) for c in codes]
        norm_to_raw = {_normalize_stock_code(c): c for c in codes}

        logger.debug("[概念强弱] board=%s 成分股%d只, 板块日期范围=%s~%s, "
                     "样本codes[:3]=%s, normalized[:3]=%s",
                     board_code, len(codes), start_date, end_date,
                     codes[:3], normalized_codes[:3])

        # 分批查询（避免 IN 子句过长）
        stock_klines = {}  # raw_code -> {date: change_percent}
        _total_kline_rows = 0
        batch_size = 50
        for i in range(0, len(normalized_codes), batch_size):
            batch = normalized_codes[i:i + batch_size]
            placeholders = ",".join(["%s"] * len(batch))
            cur.execute(
                f"SELECT stock_code, `date`, change_percent FROM stock_kline "
                f"WHERE stock_code IN ({placeholders}) "
                f"AND `date` >= %s AND `date` <= %s",
                batch + [start_date, end_date],
            )
            for r in cur.fetchall():
                _total_kline_rows += 1
                norm_code = r["stock_code"]
                raw_code = norm_to_raw.get(norm_code, norm_code)
                if raw_code not in stock_klines:
                    stock_klines[raw_code] = {}
                cp = _to_float(r["change_percent"])
                if cp is not None:
                    stock_klines[raw_code][r["date"]] = cp

        # 5. 计算每只股票的强弱指标
        logger.info("[概念强弱] board=%s 成分股%d, 查到K线行数=%d, "
                    "有K线的股票=%d",
                    board_code, len(codes), _total_kline_rows,
                    len(stock_klines))

        board_total_return = _compound_return(
            [board_klines.get(d, 0) for d in trade_dates]
        )
        board_avg_daily = (
            sum(board_klines.get(d, 0) for d in trade_dates) / len(trade_dates)
            if trade_dates else 0
        )

        no_kline_count = 0
        too_few_days_count = 0
        results = []
        for code in codes:
            sk = stock_klines.get(code, {})
            if not sk:
                no_kline_count += 1
                continue

            # 对齐日期的每日超额收益
            daily_excess = []
            stock_returns = []
            for d in trade_dates:
                s_ret = sk.get(d)
                b_ret = board_klines.get(d, 0)
                if s_ret is not None:
                    excess = s_ret - b_ret
                    daily_excess.append({"date": d, "excess": round(excess, 4),
                                         "stock": round(s_ret, 4),
                                         "board": round(b_ret, 4)})
                    stock_returns.append(s_ret)

            if len(daily_excess) < 3:
                too_few_days_count += 1
                continue

            # 累计超额收益（不同周期）
            n = len(daily_excess)
            excess_5d = sum(e["excess"] for e in daily_excess[-min(5, n):])
            excess_20d = sum(e["excess"] for e in daily_excess[-min(20, n):])
            excess_total = sum(e["excess"] for e in daily_excess)

            # 个股区间总收益
            total_return = _compound_return(stock_returns)

            # 跑赢板块天数占比
            win_days = sum(1 for e in daily_excess if e["excess"] > 0)
            win_rate = win_days / n if n > 0 else 0

            # 综合评分：短期权重高（更关注近期动量），满分100
            raw_score = (
                _normalize_excess(excess_5d, 5) * 0.40
                + _normalize_excess(excess_20d, 20) * 0.35
                + _normalize_excess(excess_total, n) * 0.25
            )
            score = max(0.0, min(100.0, raw_score))

            results.append({
                "stock_code": code,
                "stock_name": name_map.get(code, ""),
                "total_return": round(total_return, 2),
                "excess_5d": round(excess_5d, 2),
                "excess_20d": round(excess_20d, 2),
                "excess_total": round(excess_total, 2),
                "strength_score": round(score, 2),
                "win_rate": round(win_rate, 4),
                "trade_days": n,
                "daily_excess": daily_excess,
            })

        # 6. 排序 & 分级
        results.sort(key=lambda x: x["strength_score"], reverse=True)

        # 记录跳过的股票信息
        if no_kline_count or too_few_days_count:
            logger.warning(
                "[概念强弱] board=%s 成分股%d, 有效评分%d, 无K线跳过%d, 日期不足跳过%d, "
                "板块日期范围=%s~%s",
                board_code, len(codes), len(results), no_kline_count,
                too_few_days_count, start_date, end_date,
            )
        if not results:
            logger.warning(
                "[概念强弱] board=%s 全部成分股计算失败! 成分股%d, 有K线%d, 无K线%d, 日期不足%d, "
                "板块日期范围=%s~%s, stock_klines样本日期=%s",
                board_code, len(codes), len(stock_klines), no_kline_count,
                too_few_days_count, start_date, end_date,
                list(list(stock_klines.values())[0].keys())[:3] if stock_klines else "N/A",
            )

        total = len(results)
        for i, r in enumerate(results):
            rank_pct = i / total if total > 0 else 0
            if rank_pct < 0.3:
                r["strength_level"] = "强势"
            elif rank_pct < 0.7:
                r["strength_level"] = "中性"
            else:
                r["strength_level"] = "弱势"
            r["rank"] = i + 1

        return {
            "success": True,
            "board": {"board_code": board_code,
                      "board_name": board["board_name"]},
            "period": {
                "days": days,
                "start_date": start_date,
                "end_date": end_date,
                "trade_days": len(trade_dates),
            },
            "board_performance": {
                "total_return": round(board_total_return, 2),
                "avg_daily_return": round(board_avg_daily, 4),
            },
            "stocks": results,
            "total": total,
        }
    except Exception as e:
        logger.error("概念板块个股强弱分析失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        cur.close()
        conn.close()


def _compound_return(daily_pcts: list[float]) -> float:
    """计算复合收益率（百分比输入输出）"""
    product = 1.0
    for p in daily_pcts:
        product *= (1 + p / 100)
    return (product - 1) * 100


def _normalize_excess(excess: float, days: int) -> float:
    """将累计超额收益标准化为日均，再映射到评分"""
    if days <= 0:
        return 0
    daily_avg = excess / days
    # 日均超额 1% 映射为 50 分，线性，以50为中心
    return 50 + daily_avg * 50


def compute_and_save_all_boards(days: int = 60, progress_callback=None) -> dict:
    """
    批量计算所有概念板块内个股的强弱势评分，并写入数据库。

    Args:
        days: 分析天数
        progress_callback: 可选回调函数，签名 (total, success, failed) -> None，
                           每处理完一个板块调用一次，用于实时更新进度。

    Returns:
        {"total_boards": N, "success_boards": N, "failed_boards": N,
         "total_stocks_scored": N, "score_date": "YYYY-MM-DD"}
    """
    from dao.stock_concept_strength_dao import ensure_table, batch_upsert_strength

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        ensure_table()
        cur.execute("SELECT board_code, board_name FROM stock_concept_board ORDER BY board_code")
        boards = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    total_boards = len(boards)
    success_boards = 0
    failed_boards = 0
    failed_board_details = []  # 收集失败板块信息
    total_scored = 0
    score_date = date.today().isoformat()

    if progress_callback:
        progress_callback(total_boards, 0, 0)

    for i, board in enumerate(boards):
        board_code = board["board_code"]
        board_name = board["board_name"]

        result = analyze_board_stock_strength(board_code, days=days)
        if not result.get("success") or not result.get("stocks"):
            failed_boards += 1
            if not result.get("success"):
                reason = result.get("error", "unknown")
                logger.warning("[概念强弱] 板块 %s(%s) 失败: %s",
                               board_code, board_name, reason)
                failed_board_details.append(f"{board_code}({board_name}): {reason}")
            else:
                logger.warning("[概念强弱] 板块 %s(%s) success但stocks为空 (total=%s)",
                               board_code, board_name, result.get("total", "N/A"))
                failed_board_details.append(f"{board_code}({board_name}): stocks为空")
            if i < 3:
                logger.info("[概念强弱] 前3板块诊断 %s: %s", board_code,
                            {k: v for k, v in result.items() if k != 'stocks'})
        else:
            stocks = result["stocks"]
            board_total = result["total"]

            rows = []
            for s in stocks:
                rows.append({
                    "stock_code": s["stock_code"],
                    "stock_name": s["stock_name"],
                    "board_code": board_code,
                    "board_name": board_name,
                    "strength_score": s["strength_score"],
                    "strength_level": s.get("strength_level", "中性"),
                    "total_return": s.get("total_return"),
                    "excess_5d": s.get("excess_5d"),
                    "excess_20d": s.get("excess_20d"),
                    "excess_total": s.get("excess_total"),
                    "win_rate": s.get("win_rate"),
                    "rank_in_board": s.get("rank"),
                    "board_total_stocks": board_total,
                    "trade_days": s.get("trade_days"),
                    "analysis_days": days,
                    "score_date": score_date,
                })

            if rows:
                batch_upsert_strength(rows)
                total_scored += len(rows)
                success_boards += 1

        if progress_callback:
            progress_callback(total_boards, success_boards, failed_boards)

        if (i + 1) % 20 == 0 or i == total_boards - 1:
            logger.info("[概念强弱] 进度 %d/%d, 成功板块=%d, 失败=%d, 已评分个股=%d",
                        i + 1, total_boards, success_boards, failed_boards, total_scored)

    logger.info("[概念强弱] 完成: 共%d板块, 成功=%d, 失败=%d, 评分个股=%d",
                total_boards, success_boards, failed_boards, total_scored)
    if failed_board_details:
        logger.warning("[概念强弱] 失败板块汇总(%d个):\n  %s",
                       len(failed_board_details), "\n  ".join(failed_board_details))
    return {
        "total_boards": total_boards,
        "success_boards": success_boards,
        "failed_boards": failed_boards,
        "total_stocks_scored": total_scored,
        "score_date": score_date,
    }


def compute_single_board_and_save(board_code: str, days: int = 60) -> dict:
    """计算单个板块的个股强弱势并保存到数据库"""
    from dao.stock_concept_strength_dao import ensure_table, batch_upsert_strength

    ensure_table()
    result = analyze_board_stock_strength(board_code, days=days)
    if not result.get("success"):
        logger.warning("[概念强弱] 单板块计算失败 board=%s: %s", board_code, result.get("error", "unknown"))
        return result
    if not result.get("stocks"):
        logger.warning("[概念强弱] 单板块计算成功但无评分结果 board=%s (total=%s)",
                       board_code, result.get("total", "N/A"))
        return result

    board_name = result["board"]["board_name"]
    score_date = date.today().isoformat()
    stocks = result["stocks"]

    rows = []
    for s in stocks:
        rows.append({
            "stock_code": s["stock_code"],
            "stock_name": s["stock_name"],
            "board_code": board_code,
            "board_name": board_name,
            "strength_score": s["strength_score"],
            "strength_level": s.get("strength_level", "中性"),
            "total_return": s.get("total_return"),
            "excess_5d": s.get("excess_5d"),
            "excess_20d": s.get("excess_20d"),
            "excess_total": s.get("excess_total"),
            "win_rate": s.get("win_rate"),
            "rank_in_board": s.get("rank"),
            "board_total_stocks": result["total"],
            "trade_days": s.get("trade_days"),
            "analysis_days": days,
            "score_date": score_date,
        })

    if rows:
        batch_upsert_strength(rows)

    result["saved"] = len(rows)
    return result


def get_stock_concept_strength_summary(stock_code: str) -> dict:
    """
    获取某只股票在所有概念板块中的强弱势汇总。
    返回各板块评分及综合评分（取所有板块的加权平均）。
    """
    from dao.stock_concept_strength_dao import get_stock_strength

    records = get_stock_strength(stock_code)
    if not records:
        return {"success": False, "error": "该股票无概念板块强弱势评分数据"}

    # 综合分 = 所有板块评分的平均值
    scores = [float(r["strength_score"]) for r in records]
    avg_score = sum(scores) / len(scores) if scores else 0
    max_score = max(scores) if scores else 0
    min_score = min(scores) if scores else 0

    # 分级
    if avg_score >= 65:
        overall_level = "强势"
    elif avg_score >= 35:
        overall_level = "中性"
    else:
        overall_level = "弱势"

    boards = []
    for r in records:
        boards.append({
            "board_code": r["board_code"],
            "board_name": r["board_name"],
            "strength_score": float(r["strength_score"]),
            "strength_level": r["strength_level"],
            "rank_in_board": r.get("rank_in_board"),
            "board_total_stocks": r.get("board_total_stocks"),
            "excess_5d": float(r["excess_5d"]) if r.get("excess_5d") else None,
            "excess_20d": float(r["excess_20d"]) if r.get("excess_20d") else None,
            "score_date": r.get("score_date"),
        })

    return {
        "success": True,
        "stock_code": stock_code,
        "stock_name": records[0]["stock_name"] if records else "",
        "overall_score": round(avg_score, 2),
        "overall_level": overall_level,
        "max_score": round(max_score, 2),
        "min_score": round(min_score, 2),
        "board_count": len(boards),
        "boards": boards,
    }
