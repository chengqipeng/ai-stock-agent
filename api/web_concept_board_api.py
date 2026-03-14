"""概念板块浏览 API"""
import logging

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from dao import get_connection

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/concept_board", response_class=HTMLResponse)
async def concept_board_page():
    with open("static/concept_board.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache", "Expires": "0",
    })


@router.get("/api/concept_board/list")
async def concept_board_list(keyword: str = Query("", description="搜索关键词")):
    """获取所有概念板块（含成分股数量和大盘强弱势评分）"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 检查 market_strength_score 列是否存在
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'stock_concept_board' "
            "AND column_name = 'market_strength_score'"
        )
        has_score_col = cur.fetchone()["cnt"] > 0

        if has_score_col:
            sql = """
                SELECT b.board_code, b.board_name, b.board_url,
                       IFNULL(s.stock_count, 0) AS stock_count,
                       b.market_strength_score
                FROM stock_concept_board b
                LEFT JOIN (
                    SELECT board_code, COUNT(*) AS stock_count
                    FROM stock_concept_board_stock
                    GROUP BY board_code
                ) s ON b.board_code = s.board_code
            """
        else:
            sql = """
                SELECT b.board_code, b.board_name, b.board_url,
                       IFNULL(s.stock_count, 0) AS stock_count,
                       NULL AS market_strength_score
                FROM stock_concept_board b
                LEFT JOIN (
                    SELECT board_code, COUNT(*) AS stock_count
                    FROM stock_concept_board_stock
                    GROUP BY board_code
                ) s ON b.board_code = s.board_code
            """

        params = []
        if keyword:
            sql += " WHERE b.board_name LIKE %s"
            params.append(f"%{keyword}%")

        if has_score_col:
            sql += " ORDER BY b.market_strength_score IS NULL, b.market_strength_score DESC, IFNULL(s.stock_count, 0) DESC, b.board_code"
        else:
            sql += " ORDER BY IFNULL(s.stock_count, 0) DESC, b.board_code"

        cur.execute(sql, params)
        boards = cur.fetchall()
        return {"success": True, "data": boards, "total": len(boards)}
    except Exception as e:
        logger.error("概念板块列表查询失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        cur.close()
        conn.close()


@router.get("/api/concept_board/stocks")
async def concept_board_stocks(
    board_code: str = Query(..., description="板块代码"),
    keyword: str = Query("", description="股票搜索关键词"),
):
    """获取某个概念板块的所有成分股（含个股强弱势评分）"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 板块信息
        cur.execute(
            "SELECT board_code, board_name FROM stock_concept_board WHERE board_code = %s",
            (board_code,),
        )
        board = cur.fetchone()
        if not board:
            return {"success": False, "error": "板块不存在"}

        # 检查 stock_concept_strength 表是否存在
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = 'stock_concept_strength'"
        )
        has_strength_table = cur.fetchone()["cnt"] > 0

        if has_strength_table:
            sql = """
                SELECT s.stock_code, s.stock_name,
                       cs.strength_score, cs.strength_level,
                       cs.rank_in_board, cs.board_total_stocks,
                       cs.total_return, cs.excess_total, cs.win_rate,
                       cs.score_date
                FROM stock_concept_board_stock s
                LEFT JOIN stock_concept_strength cs
                    ON s.stock_code = cs.stock_code AND s.board_code = cs.board_code
                WHERE s.board_code = %s
            """
        else:
            sql = """
                SELECT s.stock_code, s.stock_name,
                       NULL AS strength_score, NULL AS strength_level,
                       NULL AS rank_in_board, NULL AS board_total_stocks,
                       NULL AS total_return, NULL AS excess_total, NULL AS win_rate,
                       NULL AS score_date
                FROM stock_concept_board_stock s
                WHERE s.board_code = %s
            """
        params = [board_code]
        if keyword:
            sql += " AND (s.stock_code LIKE %s OR s.stock_name LIKE %s)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])

        if has_strength_table:
            sql += " ORDER BY cs.strength_score IS NULL, cs.strength_score DESC, s.stock_code"
        else:
            sql += " ORDER BY s.stock_code"

        cur.execute(sql, params)
        stocks = cur.fetchall()

        return {
            "success": True,
            "board": board,
            "data": stocks,
            "total": len(stocks),
        }
    except Exception as e:
        logger.error("板块成分股查询失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        cur.close()
        conn.close()


@router.get("/api/concept_board/stock_boards")
async def stock_concept_boards(
    stock_code: str = Query(..., description="股票代码"),
):
    """查询某只股票所属的所有概念板块"""
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT DISTINCT s.board_code, s.board_name,
                      (SELECT COUNT(*) FROM stock_concept_board_stock
                       WHERE board_code = s.board_code) AS stock_count
               FROM stock_concept_board_stock s
               WHERE s.stock_code = %s
               ORDER BY stock_count DESC""",
            (stock_code,),
        )
        boards = cur.fetchall()
        return {"success": True, "stock_code": stock_code, "data": boards, "total": len(boards)}
    except Exception as e:
        logger.error("股票所属板块查询失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        cur.close()
        conn.close()

@router.get("/api/concept_board/kline")
async def concept_board_kline(
    board_code: str = Query(..., description="板块代码"),
    limit: int = Query(120, ge=1, le=800, description="K线条数"),
):
    """获取某个概念板块的日K线数据（由旧到新）"""
    import datetime as _dt
    from decimal import Decimal
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 板块名称
        cur.execute(
            "SELECT board_name FROM stock_concept_board WHERE board_code = %s",
            (board_code,),
        )
        board = cur.fetchone()
        board_name = board["board_name"] if board else board_code

        cur.execute(
            "SELECT * FROM concept_board_kline WHERE board_code = %s "
            "ORDER BY `date` DESC LIMIT %s",
            (board_code, limit),
        )
        rows = list(reversed(cur.fetchall()))
        for row in rows:
            for k, v in row.items():
                if isinstance(v, (_dt.datetime, _dt.date)):
                    row[k] = v.isoformat()
                elif isinstance(v, Decimal):
                    row[k] = float(v)
        return {
            "success": True,
            "board_code": board_code,
            "board_name": board_name,
            "data": rows,
            "total": len(rows),
        }
    except Exception as e:
        logger.error("板块K线查询失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        cur.close()
        conn.close()

@router.get("/concept_board/strength", response_class=HTMLResponse)
async def concept_board_strength_page():
    """概念板块个股强弱势分析页面"""
    with open("static/concept_stock_strength.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache", "Expires": "0",
    })


@router.get("/api/concept_board/strength")
async def concept_board_strength(
    board_code: str = Query(..., description="板块代码"),
    days: int = Query(60, ge=5, le=250, description="分析天数"),
):
    """分析概念板块内个股相对强弱势"""
    from service.analysis.concept_stock_strength import analyze_board_stock_strength
    result = analyze_board_stock_strength(board_code, days=days)
    # 前端不需要每日明细（太大），单独接口获取
    if result.get("success") and result.get("stocks"):
        for s in result["stocks"]:
            s.pop("daily_excess", None)
    return result


@router.get("/api/concept_board/strength/compute")
async def concept_board_strength_compute(
    board_code: str = Query(None, description="板块代码（空则计算全部）"),
    days: int = Query(60, ge=5, le=250, description="分析天数"),
):
    """计算个股概念板块强弱势评分并保存到数据库"""
    if board_code:
        from service.analysis.concept_stock_strength import compute_single_board_and_save
        result = compute_single_board_and_save(board_code, days=days)
        return result
    else:
        from service.analysis.concept_stock_strength import compute_and_save_all_boards
        summary = compute_and_save_all_boards(days=days)
        return {"success": True, "summary": summary}


@router.get("/api/concept_board/strength/stock_summary")
async def concept_board_stock_strength_summary(
    stock_code: str = Query(..., description="股票代码"),
):
    """获取某只股票在所有概念板块中的强弱势汇总"""
    from service.analysis.concept_stock_strength import get_stock_concept_strength_summary
    return get_stock_concept_strength_summary(stock_code)


@router.get("/api/concept_board/strength/ranking")
async def concept_board_strength_ranking(
    limit: int = Query(50, ge=1, le=500, description="返回数量"),
):
    """获取全市场概念板块强势股排名"""
    from dao.stock_concept_strength_dao import get_strongest_stocks, get_score_stats
    stocks = get_strongest_stocks(limit=limit)
    stats = get_score_stats()
    return {"success": True, "data": stocks, "stats": stats, "total": len(stocks)}


@router.get("/api/concept_board/strength/board_ranking")
async def concept_board_strength_board_ranking(
    board_code: str = Query(..., description="板块代码"),
):
    """获取某个板块内个股强弱势排名（从数据库读取已计算的结果）"""
    from dao.stock_concept_strength_dao import get_board_strength_ranking
    stocks = get_board_strength_ranking(board_code)
    return {"success": True, "data": stocks, "total": len(stocks)}


@router.get("/api/concept_board/strength/detail")
async def concept_board_strength_detail(
    board_code: str = Query(..., description="板块代码"),
    stock_code: str = Query(..., description="股票代码"),
    days: int = Query(60, ge=5, le=250, description="分析天数"),
):
    """获取单只股票相对板块的每日超额收益明细"""
    from service.analysis.concept_stock_strength import analyze_board_stock_strength
    result = analyze_board_stock_strength(board_code, days=days)
    if not result.get("success"):
        return result
    for s in result["stocks"]:
        if s["stock_code"] == stock_code:
            return {
                "success": True,
                "board": result["board"],
                "period": result["period"],
                "stock": s,
            }
    return {"success": False, "error": "该股票不在板块成分股中"}


# ═══════════════════════════════════════════════════════════
# 概念板块 vs 大盘 强弱势分析
# ═══════════════════════════════════════════════════════════

@router.get("/concept_board/market_strength", response_class=HTMLResponse)
async def concept_board_market_strength_page():
    """概念板块大盘强弱势分析页面"""
    with open("static/concept_board_market_strength.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache", "Expires": "0",
    })


@router.get("/api/concept_board/market_strength/ranking")
async def concept_board_market_strength_ranking(
    limit: int = Query(200, ge=1, le=500, description="返回数量"),
):
    """获取所有板块的大盘强弱势排名"""
    from service.analysis.concept_board_market_strength import get_all_board_strength_ranking
    results = get_all_board_strength_ranking(limit=limit)
    return {"success": True, "data": results, "total": len(results)}


@router.get("/api/concept_board/market_strength/compute")
async def concept_board_market_strength_compute(
    board_code: str = Query(None, description="板块代码（空则计算全部）"),
    days: int = Query(60, ge=5, le=250, description="分析天数"),
):
    """计算板块大盘强弱势评分（触发计算并保存）"""
    from service.analysis.concept_board_market_strength import (
        compute_board_market_strength, compute_and_save_all_boards, _update_board_score
    )
    if board_code:
        result = compute_board_market_strength(board_code, days=days)
        if not result:
            return {"success": False, "error": "计算失败，可能板块无K线数据或大盘数据不足"}
        _update_board_score(board_code, result)
        return {"success": True, "data": result}
    else:
        summary = compute_and_save_all_boards(days=days)
        return {"success": True, "summary": summary}


@router.get("/api/concept_board/market_strength/detail")
async def concept_board_market_strength_detail(
    board_code: str = Query(..., description="板块代码"),
    days: int = Query(60, ge=5, le=250, description="分析天数"),
):
    """获取单个板块的大盘强弱势评分详情"""
    from service.analysis.concept_board_market_strength import compute_board_market_strength
    result = compute_board_market_strength(board_code, days=days)
    if not result:
        return {"success": False, "error": "计算失败"}
    return {"success": True, "data": result}
