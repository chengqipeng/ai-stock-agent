"""数据浏览器 API — 调度数据分页查询"""
import datetime as _dt
import logging

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse

from dao import get_connection
from dao.stock_time_data_dao import get_time_data

logger = logging.getLogger(__name__)

router = APIRouter()

_BROWSABLE_TABLES = {
    "stock_kline", "stock_finance", "stock_highest_lowest_price",
    "stock_batch_technical_score", "stock_time_data", "stock_order_book",
    "stock_dragon_tiger", "stock_fund_flow",
}


@router.get("/data_browser", response_class=HTMLResponse)
async def data_browser_page():
    with open("static/data_browser.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    })


@router.get("/api/data_browser/tables")
async def data_browser_tables():
    """列出可浏览的数据表及行数"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        result = []
        for t in sorted(_BROWSABLE_TABLES):
            cursor.execute(f"SELECT COUNT(*) AS cnt FROM {t}")
            row = cursor.fetchone()
            result.append({"table": t, "count": row["cnt"]})
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/api/data_browser/time_data")
async def data_browser_time_data(
    stock_code: str = Query(..., description="股票代码"),
    trade_date: str = Query(..., description="交易日期 YYYY-MM-DD"),
):
    """查询某只股票某天的分时数据，用于分时图展示"""
    try:
        rows = get_time_data(stock_code, trade_date)
        # 转换 Decimal / date 等类型
        for row in rows:
            for k, v in row.items():
                if isinstance(v, (_dt.datetime, _dt.date)):
                    row[k] = v.isoformat()
        return {"success": True, "stock_code": stock_code, "trade_date": trade_date, "data": rows}
    except Exception as e:
        logger.error("分时数据查询失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/data_browser/time_data_dates")
async def data_browser_time_data_dates(
    stock_code: str = Query(..., description="股票代码"),
):
    """查询某只股票有分时数据的日期列表（最近30天）"""
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT DISTINCT trade_date FROM stock_time_data "
            "WHERE stock_code = %s ORDER BY trade_date DESC LIMIT 30",
            (stock_code,),
        )
        dates = [r["trade_date"] for r in cursor.fetchall()]
        return {"success": True, "stock_code": stock_code, "dates": dates}
    except Exception as e:
        logger.error("分时日期查询失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/api/data_browser/query")
async def data_browser_query(
    table: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    stock_code: str = Query(None),
    date_from: str = Query(None),
    date_to: str = Query(None),
    order_by: str = Query(None),
    order_dir: str = Query("DESC"),
):
    """通用分页查询调度数据表"""
    if table not in _BROWSABLE_TABLES:
        raise HTTPException(status_code=400, detail=f"不允许查询表: {table}")
    conn = get_connection(use_dict_cursor=True)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW COLUMNS FROM {table}")
        columns = [r["Field"] for r in cursor.fetchall()]

        conditions = []
        params = []
        if stock_code:
            conditions.append("stock_code LIKE %s")
            params.append(f"%{stock_code}%")

        date_col = None
        if "date" in columns:
            date_col = "`date`"
        elif "trade_date" in columns:
            date_col = "trade_date"
        elif "score_date" in columns:
            date_col = "score_date"
        if date_col:
            if date_from:
                conditions.append(f"{date_col} >= %s")
                params.append(date_from)
            if date_to:
                conditions.append(f"{date_col} <= %s")
                params.append(date_to)

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

        cursor.execute(f"SELECT COUNT(*) AS cnt FROM {table}{where}", params)
        total = cursor.fetchone()["cnt"]

        safe_dir = "ASC" if order_dir.upper() == "ASC" else "DESC"
        if order_by and order_by in columns:
            ob = f"`{order_by}`"
        elif date_col:
            ob = date_col
        else:
            ob = "id"

        offset = (page - 1) * page_size
        sql = f"SELECT * FROM {table}{where} ORDER BY {ob} {safe_dir} LIMIT %s OFFSET %s"
        cursor.execute(sql, params + [page_size, offset])
        rows = cursor.fetchall()

        for row in rows:
            for k, v in row.items():
                if isinstance(v, (_dt.datetime, _dt.date)):
                    row[k] = v.isoformat()

        return {
            "success": True, "table": table, "columns": columns,
            "data": rows, "total": total, "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }
    except Exception as e:
        logger.error("数据浏览查询失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
