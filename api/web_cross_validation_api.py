"""
数据交叉验证 API
"""
from fastapi import APIRouter, Query

router = APIRouter()


@router.get("/api/cross_validation_summary")
async def cross_validation_summary(run_date: str = Query(None)):
    """获取交叉验证汇总"""
    from dao.cross_validation_dao import get_latest_summary
    try:
        rows = get_latest_summary(run_date)
        return {"success": True, "data": rows}
    except Exception as e:
        return {"success": False, "message": str(e), "data": []}


@router.get("/api/cross_validation_details")
async def cross_validation_details(
    run_date: str = Query(...),
    category: str = Query(None),
    match_status: str = Query(None),
    limit: int = Query(200),
):
    """获取交叉验证明细"""
    from dao.cross_validation_dao import get_validation_details
    try:
        rows = get_validation_details(run_date, category, match_status, limit)
        return {"success": True, "data": rows}
    except Exception as e:
        return {"success": False, "message": str(e), "data": []}
