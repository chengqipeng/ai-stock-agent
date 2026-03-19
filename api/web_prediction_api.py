"""预测 API — 周预测 / 月预测 / DeepSeek周预测"""
import logging
import re

from fastapi import APIRouter, Query
from pydantic import BaseModel

from dao.stock_weekly_prediction_dao import (
    get_latest_predictions_page,
    get_prediction_summary,
    get_prediction_history,
    get_prediction_accuracy_stats,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ═══════════════════════════════════════════════════════════
# 周预测 API
# ═══════════════════════════════════════════════════════════

@router.get("/api/weekly_prediction/summary")
async def prediction_summary():
    """获取最新预测汇总统计"""
    try:
        data = get_prediction_summary()
        return {"success": True, "data": data}
    except Exception as e:
        logger.error("获取预测汇总失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/weekly_prediction/list")
async def prediction_list(
    direction: str = Query(None, description="UP/DOWN"),
    confidence: str = Query(None, description="high/medium/low"),
    keyword: str = Query(None, description="股票代码或名称，多个用逗号分隔"),
    sort_by: str = Query("stock_code"),
    sort_dir: str = Query("asc"),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """分页查询最新预测列表，keyword支持多个关键词（逗号/空格/分号分隔）"""
    try:
        keywords = None
        if keyword:
            terms = re.split(r'[,，、;；\s]+', keyword.strip())
            keywords = [t.strip() for t in terms if t.strip()]
            if not keywords:
                keywords = None
        rows, total = get_latest_predictions_page(
            direction=direction, confidence=confidence, keywords=keywords,
            sort_by=sort_by, sort_dir=sort_dir, limit=limit, offset=offset,
        )
        return {"success": True, "data": rows, "total": total}
    except Exception as e:
        logger.error("查询预测列表失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/weekly_prediction/history")
async def prediction_history(
    stock_code: str = Query(..., description="股票代码"),
    limit: int = Query(30),
):
    """获取某只股票的预测历史"""
    try:
        rows = get_prediction_history(stock_code, limit)
        return {"success": True, "data": rows}
    except Exception as e:
        logger.error("查询预测历史失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/weekly_prediction/accuracy")
async def prediction_accuracy(
    iso_year: int = Query(None),
    iso_week: int = Query(None),
):
    """获取预测准确率统计"""
    try:
        data = get_prediction_accuracy_stats(iso_year, iso_week)
        return {"success": True, "data": data}
    except Exception as e:
        logger.error("查询准确率统计失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════
# 月预测 API
# ═══════════════════════════════════════════════════════════

@router.get("/api/monthly_prediction/list")
async def monthly_prediction_list(
    confidence: str = Query(None, description="high/medium/low"),
    keyword: str = Query(None, description="股票代码或名称"),
    sort_by: str = Query("nm_composite_score"),
    sort_dir: str = Query("desc"),
    limit: int = Query(200),
    offset: int = Query(0),
):
    """分页查询月度预测列表（仅返回有nm_pred_direction的股票）"""
    try:
        keywords = None
        if keyword:
            terms = re.split(r'[,，、;；\s]+', keyword.strip())
            keywords = [t.strip() for t in terms if t.strip()]
            if not keywords:
                keywords = None
        rows, total = get_latest_predictions_page(
            direction=None,
            confidence=None,
            keywords=keywords,
            sort_by=sort_by, sort_dir=sort_dir, limit=limit, offset=offset,
            monthly_only=True,
        )
        if confidence:
            rows = [r for r in rows if r.get('nm_confidence') == confidence]
        return {"success": True, "data": rows, "total": len(rows)}
    except Exception as e:
        logger.error("查询月度预测列表失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════
# DeepSeek 周预测 API
# ═══════════════════════════════════════════════════════════

class DeepSeekPredictRequest(BaseModel):
    stock_codes: list[str]


@router.post("/api/deepseek_prediction/predict")
async def deepseek_predict(request: DeepSeekPredictRequest):
    """对选中的股票执行DeepSeek周预测"""
    try:
        from service.deepseek_prediction_service import run_deepseek_prediction
        result = await run_deepseek_prediction(request.stock_codes)
        for r in result.get('results', []):
            for k, v in list(r.items()):
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    r[k] = str(v)
        return {"success": True, "data": result}
    except Exception as e:
        logger.error("DeepSeek周预测失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/deepseek_prediction/history")
async def deepseek_history(
    stock_code: str = Query(None, description="股票代码"),
    limit: int = Query(50),
):
    """获取DeepSeek周预测历史"""
    try:
        from dao.deepseek_prediction_dao import (
            get_prediction_history as ds_history,
            get_latest_predictions,
        )
        if stock_code:
            rows = ds_history(stock_code, limit)
        else:
            rows = get_latest_predictions(limit)
        for r in rows:
            for k, v in list(r.items()):
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    r[k] = str(v)
        return {"success": True, "data": rows}
    except Exception as e:
        logger.error("查询DeepSeek周预测历史失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/deepseek_prediction/stats")
async def deepseek_stats():
    """获取DeepSeek周预测准确率统计"""
    try:
        from dao.deepseek_prediction_dao import get_accuracy_stats
        data = get_accuracy_stats()
        return {"success": True, "data": data}
    except Exception as e:
        logger.error("查询DeepSeek周预测统计失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}
