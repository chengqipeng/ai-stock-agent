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
    get_prediction_verification,
    get_nw_prediction_verification,
    get_available_prediction_weeks,
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
    nw_direction: str = Query(None, description="下周预测方向: UP/DOWN/UNCERTAIN/HAS_SIGNAL"),
    v5_direction: str = Query(None, description="OBV5日预测: UP/HAS_SIGNAL/NO_SIGNAL"),
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
            nw_direction=nw_direction, v5_direction=v5_direction,
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
# 预测验证 API
# ═══════════════════════════════════════════════════════════

@router.get("/api/weekly_prediction/verification")
async def prediction_verification(
    iso_year: int = Query(None, description="ISO年"),
    iso_week: int = Query(None, description="ISO周"),
    direction: str = Query(None, description="预测方向: UP/DOWN"),
    result: str = Query(None, description="验证结果: correct/wrong/pending"),
    keyword: str = Query(None, description="股票代码或名称"),
    sort_by: str = Query("stock_code"),
    sort_dir: str = Query("asc"),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """获取预测验证数据：上一周预测 vs 实际结果"""
    try:
        keywords = None
        if keyword:
            terms = re.split(r'[,，、;；\s]+', keyword.strip())
            keywords = [t.strip() for t in terms if t.strip()]
            if not keywords:
                keywords = None
        rows, total, summary = get_prediction_verification(
            iso_year=iso_year, iso_week=iso_week,
            keywords=keywords, direction_filter=direction,
            result_filter=result,
            sort_by=sort_by, sort_dir=sort_dir,
            limit=limit, offset=offset,
        )
        # 序列化非基本类型
        for r in rows:
            for k, v in list(r.items()):
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    r[k] = str(v)
        for k, v in list(summary.items()):
            if v is not None and not isinstance(v, (str, int, float, bool)):
                summary[k] = str(v)
        return {"success": True, "data": rows, "total": total, "summary": summary}
    except Exception as e:
        logger.error("查询预测验证失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/weekly_prediction/nw_verification")
async def nw_prediction_verification(
    iso_year: int = Query(None, description="预测发生的ISO年"),
    iso_week: int = Query(None, description="预测发生的ISO周"),
    direction: str = Query(None, description="下周预测方向: UP/DOWN"),
    result: str = Query(None, description="验证结果: correct/wrong/pending"),
    keyword: str = Query(None, description="股票代码或名称"),
    sort_by: str = Query("stock_code"),
    sort_dir: str = Query("asc"),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """获取下周预测验证数据：W周的nw_pred_direction vs W+1周实际结果"""
    try:
        keywords = None
        if keyword:
            terms = re.split(r'[,，、;；\s]+', keyword.strip())
            keywords = [t.strip() for t in terms if t.strip()]
            if not keywords:
                keywords = None
        rows, total, summary = get_nw_prediction_verification(
            iso_year=iso_year, iso_week=iso_week,
            keywords=keywords, direction_filter=direction,
            result_filter=result,
            sort_by=sort_by, sort_dir=sort_dir,
            limit=limit, offset=offset,
        )
        for r in rows:
            for k, v in list(r.items()):
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    r[k] = str(v)
        for k, v in list(summary.items()):
            if v is not None and not isinstance(v, (str, int, float, bool)):
                summary[k] = str(v)
        return {"success": True, "data": rows, "total": total, "summary": summary}
    except Exception as e:
        logger.error("查询下周预测验证失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/weekly_prediction/weeks")
async def prediction_weeks(limit: int = Query(20)):
    """获取有预测记录的周列表"""
    try:
        weeks = get_available_prediction_weeks(limit)
        for w in weeks:
            for k, v in list(w.items()):
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    w[k] = str(v)
        return {"success": True, "data": weeks}
    except Exception as e:
        logger.error("查询预测周列表失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════
# 预测结果验证（回填实际数据）
# ═══════════════════════════════════════════════════════════

@router.post("/api/weekly_prediction/verify_all")
async def verify_all_predictions():
    """验证所有待验证的历史预测（用实际K线数据回填），包括下周预测目标周"""
    try:
        from service.prediction_verify_service import verify_all_pending_weeks
        results = verify_all_pending_weeks()
        tw_results = [r for r in results if r.get('type') != 'nw']
        nw_results = [r for r in results if r.get('type') == 'nw']
        total_verified = sum(r.get('verified', 0) for r in results)
        total_correct = sum(r.get('correct', 0) for r in results)
        nw_verified = sum(r.get('verified', 0) for r in nw_results)
        return {
            "success": True,
            "data": results,
            "summary": {
                "weeks_processed": len(results),
                "total_verified": total_verified,
                "total_correct": total_correct,
                "accuracy": round(total_correct / total_verified * 100, 1) if total_verified > 0 else None,
                "nw_weeks_processed": len(nw_results),
                "nw_verified": nw_verified,
            }
        }
    except Exception as e:
        logger.error("批量验证预测失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/weekly_prediction/verify_week")
async def verify_week_prediction(
    iso_year: int = Query(..., description="ISO年"),
    iso_week: int = Query(..., description="ISO周"),
):
    """验证指定周的预测结果"""
    try:
        from service.prediction_verify_service import verify_week_predictions
        result = verify_week_predictions(iso_year, iso_week)
        return {"success": True, "data": result}
    except Exception as e:
        logger.error("验证 Y%d-W%02d 预测失败: %s", iso_year, iso_week, e, exc_info=True)
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
