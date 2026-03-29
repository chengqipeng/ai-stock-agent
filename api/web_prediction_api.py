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
    v20_direction: str = Query(None, description="V20量价预测: UP/NO_SIGNAL"),
    v30_direction: str = Query(None, description="V30情绪预测: UP/NO_SIGNAL"),
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
            nw_direction=nw_direction,
            v20_direction=v20_direction, v30_direction=v30_direction,
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
    iso_year: int = Query(None, description="目标周ISO年"),
    iso_week: int = Query(None, description="目标周ISO周"),
    direction: str = Query(None, description="预测方向: UP/DOWN"),
    result: str = Query(None, description="验证结果: correct/wrong/pending"),
    keyword: str = Query(None, description="股票代码或名称"),
    sort_by: str = Query("stock_code"),
    sort_dir: str = Query("asc"),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """获取本周预测验证数据：按目标周查询，与多模型叠加逻辑一致"""
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
    iso_year: int = Query(None, description="目标周ISO年"),
    iso_week: int = Query(None, description="目标周ISO周"),
    direction: str = Query(None, description="下周预测方向: UP/DOWN"),
    result: str = Query(None, description="验证结果: correct/wrong/pending"),
    keyword: str = Query(None, description="股票代码或名称"),
    sort_by: str = Query("stock_code"),
    sort_dir: str = Query("asc"),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """获取下周预测验证数据：按目标周（nw_iso_week）查询，与多模型叠加逻辑一致"""
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


@router.get("/api/weekly_prediction/v20_verification")
async def v20_prediction_verification(
    iso_year: int = Query(None, description="目标周ISO年"),
    iso_week: int = Query(None, description="目标周ISO周"),
    direction: str = Query(None, description="预测方向: UP"),
    result: str = Query(None, description="验证结果: correct/wrong/pending"),
    keyword: str = Query(None, description="股票代码或名称"),
    sort_by: str = Query("stock_code"),
    sort_dir: str = Query("asc"),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """获取V20量价超跌反弹预测验证数据：v20_pred_direction vs 实际5日涨跌"""
    try:
        from dao.stock_weekly_prediction_dao import get_v20_prediction_verification
        keywords = None
        if keyword:
            terms = re.split(r'[,，、;；\s]+', keyword.strip())
            keywords = [t.strip() for t in terms if t.strip()]
            if not keywords:
                keywords = None
        rows, total, summary = get_v20_prediction_verification(
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
        logger.error("查询V20量价预测验证失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/weekly_prediction/v30_verification")
async def v30_prediction_verification(
    iso_year: int = Query(None, description="目标周ISO年"),
    iso_week: int = Query(None, description="目标周ISO周"),
    direction: str = Query(None, description="预测方向: UP"),
    result: str = Query(None, description="验证结果: correct/wrong/pending"),
    keyword: str = Query(None, description="股票代码或名称"),
    sort_by: str = Query("stock_code"),
    sort_dir: str = Query("asc"),
    limit: int = Query(50),
    offset: int = Query(0),
):
    """获取V30情绪因子预测验证数据：v30_pred_direction vs 实际5日涨跌"""
    try:
        from dao.stock_weekly_prediction_dao import get_v30_prediction_verification
        keywords = None
        if keyword:
            terms = re.split(r'[,，、;；\s]+', keyword.strip())
            keywords = [t.strip() for t in terms if t.strip()]
            if not keywords:
                keywords = None
        rows, total, summary = get_v30_prediction_verification(
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
        logger.error("查询V30情绪预测验证失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/weekly_prediction/v12_verification")
async def v12_prediction_verification(
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
    """获取V12极端信号预测验证数据：v12_pred_direction vs 实际下周涨跌"""
    try:
        from dao.stock_weekly_prediction_dao import get_v12_prediction_verification
        keywords = None
        if keyword:
            terms = re.split(r'[,，、;；\s]+', keyword.strip())
            keywords = [t.strip() for t in terms if t.strip()]
            if not keywords:
                keywords = None
        rows, total, summary = get_v12_prediction_verification(
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
        logger.error("查询V12极端信号预测验证失败: %s", e, exc_info=True)
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
        tw_results = [r for r in results if r.get('type') not in ('nw', 'v20')]
        nw_results = [r for r in results if r.get('type') == 'nw']
        v20_results = [r for r in results if r.get('type') == 'v20']
        v30_results = [r for r in results if r.get('type') == 'v30']
        total_verified = sum(r.get('verified', 0) for r in results)
        total_correct = sum(r.get('correct', 0) for r in results)
        nw_verified = sum(r.get('verified', 0) for r in nw_results)
        nw_skipped = sum(r.get('skipped', 0) for r in nw_results)
        v20_verified = sum(r.get('verified', 0) for r in v20_results)
        v20_correct = sum(r.get('correct', 0) for r in v20_results)
        v20_skipped = sum(r.get('skipped', 0) for r in v20_results)
        v30_verified = sum(r.get('verified', 0) for r in v30_results)
        v30_correct = sum(r.get('correct', 0) for r in v30_results)
        v30_skipped = sum(r.get('skipped', 0) for r in v30_results)
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
                "nw_skipped": nw_skipped,
                "v20_verified": v20_verified,
                "v20_correct": v20_correct,
                "v20_skipped": v20_skipped,
                "v20_accuracy": round(v20_correct / v20_verified * 100, 1) if v20_verified > 0 else None,
                "v30_verified": v30_verified,
                "v30_correct": v30_correct,
                "v30_skipped": v30_skipped,
                "v30_accuracy": round(v30_correct / v30_verified * 100, 1) if v30_verified > 0 else None,
                "message": None if total_verified > 0 else "当前无可验证数据（V20/V30需predict_date后5个交易日K线，NW需目标周完整K线）",
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


@router.post("/api/weekly_prediction/verify_nw_week")
async def verify_nw_week_prediction(
    iso_year: int = Query(..., description="预测周ISO年"),
    iso_week: int = Query(..., description="预测周ISO周"),
):
    """验证指定预测周的V11下周预测结果"""
    try:
        from service.prediction_verify_service import verify_nw_week_predictions
        result = verify_nw_week_predictions(iso_year, iso_week)
        return {"success": True, "data": result}
    except Exception as e:
        logger.error("NW验证 Y%d-W%02d 失败: %s", iso_year, iso_week, e, exc_info=True)
        return {"success": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════
# 月预测 API
# ═══════════════════════════════════════════════════════════

@router.get("/api/monthly_prediction/list")
async def monthly_prediction_list(
    confidence: str = Query(None, description="high/medium/low"),
    keyword: str = Query(None, description="股票代码或名称"),
    sort_by: str = Query("composite_score"),
    sort_dir: str = Query("desc"),
    limit: int = Query(2000),
    offset: int = Query(0),
):
    """分页查询月度预测列表（从 canslim_monthly_prediction 表读取）"""
    try:
        keywords = None
        if keyword:
            terms = re.split(r'[,，、;；\s]+', keyword.strip())
            keywords = [t.strip() for t in terms if t.strip()]
            if not keywords:
                keywords = None
        from service.can_slim_algo.canslim_monthly_prediction_service import get_canslim_predictions_page
        rows, total = get_canslim_predictions_page(
            confidence=confidence, keywords=keywords,
            sort_by=sort_by, sort_dir=sort_dir, limit=limit, offset=offset,
        )
        # 序列化
        for r in rows:
            for k, v in list(r.items()):
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    r[k] = str(v)
        return {"success": True, "data": rows, "total": total}
    except Exception as e:
        logger.error("查询月度预测列表失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/monthly_prediction/predict_stock")
async def monthly_predict_single_stock(
    stock_code: str = Query(..., description="股票代码(如600519.SH)"),
):
    """对单只股票进行CAN SLIM月度预测"""
    try:
        from service.can_slim_algo.canslim_monthly_prediction_service import predict_single_stock
        result = predict_single_stock(stock_code)
        return {"success": True, "data": result}
    except Exception as e:
        logger.error("单股月度预测失败 %s: %s", stock_code, e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/monthly_prediction/summary")
async def monthly_prediction_summary():
    """获取月度预测汇总统计（从 canslim_monthly_prediction 表读取）"""
    try:
        from service.can_slim_algo.canslim_monthly_prediction_service import get_canslim_prediction_summary
        data = get_canslim_prediction_summary()
        return {"success": True, "data": data}
    except Exception as e:
        logger.error("获取月度预测汇总失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/monthly_prediction/history")
async def monthly_prediction_history(
    stock_code: str = Query(None, description="股票代码"),
    target_year: int = Query(None, description="目标年"),
    target_month: int = Query(None, description="目标月"),
    limit: int = Query(50),
):
    """查询CAN SLIM月度预测历史"""
    try:
        from service.can_slim_algo.canslim_monthly_prediction_service import get_monthly_prediction_history
        rows = get_monthly_prediction_history(stock_code, target_year, target_month, limit)
        # 序列化
        for r in rows:
            for k, v in list(r.items()):
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    r[k] = str(v)
        return {"success": True, "data": rows}
    except Exception as e:
        logger.error("查询月度预测历史失败: %s", e, exc_info=True)
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


# ═══════════════════════════════════════════════════════════
# 多模型统一验证 API
# ═══════════════════════════════════════════════════════════

@router.get("/api/prediction/unified_verification")
async def unified_verification(
    iso_year: int = Query(None),
    iso_week: int = Query(None),
    keyword: str = Query(None, description="股票代码或名称"),
    nw_dir: str = Query(None, description="NW下周预测方向: UP/DOWN/any"),
    v20_dir: str = Query(None, description="V20方向: UP/any/none"),
    v30_dir: str = Query(None, description="V30方向: UP/DOWN/any/none"),
    nw_result: str = Query(None, description="NW验证: correct/wrong/pending"),
    v20_result: str = Query(None, description="V20验证: correct/wrong/pending"),
    v30_result: str = Query(None, description="V30验证: correct/wrong/pending"),
    combo: str = Query(None, description="组合筛选: all_up/nw_v30_up/nw_v20_up"),
    confidence: str = Query(None, description="置信度筛选: all_high/nw_high/v20_high/v30_high"),
    sort_by: str = Query("stock_code"),
    sort_dir: str = Query("asc"),
    limit: int = Query(100),
    offset: int = Query(0),
):
    """多模型统一验证：按目标周（nw_iso_week）对齐 V11 + V20 + V30

    前端传入的 iso_year/iso_week 是目标周（即预测要验证的那一周），
    后端用 nw_iso_year/nw_iso_week 匹配，确保三个模型对齐到同一周。
    """
    from dao import get_connection
    from dao.stock_weekly_prediction_dao import _is_target_week_finished, _mask_unfinished_actual
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 默认取最近的目标周
        if iso_year is None or iso_week is None:
            cur.execute("""
                SELECT nw_iso_year, nw_iso_week FROM stock_weekly_prediction_history
                WHERE nw_pred_direction IS NOT NULL AND nw_pred_direction != ''
                  AND nw_iso_year IS NOT NULL AND nw_iso_week IS NOT NULL
                ORDER BY nw_iso_year DESC, nw_iso_week DESC LIMIT 1
            """)
            r = cur.fetchone()
            if r:
                iso_year, iso_week = r['nw_iso_year'], r['nw_iso_week']
            else:
                return {"success": True, "data": [], "total": 0, "summary": {}}

        # 按目标周筛选（nw_iso_year/nw_iso_week = 预测验证的那一周）
        wheres = ["h.nw_iso_year = %s", "h.nw_iso_week = %s",
                  "h.nw_pred_direction IS NOT NULL", "h.nw_pred_direction != ''"]
        params = [iso_year, iso_week]

        if keyword:
            terms = re.split(r'[,，、;；\s]+', keyword.strip())
            terms = [t for t in terms if t]
            if terms:
                kw_conds = []
                for t in terms:
                    kw_conds.append("(h.stock_code LIKE %s OR h.stock_name LIKE %s)")
                    params.extend([f'%{t}%', f'%{t}%'])
                wheres.append("(" + " OR ".join(kw_conds) + ")")

        if nw_dir and nw_dir != 'any':
            wheres.append("h.nw_pred_direction = %s")
            params.append(nw_dir)
        if v20_dir:
            if v20_dir == 'none':
                wheres.append("(h.v20_pred_direction IS NULL OR h.v20_pred_direction = '')")
            elif v20_dir != 'any':
                wheres.append("h.v20_pred_direction = %s")
                params.append(v20_dir)
            else:
                wheres.append("h.v20_pred_direction IS NOT NULL AND h.v20_pred_direction != ''")
        if v30_dir:
            if v30_dir == 'none':
                wheres.append("(h.v30_pred_direction IS NULL OR h.v30_pred_direction = '')")
            elif v30_dir != 'any':
                wheres.append("h.v30_pred_direction = %s")
                params.append(v30_dir)
            else:
                wheres.append("h.v30_pred_direction IS NOT NULL AND h.v30_pred_direction != ''")

        # 目标周未结束时，忽略结果筛选（DB中可能有脏数据）
        _week_finished = _is_target_week_finished(iso_year, iso_week)
        if _week_finished:
            if nw_result == 'correct':
                wheres.append("h.nw_is_correct = 1")
            elif nw_result == 'wrong':
                wheres.append("h.nw_is_correct = 0")
            elif nw_result == 'pending':
                wheres.append("h.nw_is_correct IS NULL")

            if v20_result == 'correct':
                wheres.append("h.v20_is_correct = 1")
            elif v20_result == 'wrong':
                wheres.append("h.v20_is_correct = 0")
            elif v20_result == 'pending':
                wheres.append("h.v20_is_correct IS NULL AND h.v20_pred_direction IS NOT NULL AND h.v20_pred_direction != ''")

            if v30_result == 'correct':
                wheres.append("h.v30_is_correct = 1")
            elif v30_result == 'wrong':
                wheres.append("h.v30_is_correct = 0")
            elif v30_result == 'pending':
                wheres.append("h.v30_is_correct IS NULL AND h.v30_pred_direction IS NOT NULL AND h.v30_pred_direction != ''")

        if combo == 'all_up':
            wheres.append("h.nw_pred_direction = 'UP'")
            wheres.append("h.v20_pred_direction = 'UP'")
            wheres.append("h.v30_pred_direction = 'UP'")
        elif combo == 'nw_v30_up':
            wheres.append("h.nw_pred_direction = 'UP'")
            wheres.append("h.v30_pred_direction = 'UP'")
        elif combo == 'nw_v20_up':
            wheres.append("h.nw_pred_direction = 'UP'")
            wheres.append("h.v20_pred_direction = 'UP'")

        if confidence == 'all_high':
            wheres.append("h.nw_confidence = 'high'")
            wheres.append("h.v20_confidence = 'high'")
            wheres.append("h.v30_confidence IN ('high', 'medium')")
        elif confidence == 'nw_high':
            wheres.append("h.nw_confidence = 'high'")
        elif confidence == 'v20_high':
            wheres.append("h.v20_confidence = 'high'")
        elif confidence == 'v30_high':
            wheres.append("h.v30_confidence IN ('high', 'medium')")

        where_sql = " AND ".join(wheres)

        # 排序
        allowed_sorts = {
            'stock_code': 'h.stock_code', 'stock_name': 'h.stock_name',
            'nw_actual_weekly_chg': 'h.nw_actual_weekly_chg',
            'v20_actual_5d_chg': 'h.v20_actual_5d_chg',
            'v30_actual_5d_chg': 'h.v30_actual_5d_chg',
            'nw_confidence': 'h.nw_confidence',
            'v30_composite_score': 'h.v30_composite_score',
        }
        order_col = allowed_sorts.get(sort_by, 'h.stock_code')
        order_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        # 总数
        cur.execute(f"SELECT COUNT(*) as cnt FROM stock_weekly_prediction_history h WHERE {where_sql}", params)
        total = cur.fetchone()['cnt']

        # 数据 — 返回目标周信息供前端显示
        cur.execute(f"""
            SELECT h.stock_code, h.stock_name, h.predict_date,
                   h.nw_iso_year, h.nw_iso_week,
                   h.nw_pred_direction, h.nw_confidence, h.nw_strategy,
                   h.nw_composite_score, h.nw_pred_chg,
                   h.nw_actual_direction, h.nw_actual_weekly_chg, h.nw_is_correct,
                   h.v20_pred_direction, h.v20_confidence, h.v20_rule_name,
                   h.v20_actual_direction, h.v20_actual_5d_chg, h.v20_is_correct,
                   h.v30_pred_direction, h.v30_confidence, h.v30_strategy,
                   h.v30_composite_score,
                   h.v30_actual_direction, h.v30_actual_5d_chg, h.v30_is_correct
            FROM stock_weekly_prediction_history h
            WHERE {where_sql}
            ORDER BY {order_col} {order_dir}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        # 如果目标周尚未结束，屏蔽 actual 字段（防止显示错误回填数据）
        if not _is_target_week_finished(iso_year, iso_week):
            _all_actual = [
                'nw_actual_direction', 'nw_actual_weekly_chg', 'nw_is_correct',
                'v20_actual_direction', 'v20_actual_5d_chg', 'v20_is_correct',
                'v30_actual_direction', 'v30_actual_5d_chg', 'v30_is_correct',
            ]
            _mask_unfinished_actual(rows, _all_actual, iso_year, iso_week)

        # 序列化
        for r in rows:
            for k, v in list(r.items()):
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    r[k] = str(v)

        # 汇总统计
        cur.execute(f"""
            SELECT COUNT(*) as total,
                   SUM(h.nw_is_correct = 1) as nw_correct,
                   SUM(h.nw_is_correct = 0) as nw_wrong,
                   SUM(h.nw_is_correct IS NULL) as nw_pending,
                   AVG(h.nw_actual_weekly_chg) as nw_avg_chg,
                   SUM(h.v20_pred_direction IS NOT NULL AND h.v20_pred_direction != '') as v20_total,
                   SUM(h.v20_is_correct = 1) as v20_correct,
                   SUM(h.v20_is_correct = 0) as v20_wrong,
                   AVG(CASE WHEN h.v20_pred_direction IS NOT NULL AND h.v20_pred_direction != '' THEN h.v20_actual_5d_chg END) as v20_avg_chg,
                   SUM(h.v30_pred_direction IS NOT NULL AND h.v30_pred_direction != '') as v30_total,
                   SUM(h.v30_is_correct = 1) as v30_correct,
                   SUM(h.v30_is_correct = 0) as v30_wrong,
                   AVG(CASE WHEN h.v30_pred_direction IS NOT NULL AND h.v30_pred_direction != '' THEN h.v30_actual_5d_chg END) as v30_avg_chg
            FROM stock_weekly_prediction_history h
            WHERE {where_sql}
        """, params)
        sm = cur.fetchone()
        summary = {}
        for k, v in sm.items():
            summary[k] = float(v) if v is not None and not isinstance(v, (str, int, bool)) else v

        # 目标周未结束时，汇总中的 actual 统计也要屏蔽
        if not _is_target_week_finished(iso_year, iso_week):
            summary['nw_correct'] = 0
            summary['nw_wrong'] = 0
            summary['nw_pending'] = summary.get('total', 0)
            summary['nw_avg_chg'] = None
            summary['v20_correct'] = 0
            summary['v20_wrong'] = 0
            summary['v20_avg_chg'] = None
            summary['v30_correct'] = 0
            summary['v30_wrong'] = 0
            summary['v30_avg_chg'] = None

        return {"success": True, "data": rows, "total": total, "summary": summary,
                "target_year": iso_year, "target_week": iso_week}
    except Exception as e:
        logger.error("统一验证查询失败: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        cur.close()
        conn.close()


@router.get("/api/prediction/unified_weeks")
async def unified_weeks():
    """获取目标周列表（按 nw_iso_year/nw_iso_week 分组，含各模型统计）

    返回的 iso_year/iso_week 是目标周（预测要验证的那一周），
    前端选择后传给 unified_verification 查询。
    """
    from dao import get_connection
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT nw_iso_year as iso_year, nw_iso_week as iso_week,
                   COUNT(*) as total,
                   MIN(predict_date) as min_date,
                   MAX(predict_date) as max_date,
                   SUM(nw_pred_direction = 'UP') as nw_up,
                   SUM(nw_pred_direction = 'DOWN') as nw_down,
                   SUM(nw_is_correct = 1) as nw_correct,
                   SUM(nw_is_correct IS NOT NULL) as nw_verified,
                   SUM(v20_pred_direction IS NOT NULL AND v20_pred_direction != '') as v20_cnt,
                   SUM(v20_is_correct = 1) as v20_correct,
                   SUM(v20_is_correct IS NOT NULL) as v20_verified,
                   SUM(v30_pred_direction IS NOT NULL AND v30_pred_direction != '') as v30_cnt,
                   SUM(v30_is_correct = 1) as v30_correct,
                   SUM(v30_is_correct IS NOT NULL) as v30_verified
            FROM stock_weekly_prediction_history
            WHERE nw_pred_direction IS NOT NULL AND nw_pred_direction != ''
              AND nw_iso_year IS NOT NULL AND nw_iso_week IS NOT NULL
            GROUP BY nw_iso_year, nw_iso_week
            ORDER BY nw_iso_year DESC, nw_iso_week DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        for r in rows:
            for k, v in list(r.items()):
                if v is not None and not isinstance(v, (str, int, bool)):
                    try:
                        r[k] = int(v)
                    except (ValueError, TypeError):
                        r[k] = str(v)
        return {"success": True, "data": rows}
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        cur.close()
        conn.close()
