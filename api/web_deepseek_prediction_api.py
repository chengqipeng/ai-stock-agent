"""DeepSeek 周预测 API — 独立于周预测系统"""
import logging

from fastapi import APIRouter, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()


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
