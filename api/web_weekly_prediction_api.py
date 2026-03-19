"""周预测页面路由

API端点已迁移至 api/web_prediction_api.py，
此文件保留HTML页面路由并聚合所有预测相关router。
"""
import logging

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from dao.stock_weekly_prediction_dao import ensure_tables

logger = logging.getLogger(__name__)

router = APIRouter()

# 启动时确保表结构（含新增列迁移）
try:
    ensure_tables()
except Exception as _e:
    logger.warning("周预测表迁移跳过: %s", _e)


@router.get("/weekly_prediction", response_class=HTMLResponse)
async def weekly_prediction_page():
    with open("static/weekly_prediction.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache", "Expires": "0",
    })


# 聚合预测API路由
from api.web_prediction_api import router as prediction_router  # noqa: E402
router.include_router(prediction_router)
