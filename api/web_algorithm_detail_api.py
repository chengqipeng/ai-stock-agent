"""算法逻辑详解页面 API"""
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/algorithm_detail", response_class=HTMLResponse)
async def algorithm_detail_page():
    with open("static/algorithm_detail.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    })
