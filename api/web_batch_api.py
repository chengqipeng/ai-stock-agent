from fastapi import APIRouter, HTTPException, FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List
import json
import asyncio
import re
from datetime import datetime

from common.utils.stock_list_parser import parse_stock_list

from starlette.responses import HTMLResponse

app = FastAPI(title="AI Stock Agent")

@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/stock_list")
async def get_stock_list():
    """获取待分析股票列表"""
    try:
        file_path = "data_results/stock_to_score_list/stock_score_list.md"
        stocks = parse_stock_list(file_path)
        return {"success": True, "data": stocks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))