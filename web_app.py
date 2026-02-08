from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import asyncio

from api.web_api import app
from common.constants.stocks_data import get_stock_code
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_structure_markdown import get_stock_markdown
from service.eastmoney.stock_technical_markdown import get_technical_indicators_prompt
from service.processor.operation_advice import get_operation_advice

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
