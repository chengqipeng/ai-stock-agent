from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import asyncio

from common.constants.stocks_data import get_stock_code
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_structure_markdown import get_stock_markdown
from service.eastmoney.stock_technical_markdown import get_technical_indicators_prompt
from service.processor.operation_advice import get_operation_advice

app = FastAPI(title="AI Stock Agent")


class StockRequest(BaseModel):
    stock_name: str
    advice_type: int = 1
    holding_price: Optional[float] = None


@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.post("/api/can_slim")
async def get_can_slim_analysis(request: StockRequest):
    try:
        stock_code = get_stock_code(request.stock_name)
        main_stock_result = await get_stock_markdown(normalize_stock_code(stock_code), request.stock_name)
        operation_advice = get_operation_advice(request.advice_type, request.holding_price)
        if operation_advice:
            main_stock_result += f"# {operation_advice}\n"
        
        return {"success": True, "data": main_stock_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/technical")
async def get_technical_analysis(request: StockRequest):
    try:
        stock_code = get_stock_code(request.stock_name)
        technical_stock_result = await get_technical_indicators_prompt(
            normalize_stock_code(stock_code), stock_code, request.stock_name
        )
        operation_advice = get_operation_advice(request.advice_type, request.holding_price)
        if operation_advice:
            technical_stock_result += f"# {operation_advice}\n"
        
        return {"success": True, "data": technical_stock_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
