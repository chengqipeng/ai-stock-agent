from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional
import json
import asyncio

from common.constants.stocks_data import get_stock_code
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_structure_markdown import get_stock_markdown, get_stock_markdown_for_llm_analyse, \
    _get_analysis_header, _build_stock_markdown
from service.eastmoney.stock_technical_markdown import get_technical_indicators_prompt
from service.processor.operation_advice import get_operation_advice
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient

app = FastAPI(title="AI Stock Agent")

class StockRequest(BaseModel):
    stock_name: str
    advice_type: int = 1
    holding_price: Optional[float] = None


@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/stocks")
async def get_stocks():
    from common.constants.stocks_data import STOCKS
    return {"success": True, "data": STOCKS}


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


@app.post("/api/can_slim_deepseek")
async def get_can_slim_deepseek_analysis(request: StockRequest):
    async def generate():
        try:
            # 阶段1: 获取数据
            yield f"data: {json.dumps({'stage': 'fetching', 'message': '正在获取数据'}, ensure_ascii=False)}\n\n"
            
            stock_code = get_stock_code(request.stock_name)

            header = _get_analysis_header(stock_code, request.stock_name, mode="analyse")
            body = await _build_stock_markdown(normalize_stock_code(stock_code), request.stock_name, 90)
            main_stock_result = header + body

            operation_advice = get_operation_advice(request.advice_type, request.holding_price)
            if operation_advice:
                main_stock_result += f"# {operation_advice}\n"
            
            # 阶段2: 调用大模型
            yield f"data: {json.dumps({'stage': 'analyzing', 'message': '正在调用大模型DeepSeek'}, ensure_ascii=False)}\n\n"
            
            client = DeepSeekClient()
            async for content in client.chat_stream(
                messages=[{"role": "user", "content": main_stock_result}],
                model="deepseek-chat"
            ):
                yield f"data: {json.dumps({'stage': 'streaming', 'content': content}, ensure_ascii=False)}\n\n"
            
            # 完成
            yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(generate(), media_type="text/event-stream")


@app.post("/api/can_slim_gemini")
async def get_can_slim_gemini_analysis(request: StockRequest):
    async def generate():
        try:
            # 阶段1: 获取数据
            yield f"data: {json.dumps({'stage': 'fetching', 'message': '正在获取数据'}, ensure_ascii=False)}\n\n"
            
            stock_code = get_stock_code(request.stock_name)
            main_stock_result = await get_stock_markdown_for_llm_analyse(normalize_stock_code(stock_code), request.stock_name)
            operation_advice = get_operation_advice(request.advice_type, request.holding_price)
            if operation_advice:
                main_stock_result += f"# {operation_advice}\n"
            
            # 阶段2: 调用大模型
            yield f"data: {json.dumps({'stage': 'analyzing', 'message': '正在调用大模型Gemini'}, ensure_ascii=False)}\n\n"
            
            client = GeminiClient()
            async for content in client.chat_stream(
                messages=[{"role": "user", "content": main_stock_result}],
                model="gemini-3-pro-all"
            ):
                yield f"data: {json.dumps({'stage': 'streaming', 'content': content}, ensure_ascii=False)}\n\n"
            
            # 完成
            yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(generate(), media_type="text/event-stream")

