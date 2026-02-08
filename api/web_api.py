from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional, AsyncIterator, Callable
import json

from common.constants.stocks_data import get_stock_code
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_structure_markdown import get_stock_markdown, get_stock_markdown_for_llm_analyse
from service.eastmoney.stock_technical_markdown import get_technical_indicators_prompt
from service.processor.operation_advice import get_operation_advice
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient
from service.tests.stock_full_analysis_with_llm_search import stock_full_analysis

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


async def _stream_llm_analysis(
    request: StockRequest,
    llm_name: str,
    chat_stream_func: Callable,
    model: str
) -> AsyncIterator[str]:
    """通用的LLM流式分析函数"""
    try:
        yield f"data: {json.dumps({'stage': 'fetching', 'message': '正在获取数据'}, ensure_ascii=False)}\n\n"
        
        stock_code = get_stock_code(request.stock_name)
        main_stock_result = await get_stock_markdown_for_llm_analyse(
            normalize_stock_code(stock_code), request.stock_name
        )
        operation_advice = get_operation_advice(request.advice_type, request.holding_price)
        if operation_advice:
            main_stock_result += f"# {operation_advice}\n"
        
        yield f"data: {json.dumps({'stage': 'analyzing', 'message': f'正在调用大模型{llm_name}'}, ensure_ascii=False)}\n\n"
        
        async for content in chat_stream_func(
            messages=[{"role": "user", "content": main_stock_result}],
            model=model
        ):
            yield f"data: {json.dumps({'stage': 'streaming', 'content': content}, ensure_ascii=False)}\n\n"
        
        yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"


@app.post("/api/can_slim_deepseek")
async def get_can_slim_deepseek_analysis(request: StockRequest):
    client = DeepSeekClient()
    return StreamingResponse(
        _stream_llm_analysis(request, "DeepSeek", client.chat_stream, "deepseek-chat"),
        media_type="text/event-stream"
    )


@app.post("/api/can_slim_gemini")
async def get_can_slim_gemini_analysis(request: StockRequest):
    client = GeminiClient()
    return StreamingResponse(
        _stream_llm_analysis(request, "Gemini", client.chat_stream, "gemini-3-pro-all"),
        media_type="text/event-stream"
    )


async def _stream_full_analysis(request: StockRequest) -> AsyncIterator[str]:
    """全量分析流式响应"""
    try:
        stock_code = get_stock_code(request.stock_name)
        normalized_code = normalize_stock_code(stock_code)
        
        # 创建一个队列来传递进度消息
        import asyncio
        progress_queue = asyncio.Queue()
        
        async def progress_callback(stage: str, message: str, status: str = None):
            await progress_queue.put((stage, message, status))
        
        # 启动分析任务
        analysis_task = asyncio.create_task(
            stock_full_analysis(normalized_code, request.stock_name, progress_callback)
        )
        
        # 发送初始状态
        yield f"data: {json.dumps({'stage': 'fetching', 'message': '正在启动全量分析'}, ensure_ascii=False)}\n\n"
        
        # 循环获取进度消息
        while not analysis_task.done():
            try:
                stage, message, status = await asyncio.wait_for(progress_queue.get(), timeout=0.1)
                if status:
                    yield f"data: {json.dumps({'stage': stage, 'message': message, 'status': status}, ensure_ascii=False)}\n\n"
                else:
                    yield f"data: {json.dumps({'stage': stage, 'message': message}, ensure_ascii=False)}\n\n"
            except asyncio.TimeoutError:
                continue
        
        # 获取分析结果
        result = await analysis_task
        
        # 发送剩余的进度消息
        while not progress_queue.empty():
            stage, message, status = await progress_queue.get()
            if status:
                yield f"data: {json.dumps({'stage': stage, 'message': message, 'status': status}, ensure_ascii=False)}\n\n"
            else:
                yield f"data: {json.dumps({'stage': stage, 'message': message}, ensure_ascii=False)}\n\n"
        
        yield f"data: {json.dumps({'stage': 'streaming', 'content': result}, ensure_ascii=False)}\n\n"
        yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"


@app.post("/api/full_analysis_deepseek")
async def get_full_analysis_deepseek(request: StockRequest):
    return StreamingResponse(
        _stream_full_analysis(request),
        media_type="text/event-stream"
    )

