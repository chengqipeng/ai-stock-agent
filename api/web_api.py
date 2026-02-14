from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional, AsyncIterator, Callable
import json

from common.constants.stocks_data import get_stock_code
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from data_results.database.history_db import init_db, insert_history, get_all_history, get_history_content
from data_results.database.batch_db import init_batch_tables
from service.eastmoney.stock_structure_markdown import get_stock_markdown, get_stock_markdown_for_llm_analyse
from service.eastmoney.stock_technical_markdown import get_technical_indicators_prompt
from service.processor.operation_advice import get_operation_advice
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient
from service.tests.stock_full_analysis_with_llm_search import stock_full_analysis
from api.web_batch_api import router as batch_router

app = FastAPI(title="AI Stock Agent")

# 初始化数据库
init_db()
init_batch_tables()

# 注册批量处理路由
app.include_router(batch_router)

def save_result(analysis_type: str, stock_info: StockInfo, result: str):
    """保存分析结果到数据库"""
    formatted_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    insert_history(analysis_type, stock_info.stock_name, stock_info.stock_code_normalize, formatted_time, result)


@app.get("/api/history")
async def get_history():
    """获取历史记录列表"""
    try:
        history = get_all_history()
        return {"success": True, "data": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history/{history_id}")
async def get_history_detail(history_id: int):
    """获取历史记录内容"""
    try:
        content = get_history_content(history_id)
        if content is None:
            raise HTTPException(status_code=404, detail="记录不存在")
        return {"success": True, "data": content}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class StockRequest(BaseModel):
    stock_name: str
    advice_type: int = 1
    holding_price: Optional[float] = None


@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/batch", response_class=HTMLResponse)
async def read_batch():
    with open("static/batch.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/stocks")
async def get_stocks():
    from common.constants.stocks_data import ALL_STOCKS
    return {"success": True, "data": ALL_STOCKS}


@app.post("/api/can_slim")
async def get_can_slim_analysis(request: StockRequest):
    try:
        stock_info: StockInfo = get_stock_info_by_name(request.stock_name)
        main_stock_result = await get_stock_markdown(stock_info)
        operation_advice = get_operation_advice(request.advice_type, request.holding_price)
        if operation_advice:
            main_stock_result += f"# {operation_advice}\n"
        
        # 保存结果
        save_result("can_slim", stock_info, main_stock_result)
        
        return {"success": True, "data": main_stock_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/technical")
async def get_technical_analysis(request: StockRequest):
    try:
        stock_info: StockInfo = get_stock_info_by_name(request.stock_name)
        technical_stock_result = await get_technical_indicators_prompt(stock_info)
        operation_advice = get_operation_advice(request.advice_type, request.holding_price)
        if operation_advice:
            technical_stock_result += f"# {operation_advice}\n"
        
        # 保存结果
        save_result("technical", stock_info, technical_stock_result)
        
        return {"success": True, "data": technical_stock_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def _stream_llm_analysis(
    request: StockRequest,
    llm_name: str,
    chat_stream_func: Callable,
    model: str,
    analysis_type: str
) -> AsyncIterator[str]:
    """通用的LLM流式分析函数"""
    try:
        yield f"data: {json.dumps({'stage': 'fetching', 'message': '正在获取数据'}, ensure_ascii=False)}\n\n"
        
        stock_info: StockInfo = get_stock_info_by_name(request.stock_name)
        main_stock_result = await get_stock_markdown_for_llm_analyse(stock_info
        )
        operation_advice = get_operation_advice(request.advice_type, request.holding_price)
        if operation_advice:
            main_stock_result += f"# {operation_advice}\n"
        
        yield f"data: {json.dumps({'stage': 'analyzing', 'message': f'正在调用大模型{llm_name}'}, ensure_ascii=False)}\n\n"
        
        full_result = ""
        async for content in chat_stream_func(
            messages=[{"role": "user", "content": main_stock_result}],
            model=model
        ):
            full_result += content
            yield f"data: {json.dumps({'stage': 'streaming', 'content': content}, ensure_ascii=False)}\n\n"
        
        # 保存结果
        save_result(analysis_type, stock_info, full_result)
        
        yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"


@app.post("/api/can_slim_deepseek")
async def get_can_slim_deepseek_analysis(request: StockRequest):
    client = DeepSeekClient()
    return StreamingResponse(
        _stream_llm_analysis(request, "DeepSeek", client.chat_stream, "deepseek-chat", "can_slim_deepseek"),
        media_type="text/event-stream"
    )


@app.post("/api/can_slim_gemini")
async def get_can_slim_gemini_analysis(request: StockRequest):
    client = GeminiClient()
    return StreamingResponse(
        _stream_llm_analysis(request, "Gemini", client.chat_stream, "gemini-3-pro-all", "can_slim_gemini"),
        media_type="text/event-stream"
    )


async def _stream_full_analysis(request: StockRequest, llm_type: str = "deepseek") -> AsyncIterator[str]:
    """全量分析流式响应"""
    try:
        stock_info: StockInfo = get_stock_info_by_name(request.stock_name)

        # 创建一个队列来传递进度消息
        import asyncio
        progress_queue = asyncio.Queue()
        
        async def progress_callback(stage: str, message: str, status: str = None):
            await progress_queue.put((stage, message, status))
        
        # 启动分析任务
        analysis_task = asyncio.create_task(
            stock_full_analysis(stock_info, progress_callback, llm_type)
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
            except Exception as e:
                yield f"data: {json.dumps({'stage': 'error', 'message': f'进度获取失败: {str(e)}'}, ensure_ascii=False)}\n\n"
                return
        
        # 获取分析结果
        try:
            result = await analysis_task
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'message': f'分析任务失败: {str(e)}'}, ensure_ascii=False)}\n\n"
            return
        
        # 发送剩余的进度消息
        while not progress_queue.empty():
            stage, message, status = await progress_queue.get()
            if status:
                yield f"data: {json.dumps({'stage': stage, 'message': message, 'status': status}, ensure_ascii=False)}\n\n"
            else:
                yield f"data: {json.dumps({'stage': stage, 'message': message}, ensure_ascii=False)}\n\n"
        
        # 保存结果
        save_result(f"full_analysis_{llm_type}", stock_info, result)
        
        yield f"data: {json.dumps({'stage': 'streaming', 'content': result}, ensure_ascii=False)}\n\n"
        yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
    except Exception as e:
        import traceback
        error_detail = f"{str(e)}\n{traceback.format_exc()}"
        yield f"data: {json.dumps({'stage': 'error', 'message': error_detail}, ensure_ascii=False)}\n\n"


@app.post("/api/full_analysis_deepseek")
async def get_full_analysis_deepseek(request: StockRequest):
    return StreamingResponse(
        _stream_full_analysis(request, "deepseek"),
        media_type="text/event-stream"
    )


@app.post("/api/full_analysis_gemini")
async def get_full_analysis_gemini(request: StockRequest):
    return StreamingResponse(
        _stream_full_analysis(request, "gemini"),
        media_type="text/event-stream"
    )


@app.post("/api/technical_deepseek")
async def get_technical_deepseek_analysis(request: StockRequest):
    client = DeepSeekClient()
    
    async def stream_technical():
        try:
            yield f"data: {json.dumps({'stage': 'fetching', 'message': '正在获取技术指标数据'}, ensure_ascii=False)}\n\n"
            
            stock_info: StockInfo = get_stock_info_by_name(request.stock_name)
            technical_result = await get_technical_indicators_prompt(stock_info)
            operation_advice = get_operation_advice(request.advice_type, request.holding_price)
            if operation_advice:
                technical_result += f"# {operation_advice}\n"
            
            yield f"data: {json.dumps({'stage': 'analyzing', 'message': '正在调用大模型DeepSeek'}, ensure_ascii=False)}\n\n"
            
            full_result = ""
            async for content in client.chat_stream(
                messages=[{"role": "user", "content": technical_result}],
                model="deepseek-chat"
            ):
                full_result += content
                yield f"data: {json.dumps({'stage': 'streaming', 'content': content}, ensure_ascii=False)}\n\n"
            
            save_result("technical_deepseek", stock_info, full_result)
            yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(stream_technical(), media_type="text/event-stream")


@app.post("/api/technical_gemini")
async def get_technical_gemini_analysis(request: StockRequest):
    client = GeminiClient()
    
    async def stream_technical():
        try:
            yield f"data: {json.dumps({'stage': 'fetching', 'message': '正在获取技术指标数据'}, ensure_ascii=False)}\n\n"
            
            stock_info: StockInfo = get_stock_info_by_name(request.stock_name)
            technical_result = await get_technical_indicators_prompt(stock_info)
            operation_advice = get_operation_advice(request.advice_type, request.holding_price)
            if operation_advice:
                technical_result += f"# {operation_advice}\n"
            
            yield f"data: {json.dumps({'stage': 'analyzing', 'message': '正在调用大模型Gemini'}, ensure_ascii=False)}\n\n"
            
            full_result = ""
            async for content in client.chat_stream(
                messages=[{"role": "user", "content": technical_result}],
                model="gemini-3-pro-all"
            ):
                full_result += content
                yield f"data: {json.dumps({'stage': 'streaming', 'content': content}, ensure_ascii=False)}\n\n"
            
            save_result("technical_gemini", stock_info, full_result)
            yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(stream_technical(), media_type="text/event-stream")
