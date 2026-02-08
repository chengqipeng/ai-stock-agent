from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional, AsyncIterator, Callable
import json
import os
from datetime import datetime
import glob

from common.constants.stocks_data import get_stock_code
from common.utils.amount_utils import normalize_stock_code
from service.eastmoney.stock_structure_markdown import get_stock_markdown, get_stock_markdown_for_llm_analyse
from service.eastmoney.stock_technical_markdown import get_technical_indicators_prompt
from service.processor.operation_advice import get_operation_advice
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient
from service.tests.stock_full_analysis_with_llm_search import stock_full_analysis

app = FastAPI(title="AI Stock Agent")

# 创建结果存储目录
RESULT_DIR = "api_results"
os.makedirs(RESULT_DIR, exist_ok=True)


def save_result(analysis_type: str, stock_name: str, stock_code: str, result: str):
    """保存分析结果到文件"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{RESULT_DIR}/{analysis_type}_{stock_name}_{stock_code}_{timestamp}.md"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(result)
    return filename


@app.get("/api/history")
async def get_history():
    """获取历史记录列表"""
    try:
        files = glob.glob(f"{RESULT_DIR}/*.md")
        history = []
        
        for file_path in files:
            filename = os.path.basename(file_path)
            # 解析文件名: {analysis_type}_{stock_name}_{stock_code}_{timestamp}.md
            parts = filename.replace('.md', '').split('_')
            if len(parts) >= 4:
                # 找到时间戳部分（最后两个部分）
                timestamp_str = '_'.join(parts[-2:])
                analysis_type = parts[0]
                stock_name = parts[1]
                # stock_code可能包含.SZ或.SH，所以需要合并中间部分
                stock_code = '_'.join(parts[2:-2])
                
                # 解析时间戳
                try:
                    dt = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                    sort_key = dt
                except:
                    formatted_time = timestamp_str
                    sort_key = datetime.min
                
                # 获取文件大小
                file_size = os.path.getsize(file_path)
                
                history.append({
                    "filename": filename,
                    "analysis_type": analysis_type,
                    "stock_name": stock_name,
                    "stock_code": stock_code,
                    "timestamp": formatted_time,
                    "file_size": file_size,
                    "file_path": file_path,
                    "sort_key": sort_key
                })
        
        # 按时间戳倒序排序
        history.sort(key=lambda x: x['sort_key'], reverse=True)
        
        # 移除sort_key字段
        for item in history:
            del item['sort_key']
        
        return {"success": True, "data": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history/{filename}")
async def get_history_content(filename: str):
    """获取历史记录内容"""
    try:
        file_path = f"{RESULT_DIR}/{filename}"
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="文件不存在")
        
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        
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
        
        # 保存结果
        save_result("can_slim", request.stock_name, stock_code, main_stock_result)
        
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
        
        # 保存结果
        save_result("technical", request.stock_name, stock_code, technical_stock_result)
        
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
        
        stock_code = get_stock_code(request.stock_name)
        main_stock_result = await get_stock_markdown_for_llm_analyse(
            normalize_stock_code(stock_code), request.stock_name
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
        save_result(analysis_type, request.stock_name, stock_code, full_result)
        
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
        stock_code = get_stock_code(request.stock_name)
        normalized_code = normalize_stock_code(stock_code)
        
        # 创建一个队列来传递进度消息
        import asyncio
        progress_queue = asyncio.Queue()
        
        async def progress_callback(stage: str, message: str, status: str = None):
            await progress_queue.put((stage, message, status))
        
        # 启动分析任务
        analysis_task = asyncio.create_task(
            stock_full_analysis(normalized_code, request.stock_name, progress_callback, llm_type)
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
        
        # 保存结果
        save_result("full_analysis", request.stock_name, stock_code, result)
        
        yield f"data: {json.dumps({'stage': 'streaming', 'content': result}, ensure_ascii=False)}\n\n"
        yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"


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
            
            stock_code = get_stock_code(request.stock_name)
            technical_result = await get_technical_indicators_prompt(
                normalize_stock_code(stock_code), stock_code, request.stock_name
            )
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
            
            save_result("technical_deepseek", request.stock_name, stock_code, full_result)
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
            
            stock_code = get_stock_code(request.stock_name)
            technical_result = await get_technical_indicators_prompt(
                normalize_stock_code(stock_code), stock_code, request.stock_name
            )
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
            
            save_result("technical_gemini", request.stock_name, stock_code, full_result)
            yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(stream_technical(), media_type="text/event-stream")

