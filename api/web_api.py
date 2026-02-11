from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional, AsyncIterator, Callable, List
import json
import os
from datetime import datetime
import glob
import asyncio
import re

from common.constants.stocks_data import get_stock_code
from common.utils.amount_utils import normalize_stock_code
from common.utils.stock_list_parser import parse_stock_list
from data_results.database.history_db import init_db, insert_history, get_all_history, get_history_content
from data_results.database.batch_db import (
    init_batch_tables, create_batch, add_batch_stock, update_batch_stock,
    get_all_batches, get_batch_stocks, get_batch_stock_detail, get_batch_progress, clear_all_batches, delete_batch_by_id
)
from service.eastmoney.stock_structure_markdown import get_stock_markdown, get_stock_markdown_for_llm_analyse, \
    get_stock_markdown_for_score
from service.eastmoney.stock_technical_markdown import get_technical_indicators_prompt, \
    get_technical_indicators_prompt_score
from service.processor.operation_advice import get_operation_advice
from service.llm.deepseek_client import DeepSeekClient
from service.llm.gemini_client import GeminiClient
from service.tests.stock_full_analysis_with_llm_search import stock_full_analysis

app = FastAPI(title="AI Stock Agent")

# 初始化数据库
init_db()
init_batch_tables()

def save_result(analysis_type: str, stock_name: str, stock_code: str, result: str):
    """保存分析结果到数据库"""
    formatted_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    insert_history(analysis_type, stock_name, stock_code, formatted_time, result)


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

class BatchRequest(BaseModel):
    stock_codes: List[str]


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
        save_result(f"full_analysis_{llm_type}", request.stock_name, stock_code, result)
        
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



@app.get("/api/stock_list")
async def get_stock_list():
    """获取待分析股票列表"""
    try:
        file_path = "data_results/stock_to_score_list/stock_score_list.md"
        stocks = parse_stock_list(file_path)
        return {"success": True, "data": stocks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/batch_analysis")
async def batch_analysis(request: BatchRequest):
    """批量分析股票"""
    try:
        # 创建批次名称
        stock_count = len(request.stock_codes)
        batch_name = f"deepseek_{stock_count}_{datetime.now().strftime('%Y%m%d%H%M')}"
        
        # 创建批次记录
        batch_id = create_batch(batch_name, stock_count)
        
        # 添加股票记录
        for stock_code in request.stock_codes:
            # 从stock_code中提取股票名称
            stock_name = stock_code.split('(')[0].strip() if '(' in stock_code else stock_code
            code = stock_code.split('(')[1].split(')')[0] if '(' in stock_code else stock_code
            add_batch_stock(batch_id, stock_name, code)
        
        return {"success": True, "data": {"batch_id": batch_id, "batch_name": batch_name}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/batch_execute/{batch_id}")
async def batch_execute(batch_id: int, deep_thinking: bool = False):
    """执行批量分析（SSE流式返回进度）"""
    async def execute_stream():
        try:
            # 获取批次下的所有股票
            stocks = get_batch_stocks(batch_id)
            if not stocks:
                yield f"data: {json.dumps({'stage': 'error', 'message': '批次不存在或无股票'}, ensure_ascii=False)}\n\n"
                return
            
            # 过滤出未完成的股票（status='pending'）
            pending_stocks = [s for s in stocks if s['status'] == 'pending']
            completed_count = len(stocks) - len(pending_stocks)
            
            if not pending_stocks:
                yield f"data: {json.dumps({'stage': 'done', 'message': '所有股票已完成分析'}, ensure_ascii=False)}\n\n"
                return
            
            # 发送初始状态
            yield f"data: {json.dumps({'stage': 'start', 'total': len(stocks), 'completed': completed_count}, ensure_ascii=False)}\n\n"
            
            # 创建信号量限制并发数为5
            semaphore = asyncio.Semaphore(5)
            completed = completed_count
            
            # 根据模式选择模型
            model = "deepseek-reasoner" if deep_thinking else "deepseek-chat"
            
            async def analyze_stock(stock):
                nonlocal completed
                async with semaphore:
                    try:
                        stock_name = stock['stock_name']
                        stock_code = stock['stock_code']
                        
                        # 获取完整提示词
                        normalized_code = normalize_stock_code(stock_code)
                        full_prompt = await get_stock_markdown(normalized_code, stock_name)

                        # 获取评分提示词
                        prompt = await get_stock_markdown_for_score(normalized_code, stock_name)
                        
                        # 调用DeepSeek分析
                        client = DeepSeekClient()
                        result = ""
                        async for content in client.chat_stream(
                            messages=[{"role": "user", "content": prompt}],
                            model=model
                        ):
                            result += content
                        
                        # 从结果中提取分数和推理过程
                        score, reason = extract_score_and_reason(result)

                        # 获取K线技术分析
                        technical_prompt = await get_technical_indicators_prompt_score(
                            normalized_code, stock_code, stock_name
                        )

                        technical_result = ""
                        async for technical_content in client.chat_stream(
                                messages=[{"role": "user", "content": technical_prompt}],
                                model=model
                        ):
                            technical_result += technical_content

                        # 从结果中提取K线分数和推理过程
                        technical_score, technical_reason = extract_score_and_reason(technical_result)
                        
                        # 更新数据库
                        update_batch_stock(batch_id, stock_code, prompt, result, score, reason, technical_prompt, technical_result, technical_score, technical_reason, "", 1 if deep_thinking else 0, full_prompt)
                        
                        completed += 1
                        return {'stage': 'progress', 'completed': completed, 'total': len(stocks), 'stock_name': stock_name, 'score': score}
                    except Exception as e:
                        # 记录错误信息到数据库
                        error_msg = str(e)
                        print(f"[错误] {stock_name} ({stock_code}): {error_msg}")
                        import traceback
                        traceback.print_exc()
                        update_batch_stock(batch_id, stock_code, "", "", 0, "", "", "", 0, "", error_msg, 1 if deep_thinking else 0, "")
                        completed += 1
                        return {'stage': 'progress', 'completed': completed, 'total': len(stocks), 'stock_name': stock_name, 'error': error_msg}
            
            # 并发执行所有未完成的股票分析
            tasks = [analyze_stock(stock) for stock in pending_stocks]
            
            # 使用as_completed处理完成的任务
            for coro in asyncio.as_completed(tasks):
                result = await coro
                yield f"data: {json.dumps(result, ensure_ascii=False)}\n\n"
            
            yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(execute_stream(), media_type="text/event-stream")


@app.get("/api/batches")
async def get_batches():
    """获取所有批次列表"""
    try:
        batches = get_all_batches()
        return {"success": True, "data": batches}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/batch/{batch_id}/stocks")
async def get_batch_stock_list(batch_id: int):
    """获取批次下的股票列表（按分数倒序）"""
    try:
        stocks = get_batch_stocks(batch_id)
        return {"success": True, "data": stocks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/batch/stock/{stock_id}")
async def get_batch_stock_info(stock_id: int):
    """获取批次股票详细信息"""
    try:
        stock = get_batch_stock_detail(stock_id)
        if not stock:
            raise HTTPException(status_code=404, detail="记录不存在")
        return {"success": True, "data": stock}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/batches/clear")
async def clear_batches():
    """清空所有批次记录"""
    try:
        clear_all_batches()
        return {"success": True, "message": "已清空所有批次记录"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/batch/{batch_id}")
async def delete_batch(batch_id: int):
    """删除单个批次记录"""
    try:
        delete_batch_by_id(batch_id)
        return {"success": True, "message": "已删除批次记录"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def extract_score_and_reason(text: str) -> tuple[int, str]:
    """从分析结果中提取分数和推理过程"""
    import html
    
    reason = ""
    score = 0
    
    # 先尝试解析JSON格式
    try:
        # 处理HTML转义
        decoded_text = html.unescape(text)
        # 移除```json标记
        decoded_text = re.sub(r'```json\s*', '', decoded_text)
        decoded_text = re.sub(r'```\s*', '', decoded_text)
        # 尝试提取JSON部分
        json_match = re.search(r'\{[^}]*"score"[^}]*\}', decoded_text)
        if json_match:
            data = json.loads(json_match.group())
            if 'score' in data:
                score = int(data['score'])
    except:
        pass
    
    # 如果没有从 JSON 提取到分数，尝试正则匹配
    if score == 0:
        patterns = [
            r'"score"[：:]*\s*(\d+)',
            r'综合评分[：:]*\s*(\d+)',
            r'总分[：:]*\s*(\d+)',
            r'评分[：:]*\s*(\d+)',
            r'得分[：:]*\s*(\d+)',
            r'分数[：:]*\s*(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                score = int(match.group(1))
                break
    
    # 提取 reasoning_content 或 reason
    try:
        reason_match = re.search(r'<think>(.*?)</think>', text, re.DOTALL)
        if reason_match:
            reason = reason_match.group(1).strip()
        else:
            # 尝试从 JSON 中提取
            reason_json = re.search(r'"reasoning_content"\s*:\s*"([^"]+)"', text)
            if reason_json:
                reason = reason_json.group(1)
    except:
        pass
    
    return score, reason


def extract_score(text: str) -> int:
    """从分析结果中提取分数（兼容旧接口）"""
    score, _ = extract_score_and_reason(text)
    return score
