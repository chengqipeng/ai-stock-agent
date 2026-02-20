from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel
from typing import List, Optional
import json
import asyncio
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

from common.utils.stock_list_parser import parse_stock_list
from common.utils.stock_info_utils import get_stock_info_by_name
from service.can_slim.can_slim_service import execute_can_slim_score
from database.models import db_manager

app = FastAPI(title="AI Stock Agent")

class BatchRequest(BaseModel):
    stock_codes: List[str]

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

@app.post("/api/batch_analysis")
async def create_batch_analysis(request: BatchRequest):
    """创建批量分析批次"""
    try:
        batch_id = db_manager.create_batch(request.stock_codes)
        return {"success": True, "data": {"batch_id": batch_id}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/batch_execute/{batch_id}")
async def execute_batch_analysis(batch_id: int, deep_thinking: bool = Query(False)):
    """执行批量分析（SSE流式响应）"""
    async def generate_progress():
        try:
            stocks = db_manager.get_batch_stocks(batch_id)
            total = len(stocks)
            completed = 0
            
            yield f"data: {json.dumps({'stage': 'start', 'completed': completed, 'total': total})}\n\n"
            
            semaphore = asyncio.Semaphore(5)
            
            async def analyze_stock(stock):
                async with semaphore:
                    try:
                        stock_info = get_stock_info_by_name(stock['stock_name'])
                        
                        # 获取打分提示词
                        from service.can_slim.can_slim_service import CAN_SLIM_SERVICES
                        c_service = CAN_SLIM_SERVICES['C'](stock_info)
                        a_service = CAN_SLIM_SERVICES['A'](stock_info)
                        
                        # 收集数据并构建提示词
                        c_service.data_cache = await c_service.collect_data()
                        await c_service.process_data()
                        c_score_prompt = c_service.build_prompt(use_score_output=True)
                        
                        a_service.data_cache = await a_service.collect_data()
                        await a_service.process_data()
                        a_score_prompt = a_service.build_prompt(use_score_output=True)
                        
                        # 执行打分
                        c_result = await execute_can_slim_score('C', stock_info, deep_thinking)
                        a_result = await execute_can_slim_score('A', stock_info, deep_thinking)

                        c_score = extract_score_from_result(c_result)
                        a_score = extract_score_from_result(a_result)
                        
                        db_manager.update_stock_dimension_score(stock['id'], 'c', c_score, c_result, None, c_score_prompt)
                        db_manager.update_stock_dimension_score(stock['id'], 'a', a_score, a_result, None, a_score_prompt)
                        db_manager.update_stock_status(stock['id'], 'completed', None, deep_thinking)
                        
                        return {
                            'success': True,
                            'stock_name': stock['stock_name'],
                            'score': f'C:{c_score}, A:{a_score}'
                        }
                    except Exception as e:
                        logger.error(f"Error analyzing {stock['stock_name']}: {e}", exc_info=True)
                        db_manager.update_stock_status(stock['id'], 'failed', str(e), deep_thinking)
                        return {
                            'success': False,
                            'stock_name': stock['stock_name'],
                            'error': str(e)
                        }
            
            tasks = [analyze_stock(stock) for stock in stocks]
            
            for task in asyncio.as_completed(tasks):
                result = await task
                completed += 1
                db_manager.update_batch_progress(batch_id)
                
                if result['success']:
                    data = json.dumps({
                        'stage': 'progress',
                        'completed': completed,
                        'total': total,
                        'stock_name': result['stock_name'],
                        'score': result['score']
                    })
                    yield f"data: {data}\n\n"
                else:
                    data = json.dumps({
                        'stage': 'progress',
                        'completed': completed,
                        'total': total,
                        'stock_name': result['stock_name'],
                        'error': result['error']
                    })
                    yield f"data: {data}\n\n"
            
            yield f"data: {json.dumps({'stage': 'done', 'completed': total, 'total': total})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(generate_progress(), media_type="text/event-stream")

@app.get("/api/batches")
async def get_batches():
    """获取所有批次列表"""
    try:
        batches = db_manager.get_batches()
        return {"success": True, "data": batches}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/batch/{batch_id}/stocks")
async def get_batch_stocks(batch_id: int):
    """获取批次中的股票列表（不含提示词字段）"""
    try:
        stocks = db_manager.get_batch_stocks(batch_id)
        exclude_fields = {
            f'{dim}{suffix}'
            for dim in ['c', 'a', 'n', 's', 'l', 'i', 'm', 'kline']
            for suffix in ['_prompt', '_score_prompt', '_summary', '_deep_prompt', '_deep_score_prompt', '_deep_summary']
        } | {'overall_prompt'}
        for stock in stocks:
            scores = [stock.get(f'{dim}_score') for dim in ['c', 'a', 'n', 's', 'l', 'i', 'm'] if stock.get(f'{dim}_score')]
            stock['score'] = round(sum(scores) / len(scores)) if scores else None
            stock['technical_score'] = stock.get('kline_score')
            stock['has_overall'] = bool(stock.get('overall_analysis'))
            for f in exclude_fields:
                stock.pop(f, None)
            stock.pop('overall_analysis', None)
        return {"success": True, "data": stocks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/batch/stock/{stock_id}/prompt")
async def get_stock_prompt(stock_id: int, dim: str, type: str = "score"):
    """按需获取股票某维度的提示词"""
    try:
        stock = db_manager.get_stock_detail(stock_id)
        if not stock:
            raise HTTPException(status_code=404, detail="股票记录不存在")
        field_map = {
            'score': f'{dim}_score_prompt',
            'deep': f'{dim}_deep_score_prompt',
            'prompt': f'{dim}_prompt',
            'deep_prompt': f'{dim}_deep_prompt',
            'summary': f'{dim}_summary',
            'deep_summary': f'{dim}_deep_summary',
            'overall_prompt': 'overall_prompt',
            'overall_result': 'overall_analysis',
        }
        field = field_map.get(type, f'{dim}_score_prompt')
        return {"success": True, "data": stock.get(field)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/batch/stock/{stock_id}")
async def get_stock_detail(stock_id: int):
    """获取股票详细信息"""
    try:
        stock = db_manager.get_stock_detail(stock_id)
        if not stock:
            raise HTTPException(status_code=404, detail="股票记录不存在")
        return {"success": True, "data": stock}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stock/history/{stock_name}")
async def get_stock_history(stock_name: str):
    """获取股票历史深度分析记录"""
    try:
        records = db_manager.get_stock_dim_analysis_history(stock_name)
        return {"success": True, "data": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/batch/{batch_id}")
async def delete_batch(batch_id: int):
    """删除批次"""
    try:
        db_manager.delete_batch(batch_id)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/batches/clear")
async def clear_batches():
    """清空所有批次"""
    try:
        db_manager.clear_all_batches()
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/stock/deep_analysis")
async def execute_deep_analysis(stock_ids: List[int], deep_thinking: bool = Query(True)):
    """执行股票深度分析（SSE流式，支持多股票并行，最多3个股票同时执行）"""
    from service.can_slim.can_slim_service import CAN_SLIM_SERVICES

    async def generate():
        stock_names = {}
        for sid in stock_ids:
            s = db_manager.get_stock_detail(sid)
            stock_names[sid] = s['stock_name'] if s else str(sid)

        total_dims = len(stock_ids) * 8
        completed_dims = 0
        dim_progress = {sid: {} for sid in stock_ids}  # {stock_id: {dim: 'pending'|'done'|'error'}}

        def progress_event(extra=None):
            payload = {
                'completed_dims': completed_dims,
                'total_dims': total_dims,
                'stocks': [
                    {
                        'stock_id': sid,
                        'stock_name': stock_names[sid],
                        'dims': dim_progress[sid]
                    } for sid in stock_ids
                ]
            }
            if extra:
                payload.update(extra)
            return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        yield progress_event()

        async def analyze_stock(stock_id: int):
            nonlocal completed_dims
            stock = db_manager.get_stock_detail(stock_id)
            if not stock:
                return
            stock_info = get_stock_info_by_name(stock['stock_name'])
            dimensions = ['C', 'A', 'N', 'S', 'L', 'I', 'M']
            dim_results = {}

            async def analyze_dim(dim: str):
                nonlocal completed_dims
                dim_progress[stock_id][dim] = 'running'
                try:
                    from service.llm.deepseek_client import DeepSeekClient
                    service = CAN_SLIM_SERVICES[dim](stock_info)
                    service.data_cache = await service.collect_data()
                    await service.process_data()
                    score_prompt = service.build_prompt(use_score_output=False)
                    model = "deepseek-reasoner" if deep_thinking else "deepseek-chat"
                    result = ""
                    async for content in DeepSeekClient().chat_stream(
                        messages=[{"role": "user", "content": score_prompt}], model=model
                    ):
                        result += content
                    score = extract_score_from_result(result)
                    summary = extract_summary_from_result(result)
                    db_manager.update_stock_dimension_deep_analysis(stock_id, dim.lower(), score, result, summary, score_prompt)
                    dim_results[dim.lower()] = {'score': score, 'result': result, 'summary': summary}
                    db_manager.add_dim_analysis_history(
                        batch_id=stock['batch_id'], stock_id=stock_id,
                        stock_name=stock['stock_name'], stock_code=stock['stock_code'],
                        dimension=dim, is_deep_thinking=deep_thinking,
                        score=score, result=result, summary=summary, status='done'
                    )
                    dim_progress[stock_id][dim] = 'done'
                    return f"{dim}维度: {score}分 - {summary}"
                except Exception as e:
                    logger.error(f"Deep analysis failed for {stock['stock_name']} dim {dim}: {e}", exc_info=True)
                    db_manager.add_dim_analysis_history(
                        batch_id=stock['batch_id'], stock_id=stock_id,
                        stock_name=stock['stock_name'], stock_code=stock['stock_code'],
                        dimension=dim, is_deep_thinking=deep_thinking,
                        status='error', error_message=str(e)
                    )
                    dim_progress[stock_id][dim] = 'error'
                    raise e
                finally:
                    completed_dims += 1

            dim_semaphore = asyncio.Semaphore(8)
            async def analyze_dim_limited(dim):
                async with dim_semaphore:
                    return await analyze_dim(dim)

            dim_progress[stock_id]['overall'] = 'running'
            results = await asyncio.gather(*[analyze_dim_limited(d) for d in dimensions], return_exceptions=True)
            all_analysis_result = "\n".join(r for r in results if isinstance(r, str))

            from service.can_slim.can_slim_service import execute_overall_analysis
            overall_prompt, overall_result = await execute_overall_analysis(stock_info, all_analysis_result, deep_thinking)

            db_manager.update_stock_overall_analysis(stock_id, overall_result, overall_prompt)
            db_manager.update_stock_status(stock_id, 'completed', None, deep_thinking)
            db_manager.add_deep_analysis_history(
                batch_id=stock['batch_id'], stock_id=stock_id,
                stock_name=stock['stock_name'], stock_code=stock['stock_code'],
                is_deep_thinking=deep_thinking, dim_results=dim_results,
                overall_analysis=overall_result, overall_prompt=overall_prompt
            )
            completed_dims += 1
            dim_progress[stock_id]['overall'] = 'done'

        queue = asyncio.Queue()
        stock_semaphore = asyncio.Semaphore(3)
        async def run_limited(sid):
            async with stock_semaphore:
                await analyze_stock(sid)
                await queue.put(sid)

        analysis_task = asyncio.ensure_future(
            asyncio.gather(*[run_limited(sid) for sid in stock_ids])
        )

        finished = 0
        while finished < len(stock_ids):
            try:
                await asyncio.wait_for(queue.get(), timeout=2.0)
                finished += 1
            except asyncio.TimeoutError:
                pass
            yield progress_event()

        await analysis_task
        yield progress_event({'stage': 'done'})

    return StreamingResponse(generate(), media_type="text/event-stream")

def extract_score_from_result(result: str) -> float:
    """从分析结果中提取分数"""
    try:
        clean_result = result.strip()
        if clean_result.startswith('```'):
            clean_result = re.sub(r'^```(?:json)?\s*\n', '', clean_result)
            clean_result = re.sub(r'\n```\s*$', '', clean_result)
        
        clean_result = clean_result.strip()
        
        if clean_result.startswith('{'):
            data = json.loads(clean_result)
            score = data.get('score', 0)
            if score:
                return round(float(score), 2)
            return 0.0
        
        score_match = re.search(r'分数[：:](\d+\.?\d*)', result)
        if score_match:
            return round(float(score_match.group(1)), 2)
        
        score_match = re.search(r'(\d+\.?\d*)分', result)
        if score_match:
            return round(float(score_match.group(1)), 2)
        
        return 0.0
    except Exception as e:
        print(f"Error extracting score: {e}, result: {result[:100]}")
        return 0.0

def extract_summary_from_result(result: str) -> str:
    """从分析结果中提取总结"""
    try:
        # 移除markdown代码块标记
        clean_result = result.strip()
        if clean_result.startswith('```'):
            clean_result = re.sub(r'^```(?:json)?\s*\n', '', clean_result)
            clean_result = re.sub(r'\n```\s*$', '', clean_result)
        
        clean_result = clean_result.strip()
        
        if clean_result.startswith('{'):
            data = json.loads(clean_result)
            return data.get('content', data.get('summary', data.get('analysis', '')))
        return result[:200] + '...' if len(result) > 200 else result
    except Exception as e:
        print(f"Error extracting summary: {e}, result: {result[:100]}")
        return result[:200] + '...' if len(result) > 200 else result
