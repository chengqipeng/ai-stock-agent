from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel
from typing import List, Optional
import json
import asyncio
import re
from datetime import datetime

from common.utils.stock_list_parser import parse_stock_list
from common.utils.stock_info_utils import get_stock_info_by_name
from service.can_slim.can_slim_service import execute_can_slim_score, execute_can_slim_completion
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
                        
                        c_result = await execute_can_slim_score('C', stock_info, deep_thinking)
                        a_result = await execute_can_slim_score('A', stock_info, deep_thinking)

                        c_score = extract_score_from_result(c_result)
                        a_score = extract_score_from_result(a_result)
                        
                        db_manager.update_stock_dimension_score(stock['id'], 'c', c_score, c_result, None)
                        db_manager.update_stock_dimension_score(stock['id'], 'a', a_score, a_result, None)
                        db_manager.update_stock_status(stock['id'], 'completed', None, deep_thinking)
                        
                        return {
                            'success': True,
                            'stock_name': stock['stock_name'],
                            'score': f'C:{c_score}, A:{a_score}'
                        }
                    except Exception as e:
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
    """获取批次中的股票列表"""
    try:
        stocks = db_manager.get_batch_stocks(batch_id)
        # 计算综合分数
        for stock in stocks:
            scores = [stock.get(f'{dim}_score') for dim in ['c', 'a', 'n', 's', 'l', 'i', 'm'] if stock.get(f'{dim}_score')]
            stock['score'] = round(sum(scores) / len(scores)) if scores else None
            stock['technical_score'] = stock.get('kline_score')
        return {"success": True, "data": stocks}
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

@app.post("/api/stock/{stock_id}/deep_analysis")
async def execute_deep_analysis(stock_id: int, deep_thinking: bool = Query(False)):
    """执行股票深度分析（所有CAN SLIM维度）"""
    try:
        stock = db_manager.get_stock_detail(stock_id)
        if not stock:
            raise HTTPException(status_code=404, detail="股票记录不存在")
        
        stock_info = get_stock_info_by_name(stock['stock_name'])
        
        # 执行所有CAN SLIM维度分析
        dimensions = ['C', 'A', 'N', 'S', 'L', 'I', 'M']
        overall_analysis = []
        
        for dim in dimensions:
            result = await execute_can_slim_completion(dim, stock_info, deep_thinking)
            score = extract_score_from_result(result)
            summary = extract_summary_from_result(result)
            
            db_manager.update_stock_dimension_score(stock_id, dim.lower(), score, result, summary)
            overall_analysis.append(f"{dim}维度: {score}分 - {summary}")
        
        # 更新整体分析
        overall_text = "\n".join(overall_analysis)
        db_manager.update_stock_overall_analysis(stock_id, overall_text)
        
        # 更新状态
        db_manager.update_stock_status(stock_id, 'completed', None, deep_thinking)
        
        return {"success": True, "message": "深度分析完成"}
    except Exception as e:
        db_manager.update_stock_status(stock_id, 'failed', str(e), deep_thinking)
        raise HTTPException(status_code=500, detail=str(e))

def extract_score_from_result(result: str) -> int:
    """从分析结果中提取分数"""
    try:
        # 移除markdown代码块标记
        clean_result = result.strip()
        if clean_result.startswith('```'):
            clean_result = re.sub(r'^```(?:json)?\s*\n', '', clean_result)
            clean_result = re.sub(r'\n```\s*$', '', clean_result)
        
        clean_result = clean_result.strip()
        
        if clean_result.startswith('{'):
            data = json.loads(clean_result)
            score = data.get('score', 0)
            if score:
                return int(float(score))
            return 0
        
        score_match = re.search(r'分数[：:](\d+\.?\d*)', result)
        if score_match:
            return int(float(score_match.group(1)))
        
        score_match = re.search(r'(\d+\.?\d*)分', result)
        if score_match:
            return int(float(score_match.group(1)))
        
        return 0
    except Exception as e:
        print(f"Error extracting score: {e}, result: {result[:100]}")
        return 0

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
    except:
        return result[:200] + '...' if len(result) > 200 else result
