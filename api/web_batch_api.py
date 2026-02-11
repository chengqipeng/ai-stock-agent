from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List
import json
import asyncio
import re
from datetime import datetime

from common.utils.amount_utils import normalize_stock_code
from common.utils.stock_list_parser import parse_stock_list
from data_results.database.batch_db import (
    create_batch, add_batch_stock, update_batch_stock,
    get_all_batches, get_batch_stocks, get_batch_stock_detail, clear_all_batches, delete_batch_by_id
)
from service.eastmoney.stock_structure_markdown import get_stock_markdown, get_stock_markdown_for_score
from service.eastmoney.stock_technical_markdown import get_technical_indicators_prompt_score
from service.llm.deepseek_client import DeepSeekClient

router = APIRouter()

class BatchRequest(BaseModel):
    stock_codes: List[str]


@router.get("/api/stock_list")
async def get_stock_list():
    """获取待分析股票列表"""
    try:
        file_path = "data_results/stock_to_score_list/stock_score_list.md"
        stocks = parse_stock_list(file_path)
        return {"success": True, "data": stocks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/batch_analysis")
async def batch_analysis(request: BatchRequest):
    """批量分析股票"""
    try:
        stock_count = len(request.stock_codes)
        batch_name = f"deepseek_{stock_count}_{datetime.now().strftime('%Y%m%d%H%M')}"
        batch_id = create_batch(batch_name, stock_count)
        
        for stock_code in request.stock_codes:
            stock_name = stock_code.split('(')[0].strip() if '(' in stock_code else stock_code
            code = stock_code.split('(')[1].split(')')[0] if '(' in stock_code else stock_code
            add_batch_stock(batch_id, stock_name, code)
        
        return {"success": True, "data": {"batch_id": batch_id, "batch_name": batch_name}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/batch_execute/{batch_id}")
async def batch_execute(batch_id: int, deep_thinking: bool = False):
    """执行批量分析（SSE流式返回进度）"""
    async def execute_stream():
        try:
            stocks = get_batch_stocks(batch_id)
            if not stocks:
                yield f"data: {json.dumps({'stage': 'error', 'message': '批次不存在或无股票'}, ensure_ascii=False)}\n\n"
                return
            
            pending_stocks = [s for s in stocks if s['status'] == 'pending']
            completed_count = len(stocks) - len(pending_stocks)
            
            if not pending_stocks:
                yield f"data: {json.dumps({'stage': 'done', 'message': '所有股票已完成分析'}, ensure_ascii=False)}\n\n"
                return
            
            yield f"data: {json.dumps({'stage': 'start', 'total': len(stocks), 'completed': completed_count}, ensure_ascii=False)}\n\n"
            
            semaphore = asyncio.Semaphore(5)
            completed = completed_count
            model = "deepseek-reasoner" if deep_thinking else "deepseek-chat"
            
            async def analyze_stock(stock):
                nonlocal completed
                async with semaphore:
                    try:
                        stock_name = stock['stock_name']
                        stock_code = stock['stock_code']
                        normalized_code = normalize_stock_code(stock_code)
                        
                        full_prompt = await get_stock_markdown(normalized_code, stock_name)
                        prompt = await get_stock_markdown_for_score(normalized_code, stock_name)
                        
                        client = DeepSeekClient()
                        result = ""
                        async for content in client.chat_stream(
                            messages=[{"role": "user", "content": prompt}],
                            model=model
                        ):
                            result += content
                        
                        score, reason = extract_score_and_reason(result)
                        
                        technical_prompt = await get_technical_indicators_prompt_score(
                            normalized_code, stock_code, stock_name
                        )
                        
                        technical_result = ""
                        async for technical_content in client.chat_stream(
                            messages=[{"role": "user", "content": technical_prompt}],
                            model=model
                        ):
                            technical_result += technical_content
                        
                        technical_score, technical_reason = extract_score_and_reason(technical_result)
                        
                        update_batch_stock(batch_id, stock_code, prompt, result, score, reason, 
                                         technical_prompt, technical_result, technical_score, 
                                         technical_reason, "", 1 if deep_thinking else 0, full_prompt)
                        
                        completed += 1
                        return {'stage': 'progress', 'completed': completed, 'total': len(stocks), 
                               'stock_name': stock_name, 'score': score}
                    except Exception as e:
                        error_msg = str(e)
                        print(f"[错误] {stock_name} ({stock_code}): {error_msg}")
                        import traceback
                        traceback.print_exc()
                        update_batch_stock(batch_id, stock_code, "", "", 0, "", "", "", 0, "", 
                                         error_msg, 1 if deep_thinking else 0, "")
                        completed += 1
                        return {'stage': 'progress', 'completed': completed, 'total': len(stocks), 
                               'stock_name': stock_name, 'error': error_msg}
            
            tasks = [analyze_stock(stock) for stock in pending_stocks]
            
            for coro in asyncio.as_completed(tasks):
                result = await coro
                yield f"data: {json.dumps(result, ensure_ascii=False)}\n\n"
            
            yield f"data: {json.dumps({'stage': 'done'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(execute_stream(), media_type="text/event-stream")


@router.get("/api/batches")
async def get_batches():
    """获取所有批次列表"""
    try:
        batches = get_all_batches()
        return {"success": True, "data": batches}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/batch/{batch_id}/stocks")
async def get_batch_stock_list(batch_id: int):
    """获取批次下的股票列表（按分数倒序）"""
    try:
        stocks = get_batch_stocks(batch_id)
        return {"success": True, "data": stocks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/batch/stock/{stock_id}")
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


@router.delete("/api/batches/clear")
async def clear_batches():
    """清空所有批次记录"""
    try:
        clear_all_batches()
        return {"success": True, "message": "已清空所有批次记录"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/batch/{batch_id}")
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
    
    try:
        decoded_text = html.unescape(text)
        decoded_text = re.sub(r'```json\s*', '', decoded_text)
        decoded_text = re.sub(r'```\s*', '', decoded_text)
        json_match = re.search(r'\{[^}]*"score"[^}]*\}', decoded_text)
        if json_match:
            data = json.loads(json_match.group())
            if 'score' in data:
                score = int(data['score'])
    except:
        pass
    
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
    
    try:
        reason_match = re.search(r'<think>(.*?)</think>', text, re.DOTALL)
        if reason_match:
            reason = reason_match.group(1).strip()
        else:
            reason_json = re.search(r'"reasoning_content"\s*:\s*"([^"]+)"', text)
            if reason_json:
                reason = reason_json.group(1)
    except:
        pass
    
    return score, reason
