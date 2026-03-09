from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import List
import json
import asyncio
import re
import ast
import logging
import uuid
from datetime import datetime, date
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")


def _sanitize_json_string(s: str) -> str:
    """移除 JSON 字符串值内部的非法控制字符（U+0000-U+001F，保留常见转义）"""
    # Remove control chars that are not valid even inside JSON strings
    # Keep \n \r \t as they are common and handled by strict=False
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', s)


def _safe_loads(s: str, **kw):
    """json.loads wrapper：允许控制字符（strict=False），并预清理非法字符"""
    kw.setdefault("strict", False)
    try:
        return json.loads(s, **kw)
    except json.JSONDecodeError:
        # Fallback: strip control characters and retry
        cleaned = _sanitize_json_string(s)
        return json.loads(cleaned, **kw)


class _DateTimeEncoder(json.JSONEncoder):
    """处理 MySQL 返回的 datetime / date / timedelta 对象"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if hasattr(obj, 'total_seconds'):  # timedelta
            return str(obj)
        return super().default(obj)


class SafeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            cls=_DateTimeEncoder,
        ).encode("utf-8")

from contextlib import asynccontextmanager
from common.utils.stock_list_parser import parse_stock_list, update_stock_score
from common.utils.stock_info_utils import get_stock_info_by_name
from service.can_slim.can_slim_service import execute_can_slim_score
from service.k_strategy.stock_k_strategy_service import get_k_strategy_analysis
from service.eastmoney.stock_info.stock_day_kline_data import get_120day_high_to_latest_change
from dao.stock_can_slim_dao import db_manager
from dao.stock_technical_score_dao import get_latest_technical_scores_for_batch, get_technical_score_history, save_score_results
from service.auto_job.kline_data_scheduler import start_scheduler, get_job_status, app_ready
from service.auto_job.kline_data_scheduler import _execute_job as _kline_execute_job
from service.auto_job.week_highest_lowest_price_scheduler import start_price_scheduler, get_price_job_status
from service.auto_job.week_highest_lowest_price_scheduler import _execute_job as _price_execute_job
from service.auto_job.kline_technical_scheduler import start_score_scheduler, get_score_job_status
from service.auto_job.kline_technical_scheduler import _execute_job as _score_execute_job
from service.auto_job.kline_score_scheduler import start_kline_score_scheduler, get_kline_score_job_status
from service.auto_job.kline_score_scheduler import _execute_job as _kline_score_execute_job
from service.auto_job.db_anomalies_scheduler import start_db_check_scheduler, get_db_check_job_status
from service.auto_job.db_anomalies_scheduler import _execute_job as _db_check_execute_job
from service.batch_technical_score.batch_technical_score import analyze_stock as technical_analyze_stock

GRADE_SCORE_MAP = {
    '积极买入': 95, '逢低建仓': 75, '持股待涨': 60,
    '逢高减仓': 40, '清仓离场': 20, '保持观望': 50
}


@asynccontextmanager
async def lifespan(application: FastAPI):
    """应用生命周期：启动时注册定时调度，就绪后触发"""
    async def _boot():
        # 无论调度器是否成功，都要确保 app_ready 被触发，否则页面会卡住
        try:
            # await start_scheduler()
            logger.info("[lifespan] K线调度器已激活")
        except Exception as e:
            logger.error("[lifespan] 启动K线调度器异常: %s", e, exc_info=True)

        try:
            await start_price_scheduler()
            logger.info("[lifespan] 最高最低价调度器已激活")
        except Exception as e:
            logger.error("[lifespan] 启动最高最低价调度器异常: %s", e, exc_info=True)

        try:
            await start_score_scheduler()
            logger.info("[lifespan] 技术打分调度器已激活")
        except Exception as e:
            logger.error("[lifespan] 启动技术打分调度器异常: %s", e, exc_info=True)

        try:
            await start_kline_score_scheduler()
            logger.info("[lifespan] K线初筛调度器已激活")
        except Exception as e:
            logger.error("[lifespan] 启动K线初筛调度器异常: %s", e, exc_info=True)

        try:
            await start_db_check_scheduler()
            logger.info("[lifespan] 数据异常检测调度器已激活")
        except Exception as e:
            logger.error("[lifespan] 启动数据异常检测调度器异常: %s", e, exc_info=True)

        # 关键：app_ready 必须在 try 之外，确保一定会被 set
        app_ready.set()
        logger.info("[lifespan] 应用启动完成，就绪信号已触发")

    _boot_task = asyncio.create_task(_boot())
    yield
    _boot_task.cancel()


app = FastAPI(title="AI Stock Agent", default_response_class=SafeJSONResponse, lifespan=lifespan)

@app.get("/.well-known/appspecific/com.chrome.devtools.json")
async def chrome_devtools():
    return {}

class BatchRequest(BaseModel):
    stock_codes: List[str]

@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open("static/index.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
    })


@app.get("/scheduler_logs", response_class=HTMLResponse)
async def scheduler_logs_page():
    with open("static/scheduler_logs.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
    })


@app.get("/api/scheduler_logs")
async def get_scheduler_logs(job_name: str = None, limit: int = 50, offset: int = 0):
    from dao.scheduler_log_dao import get_logs
    try:
        rows, total = get_logs(job_name, limit, offset)
        return {"success": True, "data": rows, "total": total}
    except Exception as e:
        logger.error("查询调度日志失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/kline_job_status")
async def kline_job_status():
    """获取日线数据定时拉取任务状态"""
    return {"success": True, "data": get_job_status()}

@app.get("/api/price_job_status")
async def price_job_status():
    """获取最高最低价定时拉取任务状态"""
    return {"success": True, "data": get_price_job_status()}

@app.get("/api/score_job_status")
async def score_job_status():
    """获取技术打分定时任务状态"""
    return {"success": True, "data": get_score_job_status()}

@app.get("/api/kline_score_job_status")
async def kline_score_job_status():
    """获取K线初筛定时任务状态"""
    return {"success": True, "data": get_kline_score_job_status()}

@app.get("/api/db_check_job_status")
async def db_check_job_status():
    """获取数据异常检测定时任务状态"""
    return {"success": True, "data": get_db_check_job_status()}


@app.post("/api/trigger_kline_job")
async def trigger_kline_job():
    """手动触发日线数据拉取"""
    status = get_job_status()
    if status.get("running"):
        return {"success": False, "message": "日线数据任务正在执行中"}
    asyncio.create_task(_kline_execute_job())
    return {"success": True, "message": "日线数据任务已触发"}


@app.post("/api/trigger_price_job")
async def trigger_price_job():
    """手动触发最高最低价拉取"""
    status = get_price_job_status()
    if status.get("running"):
        return {"success": False, "message": "最高最低价任务正在执行中"}
    asyncio.create_task(_price_execute_job())
    return {"success": True, "message": "最高最低价任务已触发"}


@app.post("/api/trigger_score_job")
async def trigger_score_job():
    """手动触发技术打分"""
    status = get_score_job_status()
    if status.get("running"):
        return {"success": False, "message": "技术打分任务正在执行中"}
    asyncio.create_task(_score_execute_job())
    return {"success": True, "message": "技术打分任务已触发"}


@app.post("/api/trigger_kline_score_job")
async def trigger_kline_score_job():
    """手动触发K线初筛"""
    status = get_kline_score_job_status()
    if status.get("running"):
        return {"success": False, "message": "K线初筛任务正在执行中"}
    asyncio.create_task(_kline_score_execute_job())
    return {"success": True, "message": "K线初筛任务已触发"}


@app.post("/api/trigger_db_check_job")
async def trigger_db_check_job():
    """手动触发数据异常检测"""
    status = get_db_check_job_status()
    if status.get("running"):
        return {"success": False, "message": "数据异常检测任务正在执行中"}
    asyncio.create_task(_db_check_execute_job())
    return {"success": True, "message": "数据异常检测任务已触发"}


@app.get("/api/stock_list")
async def get_stock_list():
    """获取待分析股票列表"""
    try:
        file_path = "data_results/stock_to_score_list/stock_score_list.md"
        stocks = parse_stock_list(file_path)
        return {"success": True, "data": stocks}
    except Exception as e:
        logger.error("获取股票列表失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/prescreening_batch")
async def create_prescreening_batch(request: BatchRequest):
    """创建涨跌初筛批次，并发获取120天涨跌幅并保存"""
    try:
        batch_id = db_manager.create_batch(request.stock_codes)
        stocks = db_manager.get_batch_stocks(batch_id)

        semaphore = asyncio.Semaphore(10)

        async def fetch_and_save(stock):
            async with semaphore:
                try:
                    stock_info = get_stock_info_by_name(stock['stock_name'])
                    result = await get_120day_high_to_latest_change(stock_info)
                    db_manager.update_stock_prescreening_data(
                        stock['id'],
                        result.get('涨跌幅(%)'),
                        result.get('120天最高价'),
                        result.get('120天最高价日期'),
                        result.get('最新收盘价')
                    )
                    db_manager.update_stock_status(stock['id'], 'completed')
                    return {**stock, **result, 'success': True}
                except Exception as e:
                    logger.warning(f"初筛获取失败 {stock['stock_name']}: {e}")
                    db_manager.update_stock_status(stock['id'], 'failed', str(e))
                    return {'stock_name': stock['stock_name'], 'success': False}

        results = await asyncio.gather(*[fetch_and_save(s) for s in stocks])
        db_manager.update_batch_progress(batch_id)
        results = [r for r in results if r.get('success')]
        results.sort(key=lambda x: (x.get('涨跌幅(%)') is None, x.get('涨跌幅(%)', 0) or 0))
        return {"success": True, "data": {"batch_id": batch_id, "stocks": results}}
    except Exception as e:
        logger.error("创建涨跌初筛批次失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/batch/{batch_id}/prescreening_update")
async def update_batch_prescreening(batch_id: int, stock_ids: List[int]):
    """对已有批次中指定股票执行涨跌初筛，更新到当前记录"""
    try:
        semaphore = asyncio.Semaphore(10)

        async def fetch_and_save(stock_id):
            async with semaphore:
                try:
                    stock = db_manager.get_stock_detail(stock_id)
                    if not stock or stock['batch_id'] != batch_id:
                        return {'stock_name': str(stock_id), 'success': False}
                    stock_info = get_stock_info_by_name(stock['stock_name'])
                    result = await get_120day_high_to_latest_change(stock_info)
                    db_manager.update_stock_prescreening_data(
                        stock_id,
                        result.get('涨跌幅(%)'),
                        result.get('120天最高价'),
                        result.get('120天最高价日期'),
                        result.get('最新收盘价')
                    )
                    return {**stock, **result, 'success': True}
                except Exception as e:
                    logger.warning(f"初筛更新失败 {stock_id}: {e}")
                    return {'stock_name': str(stock_id), 'success': False}

        results = await asyncio.gather(*[fetch_and_save(sid) for sid in stock_ids])
        success_results = [r for r in results if r.get('success')]
        success_results.sort(key=lambda x: (x.get('涨跌幅(%)') is None, x.get('涨跌幅(%)', 0) or 0))
        return {"success": True, "data": {"batch_id": batch_id, "stocks": success_results, "updated_count": len(success_results)}}
    except Exception as e:
        logger.error("初筛更新批次失败 batch_id=%s: %s", batch_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/batch/{batch_id}/kline_execute_update")
async def execute_batch_kline_update(batch_id: int, stock_ids: str = Query(...), deep_thinking: bool = Query(False)):
    """对已有批次中指定股票执行K线初筛，SSE流式更新到当前记录"""
    sid_list = [int(s) for s in stock_ids.split(',') if s.strip()]

    async def generate_progress():
        try:
            total = len(sid_list)
            completed = 0
            yield f"data: {json.dumps({'stage': 'start', 'completed': completed, 'total': total})}\n\n"

            semaphore = asyncio.Semaphore(5)

            async def analyze_stock(stock_id):
                async with semaphore:
                    try:
                        stock = db_manager.get_stock_detail(stock_id)
                        if not stock or stock['batch_id'] != batch_id:
                            return {'success': False, 'stock_name': str(stock_id), 'error': 'not found'}
                        stock_info = get_stock_info_by_name(stock['stock_name'])
                        prompt, result = await get_k_strategy_analysis(stock_info)
                        not_hold_grade, not_hold_content, hold_grade, hold_content, data_issues = extract_grade_and_content(result)
                        kline_total_score = extract_kline_total_score(result)
                        next_day_pred, next_week_pred = extract_predictions(result)
                        db_manager.update_stock_dimension_score(stock_id, 'kline', not_hold_grade, not_hold_content, None, prompt)
                        if kline_total_score is not None:
                            db_manager.update_stock_kline_scores(stock_id, kline_total_score)
                        db_manager.update_stock_kline_hold(stock_id, hold_grade, hold_content, data_issues)
                        # 保存K线初筛历史记录（每次产生新记录）
                        today_str = datetime.now(_CST).strftime('%Y-%m-%d')
                        db_manager.save_kline_screening_history(
                            batch_id, stock_id, stock['stock_name'], stock.get('stock_code', ''),
                            today_str, not_hold_grade, hold_grade, kline_total_score,
                            not_hold_content, hold_content, data_issues,
                            next_day_pred, next_week_pred
                        )
                        # numeric_score = GRADE_SCORE_MAP.get(not_hold_grade)
                        # if numeric_score is not None:
                        #     update_stock_score(
                        #         "data_results/stock_to_score_list/stock_score_list.md",
                        #         stock_info.stock_name,
                        #         stock_info.stock_code_normalize,
                        #         numeric_score
                        #     )
                        return {'success': True, 'stock_name': stock['stock_name'], 'score': not_hold_grade}
                    except Exception as e:
                        logger.error(f"K线更新失败 {stock_id}: {e}", exc_info=True)
                        return {'success': False, 'stock_name': str(stock_id), 'error': str(e)}

            tasks = [analyze_stock(sid) for sid in sid_list]
            for task in asyncio.as_completed(tasks):
                result = await task
                completed += 1
                if result['success']:
                    yield f"data: {json.dumps({'stage': 'progress', 'completed': completed, 'total': total, 'stock_name': result['stock_name'], 'score': result['score']})}\n\n"
                else:
                    yield f"data: {json.dumps({'stage': 'progress', 'completed': completed, 'total': total, 'stock_name': result['stock_name'], 'error': result.get('error', '')})}\n\n"

            yield f"data: {json.dumps({'stage': 'done', 'completed': total, 'total': total})}\n\n"
        except Exception as e:
            logger.error("K线SSE流式更新异常 batch_id=%s: %s", batch_id, e, exc_info=True)
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(generate_progress(), media_type="text/event-stream")

@app.post("/api/batch_analysis")
async def create_batch_analysis(request: BatchRequest):
    """创建批量分析批次"""
    try:
        batch_id = db_manager.create_batch(request.stock_codes)
        return {"success": True, "data": {"batch_id": batch_id}}
    except Exception as e:
        logger.error("创建批量分析批次失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/batch_canslim_prescreening/{batch_id}")
async def execute_batch_canslim_prescreening(batch_id: int, deep_thinking: bool = Query(True)):
    """执行CAN SLIM C/A维度初筛（SSE流式响应，默认使用深度思考模式）"""
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
                        from service.can_slim.can_slim_service import CAN_SLIM_SERVICES
                        c_service = CAN_SLIM_SERVICES['C'](stock_info)
                        a_service = CAN_SLIM_SERVICES['A'](stock_info)
                        c_service.data_cache = await c_service.collect_data()
                        await c_service.process_data()
                        c_score_prompt = c_service.build_prompt(use_score_output=True)
                        a_service.data_cache = await a_service.collect_data()
                        await a_service.process_data()
                        a_score_prompt = a_service.build_prompt(use_score_output=True)
                        c_result = await execute_can_slim_score('C', stock_info, deep_thinking)
                        a_result = await execute_can_slim_score('A', stock_info, deep_thinking)
                        c_score = extract_score_from_result(c_result)
                        a_score = extract_score_from_result(a_result)
                        db_manager.update_stock_dimension_score(stock['id'], 'c', c_score, c_result, None, c_score_prompt)
                        db_manager.update_stock_dimension_score(stock['id'], 'a', a_score, a_result, None, a_score_prompt)
                        db_manager.update_stock_status(stock['id'], 'completed', None, deep_thinking)
                        return {'success': True, 'stock_name': stock['stock_name'], 'score': f'C:{c_score}, A:{a_score}'}
                    except Exception as e:
                        logger.error(f"CAN SLIM初筛失败 {stock['stock_name']}: {e}", exc_info=True)
                        db_manager.update_stock_status(stock['id'], 'failed', str(e), deep_thinking)
                        return {'success': False, 'stock_name': stock['stock_name'], 'error': str(e)}

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
            logger.error("CAN SLIM初筛SSE异常 batch_id=%s: %s", batch_id, e, exc_info=True)
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(generate_progress(), media_type="text/event-stream")


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

                        # 调用策略引擎分析（大模型初筛）
                        prompt, result = await get_k_strategy_analysis(stock_info)
                        not_hold_grade, not_hold_content, hold_grade, hold_content, data_issues = extract_grade_and_content(result)
                        kline_total_score = extract_kline_total_score(result)
                        next_day_pred, next_week_pred = extract_predictions(result)

                        db_manager.update_stock_dimension_score(stock['id'], 'kline', not_hold_grade, not_hold_content, None, prompt)
                        if kline_total_score is not None:
                            db_manager.update_stock_kline_scores(stock['id'], kline_total_score)
                        db_manager.update_stock_kline_hold(stock['id'], hold_grade, hold_content, data_issues)
                        db_manager.update_stock_status(stock['id'], 'completed', None, deep_thinking)
                        # 保存K线初筛历史记录（每次产生新记录）
                        today_str = datetime.now(_CST).strftime('%Y-%m-%d')
                        db_manager.save_kline_screening_history(
                            batch_id, stock['id'], stock['stock_name'], stock.get('stock_code', ''),
                            today_str, not_hold_grade, hold_grade, kline_total_score,
                            not_hold_content, hold_content, data_issues,
                            next_day_pred, next_week_pred
                        )

                        # numeric_score = GRADE_SCORE_MAP.get(not_hold_grade)
                        # if numeric_score is not None:
                        #     update_stock_score(
                        #         "data_results/stock_to_score_list/stock_score_list.md",
                        #         stock_info.stock_name,
                        #         stock_info.stock_code_normalize,
                        #         numeric_score
                        #     )

                        return {
                            'success': True,
                            'stock_name': stock['stock_name'],
                            'score': not_hold_grade
                        }
                    except Exception as e:
                        logger.error(f"Error analyzing {stock['stock_name']}: {e}", exc_info=True)
                        db_manager.update_stock_status(stock['id'], 'failed', str(e), deep_thinking)
                        return {
                            'success': False,
                            'stock_name': stock['stock_name'],
                            'error': str(e)
                        }

            # --- 历史初筛逻辑备份（CAN SLIM C/A维度打分）---
            # async def analyze_stock_legacy(stock):
            #     async with semaphore:
            #         try:
            #             stock_info = get_stock_info_by_name(stock['stock_name'])
            #             from service.can_slim.can_slim_service import CAN_SLIM_SERVICES
            #             c_service = CAN_SLIM_SERVICES['C'](stock_info)
            #             a_service = CAN_SLIM_SERVICES['A'](stock_info)
            #             c_service.data_cache = await c_service.collect_data()
            #             await c_service.process_data()
            #             c_score_prompt = c_service.build_prompt(use_score_output=True)
            #             a_service.data_cache = await a_service.collect_data()
            #             await a_service.process_data()
            #             a_score_prompt = a_service.build_prompt(use_score_output=True)
            #             c_result = await execute_can_slim_score('C', stock_info, deep_thinking)
            #             a_result = await execute_can_slim_score('A', stock_info, deep_thinking)
            #             c_score = extract_score_from_result(c_result)
            #             a_score = extract_score_from_result(a_result)
            #             db_manager.update_stock_dimension_score(stock['id'], 'c', c_score, c_result, None, c_score_prompt)
            #             db_manager.update_stock_dimension_score(stock['id'], 'a', a_score, a_result, None, a_score_prompt)
            #             db_manager.update_stock_status(stock['id'], 'completed', None, deep_thinking)
            #             return {'success': True, 'stock_name': stock['stock_name'], 'score': f'C:{c_score}, A:{a_score}'}
            #         except Exception as e:
            #             db_manager.update_stock_status(stock['id'], 'failed', str(e), deep_thinking)
            #             return {'success': False, 'stock_name': stock['stock_name'], 'error': str(e)}
            
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
            logger.error("批量分析SSE异常 batch_id=%s: %s", batch_id, e, exc_info=True)
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(generate_progress(), media_type="text/event-stream")

@app.get("/api/batches")
async def get_batches():
    """获取所有批次列表"""
    try:
        batches = db_manager.get_batches()
        return {"success": True, "data": batches}
    except Exception as e:
        logger.error("获取批次列表失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/batch/{batch_id}/stocks")
async def get_batch_stocks(batch_id: int):
    """获取批次中的股票列表（不含提示词字段）"""
    try:
        stocks = db_manager.get_batch_stocks(batch_id)
        # 获取该批次下每只股票的最新技术打分
        tech_scores = get_latest_technical_scores_for_batch(batch_id)
        # 获取该批次下每只股票的最新预测数据
        latest_predictions = db_manager.get_latest_kline_predictions_for_batch(batch_id)
        for stock in stocks:
            scores = [stock.get(f'{dim}_score') for dim in ['c', 'a', 'n', 's', 'l', 'i', 'm'] if stock.get(f'{dim}_score')]
            stock['score'] = round(sum(scores) / len(scores)) if scores else None
            stock['technical_score'] = stock.get('kline_score')
            stock['technical_hold_score'] = stock.get('kline_hold_score')
            # has_overall 已在SQL中计算
            stock['has_overall'] = bool(stock.get('has_overall'))
            # 注入最新技术打分数据
            ts = tech_scores.get(stock.get('stock_code'))
            if not ts:
                # 兼容旧数据：stock_analysis_detail中stock_code可能是名称，按stock_name再查一次
                for _ts in tech_scores.values():
                    if _ts.get('stock_name') == stock.get('stock_name'):
                        ts = _ts
                        break
            if ts:
                stock['tech_total_score'] = ts.get('total_score')
                stock['tech_macd_score'] = ts.get('macd_score')
                stock['tech_kdj_score'] = ts.get('kdj_score')
                stock['tech_vol_score'] = ts.get('vol_score')
                stock['tech_trend_score'] = ts.get('trend_score')
                stock['tech_close_price'] = ts.get('close_price')
                stock['tech_score_date'] = ts.get('score_date')
                stock['tech_boll_score'] = ts.get('boll_score')
                stock['tech_boll_signal'] = bool(ts.get('boll_signal'))
                stock['tech_mid_bounce_score'] = ts.get('mid_bounce_score')
                stock['tech_mid_bounce_signal'] = bool(ts.get('mid_bounce_signal'))
            else:
                stock['tech_total_score'] = None
            # 注入最新预测数据
            pred = latest_predictions.get(stock.get('id'))
            if pred:
                stock['next_day_prediction'] = pred.get('next_day_prediction')
                stock['next_week_prediction'] = pred.get('next_week_prediction')
            else:
                stock['next_day_prediction'] = None
                stock['next_week_prediction'] = None
        return {"success": True, "data": stocks}
    except Exception as e:
        logger.error("获取批次股票列表失败 batch_id=%s: %s", batch_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/batch/{batch_id}/add_stocks")
async def add_stocks_to_batch(batch_id: int, request: BatchRequest):
    """向已有批次中添加股票"""
    try:
        added = db_manager.add_stocks_to_batch(batch_id, request.stock_codes)
        return {"success": True, "data": {"added_count": added}}
    except Exception as e:
        logger.error("添加股票到批次失败 batch_id=%s: %s", batch_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/batch/{batch_id}/technical_score_history/{stock_code}")
async def get_stock_technical_score_history(batch_id: int, stock_code: str):
    """获取某只股票在某批次下的所有技术打分记录"""
    try:
        records = get_technical_score_history(batch_id, stock_code)
        # 兼容旧数据：stock_code可能是名称，按名称再查一次
        if not records and not any(c.isdigit() for c in stock_code):
            info = get_stock_info_by_name(stock_code)
            if info:
                records = get_technical_score_history(batch_id, info.stock_code_normalize)
        return {"success": True, "data": records}
    except Exception as e:
        logger.error("获取技术打分历史失败 batch_id=%s, stock_code=%s: %s", batch_id, stock_code, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/batch/{batch_id}/kline_screening_history/{stock_id}")
async def get_kline_screening_history(batch_id: int, stock_id: int):
    """获取某只股票在某批次下的K线初筛历史记录（按ID倒序）"""
    try:
        records = db_manager.get_kline_screening_history(batch_id, stock_id)
        return SafeJSONResponse(content={"success": True, "data": records})
    except Exception as e:
        logger.error("获取K线初筛历史失败 batch_id=%s, stock_id=%s: %s", batch_id, stock_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/kline_screening_history/{history_id}")
async def delete_kline_screening_history(history_id: int):
    """删除单条K线初筛历史记录"""
    try:
        db_manager.delete_kline_screening_history(history_id)
        return SafeJSONResponse(content={"success": True})
    except Exception as e:
        logger.error("删除K线初筛历史失败 id=%s: %s", history_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/backtest/predictions")
async def backtest_predictions(batch_id: int = Query(None), limit: int = Query(200)):
    """预测回测：对比历史预测与实际涨跌，计算准确率"""
    try:
        from service.backtest.prediction_backtest import run_backtest
        result = run_backtest(batch_id=batch_id, limit=limit)
        return SafeJSONResponse(content={"success": True, "data": result})
    except Exception as e:
        logger.error("预测回测失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/backtest/stock/{stock_code}")
async def backtest_stock_predictions(stock_code: str, batch_id: int = Query(None), limit: int = Query(50)):
    """单只股票预测回测"""
    try:
        from service.backtest.prediction_backtest import run_stock_backtest
        result = run_stock_backtest(stock_code=stock_code, batch_id=batch_id, limit=limit)
        return SafeJSONResponse(content={"success": True, "data": result})
    except Exception as e:
        logger.error("股票预测回测失败 [%s]: %s", stock_code, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/backtest/calibration")
async def get_probability_calibration(batch_id: int = Query(None)):
    """获取概率校准数据：基于回测结果校准预测概率模型"""
    try:
        from service.backtest.prediction_backtest import get_calibrated_probability_params, run_backtest
        calibrated = get_calibrated_probability_params(batch_id=batch_id)
        backtest = run_backtest(batch_id=batch_id, limit=300)
        return SafeJSONResponse(content={
            "success": True,
            "data": {
                "calibrated_params": calibrated,
                "backtest_summary": {
                    '次日预测回测': backtest.get('次日预测回测', {}),
                    '一周预测回测': backtest.get('一周预测回测', {}),
                    '按评分区间': backtest.get('按评分区间', {}),
                    '按置信度': backtest.get('按置信度', {}),
                    '校准数据': backtest.get('校准数据', {}),
                },
            }
        })
    except Exception as e:
        logger.error("概率校准失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/backtest/historical")
async def historical_backtest(
    start_date: str = Query('2024-06-01'),
    end_date: str = Query('2026-03-01'),
    max_stocks: int = Query(30),
    max_samples: int = Query(20),
    interval: int = Query(5),
):
    """基于历史K线数据的自动回测（不依赖LLM预测记录）"""
    try:
        from service.backtest.historical_backtest import run_historical_backtest
        result = run_historical_backtest(
            start_date=start_date,
            end_date=end_date,
            sample_interval=interval,
            max_stocks=max_stocks,
            max_samples_per_stock=max_samples,
        )
        return SafeJSONResponse(content={"success": True, "data": result})
    except Exception as e:
        logger.error("历史回测失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/backtest/historical_fund_flow")
async def historical_backtest_with_fund_flow(
    max_stocks: int = Query(15),
    max_samples: int = Query(6),
    interval: int = Query(3),
):
    """基于历史K线 + 同花顺真实资金流数据的增强回测"""
    try:
        from service.backtest.historical_backtest import run_historical_backtest_with_fund_flow
        result = await run_historical_backtest_with_fund_flow(
            max_stocks=max_stocks,
            sample_interval=interval,
            max_samples_per_stock=max_samples,
        )
        return SafeJSONResponse(content={"success": True, "data": result})
    except Exception as e:
        logger.error("资金流增强回测失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/backtest/full_model")
async def full_model_backtest(
    max_stocks: int = Query(50),
    concurrency: int = Query(5),
):
    """完整7维度评分模型回测（实时API数据，验证线上评分逻辑准确性）"""
    try:
        from service.backtest.full_model_backtest import run_full_model_backtest
        result = await run_full_model_backtest(
            max_stocks=max_stocks,
            concurrency=concurrency,
        )
        return SafeJSONResponse(content={"success": True, "data": result})
    except Exception as e:
        logger.error("完整模型回测失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/backtest/technical")
async def technical_backtest(
    stock_codes: str = Query("002008.SZ,300750.SZ", description="逗号分隔的股票代码"),
    start_date: str = Query("2026-01-20"),
    end_date: str = Query("2026-03-07"),
):
    """技术+资金流+盘口维度逐日回测（K线+同花顺历史资金流+K线模拟盘口）"""
    try:
        from service.backtest.technical_backtest import run_technical_backtest
        codes = [c.strip() for c in stock_codes.split(',') if c.strip()]
        result = await run_technical_backtest(
            stock_codes=codes,
            start_date=start_date,
            end_date=end_date,
        )
        return SafeJSONResponse(content={"success": True, "data": result})
    except Exception as e:
        logger.error("纯技术回测失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/api/batch/{batch_id}/technical_score_execute")
async def execute_batch_technical_score(batch_id: int, stock_ids: str = Query(...)):
    """对批次中指定股票执行技术面打分，SSE流式返回进度"""
    sid_list = [int(s) for s in stock_ids.split(',') if s.strip()]

    async def generate_progress():
        try:
            total = len(sid_list)
            completed = 0
            yield f"data: {json.dumps({'stage': 'start', 'completed': completed, 'total': total})}\n\n"

            semaphore = asyncio.Semaphore(5)
            all_results = []

            async def score_stock(stock_id):
                async with semaphore:
                    try:
                        stock = db_manager.get_stock_detail(stock_id)
                        if not stock or stock['batch_id'] != batch_id:
                            return {'success': False, 'stock_name': str(stock_id), 'error': 'not found'}
                        code = stock['stock_code']
                        # 兼容旧数据：stock_code字段可能存的是股票名称而非代码
                        if code and not any(c.isdigit() for c in code):
                            info = get_stock_info_by_name(code)
                            if info:
                                code = info.stock_code_normalize
                        r = await technical_analyze_stock(stock['stock_name'], code, 0, 0)
                        if r:
                            all_results.append(r)
                            return {'success': True, 'stock_name': stock['stock_name'], 'score': r['total']}
                        else:
                            return {'success': False, 'stock_name': stock['stock_name'], 'error': '数据不足'}
                    except Exception as e:
                        logger.error("技术打分失败 stock_id=%s: %s", stock_id, e, exc_info=True)
                        return {'success': False, 'stock_name': str(stock_id), 'error': str(e)}

            tasks = [score_stock(sid) for sid in sid_list]
            for task in asyncio.as_completed(tasks):
                result = await task
                completed += 1
                if result['success']:
                    yield f"data: {json.dumps({'stage': 'progress', 'completed': completed, 'total': total, 'stock_name': result['stock_name'], 'score': result['score']})}\n\n"
                else:
                    yield f"data: {json.dumps({'stage': 'progress', 'completed': completed, 'total': total, 'stock_name': result['stock_name'], 'error': result.get('error', '')})}\n\n"

            # 保存打分结果到数据库
            if all_results:
                save_score_results(all_results, batch_id)

            yield f"data: {json.dumps({'stage': 'done', 'completed': total, 'total': total})}\n\n"
        except Exception as e:
            logger.error("技术打分SSE异常 batch_id=%s: %s", batch_id, e, exc_info=True)
            yield f"data: {json.dumps({'stage': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(generate_progress(), media_type="text/event-stream")


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
        # kline_hold 维度映射到 kline_hold_prompt
        if dim == 'kline_hold' and type == 'prompt':
            field = 'kline_hold_prompt'
        elif type == 'data_issues':
            field = 'data_issues'
        else:
            field = field_map.get(type, f'{dim}_score_prompt')
        return {"success": True, "data": stock.get(field)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取股票提示词失败 stock_id=%s, dim=%s: %s", stock_id, dim, e, exc_info=True)
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
        logger.error("获取股票详情失败 stock_id=%s: %s", stock_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stock/finance/{stock_code:path}")
async def get_stock_finance_data(stock_code: str, limit: int = Query(None)):
    """获取股票财报数据列表，按报告期倒序"""
    try:
        from common.utils.stock_info_utils import get_stock_info_by_code
        # stock_code 可能是名称或标准化代码（如 300602.SZ），两种都尝试
        info = get_stock_info_by_name(stock_code)
        if not info:
            info = get_stock_info_by_code(stock_code)
        if not info:
            raise HTTPException(status_code=404, detail="未找到该股票信息")
        from service.jqka10.stock_finance_data_10jqka import get_financial_data_from_db
        records = get_financial_data_from_db(info, limit=limit)
        return SafeJSONResponse(content={"success": True, "data": records})
    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取财报数据失败 stock_code=%s: %s", stock_code, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/batch/{batch_id}/finance_growth")
async def get_batch_finance_growth(batch_id: int):
    """获取批次中所有股票最近3期财报增长数据（营收同比/环比、扣非同比/环比）"""
    try:
        from dao.stock_finance_dao import get_finance_from_db
        from dao.stock_technical_score_dao import get_batch_stock_list
        stocks = get_batch_stock_list(batch_id)
        result = {}
        growth_keys = [
            "营业总收入同比增长(%)",
            "营业总收入环比增长(%)",
            "扣非净利润同比增长(%)",
            "扣非净利润环比增长(%)",
        ]
        # 单季度值 key → 对应的环比 key
        # 环比正确算法：用单季度值比较，(当期单季 - 上期单季) / |上期单季| * 100
        sq_qoq_map = [
            ("单季度营业收入(元)", "营业总收入环比增长(%)"),
            ("单季扣非净利润(元)", "扣非净利润环比增长(%)"),
        ]

        def _parse_num(v):
            """将可能带单位的金额字符串转为 float，如 '12.34亿' → 1234000000"""
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).strip()
            if not s or s == 'None':
                return None
            multiplier = 1
            if s.endswith("亿"):
                multiplier = 1e8
                s = s[:-1]
            elif s.endswith("万"):
                multiplier = 1e4
                s = s[:-1]
            try:
                return float(s) * multiplier
            except (ValueError, TypeError):
                return None

        for stock in stocks:
            code = stock.get("stock_code") or ""
            name = stock.get("stock_name", "").replace(" ", "").split("(")[0]
            # 多取1条用于计算第3期的环比
            if code and any(c.isdigit() for c in code):
                records = get_finance_from_db(code, limit=4)
            else:
                info = get_stock_info_by_name(name)
                records = get_finance_from_db(info.stock_code_normalize, limit=4) if info else []

            # 补算环比：当存储的环比为空时，用相邻两期单季度值计算
            # 公式: (当期单季 - 上期单季) / |上期单季| * 100
            for i in range(len(records) - 1):
                cur, prev = records[i], records[i + 1]
                for sq_key, qoq_key in sq_qoq_map:
                    if cur.get(qoq_key) is not None:
                        continue
                    cur_val = _parse_num(cur.get(sq_key))
                    prev_val = _parse_num(prev.get(sq_key))
                    if cur_val is not None and prev_val is not None and prev_val != 0:
                        cur[qoq_key] = round((cur_val - prev_val) / abs(prev_val) * 100, 2)

            periods = []
            for rec in records[:3]:
                period = {"报告期": rec.get("报告期", "")}
                for k in growth_keys:
                    period[k] = rec.get(k)
                periods.append(period)
            result[name] = periods
        return SafeJSONResponse(content={"success": True, "data": result})
    except Exception as e:
        logger.error("获取批次财报增长数据失败 batch_id=%s: %s", batch_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/history/{stock_name}")
async def get_stock_history(stock_name: str):
    """获取股票历史深度分析记录"""
    try:
        records = db_manager.get_stock_dim_analysis_history(stock_name)
        return {"success": True, "data": records}
    except Exception as e:
        logger.error("获取股票历史记录失败 stock_name=%s: %s", stock_name, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/stock/history/{stock_name}")
async def clear_stock_history(stock_name: str):
    """清空股票历史执行记录"""
    try:
        db_manager.clear_stock_dim_analysis_history(stock_name)
        return {"success": True}
    except Exception as e:
        logger.error("清空股票历史记录失败 stock_name=%s: %s", stock_name, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/batch/{batch_id}/rename")
async def rename_batch(batch_id: int, body: dict):
    """重命名批次"""
    new_name = (body.get("name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="名称不能为空")
    ok = db_manager.rename_batch(batch_id, new_name)
    if not ok:
        raise HTTPException(status_code=409, detail="批次名称已存在")
    return {"success": True}

@app.patch("/api/batch/{batch_id}/pin")
async def pin_batch(batch_id: int):
    """切换批次置顶状态"""
    try:
        is_pinned = db_manager.toggle_pin_batch(batch_id)
        return {"success": True, "is_pinned": is_pinned}
    except Exception as e:
        logger.error("切换批次置顶状态失败 batch_id=%s: %s", batch_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/batch/{batch_id}/sort_order")
async def update_batch_sort_order(batch_id: int, request: dict):
    """修改批次序号"""
    try:
        sort_order = request.get("sort_order")
        if sort_order is None:
            raise HTTPException(status_code=400, detail="sort_order is required")
        ok = db_manager.update_batch_sort_order(batch_id, int(sort_order))
        return {"success": ok}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("修改批次序号失败 batch_id=%s: %s", batch_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/batch/{batch_id}/continuous_analysis")
async def toggle_continuous_analysis(batch_id: int):
    """切换批次持续分析标记"""
    try:
        is_continuous = db_manager.toggle_continuous_analysis(batch_id)
        return {"success": True, "is_continuous_analysis": is_continuous}
    except Exception as e:
        logger.error("切换持续分析状态失败 batch_id=%s: %s", batch_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/batch/{batch_id}")
async def delete_batch(batch_id: int):
    """删除批次"""
    try:
        db_manager.delete_batch(batch_id)
        return {"success": True}
    except Exception as e:
        logger.error("删除批次失败 batch_id=%s: %s", batch_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/batches/clear")
async def clear_batches():
    """清空所有批次"""
    try:
        db_manager.clear_all_batches()
        return {"success": True}
    except Exception as e:
        logger.error("清空所有批次失败: %s", e, exc_info=True)
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
            completed_stocks = sum(
                1 for sid in stock_ids if dim_progress[sid].get('overall') == 'done'
            )
            payload = {
                'completed_dims': completed_dims,
                'total_dims': total_dims,
                'completed_stocks': completed_stocks,
                'total_stocks': len(stock_ids),
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
            execution_id = str(uuid.uuid4())

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
                        dimension=dim, is_deep_thinking=deep_thinking, execution_id=execution_id,
                        score=score, result=result, summary=summary, status='done'
                    )
                    dim_progress[stock_id][dim] = 'done'
                    return f"{dim}维度: {score}分 - {summary}"
                except Exception as e:
                    logger.error(f"Deep analysis failed for {stock['stock_name']} dim {dim}: {e}", exc_info=True)
                    db_manager.add_dim_analysis_history(
                        batch_id=stock['batch_id'], stock_id=stock_id,
                        stock_name=stock['stock_name'], stock_code=stock['stock_code'],
                        dimension=dim, is_deep_thinking=deep_thinking, execution_id=execution_id,
                        status='error', error_message=str(e)
                    )
                    dim_progress[stock_id][dim] = 'error'
                    raise e
                finally:
                    completed_dims += 1

            dim_semaphore = asyncio.Semaphore(7)
            async def analyze_dim_limited(dim):
                async with dim_semaphore:
                    return await analyze_dim(dim)

            dim_progress[stock_id]['overall'] = 'running'
            results = await asyncio.gather(*[analyze_dim_limited(d) for d in dimensions], return_exceptions=True)
            all_analysis_result = "\n".join(r for r in results if isinstance(r, str))

            try:
                # from service.can_slim.can_slim_service import execute_overall_analysis
                # overall_prompt, overall_result = await execute_overall_analysis(stock_info, all_analysis_result, deep_thinking)
                #
                # overall_grade = extract_grade_from_overall(overall_result)
                # db_manager.update_stock_overall_analysis(stock_id, overall_result, overall_prompt, overall_grade)
                # db_manager.update_stock_status(stock_id, 'completed', None, deep_thinking)
                # db_manager.add_deep_analysis_history(
                #     batch_id=stock['batch_id'], stock_id=stock_id,
                #     stock_name=stock['stock_name'], stock_code=stock['stock_code'],
                #     is_deep_thinking=deep_thinking, dim_results=dim_results,
                #     overall_analysis=overall_result, overall_prompt=overall_prompt
                # )
                # db_manager.update_dim_history_overall_grade(execution_id, overall_grade)
                dim_progress[stock_id]['overall'] = 'done'
            except Exception as e:
                logger.error(f"Overall analysis failed for {stock['stock_name']}: {e}", exc_info=True)
                dim_progress[stock_id]['overall'] = 'error'
            finally:
                completed_dims += 1

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
                logger.debug("batch progress queue timeout, continuing...")
                pass
            yield progress_event()

        await analysis_task
        yield progress_event({'stage': 'done'})

    return StreamingResponse(generate(), media_type="text/event-stream")


def extract_grade_and_content(result: str):
    """从策略引擎大模型结果中提取 grade、content 和 data_issues"""
    try:
        clean = result.strip()
        if clean.startswith('```'):
            clean = re.sub(r'^```(?:json)?\s*\n', '', clean)
            clean = re.sub(r'\n```\s*$', '', clean)
        clean = clean.strip()

        # 如果整体不是JSON，尝试从文本中提取最后一个 JSON 块
        if not clean.startswith('{'):
            json_blocks = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', clean, re.DOTALL)
            if json_blocks:
                clean = json_blocks[-1].strip()
                logger.debug("extract_grade_and_content 从文本中提取到JSON块（长度%d）", len(clean))
            else:
                logger.warning(
                    "extract_grade_and_content 未找到JSON块，原始结果前500字符: %s",
                    result[:500]
                )
                return '', result[:200], '', '', '无'

        if clean.startswith('{'):
            # 依次尝试多种解析策略
            parse_errors = []

            # 策略1: 直接解析
            try:
                data = _safe_loads(clean)
                return (data.get('not_hold_grade', ''), data.get('content', ''),
                        data.get('hold_grade', ''), data.get('content', ''),
                        data.get('data_issues', '无'))
            except json.JSONDecodeError as e:
                parse_errors.append(f"直接解析: {e}")

            # 策略2: 单引号转双引号（LLM常见问题）
            try:
                fixed = clean.replace("'", '"')
                data = _safe_loads(fixed)
                logger.debug("extract_grade_and_content 单引号转双引号后解析成功")
                return (data.get('not_hold_grade', ''), data.get('content', ''),
                        data.get('hold_grade', ''), data.get('content', ''),
                        data.get('data_issues', '无'))
            except (json.JSONDecodeError, ValueError) as e:
                parse_errors.append(f"单引号转双引号: {e}")

            # 策略3: 替换未转义换行符
            try:
                sanitized = clean.replace('\n', '\\n')
                data = _safe_loads(sanitized)
                logger.debug("extract_grade_and_content 替换换行符后解析成功")
                return (data.get('not_hold_grade', ''), data.get('content', ''),
                        data.get('hold_grade', ''), data.get('content', ''),
                        data.get('data_issues', '无'))
            except (json.JSONDecodeError, ValueError) as e:
                parse_errors.append(f"替换换行符: {e}")

            # 策略4: 单引号转双引号 + 替换换行符
            try:
                fixed2 = clean.replace("'", '"').replace('\n', '\\n')
                data = _safe_loads(fixed2)
                logger.debug("extract_grade_and_content 单引号+换行符修复后解析成功")
                return (data.get('not_hold_grade', ''), data.get('content', ''),
                        data.get('hold_grade', ''), data.get('content', ''),
                        data.get('data_issues', '无'))
            except (json.JSONDecodeError, ValueError) as e:
                parse_errors.append(f"单引号+换行符: {e}")

            # 所有JSON解析策略失败，记录详细错误
            logger.warning(
                "extract_grade_and_content 所有JSON解析策略均失败:\n%s\n待解析内容前500字符: %s",
                "\n".join(f"  - {err}" for err in parse_errors),
                clean[:500]
            )

            # 最后兜底：正则提取字段
            not_hold_grade = re.search(r'''["']not_hold_grade["']\s*:\s*["']([^"']*)["']''', clean)
            content = re.search(r'''["']content["']\s*:\s*["'](.*?)["']''', clean, re.DOTALL)
            hold_grade = re.search(r'''["']hold_grade["']\s*:\s*["']([^"']*)["']''', clean)
            data_issues = re.search(r'''["']data_issues["']\s*:\s*["'](.*?)["']''', clean, re.DOTALL)

            regex_found = {
                'not_hold_grade': not_hold_grade.group(1) if not_hold_grade else None,
                'hold_grade': hold_grade.group(1) if hold_grade else None,
                'content': bool(content),
                'data_issues': bool(data_issues),
            }
            logger.info("extract_grade_and_content 正则兜底提取结果: %s", regex_found)

            return (
                not_hold_grade.group(1) if not_hold_grade else '',
                content.group(1) if content else result[:200],
                hold_grade.group(1) if hold_grade else '',
                '',
                data_issues.group(1) if data_issues else '无'
            )
    except Exception as e:
        logger.error(
            "extract_grade_and_content 未预期异常: %s\n原始结果前500字符: %s",
            e, result[:500], exc_info=True
        )
    return '', result[:200], '', '', '无'


def extract_grade_from_overall(result: str) -> str:
    """从整体分析JSON结果中提取grade字段"""
    try:
        clean = result.strip()
        if clean.startswith('```'):
            clean = re.sub(r'^```(?:json)?\s*\n', '', clean)
            clean = re.sub(r'\n```\s*$', '', clean)
        clean = clean.strip()
        if clean.startswith('{'):
            return _safe_loads(clean).get('grade', '')
    except Exception as e:
        logger.error("Error extracting grade: %s, result: %s", e, result[:200], exc_info=True)
    return ''


def extract_predictions(result: str) -> tuple:
    """从策略引擎大模型结果中提取 next_day_prediction 和 next_week_prediction，返回 (day_json_str, week_json_str)"""
    try:
        clean = result.strip()
        if clean.startswith('```'):
            clean = re.sub(r'^```(?:json)?\s*\n', '', clean)
            clean = re.sub(r'\n```\s*$', '', clean)
        clean = clean.strip()

        data = None
        if clean.startswith('{'):
            for attempt_fn in [
                lambda s: _safe_loads(s),
                lambda s: _safe_loads(s.replace("'", '"')),
                lambda s: _safe_loads(s.replace('\n', '\\n')),
                lambda s: _safe_loads(s.replace("'", '"').replace('\n', '\\n')),
            ]:
                try:
                    data = attempt_fn(clean)
                    break
                except (json.JSONDecodeError, ValueError):
                    continue

        if not data:
            # 尝试从文本中提取最后一个 JSON 块
            json_blocks = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', clean, re.DOTALL)
            if json_blocks:
                for attempt_fn in [
                    lambda s: _safe_loads(s),
                    lambda s: _safe_loads(s.replace("'", '"')),
                ]:
                    try:
                        data = attempt_fn(json_blocks[-1].strip())
                        break
                    except (json.JSONDecodeError, ValueError):
                        continue

        if data:
            day_pred = data.get('next_day_prediction')
            week_pred = data.get('next_week_prediction')
            day_str = json.dumps(day_pred, ensure_ascii=False) if day_pred else None
            week_str = json.dumps(week_pred, ensure_ascii=False) if week_pred else None
            return day_str, week_str
    except Exception as e:
        logger.error("extract_predictions 异常: %s, result: %s", e, result[:200], exc_info=True)
    return None, None


def extract_kline_total_score(result: str) -> int:
    """从策略引擎大模型结果中提取综合评分总分"""
    try:
        clean = result.strip()
        if clean.startswith('```'):
            clean = re.sub(r'^```(?:json)?\s*\n', '', clean)
            clean = re.sub(r'\n```\s*$', '', clean)
        clean = clean.strip()

        if clean.startswith('{'):
            for attempt_fn in [
                lambda s: _safe_loads(s),
                lambda s: _safe_loads(s.replace("'", '"')),
                lambda s: _safe_loads(s.replace('\n', '\\n')),
                lambda s: _safe_loads(s.replace("'", '"').replace('\n', '\\n')),
            ]:
                try:
                    data = attempt_fn(clean)
                    val = data.get('score')
                    if val is not None:
                        return int(float(val))
                    return None
                except (json.JSONDecodeError, ValueError):
                    continue

        # 正则兜底
        m = re.search(r'["\']score["\']\s*:\s*(\d+)', clean)
        if m:
            return int(m.group(1))
        return None
    except Exception as e:
        logger.error("extract_kline_total_score 异常: %s, result: %s", e, result[:200], exc_info=True)
        return None

def extract_score_from_result(result: str) -> float:
    """从分析结果中提取分数"""
    try:
        clean_result = result.strip()
        if clean_result.startswith('```'):
            clean_result = re.sub(r'^```(?:json)?\s*\n', '', clean_result)
            clean_result = re.sub(r'\n```\s*$', '', clean_result)
        
        clean_result = clean_result.strip()
        
        if clean_result.startswith('{'):
            try:
                data = _safe_loads(clean_result)
            except json.JSONDecodeError:
                # LLM sometimes returns single-quoted JSON; fix and retry
                try:
                    fixed = clean_result.replace("'", '"')
                    data = _safe_loads(fixed)
                except json.JSONDecodeError as e:
                    logger.error("Error parsing score JSON: %s, result: %s", e, clean_result[:200], exc_info=True)
                    m = re.search(r'["\']score["\']\s*:\s*["\']?(\d+\.?\d*)', clean_result)
                    if m:
                        return round(float(m.group(1)), 2)
                    return 0.0
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
        logger.error("Error extracting score: %s, result: %s", e, result)
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
            try:
                data = _safe_loads(clean_result)
                return data.get('content', data.get('summary', data.get('analysis', '')))
            except json.JSONDecodeError:
                for key in ('content', 'summary', 'analysis'):
                    m = re.search(rf'"{key}"\s*:\s*"(.*?)"(?=\s*[,}}])', clean_result, re.DOTALL)
                    if m:
                        return m.group(1)
                return ''
        return result[:200] + '...' if len(result) > 200 else result
    except Exception as e:
        logger.error("Error extracting summary: %s, result: %s", e, result[:100])
        return result[:200] + '...' if len(result) > 200 else result
