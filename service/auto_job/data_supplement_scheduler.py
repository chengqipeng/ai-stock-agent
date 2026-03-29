"""
数据补充调度模块 — 行业排名 / 业绩预告 / 大单追踪

从资讯调度中拆分出来的三个独立数据抓取任务：
1. 行业排名：每个交易日 15:30 抓取各股票的行业排名数据
2. 业绩预告：每个交易日 16:00 抓取业绩预告并同步到财报表
3. 大单追踪：每个交易日 15:15 抓取当日大单成交数据
"""
import asyncio
import json
import logging
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from service.auto_job.kline_data_scheduler import app_ready, is_a_share_trading_day
from service.auto_job.scheduler_orchestrator import scheduler_lock, manual_semaphore

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)
_project_root = Path(__file__).parent.parent.parent


# ═══════════════════════════════════════════════════════════════
# 1. 行业排名调度
# ═══════════════════════════════════════════════════════════════

_ranking_status = {
    "running": False, "last_run_date": None, "last_run_time": None,
    "last_success": None, "error": None, "start_time": None,
    "total": 0, "done": 0, "total_rows": 0,
}


def get_ranking_job_status() -> dict:
    return dict(_ranking_status)


async def _execute_ranking_job(manual=False):
    if _ranking_status["running"]:
        return
    from curl_cffi.requests import AsyncSession
    from service.jqka10.stock_news_10jqka import _fetch_industry_ranking, _extract_code, IMPERSONATE
    from dao.stock_news_dao import create_news_table, batch_upsert_news
    from dao.scheduler_log_dao import insert_log, update_log

    lock = manual_semaphore if manual else scheduler_lock
    async with lock:
        _ranking_status["running"] = True
        _ranking_status["error"] = None
        start = datetime.now(_CST)
        _ranking_status["start_time"] = start.isoformat()
        _ranking_status["total_rows"] = 0
        _ranking_status["done"] = 0

        log_id = insert_log("行业排名", start)
        create_news_table()

        from service.auto_job.kline_data_scheduler import load_stocks_from_score_list
        stocks = load_stocks_from_score_list()
        stocks = [s for s in stocks if not s["code"].endswith(".BJ")]
        _ranking_status["total"] = len(stocks)
        total_rows = 0
        failed = 0

        try:
            async with AsyncSession(impersonate=IMPERSONATE) as session:
                for i, stock in enumerate(stocks):
                    code = _extract_code(stock["code"])
                    try:
                        items = await _fetch_industry_ranking(code, session)
                        if items:
                            batch_upsert_news(stock["code"], items)
                            total_rows += len(items)
                        _ranking_status["done"] = i + 1
                        _ranking_status["total_rows"] = total_rows
                        await asyncio.sleep(1.0)
                    except Exception as e:
                        failed += 1
                        logger.warning("[行业排名] %s 失败: %s", stock["code"], e)
                        await asyncio.sleep(2)

            _ranking_status["last_run_date"] = start.date().isoformat()
            _ranking_status["last_run_time"] = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
            _ranking_status["last_success"] = True
            elapsed = int((datetime.now(_CST) - start).total_seconds())
            update_log(log_id, "success", len(stocks), len(stocks) - failed, failed,
                       detail=f"共{len(stocks)}只 写入{total_rows}条 失败{failed} 耗时{elapsed}s")
            logger.info("[行业排名] 完成: %d只 %d条 %d失败 %ds", len(stocks), total_rows, failed, elapsed)
        except Exception as e:
            _ranking_status["last_success"] = False
            _ranking_status["error"] = str(e)
            logger.error("[行业排名] 异常: %s", e, exc_info=True)
            try:
                update_log(log_id, "failed", detail=str(e))
            except Exception:
                pass
        finally:
            _ranking_status["running"] = False
            _ranking_status["start_time"] = None


# ═══════════════════════════════════════════════════════════════
# 2. 业绩预告调度
# ═══════════════════════════════════════════════════════════════

_forecast_status = {
    "running": False, "last_run_date": None, "last_run_time": None,
    "last_success": None, "error": None, "start_time": None,
    "total": 0, "done": 0, "total_rows": 0, "synced": 0,
}


def get_forecast_job_status() -> dict:
    return dict(_forecast_status)


async def _execute_forecast_job(manual=False):
    if _forecast_status["running"]:
        return
    from service.jqka10.stock_news_10jqka import _fetch_performance_forecast, _extract_code
    from dao.stock_news_dao import create_news_table, batch_upsert_news
    from dao.scheduler_log_dao import insert_log, update_log

    lock = manual_semaphore if manual else scheduler_lock
    async with lock:
        _forecast_status["running"] = True
        _forecast_status["error"] = None
        start = datetime.now(_CST)
        _forecast_status["start_time"] = start.isoformat()
        _forecast_status["total_rows"] = 0
        _forecast_status["synced"] = 0
        _forecast_status["done"] = 0

        log_id = insert_log("业绩预告", start)
        create_news_table()

        from service.auto_job.kline_data_scheduler import load_stocks_from_score_list
        stocks = load_stocks_from_score_list()
        stocks = [s for s in stocks if not s["code"].endswith(".BJ")]
        _forecast_status["total"] = len(stocks)
        total_rows = 0
        failed = 0

        try:
            for i, stock in enumerate(stocks):
                code = _extract_code(stock["code"])
                try:
                    items = await _fetch_performance_forecast(code)
                    if items:
                        batch_upsert_news(stock["code"], items)
                        total_rows += len(items)
                    _forecast_status["done"] = i + 1
                    _forecast_status["total_rows"] = total_rows
                    await asyncio.sleep(0.5)
                except Exception as e:
                    failed += 1
                    logger.warning("[业绩预告] %s 失败: %s", stock["code"], e)
                    await asyncio.sleep(1)

            # 同步到财报表
            synced = 0
            try:
                from tools.extract_forecast_to_finance import run_extraction
                logger.info("[业绩预告] 同步到财报表...")
                synced = run_extraction() or 0
                _forecast_status["synced"] = synced
            except Exception as e:
                logger.error("[业绩预告] 同步财报表失败: %s", e)

            _forecast_status["last_run_date"] = start.date().isoformat()
            _forecast_status["last_run_time"] = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
            _forecast_status["last_success"] = True
            elapsed = int((datetime.now(_CST) - start).total_seconds())
            update_log(log_id, "success", len(stocks), len(stocks) - failed, failed,
                       detail=f"共{len(stocks)}只 写入{total_rows}条 同步财报{synced}条 失败{failed} 耗时{elapsed}s")
            logger.info("[业绩预告] 完成: %d只 %d条 同步%d 失败%d %ds",
                        len(stocks), total_rows, synced, failed, elapsed)
        except Exception as e:
            _forecast_status["last_success"] = False
            _forecast_status["error"] = str(e)
            logger.error("[业绩预告] 异常: %s", e, exc_info=True)
            try:
                update_log(log_id, "failed", detail=str(e))
            except Exception:
                pass
        finally:
            _forecast_status["running"] = False
            _forecast_status["start_time"] = None


# ═══════════════════════════════════════════════════════════════
# 3. 大单追踪调度
# ═══════════════════════════════════════════════════════════════

_big_order_status = {
    "running": False, "last_run_date": None, "last_run_time": None,
    "last_success": None, "error": None, "start_time": None,
    "total_rows": 0,
}


def get_big_order_job_status() -> dict:
    return dict(_big_order_status)


async def _execute_big_order_job(manual=False):
    if _big_order_status["running"]:
        return
    from dao.stock_big_order_dao import create_big_order_table, batch_insert_big_orders, has_big_orders
    from service.jqka10.stock_fund_flow_10jqka import fetch_fund_flow_all_pages
    from dao.scheduler_log_dao import insert_log, update_log

    lock = manual_semaphore if manual else scheduler_lock
    async with lock:
        _big_order_status["running"] = True
        _big_order_status["error"] = None
        start = datetime.now(_CST)
        _big_order_status["start_time"] = start.isoformat()
        _big_order_status["total_rows"] = 0
        today_str = start.date().isoformat()

        log_id = insert_log("大单追踪", start)
        create_big_order_table()

        try:
            if has_big_orders(today_str) and not manual:
                _big_order_status["last_run_date"] = today_str
                _big_order_status["last_run_time"] = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
                _big_order_status["last_success"] = True
                update_log(log_id, "success", detail="今日数据已存在，跳过")
                logger.info("[大单追踪] 今日数据已存在，跳过")
            else:
                rows = await fetch_fund_flow_all_pages("ddzz", max_pages=5)
                count = 0
                if rows:
                    count = batch_insert_big_orders(today_str, rows)
                _big_order_status["total_rows"] = count
                _big_order_status["last_run_date"] = today_str
                _big_order_status["last_run_time"] = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
                _big_order_status["last_success"] = True
                elapsed = int((datetime.now(_CST) - start).total_seconds())
                update_log(log_id, "success", count, count, 0,
                           detail=f"写入{count}条 耗时{elapsed}s")
                logger.info("[大单追踪] 完成: %d条 %ds", count, elapsed)
        except Exception as e:
            _big_order_status["last_success"] = False
            _big_order_status["error"] = str(e)
            logger.error("[大单追踪] 异常: %s", e, exc_info=True)
            try:
                update_log(log_id, "failed", detail=str(e))
            except Exception:
                pass
        finally:
            _big_order_status["running"] = False
            _big_order_status["start_time"] = None


# ═══════════════════════════════════════════════════════════════
# 调度循环
# ═══════════════════════════════════════════════════════════════

async def _ranking_loop():
    while True:
        try:
            now = datetime.now(_CST)
            target = now.replace(hour=15, minute=30, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            while not is_a_share_trading_day(target.date()):
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            if wait > 0:
                await asyncio.sleep(wait)
            if _ranking_status.get("last_run_date") == datetime.now(_CST).date().isoformat():
                await asyncio.sleep(60)
                continue
            await _execute_ranking_job()
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("[行业排名] 循环异常: %s", e)
            await asyncio.sleep(300)


async def _forecast_loop():
    while True:
        try:
            now = datetime.now(_CST)
            target = now.replace(hour=16, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            while not is_a_share_trading_day(target.date()):
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            if wait > 0:
                await asyncio.sleep(wait)
            if _forecast_status.get("last_run_date") == datetime.now(_CST).date().isoformat():
                await asyncio.sleep(60)
                continue
            await _execute_forecast_job()
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("[业绩预告] 循环异常: %s", e)
            await asyncio.sleep(300)


async def _big_order_loop():
    while True:
        try:
            now = datetime.now(_CST)
            target = now.replace(hour=15, minute=15, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            while not is_a_share_trading_day(target.date()):
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            if wait > 0:
                await asyncio.sleep(wait)
            if _big_order_status.get("last_run_date") == datetime.now(_CST).date().isoformat():
                await asyncio.sleep(60)
                continue
            await _execute_big_order_job()
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("[大单追踪] 循环异常: %s", e)
            await asyncio.sleep(300)


async def start_data_supplement_schedulers():
    """启动行业排名/业绩预告/大单追踪三个调度器"""
    async def _deferred():
        await app_ready.wait()
        logger.info("[数据补充] 应用就绪，启动行业排名/业绩预告/大单追踪调度器")
        asyncio.create_task(_ranking_loop())
        asyncio.create_task(_forecast_loop())
        asyncio.create_task(_big_order_loop())
    asyncio.create_task(_deferred())
