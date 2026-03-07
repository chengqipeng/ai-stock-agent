"""
K线初筛定时调度模块

- 在日线数据调度执行成功后自动触发
- 对所有标记为持续分析的批次执行K线初筛（大模型分析）
- 一天只执行一次，全部成功才标记完成
- 失败时自动重试（间隔5分钟），直到全部成功
- 状态通过API暴露给前端展示
"""
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from service.auto_job.kline_data_scheduler import app_ready, is_a_share_trading_day
from service.auto_job.kline_technical_scheduler import kline_done_event_for_kscore

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_RETRY_INTERVAL = 300

# ─────────── 状态持久化 ───────────
_STATUS_FILE = Path(__file__).parent.parent.parent / "data_results" / ".kline_score_scheduler_status.json"


def _load_persisted_status() -> dict:
    try:
        if _STATUS_FILE.exists():
            data = json.loads(_STATUS_FILE.read_text(encoding="utf-8"))
            logger.info("[K线初筛调度] 从文件恢复状态: last_run_date=%s", data.get("last_run_date"))
            return data
    except Exception as e:
        logger.warning("[K线初筛调度] 读取状态文件失败: %s", e)
    return {}


def _save_persisted_status(status: dict):
    try:
        _STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_run_time": status.get("last_run_time"),
            "last_run_date": status.get("last_run_date"),
            "last_success": status.get("last_success"),
            "done_stock_ids": status.get("done_stock_ids", []),
        }
        _STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[K线初筛调度] 写入状态文件失败: %s", e)


# ─────────── 全局状态 ───────────
_persisted = _load_persisted_status()

_job_status = {
    "last_run_time": _persisted.get("last_run_time"),
    "last_run_date": _persisted.get("last_run_date"),
    "last_success": _persisted.get("last_success"),
    "kline_score_total": 0,
    "kline_score_success": 0,
    "kline_score_failed": 0,
    "running": False,
    "error": None,
}


def get_kline_score_job_status() -> dict:
    status = dict(_job_status)
    if status.get("running"):
        sc = status.get("_counter") or {}
        status["kline_score_total"] = sc.get("total", 0)
        status["kline_score_success"] = sc.get("success", 0)
        status["kline_score_failed"] = sc.get("failed", 0)
    status.pop("_counter", None)
    return status


def _already_done_today() -> bool:
    now_date = datetime.now(_CST).date().isoformat()
    return _job_status["last_run_date"] == now_date and _job_status["last_success"] is True


async def _analyze_single_stock(stock, db_manager, counter, done_stock_ids):
    """对单只股票执行K线初筛"""
    from common.utils.stock_info_utils import get_stock_info_by_name
    from service.k_strategy.stock_k_strategy_service import get_k_strategy_analysis
    from api.web_batch_api import extract_grade_and_content, extract_kline_total_score

    stock_id = stock['id']
    stock_name = stock['stock_name']
    try:
        stock_info = get_stock_info_by_name(stock_name)
        prompt, result = await get_k_strategy_analysis(stock_info)
        not_hold_grade, not_hold_content, hold_grade, hold_content, data_issues = extract_grade_and_content(result)
        kline_total_score = extract_kline_total_score(result)

        db_manager.update_stock_dimension_score(stock_id, 'kline', not_hold_grade, not_hold_content, None, prompt)
        if kline_total_score is not None:
            db_manager.update_stock_kline_scores(stock_id, kline_total_score)
        db_manager.update_stock_kline_hold(stock_id, hold_grade, hold_content, data_issues)

        counter["success"] += 1
        done_stock_ids.add(stock_id)
        logger.info("[K线初筛调度 总%d 成功%d 失败%d] %s 评分: %s",
                    counter["total"], counter["success"], counter["failed"],
                    stock_name, not_hold_grade)
    except Exception as e:
        counter["failed"] += 1
        logger.error("[K线初筛调度] %s 失败: %s", stock_name, e)


async def _execute_job():
    """执行一次K线初筛"""
    from dao.stock_can_slim_dao import db_manager
    from dao.stock_technical_score_dao import get_continuous_analysis_batches
    from dao.scheduler_log_dao import insert_log, update_log

    _job_status["running"] = True
    _job_status["error"] = None
    _job_status["start_time"] = datetime.now(_CST).isoformat()
    today_str = datetime.now(_CST).date().isoformat()
    started_at = datetime.now(_CST)
    log_id = insert_log("K线初筛", started_at)
    attempt = 0

    # 断点续传：加载今日已完成的 stock_id 集合
    done_stock_ids = set()
    if _persisted.get("last_run_date") == today_str and not _persisted.get("last_success"):
        persisted_ids = _persisted.get("done_stock_ids", [])
        done_stock_ids = set(persisted_ids)
        if done_stock_ids:
            logger.info("[K线初筛调度] 断点续传：恢复 %d 只已完成的股票", len(done_stock_ids))

    total_skipped = 0

    try:
        while True:
            attempt += 1
            logger.info("[K线初筛调度] 开始执行 %s（第%d次尝试）", today_str, attempt)

            counter = {"total": 0, "success": 0, "failed": 0}
            _job_status["_counter"] = counter
            total_skipped = 0

            try:
                batches = get_continuous_analysis_batches()
                if not batches:
                    logger.info("[K线初筛调度] 没有标记为持续分析的批次，跳过")
                else:
                    for batch in batches:
                        batch_id = batch['id']
                        batch_name = batch.get('batch_name', str(batch_id))
                        stocks = db_manager.get_batch_stocks(batch_id)
                        if not stocks:
                            continue

                        # 断点续传：过滤已完成的股票
                        remaining = [s for s in stocks if s['id'] not in done_stock_ids]
                        skipped = len(stocks) - len(remaining)
                        total_skipped += skipped

                        counter["total"] += len(stocks)
                        counter["success"] += skipped
                        if skipped > 0:
                            logger.info("[K线初筛调度] 批次 %s 共 %d 只，今日已完成 %d 只，剩余 %d 只",
                                        batch_name, len(stocks), skipped, len(remaining))
                        else:
                            logger.info("[K线初筛调度] 批次 %s 共 %d 只股票", batch_name, len(remaining))

                        semaphore = asyncio.Semaphore(3)

                        async def _run(s):
                            async with semaphore:
                                await _analyze_single_stock(s, db_manager, counter, done_stock_ids)

                        await asyncio.gather(*[_run(s) for s in remaining], return_exceptions=True)
                        logger.info("[K线初筛调度] 批次 %s 完成", batch_name)

            except Exception as e:
                logger.error("[K线初筛调度] 执行异常: %s", e, exc_info=True)

            now_str = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
            total_failed = counter.get("failed", 0)
            all_success = total_failed == 0

            _job_status.update({
                "last_run_time": now_str,
                "last_run_date": today_str,
                "last_success": all_success,
                "kline_score_total": counter.get("total", 0),
                "kline_score_success": counter.get("success", 0),
                "kline_score_failed": total_failed,
                "_counter": None,
                "done_stock_ids": list(done_stock_ids) if not all_success else [],
            })

            _save_persisted_status(_job_status)

            if all_success:
                logger.info("[K线初筛调度] 全部成功 %d/%d",
                            counter.get("success", 0), counter.get("total", 0))
                detail = (f"总{counter['total']}只 成功{counter['success']}只 "
                          f"失败{total_failed}只 跳过(已完成){total_skipped}只 重试{attempt}次")
                update_log(log_id, "success", counter["total"], counter["success"],
                           total_failed, total_skipped, detail)
                break

            logger.warning("[K线初筛调度] 有 %d 只失败，%d秒后重试", total_failed, _RETRY_INTERVAL)
            await asyncio.sleep(_RETRY_INTERVAL)

    except Exception as e:
        import traceback as _tb
        err_msg = f"任务异常终止: {type(e).__name__}: {e}"
        err_detail = f"{err_msg}\n{_tb.format_exc()}"
        logger.error("[K线初筛调度] %s", err_msg, exc_info=True)
        _job_status.update({"error": err_msg, "_counter": None})
        try:
            update_log(log_id, "failed", detail=err_detail)
        except Exception:
            pass

    _job_status["running"] = False


async def _scheduler_loop():
    """调度主循环：等待日线完成信号后执行"""
    while True:
        try:
            await kline_done_event_for_kscore.wait()
            kline_done_event_for_kscore.clear()

            if _already_done_today():
                logger.info("[K线初筛调度] 今日已完成，跳过")
                continue

            logger.info("[K线初筛调度] 收到日线完成信号，开始执行K线初筛")
            await _execute_job()

        except asyncio.CancelledError:
            logger.info("[K线初筛调度] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[K线初筛调度] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_kline_score_scheduler():
    """启动K线初筛调度器"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[K线初筛调度] 应用已就绪，调度器开始工作")

        # 启动时检查：如果日线今天已完成但K线初筛未执行，则立即补拉
        from service.auto_job.kline_data_scheduler import get_job_status as get_kline_status
        kline_status = get_kline_status()
        today_str = datetime.now(_CST).date().isoformat()
        if (kline_status.get("last_run_date") == today_str
                and not kline_status.get("running")
                and not _already_done_today()):
            logger.info("[K线初筛调度] 启动补拉：日线今日已完成但K线初筛未执行，将在5秒后执行")
            async def _delayed_execute():
                await asyncio.sleep(5)
                await _execute_job()
            asyncio.create_task(_delayed_execute())

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[K线初筛调度] 调度器已注册，等待应用就绪")
