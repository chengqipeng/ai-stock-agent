"""
周预测定时调度模块

- 每周三、周四 17:30（北京时间）自动触发
  - 周三：基于 d3 信号（前3天数据）
  - 周四：基于 d4 信号（前4天数据，最完整）
- 调用 run_batch_weekly_prediction 执行全量周预测
- 项目启动时检查当天是否需要补拉（仅周三/周四）
"""
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from service.auto_job.kline_data_scheduler import app_ready

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent

# ─────────── 状态持久化 ───────────
def _load_persisted_status() -> dict:
    """从数据库恢复状态"""
    from service.auto_job.scheduler_status_helper import restore_status
    return restore_status("weekly_pred")


def _save_persisted_status(status: dict):
    """持久化到数据库"""
    from service.auto_job.scheduler_status_helper import persist_status
    persist_status("weekly_pred", {
        "last_run_date": status.get("last_run_date"),
        "last_run_time": status.get("last_run_time"),
        "last_success": status.get("last_success"),
        "error": status.get("error"),
    }, {
        "predict": {"total": status.get("predict_total", 0), "success": status.get("predict_done", 0),
                    "extra_json": {"predict_up": status.get("predict_up", 0), "predict_down": status.get("predict_down", 0),
                                   "backtest_accuracy": status.get("backtest_accuracy", 0), "backtest_lowo_accuracy": status.get("backtest_lowo_accuracy", 0)}},
    })



# ─────────── 全局状态 ───────────
_job_status = {
    "running": False,
    "last_run_date": None,
    "last_run_time": None,
    "last_success": None,
    "error": None,
    "start_time": None,
    "stage": "",  # "loading_data" | "predicting" | "backtesting" | "writing_db" | ""
    "predict_total": 0,
    "predict_done": 0,
    "predict_up": 0,
    "predict_down": 0,
    "backtest_accuracy": 0,
    "backtest_lowo_accuracy": 0,
}
_persisted = _load_persisted_status()
_job_status.update(_persisted)
# 启动时强制重置运行时状态，防止上次进程崩溃后 running=True 被持久化导致卡死
_job_status["running"] = False
_job_status["error"] = None
_job_status["start_time"] = None
_job_status["stage"] = ""
_job_status["predict_total"] = 0
_job_status["predict_done"] = 0
_job_status["predict_up"] = 0
_job_status["predict_down"] = 0


def get_weekly_prediction_job_status() -> dict:
    return dict(_job_status)


def _get_trade_date() -> str:
    """获取当天交易日日期（周末回退到周五）"""
    now = datetime.now(_CST)
    d = now.date()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def _already_done_today() -> bool:
    return (
        _job_status.get("last_run_date") == _get_trade_date()
        and _job_status.get("last_success") is True
    )


def _is_prediction_day() -> bool:
    """判断当天是否为周预测执行日（周三=2 或 周四=3）"""
    now = datetime.now(_CST)
    return now.weekday() in (2, 3)


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一个触发时间：每周三/周四 17:30 CST"""
    trigger_time = after.replace(hour=17, minute=30, second=0, microsecond=0)

    # 如果今天的触发时间还没过，且今天是周三或周四，就用今天
    if after < trigger_time and after.weekday() in (2, 3):
        return trigger_time

    # 否则找下一个周三或周四
    d = after.date() + timedelta(days=1)
    while d.weekday() not in (2, 3):
        d += timedelta(days=1)
    return datetime(d.year, d.month, d.day, 17, 30, 0, tzinfo=_CST)


# ═══════════════════════════════════════════════════════════════
# 核心执行逻辑
# ═══════════════════════════════════════════════════════════════

async def _execute_job_inner():
    """执行一次批量周预测"""
    from dao.scheduler_log_dao import insert_log, update_log

    _job_status["running"] = True
    _job_status["error"] = None
    _job_status["stage"] = "loading_data"
    _job_status["predict_total"] = 0
    _job_status["predict_done"] = 0
    _job_status["predict_up"] = 0
    _job_status["predict_down"] = 0
    _job_status["backtest_accuracy"] = 0
    _job_status["backtest_lowo_accuracy"] = 0
    start_time = datetime.now(_CST)
    trade_date = _get_trade_date()
    _job_status["last_run_date"] = trade_date
    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    _job_status["start_time"] = start_time.isoformat()
    _save_persisted_status(_job_status)

    log_id = insert_log("周预测", start_time)
    logger.info("[周预测调度] ===== 开始执行 trade_date=%s (log_id=%d) =====", trade_date, log_id)

    try:
        from service.weekly_prediction_service import run_batch_weekly_prediction

        def _prediction_progress(total, done, up_count, down_count):
            _job_status["stage"] = "predicting"
            _job_status["predict_total"] = total
            _job_status["predict_done"] = done
            _job_status["predict_up"] = up_count
            _job_status["predict_down"] = down_count

        # run_batch_weekly_prediction 是同步阻塞函数，放到线程池执行
        # 设置超时保护：最多等待30分钟
        try:
            result = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: run_batch_weekly_prediction(progress_callback=_prediction_progress)
                ),
                timeout=1800,  # 30分钟
            )
        except asyncio.TimeoutError:
            raise TimeoutError("周预测执行超时（超过30分钟），可能是数据库查询阻塞")

        if result:
            _job_status["predict_total"] = result.get("total_stocks", 0)
            _job_status["predict_done"] = result.get("total_stocks", 0)
            _job_status["predict_up"] = result.get("up_count", 0)
            _job_status["predict_down"] = result.get("down_count", 0)
            bt = result.get("backtest", {})
            _job_status["backtest_accuracy"] = bt.get("full_accuracy", 0)
            _job_status["backtest_lowo_accuracy"] = bt.get("lowo_accuracy", 0)
            _job_status["last_success"] = True

            detail = (
                f"预测日期: {result.get('predict_date', '')} "
                f"(Y{result.get('iso_year', 0)}-W{result.get('iso_week', 0):02d})\n"
                f"股票数: {result.get('total_stocks', 0)}, "
                f"预测涨: {result.get('up_count', 0)}, "
                f"预测跌: {result.get('down_count', 0)}\n"
                f"回测准确率: {bt.get('full_accuracy', 0):.1f}% "
                f"(LOWO: {bt.get('lowo_accuracy', 0):.1f}%, "
                f"{bt.get('n_weeks', 0)}周, {bt.get('total_samples', 0)}样本)\n"
                f"耗时: {result.get('elapsed', 0)}s"
            )
            update_log(log_id, "success",
                       result.get("total_stocks", 0),
                       result.get("total_stocks", 0),
                       0, detail=detail)
        else:
            _job_status["last_success"] = False
            _job_status["error"] = "预测服务返回空结果"
            update_log(log_id, "failed", detail="预测服务返回空结果")

        elapsed = (datetime.now(_CST) - start_time).total_seconds()
        logger.info("[周预测调度] ===== 执行完成，耗时%.1f秒 =====", elapsed)

    except Exception as e:
        import traceback as _tb
        err_msg = f"{type(e).__name__}: {e}"
        _job_status["last_success"] = False
        _job_status["error"] = err_msg
        logger.error("[周预测调度] 执行异常: %s", err_msg, exc_info=True)
        try:
            update_log(log_id, "failed", detail=f"{err_msg}\n{_tb.format_exc()}")
        except Exception:
            pass

    finally:
        _job_status["running"] = False
        _job_status["stage"] = ""
        _job_status["start_time"] = None
        _save_persisted_status(_job_status)


async def _execute_job(manual=False):
    from service.auto_job.scheduler_orchestrator import scheduler_lock, manual_semaphore, weekly_prediction_done_event
    if manual:
        _job_status["running"] = True
        _job_status["error"] = "等待手动调度槽位..."
        logger.info("[周预测调度] 手动触发，等待调度槽位")
        async with manual_semaphore:
            _job_status["error"] = None
            logger.info("[周预测调度] 已获取手动调度槽位")
            try:
                await _execute_job_inner()
            finally:
                weekly_prediction_done_event.set()
    else:
        async with scheduler_lock:
            logger.info("[周预测调度] 已获取全局调度锁")
            try:
                await _execute_job_inner()
            finally:
                weekly_prediction_done_event.set()
                logger.info("[周预测调度] 已发送完成信号")


# ═══════════════════════════════════════════════════════════════
# 调度循环
# ═══════════════════════════════════════════════════════════════

async def _scheduler_loop():
    """调度主循环：每周三/周四 17:30 CST触发"""
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_seconds = (next_dt - now).total_seconds()

            if wait_seconds > 0:
                weekday_name = ['周一', '周二', '周三', '周四', '周五', '周六', '周日'][next_dt.weekday()]
                logger.info("[周预测调度] 下次执行时间: %s %s (%.0f秒后)",
                            next_dt.strftime("%Y-%m-%d %H:%M"), weekday_name, wait_seconds)
                await asyncio.sleep(wait_seconds)

            if _already_done_today():
                logger.info("[周预测调度] 今日 %s 已执行过，跳过", _get_trade_date())
                await asyncio.sleep(60)
                continue

            await _execute_job()
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("[周预测调度] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[周预测调度] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_weekly_prediction_scheduler():
    """启动周预测调度器"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[周预测调度] 应用已就绪，调度器开始工作（仅周三/周四执行）")

        if _is_prediction_day() and not _already_done_today():
            logger.info("[周预测调度] 启动补拉：今天是预测日且未执行，将在20秒后执行")

            async def _delayed_execute():
                await asyncio.sleep(20)
                await _execute_job()

            asyncio.create_task(_delayed_execute())
        else:
            if not _is_prediction_day():
                logger.info("[周预测调度] 今天不是预测日（仅周三/周四），跳过补拉")
            else:
                logger.info("[周预测调度] 今日已执行过，跳过补拉")
            # 不需要执行时，立即发送完成信号，避免db_check永远等待
            from service.auto_job.scheduler_orchestrator import weekly_prediction_done_event
            weekly_prediction_done_event.set()
            logger.info("[周预测调度] 今日无需执行，已发送完成信号")

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[周预测调度] 调度器已注册，等待应用就绪")
