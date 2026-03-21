"""
月度预测定时调度模块

- 每月最后一个交易周的周四 18:00（北京时间）自动触发
- 调用 run_batch_monthly_prediction 执行全量月度预测
- 项目启动时检查当月是否需要补拉
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from service.auto_job.kline_data_scheduler import app_ready

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent

# ─────────── 状态持久化 ───────────
_STATUS_FILE = _project_root / "data_results" / ".monthly_prediction_scheduler_status.json"


def _load_persisted_status() -> dict:
    try:
        if _STATUS_FILE.exists():
            data = json.loads(_STATUS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save_persisted_status(status: dict):
    try:
        _STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_run_date": status.get("last_run_date"),
            "last_run_time": status.get("last_run_time"),
            "last_success": status.get("last_success"),
            "predict_total": status.get("predict_total", 0),
            "backtest_accuracy": status.get("backtest_accuracy", 0),
        }
        _STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[月预测调度] 状态持久化失败: %s", e)


# ─────────── 全局状态 ───────────
_job_status = {
    "running": False,
    "last_run_date": None,
    "last_run_time": None,
    "last_success": None,
    "error": None,
    "start_time": None,
    "stage": "",
    "predict_total": 0,
    "predict_done": 0,
    "predict_up": 0,
    "backtest_accuracy": 0,
}
_persisted = _load_persisted_status()
_job_status.update(_persisted)
_job_status["running"] = False
_job_status["error"] = None
_job_status["start_time"] = None
_job_status["stage"] = ""
_job_status["predict_total"] = 0
_job_status["predict_done"] = 0
_job_status["predict_up"] = 0


def get_monthly_prediction_job_status() -> dict:
    return dict(_job_status)


def _get_trade_date() -> str:
    now = datetime.now(_CST)
    d = now.date()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def _already_done_this_month() -> bool:
    """检查本月是否已执行过"""
    last_date = _job_status.get("last_run_date")
    if not last_date or not _job_status.get("last_success"):
        return False
    try:
        last_dt = datetime.strptime(last_date, '%Y-%m-%d')
        now = datetime.now(_CST)
        return last_dt.year == now.year and last_dt.month == now.month
    except Exception:
        return False


def _is_prediction_day() -> bool:
    """判断当天是否为月预测执行日。
    规则：每月25日之后的周四（即月末最后一个交易周的周四）。
    """
    now = datetime.now(_CST)
    return now.day >= 25 and now.weekday() == 3  # 周四=3


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一个触发时间：每月25日之后的第一个周四 18:00 CST"""
    trigger_time = after.replace(hour=18, minute=0, second=0, microsecond=0)

    # 如果今天满足条件且时间还没过
    if after < trigger_time and _is_prediction_day():
        return trigger_time

    # 找下一个满足条件的日期
    d = after.date() + timedelta(days=1)
    for _ in range(60):  # 最多找60天
        if d.day >= 25 and d.weekday() == 3:
            return datetime(d.year, d.month, d.day, 18, 0, 0, tzinfo=_CST)
        d += timedelta(days=1)

    # 兜底：下个月25日
    next_month = after.month + 1 if after.month < 12 else 1
    next_year = after.year if after.month < 12 else after.year + 1
    d = datetime(next_year, next_month, 25, tzinfo=_CST).date()
    while d.weekday() != 3:
        d += timedelta(days=1)
    return datetime(d.year, d.month, d.day, 18, 0, 0, tzinfo=_CST)


# ═══════════════════════════════════════════════════════════════
# 核心执行逻辑
# ═══════════════════════════════════════════════════════════════

async def _execute_job_inner():
    """执行一次批量月度预测"""
    from dao.scheduler_log_dao import insert_log, update_log

    _job_status["running"] = True
    _job_status["error"] = None
    _job_status["stage"] = "loading_data"
    _job_status["predict_total"] = 0
    _job_status["predict_done"] = 0
    _job_status["predict_up"] = 0
    _job_status["backtest_accuracy"] = 0
    start_time = datetime.now(_CST)
    trade_date = _get_trade_date()
    _job_status["last_run_date"] = trade_date
    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    _job_status["start_time"] = start_time.isoformat()
    _save_persisted_status(_job_status)

    log_id = insert_log("月预测", start_time)
    logger.info("[月预测调度] ===== 开始执行 trade_date=%s (log_id=%d) =====", trade_date, log_id)

    try:
        from service.can_slim_algo.canslim_monthly_prediction_service import run_batch_canslim_monthly_prediction

        def _progress(total, done, signal_count):
            _job_status["stage"] = "predicting"
            _job_status["predict_total"] = total
            _job_status["predict_done"] = done
            _job_status["predict_up"] = signal_count

        try:
            result = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: run_batch_canslim_monthly_prediction(progress_callback=_progress)
                ),
                timeout=2400,  # 40分钟
            )
        except asyncio.TimeoutError:
            raise TimeoutError("月预测执行超时（超过40分钟）")

        if result:
            _job_status["predict_total"] = result.get("total_predicted", 0)
            _job_status["predict_done"] = result.get("total_predicted", 0)
            _job_status["predict_up"] = result.get("total_predicted", 0)
            _job_status["backtest_accuracy"] = result.get("backtest_accuracy", 0)
            _job_status["last_success"] = True

            detail = (
                f"策略: {result.get('strategy', 'canslim_optimal')}\n"
                f"预测目标: {result.get('target_year', '')}-{result.get('target_month', 0):02d}\n"
                f"预测涨: {result.get('total_predicted', 0)}只 / {result.get('total_stocks', 0)}只\n"
                f"回测准确率: {result.get('backtest_accuracy', 0):.1f}% "
                f"({result.get('backtest_samples', 0)}样本)\n"
                f"耗时: {result.get('elapsed', 0)}s"
            )
            update_log(log_id, "success",
                       result.get("total_predicted", 0),
                       result.get("total_predicted", 0),
                       0, detail=detail)
        else:
            _job_status["last_success"] = False
            _job_status["error"] = "月预测服务返回空结果"
            update_log(log_id, "failed", detail="月预测服务返回空结果")

        elapsed = (datetime.now(_CST) - start_time).total_seconds()
        logger.info("[月预测调度] ===== 执行完成，耗时%.1f秒 =====", elapsed)

    except Exception as e:
        import traceback as _tb
        err_msg = f"{type(e).__name__}: {e}"
        _job_status["last_success"] = False
        _job_status["error"] = err_msg
        logger.error("[月预测调度] 执行异常: %s", err_msg, exc_info=True)
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
    from service.auto_job.scheduler_orchestrator import scheduler_lock, manual_semaphore, monthly_prediction_done_event
    if manual:
        _job_status["running"] = True
        _job_status["error"] = "等待手动调度槽位..."
        logger.info("[月预测调度] 手动触发，等待调度槽位")
        async with manual_semaphore:
            _job_status["error"] = None
            logger.info("[月预测调度] 已获取手动调度槽位")
            try:
                await _execute_job_inner()
            finally:
                monthly_prediction_done_event.set()
    else:
        async with scheduler_lock:
            logger.info("[月预测调度] 已获取全局调度锁")
            try:
                await _execute_job_inner()
            finally:
                monthly_prediction_done_event.set()
                logger.info("[月预测调度] 已发送完成信号")


# ═══════════════════════════════════════════════════════════════
# 调度循环
# ═══════════════════════════════════════════════════════════════

async def _scheduler_loop():
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_seconds = (next_dt - now).total_seconds()

            if wait_seconds > 0:
                logger.info("[月预测调度] 下次执行时间: %s (%.0f秒后)",
                            next_dt.strftime("%Y-%m-%d %H:%M"), wait_seconds)
                await asyncio.sleep(wait_seconds)

            if _already_done_this_month():
                logger.info("[月预测调度] 本月已执行过，跳过")
                await asyncio.sleep(3600)
                continue

            await _execute_job()
            await asyncio.sleep(3600)

        except asyncio.CancelledError:
            logger.info("[月预测调度] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[月预测调度] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_monthly_prediction_scheduler():
    """启动月度预测调度器"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[月预测调度] 应用已就绪，调度器开始工作（每月25日后周四执行）")

        if _is_prediction_day() and not _already_done_this_month():
            logger.info("[月预测调度] 启动补拉：今天是预测日且本月未执行，将在30秒后执行")

            async def _delayed_execute():
                await asyncio.sleep(30)
                await _execute_job()

            asyncio.create_task(_delayed_execute())
        else:
            if not _is_prediction_day():
                logger.info("[月预测调度] 今天不是月预测日，跳过补拉")
            else:
                logger.info("[月预测调度] 本月已执行过，跳过补拉")
            from service.auto_job.scheduler_orchestrator import monthly_prediction_done_event
            monthly_prediction_done_event.set()
            logger.info("[月预测调度] 今日无需执行，已发送完成信号")

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[月预测调度] 调度器已注册，等待应用就绪")
