"""
周预测定时调度模块

- 每个交易日 17:30（北京时间）自动触发
- 调用 run_batch_weekly_prediction 执行全量周预测
- 项目启动时检查当天是否已完成，未完成则补拉
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
_STATUS_FILE = _project_root / "data_results" / ".weekly_prediction_scheduler_status.json"


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
        _STATUS_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[周预测调度] 状态持久化失败: %s", e)


# ─────────── 全局状态 ───────────
_job_status = {
    "running": False,
    "last_run_date": None,
    "last_run_time": None,
    "last_success": None,
    "error": None,
    "start_time": None,
    "predict_total": 0,
    "predict_done": 0,
    "predict_up": 0,
    "predict_down": 0,
    "backtest_accuracy": 0,
    "backtest_lowo_accuracy": 0,
}
_persisted = _load_persisted_status()
_job_status.update(_persisted)


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


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一个触发时间：每天17:30 CST"""
    today_trigger = after.replace(hour=17, minute=30, second=0, microsecond=0)
    if after >= today_trigger:
        today_trigger += timedelta(days=1)
    return today_trigger


# ═══════════════════════════════════════════════════════════════
# 核心执行逻辑
# ═══════════════════════════════════════════════════════════════

async def _execute_job():
    """执行一次批量周预测"""
    from dao.scheduler_log_dao import insert_log, update_log

    _job_status["running"] = True
    _job_status["error"] = None
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

        # run_batch_weekly_prediction 是同步阻塞函数，放到线程池执行
        result = await asyncio.get_event_loop().run_in_executor(
            None, run_batch_weekly_prediction
        )

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
        _job_status["start_time"] = None
        _save_persisted_status(_job_status)


# ═══════════════════════════════════════════════════════════════
# 调度循环
# ═══════════════════════════════════════════════════════════════

async def _scheduler_loop():
    """调度主循环：每天17:30 CST触发"""
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_seconds = (next_dt - now).total_seconds()

            if wait_seconds > 0:
                logger.info("[周预测调度] 下次执行时间: %s (%.0f秒后)",
                            next_dt.strftime("%Y-%m-%d %H:%M"), wait_seconds)
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
        logger.info("[周预测调度] 应用已就绪，调度器开始工作")

        if not _already_done_today():
            logger.info("[周预测调度] 启动补拉：将在20秒后执行")

            async def _delayed_execute():
                await asyncio.sleep(20)
                await _execute_job()

            asyncio.create_task(_delayed_execute())

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[周预测调度] 调度器已注册，等待应用就绪")
