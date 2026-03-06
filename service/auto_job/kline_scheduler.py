"""
日线数据定时调度模块

- 每个A股交易日15:05自动触发K线+财报数据拉取
- 项目启动时检查当天是否已完成，未完成则立即补拉
- 状态通过API暴露给前端展示
"""
import asyncio
import logging
from datetime import datetime, date, timedelta, time as dtime
from zoneinfo import ZoneInfo
from chinese_calendar import is_workday

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

# ─────────── 全局状态 ───────────

_job_status = {
    "last_run_time": None,       # 最近一次执行完成时间 (str)
    "last_run_date": None,       # 最近一次执行对应的交易日 (str)
    "last_success": None,        # 最近一次是否成功 (bool)
    "kline_total": 0,
    "kline_success": 0,
    "kline_failed": 0,
    "finance_total": 0,
    "finance_success": 0,
    "finance_failed": 0,
    "running": False,            # 是否正在执行
}


def get_job_status() -> dict:
    return dict(_job_status)


def is_a_share_trading_day(d: date) -> bool:
    """判断是否为A股交易日（工作日且非节假日）"""
    return d.weekday() < 5 and is_workday(d)


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一次触发时间：下一个交易日的15:05"""
    d = after.date()
    trigger_time = dtime(15, 5)

    # 如果当天是交易日且还没到15:05，今天就触发
    if is_a_share_trading_day(d) and after.time() < trigger_time:
        return datetime.combine(d, trigger_time, tzinfo=_CST)

    # 否则找下一个交易日
    d += timedelta(days=1)
    while not is_a_share_trading_day(d):
        d += timedelta(days=1)
    return datetime.combine(d, trigger_time, tzinfo=_CST)


def _already_done_today() -> bool:
    now_date = datetime.now(_CST).date().isoformat()
    return _job_status["last_run_date"] == now_date


async def _execute_job():
    """执行一次K线+财报采集"""
    from service.auto_job.stock_history_klines_auto_job import run_kline_job, run_finance_job

    _job_status["running"] = True
    today_str = datetime.now(_CST).date().isoformat()
    logger.info("[定时调度] 开始执行日线数据拉取 %s", today_str)

    kline_counter = {"total": 0, "success": 0, "failed": 0}
    finance_counter = {"total": 0, "success": 0, "failed": 0}

    try:
        kline_counter = await run_kline_job(limit=800, max_concurrent=1)
    except Exception as e:
        logger.error("[定时调度] K线采集异常: %s", e)

    try:
        finance_counter = await run_finance_job(max_concurrent=3)
    except Exception as e:
        logger.error("[定时调度] 财报采集异常: %s", e)

    now_str = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
    total_failed = kline_counter.get("failed", 0) + finance_counter.get("failed", 0)

    _job_status.update({
        "last_run_time": now_str,
        "last_run_date": today_str,
        "last_success": total_failed == 0,
        "kline_total": kline_counter.get("total", 0),
        "kline_success": kline_counter.get("success", 0),
        "kline_failed": kline_counter.get("failed", 0),
        "finance_total": finance_counter.get("total", 0),
        "finance_success": finance_counter.get("success", 0),
        "finance_failed": finance_counter.get("failed", 0),
        "running": False,
    })

    logger.info("[定时调度] 执行完成 K线:%d/%d 财报:%d/%d",
                kline_counter.get("success", 0), kline_counter.get("total", 0),
                finance_counter.get("success", 0), finance_counter.get("total", 0))


async def _scheduler_loop():
    """调度主循环：计算下次触发时间，sleep到点后执行"""
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_seconds = (next_dt - now).total_seconds()

            if wait_seconds > 0:
                logger.info("[定时调度] 下次执行时间: %s (%.0f秒后)", next_dt.strftime("%Y-%m-%d %H:%M"), wait_seconds)
                await asyncio.sleep(wait_seconds)

            # 再次确认是交易日（防止跨天漂移）
            trigger_date = datetime.now(_CST).date()
            if not is_a_share_trading_day(trigger_date):
                continue

            if _already_done_today():
                logger.info("[定时调度] 今日 %s 已执行过，跳过", trigger_date)
                await asyncio.sleep(60)
                continue

            await _execute_job()

            # 执行完后等一分钟再进入下一轮，避免重复触发
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("[定时调度] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[定时调度] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_scheduler():
    """启动调度器：先检查是否需要补拉，再启动定时循环"""
    now = datetime.now(_CST)
    today = now.date()

    # 启动时检查：如果当天是交易日、已过15:00、且今天还没执行过 → 立即补拉
    if is_a_share_trading_day(today) and now.time() >= dtime(15, 0) and not _already_done_today():
        logger.info("[定时调度] 启动补拉：今天是交易日且已过15:00，立即执行")
        asyncio.create_task(_execute_job())

    # 启动定时循环
    asyncio.create_task(_scheduler_loop())
    logger.info("[定时调度] 调度器已启动")
