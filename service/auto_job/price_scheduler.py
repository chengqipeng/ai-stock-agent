"""
最高最低价数据定时调度模块

- 每个A股交易日15:05自动触发最高最低价数据拉取
- 一天只拉取一次，只有全部成功才标记完成
- 失败时自动重试失败的股票（间隔5分钟），直到全部成功
- 项目启动时检查当天是否已完成，未完成且已过15:00则立即补拉
- 状态通过API暴露给前端展示
"""
import asyncio
import json
import logging
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from service.auto_job.kline_scheduler import is_a_share_trading_day, app_ready

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

# 失败重试间隔（秒）
_RETRY_INTERVAL = 300

# ─────────── 状态持久化 ───────────
_PRICE_STATUS_FILE = Path(__file__).parent.parent.parent / "data_results" / ".price_scheduler_status.json"


def _query_last_update_from_db() -> dict:
    """从数据库查询最近一次 update_time 作为兜底"""
    try:
        from dao import get_connection
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT MAX(update_time) FROM stock_highest_lowest_price"
            )
            row = cursor.fetchone()
            if row and row[0]:
                ts = str(row[0])  # e.g. "2026-03-06 15:12:34"
                run_date = ts[:10]  # "2026-03-06"
                logger.info("[最高最低价调度] 从数据库恢复上次执行时间: %s", ts)
                return {
                    "last_run_time": ts,
                    "last_run_date": run_date,
                    "last_success": True,
                }
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.warning("[最高最低价调度] 从数据库查询上次执行时间失败: %s", e)
    return {}


def _load_persisted_status() -> dict:
    """从本地文件恢复上次执行状态，文件不存在时从数据库兜底"""
    try:
        if _PRICE_STATUS_FILE.exists():
            data = json.loads(_PRICE_STATUS_FILE.read_text(encoding="utf-8"))
            logger.info("[最高最低价调度] 从文件恢复状态: last_run_date=%s", data.get("last_run_date"))
            return data
    except Exception as e:
        logger.warning("[最高最低价调度] 读取状态文件失败: %s", e)
    # 文件不存在，尝试从数据库获取
    return _query_last_update_from_db()


def _save_persisted_status(status: dict):
    """将关键状态持久化到本地文件"""
    try:
        _PRICE_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_run_time": status.get("last_run_time"),
            "last_run_date": status.get("last_run_date"),
            "last_success": status.get("last_success"),
        }
        _PRICE_STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[最高最低价调度] 写入状态文件失败: %s", e)


# ─────────── 全局状态 ───────────
_persisted = _load_persisted_status()

_job_status = {
    "last_run_time": _persisted.get("last_run_time"),
    "last_run_date": _persisted.get("last_run_date"),
    "last_success": _persisted.get("last_success"),
    "price_total": 0,
    "price_success": 0,
    "price_failed": 0,
    "running": False,            # 是否正在执行
}


def get_price_job_status() -> dict:
    status = dict(_job_status)
    if status.get("running"):
        pc = status.get("_price_counter") or {}
        status["price_total"] = pc.get("total", 0)
        status["price_success"] = pc.get("success", 0)
        status["price_failed"] = pc.get("failed", 0)
    status.pop("_price_counter", None)
    return status


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一次触发时间：下一个交易日的15:05"""
    d = after.date()
    trigger_time = dtime(15, 5)

    if is_a_share_trading_day(d) and after.time() < trigger_time:
        return datetime.combine(d, trigger_time, tzinfo=_CST)

    d += timedelta(days=1)
    while not is_a_share_trading_day(d):
        d += timedelta(days=1)
    return datetime.combine(d, trigger_time, tzinfo=_CST)


def _already_done_today() -> bool:
    """只有当天执行过且全部成功，才算已完成"""
    now_date = datetime.now(_CST).date().isoformat()
    return _job_status["last_run_date"] == now_date and _job_status["last_success"] is True


async def _execute_job():
    """执行一次最高最低价采集，失败时自动重试直到全部成功"""
    from service.auto_job.stock_history_highest_lowest_price_auto_job import run_price_job

    _job_status["running"] = True
    today_str = datetime.now(_CST).date().isoformat()
    attempt = 0

    while True:
        attempt += 1
        logger.info("[最高最低价调度] 开始执行 %s（第%d次尝试）", today_str, attempt)

        price_counter = {"total": 0, "success": 0, "failed": 0}
        _job_status["_price_counter"] = price_counter

        try:
            price_counter = await run_price_job(max_concurrent=5, counter=price_counter)
            _job_status["_price_counter"] = price_counter
        except Exception as e:
            logger.error("[最高最低价调度] 采集异常: %s", e)

        now_str = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
        total_failed = price_counter.get("failed", 0)
        all_success = total_failed == 0

        _job_status.update({
            "last_run_time": now_str,
            "last_run_date": today_str,
            "last_success": all_success,
            "price_total": price_counter.get("total", 0),
            "price_success": price_counter.get("success", 0),
            "price_failed": total_failed,
            "_price_counter": None,
        })

        # 持久化状态
        _save_persisted_status(_job_status)

        if all_success:
            logger.info("[最高最低价调度] 全部成功 %d/%d",
                        price_counter.get("success", 0), price_counter.get("total", 0))
            break

        # 有失败，等待后重试（run_price_job 内部会跳过已成功入库的股票）
        logger.warning("[最高最低价调度] 有 %d 只失败，%d秒后重试失败的股票",
                       total_failed, _RETRY_INTERVAL)
        await asyncio.sleep(_RETRY_INTERVAL)

    _job_status["running"] = False


async def _scheduler_loop():
    """调度主循环"""
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_seconds = (next_dt - now).total_seconds()

            if wait_seconds > 0:
                logger.info("[最高最低价调度] 下次执行时间: %s (%.0f秒后)", next_dt.strftime("%Y-%m-%d %H:%M"), wait_seconds)
                await asyncio.sleep(wait_seconds)

            trigger_date = datetime.now(_CST).date()
            if not is_a_share_trading_day(trigger_date):
                continue

            if _already_done_today():
                logger.info("[最高最低价调度] 今日 %s 已全部成功，跳过", trigger_date)
                await asyncio.sleep(60)
                continue

            await _execute_job()
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("[最高最低价调度] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[最高最低价调度] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_price_scheduler():
    """启动调度器：等待应用就绪后，检查是否需要补拉，再启动定时循环"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[最高最低价调度] 应用已就绪，调度器开始工作")

        now = datetime.now(_CST)
        today = now.date()

        if is_a_share_trading_day(today) and now.time() >= dtime(15, 0) and not _already_done_today():
            logger.info("[最高最低价调度] 启动补拉：今天是交易日且已过15:00，将在5秒后执行")
            async def _delayed_execute():
                await asyncio.sleep(5)
                await _execute_job()
            asyncio.create_task(_delayed_execute())

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[最高最低价调度] 调度器已注册，等待应用就绪")
