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
import re
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from service.auto_job.kline_data_scheduler import is_a_share_trading_day, app_ready

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent

# 失败重试间隔（秒）
_RETRY_INTERVAL = 300
_MAX_RETRY = 3

# ─────────── 状态持久化 ───────────
_PRICE_STATUS_FILE = Path(__file__).parent.parent.parent / "data_results" / ".price_scheduler_status.json"


def _query_last_update_from_db() -> dict:
    """从数据库查询最近一次 update_time 作为兜底"""
    try:
        from dao.stock_highest_lowest_price_dao import get_last_update_time
        result = get_last_update_time()
        if result:
            logger.info("[最高最低价调度] 从数据库恢复上次执行时间: %s", result.get("last_run_time"))
        return result
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
    "error": None,               # 异常信息
}


def get_price_job_status() -> dict:
    status = dict(_job_status)
    if status.get("running"):
        # 安全超时：如果 running 状态持续超过2小时，强制重置
        start_time = status.get("start_time")
        if start_time:
            try:
                started = datetime.fromisoformat(start_time)
                elapsed = (datetime.now(_CST) - started).total_seconds()
                if elapsed > 7200:
                    logger.warning("[最高最低价调度] running 状态已持续 %.0f 秒，强制重置为 False", elapsed)
                    _job_status["running"] = False
                    _job_status["error"] = f"任务超时（已运行{int(elapsed//3600)}小时{int((elapsed%3600)//60)}分钟），已自动重置"
                    _job_status["_price_counter"] = None
                    status = dict(_job_status)
            except (ValueError, TypeError):
                pass
        pc = status.get("_price_counter") or {}
        status["price_total"] = pc.get("total", 0)
        status["price_success"] = pc.get("success", 0)
        status["price_failed"] = pc.get("failed", 0)
    status.pop("_price_counter", None)
    status.pop("start_time", None)
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


def _load_stocks() -> list[dict]:
    """从 stock_score_list.md 加载股票列表"""
    score_list_path = _project_root / "data_results/stock_to_score_list/stock_score_list.md"
    pattern = re.compile(r'^(.+?)\s+\(([^)]+)\)')
    all_stocks = []
    for line in score_list_path.read_text(encoding='utf-8').splitlines():
        m = pattern.match(line.strip())
        if m:
            all_stocks.append({'name': m.group(1), 'code': m.group(2)})
    return all_stocks


async def _execute_job_inner():
    """执行一次最高最低价采集，失败时自动重试直到全部成功"""
    from common.utils.stock_info_utils import get_stock_info_by_name
    from dao.stock_highest_lowest_price_dao import save_price_record, get_today_processed_codes
    from service.jqka10.stock_week_kline_data_10jqka import get_stock_week_kline_list_10jqka
    from dao.scheduler_log_dao import insert_log, update_log

    _job_status["running"] = True
    _job_status["error"] = None
    _job_status["start_time"] = datetime.now(_CST).isoformat()
    today_str = datetime.now(_CST).date().isoformat()
    started_at = datetime.now(_CST)
    log_id = insert_log("最高最低价", started_at)
    attempt = 0
    _db_lock = asyncio.Lock()

    async def _process_single(stock, counter):
        """处理单个股票的最高最低价数据"""
        stock_name = stock["name"]
        try:
            stock_info = get_stock_info_by_name(stock_name)
            kline_data = await get_stock_week_kline_list_10jqka(stock_info)

            if kline_data:
                highest_record = max(kline_data, key=lambda x: x["最高"])
                lowest_record = min(kline_data, key=lambda x: x["最低"])
                result = {
                    "code": stock["code"],
                    "name": stock_name,
                    "highest_price": highest_record["最高"],
                    "highest_date": highest_record["日期"],
                    "lowest_price": lowest_record["最低"],
                    "lowest_date": lowest_record["日期"],
                    "update_time": datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S"),
                }
                async with _db_lock:
                    save_price_record(result)

                counter["success"] += 1
                logger.info("[最高最低价 总%d 成功%d 失败%d 当前:%s] 最高%s(%s) 最低%s(%s)",
                            counter["total"], counter["success"], counter["failed"], stock_name,
                            highest_record["最高"], highest_record["日期"],
                            lowest_record["最低"], lowest_record["日期"])
            else:
                counter["failed"] += 1
                logger.warning("[最高最低价 %s] 返回空数据", stock_name)
        except Exception as e:
            counter["failed"] += 1
            logger.error("[最高最低价 %s] 失败: %s", stock_name, e)

    all_stocks = _load_stocks()
    total_all = len(all_stocks)

    try:
        while True:
            attempt += 1
            logger.info("[最高最低价调度] 开始执行 %s（第%d次尝试）", today_str, attempt)

            # 从数据库加载今日已处理的股票
            today_date = datetime.now(_CST).strftime("%Y-%m-%d")
            processed_codes = get_today_processed_codes(today_date)

            remaining_stocks = [s for s in all_stocks if s["code"] not in processed_codes]

            price_counter = {"total": len(remaining_stocks), "success": 0, "failed": 0}
            _job_status["_price_counter"] = price_counter

            logger.info("[最高最低价] 开始采集，共 %d 只股票（今日已完成 %d 只）",
                        len(remaining_stocks), len(processed_codes))

            if remaining_stocks:
                semaphore = asyncio.Semaphore(5)

                async def _run(s):
                    async with semaphore:
                        await _process_single(s, price_counter)

                await asyncio.gather(*[_run(s) for s in remaining_stocks], return_exceptions=True)
                logger.info("[最高最低价] 采集完成，总%d 成功%d 失败%d",
                            price_counter["total"], price_counter["success"], price_counter["failed"])

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

            _save_persisted_status(_job_status)

            if all_success:
                logger.info("[最高最低价调度] 全部成功 %d/%d",
                            price_counter.get("success", 0), price_counter.get("total", 0))
                skipped = len(processed_codes)
                detail = f"共{total_all}只 成功{total_all - total_failed}只 失败{total_failed}只 跳过(已完成){skipped}只 重试{attempt}次"
                update_log(log_id, "success", total_all, total_all, 0, skipped, detail)
                break

            logger.warning("[最高最低价调度] 有 %d 只失败，%d秒后重试失败的股票",
                           total_failed, _RETRY_INTERVAL)
            if attempt >= _MAX_RETRY:
                err_msg = f"达到最大重试次数({_MAX_RETRY})，仍有{total_failed}只失败"
                logger.error("[最高最低价调度] %s", err_msg)
                _job_status["error"] = err_msg
                detail = (f"共{total_all}只 失败{total_failed}只 重试{attempt}次 (达到上限)")
                try:
                    update_log(log_id, "failed", total_all, total_all - total_failed,
                               total_failed, len(processed_codes), detail)
                except Exception:
                    pass
                break
            await asyncio.sleep(_RETRY_INTERVAL)

    except Exception as e:
        import traceback as _tb
        err_msg = f"任务异常终止: {type(e).__name__}: {e}"
        err_detail = f"{err_msg}\n{_tb.format_exc()}"
        logger.error("[最高最低价调度] %s", err_msg, exc_info=True)
        _job_status.update({"error": err_msg, "_price_counter": None})
        try:
            update_log(log_id, "failed", detail=err_detail)
        except Exception:
            pass
    finally:
        _job_status["running"] = False


async def _execute_job():
    from service.auto_job.scheduler_orchestrator import scheduler_lock, price_done_event
    async with scheduler_lock:
        logger.info("[最高最低价调度] 已获取全局调度锁")
        try:
            await _execute_job_inner()
        finally:
            price_done_event.set()
            logger.info("[最高最低价调度] 已发送完成信号")


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
