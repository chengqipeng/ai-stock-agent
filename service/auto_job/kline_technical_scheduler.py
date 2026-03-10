"""
技术打分定时调度模块

- 在日线数据调度执行成功后自动触发
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

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_RETRY_INTERVAL = 300
_MAX_RETRY = 3

# ─────────── 日线完成信号 ───────────
# kline_data_scheduler 执行成功后 set，各下游调度器等待此信号
kline_done_event = asyncio.Event()          # 技术打分用
kline_done_event_for_kscore = asyncio.Event()  # K线初筛用
kline_done_event_for_dbcheck = asyncio.Event()  # 数据异常检测用

# ─────────── 状态持久化 ───────────
_SCORE_STATUS_FILE = Path(__file__).parent.parent.parent / "data_results" / ".score_scheduler_status.json"


def _load_persisted_status() -> dict:
    try:
        if _SCORE_STATUS_FILE.exists():
            data = json.loads(_SCORE_STATUS_FILE.read_text(encoding="utf-8"))
            logger.info("[技术打分调度] 从文件恢复状态: last_run_date=%s", data.get("last_run_date"))
            return data
    except Exception as e:
        logger.warning("[技术打分调度] 读取状态文件失败: %s", e)
    return {}


def _save_persisted_status(status: dict):
    try:
        _SCORE_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_run_time": status.get("last_run_time"),
            "last_run_date": status.get("last_run_date"),
            "last_success": status.get("last_success"),
            "running": status.get("running", False),
            "error": status.get("error"),
        }
        _SCORE_STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[技术打分调度] 写入状态文件失败: %s", e)


# ─────────── 全局状态 ───────────
_persisted = _load_persisted_status()

# 如果上次持久化时 running=True，说明进程被中断，重置为失败状态
if _persisted.get("running"):
    logger.warning("[技术打分调度] 检测到上次运行被中断，重置状态为失败")
    _persisted["running"] = False
    _persisted["last_success"] = False
    _persisted["error"] = "上次运行被中断（进程重启）"

_job_status = {
    "last_run_time": _persisted.get("last_run_time"),
    "last_run_date": _persisted.get("last_run_date"),
    "last_success": _persisted.get("last_success"),
    "score_total": 0,
    "score_success": 0,
    "score_failed": 0,
    "running": False,
    "error": _persisted.get("error"),
}


def get_score_job_status() -> dict:
    status = dict(_job_status)
    if status.get("running"):
        sc = status.get("_score_counter")
        if sc:
            status["score_total"] = sc.get("total", 0)
            status["score_success"] = sc.get("success", 0)
            status["score_failed"] = sc.get("failed", 0)
    status.pop("_score_counter", None)
    return status


def _already_done_today() -> bool:
    now_date = datetime.now(_CST).date().isoformat()
    return _job_status["last_run_date"] == now_date and _job_status["last_success"] is True


async def _execute_job():
    """执行一次技术打分"""
    from service.batch_technical_score.batch_technical_score import (
        analyze_stock,
        write_result,
        OUTPUT_PATH,
    )
    from dao.stock_technical_score_dao import (
        get_continuous_analysis_batches,
        get_batch_stock_list,
        save_score_results,
        get_today_scored_codes,
    )
    from dao.scheduler_log_dao import insert_log, update_log

    _job_status["running"] = True
    _job_status["error"] = None
    _job_status["start_time"] = datetime.now(_CST).isoformat()
    today_str = datetime.now(_CST).date().isoformat()
    started_at = datetime.now(_CST)
    _save_persisted_status(_job_status)  # 持久化 running=True，便于重启后检测中断
    log_id = insert_log("技术打分", started_at)
    attempt = 0
    total_skipped = 0

    try:
        while True:
            attempt += 1
            logger.info("[技术打分调度] 开始执行 %s（第%d次尝试）", today_str, attempt)

            score_counter = {"total": 0, "success": 0, "failed": 0}
            _job_status["_score_counter"] = score_counter
            total_skipped = 0

            try:
                batches = get_continuous_analysis_batches()
                if not batches:
                    logger.info("[技术打分调度] 没有标记为持续分析的批次，跳过")
                    score_counter["total"] = 0
                    score_counter["success"] = 0
                    score_counter["failed"] = 0
                else:
                    for batch in batches:
                        batch_id = batch['id']
                        batch_name = batch.get('batch_name', str(batch_id))
                        stocks = get_batch_stock_list(batch_id)
                        if not stocks:
                            continue

                        # 断点续传：查询今日已完成打分的股票，跳过
                        already_done = get_today_scored_codes(batch_id)
                        remaining = [s for s in stocks if s['stock_code'] not in already_done]
                        skipped = len(stocks) - len(remaining)
                        total_skipped += skipped

                        score_counter["total"] += len(stocks)
                        score_counter["success"] += skipped
                        if skipped > 0:
                            logger.info("[技术打分调度] 批次 %s 共 %d 只，今日已完成 %d 只，剩余 %d 只",
                                        batch_name, len(stocks), skipped, len(remaining))

                        results = []
                        total = len(stocks)
                        for i, s in enumerate(remaining, skipped + 1):
                            r = await analyze_stock(s['stock_name'], s['stock_code'], i, total)
                            if r:
                                results.append(r)
                                score_counter["success"] += 1
                            else:
                                score_counter["failed"] += 1

                        if results:
                            write_result(results, OUTPUT_PATH)
                            save_score_results(results, batch_id)
                            logger.info("[技术打分调度] 批次 %s 打分结果已保存 (batch_id=%d)", batch_name, batch_id)

            except Exception as e:
                logger.error("[技术打分调度] 执行异常: %s", e, exc_info=True)

            now_str = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
            total_failed = score_counter.get("failed", 0)
            all_success = total_failed == 0

            _job_status.update({
                "last_run_time": now_str,
                "last_run_date": today_str,
                "last_success": all_success,
                "score_total": score_counter.get("total", 0),
                "score_success": score_counter.get("success", 0),
                "score_failed": total_failed,
                "_score_counter": None,
            })

            _save_persisted_status(_job_status)

            if all_success:
                logger.info("[技术打分调度] 全部成功 %d/%d",
                            score_counter.get("success", 0), score_counter.get("total", 0))
                detail = (f"总{score_counter['total']}只 成功{score_counter['success']}只 "
                          f"失败{total_failed}只 跳过(已完成){total_skipped}只 重试{attempt}次")
                update_log(log_id, "success", score_counter["total"], score_counter["success"],
                           total_failed, total_skipped, detail)
                break

            logger.warning("[技术打分调度] 有 %d 只失败，%d秒后重试", total_failed, _RETRY_INTERVAL)
            if attempt >= _MAX_RETRY:
                err_msg = f"达到最大重试次数({_MAX_RETRY})，仍有{total_failed}只失败"
                logger.error("[技术打分调度] %s", err_msg)
                _job_status["error"] = err_msg
                detail = (f"总{score_counter['total']}只 成功{score_counter['success']}只 "
                          f"失败{total_failed}只 跳过(已完成){total_skipped}只 重试{attempt}次 (达到上限)")
                try:
                    update_log(log_id, "failed", score_counter["total"], score_counter["success"],
                               total_failed, total_skipped, detail)
                except Exception:
                    pass
                break
            await asyncio.sleep(_RETRY_INTERVAL)

    except Exception as e:
        import traceback as _tb
        err_msg = f"任务异常终止: {type(e).__name__}: {e}"
        err_detail = f"{err_msg}\n{_tb.format_exc()}"
        logger.error("[技术打分调度] %s", err_msg, exc_info=True)
        _job_status.update({"error": err_msg, "_score_counter": None})
        try:
            update_log(log_id, "failed", detail=err_detail)
        except Exception:
            pass
    finally:
        _job_status["running"] = False
        _save_persisted_status(_job_status)


async def _scheduler_loop():
    """调度主循环：等待日线完成信号后执行"""
    while True:
        try:
            # 等待日线完成信号
            await kline_done_event.wait()
            kline_done_event.clear()

            if _already_done_today():
                logger.info("[技术打分调度] 今日已完成，跳过")
                continue

            logger.info("[技术打分调度] 收到日线完成信号，开始执行技术打分")
            await _execute_job()

        except asyncio.CancelledError:
            logger.info("[技术打分调度] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[技术打分调度] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_score_scheduler():
    """启动技术打分调度器"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[技术打分调度] 应用已就绪，调度器开始工作")

        # 启动时检查：如果日线今天已经执行完成，但技术打分还没执行，则立即补拉
        from service.auto_job.kline_data_scheduler import get_job_status as get_kline_status
        kline_status = get_kline_status()
        now = datetime.now(_CST)
        today_str = now.date().isoformat()
        if (kline_status.get("last_run_date") == today_str
                and not kline_status.get("running")
                and not _already_done_today()):
            logger.info("[技术打分调度] 启动补拉：日线今日已完成但技术打分未执行，将在5秒后执行")
            async def _delayed_execute():
                await asyncio.sleep(5)
                await _execute_job()
            asyncio.create_task(_delayed_execute())

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[技术打分调度] 调度器已注册，等待应用就绪")
