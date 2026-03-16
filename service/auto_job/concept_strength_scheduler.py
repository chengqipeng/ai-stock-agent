"""
概念板块强弱势定时调度模块

- 概念板块 vs 大盘强弱势：每个交易日 16:30（北京时间）自动触发
- 个股 vs 概念板块强弱势：每个交易日 17:00（北京时间）自动触发
- 项目启动时检查当天是否已完成，未完成则补拉
"""
import asyncio
import json
import logging
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from service.auto_job.kline_data_scheduler import app_ready

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent

# ─────────── 状态持久化 ───────────
_STATUS_FILE = _project_root / "data_results" / ".concept_strength_scheduler_status.json"


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
        logger.warning("[概念强弱势调度] 状态持久化失败: %s", e)


# ─────────── 全局状态 ───────────
_job_status = {
    "running": False,
    "last_run_date": None,
    "last_run_time": None,
    "last_success": None,
    "error": None,
    "start_time": None,
    # 板块 vs 大盘
    "board_market_total": 0,
    "board_market_success": 0,
    "board_market_failed": 0,
    "board_market_done": False,
    # 个股 vs 板块
    "stock_board_total": 0,
    "stock_board_success": 0,
    "stock_board_failed": 0,
    "stock_board_done": False,
    # 板块K线增量拉取
    "board_kline_total": 0,
    "board_kline_success": 0,
    "board_kline_skipped": 0,
    "board_kline_failed": 0,
    "board_kline_rows": 0,
    # 当前阶段
    "stage": "",  # "board_kline_sync" | "board_market" | "stock_board" | ""
}
_persisted = _load_persisted_status()
_job_status.update(_persisted)


def get_concept_strength_job_status() -> dict:
    return dict(_job_status)


def _is_trading_day(d: date) -> bool:
    """简单判断A股交易日（周一至周五）"""
    return d.weekday() < 5


def _get_trade_date() -> str:
    """获取当天交易日日期"""
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
    """计算下一个触发时间：每天16:30 CST"""
    today_trigger = after.replace(hour=16, minute=30, second=0, microsecond=0)
    if after >= today_trigger:
        today_trigger += timedelta(days=1)
    return today_trigger


# ═══════════════════════════════════════════════════════════════
# 核心执行逻辑
# ═══════════════════════════════════════════════════════════════

async def _execute_job():
    """执行概念板块强弱势计算任务"""
    _job_status["running"] = True
    _job_status["error"] = None
    _job_status["board_market_done"] = False
    _job_status["stock_board_done"] = False
    start_time = datetime.now(_CST)
    trade_date = _get_trade_date()
    _job_status["last_run_date"] = trade_date
    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    _job_status["start_time"] = start_time.isoformat()

    logger.info("[概念强弱势调度] ===== 开始执行 trade_date=%s =====", trade_date)

    try:
        # ── 前置阶段: 增量拉取板块K线 ──
        _job_status["stage"] = "board_kline_sync"
        _save_persisted_status(_job_status)
        logger.info("[概念强弱势调度] 前置阶段: 增量拉取板块K线数据")
        try:
            from service.jqka10.concept_board_kline_10jqka import fetch_and_save_all_boards_kline
            kline_result = await fetch_and_save_all_boards_kline(
                limit=800, delay=0.5, incremental=True,
            )
            _job_status["board_kline_total"] = kline_result.get("total_boards", 0)
            _job_status["board_kline_success"] = kline_result.get("success", 0)
            _job_status["board_kline_skipped"] = kline_result.get("skipped", 0)
            _job_status["board_kline_failed"] = kline_result.get("failed", 0)
            _job_status["board_kline_rows"] = kline_result.get("total_klines", 0)
            logger.info(
                "[概念强弱势调度] 板块K线增量拉取完成: 总%d 成功%d 跳过%d 失败%d 写入%d条",
                kline_result.get("total_boards", 0), kline_result.get("success", 0),
                kline_result.get("skipped", 0), kline_result.get("failed", 0),
                kline_result.get("total_klines", 0),
            )
        except Exception as e:
            logger.error("[概念强弱势调度] 板块K线增量拉取异常(不影响后续计算): %s", e, exc_info=True)
        _save_persisted_status(_job_status)

        # ── 阶段1: 概念板块 vs 大盘强弱势 ──
        _job_status["stage"] = "board_market"
        _job_status["board_market_total"] = 0
        _job_status["board_market_success"] = 0
        _job_status["board_market_failed"] = 0
        _save_persisted_status(_job_status)

        logger.info("[概念强弱势调度] 阶段1: 计算概念板块 vs 大盘强弱势")
        try:
            from service.analysis.concept_board_market_strength import compute_and_save_all_boards as compute_board_market

            def _board_market_progress(total, success, failed):
                _job_status["board_market_total"] = total
                _job_status["board_market_success"] = success
                _job_status["board_market_failed"] = failed

            summary = await asyncio.get_event_loop().run_in_executor(
                None, lambda: compute_board_market(days=60, progress_callback=_board_market_progress)
            )
            _job_status["board_market_total"] = summary.get("total", 0)
            _job_status["board_market_success"] = summary.get("success", 0)
            _job_status["board_market_failed"] = summary.get("failed", 0)
            _job_status["board_market_done"] = True
            logger.info("[概念强弱势调度] 板块大盘强弱势完成: 总%d 成功%d 失败%d",
                        summary.get("total", 0), summary.get("success", 0), summary.get("failed", 0))
        except Exception as e:
            _job_status["board_market_failed"] = -1
            logger.error("[概念强弱势调度] 板块大盘强弱势计算异常: %s", e, exc_info=True)

        _save_persisted_status(_job_status)

        # ── 阶段2: 个股 vs 概念板块强弱势 ──
        _job_status["stage"] = "stock_board"
        _job_status["stock_board_total"] = 0
        _job_status["stock_board_success"] = 0
        _job_status["stock_board_failed"] = 0
        _save_persisted_status(_job_status)

        logger.info("[概念强弱势调度] 阶段2: 计算个股 vs 概念板块强弱势")
        try:
            from service.analysis.concept_stock_strength import compute_and_save_all_boards as compute_stock_board

            def _stock_board_progress(total, success, failed):
                _job_status["stock_board_total"] = total
                _job_status["stock_board_success"] = success
                _job_status["stock_board_failed"] = failed

            summary = await asyncio.get_event_loop().run_in_executor(
                None, lambda: compute_stock_board(days=60, progress_callback=_stock_board_progress)
            )
            _job_status["stock_board_total"] = summary.get("total_boards", summary.get("total", 0))
            _job_status["stock_board_success"] = summary.get("success_boards", summary.get("success", 0))
            _job_status["stock_board_failed"] = summary.get("failed_boards", summary.get("failed", 0))
            _job_status["stock_board_done"] = True
            logger.info("[概念强弱势调度] 个股板块强弱势完成: 总%d 成功%d 失败%d",
                        _job_status["stock_board_total"], _job_status["stock_board_success"], _job_status["stock_board_failed"])
        except Exception as e:
            _job_status["stock_board_failed"] = -1
            logger.error("[概念强弱势调度] 个股板块强弱势计算异常: %s", e, exc_info=True)

        _job_status["last_success"] = (
            _job_status["board_market_done"] and _job_status["stock_board_done"]
        )
        elapsed = (datetime.now(_CST) - start_time).total_seconds()
        logger.info(
            "[概念强弱势调度] ===== 执行完成，耗时%.1f秒 板块大盘(%d/%d) 个股板块(%d/%d) =====",
            elapsed,
            _job_status["board_market_success"], _job_status["board_market_total"],
            _job_status["stock_board_success"], _job_status["stock_board_total"],
        )

    except Exception as e:
        _job_status["last_success"] = False
        _job_status["error"] = str(e)
        logger.error("[概念强弱势调度] 执行异常: %s", e, exc_info=True)

    finally:
        _job_status["running"] = False
        _job_status["stage"] = ""
        _job_status["start_time"] = None
        _save_persisted_status(_job_status)


# ═══════════════════════════════════════════════════════════════
# 调度循环
# ═══════════════════════════════════════════════════════════════

async def _scheduler_loop():
    """调度主循环：每天16:30 CST触发"""
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_seconds = (next_dt - now).total_seconds()

            if wait_seconds > 0:
                logger.info("[概念强弱势调度] 下次执行时间: %s (%.0f秒后)",
                            next_dt.strftime("%Y-%m-%d %H:%M"), wait_seconds)
                await asyncio.sleep(wait_seconds)

            if _already_done_today():
                logger.info("[概念强弱势调度] 今日 %s 已执行过，跳过", _get_trade_date())
                await asyncio.sleep(60)
                continue

            await _execute_job()
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("[概念强弱势调度] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[概念强弱势调度] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_concept_strength_scheduler():
    """启动概念板块强弱势调度器"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[概念强弱势调度] 应用已就绪，调度器开始工作")

        if not _already_done_today():
            logger.info("[概念强弱势调度] 启动补拉：将在15秒后执行")

            async def _delayed_execute():
                await asyncio.sleep(15)
                await _execute_job()

            asyncio.create_task(_delayed_execute())

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[概念强弱势调度] 调度器已注册，等待应用就绪")
