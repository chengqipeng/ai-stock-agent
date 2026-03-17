"""
概念板块强弱势定时调度模块

- 概念板块 vs 大盘强弱势：每个交易日 16:30（北京时间）自动触发
- 个股 vs 概念板块强弱势：每个交易日 17:00（北京时间）自动触发
- 项目启动时检查当天是否已完成，未完成则补拉
"""
import asyncio
import json
import logging
import random
import time as _time
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from dao import get_connection
from service.auto_job.kline_data_scheduler import app_ready, is_a_share_trading_day

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


def _already_done_today() -> bool:
    return (
        _job_status.get("last_run_date") == _get_effective_trade_date()
        and _job_status.get("last_success") is True
    )


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一个触发时间：每天16:30 CST"""
    today_trigger = after.replace(hour=16, minute=30, second=0, microsecond=0)
    if after >= today_trigger:
        today_trigger += timedelta(days=1)
    return today_trigger


def _get_effective_trade_date() -> str:
    """获取有效目标交易日：收盘后(>=15:00)用今天，否则用上一个交易日。"""
    now = datetime.now(_CST)
    d = now.date()
    if now.time() >= dtime(15, 0) and is_a_share_trading_day(d):
        return d.isoformat()
    d -= timedelta(days=1)
    while not is_a_share_trading_day(d):
        d -= timedelta(days=1)
    return d.isoformat()


def _batch_check_board_kline_completeness(
    board_codes: list[str], target_date: str
) -> set[str]:
    """批量检查哪些板块的K线已覆盖到 target_date。

    Returns:
        已完整的 board_code 集合（可跳过拉取）
    """
    if not board_codes:
        return set()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        ph = ",".join(["%s"] * len(board_codes))
        cursor.execute(
            f"SELECT board_code, MAX(`date`) as max_d "
            f"FROM concept_board_kline "
            f"WHERE board_code IN ({ph}) "
            f"GROUP BY board_code",
            (*board_codes,),
        )
        complete = set()
        for row in cursor.fetchall():
            if row[1] and str(row[1]) >= target_date:
                complete.add(row[0])
        return complete
    finally:
        cursor.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════
# 核心执行逻辑
# ═══════════════════════════════════════════════════════════════

async def _execute_job_inner():
    """执行概念板块强弱势计算任务"""
    _job_status["running"] = True
    _job_status["error"] = None
    _job_status["board_market_done"] = False
    _job_status["stock_board_done"] = False
    start_time = datetime.now(_CST)
    trade_date = _get_effective_trade_date()
    _job_status["last_run_date"] = trade_date
    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    _job_status["start_time"] = start_time.isoformat()

    logger.info("[概念强弱势调度] ===== 开始执行 trade_date=%s =====", trade_date)

    try:
        # ── 前置阶段: 分批检查+增量拉取板块K线 ──
        _job_status["stage"] = "board_kline_sync"
        _job_status["board_kline_total"] = 0
        _job_status["board_kline_success"] = 0
        _job_status["board_kline_skipped"] = 0
        _job_status["board_kline_failed"] = 0
        _save_persisted_status(_job_status)
        logger.info("[概念强弱势调度] 前置阶段: 分批检查+增量拉取板块K线")

        try:
            from dao.stock_concept_board_dao import get_all_concept_boards
            from dao.concept_board_kline_dao import batch_upsert_klines, get_latest_date
            from service.jqka10.concept_board_kline_10jqka import (
                fetch_board_kline, _resolve_index_code,
            )

            boards = get_all_concept_boards()
            total_boards = len(boards)
            _job_status["board_kline_total"] = total_boards
            bk_success = 0
            bk_skipped = 0
            bk_failed = 0
            bk_rows = 0
            BATCH_SIZE = 10
            JQKA_MAX_RETRIES = 3  # 同花顺最多重试次数

            for batch_start in range(0, total_boards, BATCH_SIZE):
                batch = boards[batch_start:batch_start + BATCH_SIZE]
                batch_codes = [b["board_code"] for b in batch]

                # 批量检查完整性
                complete_codes = await asyncio.get_event_loop().run_in_executor(
                    None, lambda bc=batch_codes: _batch_check_board_kline_completeness(bc, trade_date)
                )

                for board in batch:
                    bc = board["board_code"]
                    bn = board["board_name"]

                    if bc in complete_codes:
                        bk_skipped += 1
                        _job_status["board_kline_skipped"] = bk_skipped
                        _job_status["board_kline_success"] = bk_success
                        _job_status["board_kline_failed"] = bk_failed
                        continue

                    # 需要拉取
                    try:
                        index_code = _resolve_index_code(bc, board)
                        if not index_code:
                            bk_failed += 1
                            _job_status["board_kline_failed"] = bk_failed
                            continue

                        # 同花顺优先，失败3次后标记失败（由后续成分股合成补全）
                        klines = None
                        for jqka_attempt in range(1, JQKA_MAX_RETRIES + 1):
                            try:
                                klines = await fetch_board_kline(index_code, limit=800)
                                break
                            except Exception as je:
                                if jqka_attempt < JQKA_MAX_RETRIES:
                                    wait = 3 * jqka_attempt + random.uniform(1, 3)
                                    logger.warning(
                                        "[概念强弱势调度] 板块K线 %s(%s) 同花顺第%d次失败，%.1f秒后重试: %s",
                                        bc, bn, jqka_attempt, wait, je)
                                    await asyncio.sleep(wait)
                                else:
                                    logger.warning(
                                        "[概念强弱势调度] 板块K线 %s(%s) 同花顺%d次均失败，将由成分股合成补全: %s",
                                        bc, bn, JQKA_MAX_RETRIES, je)

                        if not klines:
                            bk_failed += 1
                            _job_status["board_kline_failed"] = bk_failed
                            continue

                        # 增量：只保留最新日期之后的数据
                        latest = get_latest_date(bc)
                        if latest:
                            klines = [k for k in klines if k["date"] > latest]
                            if not klines:
                                bk_skipped += 1
                                _job_status["board_kline_skipped"] = bk_skipped
                                continue

                        batch_upsert_klines(bc, klines, board_index_code=index_code)
                        bk_success += 1
                        bk_rows += len(klines)
                        _job_status["board_kline_success"] = bk_success

                    except Exception as e:
                        bk_failed += 1
                        _job_status["board_kline_failed"] = bk_failed
                        logger.warning("[概念强弱势调度] 板块K线 %s(%s) 异常: %s", bc, bn, e)

                    _time.sleep(0.5 + random.uniform(0, 0.2))

            _job_status["board_kline_rows"] = bk_rows
            logger.info(
                "[概念强弱势调度] 板块K线完成: 总%d 成功%d 跳过%d 失败%d 写入%d条",
                total_boards, bk_success, bk_skipped, bk_failed, bk_rows,
            )
        except Exception as e:
            logger.error("[概念强弱势调度] 板块K线拉取异常: %s", e, exc_info=True)
        _save_persisted_status(_job_status)

        # ── 前置阶段补充: 用成分股K线合成缺失的板块当日K线 ──
        try:
            from service.analysis.board_kline_fallback import synthesize_missing_board_klines
            fallback_result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: synthesize_missing_board_klines(trade_date)
            )
            fb_synthesized = fallback_result.get("synthesized", 0)
            fb_missing = fallback_result.get("missing", 0)
            if fb_synthesized > 0:
                logger.info("[概念强弱势调度] 板块K线补全: 缺失%d 合成%d 跳过%d",
                            fb_missing, fb_synthesized,
                            fallback_result.get("skipped", 0))
                _job_status["board_kline_success"] = _job_status.get("board_kline_success", 0) + fb_synthesized
            elif fb_missing > 0:
                logger.info("[概念强弱势调度] 板块K线补全: 缺失%d个但均无法合成", fb_missing)
        except Exception as e:
            logger.error("[概念强弱势调度] 板块K线补全异常: %s", e, exc_info=True)
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


async def _execute_job(manual=False):
    from service.auto_job.scheduler_orchestrator import scheduler_lock, manual_semaphore, concept_strength_done_event
    if manual:
        _job_status["running"] = True
        _job_status["error"] = "等待手动调度槽位..."
        logger.info("[概念强弱势调度] 手动触发，等待调度槽位")
        async with manual_semaphore:
            _job_status["error"] = None
            logger.info("[概念强弱势调度] 已获取手动调度槽位")
            try:
                await _execute_job_inner()
            finally:
                concept_strength_done_event.set()
    else:
        async with scheduler_lock:
            logger.info("[概念强弱势调度] 已获取全局调度锁")
            try:
                await _execute_job_inner()
            finally:
                concept_strength_done_event.set()
                logger.info("[概念强弱势调度] 已发送完成信号")


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
                logger.info("[概念强弱势调度] 今日 %s 已执行过，跳过", _get_effective_trade_date())
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
