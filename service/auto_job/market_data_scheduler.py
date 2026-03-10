"""
盘后数据定时调度模块（分时数据 + 盘口数据 + 龙虎榜）

- 每个A股交易日16:00自动触发
- 项目启动时检查当天是否已完成，未完成则立即补拉
- 状态通过API暴露给前端展示
- 执行状态持久化到本地文件，重启后不会重复全量拉取
"""
import asyncio
import json
import logging
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
from chinese_calendar import is_workday

from common.constants.stocks_data import MAIN_STOCK
from common.utils.stock_info_utils import get_stock_info_by_code
from dao import get_connection
from dao.stock_time_data_dao import create_time_data_table, batch_upsert_time_data
from dao.stock_order_book_dao import create_order_book_table, upsert_order_book
from dao.stock_dragon_tiger_dao import create_dragon_tiger_table, batch_upsert_dragon_tiger
from service.jqka10.stock_time_kline_data_10jqka import get_stock_time_kline_10jqka
from service.jqka10.stock_order_book_10jqka import get_order_book_10jqka
from service.jqka10.stock_dragon_tiger_10jqka import fetch_dragon_tiger_all_pages

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent
_INDEX_CODES = {s['code'] for s in MAIN_STOCK}

# ─────────── 状态持久化 ───────────
_STATUS_FILE = _project_root / "data_results" / ".market_data_scheduler_status.json"


def _load_persisted_status() -> dict:
    try:
        if _STATUS_FILE.exists():
            data = json.loads(_STATUS_FILE.read_text(encoding="utf-8"))
            logger.info("[盘后数据调度] 从文件恢复状态: last_run_date=%s", data.get("last_run_date"))
            return data
    except Exception as e:
        logger.warning("[盘后数据调度] 读取状态文件失败: %s", e)
    return {}


def _save_persisted_status(status: dict):
    try:
        _STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_run_time": status.get("last_run_time"),
            "last_run_date": status.get("last_run_date"),
            "last_success": status.get("last_success"),
        }
        _STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[盘后数据调度] 写入状态文件失败: %s", e)


# ─────────── 启动就绪信号 ───────────
from service.auto_job.kline_data_scheduler import app_ready

# ─────────── 全局状态 ───────────
_persisted = _load_persisted_status()

_job_status = {
    "last_run_time": _persisted.get("last_run_time"),
    "last_run_date": _persisted.get("last_run_date"),
    "last_success": _persisted.get("last_success"),
    "time_data_total": 0,
    "time_data_success": 0,
    "time_data_failed": 0,
    "order_book_total": 0,
    "order_book_success": 0,
    "order_book_failed": 0,
    "dragon_tiger_count": 0,
    "running": False,
    "error": None,
}


def get_market_data_job_status() -> dict:
    return dict(_job_status)


# ─────────── 股票列表 ───────────

def _build_stock_list() -> list[dict]:
    """构建股票列表（复用 kline_data_scheduler 的逻辑）"""
    from service.auto_job.kline_data_scheduler import load_stocks_from_score_list
    stocks = load_stocks_from_score_list()
    main_codes = {s['code'] for s in stocks}
    stocks += [s for s in MAIN_STOCK if s['code'] not in main_codes]
    return stocks


def is_a_share_trading_day(d: date) -> bool:
    return d.weekday() < 5 and is_workday(d)


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一次触发时间：下一个交易日的16:00"""
    d = after.date()
    trigger_time = dtime(16, 0)

    if is_a_share_trading_day(d) and after.time() < trigger_time:
        return datetime.combine(d, trigger_time, tzinfo=_CST)

    d += timedelta(days=1)
    while not is_a_share_trading_day(d):
        d += timedelta(days=1)
    return datetime.combine(d, trigger_time, tzinfo=_CST)


def _already_done_today() -> bool:
    today_str = datetime.now(_CST).date().isoformat()
    return _job_status.get("last_run_date") == today_str and _job_status.get("last_success")


# ─────────── 分时数据拉取 ───────────

async def _fetch_time_data_for_stock(stock_code_normalize: str, trade_date: str, counter: dict):
    """拉取单只股票的分时数据并入库"""
    stock_code = stock_code_normalize.split(".")[0]
    max_retries = 2
    for attempt in range(1, max_retries + 1):
        try:
            stock_info = get_stock_info_by_code(stock_code_normalize)
            if not stock_info:
                logger.warning("[分时数据] 无法获取股票信息: %s", stock_code_normalize)
                counter["failed"] += 1
                return

            data_list = await get_stock_time_kline_10jqka(stock_info)
            if not data_list:
                logger.debug("[分时数据] %s 无分时数据", stock_code)
                counter["success"] += 1
                return

            conn = get_connection()
            cursor = conn.cursor()
            try:
                batch_upsert_time_data(stock_code, trade_date, data_list, cursor=cursor)
                conn.commit()
                counter["success"] += 1
            finally:
                cursor.close()
                conn.close()
            return

        except Exception as e:
            if attempt < max_retries:
                logger.warning("[分时数据] %s 第%d次异常，2秒后重试: %s", stock_code, attempt, e)
                await asyncio.sleep(2)
            else:
                logger.error("[分时数据] %s 异常: %s", stock_code, e)
                counter["failed"] += 1


# ─────────── 盘口数据拉取 ───────────

async def _fetch_order_book_for_stock(stock_code_normalize: str, trade_date: str, counter: dict):
    """拉取单只股票的盘口数据并入库"""
    stock_code = stock_code_normalize.split(".")[0]
    max_retries = 2
    for attempt in range(1, max_retries + 1):
        try:
            stock_info = get_stock_info_by_code(stock_code_normalize)
            if not stock_info:
                counter["failed"] += 1
                return

            data = await get_order_book_10jqka(stock_info)
            if not data:
                counter["success"] += 1
                return

            conn = get_connection()
            cursor = conn.cursor()
            try:
                upsert_order_book(stock_code, trade_date, data, cursor=cursor)
                conn.commit()
                counter["success"] += 1
            finally:
                cursor.close()
                conn.close()
            return

        except Exception as e:
            if attempt < max_retries:
                logger.warning("[盘口数据] %s 第%d次异常，2秒后重试: %s", stock_code, attempt, e)
                await asyncio.sleep(2)
            else:
                logger.error("[盘口数据] %s 异常: %s", stock_code, e)
                counter["failed"] += 1


# ─────────── 龙虎榜拉取 ───────────

async def _fetch_dragon_tiger(trade_date: str) -> tuple[int, bool]:
    """拉取龙虎榜数据并入库，返回 (记录数, 是否成功)"""
    try:
        rows = await fetch_dragon_tiger_all_pages(trade_date=trade_date)
        if not rows:
            logger.info("[龙虎榜] %s 无龙虎榜数据", trade_date)
            return 0, True

        conn = get_connection()
        cursor = conn.cursor()
        try:
            batch_upsert_dragon_tiger(trade_date, rows, cursor=cursor)
            conn.commit()
            logger.info("[龙虎榜] %s 写入%d条", trade_date, len(rows))
            return len(rows), True
        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error("[龙虎榜] %s 异常: %s", trade_date, e)
        return 0, False


# ─────────── 主执行逻辑 ───────────

async def _execute_job():
    """执行盘后数据拉取任务"""
    _job_status["running"] = True
    _job_status["error"] = None
    start_time = datetime.now(_CST)
    _job_status["start_time"] = start_time.isoformat()
    today_str = start_time.date().isoformat()
    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    _job_status["last_run_date"] = today_str

    logger.info("[盘后数据调度] ===== 开始执行 %s =====", today_str)

    try:
        # 1. 建表
        conn = get_connection()
        cursor = conn.cursor()
        try:
            create_time_data_table(cursor)
            create_order_book_table(cursor)
            create_dragon_tiger_table(cursor)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        # 2. 构建股票列表（排除指数）
        all_stocks = _build_stock_list()
        stocks = [s for s in all_stocks if s["code"] not in _INDEX_CODES]
        total = len(stocks)

        # 3. 拉取分时数据
        logger.info("[盘后数据调度] 开始拉取分时数据，共%d只股票", total)
        _job_status["time_data_total"] = total
        _job_status["time_data_success"] = 0
        _job_status["time_data_failed"] = 0
        time_counter = {"success": 0, "failed": 0}
        sem = asyncio.Semaphore(3)

        async def _time_task(s):
            async with sem:
                await _fetch_time_data_for_stock(s["code"], today_str, time_counter)
                _job_status["time_data_success"] = time_counter["success"]
                _job_status["time_data_failed"] = time_counter["failed"]
                await asyncio.sleep(0.5)

        await asyncio.gather(*[_time_task(s) for s in stocks])
        logger.info("[盘后数据调度] 分时数据完成: 成功%d 失败%d", time_counter["success"], time_counter["failed"])

        # 4. 拉取盘口数据
        logger.info("[盘后数据调度] 开始拉取盘口数据，共%d只股票", total)
        _job_status["order_book_total"] = total
        _job_status["order_book_success"] = 0
        _job_status["order_book_failed"] = 0
        ob_counter = {"success": 0, "failed": 0}

        async def _ob_task(s):
            async with sem:
                await _fetch_order_book_for_stock(s["code"], today_str, ob_counter)
                _job_status["order_book_success"] = ob_counter["success"]
                _job_status["order_book_failed"] = ob_counter["failed"]
                await asyncio.sleep(0.5)

        await asyncio.gather(*[_ob_task(s) for s in stocks])
        logger.info("[盘后数据调度] 盘口数据完成: 成功%d 失败%d", ob_counter["success"], ob_counter["failed"])

        # 5. 拉取龙虎榜
        logger.info("[盘后数据调度] 开始拉取龙虎榜")
        dt_count, dt_ok = await _fetch_dragon_tiger(today_str)
        _job_status["dragon_tiger_count"] = dt_count
        logger.info("[盘后数据调度] 龙虎榜完成: %d条", dt_count)

        # 判断整体是否成功：分时/盘口允许部分失败，但龙虎榜失败则标记不成功
        has_failures = time_counter["failed"] > 0 or ob_counter["failed"] > 0 or not dt_ok
        _job_status["last_success"] = not has_failures
        if has_failures:
            parts = []
            if time_counter["failed"] > 0:
                parts.append(f"分时失败{time_counter['failed']}")
            if ob_counter["failed"] > 0:
                parts.append(f"盘口失败{ob_counter['failed']}")
            if not dt_ok:
                parts.append("龙虎榜拉取失败")
            _job_status["error"] = "部分失败: " + ", ".join(parts)
            logger.warning("[盘后数据调度] 部分任务失败: %s", _job_status["error"])
        elapsed = (datetime.now(_CST) - start_time).total_seconds()
        logger.info("[盘后数据调度] ===== 执行完成，耗时%.1f秒 =====", elapsed)

    except Exception as e:
        _job_status["last_success"] = False
        _job_status["error"] = str(e)
        logger.error("[盘后数据调度] 执行异常: %s", e, exc_info=True)

    finally:
        _job_status["running"] = False
        _save_persisted_status(_job_status)


# ─────────── 调度循环 ───────────

async def _scheduler_loop():
    """调度主循环：计算下次触发时间，sleep到点后执行"""
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_seconds = (next_dt - now).total_seconds()

            if wait_seconds > 0:
                logger.info("[盘后数据调度] 下次执行时间: %s (%.0f秒后)", next_dt.strftime("%Y-%m-%d %H:%M"), wait_seconds)
                await asyncio.sleep(wait_seconds)

            trigger_date = datetime.now(_CST).date()
            if not is_a_share_trading_day(trigger_date):
                continue

            if _already_done_today():
                logger.info("[盘后数据调度] 今日 %s 已执行过，跳过", trigger_date)
                await asyncio.sleep(60)
                continue

            await _execute_job()
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("[盘后数据调度] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[盘后数据调度] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_market_data_scheduler():
    """启动盘后数据调度器"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[盘后数据调度] 应用已就绪，调度器开始工作")

        now = datetime.now(_CST)
        today = now.date()

        # 启动时检查：如果当天是交易日、已过16:00、且今天还没执行过 → 补拉
        if is_a_share_trading_day(today) and now.time() >= dtime(16, 0) and not _already_done_today():
            logger.info("[盘后数据调度] 启动补拉：今天是交易日且已过16:00，将在10秒后执行")
            async def _delayed_execute():
                await asyncio.sleep(10)
                await _execute_job()
            asyncio.create_task(_delayed_execute())

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[盘后数据调度] 调度器已注册，等待应用就绪")
