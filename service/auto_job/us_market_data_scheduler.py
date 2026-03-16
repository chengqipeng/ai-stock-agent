"""
海外市场数据定时调度模块

- 每个交易日 06:00（北京时间，对应美东收盘后）自动触发
- 拉取美股指数日K线、全球指数行情、中概股/知名美股/互联网中国涨幅榜
- 拉取美股半导体龙头个股日K线（18只）+ SOXX半导体ETF
- 所有按天存储的数据直接覆盖（upsert）
- 项目启动时检查当天是否已完成，未完成则补拉
"""
import asyncio
import json
import logging
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from dao import get_connection
from dao.us_market_dao import (
    create_us_market_tables,
    batch_upsert_index_kline,
    batch_upsert_global_index_realtime,
    batch_upsert_stock_ranking,
    batch_upsert_stock_kline,
)
from service.auto_job.kline_data_scheduler import app_ready
from service.eastmoney.indices.us_market_indices import (
    get_us_index_day_kline,
    get_us_index_realtime_all,
    get_americas_main_indices,
    get_europe_indices,
    get_asia_indices,
    get_australia_indices,
    get_china_concept_stock_ranking,
    get_famous_us_stock_ranking,
    get_internet_china_stock_ranking,
    US_INDEX_MAP,
)
from service.eastmoney.indices.us_stock_kline import (
    get_us_stock_day_kline,
    get_sox_index_day_kline,
    US_SEMI_STOCK_MAP,
)

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent

# ─────────── 状态持久化 ───────────
_STATUS_FILE = _project_root / "data_results" / ".us_market_scheduler_status.json"


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
        logger.warning("[海外数据调度] 状态持久化失败: %s", e)


# ─────────── 全局状态 ───────────
_job_status = {
    "running": False,
    "last_run_date": None,
    "last_run_time": None,
    "last_success": None,
    "error": None,
    "kline_count": 0,
    "index_realtime_count": 0,
    "ranking_count": 0,
    "us_stock_kline_count": 0,
}
_persisted = _load_persisted_status()
_job_status.update(_persisted)


def get_us_market_job_status() -> dict:
    return dict(_job_status)


def _is_us_trading_day(d: date) -> bool:
    """简单判断美股交易日（周一至周五），不含美国节假日"""
    return d.weekday() < 5


def _get_us_trade_date() -> str:
    """获取最近一个美股交易日日期（北京时间06:00前视为前一天的交易日）"""
    now = datetime.now(_CST)
    d = now.date()
    # 北京时间06:00之前，美股当天可能还在交易或刚收盘，应取前一天
    if now.time() < dtime(6, 0):
        d -= timedelta(days=1)
    # 如果是周末则回退到周五
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def _already_done_today() -> bool:
    return _job_status.get("last_run_date") == _get_us_trade_date() and _job_status.get("last_success") is True


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一个触发时间：每天06:00 CST"""
    today_trigger = after.replace(hour=6, minute=0, second=0, microsecond=0)
    if after >= today_trigger:
        today_trigger += timedelta(days=1)
    return today_trigger


# ═══════════════════════════════════════════════════════════════
# 核心执行逻辑
# ═══════════════════════════════════════════════════════════════

async def _execute_job_inner():
    """执行海外市场数据拉取任务"""
    _job_status["running"] = True
    _job_status["error"] = None
    start_time = datetime.now(_CST)
    trade_date = _get_us_trade_date()
    _job_status["last_run_date"] = trade_date
    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

    logger.info("[海外数据调度] ===== 开始执行 trade_date=%s =====", trade_date)

    try:
        # 0. 建表
        conn = get_connection()
        cursor = conn.cursor()
        try:
            create_us_market_tables(cursor)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        # 1. 美股指数日K线
        kline_total = 0
        for code in US_INDEX_MAP:
            try:
                klines = await get_us_index_day_kline(code, limit=120)
                conn = get_connection()
                cursor = conn.cursor()
                try:
                    batch_upsert_index_kline(cursor, code, klines)
                    conn.commit()
                    kline_total += len(klines)
                    logger.info("[海外数据调度] %s 日K线写入 %d 条", code, len(klines))
                finally:
                    cursor.close()
                    conn.close()
            except Exception as e:
                logger.error("[海外数据调度] %s 日K线失败: %s", code, e)
            await asyncio.sleep(0.5)
        _job_status["kline_count"] = kline_total

        # 2. 全球指数行情（美洲/欧洲/亚洲/澳洲）
        realtime_total = 0
        region_tasks = [
            ("americas", get_americas_main_indices),
            ("europe", get_europe_indices),
            ("asia", get_asia_indices),
            ("australia", get_australia_indices),
        ]
        # 同时拉取美股主要指数实时行情
        try:
            us_realtime = await get_us_index_realtime_all()
            conn = get_connection()
            cursor = conn.cursor()
            try:
                batch_upsert_global_index_realtime(cursor, "americas", trade_date, us_realtime)
                conn.commit()
                realtime_total += len(us_realtime)
                logger.info("[海外数据调度] 美股主要指数实时行情写入 %d 条", len(us_realtime))
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            logger.error("[海外数据调度] 美股主要指数实时行情失败: %s", e)

        for region, fetch_fn in region_tasks:
            try:
                items = await fetch_fn()
                conn = get_connection()
                cursor = conn.cursor()
                try:
                    batch_upsert_global_index_realtime(cursor, region, trade_date, items)
                    conn.commit()
                    realtime_total += len(items)
                    logger.info("[海外数据调度] %s 指数行情写入 %d 条", region, len(items))
                finally:
                    cursor.close()
                    conn.close()
            except Exception as e:
                logger.error("[海外数据调度] %s 指数行情失败: %s", region, e)
            await asyncio.sleep(0.3)
        _job_status["index_realtime_count"] = realtime_total

        # 3. 涨幅榜（中国概念股/知名美股/互联网中国）
        ranking_total = 0
        ranking_tasks = [
            ("china_concept", get_china_concept_stock_ranking, 50),
            ("famous_us", get_famous_us_stock_ranking, 50),
            ("internet_china", get_internet_china_stock_ranking, 50),
        ]
        for category, fetch_fn, size in ranking_tasks:
            try:
                items = await fetch_fn(page_size=size)
                conn = get_connection()
                cursor = conn.cursor()
                try:
                    batch_upsert_stock_ranking(cursor, category, trade_date, items)
                    conn.commit()
                    ranking_total += len(items)
                    logger.info("[海外数据调度] %s 涨幅榜写入 %d 条", category, len(items))
                finally:
                    cursor.close()
                    conn.close()
            except Exception as e:
                logger.error("[海外数据调度] %s 涨幅榜失败: %s", category, e)
            await asyncio.sleep(0.3)
        _job_status["ranking_count"] = ranking_total

        # 4. 美股半导体龙头个股日K线 + SOXX半导体ETF
        stock_kline_total = 0
        for code, info in US_SEMI_STOCK_MAP.items():
            try:
                klines = await get_us_stock_day_kline(code, limit=120)
                conn = get_connection()
                cursor = conn.cursor()
                try:
                    batch_upsert_stock_kline(
                        cursor, code, info["name"], info["sector"], klines,
                    )
                    conn.commit()
                    stock_kline_total += len(klines)
                    logger.info("[海外数据调度] %s(%s) 个股K线写入 %d 条",
                                code, info["name"], len(klines))
                finally:
                    cursor.close()
                    conn.close()
            except Exception as e:
                logger.error("[海外数据调度] %s 个股K线失败: %s", code, e)
            await asyncio.sleep(0.3)

        # SOXX 半导体ETF（写入 us_index_kline 表，与 NDX/SPX/DJIA 同表）
        try:
            soxx_klines = await get_sox_index_day_kline(limit=120)
            conn = get_connection()
            cursor = conn.cursor()
            try:
                batch_upsert_index_kline(cursor, "SOXX", soxx_klines)
                conn.commit()
                stock_kline_total += len(soxx_klines)
                logger.info("[海外数据调度] SOXX 半导体ETF K线写入 %d 条", len(soxx_klines))
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            logger.error("[海外数据调度] SOXX K线失败: %s", e)

        _job_status["us_stock_kline_count"] = stock_kline_total

        _job_status["last_success"] = True
        elapsed = (datetime.now(_CST) - start_time).total_seconds()
        logger.info("[海外数据调度] ===== 执行完成，耗时%.1f秒 K线%d 指数%d 涨幅榜%d 个股K线%d =====",
                    elapsed, kline_total, realtime_total, ranking_total, stock_kline_total)

    except Exception as e:
        _job_status["last_success"] = False
        _job_status["error"] = str(e)
        logger.error("[海外数据调度] 执行异常: %s", e, exc_info=True)

    finally:
        _job_status["running"] = False
        _save_persisted_status(_job_status)


async def _execute_job():
    from service.auto_job.scheduler_orchestrator import scheduler_lock, us_market_done_event
    async with scheduler_lock:
        logger.info("[海外数据调度] 已获取全局调度锁")
        try:
            await _execute_job_inner()
        finally:
            us_market_done_event.set()
            logger.info("[海外数据调度] 已发送完成信号")


# ═══════════════════════════════════════════════════════════════
# 调度循环
# ═══════════════════════════════════════════════════════════════

async def _scheduler_loop():
    """调度主循环：每天06:00 CST触发"""
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_seconds = (next_dt - now).total_seconds()

            if wait_seconds > 0:
                logger.info("[海外数据调度] 下次执行时间: %s (%.0f秒后)", next_dt.strftime("%Y-%m-%d %H:%M"), wait_seconds)
                await asyncio.sleep(wait_seconds)

            if _already_done_today():
                logger.info("[海外数据调度] 今日 %s 已执行过，跳过", _get_us_trade_date())
                await asyncio.sleep(60)
                continue

            await _execute_job()
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("[海外数据调度] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[海外数据调度] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_us_market_data_scheduler():
    """启动海外市场数据调度器"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[海外数据调度] 应用已就绪，调度器开始工作")

        now = datetime.now(_CST)
        # 启动时检查：如果还没执行过今天的数据 → 补拉
        if not _already_done_today():
            logger.info("[海外数据调度] 启动补拉：将在10秒后执行")
            async def _delayed_execute():
                await asyncio.sleep(10)
                await _execute_job()
            asyncio.create_task(_delayed_execute())

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[海外数据调度] 调度器已注册，等待应用就绪")
