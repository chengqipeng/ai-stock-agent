"""
股票新闻公告定时调度模块

- 每个A股交易日 18:30 自动触发，抓取所有关注股票的新闻公告
- 项目启动时检查当天是否已完成，未完成则立即补拉
- 四类数据：公司新闻、公司公告、行业资讯、研究报告
"""
import asyncio
import json
import logging
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
from chinese_calendar import is_workday

from curl_cffi.requests import AsyncSession

from common.constants.stocks_data import MAIN_STOCK
from dao.stock_news_dao import create_news_table, batch_upsert_news
from dao.scheduler_log_dao import insert_log, update_log
from service.jqka10.stock_news_10jqka import fetch_stock_news, IMPERSONATE
from service.auto_job.kline_data_scheduler import app_ready
from service.auto_job.scheduler_orchestrator import scheduler_lock

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent

# ─────────── 状态持久化 ───────────
_STATUS_FILE = _project_root / "data_results" / ".news_scheduler_status.json"


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
            "total_news": status.get("total_news", 0),
        }
        _STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[新闻调度] 状态持久化失败: %s", e)


# ─────────── 全局状态 ───────────
_job_status = {
    "running": False,
    "last_run_date": None,
    "last_run_time": None,
    "last_success": None,
    "error": None,
    "start_time": None,
    "total_stocks": 0,
    "done_stocks": 0,
    "total_news": 0,
    "failed_stocks": 0,
}
_persisted = _load_persisted_status()
_job_status.update(_persisted)
_job_status["running"] = False
_job_status["error"] = None
_job_status["start_time"] = None


def get_news_job_status() -> dict:
    return dict(_job_status)


def _build_stock_list() -> list[dict]:
    """构建需要抓取新闻的股票列表（排除指数）"""
    from service.auto_job.kline_data_scheduler import load_stocks_from_score_list
    stocks = load_stocks_from_score_list()
    main_codes = {s["code"] for s in stocks}
    # 不加指数，指数没有新闻公告
    # 过滤北交所
    stocks = [s for s in stocks if not s["code"].endswith(".BJ")]
    return stocks


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一个触发时间：每个交易日 18:30"""
    target_time = dtime(18, 30)
    d = after.date()
    # 如果当天还没到触发时间且是交易日，就今天触发
    if after.time() < target_time and is_workday(d):
        return datetime.combine(d, target_time, tzinfo=_CST)
    # 否则找下一个交易日
    d += timedelta(days=1)
    while not is_workday(d):
        d += timedelta(days=1)
    return datetime.combine(d, target_time, tzinfo=_CST)


def _already_done_today() -> bool:
    today_str = date.today().strftime("%Y-%m-%d")
    return _job_status.get("last_run_date") == today_str and _job_status.get("last_success") is True


async def _execute_job(manual: bool = False):
    """执行新闻抓取任务"""
    if _job_status["running"]:
        logger.warning("[新闻调度] 任务正在执行中，跳过")
        return

    lock = scheduler_lock if not manual else asyncio.Lock()
    async with lock:
        _job_status["running"] = True
        _job_status["error"] = None
        _job_status["start_time"] = datetime.now(_CST).isoformat()
        _job_status["total_news"] = 0
        _job_status["done_stocks"] = 0
        _job_status["failed_stocks"] = 0

        now = datetime.now(_CST)
        today_str = now.strftime("%Y-%m-%d")
        started_at = now

        # 建表
        create_news_table()

        stocks = _build_stock_list()
        _job_status["total_stocks"] = len(stocks)
        logger.info("[新闻调度] 开始抓取 %d 只股票的新闻公告", len(stocks))

        log_id = insert_log("stock_news", started_at)
        total_news = 0
        success_count = 0
        failed_count = 0

        try:
            async with AsyncSession(impersonate=IMPERSONATE) as session:
                for i, stock in enumerate(stocks):
                    code = stock["code"]
                    name = stock.get("name", code)
                    try:
                        result = await fetch_stock_news(code, session=session, fetch_content=True)
                        stock_news_count = 0
                        for news_type, items in result.items():
                            if items:
                                batch_upsert_news(code, items)
                                stock_news_count += len(items)

                        total_news += stock_news_count
                        success_count += 1
                        _job_status["done_stocks"] = i + 1
                        _job_status["total_news"] = total_news

                        if stock_news_count > 0:
                            logger.info("[新闻调度] [%d/%d] %s(%s) 写入 %d 条",
                                        i + 1, len(stocks), name, code, stock_news_count)

                        # 请求间隔，避免被封
                        await asyncio.sleep(1.5)

                    except Exception as e:
                        failed_count += 1
                        _job_status["failed_stocks"] = failed_count
                        logger.error("[新闻调度] %s(%s) 抓取失败: %s", name, code, e)
                        await asyncio.sleep(2)

            _job_status["last_run_date"] = today_str
            _job_status["last_run_time"] = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
            _job_status["last_success"] = True
            _job_status["total_news"] = total_news

            finished_at = datetime.now(_CST)
            duration = int((finished_at - started_at).total_seconds())
            detail = json.dumps({
                "total_stocks": len(stocks),
                "success": success_count,
                "failed": failed_count,
                "total_news": total_news,
            }, ensure_ascii=False)

            status = "success" if failed_count == 0 else "partial"
            update_log(log_id, status, len(stocks), success_count, failed_count, 0, detail)

            _save_persisted_status(_job_status)
            logger.info("[新闻调度] 完成: %d只股票, %d条新闻, %d失败, 耗时%ds",
                        len(stocks), total_news, failed_count, duration)

        except Exception as e:
            _job_status["error"] = str(e)
            _job_status["last_success"] = False
            logger.error("[新闻调度] 执行异常: %s", e, exc_info=True)
            try:
                update_log(log_id, "failed", len(stocks), success_count, failed_count, 0, str(e))
            except Exception:
                pass
        finally:
            _job_status["running"] = False


async def _scheduler_loop():
    """调度循环"""
    await app_ready.wait()
    logger.info("[新闻调度] 应用就绪，调度器启动")

    # 启动时检查是否需要补拉
    now = datetime.now(_CST)
    if is_workday(now.date()) and not _already_done_today():
        if now.time() >= dtime(18, 30):
            logger.info("[新闻调度] 启动补拉：今天是交易日且已过18:30")
            await _execute_job()

    while True:
        now = datetime.now(_CST)
        next_dt = _next_trigger_dt(now)
        wait_secs = (next_dt - now).total_seconds()
        logger.info("[新闻调度] 下次触发: %s (等待 %.0f 秒)", next_dt.strftime("%Y-%m-%d %H:%M"), wait_secs)
        await asyncio.sleep(max(wait_secs, 60))

        now = datetime.now(_CST)
        if is_workday(now.date()) and not _already_done_today():
            await _execute_job()


async def start_news_scheduler():
    """启动新闻调度器（由 lifespan 调用）"""
    asyncio.create_task(_scheduler_loop())
