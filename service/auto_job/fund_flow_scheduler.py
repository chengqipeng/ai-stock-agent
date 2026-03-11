"""
历史资金流向定时调度模块

- 每个A股交易日 16:30 自动触发（在盘后数据之后）
- 遍历股票列表，从同花顺抓取历史资金流向并入库
- 项目启动时检查当天是否已完成，未完成则补拉
- 执行状态持久化到本地文件
"""
import asyncio
import json
import logging
from datetime import datetime, date, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from common.constants.stocks_data import MAIN_STOCK
from common.utils.stock_info_utils import get_stock_info_by_code
from dao import get_connection
from dao.stock_fund_flow_dao import create_fund_flow_table, batch_upsert_fund_flow
from service.jqka10.stock_history_fund_flow_10jqka import get_fund_flow_history
from service.auto_job.kline_data_scheduler import app_ready, is_a_share_trading_day

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent
_INDEX_CODES = {s['code'] for s in MAIN_STOCK}

# ─────────── 状态持久化 ───────────
_STATUS_FILE = _project_root / "data_results" / ".fund_flow_scheduler_status.json"


def _load_persisted_status() -> dict:
    try:
        if _STATUS_FILE.exists():
            return json.loads(_STATUS_FILE.read_text("utf-8"))
    except Exception:
        pass
    return {}


def _save_persisted_status(status: dict):
    try:
        _STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATUS_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2), "utf-8")
    except Exception as e:
        logger.warning("[资金流调度] 状态持久化失败: %s", e)


_job_status = {
    "running": False,
    "last_run_date": None,
    "last_run_time": None,
    "last_success": None,
    "total": 0,
    "success": 0,
    "failed": 0,
    "error": None,
}
_persisted = _load_persisted_status()
if _persisted.get("last_run_date"):
    _job_status["last_run_date"] = _persisted["last_run_date"]
    _job_status["last_success"] = _persisted.get("last_success")


def get_fund_flow_job_status() -> dict:
    return dict(_job_status)


def _build_stock_list() -> list[dict]:
    from service.auto_job.kline_data_scheduler import load_stocks_from_score_list
    stocks = load_stocks_from_score_list()
    main_codes = {s['code'] for s in stocks}
    stocks += [s for s in MAIN_STOCK if s['code'] not in main_codes]
    return stocks


def _next_trigger_dt(after: datetime) -> datetime:
    """计算下一个触发时间：交易日 16:30"""
    d = after.date()
    trigger_time = dtime(16, 30)
    if after.time() < trigger_time and is_a_share_trading_day(d):
        return datetime.combine(d, trigger_time, tzinfo=_CST)
    d += __import__('datetime').timedelta(days=1)
    while not is_a_share_trading_day(d):
        d += __import__('datetime').timedelta(days=1)
    return datetime.combine(d, trigger_time, tzinfo=_CST)


def _already_done_today() -> bool:
    today_str = datetime.now(_CST).date().isoformat()
    return (_job_status.get("last_run_date") == today_str
            and _job_status.get("last_success") is True)


async def _fetch_fund_flow_for_stock(stock_code_normalize: str, counter: dict):
    """拉取单只股票的历史资金流向并入库"""
    stock_code = stock_code_normalize.split(".")[0]
    max_retries = 2
    for attempt in range(1, max_retries + 1):
        try:
            stock_info = get_stock_info_by_code(stock_code_normalize)
            if not stock_info:
                logger.warning("[资金流] 无法获取股票信息: %s", stock_code_normalize)
                counter["failed"] += 1
                return

            data_list = await get_fund_flow_history(stock_info)
            if not data_list:
                logger.debug("[资金流] %s 无资金流数据", stock_code)
                counter["success"] += 1
                return

            conn = get_connection()
            cursor = conn.cursor()
            try:
                batch_upsert_fund_flow(stock_code_normalize, data_list, cursor=cursor)
                conn.commit()
                counter["success"] += 1
                logger.debug("[资金流] %s 写入 %d 条", stock_code, len(data_list))
            finally:
                cursor.close()
                conn.close()
            return

        except Exception as e:
            if attempt < max_retries:
                logger.warning("[资金流] %s 第%d次异常，2秒后重试: %s", stock_code, attempt, e)
                await asyncio.sleep(2)
            else:
                logger.error("[资金流] %s 异常: %s", stock_code, e)
                counter["failed"] += 1


async def _execute_job():
    """执行资金流向数据拉取任务"""
    _job_status["running"] = True
    _job_status["error"] = None
    start_time = datetime.now(_CST)
    today_str = start_time.date().isoformat()
    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    _job_status["last_run_date"] = today_str

    logger.info("[资金流调度] ===== 开始执行 %s =====", today_str)

    try:
        # 1. 建表
        conn = get_connection()
        cursor = conn.cursor()
        try:
            create_fund_flow_table(cursor)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        # 2. 构建股票列表（排除指数）
        all_stocks = _build_stock_list()
        stocks = [s for s in all_stocks if s["code"] not in _INDEX_CODES]
        total = len(stocks)
        _job_status["total"] = total
        _job_status["success"] = 0
        _job_status["failed"] = 0

        # 3. 并发拉取（限制并发数，避免被反爬）
        logger.info("[资金流调度] 开始拉取资金流向，共%d只股票", total)
        counter = {"success": 0, "failed": 0}
        sem = asyncio.Semaphore(3)

        async def _task(s):
            async with sem:
                await _fetch_fund_flow_for_stock(s["code"], counter)
                _job_status["success"] = counter["success"]
                _job_status["failed"] = counter["failed"]
                await asyncio.sleep(0.8)

        await asyncio.gather(*[_task(s) for s in stocks])

        _job_status["last_success"] = counter["failed"] == 0
        if counter["failed"] > 0:
            _job_status["error"] = f"失败 {counter['failed']} 只"
            logger.warning("[资金流调度] 部分失败: %s", _job_status["error"])

        elapsed = (datetime.now(_CST) - start_time).total_seconds()
        logger.info("[资金流调度] ===== 完成，成功%d 失败%d 耗时%.1f秒 =====",
                    counter["success"], counter["failed"], elapsed)

    except Exception as e:
        _job_status["last_success"] = False
        _job_status["error"] = str(e)
        logger.error("[资金流调度] 执行异常: %s", e, exc_info=True)

    finally:
        _job_status["running"] = False
        _save_persisted_status(_job_status)
