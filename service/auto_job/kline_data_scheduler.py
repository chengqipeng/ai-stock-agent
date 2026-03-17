"""
日线数据定时调度模块

- 每个A股交易日15:05自动触发K线+财报数据拉取
- 项目启动时检查当天是否已完成，未完成则立即补拉
- 状态通过API暴露给前端展示
- 执行状态持久化到本地文件，重启后不会重复全量拉取
"""
import asyncio
import json
import logging
import re
import threading
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
from chinese_calendar import is_workday

from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from service.jqka10.stock_day_kline_data_10jqka import get_stock_day_kline_as_str_10jqka
from service.jqka10.stock_realtime_10jqka import get_today_kline_as_str
from service.jqka10.stock_finance_data_10jqka import get_financial_data_to_json as get_finance_data
from common.utils.stock_info_utils import get_stock_info_by_code
from common.constants.stocks_data import MAIN_STOCK
from dao import get_connection
from dao.stock_kline_dao import (
    get_missing_trading_days, get_latest_db_date,
    create_kline_table, parse_kline_data, batch_insert_or_update_kline_data, insert_suspension_day,
)
from dao.stock_finance_dao import (
    create_finance_table, batch_upsert_finance_data,
    get_finance_latest_updated_at,
)

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent

# 指数代码集合，用于判断是否为指数类股票
_INDEX_CODES = {s['code'] for s in MAIN_STOCK}

# ─────────── 状态持久化 ───────────
_STATUS_FILE = Path(__file__).parent.parent.parent / "data_results" / ".kline_scheduler_status.json"


def _load_persisted_status() -> dict:
    """从本地文件恢复上次执行状态"""
    try:
        if _STATUS_FILE.exists():
            data = json.loads(_STATUS_FILE.read_text(encoding="utf-8"))
            logger.info("[定时调度] 从文件恢复状态: last_run_date=%s", data.get("last_run_date"))
            return data
    except Exception as e:
        logger.warning("[定时调度] 读取状态文件失败: %s", e)
    return {}


def _save_persisted_status(status: dict):
    """将关键状态持久化到本地文件"""
    try:
        _STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_run_time": status.get("last_run_time"),
            "last_run_date": status.get("last_run_date"),
            "last_success": status.get("last_success"),
        }
        _STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("[定时调度] 写入状态文件失败: %s", e)


# ─────────── 启动就绪信号 ───────────
# 所有调度器共享此 Event，应用完全启动后由 lifespan 触发
app_ready = asyncio.Event()

# ─────────── 日线完成信号 ───────────
# 日线数据执行成功后 set，各下游调度器等待此信号
kline_done_event_for_kscore = asyncio.Event()  # K线初筛用
kline_done_event_for_dbcheck = asyncio.Event()  # 数据异常检测用（保留兼容）

from service.auto_job.scheduler_orchestrator import scheduler_lock, kline_done_event

# ─────────── 全局状态 ───────────
_persisted = _load_persisted_status()

_job_status = {
    "last_run_time": _persisted.get("last_run_time"),
    "last_run_date": _persisted.get("last_run_date"),
    "last_success": _persisted.get("last_success"),
    "kline_total": 0,
    "kline_success": 0,
    "kline_failed": 0,
    "finance_total": 0,
    "finance_success": 0,
    "finance_failed": 0,
    "running": False,            # 是否正在执行
    "error": None,               # 异常信息
}


def get_job_status() -> dict:
    status = dict(_job_status)
    # 运行中时，从实时计数器读取进度
    if status.get("running"):
        kc = status.get("_kline_counter") or {}
        fc = status.get("_finance_counter") or {}
        status["kline_total"] = kc.get("total", 0)
        status["kline_success"] = kc.get("success", 0)
        status["kline_failed"] = kc.get("failed", 0)
        status["finance_total"] = fc.get("total", 0)
        status["finance_success"] = fc.get("success", 0)
        status["finance_failed"] = fc.get("failed", 0)
    # 不暴露内部引用
    status.pop("_kline_counter", None)
    status.pop("_finance_counter", None)
    return status


# ─────────── K线采集流水线 ───────────

async def _process_single_kline(stock_code, stock_name, limit, counter):
    """处理单只股票的K线数据拉取和存储"""
    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        counter['failed'] += 1
        return

    t_start = asyncio.get_event_loop().time()
    missing_days = get_missing_trading_days(stock_code)
    latest_db_date = get_latest_db_date(stock_code)
    t_dao = asyncio.get_event_loop().time()

    if not missing_days:
        logger.info("[K线 总%d 成功%d 失败%d 当前:%s] 最新数据日期是%s，无需拉取 dao耗时%.2fs",
                    counter['total'], counter['success'], counter['failed'], stock_name, latest_db_date, t_dao-t_start)
        counter['success'] += 1
        return

    earliest_missing = missing_days[-1]
    today_cst = datetime.now(_CST).date()
    fetch_limit = (today_cst - earliest_missing).days + 5 if latest_db_date else limit

    klines = None
    t0 = asyncio.get_event_loop().time()

    # 仅缺最新一天时，直接从同花顺实时接口获取
    realtime_failed_index = False
    if len(missing_days) == 1 and missing_days[0] == today_cst:
        try:
            pure_code = stock_code.split('.')[0]
            kline_str = await get_today_kline_as_str(pure_code, stock_code_normalize=stock_code)
            klines = [kline_str] if kline_str else []
            elapsed = asyncio.get_event_loop().time() - t0
        except Exception as e:
            logger.error("[K线 %s] 实时K线获取失败: %s", stock_name, e)
            # 指数实时接口失败时回退到完整拉取，不直接放弃
            if stock_code in _INDEX_CODES:
                logger.info("[K线 %s] 指数实时接口失败，回退到完整拉取", stock_name)
                realtime_failed_index = True
            else:
                counter['failed'] += 1
                return

    if realtime_failed_index or not (len(missing_days) == 1 and missing_days[0] == today_cst):
        _RETRYABLE_KEYWORDS = ('Server disconnected', 'Connection closed abruptly',
                               'Expecting value', '空响应', 'JSONP解包后为空',
                               'JSON解析失败', 'ClientResponseError')
        is_index = stock_code in _INDEX_CODES
        max_attempts = 1 if is_index else 10
        for attempt in range(1, max_attempts + 1):
            try:
                klines = await get_stock_day_kline_as_str_10jqka(stock_info, fetch_limit)
                elapsed = asyncio.get_event_loop().time() - t0
                break
            except Exception as e:
                if is_index:
                    logger.warning("[K线 %s] 同花顺指数拉取失败，尝试东方财富回退: %s",
                                   stock_name, str(e)[:200])
                    break
                err_msg = str(e)
                is_retryable = any(kw in err_msg for kw in _RETRYABLE_KEYWORDS)
                if is_retryable and attempt < max_attempts:
                    wait = min(10 * attempt, 60)
                    logger.warning("[K线 %s] 请求异常(%s)，第%d次重试，等待%d秒",
                                   stock_name, err_msg[:200], attempt, wait)
                    await asyncio.sleep(wait)
                else:
                    logger.error("[K线 %s] 获取失败(重试%d次): %s", stock_name, attempt, e)
                    counter['failed'] += 1
                    return

        # 指数类：同花顺失败或返回空时，回退到东方财富API
        if is_index and (not klines):
            try:
                em_klines = await get_stock_day_range_kline(stock_info, fetch_limit)
                if em_klines:
                    klines = em_klines
                    elapsed = asyncio.get_event_loop().time() - t0
                    logger.info("[K线 %s] 东方财富回退成功，%d条", stock_name, len(klines))
                else:
                    logger.warning("[K线 %s] 东方财富也返回空数据", stock_name)
                    counter['failed'] += 1
                    return
            except Exception as e2:
                logger.error("[K线 %s] 东方财富回退也失败: %s", stock_name, str(e2)[:200])
                counter['failed'] += 1
                return

    if klines is None:
        counter['failed'] += 1
        return

    # 校验K线核心字段
    _REQUIRED_FIELD_NAMES = ("date", "open_price", "close_price", "high_price", "low_price", "trading_volume")
    _REQUIRED_FIELD_INDICES = (0, 1, 2, 3, 4, 5)
    bad_lines = []
    for idx, kline_str in enumerate(klines):
        fields = kline_str.split(",")
        for fi, fname in zip(_REQUIRED_FIELD_INDICES, _REQUIRED_FIELD_NAMES):
            if fi >= len(fields) or not fields[fi] or fields[fi].strip() == "" or fields[fi] == "None":
                bad_lines.append(f"第{idx+1}条 字段[{fname}]为空: {kline_str[:120]}")
    if bad_lines:
        err_detail = "; ".join(bad_lines[:5])
        logger.error("[K线 %s %s] 数据存在空值，共%d条异常: %s", stock_code, stock_name, len(bad_lines), err_detail)
        counter['failed'] += 1
        return

    # 写入数据库
    conn = get_connection()
    cursor = conn.cursor()
    t_db_start = asyncio.get_event_loop().time()
    create_kline_table(cursor)
    saved_dates = set()
    parsed_list = []
    for kline_str in klines:
        try:
            kline_data = parse_kline_data(kline_str)
            parsed_list.append(kline_data)
            saved_dates.add(date.fromisoformat(kline_data['date']))
        except Exception as e:
            logger.error("解析K线数据失败 %s: %s", stock_code, e)
    batch_insert_or_update_kline_data(cursor, stock_code, parsed_list)
    for d in missing_days:
        if d not in saved_dates:
            insert_suspension_day(cursor, stock_code, d)
    conn.commit()
    cursor.close()
    conn.close()
    t_db_end = asyncio.get_event_loop().time()

    counter['success'] += 1
    logger.info("[K线 总%d 成功%d 失败%d 当前:%s] 完成，%d条 网络%.2fs dao%.2fs 写db%.2fs",
                counter['total'], counter['success'], counter['failed'], stock_name,
                len(klines), elapsed, t_dao-t_start, t_db_end-t_db_start)


# ─────────── 财报采集流水线 ───────────

async def _process_single_finance(stock_code, stock_name, counter):
    """处理单只股票的财报数据拉取和存储"""
    stock_info = get_stock_info_by_code(stock_code)
    if not stock_info:
        counter['failed'] += 1
        return

    # 今天已拉取过则跳过
    today_str = datetime.now(_CST).strftime("%Y-%m-%d")
    latest_updated = get_finance_latest_updated_at(stock_code)
    if latest_updated and latest_updated[:10] >= today_str:
        counter['success'] += 1
        logger.info("[财报 总%d 成功%d 失败%d 当前:%s] 今日已更新(%s)，跳过",
                    counter['total'], counter['success'], counter['failed'], stock_name, latest_updated)
        return

    t0 = asyncio.get_event_loop().time()
    try:
        records = await get_finance_data(stock_info)
    except Exception as e:
        logger.warning("[财报 %s] 获取失败: %s", stock_name, e)
        counter['failed'] += 1
        return
    elapsed = asyncio.get_event_loop().time() - t0

    if not records:
        logger.warning("[财报 %s] 返回空数据", stock_name)
        counter['failed'] += 1
        return

    # 写入数据库
    t_db = asyncio.get_event_loop().time()
    conn = get_connection()
    cursor = conn.cursor()
    create_finance_table(cursor)
    batch_upsert_finance_data(cursor, stock_code, records)
    conn.commit()
    cursor.close()
    conn.close()
    t_db_end = asyncio.get_event_loop().time()

    counter['success'] += 1
    logger.info("[财报 总%d 成功%d 失败%d 当前:%s] 完成，%d条 网络%.2fs 写db%.2fs",
                counter['total'], counter['success'], counter['failed'], stock_name,
                len(records), elapsed, t_db_end-t_db)


# ─────────── 公共工具 ───────────

def load_stocks_from_score_list() -> list[dict]:
    score_list_path = _project_root / "data_results/stock_to_score_list/stock_score_list.md"
    stocks = []
    pattern = re.compile(r'^(.+?)\s+\(([^)]+)\)')
    for line in score_list_path.read_text(encoding='utf-8').splitlines():
        m = pattern.match(line.strip())
        if m:
            code = m.group(2)
            if code.endswith('.BJ'):
                continue  # 忽略北交所个股
            stocks.append({'name': m.group(1), 'code': code})
    return stocks


def _build_stock_list() -> list[dict]:
    """构建完整的股票列表（score_list + MAIN_STOCK 去重）"""
    stocks = load_stocks_from_score_list()
    main_codes = {s['code'] for s in stocks}
    stocks += [s for s in MAIN_STOCK if s['code'] not in main_codes]
    return stocks


# ─────────── 独立运行入口 ───────────

async def run_kline_job(limit=800, max_concurrent=1, counter=None):
    """独立运行K线采集任务"""
    stocks = _build_stock_list()
    logger.info("[K线] 开始采集，共 %d 只股票", len(stocks))

    semaphore = asyncio.Semaphore(max_concurrent)
    if counter is None:
        counter = {'total': len(stocks), 'success': 0, 'failed': 0}
    else:
        counter['total'] = len(stocks)

    async def task(stock):
        async with semaphore:
            await _process_single_kline(stock['code'], stock['name'], limit, counter)

    await asyncio.gather(*[task(s) for s in stocks], return_exceptions=True)
    logger.info("[K线] 采集完成，总%d 成功%d 失败%d", counter['total'], counter['success'], counter['failed'])
    return counter


async def run_finance_job(max_concurrent=3, counter=None):
    """独立运行财报采集任务（排除指数，指数无财报数据）"""
    all_stocks = _build_stock_list()
    stocks = [s for s in all_stocks if s['code'] not in _INDEX_CODES]
    logger.info("[财报] 开始采集，共 %d 只股票（已排除%d只指数）",
                len(stocks), len(all_stocks) - len(stocks))

    semaphore = asyncio.Semaphore(max_concurrent)
    if counter is None:
        counter = {'total': len(stocks), 'success': 0, 'failed': 0}
    else:
        counter['total'] = len(stocks)

    async def task(stock):
        async with semaphore:
            await _process_single_finance(stock['code'], stock['name'], counter)

    await asyncio.gather(*[task(s) for s in stocks], return_exceptions=True)
    logger.info("[财报] 采集完成，总%d 成功%d 失败%d", counter['total'], counter['success'], counter['failed'])
    return counter


def run_stock_klines_job(limit=800, max_concurrent=1):
    """
    在两个独立线程中分别运行K线和财报采集流水线。

    每条流水线拥有独立的线程、事件循环、计数器、信号量和错误处理，
    任何一条流水线的异常或阻塞都不会影响另一条。
    K线默认串行（max_concurrent=1），财报默认3并发。
    """
    logger.info("=" * 60)
    logger.info("  启动数据采集（K线 + 财报 独立线程）")
    logger.info("=" * 60)

    results = {}

    def _run_kline():
        try:
            results['kline'] = asyncio.run(run_kline_job(limit=limit, max_concurrent=max_concurrent))
        except Exception as e:
            logger.error("[K线线程] 异常退出: %s", e)
            results['kline'] = {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}

    def _run_finance():
        try:
            results['finance'] = asyncio.run(run_finance_job(max_concurrent=3))
        except Exception as e:
            logger.error("[财报线程] 异常退出: %s", e)
            results['finance'] = {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}

    t_kline = threading.Thread(target=_run_kline, name="Thread-Kline", daemon=True)
    t_finance = threading.Thread(target=_run_finance, name="Thread-Finance", daemon=True)

    t_kline.start()
    t_finance.start()

    t_kline.join()
    t_finance.join()

    kline_counter = results.get('kline', {})
    finance_counter = results.get('finance', {})

    logger.info("\n" + "=" * 60)
    logger.info("  全部完成")
    logger.info("  K线: 总%d 成功%d 失败%d", kline_counter.get('total',0), kline_counter.get('success',0), kline_counter.get('failed',0))
    logger.info("  财报: 总%d 成功%d 失败%d", finance_counter.get('total',0), finance_counter.get('success',0), finance_counter.get('failed',0))
    logger.info("=" * 60)
    return kline_counter, finance_counter


# ─────────── 交易日判断 ───────────

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


async def _execute_job(manual=False):
    """执行一次K线+财报采集"""
    from dao.scheduler_log_dao import insert_log, update_log

    async def _inner():
        _job_status["running"] = True
        _job_status["error"] = None
        _job_status["start_time"] = datetime.now(_CST).isoformat()
        today_str = datetime.now(_CST).date().isoformat()
        started_at = datetime.now(_CST)
        log_id = insert_log("日线数据", started_at)
        logger.info("[定时调度] 开始执行日线数据拉取 %s (log_id=%d)", today_str, log_id)

        try:
            # 实时计数器，run_kline_job / run_finance_job 内部会原地修改
            kline_counter = {"total": 0, "success": 0, "failed": 0}
            finance_counter = {"total": 0, "success": 0, "failed": 0}

            # 将引用挂到全局状态，前端轮询时可读取实时进度
            _job_status["_kline_counter"] = kline_counter
            _job_status["_finance_counter"] = finance_counter

            try:
                kline_counter = await run_kline_job(limit=800, max_concurrent=1, counter=kline_counter)
                _job_status["_kline_counter"] = kline_counter
            except Exception as e:
                logger.error("[定时调度] K线采集异常: %s", e)

            try:
                finance_counter = await run_finance_job(max_concurrent=3, counter=finance_counter)
                _job_status["_finance_counter"] = finance_counter
            except Exception as e:
                logger.error("[定时调度] 财报采集异常: %s", e)

            now_str = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
            total_failed = kline_counter.get("failed", 0) + finance_counter.get("failed", 0)
            total_count = kline_counter.get("total", 0) + finance_counter.get("total", 0)
            total_success = kline_counter.get("success", 0) + finance_counter.get("success", 0)

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
                "_kline_counter": None,
                "_finance_counter": None,
            })

            # 持久化状态，重启后可跳过已完成的日期
            _save_persisted_status(_job_status)

            detail = (f"K线: {kline_counter.get('success',0)}/{kline_counter.get('total',0)}"
                      f"(失败{kline_counter.get('failed',0)})  "
                      f"财报: {finance_counter.get('success',0)}/{finance_counter.get('total',0)}"
                      f"(失败{finance_counter.get('failed',0)})")
            update_log(log_id, "success" if total_failed == 0 else "partial",
                       total_count, total_success, total_failed, detail=detail)

            logger.info("[定时调度] 执行完成 K线:%d/%d 财报:%d/%d",
                        kline_counter.get("success", 0), kline_counter.get("total", 0),
                        finance_counter.get("success", 0), finance_counter.get("total", 0))

            # 日线执行完成后，通知下游调度器
            try:
                kline_done_event_for_kscore.set()
                kline_done_event_for_dbcheck.set()
                kline_done_event.set()
                logger.info("[定时调度] 已发送日线完成信号给下游调度器")
            except Exception as e:
                logger.warning("[定时调度] 发送日线完成信号失败: %s", e)

        except Exception as e:
            import traceback as _tb
            err_msg = f"任务异常终止: {type(e).__name__}: {e}"
            err_detail = f"{err_msg}\n{_tb.format_exc()}"
            logger.error("[定时调度] %s", err_msg, exc_info=True)
            _job_status.update({"error": err_msg, "_kline_counter": None, "_finance_counter": None})
            try:
                update_log(log_id, "failed", detail=err_detail)
            except Exception:
                pass
            # 即使失败也要发送完成信号，否则下游会永远等待
            kline_done_event.set()
            kline_done_event_for_kscore.set()
            kline_done_event_for_dbcheck.set()
        finally:
            _job_status["running"] = False
            _save_persisted_status(_job_status)
            # 确保完成信号一定被发送
            kline_done_event.set()
            kline_done_event_for_kscore.set()
            kline_done_event_for_dbcheck.set()

    if manual:
        _job_status["running"] = True
        _job_status["error"] = "等待手动调度槽位..."
        logger.info("[定时调度] 手动触发，等待调度槽位")
        from service.auto_job.scheduler_orchestrator import manual_semaphore
        async with manual_semaphore:
            _job_status["error"] = None
            logger.info("[定时调度] 已获取手动调度槽位")
            await _inner()
    else:
        async with scheduler_lock:
            logger.info("[定时调度] 已获取全局调度锁")
            await _inner()


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
    """启动调度器：等待应用就绪后，检查是否需要补拉，再启动定时循环"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[定时调度] 应用已就绪，调度器开始工作")

        now = datetime.now(_CST)
        today = now.date()

        # 启动时检查：如果当天是交易日、已过15:00、且今天还没执行过 → 延迟补拉（等待项目完全启动）
        if is_a_share_trading_day(today) and now.time() >= dtime(15, 0) and not _already_done_today():
            logger.info("[定时调度] 启动补拉：今天是交易日且已过15:00，将在5秒后执行")
            async def _delayed_execute():
                await asyncio.sleep(5)
                await _execute_job()
            asyncio.create_task(_delayed_execute())

        # 启动定时循环
        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[定时调度] 调度器已注册，等待应用就绪")


if __name__ == "__main__":
    run_stock_klines_job()
