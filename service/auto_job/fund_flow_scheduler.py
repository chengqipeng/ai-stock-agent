"""
历史资金流向定时调度模块
- 每个A股交易日 16:30 自动触发
- 遍历股票列表，从东方财富抓取历史资金流向并入库
- 项目启动时检查当天是否已完成，未完成则补拉
"""
import asyncio
import json
import logging
import random
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from common.constants.stocks_data import MAIN_STOCK
from common.utils.stock_info_utils import get_stock_info_by_code
from dao import get_connection
from dao.stock_fund_flow_dao import create_fund_flow_table, batch_upsert_fund_flow
from dao.stock_fund_flow_dao import get_fund_flow_count, get_fund_flow_latest_date
from service.auto_job.kline_data_scheduler import app_ready, is_a_share_trading_day
from service.jqka10.stock_history_fund_flow_10jqka import get_fund_flow_history as get_fund_flow_history_jqka
from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history as get_fund_flow_history_em

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)
_project_root = Path(__file__).parent.parent.parent
_INDEX_CODES = {s["code"] for s in MAIN_STOCK}
_STATUS_FILE = _project_root / "data_results" / ".fund_flow_scheduler_status.json"


def _convert_em_klines_to_dicts(klines: list[str]) -> list[dict]:
    """将东方财富 kline 字符串列表转为 batch_upsert_fund_flow 所需的 dict 列表（万元单位）。

    归一化到与同花顺一致的语义：
      big_net     = 东方财富"主力"(f[1]) = 超大单+大单  ← 对应同花顺"大单(主力)"
      mid_net     = 东方财富"中单"(f[3])
      small_net   = 东方财富"小单"(f[2])
      net_flow    = big_net + mid_net + small_net（总净流入）
      big_net_pct = 东方财富"主力净占比"(f[6])  ← 对应同花顺"大单(主力)净占比"
      mid_net_pct = 东方财富"中单净占比"(f[8])
      small_net_pct = 东方财富"小单净占比"(f[7])

    东方财富 kline 字段顺序（逗号分隔，共15个字段）：
      0:日期, 1:主力净流入(元), 2:小单净流入(元), 3:中单净流入(元), 4:大单净流入(元),
      5:超大单净流入(元), 6:主力净占比, 7:小单净占比, 8:中单净占比, 9:大单净占比,
      10:超大单净占比, 11:收盘价, 12:涨跌幅, 13:(?), 14:(?)

    注意：东方财富与同花顺的资金分类阈值不同，同一天的数值会有差异，
    但字段语义已对齐：big_net 都表示"主力/大单(主力)"，mid/small 都表示中单/小单。
    东方财富数据中 net_flow ≈ 0（因为主力+中单+小单=全市场，资金守恒），
    而同花顺的 net_flow 通常不为 0（统计口径不同）。
    """

    def _float(v):
        return float(v) if v and v != "-" else 0

    def _pct(v):
        return round(float(v), 2) if v and v != "-" else None

    # 第一遍：解析所有行，提取主力净流入（万元）
    parsed = []
    for kline in klines:
        f = kline.split(",")
        if len(f) < 13:
            continue
        main_net_wan = round(_float(f[1]) / 10000, 2)
        parsed.append((f, main_net_wan))

    # 第二遍：计算5日主力净额并构建结果
    # klines 按日期倒序，索引 i 对应的5日窗口为 [i, i+1, i+2, i+3, i+4]
    result = []
    for i, (f, main_net_wan) in enumerate(parsed):
        # big_net = 主力(f[1]) = 超大单+大单，对齐同花顺"大单(主力)"
        big_net_wan = main_net_wan
        mid_net_wan = round(_float(f[3]) / 10000, 2)
        small_net_wan = round(_float(f[2]) / 10000, 2)
        # net_flow = 总净流入 = big + mid + small
        net_flow_wan = round(big_net_wan + mid_net_wan + small_net_wan, 2)

        # 5日主力净额：当天及之后4天（更早的4天）的主力净流入之和
        if i + 5 <= len(parsed):
            main_net_5day = round(sum(p[1] for p in parsed[i:i + 5]), 2)
        else:
            main_net_5day = None

        result.append({
            "date":         f[0],
            "close_price":  round(float(f[11]), 2) if f[11] and f[11] != "-" else None,
            "change_pct":   _pct(f[12]),
            "net_flow":     net_flow_wan,
            "main_net_5day": main_net_5day,
            "big_net":      big_net_wan,
            "big_net_pct":  _pct(f[6]),     # 主力净占比
            "mid_net":      mid_net_wan,
            "mid_net_pct":  _pct(f[8]),     # 中单净占比
            "small_net":    small_net_wan,
            "small_net_pct": _pct(f[7]),    # 小单净占比
        })
    return result


def _load_persisted_status():
    try:
        if _STATUS_FILE.exists():
            return json.loads(_STATUS_FILE.read_text("utf-8"))
    except Exception:
        pass
    return {}


def _save_persisted_status(status):
    try:
        _STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        txt = json.dumps(status, ensure_ascii=False, indent=2)
        _STATUS_FILE.write_text(txt, "utf-8")
    except Exception as e:
        logger.warning("状态持久化失败: %s", e)


_job_status = {
    "running": False, "last_run_date": None, "last_run_time": None,
    "last_success": None, "total": 0, "success": 0, "failed": 0,
    "skipped": 0, "error": None,
}
_persisted = _load_persisted_status()
if _persisted.get("last_run_date"):
    _job_status["last_run_date"] = _persisted["last_run_date"]
    _job_status["last_success"] = _persisted.get("last_success")


def get_fund_flow_job_status():
    return dict(_job_status)


def _build_stock_list():
    from service.auto_job.kline_data_scheduler import load_stocks_from_score_list
    stocks = load_stocks_from_score_list()
    main_codes = {s["code"] for s in stocks}
    stocks += [s for s in MAIN_STOCK if s["code"] not in main_codes]
    return stocks


def _next_trigger_dt(after):
    d = after.date()
    trigger_time = dtime(16, 30)
    if after.time() < trigger_time and is_a_share_trading_day(d):
        return datetime.combine(d, trigger_time, tzinfo=_CST)
    d += timedelta(days=1)
    while not is_a_share_trading_day(d):
        d += timedelta(days=1)
    return datetime.combine(d, trigger_time, tzinfo=_CST)


def _already_done_today():
    today_str = datetime.now(_CST).date().isoformat()
    return (_job_status.get("last_run_date") == today_str
            and _job_status.get("last_success") is True)


async def _fetch_fund_flow_for_stock(code, counter, today_str=None):
    """拉取单只股票的资金流向数据。

    策略：
    - 如果该股票当天数据已存在，跳过不拉取
    - 数据库中记录 < 60 条时，使用东方财富全量拉取（~120条），经 _convert_em_klines_to_dicts 转换后写入
    - 数据库中记录 >= 60 条时，使用同花顺增量拉取（~30条）覆盖最近数据
    """
    stock_code = code.split(".")[0]

    # 跳过已有当天数据的股票
    if today_str:
        latest_date = get_fund_flow_latest_date(code)
        if latest_date and str(latest_date) >= today_str:
            counter["skipped"] += 1
            return

    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        try:
            stock_info = get_stock_info_by_code(code)
            if not stock_info:
                counter["failed"] += 1
                return

            existing_count = get_fund_flow_count(code)
            use_em = existing_count < 60

            if use_em:
                # 东方财富全量拉取（返回字符串列表，需转换）
                klines = await get_fund_flow_history_em(stock_info)
                print(f"{code} em_full {len(klines)}")
                if not klines:
                    counter["success"] += 1
                    return
                data_list = _convert_em_klines_to_dicts(klines)
                if not data_list:
                    counter["success"] += 1
                    return
            else:
                # 同花顺增量拉取（返回 dict 列表，可直接写入）
                data_list = await get_fund_flow_history_jqka(stock_info)
                print(f"{code} jqka_inc {len(data_list)}")
                if not data_list:
                    counter["success"] += 1
                    return

            conn = get_connection()
            cursor = conn.cursor()
            try:
                batch_upsert_fund_flow(code, data_list, cursor=cursor)
                conn.commit()
                counter["success"] += 1
            finally:
                cursor.close()
                conn.close()
            return
        except Exception as e:
            if attempt < max_attempts:
                wait = 3 ** attempt + random.uniform(1, 3)
                logger.warning("[资金流] %s 第%d次失败，%.1f秒后重试: %s", stock_code, attempt, wait, e)
                await asyncio.sleep(wait)
            else:
                logger.error("[资金流] %s 异常: %s", stock_code, e)
                counter["failed"] += 1


async def _execute_job_inner():
    _job_status["running"] = True
    _job_status["error"] = None
    start_time = datetime.now(_CST)
    today_str = start_time.date().isoformat()
    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    _job_status["last_run_date"] = today_str
    logger.info("[资金流调度] 开始执行 %s", today_str)
    try:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            create_fund_flow_table(cursor)
            conn.commit()
        finally:
            cursor.close()
            conn.close()
        all_stocks = _build_stock_list()
        stocks = [s for s in all_stocks if s["code"] not in _INDEX_CODES]
        total = len(stocks)
        _job_status["total"] = total
        _job_status["success"] = 0
        _job_status["failed"] = 0
        _job_status["skipped"] = 0
        counter = {"success": 0, "failed": 0, "skipped": 0}
        sem = asyncio.Semaphore(2)
        today_str = start_time.date().isoformat()

        async def _task(s):
            async with sem:
                await _fetch_fund_flow_for_stock(s["code"], counter, today_str=today_str)
                _job_status["success"] = counter["success"] + counter["skipped"]
                _job_status["skipped"] = counter["skipped"]
                _job_status["failed"] = counter["failed"]
                await asyncio.sleep(1.2 + random.uniform(0, 0.8))

        await asyncio.gather(*[_task(s) for s in stocks])
        _job_status["last_success"] = counter["failed"] == 0
        if counter["failed"] > 0:
            _job_status["error"] = "失败 {} 只".format(counter["failed"])
        elapsed = (datetime.now(_CST) - start_time).total_seconds()
        logger.info("[资金流调度] 完成 成功%d 失败%d 跳过%d 耗时%.1fs",
                    counter["success"], counter["failed"], counter["skipped"], elapsed)
    except Exception as e:
        _job_status["last_success"] = False
        _job_status["error"] = str(e)
        logger.error("[资金流调度] 异常: %s", e, exc_info=True)
    finally:
        _job_status["running"] = False
        _save_persisted_status(_job_status)


async def _execute_job(manual=False):
    from service.auto_job.scheduler_orchestrator import scheduler_lock, manual_semaphore, fund_flow_done_event
    if manual:
        _job_status["running"] = True
        _job_status["error"] = "等待手动调度槽位..."
        logger.info("[资金流调度] 手动触发，等待调度槽位")
        async with manual_semaphore:
            _job_status["error"] = None
            logger.info("[资金流调度] 已获取手动调度槽位")
            try:
                await _execute_job_inner()
            finally:
                fund_flow_done_event.set()
    else:
        _job_status["running"] = True
        _job_status["error"] = "等待调度锁..."
        logger.info("[资金流调度] 等待全局调度锁")
        async with scheduler_lock:
            _job_status["error"] = None
            logger.info("[资金流调度] 已获取全局调度锁")
            try:
                await _execute_job_inner()
            finally:
                fund_flow_done_event.set()
                logger.info("[资金流调度] 已发送完成信号")


async def _scheduler_loop():
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_secs = (next_dt - now).total_seconds()
            if wait_secs > 0:
                await asyncio.sleep(wait_secs)
            trigger_date = datetime.now(_CST).date()
            if not is_a_share_trading_day(trigger_date):
                continue
            if _already_done_today():
                await asyncio.sleep(60)
                continue
            await _execute_job()
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("[资金流调度] 循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_fund_flow_scheduler():
    async def _deferred_start():
        await app_ready.wait()
        now = datetime.now(_CST)
        today = now.date()
        if (is_a_share_trading_day(today)
                and now.time() >= dtime(16, 30)
                and not _already_done_today()):
            async def _delayed():
                await asyncio.sleep(15)
                await _execute_job()
            asyncio.create_task(_delayed())
        else:
            # 不需要执行时，立即发送完成信号
            from service.auto_job.scheduler_orchestrator import fund_flow_done_event
            fund_flow_done_event.set()
            logger.info("[资金流调度] 今日无需补拉，已发送完成信号")
        asyncio.create_task(_scheduler_loop())
    asyncio.create_task(_deferred_start())
    logger.info("[资金流调度] 调度器已注册")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(_execute_job())
