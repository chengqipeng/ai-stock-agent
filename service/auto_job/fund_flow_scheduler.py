"""
历史资金流向定时调度模块
- 每个A股交易日 16:30 自动触发
- 遍历股票列表，从东方财富抓取历史资金流向并入库
- 项目启动时检查当天是否已完成，未完成则补拉
"""
import asyncio
import logging
import random
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from common.constants.stocks_data import MAIN_STOCK
from common.utils.stock_info_utils import get_stock_info_by_code, is_bj_stock
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
    """从数据库恢复状态"""
    from service.auto_job.scheduler_status_helper import restore_status
    return restore_status("fund_flow")


def _save_persisted_status(status):
    """持久化到数据库"""
    from service.auto_job.scheduler_status_helper import persist_status
    persist_status("fund_flow", {
        "last_run_date": status.get("last_run_date"),
        "last_run_time": status.get("last_run_time"),
        "last_success": status.get("last_success"),
        "error": status.get("error"),
    }, {
        "fund_flow": {"total": status.get("total", 0), "success": status.get("success", 0), "failed": status.get("failed", 0), "skipped": status.get("skipped", 0)},
    })



_job_status = {
    "running": False, "last_run_date": None, "last_run_time": None,
    "last_success": None, "total": 0, "success": 0, "failed": 0,
    "skipped": 0, "error": None,
}
_persisted = _load_persisted_status()
if _persisted.get("last_run_date"):
    _job_status["last_run_date"] = _persisted["last_run_date"]
    _job_status["last_run_time"] = _persisted.get("last_run_time")
    _job_status["last_success"] = _persisted.get("last_success")
    _job_status["total"] = _persisted.get("total", 0)
    _job_status["success"] = _persisted.get("success", 0)
    _job_status["failed"] = _persisted.get("failed", 0)
    _job_status["skipped"] = _persisted.get("skipped", 0)


def get_fund_flow_job_status():
    status = dict(_job_status)
    if status.get("running"):
        status["done"] = status.get("success", 0) + status.get("failed", 0)
    return status


def _build_stock_list():
    from service.auto_job.kline_data_scheduler import load_stocks_from_score_list
    stocks = load_stocks_from_score_list()
    main_codes = {s["code"] for s in stocks}
    stocks += [s for s in MAIN_STOCK if s["code"] not in main_codes]
    # 过滤北交所股票：东方财富资金流向API不支持北交所
    stocks = [s for s in stocks if not is_bj_stock(s["code"])]
    return stocks

def _batch_check_completeness(stock_codes: list[str], target_date: str) -> tuple[set[str], set[str]]:
    """批量检查股票过去120个交易日的资金流向数据完整性。

    利用 A 股统一交易日历，先确定120个交易日前的日期下界，
    再用 COUNT + GROUP BY 在该窗口内对比 kline 和 fund_flow 记录数。

    Returns:
        (complete_codes, only_latest_missing_codes)
    """
    if not stock_codes:
        return set(), set()

    conn = get_connection()
    cursor = conn.cursor()
    try:
        # 1) 确定120个交易日的窗口下界
        #    从任意一只活跃股票的 kline 中取最近120天的最小日期
        cursor.execute(
            "SELECT `date` FROM stock_kline "
            "WHERE `date` <= %s "
            "GROUP BY `date` ORDER BY `date` DESC LIMIT 120",
            (target_date,),
        )
        trade_dates = [str(r[0]) for r in cursor.fetchall()]
        if not trade_dates:
            return set(), set()
        window_start = trade_dates[-1]  # 第120个交易日
        window_size = len(trade_dates)  # 实际交易日数（可能 < 120）

        # 2) 在窗口内批量统计 kline 和 fund_flow 的记录数
        ph = ",".join(["%s"] * len(stock_codes))

        cursor.execute(
            f"SELECT stock_code, COUNT(*) as cnt, MAX(`date`) as max_d "
            f"FROM stock_kline "
            f"WHERE stock_code IN ({ph}) AND `date` >= %s AND `date` <= %s "
            f"GROUP BY stock_code",
            (*stock_codes, window_start, target_date),
        )
        kline_stats = {}  # code -> (cnt, max_date)
        for row in cursor.fetchall():
            kline_stats[row[0]] = (row[1], str(row[2]))

        codes_with_kline = [c for c in stock_codes if c in kline_stats]
        if not codes_with_kline:
            return set(), set()

        ph2 = ",".join(["%s"] * len(codes_with_kline))
        cursor.execute(
            f"SELECT stock_code, COUNT(*) as cnt, MAX(`date`) as max_d "
            f"FROM stock_fund_flow "
            f"WHERE stock_code IN ({ph2}) AND `date` >= %s AND `date` <= %s "
            f"GROUP BY stock_code",
            (*codes_with_kline, window_start, target_date),
        )
        ff_stats = {}  # code -> (cnt, max_date)
        for row in cursor.fetchall():
            ff_stats[row[0]] = (row[1], str(row[2]))

        # 3) 对比
        complete_codes = set()
        only_latest_missing_codes = set()
        for code in codes_with_kline:
            k_cnt, k_max = kline_stats[code]
            f_cnt, f_max = ff_stats.get(code, (0, ""))

            if f_cnt >= k_cnt:
                complete_codes.add(code)
            elif f_cnt == k_cnt - 1 and k_max == target_date and f_max != target_date:
                only_latest_missing_codes.add(code)

        return complete_codes, only_latest_missing_codes
    finally:
        cursor.close()
        conn.close()



async def _fill_latest_day_from_realtime(stock_codes: list[str], target_date: str) -> set[str]:
    """对仅缺最新一个交易日的股票，通过东方财富实时资金流向API补全。

    调用实时API获取当日资金流向原始数据（元），转换为万元写入 stock_fund_flow 表。
    close_price / change_pct 从 stock_kline 获取。

    Returns:
        成功补全的股票代码集合
    """
    if not stock_codes:
        return set()

    # 1) 批量获取 K 线中 target_date 的 close_price / change_pct
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        ph = ",".join(["%s"] * len(stock_codes))
        cur.execute(
            f"SELECT stock_code, close_price, change_percent "
            f"FROM stock_kline WHERE stock_code IN ({ph}) AND `date` = %s",
            (*stock_codes, target_date),
        )
        kline_map = {r["stock_code"]: r for r in cur.fetchall()}

        # 获取前4天 big_net 用于计算 main_net_5day
        # 只需最近4个交易日，用10天自然日窗口足够覆盖（含节假日）
        from collections import defaultdict
        date_obj = datetime.fromisoformat(target_date).date()
        prev_window_start = (date_obj - timedelta(days=10)).isoformat()
        prev_big_net_map = defaultdict(list)
        cur.execute(
            f"SELECT stock_code, big_net FROM stock_fund_flow "
            f"WHERE stock_code IN ({ph}) AND `date` < %s AND `date` >= %s "
            f"ORDER BY stock_code, `date` DESC",
            (*stock_codes, target_date, prev_window_start),
        )
        for r in cur.fetchall():
            if len(prev_big_net_map[r["stock_code"]]) < 4:
                prev_big_net_map[r["stock_code"]].append(r["big_net"])
    finally:
        cur.close()
        conn.close()

    # 2) 逐只调用实时资金流向 API
    filled_codes = set()
    sem = asyncio.Semaphore(5)

    async def _fetch_one(code):
        async with sem:
            try:
                stock_info = get_stock_info_by_code(code)
                if not stock_info:
                    return

                # 直接调用东方财富实时资金流向 API 获取原始数值（元）
                from common.http.http_utils import EASTMONEY_PUSH_API_URL, fetch_eastmoney_api
                url = f"{EASTMONEY_PUSH_API_URL}/ulist.np/get"
                params = {
                    "fltt": "2",
                    "secids": stock_info.secid,
                    "fields": "f62,f184,f78,f84,f6,f124",
                    "ut": "b2884a393a59ad64002292a3e90d46a5"
                }
                data = await fetch_eastmoney_api(
                    url, params, referer="https://quote.eastmoney.com/")
                if not (data.get("data") and data["data"].get("diff")):
                    return
                d = data["data"]["diff"][0]

                # 原始值（元）
                main_net_yuan = d.get("f62", 0) or 0  # 主力净流入 = big_net
                mid_net_yuan = d.get("f78", 0) or 0
                small_net_yuan = d.get("f84", 0) or 0
                amount_yuan = d.get("f6", 0) or 0
                main_pct = d.get("f184", 0) or 0  # 主力净占比

                # 转万元
                big_net = round(main_net_yuan / 10000, 2)
                mid_net = round(mid_net_yuan / 10000, 2)
                small_net = round(small_net_yuan / 10000, 2)
                net_flow = round(big_net + mid_net + small_net, 2)
                big_net_pct = round(main_pct, 2)
                mid_net_pct = round(mid_net_yuan / amount_yuan * 100, 2) if amount_yuan else None
                small_net_pct = round(small_net_yuan / amount_yuan * 100, 2) if amount_yuan else None

                # close_price / change_pct 从 K 线获取
                kline = kline_map.get(code)
                close_price = kline["close_price"] if kline else None
                change_pct = kline["change_percent"] if kline else None

                # main_net_5day: 当天 big_net + 前4天 big_net
                prev_bigs = prev_big_net_map.get(code, [])
                valid_bigs = [b for b in prev_bigs if b is not None]
                if len(valid_bigs) == 4:
                    main_net_5day = round(big_net + sum(valid_bigs), 2)
                else:
                    main_net_5day = None

                record = {
                    "date": target_date,
                    "close_price": close_price,
                    "change_pct": change_pct,
                    "net_flow": net_flow,
                    "main_net_5day": main_net_5day,
                    "big_net": big_net,
                    "big_net_pct": big_net_pct,
                    "mid_net": mid_net,
                    "mid_net_pct": mid_net_pct,
                    "small_net": small_net,
                    "small_net_pct": small_net_pct,
                }

                conn2 = get_connection()
                cur2 = conn2.cursor()
                try:
                    batch_upsert_fund_flow(code, [record], cursor=cur2)
                    conn2.commit()
                    filled_codes.add(code)
                finally:
                    cur2.close()
                    conn2.close()

            except Exception as e:
                logger.warning("[实时资金流向补全] %s 失败: %s", code, e)

    await asyncio.gather(*[_fetch_one(c) for c in stock_codes])
    return filled_codes


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
    - 数据库中记录 < 60 条时，使用东方财富全量拉取（~120条）
    - 数据库中记录 >= 60 条时，优先同花顺增量拉取（~30条）
    - 同花顺连续失败3次后，自动切换东方财富全量拉取
    """
    stock_code = code.split(".")[0]
    JQKA_MAX_RETRIES = 3

    try:
        stock_info = get_stock_info_by_code(code)
        if not stock_info:
            counter["failed"] += 1
            return

        existing_count = get_fund_flow_count(code)
        use_em = existing_count < 60

        data_list = None
        source = None

        if use_em:
            # 东方财富全量拉取（返回字符串列表，需转换）
            klines = await get_fund_flow_history_em(stock_info)
            if klines:
                data_list = _convert_em_klines_to_dicts(klines)
                source = "em_full"
        else:
            # 同花顺增量拉取，失败3次后切换东方财富
            for jqka_attempt in range(1, JQKA_MAX_RETRIES + 1):
                try:
                    data_list = await get_fund_flow_history_jqka(stock_info)
                    if data_list:
                        source = "jqka_inc"
                    break
                except Exception as je:
                    if jqka_attempt < JQKA_MAX_RETRIES:
                        wait = 3 ** jqka_attempt + random.uniform(1, 3)
                        logger.warning("[资金流] %s 同花顺第%d次失败，%.1f秒后重试: %s",
                                       stock_code, jqka_attempt, wait, je)
                        await asyncio.sleep(wait)
                    else:
                        logger.warning("[资金流] %s 同花顺%d次均失败，切换东方财富: %s",
                                       stock_code, JQKA_MAX_RETRIES, je)

            if not data_list:
                # 东方财富备用全量拉取
                try:
                    klines = await get_fund_flow_history_em(stock_info)
                    if klines:
                        data_list = _convert_em_klines_to_dicts(klines)
                        source = "em_fallback"
                except Exception as em_e:
                    logger.warning("[资金流] %s 东方财富备用也失败: %s", stock_code, em_e)

        if not data_list:
            counter["success"] += 1
            return

        conn = get_connection()
        cursor = conn.cursor()
        try:
            batch_upsert_fund_flow(code, data_list, cursor=cursor)
            conn.commit()
            counter["success"] += 1
            if source == "em_fallback":
                logger.info("[资金流] %s 东方财富备用成功 %d条", stock_code, len(data_list))
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error("[资金流] %s 异常: %s", stock_code, e)
        counter["failed"] += 1


async def _execute_job_inner():
    _job_status["running"] = True
    _job_status["error"] = None
    start_time = datetime.now(_CST)
    today_str = start_time.date().isoformat()

    # 判断当前是否已收盘：未收盘时目标日期回退到上一个交易日
    if start_time.time() < dtime(15, 0) and is_a_share_trading_day(start_time.date()):
        d = start_time.date() - timedelta(days=1)
        while not is_a_share_trading_day(d):
            d -= timedelta(days=1)
        target_date = d.isoformat()
        logger.info("[资金流调度] 当前未收盘(%s)，目标日期回退到 %s",
                    start_time.strftime("%H:%M"), target_date)
    else:
        target_date = today_str

    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    _job_status["last_run_date"] = target_date
    logger.info("[资金流调度] 开始执行 目标日期=%s", target_date)
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

        def _sync_status():
            _job_status["success"] = counter["success"] + counter["skipped"]
            _job_status["skipped"] = counter["skipped"]
            _job_status["failed"] = counter["failed"]

        # ── 分批处理：每 BATCH_SIZE 只一批，检查 → 实时补全 → 网络拉取 ──
        BATCH_SIZE = 10

        for batch_start in range(0, len(stocks), BATCH_SIZE):
            batch = stocks[batch_start:batch_start + BATCH_SIZE]
            batch_codes = [s["code"] for s in batch]

            # 1) 批量完整性检查
            skip_codes = set()
            rt_fill_codes = set()
            try:
                skip_codes, rt_fill_codes = await asyncio.get_event_loop().run_in_executor(
                    None, lambda bc=batch_codes: _batch_check_completeness(bc, target_date)
                )
            except Exception as e:
                logger.warning("[资金流调度] 批次%d预检查异常: %s", batch_start, e)

            # 2) 仅缺最新日的股票 → 实时资金流向补全
            if rt_fill_codes:
                try:
                    filled = await _fill_latest_day_from_realtime(
                        list(rt_fill_codes), target_date)
                    skip_codes = skip_codes | filled
                except Exception as e:
                    logger.warning("[资金流调度] 实时补全异常: %s", e)

            # 3) 分类：跳过 vs 需拉取
            to_fetch = []
            for s in batch:
                if s["code"] in skip_codes:
                    counter["skipped"] += 1
                else:
                    to_fetch.append(s)
            _sync_status()

            # 4) 网络拉取需要的股票
            async def _task(s):
                async with sem:
                    await _fetch_fund_flow_for_stock(
                        s["code"], counter, today_str=target_date)
                    _sync_status()
                    await asyncio.sleep(1.2 + random.uniform(0, 0.8))

            if to_fetch:
                await asyncio.gather(*[_task(s) for s in to_fetch])

        # ── 补全阶段: 用K线数据补全拉取失败的 ──
        if counter["failed"] > 0:
            try:
                from service.analysis.fund_flow_fallback import fill_missing_fund_flow_from_kline
                all_codes = [s["code"] for s in stocks]
                fb_result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: fill_missing_fund_flow_from_kline(all_codes, target_date)
                )
                fb_filled = fb_result.get("filled", 0)
                if fb_filled > 0:
                    logger.info("[资金流调度] K线补全: 补全%d只", fb_filled)
                    counter["success"] += fb_filled
                    counter["failed"] = max(0, counter["failed"] - fb_filled)
                    _sync_status()
            except Exception as e:
                logger.error("[资金流调度] K线补全异常: %s", e, exc_info=True)

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
