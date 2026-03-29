"""
数据异常检测定时调度模块

- 在日线数据调度执行成功后自动触发
- 检测 stock_kline 表中的数据异常并自动修复
- 一天只执行一次
- 状态通过API暴露给前端展示

检测场景：
1. 价格逻辑错误：close_price <= 0 / open_price <= 0 / high_price <= 0 / low_price <= 0
2. 价格关系异常：high < low / close > high / close < low / open > high / open < low
3. 交易量/金额异常：trading_volume < 0 / trading_amount < 0
4. 涨跌幅异常：|change_percent| > 21%
5. 日期格式异常：date 不符合 YYYY-MM-DD 格式
6. 日期重复：同一股票存在重复日期
7. 缺失交易日：相邻两条记录间存在未记录的交易日
8. 停牌占位记录异常：全零记录中存在非零字段
9. 数据库无表或表为空
10. 非交易日数据：date 对应非交易日，立即删除
11. 衍生字段全零异常：amplitude/change_percent/change_amount 全为 0 但价格有变动
   （如智洋创新等个股数据源返回衍生字段缺失的场景）

资金流向强一致性校验（首日交易数据允许误差，跳过校验）：
12. 资金流向关键字段缺失：close_price / change_pct / net_flow 为 NULL

注意：
- close_price / change_pct 直接从 K线表同步（K线是价格权威来源），不再做 mismatch 检测。
- 资金守恒（net_flow = big+mid+small）和占比守恒不作为校验规则，
  因为同花顺数据源的统计口径与东方财富不同，混合数据源下守恒关系不成立。

概念板块K线数据质量校验：
15. 无指数代码：board_index_code 为空，无法拉取K线
16. 无K线数据：concept_board_kline 表中无记录
17. K线过少：不足10条
18. 数据过旧：最新K线距大盘最新日期超过7天
19. 价格逻辑异常：close_price <= 0 / high < low
20. 日期重复：同一板块存在重复日期

盘口数据校验（最近30个交易日）：
21. 无盘口数据：有K线但无盘口记录
22. 盘口覆盖率低：盘口天数 < K线天数的50%

分时数据校验（最近30个交易日）：
23. 无分时数据：有K线但无分时记录
24. 分时覆盖率低：分时天数 < K线天数的50%
25. 分时数据稀疏：平均每天不足100条（正常~240条）

发现K线异常时：仅记录异常结果到日志和调度记录中，不重新拉取数据。
发现资金流向异常时：
  1. 先从 K线同步 close_price / change_pct（一条 SQL 批量完成，K线是价格权威来源）
  2. 若仍有 net_flow 等资金流字段 NULL，同时拉取同花顺和东方财富数据
  3. 评估两个数据源在重叠日期上的维度差异（价格字段、资金流字段）
  4. 根据评估结果选择修复策略：
     - jqka_only: 资金流口径差异大时，仅用同花顺修复
     - em_with_kline_fix: 仅用东方财富修复
     - jqka_recent_em_history: 东方财富全量打底 + 同花顺覆盖近期
  5. 数据源写入后再次从 K线同步价格（防止数据源带入不准确的价格）
"""
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from service.auto_job.kline_data_scheduler import app_ready, kline_done_event_for_dbcheck
from service.auto_job.scheduler_orchestrator import scheduler_lock, wait_all_data_jobs_done

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

# ─────────── 状态持久化 ───────────
def _load_persisted_status() -> dict:
    """从数据库恢复状态"""
    from service.auto_job.scheduler_status_helper import restore_status
    return restore_status("db_check")


def _save_persisted_status(status: dict):
    """持久化到数据库"""
    from service.auto_job.scheduler_status_helper import persist_status
    persist_status("db_check", {
        "last_run_date": status.get("last_run_date"),
        "last_run_time": status.get("last_run_time"),
        "last_success": status.get("last_success"),
        "error": status.get("error"),
        "extra_json": {"check_total": status.get("check_total", 0), "check_anomalies": status.get("check_anomalies", 0), "check_repaired": status.get("check_repaired", 0)},
    })



# ─────────── 全局状态 ───────────
_persisted = _load_persisted_status()

_job_status = {
    "last_run_time": _persisted.get("last_run_time"),
    "last_run_date": _persisted.get("last_run_date"),
    "last_success": _persisted.get("last_success"),
    "check_total": 0,
    "check_anomalies": 0,
    "check_repaired": 0,
    "running": False,
    "error": None,
}


def get_db_check_job_status() -> dict:
    status = dict(_job_status)
    if status.get("running"):
        sc = status.get("_counter") or {}
        status["check_total"] = sc.get("total", 0)
        status["check_checked"] = sc.get("checked", 0)
        status["check_anomalies"] = sc.get("anomalies", 0)
        status["check_repaired"] = sc.get("repaired", 0)
        status["phase"] = sc.get("phase", "kline")
        status["phase_label"] = sc.get("phase_label", "K线检测")
        # 资金流向阶段进度
        status["ff_total"] = sc.get("ff_total", 0)
        status["ff_checked"] = sc.get("ff_checked", 0)
        # 板块K线阶段进度
        status["board_total"] = sc.get("board_total", 0)
        status["board_checked"] = sc.get("board_checked", 0)
        # 盘口数据阶段进度
        status["ob_total"] = sc.get("ob_total", 0)
        status["ob_checked"] = sc.get("ob_checked", 0)
        # 分时数据阶段进度
        status["td_total"] = sc.get("td_total", 0)
        status["td_checked"] = sc.get("td_checked", 0)
    status.pop("_counter", None)
    return status


def _already_done_today() -> bool:
    now_date = datetime.now(_CST).date().isoformat()
    return _job_status["last_run_date"] == now_date and _job_status["last_success"] is True


# ─────────── 概念板块K线校验辅助函数 ───────────

def _check_board_kline(board_code: str, board_name: str, index_code: str | None) -> list[dict]:
    """
    校验单个概念板块的K线数据质量。

    检测项：
    1. 无index_code：板块缺少指数代码映射
    2. 无K线数据：concept_board_kline 表中无记录
    3. K线过少：不足10条（新板块除外）
    4. 数据过旧：最新K线距今超过5个交易日
    5. 价格逻辑异常：close_price <= 0 / high < low
    6. 日期重复：同一板块存在重复日期
    """
    from dao import get_connection as _get_conn
    issues = []

    if not index_code:
        issues.append({"type": "无指数代码", "detail": f"board_index_code 为空，无法拉取K线"})

    conn = _get_conn(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # K线记录数
        cur.execute("SELECT COUNT(*) as cnt FROM concept_board_kline WHERE board_code = %s",
                    (board_code,))
        cnt = cur.fetchone()["cnt"]

        if cnt == 0:
            issues.append({"type": "无K线数据", "detail": f"concept_board_kline 中无任何记录"})
            return issues

        if cnt < 10:
            issues.append({"type": "K线过少", "detail": f"仅有 {cnt} 条K线记录"})

        # 最新日期
        cur.execute("SELECT MAX(`date`) as max_d FROM concept_board_kline WHERE board_code = %s",
                    (board_code,))
        max_date = cur.fetchone()["max_d"]

        # 与大盘最新日期对比
        cur.execute("SELECT MAX(`date`) as max_d FROM stock_kline WHERE stock_code = '000001.SH'")
        market_max = cur.fetchone()["max_d"]

        if max_date and market_max and max_date < market_max:
            from datetime import date as _date
            try:
                board_d = _date.fromisoformat(str(max_date))
                market_d = _date.fromisoformat(str(market_max))
                gap = (market_d - board_d).days
                if gap > 7:
                    issues.append({
                        "type": "数据过旧",
                        "detail": f"最新K线 {max_date}，大盘最新 {market_max}，落后 {gap} 天"
                    })
            except (ValueError, TypeError):
                pass

        # 价格逻辑异常（抽查最近60条）
        cur.execute(
            "SELECT `date`, open_price, close_price, high_price, low_price "
            "FROM concept_board_kline WHERE board_code = %s "
            "ORDER BY `date` DESC LIMIT 60",
            (board_code,)
        )
        price_issues = 0
        for row in cur.fetchall():
            d = row["date"]
            cp = row["close_price"]
            hp = row["high_price"]
            lp = row["low_price"]
            op = row["open_price"]
            if cp is not None and cp <= 0:
                price_issues += 1
            if hp is not None and lp is not None and hp < lp:
                price_issues += 1
        if price_issues > 0:
            issues.append({"type": "价格异常", "detail": f"最近60条中有 {price_issues} 条价格逻辑异常"})

        # 日期重复
        cur.execute(
            "SELECT `date`, COUNT(*) as c FROM concept_board_kline "
            "WHERE board_code = %s GROUP BY `date` HAVING c > 1",
            (board_code,)
        )
        dup_rows = cur.fetchall()
        if dup_rows:
            issues.append({
                "type": "日期重复",
                "detail": f"{len(dup_rows)} 个日期存在重复记录"
            })

    finally:
        cur.close()
        conn.close()

    return issues


async def _repair_board_kline(board_code: str, board_name: str,
                              index_code: str | None) -> bool:
    """尝试修复板块K线：重新拉取并校验"""
    try:
        if not index_code:
            # 尝试从网页获取 index_code
            from service.jqka10.concept_board_kline_10jqka import fetch_board_index_code
            index_code = fetch_board_index_code(board_code)
            if index_code:
                from dao.stock_concept_board_dao import update_board_index_code
                update_board_index_code(board_code, index_code)
                logger.info("[板块K线修复 %s(%s)] 获取到 index_code=%s 并回写",
                            board_code, board_name, index_code)
            else:
                logger.warning("[板块K线修复 %s(%s)] 无法获取 index_code，跳过修复",
                               board_code, board_name)
                return False

        from service.jqka10.concept_board_kline_10jqka import fetch_board_kline
        from dao.concept_board_kline_dao import batch_upsert_klines

        klines = await fetch_board_kline(index_code, limit=800)
        if not klines:
            logger.warning("[板块K线修复 %s(%s)] 拉取到空数据", board_code, board_name)
            return False

        batch_upsert_klines(board_code, klines, board_index_code=index_code)
        logger.info("[板块K线修复 %s(%s)] 重新写入 %d 条K线", board_code, board_name, len(klines))

        # 重新校验
        re_issues = _check_board_kline(board_code, board_name, index_code)
        # 修复后只要没有严重问题（无K线/价格异常/日期重复）就算通过
        serious = [i for i in re_issues if i["type"] not in ("K线过少",)]
        if not serious:
            logger.info("[板块K线修复 %s(%s)] 修复后校验通过 ✓", board_code, board_name)
            return True
        else:
            logger.warning("[板块K线修复 %s(%s)] 修复后仍有异常: %s",
                           board_code, board_name,
                           "; ".join(i["detail"] for i in serious))
            return False

    except Exception as e:
        logger.error("[板块K线修复 %s(%s)] 异常: %s", board_code, board_name, e, exc_info=True)
        return False


async def _execute_job():
    """执行一次数据异常检测与修复"""
    from dao.stock_kline_dao import check_db, get_all_stock_codes
    from dao.stock_fund_flow_dao import check_fund_flow_db, batch_upsert_fund_flow, sync_price_fields_from_kline, evaluate_source_divergence
    from service.jqka10.stock_history_fund_flow_10jqka import get_fund_flow_history as get_fund_flow_jqka
    from service.eastmoney.stock_info.stock_history_flow import get_fund_flow_history as get_fund_flow_em
    from service.auto_job.fund_flow_scheduler import _convert_em_klines_to_dicts
    from common.utils.stock_info_utils import get_stock_info_by_code
    from dao.scheduler_log_dao import insert_log, update_log

    _job_status["running"] = True
    _job_status["error"] = None
    _job_status["start_time"] = datetime.now(_CST).isoformat()
    today_str = datetime.now(_CST).date().isoformat()
    started_at = datetime.now(_CST)
    log_id = insert_log("数据异常检测", started_at)
    logger.info("[数据异常检测] 开始执行 %s (log_id=%d)", today_str, log_id)

    counter = {"total": 0, "checked": 0, "anomalies": 0, "repaired": 0,
               "phase": "kline", "phase_label": "K线检测",
               "ff_total": 0, "ff_checked": 0,
               "board_total": 0, "board_checked": 0,
               "ob_total": 0, "ob_checked": 0,
               "td_total": 0, "td_checked": 0}
    _job_status["_counter"] = counter

    try:
        anomaly_details = []

        def _is_recent_issue(issue: dict) -> bool:
            """过滤掉2024年之前的数据异常，只关注近期数据"""
            d = issue.get("date", "")
            if not d or d == "-":
                return True  # 无日期的异常（如表为空）保留
            return d >= "2024-01-01"

        try:
            stock_codes = get_all_stock_codes()
            stock_codes = [c for c in stock_codes if not c.endswith('.BJ')]  # 忽略北交所
            counter["total"] = len(stock_codes)

            if not stock_codes:
                logger.info("[数据异常检测] 未找到任何K线数据")
            else:
                logger.info("[数据异常检测] 共 %d 只股票，开始检测...", len(stock_codes))
                total_issues = 0
                for stock_code in stock_codes:
                    issues = check_db(stock_code)
                    counter["checked"] += 1
                    if counter["checked"] % 50 == 0:
                        await asyncio.sleep(0)
                    if not issues:
                        continue

                    active_issues = [i for i in issues if not i.get("legacy") and _is_recent_issue(i)]
                    if not active_issues:
                        continue

                    counter["anomalies"] += 1
                    total_issues += len(active_issues)

                    # 收集异常详情（仅记录，不重新拉取修复）
                    issue_lines = [f"[{iss['type']}] 日期={iss['date']} {iss['detail']}" for iss in active_issues]
                    logger.warning("[数据异常检测 %s] 发现 %d 条异常（仅记录）:", stock_code, len(active_issues))
                    for iss in active_issues:
                        logger.warning("  [%s] 日期=%s  %s", iss["type"], iss["date"], iss["detail"])
                    anomaly_details.append(f"{stock_code}(仅记录): " + "; ".join(issue_lines))

                logger.info("[数据异常检测] K线检测完成：共 %d 只股票，%d 只有异常，共 %d 条异常记录（仅记录，未修复）",
                            len(stock_codes), counter["anomalies"], total_issues)

        except Exception as e:
            logger.error("[数据异常检测] K线检测异常: %s", e, exc_info=True)

        # ── 第二阶段：资金流向强一致性校验 ──
        ff_counter = {"checked": 0, "anomalies": 0, "repaired": 0}
        try:
            stock_codes = stock_codes if stock_codes else [c for c in get_all_stock_codes() if not c.endswith('.BJ')]
            counter["phase"] = "fund_flow"
            counter["phase_label"] = "资金流向校验"
            counter["ff_total"] = len(stock_codes)
            counter["ff_checked"] = 0
            logger.info("[资金流向校验] 开始对 %d 只股票执行强一致性校验...", len(stock_codes))

            for stock_code in stock_codes:
                ff_issues = check_fund_flow_db(stock_code)
                ff_counter["checked"] += 1
                counter["ff_checked"] = ff_counter["checked"]
                if ff_counter["checked"] % 50 == 0:
                    await asyncio.sleep(0)
                if not ff_issues:
                    continue

                # 过滤掉2024年之前的异常
                ff_issues = [i for i in ff_issues if _is_recent_issue(i)]
                if not ff_issues:
                    continue

                ff_counter["anomalies"] += 1
                issue_lines = [f"[{iss['type']}] 日期={iss['date']} {iss['detail']}" for iss in ff_issues]
                logger.info("[资金流向校验 %s] 发现 %d 条异常", stock_code, len(ff_issues))
                for iss in ff_issues:
                    logger.info("  [%s] 日期=%s  %s", iss["type"], iss["date"], iss["detail"])

                # 修复策略：
                # 1. 先从 K线同步 close_price / change_pct（K线是价格权威来源）
                # 2. 若仍有 net_flow 等资金流字段 NULL，评估数据源差异后拉取修复
                repaired = False
                try:
                    # 第一步：K线同步价格字段（一条 SQL 批量完成）
                    synced = sync_price_fields_from_kline(stock_code)
                    if synced > 0:
                        logger.info("[资金流向校验 %s] K线同步了 %d 条价格字段", stock_code, synced)

                    # 重新检测（价格问题应已消除，仅剩资金流字段 NULL）
                    re_issues = check_fund_flow_db(stock_code)
                    if not re_issues:
                        repaired = True
                        logger.info("[资金流向校验 %s] K线同步后检测通过 ✓", stock_code)
                    else:
                        # 第二步：仍有异常（net_flow NULL 等），拉取数据源修复资金流字段
                        stock_info = get_stock_info_by_code(stock_code)
                        if stock_info:
                            jqka_rows = None
                            em_data = None
                            try:
                                jqka_rows = await get_fund_flow_jqka(stock_info)
                            except Exception as je:
                                logger.warning("[资金流向校验 %s] 同花顺拉取失败: %s", stock_code, je)
                            try:
                                em_klines = await get_fund_flow_em(stock_info)
                                if em_klines:
                                    em_data = _convert_em_klines_to_dicts(em_klines)
                            except Exception as ee:
                                logger.warning("[资金流向校验 %s] 东方财富拉取失败: %s", stock_code, ee)

                            # 评估数据源差异并选择修复策略
                            recommendation = "jqka_recent_em_history"
                            if jqka_rows and em_data:
                                divergence = evaluate_source_divergence(jqka_rows, em_data)
                                recommendation = divergence["recommendation"]
                                logger.info("[资金流向校验 %s] 数据源差异评估: 重叠%d天 策略=%s 原因=%s",
                                            stock_code, divergence["overlap_count"],
                                            recommendation, divergence["reason"])

                            from dao import get_connection as _get_conn

                            if recommendation == "jqka_only":
                                if jqka_rows:
                                    conn = _get_conn()
                                    cursor = conn.cursor()
                                    try:
                                        batch_upsert_fund_flow(stock_code, jqka_rows, cursor=cursor)
                                        conn.commit()
                                    finally:
                                        cursor.close()
                                        conn.close()
                            else:
                                # jqka_recent_em_history 或 em_with_kline_fix
                                if em_data:
                                    conn = _get_conn()
                                    cursor = conn.cursor()
                                    try:
                                        batch_upsert_fund_flow(stock_code, em_data, cursor=cursor)
                                        conn.commit()
                                    finally:
                                        cursor.close()
                                        conn.close()
                                if jqka_rows and recommendation != "em_with_kline_fix":
                                    conn = _get_conn()
                                    cursor = conn.cursor()
                                    try:
                                        batch_upsert_fund_flow(stock_code, jqka_rows, cursor=cursor)
                                        conn.commit()
                                    finally:
                                        cursor.close()
                                        conn.close()

                            # 数据源写入后再次从 K线同步价格（数据源可能带入不准确的价格）
                            sync_price_fields_from_kline(stock_code)

                            re_issues2 = check_fund_flow_db(stock_code)
                            if not re_issues2:
                                repaired = True
                                logger.info("[资金流向校验 %s] 数据源修复后检测通过 ✓ (策略=%s)",
                                            stock_code, recommendation)
                            elif all(
                                i["type"] == "ff_null_field" and "net_flow" in i["detail"]
                                for i in re_issues2
                            ):
                                logger.info("[资金流向校验 %s] 仅剩 %d 条 net_flow NULL（数据源无此数据）",
                                            stock_code, len(re_issues2))
                except Exception as e:
                    logger.error("[资金流向校验 %s] 修复失败: %s", stock_code, e)

                status_tag = "已修复" if repaired else "未修复"
                anomaly_details.append(f"{stock_code}[资金流向]({status_tag}): " + "; ".join(issue_lines))
                if repaired:
                    ff_counter["repaired"] += 1
                    counter["repaired"] += 1
                counter["anomalies"] += 1

            logger.info("[资金流向校验] 完成：%d 只检测，%d 只有异常，%d 只已修复",
                        ff_counter["checked"], ff_counter["anomalies"], ff_counter["repaired"])

        except Exception as e:
            logger.error("[资金流向校验] 执行异常: %s", e, exc_info=True)

        # ── 第三阶段：概念板块K线数据质量校验 ──
        board_kline_counter = {"checked": 0, "anomalies": 0, "repaired": 0}
        try:
            from dao import get_connection as _get_conn
            conn = _get_conn(use_dict_cursor=True)
            cur = conn.cursor()
            try:
                cur.execute("SELECT board_code, board_name, board_index_code "
                            "FROM stock_concept_board ORDER BY board_code")
                all_boards = cur.fetchall()
            finally:
                cur.close()
                conn.close()

            logger.info("[板块K线校验] 开始对 %d 个概念板块执行K线数据质量校验...", len(all_boards))
            counter["phase"] = "board_kline"
            counter["phase_label"] = "板块K线校验"
            counter["board_total"] = len(all_boards)
            counter["board_checked"] = 0

            for board in all_boards:
                board_code = board["board_code"]
                board_name = board["board_name"]
                index_code = board.get("board_index_code")
                board_kline_counter["checked"] += 1
                counter["board_checked"] = board_kline_counter["checked"]

                if board_kline_counter["checked"] % 50 == 0:
                    await asyncio.sleep(0)

                issues = _check_board_kline(board_code, board_name, index_code)
                if not issues:
                    continue

                board_kline_counter["anomalies"] += 1
                counter["anomalies"] += 1
                issue_lines = [f"[{iss['type']}] {iss['detail']}" for iss in issues]
                logger.warning("[板块K线校验 %s(%s)] 发现 %d 条异常:",
                               board_code, board_name, len(issues))
                for iss in issues:
                    logger.warning("  [%s] %s", iss["type"], iss["detail"])

                # 尝试修复：重新拉取板块K线
                repaired = await _repair_board_kline(board_code, board_name, index_code)
                status_tag = "已修复" if repaired else "未修复"
                anomaly_details.append(
                    f"{board_code}({board_name})[板块K线]({status_tag}): " + "; ".join(issue_lines)
                )
                if repaired:
                    board_kline_counter["repaired"] += 1
                    counter["repaired"] += 1

            logger.info("[板块K线校验] 完成：%d 个板块检测，%d 个有异常，%d 个已修复",
                        board_kline_counter["checked"], board_kline_counter["anomalies"],
                        board_kline_counter["repaired"])

        except Exception as e:
            logger.error("[板块K线校验] 执行异常: %s", e, exc_info=True)

        # ── 第四阶段：盘口数据校验 ──
        ob_counter = {"checked": 0, "anomalies": 0}
        try:
            from dao import get_connection as _get_conn
            conn = _get_conn(use_dict_cursor=True)
            cur = conn.cursor()
            try:
                # 获取最近30个交易日有K线数据的日期
                cur.execute(
                    "SELECT DISTINCT `date` FROM stock_kline "
                    "WHERE `date` >= DATE_SUB(CURDATE(), INTERVAL 60 DAY) "
                    "ORDER BY `date` DESC LIMIT 30"
                )
                recent_dates = [str(r["date"]) for r in cur.fetchall()]
            finally:
                cur.close()
                conn.close()

            if recent_dates and stock_codes:
                ob_stock_codes = [c for c in stock_codes if not c.endswith('.BJ')]
                counter["phase"] = "order_book"
                counter["phase_label"] = "盘口数据校验"
                counter["ob_total"] = len(ob_stock_codes)
                counter["ob_checked"] = 0
                logger.info("[盘口数据校验] 开始对 %d 只股票校验最近 %d 个交易日...",
                            len(ob_stock_codes), len(recent_dates))

                # 批量查询：哪些(stock_code, date)有盘口数据
                conn = _get_conn(use_dict_cursor=True)
                cur = conn.cursor()
                try:
                    ph_dates = ",".join(["%s"] * len(recent_dates))
                    # 统计每只股票在最近交易日中有盘口数据的天数
                    cur.execute(
                        f"SELECT stock_code, COUNT(*) AS cnt "
                        f"FROM stock_order_book "
                        f"WHERE trade_date IN ({ph_dates}) AND prev_close > 0 "
                        f"GROUP BY stock_code",
                        recent_dates,
                    )
                    ob_count_map = {r["stock_code"].split(".")[0]: r["cnt"] for r in cur.fetchall()}

                    # 统计每只股票在最近交易日中有K线数据的天数
                    cur.execute(
                        f"SELECT stock_code, COUNT(*) AS cnt "
                        f"FROM stock_kline "
                        f"WHERE `date` IN ({ph_dates}) "
                        f"GROUP BY stock_code",
                        recent_dates,
                    )
                    kline_count_map = {r["stock_code"]: r["cnt"] for r in cur.fetchall()}
                finally:
                    cur.close()
                    conn.close()

                for code in ob_stock_codes:
                    ob_counter["checked"] += 1
                    counter["ob_checked"] = ob_counter["checked"]
                    if ob_counter["checked"] % 100 == 0:
                        await asyncio.sleep(0)

                    pure_code = code.split(".")[0]
                    kline_days = kline_count_map.get(code, 0)
                    ob_days = ob_count_map.get(pure_code, 0)

                    if kline_days > 0 and ob_days == 0:
                        ob_counter["anomalies"] += 1
                        counter["anomalies"] += 1
                        anomaly_details.append(f"{code}[盘口] 最近{len(recent_dates)}个交易日无盘口数据")
                    elif kline_days > 5 and ob_days < kline_days * 0.5:
                        ob_counter["anomalies"] += 1
                        counter["anomalies"] += 1
                        anomaly_details.append(
                            f"{code}[盘口] 盘口覆盖率低: K线{kline_days}天 盘口{ob_days}天"
                        )

                logger.info("[盘口数据校验] 完成：%d 只检测，%d 只有异常",
                            ob_counter["checked"], ob_counter["anomalies"])

        except Exception as e:
            logger.error("[盘口数据校验] 执行异常: %s", e, exc_info=True)

        # ── 第五阶段：分时数据校验 ──
        td_counter = {"checked": 0, "anomalies": 0}
        try:
            from dao import get_connection as _get_conn

            if recent_dates and stock_codes:
                td_stock_codes = [c for c in stock_codes if not c.endswith('.BJ')]
                counter["phase"] = "time_data"
                counter["phase_label"] = "分时数据校验"
                counter["td_total"] = len(td_stock_codes)
                counter["td_checked"] = 0
                logger.info("[分时数据校验] 开始对 %d 只股票校验最近 %d 个交易日...",
                            len(td_stock_codes), len(recent_dates))

                # 批量查询：每只股票在最近交易日中有分时数据的天数和平均条数
                conn = _get_conn(use_dict_cursor=True)
                cur = conn.cursor()
                try:
                    ph_dates = ",".join(["%s"] * len(recent_dates))
                    cur.execute(
                        f"SELECT stock_code, COUNT(DISTINCT trade_date) AS day_cnt, "
                        f"COUNT(*) AS row_cnt "
                        f"FROM stock_time_data "
                        f"WHERE trade_date IN ({ph_dates}) "
                        f"GROUP BY stock_code",
                        recent_dates,
                    )
                    td_stats_map = {r["stock_code"]: r for r in cur.fetchall()}
                finally:
                    cur.close()
                    conn.close()

                for code in td_stock_codes:
                    td_counter["checked"] += 1
                    counter["td_checked"] = td_counter["checked"]
                    if td_counter["checked"] % 100 == 0:
                        await asyncio.sleep(0)

                    kline_days = kline_count_map.get(code, 0)
                    td_stat = td_stats_map.get(code)

                    if kline_days > 0 and not td_stat:
                        td_counter["anomalies"] += 1
                        counter["anomalies"] += 1
                        anomaly_details.append(f"{code}[分时] 最近{len(recent_dates)}个交易日无分时数据")
                    elif td_stat and kline_days > 5:
                        td_days = td_stat["day_cnt"]
                        avg_rows = td_stat["row_cnt"] / td_days if td_days > 0 else 0
                        if td_days < kline_days * 0.5:
                            td_counter["anomalies"] += 1
                            counter["anomalies"] += 1
                            anomaly_details.append(
                                f"{code}[分时] 覆盖率低: K线{kline_days}天 分时{td_days}天"
                            )
                        elif avg_rows < 100:
                            # 正常交易日应有 ~240 条分时数据，低于100条说明数据不完整
                            td_counter["anomalies"] += 1
                            counter["anomalies"] += 1
                            anomaly_details.append(
                                f"{code}[分时] 数据稀疏: 平均每天{avg_rows:.0f}条（正常~240条）"
                            )

                logger.info("[分时数据校验] 完成：%d 只检测，%d 只有异常",
                            td_counter["checked"], td_counter["anomalies"])

        except Exception as e:
            logger.error("[分时数据校验] 执行异常: %s", e, exc_info=True)

        now_str = datetime.now(_CST).strftime("%Y-%m-%d %H:%M:%S")
        _job_status.update({
            "last_run_time": now_str,
            "last_run_date": today_str,
            "last_success": True,
            "check_total": counter.get("total", 0),
            "check_anomalies": counter.get("anomalies", 0),
            "check_repaired": counter.get("repaired", 0),
            "running": False,
            "_counter": None,
        })

        _save_persisted_status(_job_status)

        detail = (f"共{counter['total']}只股票 异常{counter['anomalies']}只 "
                  f"已修复{counter['repaired']}只\n"
                  f"资金流向校验: {ff_counter['checked']}只检测 "
                  f"异常{ff_counter['anomalies']}只 已修复{ff_counter['repaired']}只\n"
                  f"板块K线校验: {board_kline_counter['checked']}个板块检测 "
                  f"异常{board_kline_counter['anomalies']}个 已修复{board_kline_counter['repaired']}个\n"
                  f"盘口数据校验: {ob_counter['checked']}只检测 异常{ob_counter['anomalies']}只\n"
                  f"分时数据校验: {td_counter['checked']}只检测 异常{td_counter['anomalies']}只")
        if anomaly_details:
            detail += "\n" + "\n".join(anomaly_details)
        # 防止 detail 超出数据库列长度限制
        if len(detail.encode('utf-8')) > 60000:
            detail = detail[:20000] + f"\n... (截断，共{len(anomaly_details)}条异常记录)"
        update_log(log_id, "success", counter["total"], counter["total"],
                   counter["anomalies"] - counter["repaired"], detail=detail)

        logger.info("[数据异常检测] 完成：共 %d 只股票，%d 只有异常，%d 只已修复",
                    counter["total"], counter["anomalies"], counter["repaired"])

    except Exception as e:
        import traceback as _tb
        err_msg = f"任务异常终止: {type(e).__name__}: {e}"
        err_detail = f"{err_msg}\n{_tb.format_exc()}"
        logger.error("[数据异常检测] %s", err_msg, exc_info=True)
        _job_status.update({"error": err_msg, "_counter": None})
        try:
            update_log(log_id, "failed", detail=err_detail)
        except Exception:
            pass
    finally:
        _job_status["running"] = False


async def _scheduler_loop():
    """调度主循环：等待所有数据拉取任务完成后执行"""
    while True:
        try:
            # 等待所有数据拉取任务完成（kline, price, market_data, us_market, fund_flow, concept_strength, weekly_prediction）
            await wait_all_data_jobs_done()

            if _already_done_today():
                logger.info("[数据异常检测] 今日已完成，跳过")
                # 等待下一轮信号（需要等事件被重置）
                await asyncio.sleep(3600)
                continue

            logger.info("[数据异常检测] 所有数据任务已完成，开始执行数据异常检测")
            async with scheduler_lock:
                logger.info("[数据异常检测] 已获取全局调度锁")
                await _execute_job()

            # 执行完后等待较长时间，避免重复触发
            await asyncio.sleep(3600)

        except asyncio.CancelledError:
            logger.info("[数据异常检测] 调度循环被取消")
            break
        except Exception as e:
            logger.error("[数据异常检测] 调度循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_db_check_scheduler():
    """启动数据异常检测调度器"""

    async def _deferred_start():
        await app_ready.wait()
        logger.info("[数据异常检测] 应用已就绪，调度器开始工作")

        # 启动时检查：如果所有数据任务今日已完成但异常检测未执行 → 补拉
        if not _already_done_today():
            from service.auto_job.kline_data_scheduler import get_job_status as get_kline_status
            kline_status = get_kline_status()
            today_str = datetime.now(_CST).date().isoformat()
            if (kline_status.get("last_run_date") == today_str
                    and not kline_status.get("running")):
                logger.info("[数据异常检测] 启动补拉：日线今日已完成但异常检测未执行，将在5秒后执行")
                async def _delayed_execute():
                    await asyncio.sleep(5)
                    async with scheduler_lock:
                        logger.info("[数据异常检测] 已获取全局调度锁（补拉）")
                        await _execute_job()
                asyncio.create_task(_delayed_execute())

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[数据异常检测] 调度器已注册，等待应用就绪")


if __name__ == "__main__":
    asyncio.run(_execute_job())
