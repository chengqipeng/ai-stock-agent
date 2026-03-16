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
12. close_price 与 K线表不一致（容差 0.02）
13. change_pct 与 K线表不一致（容差 0.5 个百分点）
14. 资金流向关键字段缺失：close_price / change_pct / net_flow 为 NULL

注意：资金守恒（net_flow = big+mid+small）和占比守恒不作为校验规则，
因为同花顺数据源的统计口径与东方财富不同，混合数据源下守恒关系不成立。

概念板块K线数据质量校验：
15. 无指数代码：board_index_code 为空，无法拉取K线
16. 无K线数据：concept_board_kline 表中无记录
17. K线过少：不足10条
18. 数据过旧：最新K线距大盘最新日期超过7天
19. 价格逻辑异常：close_price <= 0 / high < low
20. 日期重复：同一板块存在重复日期

发现K线异常时：调用 get_stock_day_kline_10jqka 重新拉取数据，重新检测，
若通过则覆盖写入数据库，否则输出日志。
发现资金流向异常时：先同花顺增量修复，若仍有异常则东方财富全量修复+同花顺覆盖，
重新检测，若通过则标记已修复，否则输出日志。
"""
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from service.auto_job.kline_data_scheduler import app_ready, kline_done_event_for_dbcheck

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

# ─────────── 状态持久化 ───────────
_STATUS_FILE = Path(__file__).parent.parent.parent / "data_results" / ".db_anomalies_scheduler_status.json"


def _load_persisted_status() -> dict:
    try:
        if _STATUS_FILE.exists():
            data = json.loads(_STATUS_FILE.read_text(encoding="utf-8"))
            logger.info("[数据异常检测] 从文件恢复状态: last_run_date=%s", data.get("last_run_date"))
            return data
    except Exception as e:
        logger.warning("[数据异常检测] 读取状态文件失败: %s", e)
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
        logger.warning("[数据异常检测] 写入状态文件失败: %s", e)


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
    from dao.stock_kline_dao import check_db, save_kline_to_db, get_all_stock_codes
    from dao.stock_fund_flow_dao import check_fund_flow_db, batch_upsert_fund_flow
    from service.jqka10.stock_day_kline_data_10jqka import get_stock_day_kline_10jqka
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

    counter = {"total": 0, "checked": 0, "anomalies": 0, "repaired": 0}
    _job_status["_counter"] = counter

    _ALL_FIELDS = ("date", "open_price", "close_price", "high_price", "low_price",
                   "trading_volume", "trading_amount", "amplitude", "change_percent",
                   "change_amount", "change_hand")

    async def _repair_stock(stock_code: str, active_issues: list[dict]):
        """拉取最新数据，重新检测，通过则保存，否则记录日志"""
        logger.info("[数据异常检测 %s] 发现 %d 条异常，开始重新拉取数据...",
                    stock_code, len(active_issues))
        for iss in active_issues:
            logger.info("  [%s] 日期=%s  %s", iss["type"], iss["date"], iss["detail"])

        try:
            stock_info = get_stock_info_by_code(stock_code)
        except Exception as e:
            logger.error("[数据异常检测 %s] 获取 StockInfo 失败: %s", stock_code, e)
            return False

        try:
            klines = await get_stock_day_kline_10jqka(stock_info, limit=800)
        except Exception as e:
            logger.error("[数据异常检测 %s] 拉取K线数据失败: %s", stock_code, e)
            return False

        if not klines:
            logger.error("[数据异常检测 %s] 拉取到空数据，跳过", stock_code)
            return False

        clean_klines = []
        for k in klines:
            empty_fields = [f for f in _ALL_FIELDS if k.get(f) is None or k.get(f) == ""]
            if empty_fields:
                logger.error("[数据异常检测 %s] K线数据存在空字段，date=%s, 空字段=%s，跳过该条",
                             stock_code, k.get("date"), empty_fields)
            else:
                clean_klines.append(k)

        if not clean_klines:
            logger.error("[数据异常检测 %s] 过滤空字段后无有效数据，跳过写入", stock_code)
            return False

        save_kline_to_db(stock_code, clean_klines)

        re_issues = check_db(stock_code)
        if not re_issues:
            logger.info("[数据异常检测 %s] 重新拉取后检测通过，数据已更新 ✓", stock_code)
            return True
        else:
            logger.warning("[数据异常检测 %s] 重新拉取后仍有 %d 条异常，请人工核查：",
                           stock_code, len(re_issues))
            for iss in re_issues:
                logger.warning("  [%s] 日期=%s  %s", iss["type"], iss["date"], iss["detail"])
            return False

    try:
        anomaly_details = []
        try:
            stock_codes = get_all_stock_codes()
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

                    active_issues = [i for i in issues if not i.get("legacy")]
                    if not active_issues:
                        continue

                    counter["anomalies"] += 1
                    total_issues += len(active_issues)

                    # 收集异常详情
                    issue_lines = [f"[{iss['type']}] 日期={iss['date']} {iss['detail']}" for iss in active_issues]
                    repaired = await _repair_stock(stock_code, active_issues)
                    status_tag = "已修复" if repaired else "未修复"
                    anomaly_details.append(f"{stock_code}({status_tag}): " + "; ".join(issue_lines))
                    if repaired:
                        counter["repaired"] += 1

                logger.info("[数据异常检测] K线检测完成：共 %d 只股票，%d 只有异常，共 %d 条异常记录",
                            len(stock_codes), counter["anomalies"], total_issues)

        except Exception as e:
            logger.error("[数据异常检测] K线检测异常: %s", e, exc_info=True)

        # ── 第二阶段：资金流向强一致性校验 ──
        ff_counter = {"checked": 0, "anomalies": 0, "repaired": 0}
        try:
            stock_codes = stock_codes if stock_codes else get_all_stock_codes()
            logger.info("[资金流向校验] 开始对 %d 只股票执行强一致性校验...", len(stock_codes))

            for stock_code in stock_codes:
                ff_issues = check_fund_flow_db(stock_code)
                ff_counter["checked"] += 1
                if ff_counter["checked"] % 50 == 0:
                    await asyncio.sleep(0)
                if not ff_issues:
                    continue

                ff_counter["anomalies"] += 1
                issue_lines = [f"[{iss['type']}] 日期={iss['date']} {iss['detail']}" for iss in ff_issues]
                logger.info("[资金流向校验 %s] 发现 %d 条异常", stock_code, len(ff_issues))
                for iss in ff_issues:
                    logger.info("  [%s] 日期=%s  %s", iss["type"], iss["date"], iss["detail"])

                # 尝试重新拉取资金流向数据修复
                # 先用同花顺增量覆盖最近30条，再检测；若仍有异常（旧数据），用东方财富全量覆盖
                repaired = False
                try:
                    stock_info = get_stock_info_by_code(stock_code)
                    if stock_info:
                        # 第一步：同花顺增量修复（~30条）
                        raw_rows = await get_fund_flow_jqka(stock_info)
                        if raw_rows:
                            from dao import get_connection as _get_conn
                            conn = _get_conn()
                            cursor = conn.cursor()
                            try:
                                batch_upsert_fund_flow(stock_code, raw_rows, cursor=cursor)
                                conn.commit()
                            finally:
                                cursor.close()
                                conn.close()
                        # 重新检测
                        re_issues = check_fund_flow_db(stock_code)
                        if not re_issues:
                            repaired = True
                            logger.info("[资金流向校验 %s] 同花顺增量修复后检测通过 ✓", stock_code)
                        else:
                            # 第二步：东方财富全量修复（~120条）
                            logger.info("[资金流向校验 %s] 同花顺增量修复后仍有 %d 条异常，尝试东方财富全量修复",
                                        stock_code, len(re_issues))
                            try:
                                em_klines = await get_fund_flow_em(stock_info)
                                if em_klines:
                                    em_data = _convert_em_klines_to_dicts(em_klines)
                                    if em_data:
                                        conn = _get_conn()
                                        cursor = conn.cursor()
                                        try:
                                            batch_upsert_fund_flow(stock_code, em_data, cursor=cursor)
                                            conn.commit()
                                        finally:
                                            cursor.close()
                                            conn.close()
                                        # 再用同花顺覆盖最近数据（同花顺数据更准确）
                                        if raw_rows:
                                            conn = _get_conn()
                                            cursor = conn.cursor()
                                            try:
                                                batch_upsert_fund_flow(stock_code, raw_rows, cursor=cursor)
                                                conn.commit()
                                            finally:
                                                cursor.close()
                                                conn.close()
                                        re_issues2 = check_fund_flow_db(stock_code)
                                        if not re_issues2:
                                            repaired = True
                                            logger.info("[资金流向校验 %s] 东方财富全量修复后检测通过 ✓", stock_code)
                                        else:
                                            logger.warning("[资金流向校验 %s] 全量修复后仍有 %d 条异常",
                                                           stock_code, len(re_issues2))
                            except Exception as em_err:
                                logger.warning("[资金流向校验 %s] 东方财富全量修复失败: %s", stock_code, em_err)
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

            for board in all_boards:
                board_code = board["board_code"]
                board_name = board["board_name"]
                index_code = board.get("board_index_code")
                board_kline_counter["checked"] += 1

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
                  f"异常{board_kline_counter['anomalies']}个 已修复{board_kline_counter['repaired']}个")
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
    """调度主循环：等待日线完成信号后执行"""
    while True:
        try:
            await kline_done_event_for_dbcheck.wait()
            kline_done_event_for_dbcheck.clear()

            if _already_done_today():
                logger.info("[数据异常检测] 今日已完成，跳过")
                continue

            logger.info("[数据异常检测] 收到日线完成信号，开始执行数据异常检测")
            await _execute_job()

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

        from service.auto_job.kline_data_scheduler import get_job_status as get_kline_status
        kline_status = get_kline_status()
        today_str = datetime.now(_CST).date().isoformat()
        if (kline_status.get("last_run_date") == today_str
                and not kline_status.get("running")
                and not _already_done_today()):
            logger.info("[数据异常检测] 启动补拉：日线今日已完成但异常检测未执行，将在5秒后执行")
            async def _delayed_execute():
                await asyncio.sleep(5)
                await _execute_job()
            asyncio.create_task(_delayed_execute())

        asyncio.create_task(_scheduler_loop())

    asyncio.create_task(_deferred_start())
    logger.info("[数据异常检测] 调度器已注册，等待应用就绪")


if __name__ == "__main__":
    asyncio.run(_execute_job())
