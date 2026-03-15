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

发现异常时：调用 get_stock_day_kline_10jqka 重新拉取数据，重新检测，
若通过则覆盖写入数据库，否则输出日志。
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


async def _execute_job():
    """执行一次数据异常检测与修复"""
    from dao.stock_kline_dao import check_db, save_kline_to_db, get_all_stock_codes
    from service.jqka10.stock_day_kline_data_10jqka import get_stock_day_kline_10jqka
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

                logger.info("[数据异常检测] 检测完成：共 %d 只股票，%d 只有异常，共 %d 条异常记录",
                            len(stock_codes), counter["anomalies"], total_issues)

        except Exception as e:
            logger.error("[数据异常检测] 执行异常: %s", e, exc_info=True)

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
                  f"已修复{counter['repaired']}只")
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
