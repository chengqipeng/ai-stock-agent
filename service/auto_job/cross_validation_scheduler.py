"""
数据交叉验证定时调度模块

- 每个A股交易日 18:00 自动触发
- 从数据库抽样股票，与新浪财经数据进行交叉对比
- 验证类别：日K线(30天)、财报、最高最低价(30天)、分时(当天)、盘口(当天)、资金流向(30天)
- 验证结果保存到 data_cross_validation / data_cross_validation_summary 表
"""
import asyncio
import json
import logging
import random
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from service.auto_job.kline_data_scheduler import app_ready, is_a_share_trading_day

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)
_project_root = Path(__file__).parent.parent.parent

# 全量验证（不再抽样）
SAMPLE_SIZE = 0  # 保留变量兼容，0 表示全量

# ─────────── 状态持久化 ───────────
_STATUS_FILE = _project_root / "data_results" / ".cross_validation_scheduler_status.json"


def _load_persisted_status() -> dict:
    """从数据库恢复状态，JSON 文件兜底"""
    from service.auto_job.scheduler_status_helper import restore_status
    return restore_status("cross_val", _STATUS_FILE)


def _save_persisted_status(status: dict):
    """持久化到数据库 + JSON 文件双写"""
    from service.auto_job.scheduler_status_helper import persist_status
    persist_status("cross_val", {
        "last_run_date": status.get("last_run_date"),
        "last_run_time": status.get("last_run_time"),
        "last_success": status.get("last_success"),
        "error": status.get("error"),
    })
    # JSON 文件兜底
    try:
        _STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: status.get(k) for k in ("last_run_date", "last_run_time", "last_success")}
        _STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


_job_status = {
    "running": False, "last_run_date": None, "last_run_time": None,
    "last_success": None, "error": None, "start_time": None,
    "stage": "",
    "categories_done": 0, "categories_total": 6,
    "current_category": "",
    "current_done": 0, "current_total": 0,
    "results": {},  # category -> {match_rate, total, match, mismatch, missing}
}
_persisted = _load_persisted_status()
_job_status.update({k: _persisted.get(k) for k in ("last_run_date", "last_run_time", "last_success") if _persisted.get(k) is not None})
_job_status["running"] = False
_job_status["error"] = None


def get_cross_validation_job_status() -> dict:
    return dict(_job_status)


def _already_done_today() -> bool:
    today_str = datetime.now(_CST).date().isoformat()
    return (_job_status.get("last_run_date") == today_str
            and _job_status.get("last_success") is True)


def _next_trigger_dt(after: datetime) -> datetime:
    d = after.date()
    trigger_time = dtime(18, 0)
    if after.time() < trigger_time and is_a_share_trading_day(d):
        return datetime.combine(d, trigger_time, tzinfo=_CST)
    d += timedelta(days=1)
    while not is_a_share_trading_day(d):
        d += timedelta(days=1)
    return datetime.combine(d, trigger_time, tzinfo=_CST)


# ─────────── 抽样股票 ───────────

def _sample_stocks(n: int = SAMPLE_SIZE) -> list[dict]:
    """获取全量股票列表（排除北交所）。n=0 表示全量，n>0 表示抽样。"""
    from service.auto_job.kline_data_scheduler import load_stocks_from_score_list
    stocks = load_stocks_from_score_list()
    stocks = [s for s in stocks if not s["code"].endswith(".BJ")]
    if n > 0 and len(stocks) > n:
        return random.sample(stocks, n)
    return stocks


# ─────────── 对比工具 ───────────

def _compare_float(db_val, sina_val, tolerance_pct=1.0) -> tuple[str, float | None]:
    """对比两个浮点数，返回 (match_status, diff_pct)"""
    if db_val is None and sina_val is None:
        return "match", None
    if db_val is None or sina_val is None:
        return "missing", None
    try:
        db_f = float(db_val)
        sina_f = float(sina_val)
    except (ValueError, TypeError):
        return "mismatch", None
    if db_f == 0 and sina_f == 0:
        return "match", 0.0
    base = max(abs(db_f), abs(sina_f), 0.01)
    diff = abs(db_f - sina_f) / base * 100
    status = "match" if diff <= tolerance_pct else "mismatch"
    return status, round(diff, 4)


def _compare_int(db_val, sina_val, tolerance_pct=2.0) -> tuple[str, float | None]:
    """对比两个整数值"""
    if db_val is None and sina_val is None:
        return "match", None
    if db_val is None or sina_val is None:
        return "missing", None
    try:
        db_i = int(float(db_val))
        sina_i = int(float(sina_val))
    except (ValueError, TypeError):
        return "mismatch", None
    if db_i == 0 and sina_i == 0:
        return "match", 0.0
    base = max(abs(db_i), abs(sina_i), 1)
    diff = abs(db_i - sina_i) / base * 100
    status = "match" if diff <= tolerance_pct else "mismatch"
    return status, round(diff, 4)


# ═══════════════════════════════════════════════════════════════
# 各类别验证逻辑
# ═══════════════════════════════════════════════════════════════

async def _validate_kline(stocks: list[dict], run_date: str) -> dict:
    """验证日K线数据（近30天）— 全量并发"""
    from dao import get_connection
    from common.utils.stock_info_utils import get_stock_info_by_code
    from service.sina.sina_cross_validate import fetch_sina_kline
    from dao.cross_validation_dao import batch_insert_validation_details

    details = []
    counter = {"total": 0, "match": 0, "mismatch": 0, "missing": 0}
    cutoff = (date.fromisoformat(run_date) - timedelta(days=45)).isoformat()
    sem = asyncio.Semaphore(5)
    _job_status["current_done"] = 0
    _job_status["current_total"] = len(stocks)

    async def _check_one(stock):
        code = stock["code"]
        stock_info = get_stock_info_by_code(code)
        if not stock_info:
            return

        async with sem:
            try:
                sina_klines = await fetch_sina_kline(stock_info, limit=60)
            except Exception as e:
                logger.warning("[交叉验证-K线] %s 新浪拉取失败: %s", code, e)
                return
            await asyncio.sleep(0.3)

        sina_map = {k["date"]: k for k in sina_klines if k["date"] >= cutoff}
        if not sina_map:
            return

        conn = get_connection(use_dict_cursor=True)
        cur = conn.cursor()
        try:
            dates = list(sina_map.keys())
            ph = ",".join(["%s"] * len(dates))
            cur.execute(
                f"SELECT `date`, open_price, close_price, high_price, low_price, trading_volume "
                f"FROM stock_kline WHERE stock_code = %s AND `date` IN ({ph})",
                (code, *dates))
            db_map = {str(r["date"]): r for r in cur.fetchall()}
        finally:
            cur.close()
            conn.close()

        for d, sina_row in sina_map.items():
            db_row = db_map.get(d)
            if not db_row:
                continue
            for field, tolerance in [("close_price", 0.5), ("open_price", 0.5),
                                     ("high_price", 0.5), ("low_price", 0.5),
                                     ("trading_volume", 5.0)]:
                db_v = db_row.get(field)
                sina_v = sina_row.get(field)
                cmp_fn = _compare_int if field == "trading_volume" else _compare_float
                status, diff = cmp_fn(db_v, sina_v, tolerance)
                counter["total"] += 1
                counter[status] += 1
                if status != "match":
                    details.append({
                        "run_date": run_date, "category": "kline", "stock_code": code,
                        "check_date": d, "field_name": field,
                        "db_value": db_v, "sina_value": sina_v,
                        "diff_pct": diff, "match_status": status,
                    })
        _job_status["current_done"] += 1

    # 分批并发
    batch_size = 20
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i + batch_size]
        await asyncio.gather(*[_check_one(s) for s in batch])

    if details:
        batch_insert_validation_details(details)
    total = counter["total"]
    rate = round(counter["match"] / total * 100, 2) if total > 0 else 0
    return {**counter, "match_rate": rate, "sample_stocks": len(stocks)}


async def _validate_order_book(stocks: list[dict], run_date: str) -> dict:
    """验证盘口数据（当天）— 批量获取新浪实时行情对比"""
    from dao import get_connection
    from common.utils.stock_info_utils import get_stock_info_by_code
    from service.sina.sina_cross_validate import fetch_sina_realtime_batch
    from dao.cross_validation_dao import batch_insert_validation_details

    details = []
    counter = {"total": 0, "match": 0, "mismatch": 0, "missing": 0}
    _job_status["current_done"] = 0
    _job_status["current_total"] = len(stocks)

    # 构建 stock_info 列表
    stock_infos = []
    code_map = {}
    for stock in stocks:
        code = stock["code"]
        si = get_stock_info_by_code(code)
        if si:
            stock_infos.append(si)
            code_map[code] = si

    # 批量获取新浪数据（每批50只）
    sina_all = {}
    batch_size = 50
    for i in range(0, len(stock_infos), batch_size):
        batch = stock_infos[i:i + batch_size]
        try:
            batch_result = await fetch_sina_realtime_batch(batch)
            sina_all.update(batch_result)
        except Exception as e:
            logger.warning("[交叉验证-盘口] 批量拉取失败(batch %d): %s", i, e)
        await asyncio.sleep(0.3)

    # 批量获取数据库盘口数据
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        all_pure_codes = [s["code"].split(".")[0] for s in stocks]
        ph = ",".join(["%s"] * len(all_pure_codes))
        cur.execute(
            f"SELECT stock_code, trade_date, current_price, open_price, high_price, "
            f"low_price, prev_close, buy1_price, sell1_price "
            f"FROM stock_order_book WHERE stock_code IN ({ph}) "
            f"AND trade_date = (SELECT MAX(trade_date) FROM stock_order_book)",
            all_pure_codes)
        db_map = {r["stock_code"]: r for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()

    for stock in stocks:
        code = stock["code"]
        code_norm = code.split(".")[0] + (".SH" if code.endswith(".SH") else ".SZ")
        pure_code = code.split(".")[0]

        sina = sina_all.get(code_norm)
        if not sina or sina.get("current_price", 0) == 0:
            _job_status["current_done"] += 1
            continue

        db_row = db_map.get(pure_code)
        if not db_row:
            _job_status["current_done"] += 1
            continue

        sina_date = sina.get("date", "")
        db_date = str(db_row.get("trade_date", ""))
        if sina_date and db_date and sina_date != db_date:
            _job_status["current_done"] += 1
            continue

        for field, sina_field, tolerance in [
            ("current_price", "current_price", 0.5),
            ("open_price", "open_price", 0.5),
            ("high_price", "high_price", 0.5),
            ("low_price", "low_price", 0.5),
            ("prev_close", "prev_close", 0.5),
            ("buy1_price", "buy1_price", 0.5),
            ("sell1_price", "sell1_price", 0.5),
        ]:
            db_v = db_row.get(field)
            sina_v = sina.get(sina_field)
            status, diff = _compare_float(db_v, sina_v, tolerance)
            counter["total"] += 1
            counter[status] += 1
            if status != "match":
                details.append({
                    "run_date": run_date, "category": "order_book", "stock_code": code,
                    "check_date": db_date, "field_name": field,
                    "db_value": db_v, "sina_value": sina_v,
                    "diff_pct": diff, "match_status": status,
                })
        _job_status["current_done"] += 1

    if details:
        batch_insert_validation_details(details)
    total = counter["total"]
    rate = round(counter["match"] / total * 100, 2) if total > 0 else 0
    return {**counter, "match_rate": rate, "sample_stocks": len(stocks)}


async def _validate_time_data(stocks: list[dict], run_date: str) -> dict:
    """验证分时数据（当天）— 批量获取新浪实时行情对比"""
    from dao import get_connection
    from common.utils.stock_info_utils import get_stock_info_by_code
    from service.sina.sina_cross_validate import fetch_sina_realtime_batch
    from dao.cross_validation_dao import batch_insert_validation_details

    details = []
    counter = {"total": 0, "match": 0, "mismatch": 0, "missing": 0}
    _job_status["current_done"] = 0
    _job_status["current_total"] = len(stocks)

    # 构建 stock_info 列表
    stock_infos = []
    for stock in stocks:
        si = get_stock_info_by_code(stock["code"])
        if si:
            stock_infos.append(si)

    # 批量获取新浪数据
    sina_all = {}
    batch_size = 50
    for i in range(0, len(stock_infos), batch_size):
        batch = stock_infos[i:i + batch_size]
        try:
            batch_result = await fetch_sina_realtime_batch(batch)
            sina_all.update(batch_result)
        except Exception as e:
            logger.warning("[交叉验证-分时] 批量拉取失败(batch %d): %s", i, e)
        await asyncio.sleep(0.3)

    # 批量获取数据库分时数据（每只股票最新一条）
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        all_pure_codes = [s["code"].split(".")[0] for s in stocks]
        ph = ",".join(["%s"] * len(all_pure_codes))
        cur.execute(
            f"SELECT t1.stock_code, t1.trade_date, t1.close_price "
            f"FROM stock_time_data t1 "
            f"INNER JOIN (SELECT stock_code, MAX(CONCAT(trade_date, ' ', `time`)) AS max_dt "
            f"            FROM stock_time_data WHERE stock_code IN ({ph}) "
            f"            GROUP BY stock_code) t2 "
            f"ON t1.stock_code = t2.stock_code "
            f"   AND CONCAT(t1.trade_date, ' ', t1.`time`) = t2.max_dt",
            all_pure_codes)
        db_map = {r["stock_code"]: r for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()

    for stock in stocks:
        code = stock["code"]
        code_norm = code.split(".")[0] + (".SH" if code.endswith(".SH") else ".SZ")
        pure_code = code.split(".")[0]

        sina = sina_all.get(code_norm)
        db_row = db_map.get(pure_code)

        if not sina or sina.get("current_price", 0) == 0 or not db_row:
            _job_status["current_done"] += 1
            continue

        sina_date = sina.get("date", "")
        db_date = str(db_row.get("trade_date", ""))
        if sina_date and db_date and sina_date != db_date:
            _job_status["current_done"] += 1
            continue

        db_v = db_row.get("close_price")
        sina_v = sina.get("current_price")
        status, diff = _compare_float(db_v, sina_v, 1.0)
        counter["total"] += 1
        counter[status] += 1
        if status != "match":
            details.append({
                "run_date": run_date, "category": "time_data", "stock_code": code,
                "check_date": db_date, "field_name": "close_price",
                "db_value": db_v, "sina_value": sina_v,
                "diff_pct": diff, "match_status": status,
            })
        _job_status["current_done"] += 1

    if details:
        batch_insert_validation_details(details)
    total = counter["total"]
    rate = round(counter["match"] / total * 100, 2) if total > 0 else 0
    return {**counter, "match_rate": rate, "sample_stocks": len(stocks)}


async def _validate_highest_lowest(stocks: list[dict], run_date: str) -> dict:
    """验证最高最低价（近30天）— 并发"""
    from dao import get_connection
    from common.utils.stock_info_utils import get_stock_info_by_code
    from service.sina.sina_cross_validate import fetch_sina_kline
    from dao.cross_validation_dao import batch_insert_validation_details

    details = []
    counter = {"total": 0, "match": 0, "mismatch": 0, "missing": 0}
    cutoff = (date.fromisoformat(run_date) - timedelta(days=45)).isoformat()
    sem = asyncio.Semaphore(5)
    _job_status["current_done"] = 0
    _job_status["current_total"] = len(stocks)

    async def _check_one(stock):
        code = stock["code"]
        stock_info = get_stock_info_by_code(code)
        if not stock_info:
            return

        async with sem:
            try:
                sina_klines = await fetch_sina_kline(stock_info, limit=60)
            except Exception as e:
                logger.warning("[交叉验证-最高最低价] %s 新浪拉取失败: %s", code, e)
                return
            await asyncio.sleep(0.3)

        if not sina_klines:
            return

        sina_map = {k["date"]: k for k in sina_klines if k["date"] >= cutoff}
        if not sina_map:
            return

        conn = get_connection(use_dict_cursor=True)
        cur = conn.cursor()
        try:
            dates = list(sina_map.keys())
            ph = ",".join(["%s"] * len(dates))
            cur.execute(
                f"SELECT `date`, high_price, low_price FROM stock_kline "
                f"WHERE stock_code = %s AND `date` IN ({ph})",
                (code, *dates))
            db_map = {str(r["date"]): r for r in cur.fetchall()}
        finally:
            cur.close()
            conn.close()

        for d, sina_row in sina_map.items():
            db_row = db_map.get(d)
            if not db_row:
                continue
            for field in ("high_price", "low_price"):
                db_v = db_row.get(field)
                sina_v = sina_row.get(field)
                status, diff = _compare_float(db_v, sina_v, 0.5)
                counter["total"] += 1
                counter[status] += 1
                if status != "match":
                    details.append({
                        "run_date": run_date, "category": "price", "stock_code": code,
                        "check_date": d, "field_name": field,
                        "db_value": db_v, "sina_value": sina_v,
                        "diff_pct": diff, "match_status": status,
                    })
        _job_status["current_done"] += 1

    batch_size = 20
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i + batch_size]
        await asyncio.gather(*[_check_one(s) for s in batch])

    if details:
        batch_insert_validation_details(details)
    total = counter["total"]
    rate = round(counter["match"] / total * 100, 2) if total > 0 else 0
    return {**counter, "match_rate": rate, "sample_stocks": len(stocks)}


async def _validate_finance(stocks: list[dict], run_date: str) -> dict:
    """验证财报数据 —— 检查数据库中财报记录的完整性和时效性"""
    _job_status["current_done"] = 0
    _job_status["current_total"] = len(stocks)
    import json as _json
    from dao import get_connection
    from dao.cross_validation_dao import batch_insert_validation_details

    details = []
    counter = {"total": 0, "match": 0, "mismatch": 0, "missing": 0}

    for stock in stocks:
        code = stock["code"]
        _job_status["current_done"] += 1
        conn = get_connection(use_dict_cursor=True)
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT report_date, data_json, updated_at "
                "FROM stock_finance WHERE stock_code = %s "
                "ORDER BY report_date DESC LIMIT 4", (code,))
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        if not rows:
            counter["total"] += 1
            counter["missing"] += 1
            details.append({
                "run_date": run_date, "category": "finance", "stock_code": code,
                "check_date": run_date, "field_name": "finance_data",
                "db_value": "无记录", "sina_value": "N/A",
                "diff_pct": None, "match_status": "missing",
            })
            continue

        # 检查最新财报是否在合理时间范围内（6个月内）
        latest = rows[0]
        report_date = str(latest.get("report_date", ""))
        if report_date:
            try:
                rd = date.fromisoformat(report_date)
                gap = (date.fromisoformat(run_date) - rd).days
                counter["total"] += 1
                if gap <= 180:
                    counter["match"] += 1
                else:
                    counter["mismatch"] += 1
                    details.append({
                        "run_date": run_date, "category": "finance", "stock_code": code,
                        "check_date": report_date, "field_name": "report_freshness",
                        "db_value": f"{gap}天前", "sina_value": "应<=180天",
                        "diff_pct": None, "match_status": "mismatch",
                    })
            except ValueError:
                counter["total"] += 1
                counter["missing"] += 1

        # 解析 data_json 检查关键字段非空
        data_json_str = latest.get("data_json", "{}")
        try:
            finance_data = _json.loads(data_json_str) if isinstance(data_json_str, str) else {}
        except (_json.JSONDecodeError, TypeError):
            finance_data = {}

        # 检查常见财报关键字段非空
        # 同花顺返回的 key 带单位后缀，如 "营业总收入(元)"，值可能是 "2.28亿" 字符串
        # 净利润的 key 是 "归母净利润(元)"
        for field_candidates, field_label in [
            (["营业总收入(元)", "营业总收入"], "revenue"),
            (["归母净利润(元)", "净利润(元)", "净利润"], "net_profit"),
        ]:
            counter["total"] += 1
            val = None
            for fc in field_candidates:
                val = finance_data.get(fc)
                if val is not None and val != "" and val != "None":
                    break
            if val is not None and val != "" and val != "None":
                counter["match"] += 1
            else:
                counter["missing"] += 1
                details.append({
                    "run_date": run_date, "category": "finance", "stock_code": code,
                    "check_date": report_date, "field_name": field_label,
                    "db_value": val, "sina_value": "应非空",
                    "diff_pct": None, "match_status": "missing",
                })

    if details:
        batch_insert_validation_details(details)
    total = counter["total"]
    rate = round(counter["match"] / total * 100, 2) if total > 0 else 0
    return {**counter, "match_rate": rate, "sample_stocks": len(stocks)}


async def _validate_fund_flow(stocks: list[dict], run_date: str) -> dict:
    """验证资金流向（近30天）—— 用新浪K线的 close_price/change_pct 交叉验证资金流向表中的价格字段"""
    from dao import get_connection
    from common.utils.stock_info_utils import get_stock_info_by_code
    from service.sina.sina_cross_validate import fetch_sina_kline
    from dao.cross_validation_dao import batch_insert_validation_details

    details = []
    counter = {"total": 0, "match": 0, "mismatch": 0, "missing": 0}
    cutoff = (date.fromisoformat(run_date) - timedelta(days=45)).isoformat()

    for stock in stocks:
        code = stock["code"]
        stock_info = get_stock_info_by_code(code)
        if not stock_info:
            continue

        # 从新浪拉取K线作为第三方基准
        try:
            sina_klines = await fetch_sina_kline(stock_info, limit=60)
        except Exception as e:
            logger.warning("[交叉验证-资金流向] %s 新浪拉取失败: %s", code, e)
            continue

        sina_map = {k["date"]: k for k in sina_klines if k["date"] >= cutoff}
        if not sina_map:
            continue

        # 从数据库取资金流向中的价格字段
        conn = get_connection(use_dict_cursor=True)
        cur = conn.cursor()
        try:
            dates = list(sina_map.keys())
            ph = ",".join(["%s"] * len(dates))
            cur.execute(
                f"SELECT `date`, close_price, change_pct, net_flow, big_net "
                f"FROM stock_fund_flow WHERE stock_code = %s AND `date` IN ({ph})",
                (code, *dates))
            ff_map = {str(r["date"]): r for r in cur.fetchall()}
        finally:
            cur.close()
            conn.close()

        if not ff_map:
            counter["total"] += 1
            counter["missing"] += 1
            continue

        for d, sina_row in sina_map.items():
            ff_row = ff_map.get(d)
            if not ff_row:
                continue

            # 对比资金流向表的 close_price 与新浪 close_price
            status, diff = _compare_float(ff_row.get("close_price"), sina_row.get("close_price"), 0.5)
            counter["total"] += 1
            counter[status] += 1
            if status != "match":
                details.append({
                    "run_date": run_date, "category": "fund_flow", "stock_code": code,
                    "check_date": d, "field_name": "close_price_vs_sina",
                    "db_value": ff_row.get("close_price"), "sina_value": sina_row.get("close_price"),
                    "diff_pct": diff, "match_status": status,
                })

            # 对比资金流向表的 change_pct 与新浪计算的 change_percent
            sina_chg = sina_row.get("change_percent")
            if sina_chg is not None:
                status, diff = _compare_float(ff_row.get("change_pct"), sina_chg, 2.0)
                counter["total"] += 1
                counter[status] += 1
                if status != "match":
                    details.append({
                        "run_date": run_date, "category": "fund_flow", "stock_code": code,
                        "check_date": d, "field_name": "change_pct_vs_sina",
                        "db_value": ff_row.get("change_pct"), "sina_value": sina_chg,
                        "diff_pct": diff, "match_status": status,
                    })

            # 检查资金流向关键字段非空
            for ff_field in ("net_flow", "big_net"):
                counter["total"] += 1
                val = ff_row.get(ff_field)
                if val is not None:
                    counter["match"] += 1
                else:
                    counter["missing"] += 1
                    details.append({
                        "run_date": run_date, "category": "fund_flow", "stock_code": code,
                        "check_date": d, "field_name": ff_field,
                        "db_value": val, "sina_value": "应非空",
                        "diff_pct": None, "match_status": "missing",
                    })

        await asyncio.sleep(0.5)

    if details:
        batch_insert_validation_details(details)
    total = counter["total"]
    rate = round(counter["match"] / total * 100, 2) if total > 0 else 0
    return {**counter, "match_rate": rate, "sample_stocks": len(stocks)}


# ═══════════════════════════════════════════════════════════════
# 主执行逻辑
# ═══════════════════════════════════════════════════════════════

_CATEGORIES = [
    ("kline", "日K线", _validate_kline),
    ("order_book", "盘口数据", _validate_order_book),
    ("time_data", "分时数据", _validate_time_data),
    ("price", "最高最低价", _validate_highest_lowest),
    ("finance", "财报数据", _validate_finance),
    ("fund_flow", "资金流向", _validate_fund_flow),
]


async def _execute_job_inner():
    from dao.cross_validation_dao import (
        create_cross_validation_table, upsert_validation_summary,
    )
    from dao.scheduler_log_dao import insert_log, update_log

    _job_status["running"] = True
    _job_status["error"] = None
    _job_status["categories_done"] = 0
    _job_status["categories_total"] = len(_CATEGORIES)
    _job_status["results"] = {}
    _job_status["current_done"] = 0
    _job_status["current_total"] = 0
    start_time = datetime.now(_CST)

    # 确定验证目标日期：最近一个交易日
    target = start_time.date()
    if not is_a_share_trading_day(target) or start_time.time() < dtime(15, 30):
        target = target - timedelta(days=1)
        while not is_a_share_trading_day(target):
            target = target - timedelta(days=1)
    run_date = target.isoformat()

    _job_status["last_run_date"] = run_date
    _job_status["last_run_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    _job_status["start_time"] = start_time.isoformat()

    log_id = insert_log("数据交叉验证", start_time)
    logger.info("[交叉验证调度] ===== 开始执行 验证日期=%s =====", run_date)

    try:
        create_cross_validation_table()
        stocks = _sample_stocks(SAMPLE_SIZE)
        logger.info("[交叉验证调度] 全量验证 %d 只股票", len(stocks))

        all_results = {}
        total_checks = 0
        total_match = 0
        total_mismatch = 0

        for cat_key, cat_label, validate_fn in _CATEGORIES:
            _job_status["current_category"] = cat_label
            _job_status["stage"] = cat_key
            cat_start = datetime.now(_CST)
            logger.info("[交叉验证调度] 开始验证: %s", cat_label)

            try:
                result = await validate_fn(stocks, run_date)
                cat_end = datetime.now(_CST)
                duration = int((cat_end - cat_start).total_seconds())

                upsert_validation_summary({
                    "run_date": run_date, "category": cat_key,
                    "total_checks": result.get("total", 0),
                    "match_count": result.get("match", 0),
                    "mismatch_count": result.get("mismatch", 0),
                    "missing_count": result.get("missing", 0),
                    "match_rate": result.get("match_rate"),
                    "sample_stocks": result.get("sample_stocks", 0),
                    "started_at": cat_start, "finished_at": cat_end,
                    "duration_seconds": duration,
                })

                all_results[cat_key] = result
                total_checks += result.get("total", 0)
                total_match += result.get("match", 0)
                total_mismatch += result.get("mismatch", 0)
                _job_status["results"][cat_key] = result

                logger.info("[交叉验证调度] %s 完成: 匹配率%.1f%% (%d/%d) 耗时%ds",
                            cat_label, result.get("match_rate", 0),
                            result.get("match", 0), result.get("total", 0), duration)

            except Exception as e:
                logger.error("[交叉验证调度] %s 验证异常: %s", cat_label, e, exc_info=True)
                all_results[cat_key] = {"error": str(e)}

            _job_status["categories_done"] += 1

        _job_status["last_success"] = True
        elapsed = int((datetime.now(_CST) - start_time).total_seconds())

        overall_rate = round(total_match / total_checks * 100, 2) if total_checks > 0 else 0
        detail_lines = [f"全量{len(stocks)}只股票 验证日期{run_date} 总检查{total_checks}项 "
                        f"匹配{total_match} 不匹配{total_mismatch} 总匹配率{overall_rate}%"]
        for cat_key, cat_label, _ in _CATEGORIES:
            r = all_results.get(cat_key, {})
            if "error" in r:
                detail_lines.append(f"  {cat_label}: 异常 - {r['error']}")
            else:
                detail_lines.append(f"  {cat_label}: {r.get('match_rate', 0)}% "
                                    f"({r.get('match', 0)}/{r.get('total', 0)})")

        update_log(log_id, "success", total_checks, total_match,
                   total_mismatch, detail="\n".join(detail_lines))
        logger.info("[交叉验证调度] ===== 完成 总匹配率%.1f%% 耗时%ds =====", overall_rate, elapsed)

    except Exception as e:
        import traceback as _tb
        err_msg = f"{type(e).__name__}: {e}"
        _job_status["last_success"] = False
        _job_status["error"] = err_msg
        logger.error("[交叉验证调度] 执行异常: %s", err_msg, exc_info=True)
        try:
            update_log(log_id, "failed", detail=f"{err_msg}\n{_tb.format_exc()}")
        except Exception:
            pass
    finally:
        _job_status["running"] = False
        _job_status["stage"] = ""
        _job_status["current_category"] = ""
        _job_status["start_time"] = None
        _save_persisted_status(_job_status)


async def _execute_job(manual=False):
    from service.auto_job.scheduler_orchestrator import manual_semaphore, scheduler_lock
    if manual:
        _job_status["running"] = True
        _job_status["error"] = "等待手动调度槽位..."
        async with manual_semaphore:
            _job_status["error"] = None
            await _execute_job_inner()
    else:
        async with scheduler_lock:
            await _execute_job_inner()


# ═══════════════════════════════════════════════════════════════
# 调度循环
# ═══════════════════════════════════════════════════════════════

async def _scheduler_loop():
    while True:
        try:
            now = datetime.now(_CST)
            next_dt = _next_trigger_dt(now)
            wait_secs = (next_dt - now).total_seconds()
            if wait_secs > 0:
                logger.info("[交叉验证调度] 下次执行: %s (%.0f秒后)",
                            next_dt.strftime("%Y-%m-%d %H:%M"), wait_secs)
                await asyncio.sleep(wait_secs)
            if not is_a_share_trading_day(datetime.now(_CST).date()):
                continue
            if _already_done_today():
                await asyncio.sleep(60)
                continue
            await _execute_job()
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("[交叉验证调度] 循环异常: %s", e, exc_info=True)
            await asyncio.sleep(300)


async def start_cross_validation_scheduler():
    async def _deferred_start():
        await app_ready.wait()
        logger.info("[交叉验证调度] 应用已就绪，调度器开始工作")
        if is_a_share_trading_day(datetime.now(_CST).date()) and not _already_done_today():
            now = datetime.now(_CST)
            if now.time() >= dtime(18, 0):
                logger.info("[交叉验证调度] 启动补拉，20秒后执行")
                async def _delayed():
                    await asyncio.sleep(20)
                    await _execute_job()
                asyncio.create_task(_delayed())
        asyncio.create_task(_scheduler_loop())
    asyncio.create_task(_deferred_start())
    logger.info("[交叉验证调度] 调度器已注册，等待应用就绪")
