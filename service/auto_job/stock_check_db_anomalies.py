"""
批量检测 stock_*.db 数据异常脚本

检测场景：
1. 价格逻辑错误：close_price <= 0 / open_price <= 0 / high_price <= 0 / low_price <= 0
2. 价格关系异常：high < low / close > high / close < low / open > high / open < low
3. 交易量/金额异常：trading_volume < 0 / trading_amount < 0
4. 涨跌幅异常：|change_percent| > 21%（A股单日涨跌幅上限，ST股10%，新股除外）
5. 日期格式异常：date 不符合 YYYY-MM-DD 格式
6. 日期重复：同一股票存在重复日期
7. 缺失交易日：相邻两条记录间存在未记录的交易日
8. 停牌占位记录异常：全零记录中存在非零字段
9. 数据库无表或表为空
10. 非交易日数据：date 对应非交易日（周末/节假日），立即删除并打印日志

发现异常时：调用 get_stock_day_kline_10jqka 重新拉取数据，重新检测，
若通过则覆盖写入数据库，否则输出日志。
"""

import sys
import asyncio
import logging
import sqlite3
import re
from datetime import date, timedelta
from pathlib import Path
from chinese_calendar import is_workday

sys.path.insert(0, str(Path(__file__).parent.parent))

from dao.stock_kline_dao import (
    get_db_path_for_stock, create_kline_table, batch_insert_or_update_kline_data
)
from service.jqka10.stock_day_kline_data_10jqka import get_stock_day_kline_10jqka
from common.utils.stock_info_utils import get_stock_info_by_code

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

DB_DIR = Path(__file__).parent.parent / "data_results/sql_lite"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_trading_day(d: date) -> bool:
    try:
        return d.weekday() < 5 and is_workday(d)
    except Exception as e:
        logger.warning("_is_trading_day 判断失败 [%s]: %s", d, e)
        return True  # 无法判断时保守处理，不删除


def check_db(db_path: Path) -> list[dict]:
    stock_code = db_path.stem.removeprefix("stock_").replace("_", ".")
    table_name = f"kline_{db_path.stem.removeprefix('stock_')}"
    issues = []

    def issue(row_date, anomaly_type, detail):
        issues.append({"stock_code": stock_code, "date": row_date, "type": anomaly_type, "detail": detail,
                       "legacy": bool(row_date and str(row_date) < "2025-07-01")})

    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
    except Exception as e:
        return [{"stock_code": stock_code, "date": None, "type": "DB_OPEN_ERROR", "detail": str(e)}]

    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
    except sqlite3.OperationalError:
        conn.close()
        return [{"stock_code": stock_code, "date": None, "type": "TABLE_MISSING", "detail": f"表 {table_name} 不存在"}]

    if count == 0:
        conn.close()
        return [{"stock_code": stock_code, "date": None, "type": "TABLE_EMPTY", "detail": "表为空，无任何数据"}]

    cur.execute(
        f"SELECT date, open_price, close_price, high_price, low_price, "
        f"trading_volume, trading_amount, change_percent FROM {table_name} ORDER BY date ASC"
    )
    rows = cur.fetchall()

    seen_dates = {}
    prev_date = None

    for row in rows:
        d = row["date"]
        op, cp, hp, lp = row["open_price"], row["close_price"], row["high_price"], row["low_price"]
        vol, amt, chg = row["trading_volume"], row["trading_amount"], row["change_percent"]

        # 1. 日期格式
        if not DATE_RE.match(str(d)):
            issue(d, "INVALID_DATE_FORMAT", f"日期格式异常: {d}")
            continue

        # 非交易日数据：立即删除
        try:
            row_date = date.fromisoformat(d)
            if not _is_trading_day(row_date):
                cur.execute(f"DELETE FROM {table_name} WHERE date = ?", (d,))
                conn.commit()
                log.warning("[%s] 删除非交易日数据: date=%s", stock_code, d)
                continue
        except ValueError as e:
            log.debug("[%s] 日期解析失败: date=%s, %s", stock_code, d, e)

        # 2. 日期重复
            issue(d, "DUPLICATE_DATE", f"日期重复出现")
        seen_dates[d] = True

        is_suspension = (cp == 0 and op == 0 and hp == 0 and lp == 0 and vol == 0 and amt == 0)

        if is_suspension:
            # 8. 停牌占位记录中存在非零字段
            if chg != 0:
                issue(d, "SUSPENSION_NONZERO_FIELD", f"停牌占位记录中 change_percent={chg} 非零")
        else:
            # 1. 价格 <= 0
            for field, val in [("close_price", cp), ("open_price", op), ("high_price", hp), ("low_price", lp)]:
                if val is not None and val <= 0:
                    issue(d, "PRICE_NON_POSITIVE", f"{field}={val} 不合法（应 > 0）")

            # 2. 价格关系
            if hp is not None and lp is not None and hp < lp:
                issue(d, "PRICE_HIGH_LESS_THAN_LOW", f"high_price={hp} < low_price={lp}")
            if cp is not None and hp is not None and cp > hp:
                issue(d, "PRICE_CLOSE_ABOVE_HIGH", f"close_price={cp} > high_price={hp}")
            if cp is not None and lp is not None and cp < lp:
                issue(d, "PRICE_CLOSE_BELOW_LOW", f"close_price={cp} < low_price={lp}")
            if op is not None and hp is not None and op > hp:
                issue(d, "PRICE_OPEN_ABOVE_HIGH", f"open_price={op} > high_price={hp}")
            if op is not None and lp is not None and op < lp:
                issue(d, "PRICE_OPEN_BELOW_LOW", f"open_price={op} < low_price={lp}")

            # 3. 交易量/金额
            if vol is not None and vol < 0:
                issue(d, "NEGATIVE_VOLUME", f"trading_volume={vol} < 0")
            if amt is not None and amt < 0:
                issue(d, "NEGATIVE_AMOUNT", f"trading_amount={amt} < 0")

            # 4. 涨跌幅异常（超过±21%视为可疑，新股首日除外无法判断）
            if chg is not None and abs(chg) > 21:
                issue(d, "ABNORMAL_CHANGE_PERCENT", f"change_percent={chg}% 超过±21%")

        # 7. 缺失交易日：统计相邻两条记录之间应有的交易日数，若 > 1 则报告缺失
        if prev_date is not None:
            try:
                d0 = date.fromisoformat(prev_date)
                d1 = date.fromisoformat(d)
                gap_days = (d1 - d0).days
                if gap_days > 1:
                    missing_days = [
                        (d0 + timedelta(days=i)).isoformat()
                        for i in range(1, gap_days)
                        if (d0 + timedelta(days=i)).weekday() < 5
                        and is_workday(d0 + timedelta(days=i))
                    ]
                    if missing_days:
                        issue(d, "MISSING_TRADING_DAYS",
                              f"缺失 {len(missing_days)} 个交易日: {', '.join(missing_days)}")
            except (ValueError, Exception) as e:
                log.debug("[%s] 缺失交易日检测异常: %s", stock_code, e)
        prev_date = d

    conn.close()
    return issues


def _kline_to_dao_record(k: dict) -> dict:
    """将 get_stock_day_kline_10jqka 返回的记录转换为 dao 层所需格式"""
    return {
        "date":           k["date"],
        "open_price":     k["open_price"],
        "close_price":    k["close_price"],
        "high_price":     k["high_price"],
        "low_price":      k["low_price"],
        "trading_volume": k["trading_volume"],
        "trading_amount": k.get("trading_amount") or 0.0,
        "amplitude":      0.0,
        "change_percent": 0.0,
        "change_amount":  0.0,
        "change_hand":    k.get("change_hand") or 0.0,
    }


def _save_kline_to_db(stock_code_normalize: str, klines: list[dict]) -> None:
    """将重新拉取的 K 线数据覆盖写入数据库"""
    db_path = get_db_path_for_stock(stock_code_normalize)
    table_name = f"kline_{stock_code_normalize.replace('.', '_')}"
    records = [_kline_to_dao_record(k) for k in klines]
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        cur = conn.cursor()
        create_kline_table(cur, table_name)
        batch_insert_or_update_kline_data(cur, table_name, records)
        conn.commit()
    finally:
        conn.close()


async def _repair_stock(stock_code_normalize: str, db_path: Path, original_issues: list[dict]) -> None:
    """拉取最新数据，重新检测，通过则保存，否则记录日志"""
    log.info("[%s] 发现 %d 条异常，开始重新拉取数据...", stock_code_normalize, len(original_issues))
    try:
        stock_info = get_stock_info_by_code(stock_code_normalize)
    except Exception as e:
        log.error("[%s] 获取 StockInfo 失败: %s", stock_code_normalize, e)
        return

    try:
        klines = await get_stock_day_kline_10jqka(stock_info, limit=800)
    except Exception as e:
        log.error("[%s] 拉取 K 线数据失败: %s", stock_code_normalize, e)
        return

    if not klines:
        log.error("[%s] 拉取到空数据，跳过", stock_code_normalize)
        return

    _save_kline_to_db(stock_code_normalize, klines)

    re_issues = check_db(db_path)
    if not re_issues:
        log.info("[%s] 重新拉取后检测通过，数据已更新 ✓", stock_code_normalize)
    else:
        log.warning(
            "[%s] 重新拉取后仍有 %d 条异常，请人工核查：",
            stock_code_normalize, len(re_issues)
        )
        for iss in re_issues:
            log.warning("  [%s] 日期=%s  %s", iss["type"], iss["date"], iss["detail"])


async def main():
    db_files = sorted(DB_DIR.glob("stock_*.db"))
    if not db_files:
        log.info("未找到任何 stock_*.db 文件，路径: %s", DB_DIR)
        return

    log.info("共发现 %d 个数据库文件，开始检测...", len(db_files))

    total_issues = 0
    stocks_with_issues = 0

    for db_path in db_files:
        issues = check_db(db_path)
        if not issues:
            continue

        stocks_with_issues += 1
        total_issues += len(issues)
        stock_code_normalize = db_path.stem.removeprefix("stock_").replace("_", ".")

        legacy_issues = [i for i in issues if i.get("legacy")]
        active_issues = [i for i in issues if not i.get("legacy")]

        GREEN = "\033[32m"
        RESET = "\033[0m"
        if legacy_issues:
            for iss in legacy_issues:
                print(f"{GREEN}[LEGACY][{iss['type']}] {stock_code_normalize} 日期={iss['date']}  {iss['detail']}{RESET}")

        if active_issues:
            log.info("[%s] 发现 %d 条异常：", db_path.name, len(active_issues))
            for iss in active_issues:
                log.info("  [%s] 日期=%s  %s", iss["type"], iss["date"], iss["detail"])
            await _repair_stock(stock_code_normalize, db_path, active_issues)

    log.info(
        "检测完成：共 %d 只股票，%d 只有异常，共 %d 条异常记录",
        len(db_files), stocks_with_issues, total_issues
    )


if __name__ == "__main__":
    asyncio.run(main())
