"""
资金流向缺失补全模块

当资金流向调度拉取失败后，部分股票仍缺少最近一个交易日的数据时，
利用个股当日K线数据（stock_kline）合成部分资金流向记录。

合成逻辑：
  - close_price, change_pct 从 stock_kline 获取
  - net_flow, big_net, big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct 设为 NULL
    （这些资金流特有字段无法从K线推导）
  - main_net_5day: 尝试从前4天的 big_net 累加计算（如果前4天数据存在）

交易日判断：
  - 如果当前时间在收盘前（< 15:00），使用上一个交易日
  - 如果当前时间在收盘后（>= 15:00），使用当天
  - 确保 stock_kline 中有目标日期的数据
"""
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from dao import get_connection
from dao.stock_fund_flow_dao import TABLE_NAME

logger = logging.getLogger(__name__)
_CST = ZoneInfo("Asia/Shanghai")


def _get_effective_trade_date(trade_date: str = None) -> str:
    """
    获取有效的补全目标交易日。

    逻辑：
      - 如果指定了 trade_date，检查当前是否已收盘：
        · 如果 trade_date 是今天且未收盘（< 15:00），回退到上一个交易日
        · 如果 trade_date 是今天且已收盘（>= 15:00），使用今天
        · 如果 trade_date 不是今天（历史日期），直接使用
      - 如果未指定 trade_date，自动判断：
        · 已收盘用今天，未收盘用上一个交易日

    Returns:
        有效的交易日字符串 (YYYY-MM-DD)
    """
    from service.auto_job.kline_data_scheduler import is_a_share_trading_day

    now = datetime.now(_CST)
    today_str = now.date().isoformat()

    if trade_date and trade_date != today_str:
        # 指定了历史日期，直接使用
        return trade_date

    # trade_date 是今天或未指定，需要判断是否收盘
    if now.time() >= dtime(15, 0):
        # 已收盘，使用今天（如果是交易日）
        if is_a_share_trading_day(now.date()):
            return today_str
        # 今天不是交易日，回退
        d = now.date() - timedelta(days=1)
        while not is_a_share_trading_day(d):
            d -= timedelta(days=1)
        return d.isoformat()
    else:
        # 未收盘，使用上一个交易日
        d = now.date() - timedelta(days=1)
        while not is_a_share_trading_day(d):
            d -= timedelta(days=1)
        logger.info("[资金流补全] 当前未收盘(%s)，使用上一个交易日 %s",
                    now.strftime("%H:%M"), d.isoformat())
        return d.isoformat()


def fill_missing_fund_flow_from_kline(stock_codes: list[str], trade_date: str = None) -> dict:
    """
    对缺少 trade_date 当日资金流向的股票，用 stock_kline 数据补全。

    Args:
        stock_codes: 需要检查的股票代码列表
        trade_date: 目标交易日 (YYYY-MM-DD)，为空则自动判断

    Returns:
        {"checked": N, "missing": N, "filled": N, "skipped": N, "trade_date": str, "details": [...]}
    """
    if not stock_codes:
        return {"checked": 0, "missing": 0, "filled": 0, "skipped": 0, "details": []}

    # 确定有效的补全目标交易日
    trade_date = _get_effective_trade_date(trade_date)

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        total = len(stock_codes)

        # 1. 找出已有当日资金流向的股票
        batch_size = 200
        codes_with_today = set()
        for i in range(0, total, batch_size):
            batch = stock_codes[i:i + batch_size]
            ph = ",".join(["%s"] * len(batch))
            cur.execute(
                f"SELECT DISTINCT stock_code FROM {TABLE_NAME} "
                f"WHERE stock_code IN ({ph}) AND `date` = %s",
                batch + [trade_date],
            )
            codes_with_today.update(r["stock_code"] for r in cur.fetchall())

        missing_codes = [c for c in stock_codes if c not in codes_with_today]
        if not missing_codes:
            logger.info("[资金流补全] %s 所有%d只股票均已有当日数据", trade_date, total)
            return {"checked": total, "missing": 0, "filled": 0, "skipped": 0,
                    "trade_date": trade_date, "details": []}

        logger.info("[资金流补全] %s 共%d只股票，%d只缺少当日数据，开始补全",
                    trade_date, total, len(missing_codes))

        # 2. 从 stock_kline 获取缺失股票的当日K线
        kline_map = {}
        for i in range(0, len(missing_codes), batch_size):
            batch = missing_codes[i:i + batch_size]
            ph = ",".join(["%s"] * len(batch))
            cur.execute(
                f"SELECT stock_code, close_price, change_percent "
                f"FROM stock_kline "
                f"WHERE stock_code IN ({ph}) AND `date` = %s",
                batch + [trade_date],
            )
            for r in cur.fetchall():
                kline_map[r["stock_code"]] = r

        # 3. 获取前4天的 big_net 用于计算 main_net_5day
        from collections import defaultdict
        prev_big_net_map = defaultdict(list)
        for i in range(0, len(missing_codes), batch_size):
            batch = missing_codes[i:i + batch_size]
            ph = ",".join(["%s"] * len(batch))
            cur.execute(
                f"SELECT stock_code, `date`, big_net "
                f"FROM {TABLE_NAME} "
                f"WHERE stock_code IN ({ph}) AND `date` < %s "
                f"ORDER BY stock_code, `date` DESC",
                batch + [trade_date],
            )
            for r in cur.fetchall():
                if len(prev_big_net_map[r["stock_code"]]) < 4:
                    prev_big_net_map[r["stock_code"]].append(r["big_net"])

        # 4. 构建并插入补全记录
        filled = 0
        skipped = 0
        details = []
        insert_rows = []

        for code in missing_codes:
            kline = kline_map.get(code)
            if not kline or kline["close_price"] is None:
                skipped += 1
                continue

            close_price = kline["close_price"]
            change_pct = kline["change_percent"]

            # 计算 main_net_5day: 前4天 big_net 之和（当天 big_net 为 NULL，无法参与）
            prev_bigs = prev_big_net_map.get(code, [])
            valid_bigs = [b for b in prev_bigs if b is not None]
            main_net_5day = round(sum(valid_bigs), 2) if len(valid_bigs) == 4 else None

            insert_rows.append((
                code, trade_date, close_price, change_pct,
                None,  # net_flow
                main_net_5day,
                None, None,  # big_net, big_net_pct
                None, None,  # mid_net, mid_net_pct
                None, None,  # small_net, small_net_pct
            ))
            filled += 1

        if insert_rows:
            sql = f"""
                INSERT INTO {TABLE_NAME}
                    (stock_code, `date`, close_price, change_pct,
                     net_flow, main_net_5day,
                     big_net, big_net_pct, mid_net, mid_net_pct,
                     small_net, small_net_pct)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    close_price=COALESCE(VALUES(close_price), close_price),
                    change_pct=COALESCE(VALUES(change_pct), change_pct),
                    main_net_5day=COALESCE(VALUES(main_net_5day), main_net_5day)
            """
            cur.executemany(sql, insert_rows)
            conn.commit()

        logger.info("[资金流补全] %s 完成: 缺失%d 补全%d 跳过%d(无K线)",
                    trade_date, len(missing_codes), filled, skipped)

        return {
            "checked": total,
            "missing": len(missing_codes),
            "filled": filled,
            "skipped": skipped,
            "trade_date": trade_date,
            "details": details,
        }

    except Exception as e:
        logger.error("[资金流补全] 异常: %s", e, exc_info=True)
        conn.rollback()
        return {"checked": len(stock_codes), "missing": 0, "filled": 0,
                "skipped": 0, "trade_date": trade_date, "error": str(e), "details": []}
    finally:
        cur.close()
        conn.close()
