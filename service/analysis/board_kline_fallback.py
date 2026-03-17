"""
概念板块K线缺失补全模块

当板块K线增量拉取后，部分板块仍缺少最近一个交易日的数据时，
利用成分股当日K线数据（stock_kline）合成板块当日K线，写入 concept_board_kline。

合成逻辑：
  - change_percent = 成分股当日 change_percent 的等权平均值
  - close_price = 板块前一日收盘价 × (1 + change_percent/100)
  - open/high/low/volume/amount = 成分股对应字段的均值或求和
  - amplitude = high_price/low_price 推算
  - 标记 board_index_code = 'SYNTHETIC' 以便区分

交易日判断：
  - 如果当前时间在收盘前（< 15:00），使用上一个交易日
  - 如果当前时间在收盘后（>= 15:00），使用当天
  - 确保 stock_kline 中有目标日期的数据
"""
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from dao import get_connection

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
        logger.info("[板块K线补全] 当前未收盘(%s)，使用上一个交易日 %s",
                    now.strftime("%H:%M"), d.isoformat())
        return d.isoformat()


def synthesize_missing_board_klines(trade_date: str = None) -> dict:
    """
    检查所有概念板块，对缺少 trade_date 当日K线的板块，
    用成分股K线数据合成并写入。

    Args:
        trade_date: 目标交易日，默认当天

    Returns:
        {"checked": N, "missing": N, "synthesized": N, "skipped": N, "details": [...]}
    """
    if not trade_date:
        trade_date = _get_effective_trade_date()
    else:
        trade_date = _get_effective_trade_date(trade_date)

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 1. 获取所有板块
        cur.execute("SELECT board_code, board_name FROM stock_concept_board ORDER BY board_code")
        all_boards = cur.fetchall()
        total_boards = len(all_boards)

        # 2. 找出已有当日K线的板块
        cur.execute(
            "SELECT DISTINCT board_code FROM concept_board_kline WHERE `date` = %s",
            (trade_date,)
        )
        boards_with_today = {r["board_code"] for r in cur.fetchall()}

        # 3. 筛选缺失当日K线的板块
        missing_boards = [
            b for b in all_boards if b["board_code"] not in boards_with_today
        ]

        if not missing_boards:
            logger.info("[板块K线补全] %s 所有%d个板块均已有当日K线，无需补全",
                        trade_date, total_boards)
            return {"checked": total_boards, "missing": 0, "synthesized": 0,
                    "skipped": 0, "details": []}

        logger.info("[板块K线补全] %s 共%d个板块，%d个缺少当日K线，开始合成",
                    trade_date, total_boards, len(missing_boards))

        # 4. 验证 stock_kline 中当日数据是否充足
        cur.execute(
            "SELECT COUNT(DISTINCT stock_code) FROM stock_kline WHERE `date` = %s",
            (trade_date,)
        )
        stock_count_today = cur.fetchone()["COUNT(DISTINCT stock_code)"]
        if stock_count_today < 100:
            logger.warning("[板块K线补全] stock_kline 中 %s 仅有 %d 只股票数据，"
                           "可能个股K线尚未拉取完成，跳过合成",
                           trade_date, stock_count_today)
            return {"checked": total_boards, "missing": len(missing_boards),
                    "synthesized": 0, "skipped": len(missing_boards),
                    "details": [{"reason": f"stock_kline当日数据不足({stock_count_today}只)"}]}

        synthesized = 0
        skipped = 0
        details = []

        for board in missing_boards:
            board_code = board["board_code"]
            board_name = board["board_name"]
            result = _synthesize_single_board(cur, board_code, board_name, trade_date)
            if result:
                synthesized += 1
                details.append(result)
            else:
                skipped += 1

        conn.commit()

        logger.info("[板块K线补全] %s 完成: 缺失%d 合成%d 跳过%d",
                    trade_date, len(missing_boards), synthesized, skipped)
        return {
            "checked": total_boards,
            "missing": len(missing_boards),
            "synthesized": synthesized,
            "skipped": skipped,
            "details": details,
        }

    except Exception as e:
        logger.error("[板块K线补全] 异常: %s", e, exc_info=True)
        conn.rollback()
        return {"checked": 0, "missing": 0, "synthesized": 0, "skipped": 0,
                "error": str(e), "details": []}
    finally:
        cur.close()
        conn.close()


def _normalize_stock_code(code: str) -> str:
    """将6位纯数字股票代码转换为带市场后缀的格式"""
    if "." in code:
        return code
    if code.startswith("6"):
        return f"{code}.SH"
    elif code.startswith("0") or code.startswith("3"):
        return f"{code}.SZ"
    elif code.startswith("4") or code.startswith("8") or code.startswith("9"):
        return f"{code}.BJ"
    return f"{code}.SZ"


def _synthesize_single_board(cur, board_code: str, board_name: str,
                             trade_date: str) -> dict | None:
    """
    为单个板块合成当日K线。

    逻辑：
    1. 查询板块成分股列表
    2. 查询成分股当日K线数据
    3. 要求至少 50% 成分股有当日数据
    4. 等权平均计算板块涨跌幅
    5. 结合前一日收盘价推算当日收盘价
    6. 写入 concept_board_kline

    Returns:
        成功返回 {"board_code", "board_name", "change_percent", "member_count", "used_count"}
        失败返回 None
    """
    # 1. 成分股列表
    cur.execute(
        "SELECT stock_code FROM stock_concept_board_stock WHERE board_code = %s",
        (board_code,)
    )
    members = cur.fetchall()
    if not members:
        return None

    member_codes_raw = [m["stock_code"] for m in members]
    member_codes_norm = [_normalize_stock_code(c) for c in member_codes_raw]
    # 过滤北交所个股
    member_codes_norm = [c for c in member_codes_norm if not c.endswith('.BJ')]
    total_members = len(member_codes_norm)

    # 2. 查询成分股当日K线（分批避免 IN 过长）
    stock_klines = []
    batch_size = 100
    for i in range(0, len(member_codes_norm), batch_size):
        batch = member_codes_norm[i:i + batch_size]
        placeholders = ",".join(["%s"] * len(batch))
        cur.execute(
            f"SELECT stock_code, open_price, close_price, high_price, low_price, "
            f"trading_volume, trading_amount, change_percent "
            f"FROM stock_kline "
            f"WHERE stock_code IN ({placeholders}) AND `date` = %s",
            batch + [trade_date],
        )
        stock_klines.extend(cur.fetchall())

    # 过滤掉停牌股（涨跌幅为0且成交量为0的视为停牌）
    valid_klines = [
        k for k in stock_klines
        if k["change_percent"] is not None
        and not (k["change_percent"] == 0 and (k["trading_volume"] or 0) == 0)
    ]

    if not valid_klines:
        logger.debug("[板块K线补全] %s %s 无有效成分股K线数据", board_code, board_name)
        return None

    # 3. 覆盖率检查：至少 50% 成分股有数据
    coverage = len(valid_klines) / total_members
    if coverage < 0.5:
        logger.debug("[板块K线补全] %s %s 成分股覆盖率%.1f%%(%d/%d)不足50%%，跳过",
                     board_code, board_name, coverage * 100,
                     len(valid_klines), total_members)
        return None

    # 4. 等权平均计算板块涨跌幅
    #    同花顺板块指数的具体加权方式不公开，等权平均在实测中偏差最小（平均<0.5%）
    n = len(valid_klines)
    avg_chg_pct = sum(k["change_percent"] for k in valid_klines) / n

    # open/close/high/low 用均值（仅用于参考，核心是 change_percent）
    avg_open = sum((k["open_price"] or 0) for k in valid_klines) / n
    avg_close = sum((k["close_price"] or 0) for k in valid_klines) / n
    avg_high = sum((k["high_price"] or 0) for k in valid_klines) / n
    avg_low = sum((k["low_price"] or 0) for k in valid_klines) / n

    # volume/amount 用求和
    total_volume = sum((k["trading_volume"] or 0) for k in valid_klines)
    total_amount = sum((k["trading_amount"] or 0) for k in valid_klines)

    # 5. 结合前一日收盘价推算当日板块价格
    cur.execute(
        "SELECT close_price FROM concept_board_kline "
        "WHERE board_code = %s AND `date` < %s "
        "ORDER BY `date` DESC LIMIT 1",
        (board_code, trade_date),
    )
    prev_row = cur.fetchone()
    if prev_row and prev_row["close_price"]:
        prev_close = prev_row["close_price"]
        synth_close = round(prev_close * (1 + avg_chg_pct / 100), 4)
        synth_change_amount = round(synth_close - prev_close, 4)
        # open/high/low 用成分股均值的涨跌幅比例推算
        avg_open_chg = (avg_open / avg_close - 1) * 100 if avg_close else 0
        avg_high_chg = (avg_high / avg_close - 1) * 100 if avg_close else avg_chg_pct
        avg_low_chg = (avg_low / avg_close - 1) * 100 if avg_close else avg_chg_pct
        synth_open = round(prev_close * (1 + (avg_chg_pct + avg_open_chg) / 100), 4)
        synth_high = round(prev_close * (1 + (avg_chg_pct + avg_high_chg) / 100), 4)
        synth_low = round(prev_close * (1 + (avg_chg_pct + avg_low_chg) / 100), 4)
    else:
        # 无前一日数据，使用成分股均值作为绝对价格（不太精确但可用）
        synth_close = avg_close
        synth_open = avg_open
        synth_high = avg_high
        synth_low = avg_low
        synth_change_amount = 0

    # 振幅
    synth_amplitude = round(
        (synth_high - synth_low) / synth_low * 100, 4
    ) if synth_low and synth_low > 0 else 0

    # 6. 写入 concept_board_kline（标记为合成数据）
    cur.execute(
        """
        INSERT INTO concept_board_kline
            (board_code, board_index_code, `date`, open_price, close_price,
             high_price, low_price, trading_volume, trading_amount,
             change_percent, change_amount, amplitude, change_hand)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open_price=VALUES(open_price),
            close_price=VALUES(close_price),
            high_price=VALUES(high_price),
            low_price=VALUES(low_price),
            trading_volume=VALUES(trading_volume),
            trading_amount=VALUES(trading_amount),
            change_percent=VALUES(change_percent),
            change_amount=VALUES(change_amount),
            amplitude=VALUES(amplitude),
            change_hand=VALUES(change_hand)
        """,
        (board_code, "SYNTHETIC", trade_date,
         round(synth_open, 4), round(synth_close, 4),
         round(synth_high, 4), round(synth_low, 4),
         round(total_volume, 2), round(total_amount, 2),
         round(avg_chg_pct, 4), round(synth_change_amount, 4),
         synth_amplitude, None),
    )

    logger.info("[板块K线补全] %s %s 合成成功: 涨跌幅=%.2f%% 使用%d/%d只成分股",
                board_code, board_name, avg_chg_pct, n, total_members)

    return {
        "board_code": board_code,
        "board_name": board_name,
        "change_percent": round(avg_chg_pct, 4),
        "member_count": total_members,
        "used_count": n,
        "coverage": round(coverage * 100, 1),
    }
