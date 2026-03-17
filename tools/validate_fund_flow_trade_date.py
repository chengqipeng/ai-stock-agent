"""
验证资金流向补全的交易日判断逻辑。

测试场景：
  1. 收盘后（>= 15:00）传入今天 → 应使用今天
  2. 收盘前（< 15:00）传入今天 → 应回退到上一个交易日
  3. 传入历史日期 → 直接使用该日期
  4. 不传日期 → 自动判断
  5. 周末/节假日 → 回退到最近交易日
  6. 端到端：模拟盘中调用，验证补全使用正确的交易日

Usage:
    .venv/bin/python -m tools.validate_fund_flow_trade_date
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, date, timedelta, time as dtime
from zoneinfo import ZoneInfo
from unittest.mock import patch

_CST = ZoneInfo("Asia/Shanghai")


def test_trade_date_logic():
    """测试 _get_effective_trade_date 在各种时间场景下的行为"""
    from service.analysis.fund_flow_fallback import _get_effective_trade_date
    from service.auto_job.kline_data_scheduler import is_a_share_trading_day

    now_real = datetime.now(_CST)
    today = now_real.date()
    today_str = today.isoformat()

    print(f"当前时间: {now_real.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"今天是否交易日: {is_a_share_trading_day(today)}")
    print()

    # 找到最近的交易日和上一个交易日
    d = today
    while not is_a_share_trading_day(d):
        d -= timedelta(days=1)
    latest_trade_day = d

    d = latest_trade_day - timedelta(days=1)
    while not is_a_share_trading_day(d):
        d -= timedelta(days=1)
    prev_trade_day = d

    print(f"最近交易日: {latest_trade_day}")
    print(f"上一个交易日: {prev_trade_day}")
    print()

    errors = []

    # ── 场景1: 收盘后传入今天 ──
    print("=" * 80)
    print("场景1: 收盘后(16:30)传入今天日期")
    mock_time = datetime.combine(latest_trade_day, dtime(16, 30), tzinfo=_CST)
    with patch('service.analysis.fund_flow_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(latest_trade_day.isoformat())
    expected = latest_trade_day.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {latest_trade_day.isoformat()} → 结果: {result} (期望: {expected})")
    if result != expected:
        errors.append(f"场景1: 期望{expected} 实际{result}")

    # ── 场景2: 收盘前传入今天 ──
    print("\n场景2: 收盘前(10:30)传入今天日期")
    mock_time = datetime.combine(latest_trade_day, dtime(10, 30), tzinfo=_CST)
    with patch('service.analysis.fund_flow_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(latest_trade_day.isoformat())
    expected = prev_trade_day.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {latest_trade_day.isoformat()} → 结果: {result} (期望: {expected})")
    if result != expected:
        errors.append(f"场景2: 期望{expected} 实际{result}")

    # ── 场景3: 传入历史日期 ──
    print("\n场景3: 传入历史日期")
    hist_date = prev_trade_day.isoformat()
    result = _get_effective_trade_date(hist_date)
    expected = hist_date
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {hist_date} → 结果: {result} (期望: {expected})")
    if result != expected:
        errors.append(f"场景3: 期望{expected} 实际{result}")

    # ── 场景4: 不传日期，自动判断 ──
    print("\n场景4: 不传日期，自动判断（当前真实时间）")
    result = _get_effective_trade_date(None)
    if now_real.time() >= dtime(15, 0) and is_a_share_trading_day(today):
        expected = today_str
    else:
        expected = prev_trade_day.isoformat() if is_a_share_trading_day(today) else latest_trade_day.isoformat()
        # 如果今天不是交易日，回退逻辑不同
        if not is_a_share_trading_day(today):
            d = today - timedelta(days=1)
            while not is_a_share_trading_day(d):
                d -= timedelta(days=1)
            expected = d.isoformat()
    status = "✅" if result == expected else "⚠️"
    print(f"  {status} 输入: None → 结果: {result} (期望: {expected})")
    if result != expected:
        # 不算硬错误，因为真实时间可能在边界
        print(f"  ⚠️ 可能是时间边界问题")

    # ── 场景5: 模拟周六 ──
    print("\n场景5: 模拟周六下午")
    # 找到下一个周六
    d = today
    while d.weekday() != 5:
        d += timedelta(days=1)
    saturday = d
    mock_time = datetime.combine(saturday, dtime(16, 0), tzinfo=_CST)
    with patch('service.analysis.fund_flow_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(saturday.isoformat())
    # 周六不是交易日，应回退到周五（或更早的交易日）
    d = saturday - timedelta(days=1)
    while not is_a_share_trading_day(d):
        d -= timedelta(days=1)
    expected = d.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {saturday.isoformat()}(周六) → 结果: {result} (期望: {expected})")
    if result != expected:
        errors.append(f"场景5: 期望{expected} 实际{result}")

    # ── 场景6: 恰好15:00 ──
    print("\n场景6: 恰好15:00（收盘时刻）")
    mock_time = datetime.combine(latest_trade_day, dtime(15, 0), tzinfo=_CST)
    with patch('service.analysis.fund_flow_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(latest_trade_day.isoformat())
    expected = latest_trade_day.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {latest_trade_day.isoformat()} @15:00 → 结果: {result} (期望: {expected})")
    if result != expected:
        errors.append(f"场景6: 期望{expected} 实际{result}")

    # ── 场景7: 14:59（收盘前1分钟） ──
    print("\n场景7: 14:59（收盘前1分钟）")
    mock_time = datetime.combine(latest_trade_day, dtime(14, 59), tzinfo=_CST)
    with patch('service.analysis.fund_flow_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(latest_trade_day.isoformat())
    expected = prev_trade_day.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {latest_trade_day.isoformat()} @14:59 → 结果: {result} (期望: {expected})")
    if result != expected:
        errors.append(f"场景7: 期望{expected} 实际{result}")

    # 汇总
    print("\n" + "=" * 80)
    if errors:
        print(f"❌ {len(errors)} 个场景失败:")
        for e in errors:
            print(f"   - {e}")
    else:
        print("✅ 所有交易日判断场景通过")

    return len(errors) == 0


def test_e2e_with_real_data():
    """端到端测试：验证补全函数使用正确的交易日"""
    from dao import get_connection
    from service.analysis.fund_flow_fallback import fill_missing_fund_flow_from_kline

    print("\n\n" + "=" * 80)
    print("端到端测试: 验证补全函数使用正确的交易日")
    print("=" * 80)

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        # 找到有K线数据的最新交易日
        cur.execute("SELECT MAX(`date`) as d FROM stock_kline")
        kline_max = str(cur.fetchone()["d"])

        cur.execute(
            "SELECT COUNT(DISTINCT stock_code) as cnt FROM stock_kline WHERE `date` = %s",
            (kline_max,)
        )
        kline_cnt = cur.fetchone()["cnt"]
        print(f"stock_kline 最新日期: {kline_max} ({kline_cnt}只股票)")

        # 选3只有资金流向的股票
        cur.execute(
            "SELECT stock_code FROM stock_fund_flow "
            "WHERE `date` = %s AND net_flow IS NOT NULL "
            "ORDER BY RAND() LIMIT 3",
            (kline_max,)
        )
        test_codes = [r["stock_code"] for r in cur.fetchall()]
        if not test_codes:
            print("⚠️ 该日期无资金流向数据，跳过端到端测试")
            return True

        print(f"测试股票: {test_codes}")

        # 保存原始数据
        originals = {}
        for code in test_codes:
            cur.execute(
                "SELECT * FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                (code, kline_max)
            )
            originals[code] = cur.fetchone()

        # 删除当日数据
        for code in test_codes:
            cur.execute(
                "DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                (code, kline_max)
            )
        conn.commit()

        # 直接传入该日期调用补全
        result = fill_missing_fund_flow_from_kline(test_codes, kline_max)
        actual_date = result.get("trade_date", "?")
        print(f"\n传入日期: {kline_max}")
        print(f"实际使用: {actual_date}")
        print(f"补全结果: filled={result['filled']} missing={result['missing']}")

        # 验证补全的数据
        ok = True
        for code in test_codes:
            cur.execute(
                "SELECT close_price, change_pct FROM stock_fund_flow "
                "WHERE stock_code = %s AND `date` = %s",
                (code, actual_date)
            )
            row = cur.fetchone()
            orig = originals.get(code)
            if row and orig:
                cp_match = row["close_price"] == orig["close_price"]
                chg_match = row["change_pct"] == orig["change_pct"]
                status = "✅" if cp_match and chg_match else "❌"
                print(f"  {status} {code}: close={row['close_price']}(原{orig['close_price']}) "
                      f"chg={row['change_pct']}(原{orig['change_pct']})")
                if not (cp_match and chg_match):
                    ok = False
            elif not row:
                print(f"  ❌ {code}: 补全后无数据")
                ok = False

        # 恢复原始数据
        for code, orig in originals.items():
            cur.execute(
                "DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                (code, kline_max)
            )
            if orig:
                cur.execute(
                    "INSERT INTO stock_fund_flow "
                    "(stock_code, `date`, close_price, change_pct, net_flow, main_net_5day, "
                    "big_net, big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (code, kline_max, orig["close_price"], orig["change_pct"],
                     orig["net_flow"], orig["main_net_5day"],
                     orig["big_net"], orig["big_net_pct"],
                     orig["mid_net"], orig["mid_net_pct"],
                     orig["small_net"], orig["small_net_pct"]),
                )
        conn.commit()
        print("✅ 原始数据已恢复")
        return ok

    except Exception as e:
        print(f"❌ 端到端测试异常: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    ok1 = test_trade_date_logic()
    ok2 = test_e2e_with_real_data()

    print("\n" + "=" * 80)
    if ok1 and ok2:
        print("✅ 全部验证通过")
    else:
        print("❌ 存在失败项")
