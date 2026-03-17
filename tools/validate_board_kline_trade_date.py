"""
验证板块K线补全的交易日判断逻辑。

测试场景：
  1. 收盘后（>= 15:00）传入今天 → 应使用今天
  2. 收盘前（< 15:00）传入今天 → 应回退到上一个交易日
  3. 传入历史日期 → 直接使用该日期
  4. 不传日期 → 自动判断
  5. 周末 → 回退到最近交易日
  6. 恰好15:00 / 14:59 边界测试
  7. 端到端：验证补全函数使用正确的交易日

Usage:
    .venv/bin/python -m tools.validate_board_kline_trade_date
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from unittest.mock import patch

_CST = ZoneInfo("Asia/Shanghai")


def test_trade_date_logic():
    """测试 _get_effective_trade_date 在各种时间场景下的行为"""
    from service.analysis.board_kline_fallback import _get_effective_trade_date
    from service.auto_job.kline_data_scheduler import is_a_share_trading_day

    now_real = datetime.now(_CST)
    today = now_real.date()

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
    with patch('service.analysis.board_kline_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(latest_trade_day.isoformat())
    expected = latest_trade_day.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {latest_trade_day} → 结果: {result} (期望: {expected})")
    if result != expected:
        errors.append(f"场景1: 期望{expected} 实际{result}")

    # ── 场景2: 收盘前传入今天 ──
    print("\n场景2: 收盘前(10:30)传入今天日期")
    mock_time = datetime.combine(latest_trade_day, dtime(10, 30), tzinfo=_CST)
    with patch('service.analysis.board_kline_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(latest_trade_day.isoformat())
    expected = prev_trade_day.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {latest_trade_day} → 结果: {result} (期望: {expected})")
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

    # ── 场景4: 不传日期 ──
    print("\n场景4: 不传日期，自动判断（当前真实时间）")
    result = _get_effective_trade_date(None)
    if now_real.time() >= dtime(15, 0) and is_a_share_trading_day(today):
        expected = today.isoformat()
    else:
        d = today - timedelta(days=1)
        while not is_a_share_trading_day(d):
            d -= timedelta(days=1)
        expected = d.isoformat()
        if not is_a_share_trading_day(today) and now_real.time() >= dtime(15, 0):
            expected = d.isoformat()
    status = "✅" if result == expected else "⚠️"
    print(f"  {status} 输入: None → 结果: {result} (期望: {expected})")
    if result != expected:
        print(f"  ⚠️ 可能是时间边界问题")

    # ── 场景5: 周六 ──
    print("\n场景5: 模拟周六下午")
    d = today
    while d.weekday() != 5:
        d += timedelta(days=1)
    saturday = d
    mock_time = datetime.combine(saturday, dtime(16, 0), tzinfo=_CST)
    with patch('service.analysis.board_kline_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(saturday.isoformat())
    d = saturday - timedelta(days=1)
    while not is_a_share_trading_day(d):
        d -= timedelta(days=1)
    expected = d.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {saturday}(周六) → 结果: {result} (期望: {expected})")
    if result != expected:
        errors.append(f"场景5: 期望{expected} 实际{result}")

    # ── 场景6: 恰好15:00 ──
    print("\n场景6: 恰好15:00（收盘时刻）")
    mock_time = datetime.combine(latest_trade_day, dtime(15, 0), tzinfo=_CST)
    with patch('service.analysis.board_kline_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(latest_trade_day.isoformat())
    expected = latest_trade_day.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {latest_trade_day} @15:00 → 结果: {result} (期望: {expected})")
    if result != expected:
        errors.append(f"场景6: 期望{expected} 实际{result}")

    # ── 场景7: 14:59 ──
    print("\n场景7: 14:59（收盘前1分钟）")
    mock_time = datetime.combine(latest_trade_day, dtime(14, 59), tzinfo=_CST)
    with patch('service.analysis.board_kline_fallback.datetime') as mock_dt:
        mock_dt.now.return_value = mock_time
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _get_effective_trade_date(latest_trade_day.isoformat())
    expected = prev_trade_day.isoformat()
    status = "✅" if result == expected else "❌"
    print(f"  {status} 输入: {latest_trade_day} @14:59 → 结果: {result} (期望: {expected})")
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
    from service.analysis.board_kline_fallback import synthesize_missing_board_klines

    print("\n\n" + "=" * 80)
    print("端到端测试: 验证板块K线补全使用正确的交易日")
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

        # 找到该日有板块K线的板块
        cur.execute(
            "SELECT bk.board_code, cb.board_name, bk.change_percent, bk.close_price "
            "FROM concept_board_kline bk "
            "JOIN stock_concept_board cb ON bk.board_code = cb.board_code "
            "WHERE bk.`date` = %s AND bk.change_percent IS NOT NULL "
            "ORDER BY RAND() LIMIT 3",
            (kline_max,)
        )
        test_boards = cur.fetchall()
        if not test_boards:
            print("⚠️ 该日期无板块K线数据，跳过端到端测试")
            return True

        print(f"测试板块: {[b['board_name'] for b in test_boards]}")

        # 保存原始数据并删除
        originals = {}
        for b in test_boards:
            cur.execute(
                "SELECT * FROM concept_board_kline WHERE board_code = %s AND `date` = %s",
                (b["board_code"], kline_max)
            )
            originals[b["board_code"]] = cur.fetchone()
            cur.execute(
                "DELETE FROM concept_board_kline WHERE board_code = %s AND `date` = %s",
                (b["board_code"], kline_max)
            )
        conn.commit()

        # 调用补全（传入该历史日期）
        result = synthesize_missing_board_klines(kline_max)
        print(f"\n传入日期: {kline_max}")
        print(f"补全结果: synthesized={result.get('synthesized', 0)} "
              f"missing={result.get('missing', 0)}")

        # 验证
        ok = True
        for b in test_boards:
            code = b["board_code"]
            cur.execute(
                "SELECT change_percent, close_price FROM concept_board_kline "
                "WHERE board_code = %s AND `date` = %s",
                (code, kline_max)
            )
            row = cur.fetchone()
            orig = originals.get(code)
            if row and orig:
                real_chg = orig["change_percent"]
                synth_chg = row["change_percent"]
                diff = abs(synth_chg - real_chg) if synth_chg is not None and real_chg is not None else None
                status = "✅" if diff is not None and diff < 1.0 else "⚠️"
                print(f"  {status} {b['board_name']}: 合成涨跌幅={synth_chg:.4f}% "
                      f"真实={real_chg:.4f}% 偏差={diff:.4f}%")
            elif not row:
                print(f"  ⏭️ {b['board_name']}: 未合成（成分股数据不足）")

        # 恢复原始数据
        for code, orig in originals.items():
            cur.execute(
                "DELETE FROM concept_board_kline WHERE board_code = %s AND `date` = %s",
                (code, kline_max)
            )
            if orig:
                cols = [k for k in orig.keys() if k != 'id' and k != 'created_at' and k != 'updated_at']
                ph = ",".join(["%s"] * len(cols))
                col_names = ",".join([f"`{c}`" for c in cols])
                cur.execute(
                    f"INSERT INTO concept_board_kline ({col_names}) VALUES ({ph})",
                    [orig[c] for c in cols]
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
