"""
验证资金流向调度的跳过逻辑（含批量方法 + 仅缺最新日K线补全）。

测试场景：
  1. 数据完整的股票 → 应跳过（单只 + 批量）
  2. 删除某天数据后 → 应不跳过（检测到缺失）
  3. 恢复后再次检查 → 应跳过
  4. 验证 target_date 的影响：盘中用昨天，收盘后用今天
  5. 批量方法三分类：完整 / 仅缺最新日 / 需拉取
  6. 删除最新日数据 → 应归入 only_latest_missing
  7. 删除非最新日数据 → 应归入需拉取（不在 complete 也不在 only_latest）
  8. 批量方法性能

Usage:
    .venv/bin/python -m tools.validate_fund_flow_skip_logic
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from dao import get_connection
from service.auto_job.fund_flow_scheduler import _batch_check_completeness

_CST = ZoneInfo("Asia/Shanghai")


def check_skip_logic(code: str, target_date: str) -> tuple[bool, int, int, set]:
    """
    单只股票检查逻辑。
    返回 (should_skip, kline_count, missing_count, missing_dates)
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT `date` FROM stock_kline "
            "WHERE stock_code = %s AND `date` <= %s "
            "ORDER BY `date` DESC LIMIT 120",
            (code, target_date),
        )
        kline_dates = {str(r[0]) for r in cur.fetchall()}
        if not kline_dates:
            return False, 0, 0, set()

        cur.execute(
            "SELECT `date` FROM stock_fund_flow "
            "WHERE stock_code = %s AND `date` <= %s "
            "ORDER BY `date` DESC LIMIT 120",
            (code, target_date),
        )
        ff_dates = {str(r[0]) for r in cur.fetchall()}
        missing = kline_dates - ff_dates
        return len(missing) == 0, len(kline_dates), len(missing), missing
    finally:
        cur.close()
        conn.close()


def _restore_row(cur, conn, code, date_str, original):
    """恢复删除的资金流向记录"""
    if not original:
        return
    cur.execute(
        "INSERT INTO stock_fund_flow "
        "(stock_code, `date`, close_price, change_pct, net_flow, main_net_5day, "
        "big_net, big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (code, date_str, original["close_price"], original["change_pct"],
         original["net_flow"], original["main_net_5day"],
         original["big_net"], original["big_net_pct"],
         original["mid_net"], original["mid_net_pct"],
         original["small_net"], original["small_net_pct"]),
    )
    conn.commit()


def validate():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    try:
        from service.auto_job.kline_data_scheduler import is_a_share_trading_day

        # 找到有K线数据的最新交易日
        cur.execute("SELECT MAX(`date`) as d FROM stock_kline")
        kline_max = str(cur.fetchone()["d"])
        print(f"stock_kline 最新日期: {kline_max}")

        # 选5只有完整资金流向数据的股票
        cur.execute(
            "SELECT ff.stock_code, COUNT(*) as cnt "
            "FROM stock_fund_flow ff "
            "INNER JOIN stock_kline sk ON ff.stock_code = sk.stock_code AND sk.`date` = %s "
            "GROUP BY ff.stock_code "
            "HAVING cnt >= 100 "
            "ORDER BY RAND() LIMIT 5",
            (kline_max,)
        )
        test_stocks = [r["stock_code"] for r in cur.fetchall()]
        if not test_stocks:
            print("❌ 找不到有足够数据的测试股票")
            return
        print(f"测试股票: {test_stocks}")

        errors = []

        # ── 场景1: 数据完整 → 应跳过 ──
        print("\n" + "=" * 80)
        print("场景1: 数据完整的股票应被跳过")
        for code in test_stocks:
            skip, kline_cnt, missing_cnt, _ = check_skip_logic(code, kline_max)
            status = "✅" if skip else "❌"
            print(f"  {status} {code}: skip={skip} K线天数={kline_cnt} 缺失={missing_cnt}")

        # ── 场景2: 删除非最新日数据 → 应不跳过 ──
        print("\n" + "=" * 80)
        print("场景2: 删除非最新日数据后应检测到缺失")
        test_code = test_stocks[0]
        cur.execute(
            "SELECT `date` FROM stock_fund_flow "
            "WHERE stock_code = %s AND `date` <= %s "
            "ORDER BY `date` DESC LIMIT 10",
            (test_code, kline_max)
        )
        ff_rows = cur.fetchall()
        if len(ff_rows) < 5:
            print(f"  ⚠️ {test_code} 数据不足，跳过场景2")
        else:
            delete_date = str(ff_rows[4]["date"])
            print(f"  删除 {test_code} 在 {delete_date} 的数据")
            cur.execute("SELECT * FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                        (test_code, delete_date))
            original = cur.fetchone()
            cur.execute("DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                        (test_code, delete_date))
            conn.commit()

            skip, _, missing_cnt, _ = check_skip_logic(test_code, kline_max)
            status = "✅" if not skip and missing_cnt > 0 else "❌"
            print(f"  {status} {test_code}: skip={skip} 缺失={missing_cnt} (期望: skip=False, 缺失>=1)")
            if skip:
                errors.append("场景2: 删除数据后仍被跳过")

            _restore_row(cur, conn, test_code, delete_date, original)
            print(f"  ✅ 数据已恢复")

            skip, _, missing_cnt, _ = check_skip_logic(test_code, kline_max)
            status = "✅" if skip else "❌"
            print(f"  {status} 恢复后: skip={skip} 缺失={missing_cnt} (期望: skip=True)")
            if not skip:
                errors.append("场景2: 恢复数据后仍不跳过")

        # ── 场景3: target_date 影响 ──
        print("\n" + "=" * 80)
        print("场景3: target_date 对跳过逻辑的影响")
        test_code = test_stocks[0]
        skip_1, _, missing_1, _ = check_skip_logic(test_code, kline_max)
        print(f"  target_date={kline_max}: skip={skip_1} 缺失={missing_1}")

        d = datetime.fromisoformat(kline_max).date() - timedelta(days=1)
        while not is_a_share_trading_day(d):
            d -= timedelta(days=1)
        prev_date = d.isoformat()
        skip_2, _, missing_2, _ = check_skip_logic(test_code, prev_date)
        print(f"  target_date={prev_date}: skip={skip_2} 缺失={missing_2}")

        # ── 场景4: 批量方法三分类 ──
        print("\n" + "=" * 80)
        print("场景4: _batch_check_completeness 三分类")
        complete, only_latest = _batch_check_completeness(test_stocks, kline_max)
        print(f"  完整={len(complete)} 仅缺最新日={len(only_latest)} 需拉取={len(test_stocks)-len(complete)-len(only_latest)}")
        for code in test_stocks:
            single_skip, _, missing_cnt, missing_dates = check_skip_logic(code, kline_max)
            in_complete = code in complete
            in_latest = code in only_latest
            # 验证一致性
            if single_skip:
                ok = in_complete and not in_latest
            elif missing_cnt == 1 and missing_dates == {kline_max}:
                ok = not in_complete and in_latest
            else:
                ok = not in_complete and not in_latest
            status = "✅" if ok else "❌"
            cat = "完整" if in_complete else ("仅缺最新日" if in_latest else "需拉取")
            print(f"  {status} {code}: 分类={cat} 单只skip={single_skip} 缺失={missing_cnt}")
            if not ok:
                errors.append(f"场景4: {code} 分类不一致")

        # ── 场景5: 删除最新日 → 应归入 only_latest_missing ──
        print("\n" + "=" * 80)
        print("场景5: 删除最新日数据 → 应归入 only_latest_missing")
        test_code_5 = test_stocks[0]
        cur.execute("SELECT * FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                    (test_code_5, kline_max))
        orig_5 = cur.fetchone()
        if orig_5:
            cur.execute("DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                        (test_code_5, kline_max))
            conn.commit()

            complete_5, latest_5 = _batch_check_completeness([test_code_5], kline_max)
            in_complete = test_code_5 in complete_5
            in_latest = test_code_5 in latest_5
            status = "✅" if in_latest and not in_complete else "❌"
            print(f"  {status} 删除{kline_max}后: complete={in_complete} only_latest={in_latest} (期望: only_latest=True)")
            if not in_latest:
                errors.append("场景5: 删除最新日后未归入 only_latest_missing")

            _restore_row(cur, conn, test_code_5, kline_max, orig_5)
            print(f"  ✅ 数据已恢复")
        else:
            print(f"  ⚠️ {test_code_5} 在 {kline_max} 无资金流向数据，跳过")

        # ── 场景6: 删除非最新日 → 不应在 complete 也不在 only_latest ──
        print("\n" + "=" * 80)
        print("场景6: 删除非最新日数据 → 应归入需拉取")
        test_code_6 = test_stocks[0]
        cur.execute(
            "SELECT `date` FROM stock_fund_flow "
            "WHERE stock_code = %s AND `date` < %s "
            "ORDER BY `date` DESC LIMIT 5",
            (test_code_6, kline_max)
        )
        rows_6 = cur.fetchall()
        if len(rows_6) >= 3:
            del_date_6 = str(rows_6[2]["date"])
            cur.execute("SELECT * FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                        (test_code_6, del_date_6))
            orig_6 = cur.fetchone()
            cur.execute("DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                        (test_code_6, del_date_6))
            conn.commit()

            complete_6, latest_6 = _batch_check_completeness([test_code_6], kline_max)
            in_complete = test_code_6 in complete_6
            in_latest = test_code_6 in latest_6
            status = "✅" if not in_complete and not in_latest else "❌"
            print(f"  {status} 删除{del_date_6}后: complete={in_complete} only_latest={in_latest} (期望: 都为False)")
            if in_complete or in_latest:
                errors.append("场景6: 删除非最新日后分类错误")

            _restore_row(cur, conn, test_code_6, del_date_6, orig_6)
            print(f"  ✅ 数据已恢复")

        # ── 场景7: 性能对比 ──
        print("\n" + "=" * 80)
        print("场景7: 性能对比 (100只股票)")
        import time
        cur.execute("SELECT DISTINCT stock_code FROM stock_fund_flow ORDER BY RAND() LIMIT 100")
        perf_codes = [r["stock_code"] for r in cur.fetchall()]

        t0 = time.time()
        single_results = {}
        for code in perf_codes:
            skip, _, _, _ = check_skip_logic(code, kline_max)
            single_results[code] = skip
        single_elapsed = time.time() - t0
        single_skip_cnt = sum(1 for v in single_results.values() if v)

        t0 = time.time()
        batch_complete, batch_latest = _batch_check_completeness(perf_codes, kline_max)
        batch_elapsed = time.time() - t0

        print(f"  单只: {single_elapsed:.2f}s ({single_elapsed/100*1000:.1f}ms/只) 跳过={single_skip_cnt}")
        print(f"  批量: {batch_elapsed:.2f}s 完整={len(batch_complete)} 仅缺最新日={len(batch_latest)}")
        print(f"  加速比: {single_elapsed/max(batch_elapsed, 0.001):.1f}x")

        # 一致性：complete 应与单只 skip=True 一致
        mismatch = 0
        for code in perf_codes:
            s = single_results[code]
            b = code in batch_complete
            if s != b:
                mismatch += 1
                if mismatch <= 3:
                    print(f"  ⚠️ 不一致: {code} 单只skip={s} 批量complete={b}")
        if mismatch:
            print(f"  ❌ {mismatch} 只股票结果不一致")
            errors.append(f"场景7: {mismatch}只股票单只与批量结果不一致")
        else:
            print(f"  ✅ 100只股票 complete 结果完全一致")

        # 汇总
        print("\n" + "=" * 80)
        if errors:
            print(f"❌ {len(errors)} 个问题:")
            for e in errors:
                print(f"   - {e}")
        else:
            print("✅ 所有验证通过（三分类 + 性能）")

    except Exception as e:
        print(f"❌ 验证异常: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    validate()
