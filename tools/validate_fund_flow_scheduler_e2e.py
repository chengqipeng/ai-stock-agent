"""
端到端验证资金流向调度的补全逻辑。

模拟调度器场景：
  1. 选取10只有当日资金流向数据的股票
  2. 删除其中5只的当日数据（模拟拉取失败）
  3. 调用 fill_missing_fund_flow_from_kline 补全
  4. 验证补全后的数据正确性
  5. 验证 counter 计数逻辑（模拟调度器中的计数更新）
  6. 恢复所有原始数据

Usage:
    python -m tools.validate_fund_flow_scheduler_e2e
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dao import get_connection
from service.analysis.fund_flow_fallback import fill_missing_fund_flow_from_kline


def validate_e2e():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    try:
        # 1. 找最新的同时有资金流向和K线数据的交易日
        cur.execute(
            "SELECT MAX(`date`) as max_date FROM stock_kline WHERE `date` <= CURDATE()"
        )
        kline_max = cur.fetchone()["max_date"]
        if not kline_max:
            print("❌ stock_kline 无数据")
            return

        cur.execute(
            "SELECT COUNT(DISTINCT stock_code) as cnt FROM stock_kline WHERE `date` = %s",
            (kline_max,)
        )
        kline_cnt = cur.fetchone()["cnt"]

        # 确认该日也有资金流向数据
        cur.execute(
            "SELECT COUNT(*) as cnt FROM stock_fund_flow WHERE `date` = %s AND net_flow IS NOT NULL",
            (kline_max,)
        )
        ff_cnt = cur.fetchone()["cnt"]

        if ff_cnt < 10 or kline_cnt < 100:
            # 回退一天
            cur.execute(
                "SELECT MAX(`date`) as max_date FROM stock_kline WHERE `date` < %s",
                (kline_max,)
            )
            kline_max = cur.fetchone()["max_date"]
            cur.execute(
                "SELECT COUNT(DISTINCT stock_code) as cnt FROM stock_kline WHERE `date` = %s",
                (kline_max,)
            )
            kline_cnt = cur.fetchone()["cnt"]

        latest_date = str(kline_max)
        print(f"测试日期: {latest_date}")
        print(f"stock_kline 当日股票数: {kline_cnt}")

        # 2. 随机选10只有完整资金流向数据的股票
        cur.execute(
            "SELECT stock_code, close_price, change_pct, net_flow, big_net, "
            "       big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct, "
            "       main_net_5day "
            "FROM stock_fund_flow "
            "WHERE `date` = %s AND close_price IS NOT NULL AND net_flow IS NOT NULL "
            "ORDER BY RAND() LIMIT 10",
            (latest_date,)
        )
        test_stocks = cur.fetchall()
        if len(test_stocks) < 10:
            print(f"⚠️ 只找到 {len(test_stocks)} 只股票，继续测试")
        if not test_stocks:
            print("❌ 无可测试股票")
            return

        all_codes = [s["stock_code"] for s in test_stocks]
        # 前5只模拟"拉取失败"（删除当日数据），后5只保持不变（模拟"已有数据"）
        fail_codes = all_codes[:5]
        ok_codes = all_codes[5:]

        print(f"\n模拟场景: {len(all_codes)} 只股票, {len(fail_codes)} 只模拟失败, {len(ok_codes)} 只正常")
        print(f"  失败股票: {fail_codes}")
        print(f"  正常股票: {ok_codes}")

        # 保存原始数据
        originals = {}
        for s in test_stocks:
            originals[s["stock_code"]] = dict(s)

        # 3. 删除"失败"股票的当日数据
        for code in fail_codes:
            cur.execute(
                "DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                (code, latest_date)
            )
        conn.commit()
        print("\n已删除失败股票的当日资金流向数据")

        # 4. 调用补全函数
        print("\n调用 fill_missing_fund_flow_from_kline...")
        result = fill_missing_fund_flow_from_kline(all_codes, latest_date)
        print(f"  结果: checked={result['checked']} missing={result['missing']} "
              f"filled={result['filled']} skipped={result['skipped']}")

        # 5. 验证结果
        print("\n" + "=" * 100)
        print("验证补全结果:")

        errors = []

        # 5a. 验证 missing 数量 = 失败股票数
        if result["missing"] != len(fail_codes):
            errors.append(f"missing 应为 {len(fail_codes)}，实际 {result['missing']}")
            print(f"  ❌ missing 数量不对: 期望{len(fail_codes)} 实际{result['missing']}")
        else:
            print(f"  ✅ missing = {result['missing']} (正确)")

        # 5b. 验证 filled 数量
        if result["filled"] != len(fail_codes):
            print(f"  ⚠️ filled = {result['filled']} (期望{len(fail_codes)}，可能部分无K线)")
        else:
            print(f"  ✅ filled = {result['filled']} (正确)")

        # 5c. 验证补全后的数据
        print("\n补全数据对比:")
        for code in fail_codes:
            cur.execute(
                "SELECT close_price, change_pct, net_flow, big_net, big_net_pct, "
                "       mid_net, mid_net_pct, small_net, small_net_pct, main_net_5day "
                "FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                (code, latest_date)
            )
            filled_row = cur.fetchone()
            orig = originals[code]

            if not filled_row:
                errors.append(f"{code}: 补全后无数据")
                print(f"  ❌ {code}: 补全后无数据")
                continue

            # close_price 应完全一致
            if filled_row["close_price"] != orig["close_price"]:
                errors.append(f"{code}: close_price 不一致 {filled_row['close_price']} vs {orig['close_price']}")
                print(f"  ❌ {code}: close_price {filled_row['close_price']} != {orig['close_price']}")
            else:
                print(f"  ✅ {code}: close_price={filled_row['close_price']} ✓", end="")

            # change_pct 应完全一致
            if filled_row["change_pct"] != orig["change_pct"]:
                errors.append(f"{code}: change_pct 不一致")
                print(f" change_pct ❌", end="")
            else:
                print(f" change_pct={filled_row['change_pct']} ✓", end="")

            # 资金流字段应为 NULL
            null_fields = ["net_flow", "big_net", "big_net_pct", "mid_net",
                           "mid_net_pct", "small_net", "small_net_pct"]
            non_null = [f for f in null_fields if filled_row[f] is not None]
            if non_null:
                errors.append(f"{code}: 资金流字段应为NULL但不是: {non_null}")
                print(f" 资金流NULL ❌({non_null})")
            else:
                print(f" 资金流字段=NULL ✓")

        # 5d. 验证正常股票未被修改
        print("\n正常股票验证:")
        for code in ok_codes:
            cur.execute(
                "SELECT close_price, change_pct, net_flow, big_net "
                "FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                (code, latest_date)
            )
            row = cur.fetchone()
            orig = originals[code]
            if row and row["net_flow"] == orig["net_flow"]:
                print(f"  ✅ {code}: 数据未被修改 net_flow={row['net_flow']}")
            else:
                errors.append(f"{code}: 正常股票数据被意外修改")
                print(f"  ❌ {code}: 数据被意外修改")

        # 5e. 模拟调度器的 counter 更新逻辑
        print("\n模拟调度器 counter 逻辑:")
        counter = {"success": len(ok_codes), "failed": len(fail_codes), "skipped": 0}
        print(f"  补全前: success={counter['success']} failed={counter['failed']}")

        fb_filled = result.get("filled", 0)
        if fb_filled > 0:
            counter["success"] += fb_filled
            counter["failed"] = max(0, counter["failed"] - fb_filled)
        print(f"  补全后: success={counter['success']} failed={counter['failed']}")

        expected_success = len(ok_codes) + result["filled"]
        expected_failed = max(0, len(fail_codes) - result["filled"])
        if counter["success"] == expected_success and counter["failed"] == expected_failed:
            print(f"  ✅ counter 计算正确")
        else:
            errors.append(f"counter 不正确: 期望 success={expected_success} failed={expected_failed}")
            print(f"  ❌ counter 不正确")

        # 6. 恢复原始数据
        print("\n恢复原始数据...")
        for code in fail_codes:
            cur.execute(
                "DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                (code, latest_date)
            )
            orig = originals[code]
            cur.execute(
                "INSERT INTO stock_fund_flow "
                "(stock_code, `date`, close_price, change_pct, net_flow, main_net_5day, "
                "big_net, big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (code, latest_date, orig["close_price"], orig["change_pct"],
                 orig["net_flow"], orig["main_net_5day"],
                 orig["big_net"], orig["big_net_pct"],
                 orig["mid_net"], orig["mid_net_pct"],
                 orig["small_net"], orig["small_net_pct"]),
            )
        conn.commit()
        print("✅ 原始数据已恢复")

        # 7. 汇总
        print("\n" + "=" * 100)
        if errors:
            print(f"❌ 发现 {len(errors)} 个问题:")
            for e in errors:
                print(f"   - {e}")
        else:
            print("✅ 端到端验证全部通过")
            print("   - 补全函数正确识别缺失股票")
            print("   - close_price / change_pct 与K线完全一致")
            print("   - 资金流特有字段正确设为 NULL")
            print("   - 已有数据的股票不受影响")
            print("   - 调度器 counter 更新逻辑正确")

    except Exception as e:
        print(f"❌ 验证异常: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    validate_e2e()
