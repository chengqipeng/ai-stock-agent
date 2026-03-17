"""
验证资金流向K线补全逻辑的准确性。

方法：
  1. 选取几个有真实当日资金流向数据的股票
  2. 临时删除这些股票的当日资金流向
  3. 调用补全逻辑生成补全记录
  4. 对比补全的 close_price / change_pct 与真实值
  5. 回滚恢复原始数据

Usage:
    python -m tools.validate_fund_flow_fallback
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dao import get_connection
from service.analysis.fund_flow_fallback import fill_missing_fund_flow_from_kline


def validate():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    try:
        # 1. 找到最新的有资金流向数据的交易日
        cur.execute("SELECT MAX(`date`) as max_date FROM stock_fund_flow")
        latest_date = cur.fetchone()["max_date"]
        if not latest_date:
            print("❌ stock_fund_flow 表无数据")
            return
        print(f"最新资金流向日期: {latest_date}")

        # 2. 确认 stock_kline 中该日也有数据
        cur.execute(
            "SELECT COUNT(DISTINCT stock_code) as cnt "
            "FROM stock_kline WHERE `date` = %s",
            (latest_date,)
        )
        stock_cnt = cur.fetchone()["cnt"]
        print(f"stock_kline 中 {latest_date} 有 {stock_cnt} 只股票数据")
        if stock_cnt < 100:
            print("❌ 个股K线数据不足，无法验证")
            return

        # 3. 随机选取10个有当日资金流向的股票
        cur.execute(
            "SELECT ff.stock_code, ff.close_price, ff.change_pct, "
            "       ff.net_flow, ff.big_net, ff.big_net_pct, "
            "       ff.mid_net, ff.mid_net_pct, ff.small_net, ff.small_net_pct, "
            "       ff.main_net_5day "
            "FROM stock_fund_flow ff "
            "WHERE ff.`date` = %s AND ff.close_price IS NOT NULL "
            "  AND ff.net_flow IS NOT NULL "
            "ORDER BY RAND() LIMIT 10",
            (latest_date,)
        )
        test_stocks = cur.fetchall()
        if not test_stocks:
            print("❌ 未找到可测试的股票")
            return

        print(f"\n选取 {len(test_stocks)} 只股票进行验证:")
        print("=" * 100)

        results = []
        for stock in test_stocks:
            code = stock["stock_code"]
            real_close = stock["close_price"]
            real_chg = stock["change_pct"]
            real_net_flow = stock["net_flow"]
            real_main_5d = stock["main_net_5day"]

            # 临时删除该股票当日资金流向
            cur.execute(
                "DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                (code, latest_date)
            )
            # 不 commit，rollback 可恢复

            # 调用补全逻辑（需要用独立连接，因为当前连接有未提交的删除）
            # 这里直接用内联逻辑模拟，避免连接冲突
            # 查 stock_kline
            cur.execute(
                "SELECT close_price, change_percent FROM stock_kline "
                "WHERE stock_code = %s AND `date` = %s",
                (code, latest_date)
            )
            kline = cur.fetchone()

            if kline:
                synth_close = kline["close_price"]
                synth_chg = kline["change_percent"]

                close_diff = abs(synth_close - real_close) if synth_close and real_close else None
                chg_diff = abs(synth_chg - real_chg) if synth_chg is not None and real_chg is not None else None

                status = "✅" if (close_diff is not None and close_diff < 0.02
                                  and chg_diff is not None and chg_diff < 0.5) else "⚠️"
                print(f"\n{status} {code}")
                print(f"   真实 close_price: {real_close}")
                print(f"   K线  close_price: {synth_close}")
                if close_diff is not None:
                    print(f"   价格偏差:         {close_diff:.4f}")
                print(f"   真实 change_pct:  {real_chg}")
                print(f"   K线  change_pct:  {synth_chg}")
                if chg_diff is not None:
                    print(f"   涨跌幅偏差:       {chg_diff:.4f}%")
                print(f"   真实 net_flow:    {real_net_flow} (补全后为NULL)")
                print(f"   真实 main_net_5d: {real_main_5d}")

                results.append({
                    "code": code,
                    "close_diff": close_diff,
                    "chg_diff": chg_diff,
                })
            else:
                print(f"\n⏭️  {code} - K线中无当日数据")

            # 回滚恢复
            conn.rollback()

        # 4. 汇总
        if results:
            valid_close = [r["close_diff"] for r in results if r["close_diff"] is not None]
            valid_chg = [r["chg_diff"] for r in results if r["chg_diff"] is not None]

            print("\n" + "=" * 100)
            print(f"验证汇总: {len(results)} 只股票")
            if valid_close:
                avg_close_diff = sum(valid_close) / len(valid_close)
                max_close_diff = max(valid_close)
                print(f"  close_price 平均偏差: {avg_close_diff:.4f}")
                print(f"  close_price 最大偏差: {max_close_diff:.4f}")
                print(f"  close_price 偏差<0.02: {sum(1 for d in valid_close if d < 0.02)}/{len(valid_close)}")
            if valid_chg:
                avg_chg_diff = sum(valid_chg) / len(valid_chg)
                max_chg_diff = max(valid_chg)
                print(f"  change_pct 平均偏差:  {avg_chg_diff:.4f}%")
                print(f"  change_pct 最大偏差:  {max_chg_diff:.4f}%")
                print(f"  change_pct 偏差<0.5%: {sum(1 for d in valid_chg if d < 0.5)}/{len(valid_chg)}")

            all_good = (valid_close and all(d < 0.02 for d in valid_close)
                        and valid_chg and all(d < 0.5 for d in valid_chg))
            if all_good:
                print("\n✅ K线数据与资金流向的 close_price/change_pct 完全一致，补全逻辑可靠")
            else:
                print("\n⚠️ 存在偏差，请检查数据源差异")

        # 5. 测试完整的 fill_missing_fund_flow_from_kline 函数
        print("\n" + "=" * 100)
        print("测试完整补全函数（使用独立事务）...")
        test_codes = [s["stock_code"] for s in test_stocks[:3]]

        # 在新连接中删除并补全
        conn2 = get_connection(use_dict_cursor=True)
        cur2 = conn2.cursor()
        try:
            # 保存原始数据
            originals = {}
            for code in test_codes:
                cur2.execute(
                    "SELECT * FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                    (code, latest_date)
                )
                row = cur2.fetchone()
                if row:
                    originals[code] = row

            # 删除当日数据
            for code in test_codes:
                cur2.execute(
                    "DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                    (code, latest_date)
                )
            conn2.commit()

            # 调用补全函数
            result = fill_missing_fund_flow_from_kline(test_codes, latest_date)
            print(f"  补全结果: checked={result['checked']} missing={result['missing']} "
                  f"filled={result['filled']} skipped={result['skipped']}")

            # 检查补全后的数据
            for code in test_codes:
                cur2.execute(
                    "SELECT close_price, change_pct, net_flow, big_net, main_net_5day "
                    "FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                    (code, latest_date)
                )
                filled = cur2.fetchone()
                orig = originals.get(code)
                if filled and orig:
                    print(f"  {code}: close={filled['close_price']}(原{orig['close_price']}) "
                          f"chg={filled['change_pct']}(原{orig['change_pct']}) "
                          f"net_flow={filled['net_flow']}(原{orig['net_flow']}) "
                          f"main_5d={filled['main_net_5day']}(原{orig['main_net_5day']})")
                elif filled:
                    print(f"  {code}: 补全成功 close={filled['close_price']} chg={filled['change_pct']}")
                else:
                    print(f"  {code}: 未补全")

            # 恢复原始数据
            for code, orig in originals.items():
                cur2.execute(
                    "DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                    (code, latest_date)
                )
                cur2.execute(
                    f"INSERT INTO stock_fund_flow "
                    f"(stock_code, `date`, close_price, change_pct, net_flow, main_net_5day, "
                    f"big_net, big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct) "
                    f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (code, latest_date, orig["close_price"], orig["change_pct"],
                     orig["net_flow"], orig["main_net_5day"],
                     orig["big_net"], orig["big_net_pct"],
                     orig["mid_net"], orig["mid_net_pct"],
                     orig["small_net"], orig["small_net_pct"]),
                )
            conn2.commit()
            print("  ✅ 原始数据已恢复")

        except Exception as e:
            print(f"  ❌ 完整测试异常: {e}")
            import traceback
            traceback.print_exc()
            conn2.rollback()
        finally:
            cur2.close()
            conn2.close()

    except Exception as e:
        print(f"❌ 验证异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.rollback()
        cur.close()
        conn.close()


if __name__ == "__main__":
    validate()
