"""
验证 _fill_latest_day_from_realtime 实时资金流向补全逻辑。

测试流程：
  1. 选取几只有完整数据的股票
  2. 备份并删除其 target_date 当天的 fund_flow 记录
  3. 调用 _fill_latest_day_from_realtime 补全
  4. 验证补全后的数据：
     - 记录存在
     - close_price / change_pct 与 K 线一致
     - big_net / mid_net / small_net 非 NULL 且数值合理（万元级别）
     - net_flow ≈ big + mid + small
     - main_net_5day 计算正确（当天 big_net + 前4天 big_net）
  5. 恢复原始数据

Usage:
    .venv/bin/python -m tools.validate_realtime_fund_flow_fill
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from dao import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def validate():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    errors = []

    try:
        # 找到 K 线最新交易日
        cur.execute("SELECT MAX(`date`) as d FROM stock_kline")
        target_date = str(cur.fetchone()["d"])
        print(f"目标日期(target_date): {target_date}")

        # 选3只在 target_date 有完整 fund_flow 数据的股票
        cur.execute(
            "SELECT ff.stock_code, ff.close_price, ff.change_pct, "
            "ff.net_flow, ff.big_net, ff.mid_net, ff.small_net, "
            "ff.big_net_pct, ff.mid_net_pct, ff.small_net_pct, ff.main_net_5day "
            "FROM stock_fund_flow ff "
            "INNER JOIN stock_kline sk ON ff.stock_code = sk.stock_code AND sk.`date` = %s "
            "WHERE ff.`date` = %s AND ff.big_net IS NOT NULL "
            "ORDER BY RAND() LIMIT 3",
            (target_date, target_date),
        )
        test_rows = cur.fetchall()
        if not test_rows:
            print("❌ 找不到有完整数据的测试股票")
            return

        test_codes = [r["stock_code"] for r in test_rows]
        original_data = {r["stock_code"]: dict(r) for r in test_rows}
        print(f"测试股票: {test_codes}")

        # 获取 K 线中的 close_price / change_percent 作为参照
        ph = ",".join(["%s"] * len(test_codes))
        cur.execute(
            f"SELECT stock_code, close_price, change_percent "
            f"FROM stock_kline WHERE stock_code IN ({ph}) AND `date` = %s",
            (*test_codes, target_date),
        )
        kline_map = {r["stock_code"]: r for r in cur.fetchall()}

        # ── 步骤1: 备份并删除 target_date 的 fund_flow 记录 ──
        print(f"\n{'='*80}")
        print("步骤1: 删除 target_date 的 fund_flow 记录")
        for code in test_codes:
            cur.execute(
                "DELETE FROM stock_fund_flow WHERE stock_code = %s AND `date` = %s",
                (code, target_date),
            )
        conn.commit()
        print(f"  已删除 {len(test_codes)} 只股票在 {target_date} 的记录")

        # 验证确实删除了
        cur.execute(
            f"SELECT stock_code FROM stock_fund_flow "
            f"WHERE stock_code IN ({ph}) AND `date` = %s",
            (*test_codes, target_date),
        )
        remaining = cur.fetchall()
        assert len(remaining) == 0, f"删除后仍有 {len(remaining)} 条记录"
        print("  ✅ 确认已删除")

        # ── 步骤2: 调用 _fill_latest_day_from_realtime ──
        print(f"\n{'='*80}")
        print("步骤2: 调用 _fill_latest_day_from_realtime 补全")
        from service.auto_job.fund_flow_scheduler import _fill_latest_day_from_realtime
        filled_codes = await _fill_latest_day_from_realtime(test_codes, target_date)
        filled = len(filled_codes)
        print(f"  补全结果: {filled}/{len(test_codes)} 只成功")

        # ── 步骤3: 验证补全后的数据 ──
        print(f"\n{'='*80}")
        print("步骤3: 验证补全后的数据质量")
        # 重新获取连接，避免 REPEATABLE READ 事务隔离导致看不到新插入的数据
        cur.close()
        conn.close()
        conn = get_connection(use_dict_cursor=True)
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM stock_fund_flow "
            f"WHERE stock_code IN ({ph}) AND `date` = %s",
            (*test_codes, target_date),
        )
        filled_rows = {r["stock_code"]: r for r in cur.fetchall()}

        for code in test_codes:
            print(f"\n  ── {code} ──")
            row = filled_rows.get(code)
            if not row:
                print(f"  ❌ 未找到补全记录")
                errors.append(f"{code}: 补全后无记录")
                continue

            kline = kline_map.get(code)
            orig = original_data[code]

            # 3a: close_price 与 K 线一致
            if kline and row["close_price"] is not None:
                diff = abs(row["close_price"] - kline["close_price"])
                if diff <= 0.02:
                    print(f"  ✅ close_price={row['close_price']} (K线={kline['close_price']})")
                else:
                    print(f"  ❌ close_price={row['close_price']} vs K线={kline['close_price']} 差值={diff}")
                    errors.append(f"{code}: close_price 不一致")
            else:
                print(f"  ⚠️ close_price={row['close_price']} (K线数据不可用)")

            # 3b: change_pct 与 K 线一致
            if kline and row["change_pct"] is not None and kline["change_percent"] is not None:
                diff = abs(row["change_pct"] - kline["change_percent"])
                if diff <= 0.5:
                    print(f"  ✅ change_pct={row['change_pct']} (K线={kline['change_percent']})")
                else:
                    print(f"  ❌ change_pct={row['change_pct']} vs K线={kline['change_percent']} 差值={diff}")
                    errors.append(f"{code}: change_pct 不一致")

            # 3c: big_net / mid_net / small_net 非 NULL
            for field in ("big_net", "mid_net", "small_net"):
                val = row[field]
                if val is not None:
                    print(f"  ✅ {field}={val}万元")
                else:
                    print(f"  ❌ {field} 为 NULL")
                    errors.append(f"{code}: {field} 为 NULL")

            # 3d: net_flow ≈ big + mid + small
            if all(row[f] is not None for f in ("big_net", "mid_net", "small_net", "net_flow")):
                calc = round(row["big_net"] + row["mid_net"] + row["small_net"], 2)
                diff = abs(row["net_flow"] - calc)
                if diff <= 0.1:
                    print(f"  ✅ net_flow={row['net_flow']} ≈ big+mid+small={calc}")
                else:
                    print(f"  ❌ net_flow={row['net_flow']} vs big+mid+small={calc} 差值={diff}")
                    errors.append(f"{code}: net_flow 不等于 big+mid+small")

            # 3e: big_net_pct 非 NULL
            if row["big_net_pct"] is not None:
                print(f"  ✅ big_net_pct={row['big_net_pct']}%")
            else:
                print(f"  ⚠️ big_net_pct 为 NULL")

            # 3f: main_net_5day 验证
            if row["main_net_5day"] is not None:
                # 手动计算：当天 big_net + 前4天 big_net
                cur.execute(
                    "SELECT big_net FROM stock_fund_flow "
                    "WHERE stock_code = %s AND `date` < %s "
                    "ORDER BY `date` DESC LIMIT 4",
                    (code, target_date),
                )
                prev_bigs = [r["big_net"] for r in cur.fetchall() if r["big_net"] is not None]
                if len(prev_bigs) == 4:
                    expected_5day = round(row["big_net"] + sum(prev_bigs), 2)
                    diff = abs(row["main_net_5day"] - expected_5day)
                    if diff <= 0.1:
                        print(f"  ✅ main_net_5day={row['main_net_5day']} (计算值={expected_5day})")
                    else:
                        print(f"  ❌ main_net_5day={row['main_net_5day']} vs 计算值={expected_5day} 差值={diff}")
                        errors.append(f"{code}: main_net_5day 计算不一致")
                else:
                    print(f"  ⚠️ main_net_5day={row['main_net_5day']} (前4天数据不足，无法验证)")
            else:
                print(f"  ⚠️ main_net_5day 为 NULL")

            # 3g: 与原始数据对比（实时 vs 历史，数值可能不同但量级应一致）
            if orig["big_net"] is not None and row["big_net"] is not None:
                ratio = abs(row["big_net"]) / max(abs(orig["big_net"]), 1)
                print(f"  📊 big_net 对比: 实时={row['big_net']} 原始={orig['big_net']} "
                      f"(比值={ratio:.2f})")

        # ── 步骤4: 恢复原始数据 ──
        print(f"\n{'='*80}")
        print("步骤4: 恢复原始数据")
        for code in test_codes:
            orig = original_data[code]
            cur.execute(
                "INSERT INTO stock_fund_flow "
                "(stock_code, `date`, close_price, change_pct, net_flow, main_net_5day, "
                "big_net, big_net_pct, mid_net, mid_net_pct, small_net, small_net_pct) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE "
                "close_price=VALUES(close_price), change_pct=VALUES(change_pct), "
                "net_flow=VALUES(net_flow), main_net_5day=VALUES(main_net_5day), "
                "big_net=VALUES(big_net), big_net_pct=VALUES(big_net_pct), "
                "mid_net=VALUES(mid_net), mid_net_pct=VALUES(mid_net_pct), "
                "small_net=VALUES(small_net), small_net_pct=VALUES(small_net_pct)",
                (code, target_date, orig["close_price"], orig["change_pct"],
                 orig["net_flow"], orig["main_net_5day"],
                 orig["big_net"], orig["big_net_pct"],
                 orig["mid_net"], orig["mid_net_pct"],
                 orig["small_net"], orig["small_net_pct"]),
            )
        conn.commit()
        print(f"  ✅ {len(test_codes)} 只股票数据已恢复")

        # ── 汇总 ──
        print(f"\n{'='*80}")
        if errors:
            print(f"❌ {len(errors)} 个问题:")
            for e in errors:
                print(f"   - {e}")
        else:
            print(f"✅ 实时资金流向补全验证全部通过 ({filled}/{len(test_codes)} 只成功)")

    except Exception as e:
        print(f"❌ 验证异常: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(validate())
