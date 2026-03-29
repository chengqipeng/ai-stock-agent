"""
全量资金流向数据验证与修复脚本

对所有股票执行：
  1. K线同步 close_price / change_pct（单条批量 UPDATE JOIN，全表一次完成）
  2. 异常检测（net_flow / close_price / change_pct 为 NULL）
  3. 输出修复统计和剩余异常明细

用法: .venv/bin/python tools/verify_and_fix_all_fund_flow.py
"""
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dao import get_connection


def main():
    start = time.time()

    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    try:
        # 统计修复前的状态
        cur.execute("SELECT COUNT(DISTINCT stock_code) AS cnt FROM stock_fund_flow")
        total_stocks = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) AS cnt FROM stock_fund_flow")
        total_records = cur.fetchone()["cnt"]

        print(f"共 {total_stocks} 只股票，{total_records} 条资金流向记录\n")

        # ── 修复前：统计有问题的记录数 ──
        print("=" * 60)
        print("  修复前状态")
        print("=" * 60)

        # close_price 为 NULL
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow WHERE close_price IS NULL"
        )
        null_price_before = cur.fetchone()["cnt"]

        # change_pct 为 NULL
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow WHERE change_pct IS NULL"
        )
        null_pct_before = cur.fetchone()["cnt"]

        # close_price 与 K线不一致（差值 > 0.001）
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow ff "
            "JOIN stock_kline k ON ff.stock_code = k.stock_code AND ff.`date` = k.`date` "
            "WHERE k.close_price IS NOT NULL "
            "  AND ff.close_price IS NOT NULL "
            "  AND ABS(ff.close_price - k.close_price) > 0.001"
        )
        mismatch_price_before = cur.fetchone()["cnt"]

        # change_pct 与 K线不一致
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow ff "
            "JOIN stock_kline k ON ff.stock_code = k.stock_code AND ff.`date` = k.`date` "
            "WHERE k.change_percent IS NOT NULL "
            "  AND ff.change_pct IS NOT NULL "
            "  AND ABS(ff.change_pct - k.change_percent) > 0.001"
        )
        mismatch_pct_before = cur.fetchone()["cnt"]

        # net_flow 为 NULL
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow WHERE net_flow IS NULL"
        )
        null_netflow_before = cur.fetchone()["cnt"]

        print(f"  close_price NULL:       {null_price_before}")
        print(f"  change_pct NULL:        {null_pct_before}")
        print(f"  close_price mismatch:   {mismatch_price_before}")
        print(f"  change_pct mismatch:    {mismatch_pct_before}")
        print(f"  net_flow NULL:          {null_netflow_before}")

        # ── 第一步：全表 K线同步 ──
        print(f"\n{'=' * 60}")
        print("  执行 K线同步 close_price / change_pct")
        print("=" * 60)

        sync_start = time.time()
        cur.execute(
            "UPDATE stock_fund_flow ff "
            "JOIN stock_kline k ON ff.stock_code = k.stock_code AND ff.`date` = k.`date` "
            "SET ff.close_price = k.close_price, ff.change_pct = k.change_percent "
            "WHERE k.close_price IS NOT NULL "
            "  AND (ff.close_price IS NULL "
            "       OR ff.change_pct IS NULL "
            "       OR ABS(ff.close_price - k.close_price) > 0.001 "
            "       OR ABS(ff.change_pct - k.change_percent) > 0.001)"
        )
        synced = cur.rowcount
        conn.commit()
        sync_elapsed = time.time() - sync_start
        print(f"  同步更新: {synced} 条记录  耗时: {sync_elapsed:.1f}s")

        # ── 修复后：重新统计 ──
        print(f"\n{'=' * 60}")
        print("  修复后状态")
        print("=" * 60)

        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow WHERE close_price IS NULL"
        )
        null_price_after = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow WHERE change_pct IS NULL"
        )
        null_pct_after = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow ff "
            "JOIN stock_kline k ON ff.stock_code = k.stock_code AND ff.`date` = k.`date` "
            "WHERE k.close_price IS NOT NULL "
            "  AND ff.close_price IS NOT NULL "
            "  AND ABS(ff.close_price - k.close_price) > 0.001"
        )
        mismatch_price_after = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow ff "
            "JOIN stock_kline k ON ff.stock_code = k.stock_code AND ff.`date` = k.`date` "
            "WHERE k.change_percent IS NOT NULL "
            "  AND ff.change_pct IS NOT NULL "
            "  AND ABS(ff.change_pct - k.change_percent) > 0.001"
        )
        mismatch_pct_after = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) AS cnt FROM stock_fund_flow WHERE net_flow IS NULL"
        )
        null_netflow_after = cur.fetchone()["cnt"]

        print(f"  close_price NULL:       {null_price_before} → {null_price_after}")
        print(f"  change_pct NULL:        {null_pct_before} → {null_pct_after}")
        print(f"  close_price mismatch:   {mismatch_price_before} → {mismatch_price_after}")
        print(f"  change_pct mismatch:    {mismatch_pct_before} → {mismatch_pct_after}")
        print(f"  net_flow NULL:          {null_netflow_before} → {null_netflow_after}")

        # ── 剩余异常明细 ──
        remaining_issues = (null_price_after + null_pct_after +
                            mismatch_price_after + mismatch_pct_after)

        if null_price_after > 0 or null_pct_after > 0:
            print(f"\n  剩余 close_price/change_pct NULL 的记录（K线中也无数据）:")
            cur.execute(
                "SELECT ff.stock_code, ff.`date`, ff.close_price, ff.change_pct "
                "FROM stock_fund_flow ff "
                "LEFT JOIN stock_kline k ON ff.stock_code = k.stock_code AND ff.`date` = k.`date` "
                "WHERE (ff.close_price IS NULL OR ff.change_pct IS NULL) "
                "  AND ff.`date` >= '2024-01-01' "
                "ORDER BY ff.stock_code, ff.`date` "
                "LIMIT 30"
            )
            rows = cur.fetchall()
            for r in rows:
                print(f"    {r['stock_code']}  {r['date']}  "
                      f"close_price={'NULL' if r['close_price'] is None else r['close_price']}  "
                      f"change_pct={'NULL' if r['change_pct'] is None else r['change_pct']}")
            if len(rows) == 30:
                print(f"    ... (仅显示前30条)")

        if null_netflow_after > 0:
            print(f"\n  net_flow NULL 的记录（K线无法补全，需数据源修复）:")
            cur.execute(
                "SELECT stock_code, COUNT(*) AS cnt "
                "FROM stock_fund_flow "
                "WHERE net_flow IS NULL AND `date` >= '2024-01-01' "
                "GROUP BY stock_code "
                "ORDER BY cnt DESC "
                "LIMIT 20"
            )
            rows = cur.fetchall()
            for r in rows:
                print(f"    {r['stock_code']}: {r['cnt']} 条")
            if len(rows) == 20:
                cur.execute(
                    "SELECT COUNT(DISTINCT stock_code) AS cnt "
                    "FROM stock_fund_flow "
                    "WHERE net_flow IS NULL AND `date` >= '2024-01-01'"
                )
                total_null_nf_stocks = cur.fetchone()["cnt"]
                print(f"    ... 共 {total_null_nf_stocks} 只股票")

        # ── 最终结论 ──
        elapsed = time.time() - start
        print(f"\n{'=' * 60}")
        if remaining_issues == 0 and null_netflow_after == 0:
            print(f"  ✅ 全部通过  {total_stocks} 只股票 {total_records} 条记录无异常")
        elif remaining_issues == 0:
            print(f"  ✅ 价格字段全部修复  剩余 net_flow NULL {null_netflow_after} 条（需数据源补全）")
        else:
            print(f"  ⚠️ 仍有 {remaining_issues} 条价格异常（K线中无对应数据）")
        print(f"  耗时: {elapsed:.1f}s")
        print("=" * 60)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
