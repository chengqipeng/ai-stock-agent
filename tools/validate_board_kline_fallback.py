"""
验证板块K线合成逻辑的准确性。

方法：
  1. 选取几个有真实当日K线数据的板块
  2. 临时删除这些板块的当日K线
  3. 调用合成逻辑生成合成K线
  4. 对比合成的 change_percent 与真实值的偏差
  5. 恢复原始数据

Usage:
    python -m tools.validate_board_kline_fallback
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dao import get_connection
from service.analysis.board_kline_fallback import (
    _normalize_stock_code, _synthesize_single_board, synthesize_missing_board_klines,
)


def validate():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()

    try:
        # 1. 找到最新的有板块K线数据的交易日
        cur.execute(
            "SELECT MAX(`date`) as max_date FROM concept_board_kline"
        )
        latest_date = cur.fetchone()["max_date"]
        if not latest_date:
            print("❌ concept_board_kline 表无数据")
            return
        print(f"最新板块K线日期: {latest_date}")

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

        # 3. 随机选取5个有当日K线的板块
        cur.execute(
            "SELECT bk.board_code, cb.board_name, bk.change_percent, "
            "       bk.close_price, bk.open_price, bk.high_price, bk.low_price, "
            "       bk.trading_volume, bk.trading_amount "
            "FROM concept_board_kline bk "
            "JOIN stock_concept_board cb ON bk.board_code = cb.board_code "
            "WHERE bk.`date` = %s AND bk.change_percent IS NOT NULL "
            "ORDER BY RAND() LIMIT 5",
            (latest_date,)
        )
        test_boards = cur.fetchall()
        if not test_boards:
            print("❌ 未找到可测试的板块")
            return

        print(f"\n选取 {len(test_boards)} 个板块进行验证:")
        print("=" * 90)

        # 4. 保存原始数据，删除后合成，再对比
        results = []
        for board in test_boards:
            board_code = board["board_code"]
            board_name = board["board_name"]
            real_chg = board["change_percent"]
            real_close = board["close_price"]

            # 临时删除该板块当日K线
            cur.execute(
                "DELETE FROM concept_board_kline "
                "WHERE board_code = %s AND `date` = %s",
                (board_code, latest_date)
            )
            # 不 commit，这样 rollback 可以恢复

            # 调用合成逻辑
            synth_result = _synthesize_single_board(
                cur, board_code, board_name, latest_date
            )

            if synth_result:
                synth_chg = synth_result["change_percent"]
                diff = abs(synth_chg - real_chg)
                used = synth_result["used_count"]
                total = synth_result["member_count"]
                coverage = synth_result["coverage"]

                # 读取合成后写入的数据
                cur.execute(
                    "SELECT close_price, change_percent "
                    "FROM concept_board_kline "
                    "WHERE board_code = %s AND `date` = %s",
                    (board_code, latest_date)
                )
                synth_row = cur.fetchone()
                synth_close = synth_row["close_price"] if synth_row else None

                status = "✅" if diff < 0.5 else ("⚠️" if diff < 1.0 else "❌")
                print(f"\n{status} {board_code} {board_name}")
                print(f"   真实涨跌幅: {real_chg:+.4f}%")
                print(f"   合成涨跌幅: {synth_chg:+.4f}%")
                print(f"   偏差:       {diff:.4f}%")
                print(f"   真实收盘价: {real_close}")
                print(f"   合成收盘价: {synth_close}")
                print(f"   成分股:     {used}/{total} ({coverage}%)")

                results.append({
                    "board": board_name, "real": real_chg,
                    "synth": synth_chg, "diff": diff
                })
            else:
                print(f"\n⏭️  {board_code} {board_name} - 合成失败(成分股数据不足)")

            # 回滚恢复原始数据
            conn.rollback()

        # 5. 汇总
        if results:
            avg_diff = sum(r["diff"] for r in results) / len(results)
            max_diff = max(r["diff"] for r in results)
            print("\n" + "=" * 90)
            print(f"验证汇总: {len(results)} 个板块")
            print(f"  平均偏差: {avg_diff:.4f}%")
            print(f"  最大偏差: {max_diff:.4f}%")
            print(f"  偏差<0.5%: {sum(1 for r in results if r['diff'] < 0.5)}/{len(results)}")
            print(f"  偏差<1.0%: {sum(1 for r in results if r['diff'] < 1.0)}/{len(results)}")

            if avg_diff < 0.5:
                print("\n✅ 合成精度良好，平均偏差在0.5%以内")
            elif avg_diff < 1.0:
                print("\n⚠️ 合成精度可接受，平均偏差在1.0%以内")
            else:
                print("\n❌ 合成精度偏大，需要检查逻辑")

    except Exception as e:
        print(f"❌ 验证异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.rollback()  # 确保不会意外修改数据
        cur.close()
        conn.close()


if __name__ == "__main__":
    validate()
