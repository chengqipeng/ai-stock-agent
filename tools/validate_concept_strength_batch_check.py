"""
验证概念板块K线分批完整性检查逻辑。

测试场景：
  1. 批量检查已有完整K线的板块 → 应返回完整
  2. 删除某板块最新日K线 → 应不在完整集合中
  3. 恢复后再次检查 → 应返回完整
  4. 性能测试

Usage:
    .venv/bin/python -m tools.validate_concept_strength_batch_check
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from dao import get_connection
from service.auto_job.concept_strength_scheduler import (
    _batch_check_board_kline_completeness, _get_effective_trade_date,
)


def validate():
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    errors = []

    try:
        target_date = _get_effective_trade_date()
        print(f"目标交易日: {target_date}")

        # 找有K线数据的板块
        cur.execute(
            "SELECT board_code, MAX(`date`) as max_d, COUNT(*) as cnt "
            "FROM concept_board_kline GROUP BY board_code "
            "HAVING max_d >= %s ORDER BY RAND() LIMIT 10",
            (target_date,),
        )
        test_boards = cur.fetchall()
        if not test_boards:
            print("❌ 找不到有完整K线数据的板块")
            return
        test_codes = [r["board_code"] for r in test_boards]
        print(f"测试板块: {len(test_codes)} 个")

        # ── 场景1: 完整板块应被识别 ──
        print(f"\n{'='*60}")
        print("场景1: 完整板块应被识别")
        complete = _batch_check_board_kline_completeness(test_codes, target_date)
        print(f"  完整: {len(complete)}/{len(test_codes)}")
        for code in test_codes:
            status = "✅" if code in complete else "❌"
            board = next(b for b in test_boards if b["board_code"] == code)
            print(f"  {status} {code} max_date={board['max_d']} cnt={board['cnt']}")
            if code not in complete:
                errors.append(f"场景1: {code} 应为完整但未识别")

        # ── 场景2: 删除最新日 → 不完整 ──
        print(f"\n{'='*60}")
        print("场景2: 删除最新日K线后应不完整")
        test_code = test_codes[0]
        cur.execute(
            "SELECT * FROM concept_board_kline WHERE board_code = %s AND `date` = %s",
            (test_code, target_date),
        )
        orig = cur.fetchone()
        if orig:
            cur.execute(
                "DELETE FROM concept_board_kline WHERE board_code = %s AND `date` = %s",
                (test_code, target_date),
            )
            conn.commit()

            complete2 = _batch_check_board_kline_completeness([test_code], target_date)
            in_complete = test_code in complete2
            status = "✅" if not in_complete else "❌"
            print(f"  {status} 删除后: complete={in_complete} (期望: False)")
            if in_complete:
                errors.append("场景2: 删除后仍为完整")

            # 恢复
            cur.execute(
                "INSERT INTO concept_board_kline "
                "(board_code, board_index_code, `date`, open_price, close_price, "
                "high_price, low_price, trading_volume, trading_amount, "
                "change_percent, change_amount, amplitude, change_hand) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE close_price=VALUES(close_price)",
                (orig["board_code"], orig["board_index_code"], orig["date"],
                 orig["open_price"], orig["close_price"],
                 orig["high_price"], orig["low_price"],
                 orig["trading_volume"], orig["trading_amount"],
                 orig["change_percent"], orig["change_amount"],
                 orig["amplitude"], orig["change_hand"]),
            )
            conn.commit()
            print("  ✅ 数据已恢复")

            complete3 = _batch_check_board_kline_completeness([test_code], target_date)
            status = "✅" if test_code in complete3 else "❌"
            print(f"  {status} 恢复后: complete={test_code in complete3} (期望: True)")
            if test_code not in complete3:
                errors.append("场景2: 恢复后仍不完整")
        else:
            print(f"  ⚠️ {test_code} 在 {target_date} 无K线数据")

        # ── 场景3: 性能测试 ──
        print(f"\n{'='*60}")
        print("场景3: 性能测试 (全部板块)")
        cur.execute("SELECT DISTINCT board_code FROM concept_board_kline")
        all_codes = [r["board_code"] for r in cur.fetchall()]
        print(f"  共 {len(all_codes)} 个板块")

        t0 = time.time()
        complete_all = _batch_check_board_kline_completeness(all_codes, target_date)
        elapsed = time.time() - t0
        print(f"  批量检查: {elapsed*1000:.0f}ms 完整={len(complete_all)} 需拉取={len(all_codes)-len(complete_all)}")

        # 汇总
        print(f"\n{'='*60}")
        if errors:
            print(f"❌ {len(errors)} 个问题:")
            for e in errors:
                print(f"   - {e}")
        else:
            print("✅ 所有验证通过")

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
