"""快速验证 zero_derived 修复结果"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dao import get_connection

TABLE_NAME = "stock_kline"

def main():
    conn = get_connection()
    cursor = conn.cursor()

    # 1. 全局统计：还有多少 zero_derived 异常
    cursor.execute(f"""
        SELECT COUNT(*) FROM {TABLE_NAME}
        WHERE amplitude = 0 AND change_percent = 0 AND change_amount = 0
          AND NOT (open_price = 0 AND close_price = 0 AND high_price = 0 AND low_price = 0)
          AND NOT (open_price = close_price AND close_price = high_price AND high_price = low_price)
    """)
    remaining = cursor.fetchone()[0]
    print(f"\n[全局] 剩余 zero_derived 异常记录数: {remaining}")

    # 2. 针对 000132.SH 验证
    cursor.execute(f"""
        SELECT `date`, open_price, close_price, high_price, low_price,
               amplitude, change_percent, change_amount
        FROM {TABLE_NAME}
        WHERE stock_code = '000132.SH' AND `date` = '2026-03-10'
    """)
    row = cursor.fetchone()
    if row:
        d, op, cp, hp, lp, amp, chg_pct, chg_amt = row
        print(f"\n[000132.SH 2026-03-10]")
        print(f"  O={op}, C={cp}, H={hp}, L={lp}")
        print(f"  amplitude={amp}, change_percent={chg_pct}, change_amount={chg_amt}")
        if amp == 0 and chg_pct == 0 and chg_amt == 0:
            print("  ❌ 仍未修复")
        else:
            print("  ✅ 已修复")
    else:
        print("\n[000132.SH 2026-03-10] 未找到记录")

    # 3. 如果还有残留，看看分布
    if remaining > 0:
        cursor.execute(f"""
            SELECT stock_code, COUNT(*) as cnt
            FROM {TABLE_NAME}
            WHERE amplitude = 0 AND change_percent = 0 AND change_amount = 0
              AND NOT (open_price = 0 AND close_price = 0 AND high_price = 0 AND low_price = 0)
              AND NOT (open_price = close_price AND close_price = high_price AND high_price = low_price)
            GROUP BY stock_code
            ORDER BY cnt DESC
            LIMIT 20
        """)
        rows = cursor.fetchall()
        print(f"\n[残留异常 Top20]")
        for r in rows:
            print(f"  {r[0]:12s}  {r[1]}条")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
