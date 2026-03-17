"""只修复上证(.SH)和深证(.SZ)的 zero_derived 异常，跳过北交所"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dao import get_connection

TABLE = "stock_kline"

def main():
    conn = get_connection()
    cur = conn.cursor()

    # 1. 诊断：只看 SH/SZ
    cur.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE amplitude = 0 AND change_percent = 0 AND change_amount = 0
          AND NOT (open_price = 0 AND close_price = 0 AND high_price = 0 AND low_price = 0)
          AND NOT (open_price = close_price AND close_price = high_price AND high_price = low_price)
          AND (stock_code LIKE '%%.SH' OR stock_code LIKE '%%.SZ')
    """)
    total = cur.fetchone()[0]
    print(f"[诊断] SH/SZ 待修复记录数: {total}")

    if total == 0:
        print("无需修复")
        cur.close(); conn.close()
        return

    # 2. SQL 批量修复
    print("执行 SQL 批量修复...")
    t0 = time.time()
    cur.execute(f"""
        UPDATE {TABLE} AS t
        INNER JOIN (
            SELECT id,
                   LAG(close_price) OVER (PARTITION BY stock_code ORDER BY `date`) AS prev_close,
                   high_price, low_price, close_price
            FROM {TABLE}
            WHERE stock_code LIKE '%%.SH' OR stock_code LIKE '%%.SZ'
        ) AS calc ON t.id = calc.id
        SET
            t.amplitude      = IF(calc.prev_close > 0,
                                  ROUND((calc.high_price - calc.low_price) / calc.prev_close * 100, 2), 0),
            t.change_percent = IF(calc.prev_close > 0,
                                  ROUND((calc.close_price - calc.prev_close) / calc.prev_close * 100, 2), 0),
            t.change_amount  = IF(calc.prev_close > 0,
                                  ROUND(calc.close_price - calc.prev_close, 2), 0)
        WHERE t.amplitude = 0 AND t.change_percent = 0 AND t.change_amount = 0
          AND NOT (t.open_price = 0 AND t.close_price = 0 AND t.high_price = 0 AND t.low_price = 0)
          AND NOT (t.open_price = t.close_price AND t.close_price = t.high_price AND t.high_price = t.low_price)
          AND (t.stock_code LIKE '%%.SH' OR t.stock_code LIKE '%%.SZ')
          AND calc.prev_close IS NOT NULL AND calc.prev_close > 0
    """)
    updated = cur.rowcount
    conn.commit()
    elapsed = time.time() - t0
    print(f"修复完成: 更新 {updated} 行, 耗时 {elapsed:.1f}s")

    # 3. 验证
    cur.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE amplitude = 0 AND change_percent = 0 AND change_amount = 0
          AND NOT (open_price = 0 AND close_price = 0 AND high_price = 0 AND low_price = 0)
          AND NOT (open_price = close_price AND close_price = high_price AND high_price = low_price)
          AND (stock_code LIKE '%%.SH' OR stock_code LIKE '%%.SZ')
    """)
    remaining = cur.fetchone()[0]
    print(f"[验证] SH/SZ 剩余异常: {remaining}")

    # 4. 验证 000132.SH
    cur.execute(f"""
        SELECT `date`, open_price, close_price, high_price, low_price,
               amplitude, change_percent, change_amount
        FROM {TABLE}
        WHERE stock_code = '000132.SH' AND `date` = '2026-03-10'
    """)
    row = cur.fetchone()
    if row:
        d, op, cp, hp, lp, amp, chg_pct, chg_amt = row
        print(f"\n[000132.SH 2026-03-10]")
        print(f"  O={op}, C={cp}, H={hp}, L={lp}")
        print(f"  amplitude={amp}, change_percent={chg_pct}, change_amount={chg_amt}")
        ok = not (amp == 0 and chg_pct == 0 and chg_amt == 0)
        print(f"  {'✅ 已修复' if ok else '❌ 仍未修复'}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
