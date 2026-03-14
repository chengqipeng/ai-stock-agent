"""
诊断概念强弱势全部失败的原因
"""
import sys
sys.path.insert(0, ".")

from dao import get_connection

conn = get_connection(use_dict_cursor=True)
cur = conn.cursor()

try:
    # 1. 板块数量
    cur.execute("SELECT COUNT(*) as cnt FROM stock_concept_board")
    print(f"1. stock_concept_board 总数: {cur.fetchone()['cnt']}")

    # 2. 板块K线数据
    cur.execute("SELECT COUNT(*) as cnt FROM concept_board_kline")
    print(f"2. concept_board_kline 总记录数: {cur.fetchone()['cnt']}")

    cur.execute("SELECT COUNT(DISTINCT board_code) as cnt FROM concept_board_kline")
    print(f"   有K线的板块数: {cur.fetchone()['cnt']}")

    cur.execute("SELECT `date` FROM concept_board_kline ORDER BY `date` DESC LIMIT 3")
    rows = cur.fetchall()
    print(f"   最新3条日期: {[r['date'] for r in rows]}")

    cur.execute("SELECT `date` FROM concept_board_kline ORDER BY `date` ASC LIMIT 3")
    rows = cur.fetchall()
    print(f"   最早3条日期: {[r['date'] for r in rows]}")

    # 3. 成分股数据
    cur.execute("SELECT COUNT(*) as cnt FROM stock_concept_board_stock")
    print(f"3. stock_concept_board_stock 总记录数: {cur.fetchone()['cnt']}")

    cur.execute("SELECT COUNT(DISTINCT board_code) as cnt FROM stock_concept_board_stock")
    print(f"   有成分股的板块数: {cur.fetchone()['cnt']}")

    # 4. 个股K线数据
    cur.execute("SELECT COUNT(*) as cnt FROM stock_kline")
    print(f"4. stock_kline 总记录数: {cur.fetchone()['cnt']}")

    cur.execute("SELECT `date` FROM stock_kline ORDER BY `date` DESC LIMIT 3")
    rows = cur.fetchall()
    print(f"   最新3条日期: {[r['date'] for r in rows]}")

    # 5. 抽样一个板块做完整诊断
    cur.execute("SELECT board_code, board_name FROM stock_concept_board LIMIT 1")
    sample = cur.fetchone()
    if sample:
        bc = sample['board_code']
        bn = sample['board_name']
        print(f"\n5. 抽样板块: {bc} ({bn})")

        cur.execute(
            "SELECT COUNT(*) as cnt FROM concept_board_kline WHERE board_code = %s", (bc,)
        )
        print(f"   板块K线数: {cur.fetchone()['cnt']}")

        cur.execute(
            "SELECT `date`, change_percent FROM concept_board_kline "
            "WHERE board_code = %s ORDER BY `date` DESC LIMIT 5", (bc,)
        )
        rows = cur.fetchall()
        print(f"   板块K线样本: {rows}")

        cur.execute(
            "SELECT COUNT(*) as cnt FROM stock_concept_board_stock WHERE board_code = %s", (bc,)
        )
        member_cnt = cur.fetchone()['cnt']
        print(f"   成分股数: {member_cnt}")

        if member_cnt > 0:
            cur.execute(
                "SELECT stock_code FROM stock_concept_board_stock WHERE board_code = %s LIMIT 3", (bc,)
            )
            sample_stocks = [r['stock_code'] for r in cur.fetchall()]
            print(f"   样本成分股: {sample_stocks}")

            for sc in sample_stocks:
                cur.execute(
                    "SELECT COUNT(*) as cnt FROM stock_kline WHERE stock_code = %s", (sc,)
                )
                cnt = cur.fetchone()['cnt']
                cur.execute(
                    "SELECT `date` FROM stock_kline WHERE stock_code = %s ORDER BY `date` DESC LIMIT 1", (sc,)
                )
                latest = cur.fetchone()
                print(f"   {sc}: K线数={cnt}, 最新日期={latest['date'] if latest else 'N/A'}")

    # 6. 直接调用 analyze_board_stock_strength 看返回
    print("\n6. 直接调用 analyze_board_stock_strength 测试:")
    from service.analysis.concept_stock_strength import analyze_board_stock_strength
    result = analyze_board_stock_strength(bc, days=60)
    print(f"   success: {result.get('success')}")
    print(f"   error: {result.get('error', 'N/A')}")
    if result.get('success'):
        print(f"   stocks count: {len(result.get('stocks', []))}")
        print(f"   total: {result.get('total')}")
    else:
        print(f"   完整返回: {result}")

finally:
    cur.close()
    conn.close()
