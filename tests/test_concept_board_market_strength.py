"""
概念板块大盘强弱势评分 - 验证测试

验证：
1. 单个板块评分计算
2. 批量计算并写入数据库
3. 排名查询
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from service.analysis.concept_board_market_strength import (
    compute_board_market_strength,
    compute_and_save_all_boards,
    get_all_board_strength_ranking,
)


def test_single_board():
    """测试单个板块评分"""
    print("\n" + "=" * 60)
    print("测试1: 单个板块评分计算")
    print("=" * 60)

    # 取一个有数据的板块
    from dao import get_connection
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT b.board_code, b.board_name "
            "FROM stock_concept_board b "
            "INNER JOIN concept_board_kline k ON b.board_code = k.board_code "
            "GROUP BY b.board_code, b.board_name "
            "HAVING COUNT(*) >= 30 "
            "ORDER BY COUNT(*) DESC LIMIT 5"
        )
        boards = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not boards:
        print("❌ 没有找到有足够K线数据的板块")
        return False

    success = True
    for board in boards:
        code = board["board_code"]
        name = board["board_name"]
        result = compute_board_market_strength(code, days=60)
        if result:
            ds = result["detail_scores"]
            print(f"\n  ✅ {code} {name}")
            print(f"     综合评分: {result['score']}/100")
            print(f"     板块涨幅: {result['board_return']:+.2f}%")
            print(f"     大盘涨幅: {result['market_return']:+.2f}%")
            print(f"     超额收益: {result['excess_return']:+.2f}%")
            print(f"     5日超额:  {result['excess_5d']:+.2f}%")
            print(f"     20日超额: {result['excess_20d']:+.2f}%")
            print(f"     胜率:     {result['win_rate']:.1%}")
            print(f"     交易日:   {result['trade_days']}天")
            print(f"     维度明细: 超额={ds['excess_return_score']:.1f}/30 "
                  f"短期={ds['short_momentum_score']:.1f}/25 "
                  f"中期={ds['mid_trend_score']:.1f}/20 "
                  f"胜率={ds['win_rate_score']:.1f}/15 "
                  f"回撤={ds['drawdown_score']:.1f}/10")
        else:
            print(f"\n  ❌ {code} {name} -> 计算失败")
            success = False

    return success


def test_batch_compute():
    """测试批量计算并写入数据库"""
    print("\n" + "=" * 60)
    print("测试2: 批量计算所有板块并写入数据库")
    print("=" * 60)

    result = compute_and_save_all_boards(days=60)
    print(f"\n  总板块数: {result['total']}")
    print(f"  成功计算: {result['success']}")
    print(f"  计算失败: {result['failed']}")

    if result["success"] > 0:
        print("  ✅ 批量计算成功")
        return True
    else:
        print("  ❌ 没有成功计算的板块")
        return False


def test_ranking():
    """测试排名查询"""
    print("\n" + "=" * 60)
    print("测试3: 板块强弱排名查询")
    print("=" * 60)

    ranking = get_all_board_strength_ranking(limit=20)
    if not ranking:
        print("  ❌ 排名为空")
        return False

    print(f"\n  共 {len(ranking)} 个板块有评分")
    print(f"\n  {'排名':>4} {'代码':>8} {'板块名称':<12} {'评分':>6} {'超额收益':>8} {'板块涨幅':>8} {'成分股':>6}")
    print("  " + "-" * 70)

    strong = 0
    weak = 0
    for i, r in enumerate(ranking):
        level = "强势" if r["score"] >= 70 else ("弱势" if r["score"] < 40 else "中性")
        if r["score"] >= 70:
            strong += 1
        elif r["score"] < 40:
            weak += 1
        print(f"  {i+1:>4} {r['board_code']:>8} {r['board_name']:<12} "
              f"{r['score']:>6.1f} {r['excess_return']:>+8.2f}% "
              f"{r['board_return']:>+8.2f}% {r['stock_count']:>6}")

    print(f"\n  强势(≥70): {strong}个, 弱势(<40): {len(ranking)-strong-(len(ranking)-strong-weak)}个")
    print("  ✅ 排名查询成功")
    return True


def test_db_columns():
    """验证数据库列已正确写入"""
    print("\n" + "=" * 60)
    print("测试4: 验证数据库写入")
    print("=" * 60)

    from dao import get_connection
    conn = get_connection(use_dict_cursor=True)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT board_code, board_name, market_strength_score, "
            "market_excess_return, market_board_return, market_strength_updated "
            "FROM stock_concept_board "
            "WHERE market_strength_score IS NOT NULL "
            "ORDER BY market_strength_score DESC LIMIT 5"
        )
        rows = cur.fetchall()
        if not rows:
            print("  ❌ 数据库中没有评分数据")
            return False

        print(f"\n  数据库中有评分的板块（前5）:")
        for r in rows:
            print(f"    {r['board_code']} {r['board_name']}: "
                  f"评分={r['market_strength_score']}, "
                  f"超额={r['market_excess_return']}, "
                  f"涨幅={r['market_board_return']}, "
                  f"更新={r['market_strength_updated']}")

        # 验证评分范围
        cur.execute(
            "SELECT COUNT(*) AS cnt, "
            "MIN(market_strength_score) AS min_score, "
            "MAX(market_strength_score) AS max_score, "
            "AVG(market_strength_score) AS avg_score "
            "FROM stock_concept_board "
            "WHERE market_strength_score IS NOT NULL"
        )
        stats = cur.fetchone()
        print(f"\n  统计: 共{stats['cnt']}个板块有评分")
        print(f"  最低分: {stats['min_score']:.1f}")
        print(f"  最高分: {stats['max_score']:.1f}")
        print(f"  平均分: {stats['avg_score']:.1f}")

        if 0 <= float(stats['min_score']) and float(stats['max_score']) <= 100:
            print("  ✅ 评分范围正确 [0, 100]")
            return True
        else:
            print("  ❌ 评分范围异常")
            return False
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    results = {}

    results["单板块评分"] = test_single_board()
    results["批量计算"] = test_batch_compute()
    results["排名查询"] = test_ranking()
    results["数据库验证"] = test_db_columns()

    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    all_pass = True
    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_pass = False

    print("\n" + ("🎉 全部通过" if all_pass else "⚠️ 存在失败项"))
