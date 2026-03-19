"""测试同花顺资金流向数据接口"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.jqka10.stock_fund_flow_10jqka import (
    get_industry_fund_flow,
    get_concept_fund_flow,
    get_stock_fund_flow,
    get_big_order_tracking,
    get_industry_fund_flow_all,
    to_cn_rows,
    TYPE_LABEL,
)


async def test_single_pages():
    """测试四个维度的单页数据"""
    print("=" * 70)
    print("  测试单页数据获取")
    print("=" * 70)

    for label, func in [
        ("行业资金流", get_industry_fund_flow),
        ("概念资金流", get_concept_fund_flow),
        ("个股资金流", get_stock_fund_flow),
        ("大单追踪", get_big_order_tracking),
    ]:
        result = await func()
        print(f"\n[{label}] 总页数={result['total_pages']}, 本页={len(result['data'])}条")
        if result["data"]:
            print(f"  首条: {json.dumps(result['data'][0], ensure_ascii=False)}")


async def test_all_pages():
    """测试行业资金流全部页获取"""
    print("\n" + "=" * 70)
    print("  测试行业资金流全部页（限2页）")
    print("=" * 70)

    rows = await get_industry_fund_flow_all(max_pages=2)
    print(f"  共获取 {len(rows)} 条记录")
    if rows:
        print(f"  首条: {json.dumps(rows[0], ensure_ascii=False)}")
        print(f"  末条: {json.dumps(rows[-1], ensure_ascii=False)}")

    # 测试中文转换
    cn_rows = to_cn_rows(rows[:2], "hyzjl")
    print(f"\n  中文格式:")
    for r in cn_rows:
        print(f"    {json.dumps(r, ensure_ascii=False)}")


async def main():
    await test_single_pages()
    await test_all_pages()


if __name__ == "__main__":
    asyncio.run(main())
