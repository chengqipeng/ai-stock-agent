"""
测试美股半导体龙头个股K线数据抓取 + 入库

测试内容：
1. 单只股票K线抓取（NVDA）
2. 费城半导体指数K线抓取（SOX）
3. 批量抓取所有美股半导体龙头K线并写入数据库
4. 实时行情批量获取

Usage:
    python tests/test_us_stock_kline.py
    python tests/test_us_stock_kline.py --save   # 抓取并写入数据库
"""
import sys
import os
import json
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from service.eastmoney.indices.us_stock_kline import (
    get_us_stock_day_kline,
    get_us_stock_day_kline_batch,
    get_sox_index_day_kline,
    get_us_stock_realtime_batch,
    US_SEMI_STOCK_MAP,
    US_SEMI_INDEX_MAP,
)


async def test_single_stock():
    """测试单只股票K线"""
    print("=" * 60)
    print("1. NVDA 日K线（最近5条）")
    print("=" * 60)
    klines = await get_us_stock_day_kline("NVDA", limit=5)
    for k in klines:
        print(f"  {k['日期']}  收盘:{k['收盘价']:>10}  涨跌:{k['涨跌幅(%)']:>6}%  量:{k['成交量']}")
    print(f"  共 {len(klines)} 条\n")
    return len(klines) > 0


async def test_sox_index():
    """测试半导体ETF(SOXX)"""
    print("=" * 60)
    print("2. SOXX 半导体ETF（最近5条）")
    print("=" * 60)
    klines = await get_sox_index_day_kline(limit=5)
    for k in klines:
        print(f"  {k['日期']}  收盘:{k['收盘价']:>10}  涨跌:{k['涨跌幅(%)']:>6}%")
    print(f"  共 {len(klines)} 条\n")
    return len(klines) > 0


async def test_batch_kline():
    """测试批量K线抓取"""
    print("=" * 60)
    print("3. 批量抓取所有美股半导体龙头（最近3条）")
    print("=" * 60)
    batch = await get_us_stock_day_kline_batch(limit=3, delay=0.2)
    for code, data in batch.items():
        info = US_SEMI_STOCK_MAP[code]
        latest = data[0] if data else {}
        print(f"  {code:6s} {info['name']:12s} [{info['sector']:6s}] | "
              f"收盘: {latest.get('收盘价', '-'):>10} | "
              f"涨跌: {latest.get('涨跌幅(%)', '-'):>6}%  | "
              f"K线数: {len(data)}")
    print(f"  共 {len(batch)} 只股票\n")
    return all(len(v) > 0 for v in batch.values())


async def test_realtime():
    """测试实时行情"""
    print("=" * 60)
    print("4. 批量实时行情")
    print("=" * 60)
    items = await get_us_stock_realtime_batch()
    for item in items:
        print(f"  {item['代码']:6s} {item['名称']:12s} [{item['细分领域']:6s}] | "
              f"最新: {item.get('最新价', '-'):>10} | "
              f"涨跌: {item.get('涨跌幅(%)', '-'):>6}%")
    print(f"  共 {len(items)} 只\n")
    return len(items) > 0


async def test_save_to_db():
    """抓取全部K线并写入数据库"""
    from dao import get_connection
    from dao.us_market_dao import create_us_market_tables, batch_upsert_stock_kline, batch_upsert_index_kline

    print("=" * 60)
    print("5. 抓取并写入数据库（120条/只）")
    print("=" * 60)

    # 建表
    conn = get_connection()
    cursor = conn.cursor()
    try:
        create_us_market_tables(cursor)
        conn.commit()
        print("  表创建/检查完成 ✓")
    finally:
        cursor.close()
        conn.close()

    # 抓取个股K线
    batch = await get_us_stock_day_kline_batch(limit=120, delay=0.3)

    # 写入
    conn = get_connection()
    cursor = conn.cursor()
    total_rows = 0
    try:
        for code, klines in batch.items():
            if not klines:
                continue
            info = US_SEMI_STOCK_MAP[code]
            batch_upsert_stock_kline(cursor, code, info["name"], info["sector"], klines)
            total_rows += len(klines)
            print(f"  {code:6s} {info['name']:12s} 写入 {len(klines)} 条")

        # 抓取并写入 SOXX（半导体ETF，跟踪费城半导体指数）
        sox_klines = await get_sox_index_day_kline(limit=120)
        if sox_klines:
            batch_upsert_index_kline(cursor, "SOXX", sox_klines)
            total_rows += len(sox_klines)
            print(f"  {'SOXX':6s} {'半导体ETF':12s} 写入 {len(sox_klines)} 条")

        conn.commit()
        print(f"\n  总计写入 {total_rows} 条记录 ✓")
    except Exception as e:
        conn.rollback()
        print(f"\n  写入失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


async def main():
    save_mode = "--save" in sys.argv

    ok1 = await test_single_stock()
    ok2 = await test_sox_index()
    ok3 = await test_batch_kline()
    ok4 = await test_realtime()

    if save_mode:
        await test_save_to_db()

    print("=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    print(f"  单只K线:   {'✓' if ok1 else '✗'}")
    print(f"  SOX指数:   {'✓' if ok2 else '✗'}")
    print(f"  批量K线:   {'✓' if ok3 else '✗'}")
    print(f"  实时行情:  {'✓' if ok4 else '✗'}")
    if save_mode:
        print(f"  数据库写入: ✓")
    else:
        print(f"  数据库写入: 跳过（加 --save 参数启用）")


if __name__ == "__main__":
    asyncio.run(main())
