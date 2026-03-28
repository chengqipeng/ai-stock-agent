"""
验证大单追踪功能：
1. 从 https://data.10jqka.com.cn/funds/ddzz/ 拉取大单追踪数据
2. 写入数据库
3. 从数据库查询验证

选取10只股票进行验证
"""
import asyncio
import logging
import json
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# 10只验证股票（6位代码）
TEST_STOCKS = [
    "600519",  # 贵州茅台
    "000858",  # 五粮液
    "002371",  # 北方华创
    "300750",  # 宁德时代
    "601318",  # 中国平安
    "000001",  # 平安银行
    "600036",  # 招商银行
    "002475",  # 立讯精密
    "300059",  # 东方财富
    "601012",  # 隆基绿能
]


async def main():
    from dao.stock_big_order_dao import (
        create_big_order_table, batch_insert_big_orders,
        get_big_orders_by_stock, has_big_orders, get_big_orders_by_date,
    )
    from service.jqka10.stock_fund_flow_10jqka import fetch_fund_flow_all_pages

    today_str = date.today().isoformat()

    # 1. 建表
    logger.info("=== 步骤1: 创建 stock_big_order 表 ===")
    create_big_order_table()
    logger.info("表创建成功 ✓")

    # 2. 拉取大单追踪数据（最多5页）
    logger.info("=== 步骤2: 从同花顺拉取大单追踪数据 ===")
    rows = await fetch_fund_flow_all_pages("ddzz", max_pages=5)
    logger.info("拉取到 %d 条大单追踪记录", len(rows))

    if rows:
        logger.info("样例数据（前3条）:")
        for r in rows[:3]:
            logger.info("  %s", json.dumps(r, ensure_ascii=False))

    # 3. 写入数据库
    logger.info("=== 步骤3: 写入数据库 ===")
    if not has_big_orders(today_str):
        count = batch_insert_big_orders(today_str, rows)
        logger.info("写入 %d 条记录 ✓", count)
    else:
        logger.info("今日数据已存在，跳过写入")

    # 4. 验证10只股票的查询
    logger.info("=== 步骤4: 验证10只股票查询 ===")
    found_count = 0
    for code in TEST_STOCKS:
        orders = get_big_orders_by_stock(code, limit=10)
        if orders:
            found_count += 1
            logger.info("  ✓ %s: 查到 %d 条大单记录", code, len(orders))
            for o in orders[:2]:
                logger.info("    %s %s %s %s %s手 %s万 %s",
                            o["trade_date"], o["time"], o["stock_name"],
                            o["price"], o["volume"], o["amount"], o["direction"])
        else:
            logger.info("  - %s: 无大单记录（该股今日可能无大单交易）", code)

    # 5. 按日期查询验证
    logger.info("=== 步骤5: 按日期查询验证 ===")
    date_orders = get_big_orders_by_date(today_str, limit=20)
    logger.info("今日(%s)共 %d 条大单记录（前20条）", today_str, len(date_orders))

    # 6. 汇总
    logger.info("=" * 60)
    logger.info("验证完成:")
    logger.info("  拉取总数: %d 条", len(rows))
    logger.info("  10只股票中有大单记录: %d 只", found_count)
    logger.info("  今日大单总数: %d 条", len(date_orders))
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
