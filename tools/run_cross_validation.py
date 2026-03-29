"""
手动运行数据交叉验证，直接输出结果到控制台。

用法: .venv/bin/python tools/run_cross_validation.py
"""
import asyncio
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def main():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from dao.cross_validation_dao import create_cross_validation_table
    from service.auto_job.cross_validation_scheduler import (
        _sample_stocks, _validate_kline, _validate_order_book,
        _validate_time_data, _validate_highest_lowest,
        _validate_finance, _validate_fund_flow,
    )

    _CST = ZoneInfo("Asia/Shanghai")
    run_date = datetime.now(_CST).date().isoformat()

    print(f"\n{'='*70}")
    print(f"  数据交叉验证  {run_date}")
    print(f"{'='*70}\n")

    # 建表
    try:
        create_cross_validation_table()
    except Exception as e:
        logger.warning("建表异常（可能已存在）: %s", e)

    # 抽样
    stocks = _sample_stocks(10)
    print(f"抽样 {len(stocks)} 只股票: {', '.join(s['name'] for s in stocks)}\n")

    categories = [
        ("日K线(30天)", "kline", _validate_kline),
        ("盘口数据(当天)", "order_book", _validate_order_book),
        ("分时数据(当天)", "time_data", _validate_time_data),
        ("最高最低价", "price", _validate_highest_lowest),
        ("财报数据", "finance", _validate_finance),
        ("资金流向(30天)", "fund_flow", _validate_fund_flow),
    ]

    all_total = 0
    all_match = 0

    for label, key, fn in categories:
        print(f"── {label} ──")
        try:
            result = await fn(stocks, run_date)
            total = result.get("total", 0)
            match = result.get("match", 0)
            mismatch = result.get("mismatch", 0)
            missing = result.get("missing", 0)
            rate = result.get("match_rate", 0)
            all_total += total
            all_match += match

            if rate >= 95:
                icon = "✅"
            elif rate >= 80:
                icon = "⚠️"
            else:
                icon = "❌"

            print(f"  {icon} 匹配率: {rate}%  (匹配{match} 不匹配{mismatch} 缺失{missing} 共{total}项)")
        except Exception as e:
            print(f"  ❌ 异常: {e}")
            import traceback
            traceback.print_exc()
        print()

    overall = round(all_match / all_total * 100, 2) if all_total > 0 else 0
    print(f"{'='*70}")
    print(f"  总匹配率: {overall}%  ({all_match}/{all_total})")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    asyncio.run(main())
