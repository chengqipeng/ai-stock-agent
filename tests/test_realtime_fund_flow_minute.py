"""测试当日分时主力资金流向"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils.stock_info_utils import get_stock_info_by_name
from service.eastmoney.stock_info.stock_realtime_fund_flow_minute import (
    get_realtime_fund_flow_minute,
    get_realtime_fund_flow_minute_cn,
    analyze_minute_fund_flow,
)


async def main():
    # 测试中轨反弹和下轨反弹的5只股票
    test_stocks = ["闰土股份", "晓鸣股份", "金徽股份", "天安新材", "赛摩智能"]

    for name in test_stocks:
        stock_info = get_stock_info_by_name(name)
        if not stock_info:
            print(f"[{name}] 未找到股票信息")
            continue

        print(f"\n{'='*60}")
        print(f"{stock_info.stock_name}({stock_info.stock_code_normalize})")
        print(f"{'='*60}")

        rows = await get_realtime_fund_flow_minute(stock_info)
        if not rows:
            print("  无数据（可能非交易时间）")
            continue

        # 最近5条
        print(f"  共{len(rows)}条分时数据，最近5条：")
        for r in rows[-5:]:
            print(f"    {r['time']}  主力:{r['main_net_str']}  超大单:{r['super_net_str']}  大单:{r['big_net_str']}")

        # 行为分析
        analysis = analyze_minute_fund_flow(rows)
        print(f"\n  主力行为分析：")
        print(f"    {json.dumps(analysis, ensure_ascii=False, indent=4)}")


if __name__ == "__main__":
    asyncio.run(main())
