"""检查同花顺历史资金流返回多少天数据"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
logging.basicConfig(level=logging.WARNING)

from common.utils.stock_info_utils import get_stock_info_by_name
from service.jqka10.stock_history_fund_flow_10jqka import get_fund_flow_history

async def main():
    stock_info = get_stock_info_by_name("贵州茅台")
    rows = await get_fund_flow_history(stock_info)
    print(f"返回 {len(rows)} 条记录")
    if rows:
        print(f"最新: {rows[0].get('date')} close={rows[0].get('close_price')}")
        print(f"最早: {rows[-1].get('date')} close={rows[-1].get('close_price')}")
        print(f"字段: {list(rows[0].keys())}")
        print(f"\n前3条:")
        for r in rows[:3]:
            print(f"  {r['date']} big_net={r.get('big_net')} main_net_5day={r.get('main_net_5day')}")

asyncio.run(main())
