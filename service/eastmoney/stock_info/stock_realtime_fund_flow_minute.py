"""
当日分时主力资金流向模块

通过东方财富 push2his API 获取个股当日分钟级别的主力资金流入流出数据。
接口：push2his.eastmoney.com/api/qt/stock/fflow/kline/get
参数 klt=1 表示1分钟级别。

数据包含：超大单、大单、中单、小单的净流入，以及主力（超大单+大单）净流入。
"""

import asyncio
import logging
from datetime import datetime

from common.http.http_utils import fetch_eastmoney_api, EASTMONEY_PUSH2HIS_API_URL
from common.utils.amount_utils import convert_amount_unit
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name

logger = logging.getLogger(__name__)


async def get_realtime_fund_flow_minute(stock_info: StockInfo) -> list[dict]:
    """
    获取当日分时主力资金流向（1分钟级别）。

    Returns:
        list[dict]: 每条记录包含：
            time, main_net, main_net_str, super_net, super_net_str,
            big_net, big_net_str, mid_net, mid_net_str, small_net, small_net_str
    """
    url = f"{EASTMONEY_PUSH2HIS_API_URL}/stock/fflow/kline/get"
    params = {
        "lmt": "0",
        "klt": "1",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "secid": stock_info.secid,
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")

    klines = (data.get("data") or {}).get("klines") or []
    if not klines:
        logger.warning("[%s] 未获取到分时资金流向数据", stock_info.stock_code)
        return []

    # 字段结构（6字段）：
    # [0] 时间  [1] 主力净流入(累计)  [2] 小单净流入(累计)
    # [3] 中单净流入(累计)  [4] 大单净流入(累计)  [5] 超大单净流入(累计)
    result = []
    for line in klines:
        parts = line.split(",")
        if len(parts) < 6:
            continue

        def _f(idx):
            v = parts[idx]
            return float(v) if v and v != "-" else 0.0

        main_net = _f(1)
        small_net = _f(2)
        mid_net = _f(3)
        big_net = _f(4)
        super_net = _f(5)

        result.append({
            "time": parts[0],
            "main_net": main_net,
            "main_net_str": convert_amount_unit(main_net),
            "super_net": super_net,
            "super_net_str": convert_amount_unit(super_net),
            "big_net": big_net,
            "big_net_str": convert_amount_unit(big_net),
            "mid_net": mid_net,
            "mid_net_str": convert_amount_unit(mid_net),
            "small_net": small_net,
            "small_net_str": convert_amount_unit(small_net),
        })

    return result


async def get_realtime_fund_flow_minute_cn(stock_info: StockInfo) -> list[dict]:
    """获取当日分时主力资金流向（中文key版本）"""
    rows = await get_realtime_fund_flow_minute(stock_info)
    return [{
        "时间": r["time"],
        "主力净流入": r["main_net_str"],
        "超大单净流入": r["super_net_str"],
        "大单净流入": r["big_net_str"],
        "中单净流入": r["mid_net_str"],
        "小单净流入": r["small_net_str"],
    } for r in rows]


def analyze_minute_fund_flow(rows: list[dict]) -> dict:
    """
    分析分时资金流向，提取主力行为特征。

    返回：
        - 主力累计净流入
        - 主力买入/卖出阶段（连续流入/流出区间）
        - 最大单笔分钟净流入/流出
        - 尾盘资金动向（最后30分钟）
        - 主力流入占比趋势
    """
    if not rows:
        return {"状态": "无分时资金数据"}

    total_main = rows[-1]["main_net"] if rows else 0
    total_super = rows[-1]["super_net"] if rows else 0
    total_big = rows[-1]["big_net"] if rows else 0

    # 计算每分钟增量
    increments = []
    for i, r in enumerate(rows):
        if i == 0:
            inc = r["main_net"]
        else:
            inc = r["main_net"] - rows[i - 1]["main_net"]
        increments.append(inc)

    # 最大单分钟流入/流出
    max_inflow_min = max(increments) if increments else 0
    max_outflow_min = min(increments) if increments else 0

    # 连续流入/流出段统计
    inflow_streaks = []
    outflow_streaks = []
    streak_start = 0
    streak_dir = 0  # 1=流入, -1=流出
    for i, inc in enumerate(increments):
        cur_dir = 1 if inc > 0 else (-1 if inc < 0 else 0)
        if cur_dir != streak_dir:
            if streak_dir == 1 and (i - streak_start) >= 3:
                inflow_streaks.append((rows[streak_start]["time"], rows[i - 1]["time"], i - streak_start))
            elif streak_dir == -1 and (i - streak_start) >= 3:
                outflow_streaks.append((rows[streak_start]["time"], rows[i - 1]["time"], i - streak_start))
            streak_start = i
            streak_dir = cur_dir

    # 处理最后一段
    if streak_dir == 1 and (len(increments) - streak_start) >= 3:
        inflow_streaks.append((rows[streak_start]["time"], rows[-1]["time"], len(increments) - streak_start))
    elif streak_dir == -1 and (len(increments) - streak_start) >= 3:
        outflow_streaks.append((rows[streak_start]["time"], rows[-1]["time"], len(increments) - streak_start))

    # 尾盘分析（最后30分钟）
    tail_rows = rows[-30:] if len(rows) >= 30 else rows
    if len(tail_rows) >= 2:
        tail_main_change = tail_rows[-1]["main_net"] - tail_rows[0]["main_net"]
    else:
        tail_main_change = 0

    if tail_main_change > 0:
        tail_desc = f"尾盘主力净流入{convert_amount_unit(tail_main_change)}，资金抢筹"
    elif tail_main_change < 0:
        tail_desc = f"尾盘主力净流出{convert_amount_unit(abs(tail_main_change))}，资金撤退"
    else:
        tail_desc = "尾盘资金流向平稳"

    # 整体行为判断
    if total_main > 0 and total_super > 0:
        behavior = "超大单+大单双向流入，主力积极做多"
    elif total_main > 0 and total_super <= 0:
        behavior = "大单流入为主，超大单观望或小幅流出"
    elif total_main < 0 and total_super < 0:
        behavior = "超大单+大单双向流出，主力撤退"
    elif total_main < 0 and total_super >= 0:
        behavior = "大单流出为主，超大单有托底动作"
    else:
        behavior = "主力资金流向不明显"

    return {
        "主力累计净流入": convert_amount_unit(total_main),
        "超大单累计净流入": convert_amount_unit(total_super),
        "大单累计净流入": convert_amount_unit(total_big),
        "主力行为判断": behavior,
        "最大单分钟流入": convert_amount_unit(max_inflow_min),
        "最大单分钟流出": convert_amount_unit(abs(max_outflow_min)),
        "连续流入段": [f"{s[0]}~{s[1]}({s[2]}分钟)" for s in inflow_streaks[-5:]],
        "连续流出段": [f"{s[0]}~{s[1]}({s[2]}分钟)" for s in outflow_streaks[-5:]],
        "尾盘动向": tail_desc,
        "数据点数": len(rows),
    }


if __name__ == "__main__":
    import json

    async def main():
        stock_info = get_stock_info_by_name("闰土股份")
        if not stock_info:
            print("未找到股票")
            return

        print(f"=== {stock_info.stock_name}({stock_info.stock_code_normalize}) 当日分时主力资金 ===\n")

        rows = await get_realtime_fund_flow_minute(stock_info)
        if not rows:
            print("无数据（可能非交易时间）")
            return

        # 打印最近10条
        print("最近10条分时数据：")
        for r in rows[-10:]:
            print(f"  {r['time']}  主力:{r['main_net_str']}  超大单:{r['super_net_str']}  "
                  f"大单:{r['big_net_str']}  中单:{r['mid_net_str']}  小单:{r['small_net_str']}")

        print("\n--- 资金行为分析 ---")
        analysis = analyze_minute_fund_flow(rows)
        print(json.dumps(analysis, ensure_ascii=False, indent=2))

    asyncio.run(main())
