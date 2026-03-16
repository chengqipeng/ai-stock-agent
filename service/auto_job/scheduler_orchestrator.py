"""
调度编排模块

- 提供全局互斥锁，确保所有数据拉取调度器串行执行（不并行）
- 提供各调度器的完成事件，供数据异常检测等下游任务等待
- 手动触发的任务也需要获取锁，保证不与自动调度冲突
"""
import asyncio
import logging

logger = logging.getLogger(__name__)

# ─────────── 全局互斥锁：同一时刻只允许一个调度任务执行 ───────────
scheduler_lock = asyncio.Lock()

# ─────────── 各数据拉取任务的完成事件 ───────────
# 数据异常检测需要等待所有这些事件都被 set 后才执行
kline_done_event = asyncio.Event()
price_done_event = asyncio.Event()
market_data_done_event = asyncio.Event()
us_market_done_event = asyncio.Event()
fund_flow_done_event = asyncio.Event()
concept_strength_done_event = asyncio.Event()
weekly_prediction_done_event = asyncio.Event()

# 所有数据任务完成事件列表（db_check 需要等待全部）
_all_data_done_events = [
    kline_done_event,
    price_done_event,
    market_data_done_event,
    us_market_done_event,
    fund_flow_done_event,
    concept_strength_done_event,
    weekly_prediction_done_event,
]


def reset_all_done_events():
    """重置所有完成事件（每天新一轮调度前调用）"""
    for evt in _all_data_done_events:
        evt.clear()


async def wait_all_data_jobs_done():
    """等待所有数据拉取任务完成（供数据异常检测使用）"""
    logger.info("[调度编排] 等待所有数据任务完成...")
    for evt in _all_data_done_events:
        await evt.wait()
    logger.info("[调度编排] 所有数据任务已完成，可以开始数据异常检测")
