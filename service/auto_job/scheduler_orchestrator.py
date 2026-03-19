"""
调度编排模块

- 提供全局互斥锁，确保所有数据拉取调度器串行执行（不并行）
- 提供各调度器的完成事件，供数据异常检测等下游任务等待
- 手动触发的任务也需要获取锁，保证不与自动调度冲突
- 通过 system.properties 中的 run_auto_job 控制是否启动自动调度
"""
import asyncio
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_project_root = Path(__file__).parent.parent.parent


def is_auto_job_enabled() -> bool:
    """读取 system.properties 中的 run_auto_job 参数。
    仅当 run_auto_job=1 时返回 True，其他值或文件不存在均返回 False。
    """
    props_file = _project_root / "system.properties"
    try:
        for line in props_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() == "run_auto_job":
                return value.strip() == "1"
    except FileNotFoundError:
        logger.warning("[调度编排] system.properties 不存在，自动调度已禁用")
    except Exception as e:
        logger.warning("[调度编排] 读取 system.properties 异常: %s，自动调度已禁用", e)
    return False

# ─────────── 全局互斥锁：同一时刻只允许一个自动调度任务执行 ───────────
scheduler_lock = asyncio.Lock()

# ─────────── 手动触发信号量：最多允许2个手动任务并行执行 ───────────
manual_semaphore = asyncio.Semaphore(2)

# ─────────── 各数据拉取任务的完成事件 ───────────
# 数据异常检测需要等待所有这些事件都被 set 后才执行
kline_done_event = asyncio.Event()
price_done_event = asyncio.Event()
market_data_done_event = asyncio.Event()
us_market_done_event = asyncio.Event()
fund_flow_done_event = asyncio.Event()
concept_strength_done_event = asyncio.Event()
weekly_prediction_done_event = asyncio.Event()
monthly_prediction_done_event = asyncio.Event()

# 所有数据任务完成事件列表（db_check 需要等待全部）
_all_data_done_events = [
    kline_done_event,
    price_done_event,
    market_data_done_event,
    us_market_done_event,
    fund_flow_done_event,
    concept_strength_done_event,
    weekly_prediction_done_event,
    monthly_prediction_done_event,
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
