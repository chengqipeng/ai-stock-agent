"""
新闻正文异步拉取 Worker

独立于新闻调度器运行，负责拉取 content_status='pending' 的新闻正文内容。
- 项目启动时自动启动，持续扫描待处理记录
- 开启 2 个并发协程同时拉取
- 每条记录拉取后更新 content + content_status
- 支持 PDF 和网页两种内容类型
"""
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from curl_cffi.requests import AsyncSession

from dao.stock_news_dao import (
    create_news_table,
    get_pending_content_records,
    update_content_status,
    mark_as_fetching,
    get_content_status_stats,
    reset_stale_fetching,
)
from service.jqka10.stock_news_10jqka import _fetch_article_content, IMPERSONATE
from service.auto_job.kline_data_scheduler import app_ready

_CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

# Worker 并发数
_WORKER_COUNT = 2

# 全局状态
_worker_status = {
    "running": False,
    "total_processed": 0,
    "total_success": 0,
    "total_failed": 0,
    "pending_count": 0,
}


def get_content_worker_status() -> dict:
    return dict(_worker_status)


async def _process_record(record: dict, session: AsyncSession) -> bool:
    """处理单条记录：拉取正文并更新数据库。返回是否成功。"""
    record_id = record["id"]
    url = record.get("url", "")

    if not url:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, update_content_status, record_id, "skip")
        return False

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            content = await _fetch_article_content(url, session)
            loop = asyncio.get_event_loop()
            if content and len(content.strip()) > 10:
                await loop.run_in_executor(None, update_content_status, record_id, "done", content)
                return True
            else:
                await loop.run_in_executor(None, update_content_status, record_id, "skip")
                return False
        except Exception as e:
            err_str = str(e)
            is_cf = any(f'52{c}' in err_str for c in '123')
            if is_cf and attempt < max_retries:
                wait = 3 * attempt
                logger.debug("[内容Worker] id=%d Cloudflare 521，%d秒后重试(%d/%d)",
                             record_id, wait, attempt, max_retries)
                await asyncio.sleep(wait)
                continue
            logger.debug("[内容Worker] id=%d url=%s 拉取失败: %s", record_id, url, e)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, update_content_status, record_id, "failed")
            return False
    return False


async def _worker_loop(worker_id: int, queue: asyncio.Queue, session: AsyncSession):
    """单个 worker 协程：从队列取任务并处理"""
    while True:
        record = await queue.get()
        if record is None:
            queue.task_done()
            break
        try:
            ok = await _process_record(record, session)
            if ok:
                _worker_status["total_success"] += 1
            else:
                _worker_status["total_failed"] += 1
            _worker_status["total_processed"] += 1
        except Exception as e:
            logger.error("[内容Worker-%d] 处理异常: %s", worker_id, e)
            _worker_status["total_failed"] += 1
            _worker_status["total_processed"] += 1
        finally:
            queue.task_done()
            # 请求间隔，避免被封
            await asyncio.sleep(1.0)


async def _run_batch(batch: list[dict], session: AsyncSession):
    """处理一批记录：先标记 fetching，然后用多 worker 并发拉取"""
    record_ids = [r["id"] for r in batch]
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, mark_as_fetching, record_ids)

    queue: asyncio.Queue = asyncio.Queue()
    for record in batch:
        await queue.put(record)

    # 放入终止信号
    for _ in range(_WORKER_COUNT):
        await queue.put(None)

    workers = [
        asyncio.create_task(_worker_loop(i, queue, session))
        for i in range(_WORKER_COUNT)
    ]
    await asyncio.gather(*workers)


async def _content_worker_main():
    """主循环：持续扫描 pending 记录并分批处理"""
    await app_ready.wait()
    logger.info("[内容Worker] 应用就绪，内容拉取 Worker 启动（%d 并发）", _WORKER_COUNT)

    _worker_status["running"] = True

    # 让出控制权，确保其他协程（HTTP服务等）能正常启动
    await asyncio.sleep(1)

    loop = asyncio.get_event_loop()

    # 同步 DB 操作放到线程池，避免阻塞事件循环
    await loop.run_in_executor(None, create_news_table)
    await loop.run_in_executor(None, lambda: reset_stale_fetching(timeout_minutes=30))

    while True:
        try:
            # 同步 DB 查询放到线程池
            batch = await loop.run_in_executor(None, lambda: get_pending_content_records(limit=20))
            if not batch:
                _worker_status["pending_count"] = 0
                logger.debug("[内容Worker] 无待处理记录，60秒后重新检查")
                await asyncio.sleep(60)
                await loop.run_in_executor(None, lambda: reset_stale_fetching(timeout_minutes=30))
                continue

            _worker_status["pending_count"] = len(batch)
            logger.info("[内容Worker] 发现 %d 条待拉取记录", len(batch))

            async with AsyncSession(impersonate=IMPERSONATE) as session:
                await _run_batch(batch, session)

            # 批次间短暂休眠
            await asyncio.sleep(2)

        except Exception as e:
            logger.error("[内容Worker] 主循环异常: %s", e, exc_info=True)
            await asyncio.sleep(30)


async def start_news_content_worker():
    """启动新闻正文拉取 Worker（由 lifespan 调用）"""
    asyncio.create_task(_content_worker_main())
