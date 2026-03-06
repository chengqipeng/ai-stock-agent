import asyncio
import logging
import re
from datetime import datetime
from pathlib import Path

from common.utils.stock_info_utils import get_stock_info_by_name
from dao.stock_highest_lowest_price_dao import save_price_record, get_today_processed_codes
from service.jqka10.stock_week_kline_data_10jqka import get_stock_week_kline_list_10jqka

logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent.parent
lock = asyncio.Lock()


async def _process_single_price(stock, counter):
    """处理单个股票的最高最低价数据"""
    try:
        stock_name = stock["name"]
        stock_info = get_stock_info_by_name(stock_name)
        kline_data = await get_stock_week_kline_list_10jqka(stock_info)

        if kline_data:
            highest_record = max(kline_data, key=lambda x: x["最高"])
            lowest_record = min(kline_data, key=lambda x: x["最低"])
            result = {
                "code": stock["code"],
                "name": stock_name,
                "highest_price": highest_record["最高"],
                "highest_date": highest_record["日期"],
                "lowest_price": lowest_record["最低"],
                "lowest_date": lowest_record["日期"],
                "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            async with lock:
                save_price_record(result)

            counter['success'] += 1
            logger.info("[最高最低价 总%d 成功%d 失败%d 当前:%s] 最高%s(%s) 最低%s(%s)",
                        counter['total'], counter['success'], counter['failed'], stock_name,
                        highest_record['最高'], highest_record['日期'],
                        lowest_record['最低'], lowest_record['日期'])
        else:
            counter['failed'] += 1
            logger.warning("[最高最低价 %s] 返回空数据", stock_name)
    except Exception as e:
        counter['failed'] += 1
        logger.error("[最高最低价 %s] 失败: %s", stock.get('name', ''), e)


def _load_stocks() -> list[dict]:
    """从 stock_score_list.md 加载股票列表"""
    score_list_path = project_root / "data_results/stock_to_score_list/stock_score_list.md"
    pattern = re.compile(r'^(.+?)\s+\(([^)]+)\)')
    all_stocks = []
    for line in score_list_path.read_text(encoding='utf-8').splitlines():
        m = pattern.match(line.strip())
        if m:
            all_stocks.append({'name': m.group(1), 'code': m.group(2)})
    return all_stocks


async def run_price_job(max_concurrent=5, counter=None):
    """独立运行最高最低价采集任务（供调度器调用）"""
    today = datetime.now().strftime("%Y-%m-%d")

    # 从数据库加载已处理的股票
    processed_codes = get_today_processed_codes(today)

    all_stocks = _load_stocks()
    remaining_stocks = [s for s in all_stocks if s["code"] not in processed_codes]

    if counter is None:
        counter = {'total': len(remaining_stocks), 'success': 0, 'failed': 0}
    else:
        counter['total'] = len(remaining_stocks)

    logger.info("[最高最低价] 开始采集，共 %d 只股票（今日已完成 %d 只）",
                len(remaining_stocks), len(processed_codes))

    if not remaining_stocks:
        logger.info("[最高最低价] 所有股票已处理完成")
        return counter

    semaphore = asyncio.Semaphore(max_concurrent)

    async def task(stock):
        async with semaphore:
            await _process_single_price(stock, counter)

    await asyncio.gather(*[task(s) for s in remaining_stocks], return_exceptions=True)
    logger.info("[最高最低价] 采集完成，总%d 成功%d 失败%d",
                counter['total'], counter['success'], counter['failed'])
    return counter


if __name__ == "__main__":
    asyncio.run(run_price_job())
