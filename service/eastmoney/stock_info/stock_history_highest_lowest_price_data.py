import asyncio
import json
import logging
from datetime import datetime, timedelta

from common.utils.stock_info_utils import get_stock_info_by_name
from dao.stock_highest_lowest_price_dao import (
    get_new_high_low_count_from_db,
    get_candidates_by_high_date,
)
from service.eastmoney.stock_info.stock_realtime import get_stock_realtime

logger = logging.getLogger(__name__)


def get_new_high_low_count(days: int = 120) -> dict:
    """查询过去指定天数内创新高和新低的股票数量"""
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    stats = get_new_high_low_count_from_db(start_date)

    return {
        "创新高股票数量": stats["new_high"],
        "创新低股票数量": stats["new_low"],
        "总股票数": stats["total"],
        "回溯天数": days,
        "开始日期": start_date,
        "结束日期": end_date,
    }


async def get_top_strongest_stocks(days: int = 120, top_n: int = 10) -> list:
    """提取过去指定天数内多次创新高的最强股票"""
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # 第一步：从数据库查询候选股票（已按粗略涨幅排序）
    candidates = get_candidates_by_high_date(start_date, top_n)
    if not candidates:
        return []

    # 第二步：对候选股票获取K线数据计算真实涨幅
    from service.eastmoney.stock_info.stock_day_kline_data import get_stock_history_kline_max_min

    top_candidates = []
    for stock in candidates:
        try:
            days_ago = (datetime.now() - datetime.strptime(stock["highest_date"], "%Y-%m-%d")).days
        except ValueError as e:
            logger.warning("Invalid highest_date format for stock %s: %s", stock.get("name"), e)
            continue
        rough_gain = round((stock["highest_price"] - stock["lowest_price"]) / stock["lowest_price"] * 100, 2)
        top_candidates.append({
            "name": stock["name"],
            "code": stock["code"],
            "highest_price": stock["highest_price"],
            "highest_date": stock["highest_date"],
            "days_ago": days_ago,
            "rough_gain": rough_gain,
        })

    for stock in top_candidates:
        try:
            stock_info = get_stock_info_by_name(stock["name"])
            kline_data = await get_stock_history_kline_max_min(stock_info)

            # 获取120天前的价格
            dates = sorted(kline_data.keys())
            target_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            price_120_days_ago = None

            for date in dates:
                if date >= target_date:
                    price_120_days_ago = kline_data[date]['close_price']
                    break

            # 获取实时价格
            realtime_data = await get_stock_realtime(stock_info)
            current_price = realtime_data.get('f43')

            # 计算120天涨幅
            if price_120_days_ago and current_price:
                gain_pct = round((current_price - price_120_days_ago) / price_120_days_ago * 100, 2)
            else:
                gain_pct = None

            stock['current_price'] = current_price
            stock['gain_pct'] = gain_pct
        except Exception as e:
            logger.warning("Failed to get kline/realtime data for %s: %s", stock.get('name'), e)
            stock['current_price'] = None
            stock['gain_pct'] = None

    # 第三步：构建返回结果
    strongest_stocks = []
    for stock in top_candidates:
        drop_pct = round((stock['current_price'] - stock["highest_price"]) / stock["highest_price"] * 100, 2) if stock.get('current_price') else None

        strongest_stocks.append({
            "股票名称": stock["name"],
            "股票代码": stock["code"],
            "最高价": stock["highest_price"],
            "创新高日期": stock["highest_date"],
            "距今天数": stock["days_ago"],
            "120天涨幅%": stock.get('gain_pct'),
            "当前价格": stock.get('current_price'),
            "距最高价跌幅%": drop_pct,
        })

    return strongest_stocks


if __name__ == "__main__":
    async def main():
        logger.info("\n=== 最强10只股票 ===")
        result = await get_top_strongest_stocks()
        logger.info(json.dumps(result, ensure_ascii=False))

    asyncio.run(main())
