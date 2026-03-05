import logging

import pandas as pd
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline, \
    get_stock_day_range_kline_by_db_cache

logger = logging.getLogger(__name__)


async def get_volume_avg(stock_info: StockInfo, days=20, page_size=120):
    """计算N日成交量均值

    Returns:
        list: [{"date": "2024-01-01", "volume_avg": 1234.56}, ...]
    """
    klines = await get_stock_day_range_kline_by_db_cache(stock_info, limit=page_size + days)
    rows = [{'date': k.split(',')[0], 'trading_volume': round(float(k.split(',')[5]) / 10000, 2)} for k in klines]
    rows.sort(key=lambda x: x['date'])
    df = pd.DataFrame(rows)
    df['volume_avg'] = df['trading_volume'].rolling(window=days, min_periods=days).mean().round(2)
    return df[['date', 'volume_avg']].tail(page_size).to_dict('records')[::-1]


async def get_volume_avg_cn(stock_info: StockInfo, days=20, page_size=120):
    """计算N日成交量均值（中文键）

    Returns:
        list: [{"日期": "2024-01-01", "20日均成交量（万手）": 1234.56}, ...]
    """
    result = await get_volume_avg(stock_info, days, page_size)
    return [{"日期": item["date"], f"{days}日均成交量（万手）": item["volume_avg"]} for item in result]


async def get_20day_volume_avg(stock_info: StockInfo, page_size=120):
    return await get_volume_avg(stock_info, days=20, page_size=page_size)


async def get_5day_volume_avg(stock_info: StockInfo, page_size=120):
    return await get_volume_avg(stock_info, days=5, page_size=page_size)

async def get_60day_volume_avg(stock_info: StockInfo, page_size=120):
    return await get_volume_avg(stock_info, days=60, page_size=page_size)


async def get_20day_volume_avg_cn(stock_info: StockInfo, page_size=120):
    return await get_volume_avg_cn(stock_info, days=20, page_size=page_size)


if __name__ == '__main__':
    import asyncio
    import json
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name('北方华创')
        result = await get_60day_volume_avg(stock_info)
        logger.info(json.dumps(result, ensure_ascii=False))

    asyncio.run(main())
