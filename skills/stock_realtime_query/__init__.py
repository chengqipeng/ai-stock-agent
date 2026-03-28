"""
个股实时盘口与分时数据查询 Skill

纯数据抓取模块，无数据库依赖。仅需 aiohttp。

使用：
    from skills.stock_realtime_query import fetch_order_book, fetch_time_data

    data = await fetch_order_book("600519.SH")
    data = await fetch_time_data("600519.SH")
"""

from skills.stock_realtime_query.order_book_fetcher import fetch_order_book
from skills.stock_realtime_query.time_data_fetcher import fetch_time_data

__all__ = ["fetch_order_book", "fetch_time_data"]
