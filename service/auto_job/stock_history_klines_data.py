from dao.stock_kline_dao import get_kline_data as get_db_cache_kline_data

if __name__ == "__main__":
    rows = get_db_cache_kline_data("300812.SZ", start_date="2024-01-01", limit=5)
    for row in rows:
        print(row)
