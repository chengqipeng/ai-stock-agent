import sqlite3
from pathlib import Path

DB_PATH = Path("data_results/sql_lite/stock_klines.db")


def get_kline_data(stock_code: str, start_date: str = None, end_date: str = None, limit: int = None) -> list[dict]:
    """
    查询股票K线数据
    
    Args:
        stock_code: 股票代码，如 "300812.SZ"
        start_date: 开始日期，如 "2024-01-01"（可选）
        end_date: 结束日期，如 "2024-12-31"（可选）
        limit: 返回条数限制（可选）
    
    Returns:
        list[dict]: K线数据列表，按日期升序排列
    """
    table_name = f"kline_{stock_code.replace('.', '_')}"
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    sql = f"SELECT date, open_price, close_price, high_price, low_price, trading_volume, trading_amount, amplitude, change_percent, change_amount, change_hand FROM {table_name} WHERE 1=1"
    params = []

    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND date <= ?"
        params.append(end_date)

    sql += " ORDER BY date ASC"

    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    try:
        cursor.execute(sql, params)
        rows = [dict(row) for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        rows = []
    finally:
        conn.close()

    return rows


if __name__ == "__main__":
    rows = get_kline_data("300812.SZ", start_date="2024-01-01", limit=5)
    for row in rows:
        print(row)
