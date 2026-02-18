import json

from service.auto_job.stock_history_highest_lowest_price_auto_job import output_file


def get_new_high_low_count(days: int = 120) -> dict:
    """查询过去指定天数内创新高和新低的股票数量

    Args:
        days: 回溯天数，默认120天

    Returns:
        包含创新高和新低股票数量的字典
    """
    if not output_file.exists():
        return {
            "创新高股票数量": 0,
            "创新低股票数量": 0,
            "总股票数": 0
        }

    with open(output_file, "r", encoding="utf-8") as f:
        results = json.load(f)

    from datetime import datetime, timedelta
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    new_high_count = 0
    new_low_count = 0

    for stock in results:
        # 检查是否在指定天数内创新高
        if stock.get("highest_date", "") >= start_date:
            new_high_count += 1

        # 检查是否在指定天数内创新低
        if stock.get("lowest_date", "") >= start_date:
            new_low_count += 1

    return {
        "创新高股票数量": new_high_count,
        "创新低股票数量": new_low_count,
        "总股票数": len(results),
        "回溯天数": days,
        "开始日期": start_date,
        "结束日期": end_date
    }