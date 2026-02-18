import json
import asyncio

from common.utils.stock_info_utils import get_stock_info_by_name
from service.auto_job.stock_history_highest_lowest_price_auto_job import output_file
from service.eastmoney.stock_info.stock_realtime import get_stock_realtime


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


async def get_top_strongest_stocks(days: int = 120, top_n: int = 10) -> list:
    """提取过去指定天数内多次创新高的最强股票

    Args:
        days: 回溯天数，默认120天
        top_n: 返回前N只股票，默认10只

    Returns:
        按创新高次数排序的股票列表
    """
    if not output_file.exists():
        return []

    with open(output_file, "r", encoding="utf-8") as f:
        results = json.load(f)

    from datetime import datetime, timedelta
    from service.eastmoney.technical.abs.stock_indicator_base import get_stock_history_kline_max_min
    
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # 第一步：筛选120天内创新高的股票，粗略计算涨幅并排序取前10
    candidates = []
    for stock in results:
        highest_date = stock.get("highest_date", "")
        highest_price = stock.get("highest_price")
        lowest_price = stock.get("lowest_price")
        
        if highest_date >= start_date and highest_price and lowest_price and stock.get("name"):
            days_ago = (datetime.now() - datetime.strptime(highest_date, "%Y-%m-%d")).days
            rough_gain = round((highest_price - lowest_price) / lowest_price * 100, 2)
            
            candidates.append({
                "name": stock.get("name"),
                "code": stock.get("code"),
                "highest_price": highest_price,
                "highest_date": highest_date,
                "days_ago": days_ago,
                "rough_gain": rough_gain
            })
    
    candidates.sort(key=lambda x: -x["rough_gain"])
    top_candidates = candidates[:top_n]
    
    # 第二步：对前10只股票获取K线数据计算真实涨幅
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
        except:
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
            "距最高价跌幅%": drop_pct
        })
    
    return strongest_stocks


if __name__ == "__main__":
    async def main():
        #print("=== 创新高/新低统计 ===")
        #print(json.dumps(get_new_high_low_count(), ensure_ascii=False, indent=2))
        
        print("\n=== 最强10只股票 ===")
        result = await get_top_strongest_stocks()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())