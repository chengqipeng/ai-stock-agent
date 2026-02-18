import asyncio
import json
from datetime import datetime
from pathlib import Path

from common.constants.stocks_data import STOCKS
from common.utils.stock_info_utils import get_stock_info_by_name
from service.eastmoney.stock_info.stock_month_kline_data import get_stock_month_kline_list


# 获取项目根目录
project_root = Path(__file__).parent.parent.parent
output_file = project_root / "data_results/stock_highest_lowest_price/stock_highest_lowest_price.json"
lock = asyncio.Lock()
completed_count = 0
total_count = 0


def save_result(result):
    """保存单条结果，如果有更高价或更新时间则覆盖"""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # 读取现有数据
    if output_file.exists():
        with open(output_file, "r", encoding="utf-8") as f:
            results = json.load(f)
    else:
        results = []
    
    # 查找是否已存在
    updated = False
    for i, r in enumerate(results):
        if r["code"] == result["code"]:
            # 如果新数据的最高价更高或日期更新，则覆盖
            if result["highest_price"] > r["highest_price"] or result["highest_date"] > r["highest_date"]:
                results[i] = result
                updated = True
            break
    else:
        # 不存在则添加
        results.append(result)
        updated = True
    
    # 只有数据变化时才保存
    if updated:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)


async def process_stock(stock):
    """处理单个股票"""
    global completed_count
    try:
        stock_name = stock["name"]
        stock_info = get_stock_info_by_name(stock_name)
        kline_data = await get_stock_month_kline_list(stock_info)
        
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
                save_result(result)
                completed_count += 1
            
            print(f"✓ {stock_name}: 最高{highest_record['最高']}({highest_record['日期']}) 最低{lowest_record['最低']}({lowest_record['日期']}) - 当前执行 {completed_count}/{total_count}")
    except Exception as e:
        print(f"✗ {stock.get('name', '')} 失败: {str(e)}")


async def process_batch(stocks):
    """处理一批股票"""
    for stock in stocks:
        await process_stock(stock)
    await asyncio.sleep(2)


async def main():
    """主函数：5线程遍历所有股票"""
    global completed_count, total_count
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 加载已处理的股票
    processed_codes = set()
    if output_file.exists():
        with open(output_file, "r", encoding="utf-8") as f:
            existing_results = json.load(f)
            # 只保留今天处理的数据
            for r in existing_results:
                if r.get("update_time", "").startswith(today):
                    processed_codes.add(r["code"])
    
    # 过滤未处理的股票
    remaining_stocks = [s for s in STOCKS if s["code"] not in processed_codes]
    total_count = len(remaining_stocks)
    completed_count = 0
    
    print(f"开始处理 {total_count} 只股票（今日已完成 {len(processed_codes)} 只）...")
    
    if not remaining_stocks:
        print("所有股票已处理完成！")
        return
    
    # 将股票分成5批
    batch_size = (len(remaining_stocks) + 4) // 5
    batches = [remaining_stocks[i:i + batch_size] for i in range(0, len(remaining_stocks), batch_size)]
    
    # 5线程并发处理
    tasks = [process_batch(batch) for batch in batches]
    await asyncio.gather(*tasks)
    
    print(f"\n处理完成！结果已保存到: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())


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
