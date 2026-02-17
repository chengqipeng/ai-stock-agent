from service.eastmoney.stock_info.stock_top_ten_shareholders_circulation import get_top_ten_shareholders_circulation_by_dates
from common.utils.stock_info_utils import StockInfo


async def get_detect_new_major_shareholders(stock_info: StockInfo):
    """检测近两个季度是否有新进大股东
    
    Args:
        stock_info: 股票信息对象
        
    Returns:
        dict: 包含检测结果的字典
        {
            "是否有新进股东": bool,
            "新进股东列表": list,
            "最新季度": str,
            "上一季度": str
        }
    """
    # 获取最近两个季度的股东数据（包含完整信息）
    shareholders_data = await get_top_ten_shareholders_circulation_by_dates(
        stock_info, 
        page_size=3,
        limit=3
    )
    
    if not shareholders_data:
        return {
            "是否有新进股东": False,
            "新进股东列表": [],
            "最新季度": "",
            "上一季度": "",
            "错误信息": "无法获取股东数据"
        }
    
    # 按报告期分组
    quarters = {}
    shareholders_detail = {}  # 存储股东详细信息
    for item in shareholders_data:
        quarter = item.get('报告期', '')
        holder_name = item.get('股东名称', '')
        if quarter not in quarters:
            quarters[quarter] = []
        quarters[quarter].append(holder_name)
        # 存储最新季度的股东详细信息
        if holder_name:
            shareholders_detail[holder_name] = item
    
    quarter_list = sorted(quarters.keys(), reverse=True)
    
    if len(quarter_list) < 2:
        return {
            "是否有新进股东": False,
            "新进股东列表": [],
            "最新季度": quarter_list[0] if quarter_list else "",
            "上一季度": "",
            "错误信息": "数据不足，无法比较两个季度"
        }
    
    latest_quarter = quarter_list[0]
    previous_quarter = quarter_list[1]
    
    latest_shareholders = set(quarters[latest_quarter])
    previous_shareholders = set(quarters[previous_quarter])
    
    # 找出新进股东并获取其持股数据
    new_shareholders_names = latest_shareholders - previous_shareholders
    new_shareholders_list = [
        shareholders_detail[name] for name in new_shareholders_names if name in shareholders_detail
    ]
    
    return {
        "是否有新进股东": len(new_shareholders_list) > 0,
        "新进股东列表": new_shareholders_list,
        "最新季度": latest_quarter,
        "上一季度": previous_quarter
    }


if __name__ == "__main__":
    import asyncio
    import json
    from common.utils.stock_info_utils import get_stock_info_by_name
    
    async def main():
        stock_name = "三花智控"
        stock_info = get_stock_info_by_name(stock_name)
        
        result = await get_detect_new_major_shareholders(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    
    asyncio.run(main())