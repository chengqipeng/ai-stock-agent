import json

from common.http.http_utils import fetch_eastmoney_api
from common.utils.amount_utils import convert_amount_unit, convert_amount_org_holder_2
from datetime import datetime, timedelta

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from common.utils.cache_utils import get_cache_path, load_cache, save_cache


async def get_revenue_analysis(stock_info: StockInfo, report_date="2024-12-31", page_size=200):
    """获取主营业务构成数据"""
    cache_key = f"{stock_info.stock_code}_{report_date}"
    cache_path = get_cache_path("revenue_analysis", cache_key)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_F10_FN_MAINOP",
        "columns": "SECUCODE,SECURITY_CODE,REPORT_DATE,MAINOP_TYPE,ITEM_NAME,MAIN_BUSINESS_INCOME,MBI_RATIO,MAIN_BUSINESS_COST,MBC_RATIO,MAIN_BUSINESS_RPOFIT,MBR_RATIO,GROSS_RPOFIT_RATIO,RANK",
        "filter": f'(SECUCODE="{stock_info.stock_code_normalize}")(REPORT_DATE=\'{report_date}\')',
        "pageNumber": "1",
        "pageSize": str(page_size),
        "sortTypes": "1,1",
        "sortColumns": "MAINOP_TYPE,RANK",
        "source": "HSF10",
        "client": "PC"
    }
    
    data = await fetch_eastmoney_api(url, params, referer="https://emweb.securities.eastmoney.com/")
    if data.get("result") and data["result"].get("data"):
        result = data["result"]["data"]
        # 保存缓存
        save_cache(cache_path, result)
        return result
    else:
        raise Exception(f"未获取到股票 {stock_info.stock_code_normalize} 的主营业务构成数据")


async def get_revenue_analysis_with_count(stock_info: StockInfo, report_date="2024-12-31", page_size=200):
    """获取主营业务构成数据及数量"""
    data = await get_revenue_analysis(stock_info, report_date, page_size)
    return {"data": data, "count": len(data)}


async def get_revenue_analysis_three_years(stock_info: StockInfo, page_size=200):
    """获取过去三年的主营业务构成数据（每年4个季度）"""
    current_year = datetime.now().year
    quarters = ["12-31", "09-30", "06-30", "03-31"]
    
    all_data = []
    for year in range(current_year - 3, current_year + 1):
        for quarter in quarters:
            date = f"{year}-{quarter}"
            try:
                data = await get_revenue_analysis(stock_info, date, page_size)
                all_data.extend(data)
            except:
                pass
    
    # 转换为中文key
    chinese_data = []
    for item in all_data:
        chinese_item = {
            "报告日期": item.get("REPORT_DATE"),
            "类型": "按产品" if item.get('MAINOP_TYPE') == '1' else "按地区" if item.get('MAINOP_TYPE') == '2' else "其他",
            "项目名称": item.get("ITEM_NAME"),
            "主营收入（亿元）": convert_amount_org_holder_2(item.get("MAIN_BUSINESS_INCOME")),
            "收入比例": (str(item["MBI_RATIO"] * 100) + "%") if item.get("MBI_RATIO") is not None else None,
            "主营成本（亿元）": convert_amount_org_holder_2(item.get("MAIN_BUSINESS_COST")),
            "成本比例": (str(item["MBC_RATIO"] * 100) + "%") if item.get("MBC_RATIO") is not None else None,
            "主营利润（亿元）": convert_amount_org_holder_2(item.get("MAIN_BUSINESS_RPOFIT")),
            "利润比例": (str(item["MBR_RATIO"] * 100) + "%") if item.get("MBR_RATIO") is not None else None,
            "毛利率": (str(item["GROSS_RPOFIT_RATIO"] * 100) + "%") if item.get("GROSS_RPOFIT_RATIO") is not None else None
        }
        chinese_data.append(chinese_item)
    
    # 按报告日期倒序排序
    chinese_data.sort(key=lambda x: x.get("报告日期") or "", reverse=True)
    
    return {"数据": chinese_data, "数量": len(chinese_data)}

if __name__ == "__main__":
    import asyncio
    
    async def main():
        # 测试获取过去三年json数据
        stock_info: StockInfo = get_stock_info_by_name("北方华创")
        result = await get_revenue_analysis_three_years(stock_info)
        print(f"过去三年数据条数: {json.dumps(result, ensure_ascii=False, indent=2)}\n")

    asyncio.run(main())
