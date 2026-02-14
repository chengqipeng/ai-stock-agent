from common.http.http_utils import fetch_eastmoney_api
from common.utils.amount_utils import convert_amount_unit
from datetime import datetime, timedelta


async def get_revenue_analysis(secucode="002371.SZ", report_date="2024-12-31", page_size=200):
    """获取主营业务构成数据"""
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_F10_FN_MAINOP",
        "columns": "SECUCODE,SECURITY_CODE,REPORT_DATE,MAINOP_TYPE,ITEM_NAME,MAIN_BUSINESS_INCOME,MBI_RATIO,MAIN_BUSINESS_COST,MBC_RATIO,MAIN_BUSINESS_RPOFIT,MBR_RATIO,GROSS_RPOFIT_RATIO,RANK",
        "filter": f'(SECUCODE="{secucode}")(REPORT_DATE=\'{report_date}\')',
        "pageNumber": "1",
        "pageSize": str(page_size),
        "sortTypes": "1,1",
        "sortColumns": "MAINOP_TYPE,RANK",
        "source": "HSF10",
        "client": "PC"
    }
    
    data = await fetch_eastmoney_api(url, params, referer="https://emweb.securities.eastmoney.com/")
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        raise Exception(f"未获取到股票 {secucode} 的主营业务构成数据")


async def get_revenue_analysis_with_count(secucode="002371.SZ", report_date="2024-12-31", page_size=200):
    """获取主营业务构成数据及数量"""
    data = await get_revenue_analysis(secucode, report_date, page_size)
    return {"data": data, "count": len(data)}


async def get_revenue_analysis_three_years(secucode="002371.SZ", stock_name = None, page_size=200):
    """获取过去三年的主营业务构成数据（每年4个季度）"""
    current_year = datetime.now().year
    quarters = ["12-31", "09-30", "06-30", "03-31"]
    
    all_data = []
    for year in range(current_year - 3, current_year + 1):
        for quarter in quarters:
            date = f"{year}-{quarter}"
            try:
                data = await get_revenue_analysis(secucode, date, page_size)
                all_data.extend(data)
            except:
                pass
    
    return {"data": all_data, "count": len(all_data)}


async def get_revenue_analysis_markdown(secucode="002371.SZ", report_date="2024-12-31", stock_code=None, stock_name=None):
    """获取主营业务构成并转换为markdown"""
    data = await get_revenue_analysis(secucode, report_date)
    if not data:
        return ""
    
    if not stock_code:
        stock_code = secucode.split('.')[0]
    header = f"## <{stock_code} {stock_name}> - 主营业务构成 ({report_date})" if stock_name else f"## 主营业务构成 ({report_date})"
    
    markdown = f"""{header}
| 类型 | 项目名称 | 主营收入(元) | 收入比例 | 主营成本(元) | 成本比例 | 主营利润(元) | 利润比例 | 毛利率(%) |
|------|---------|------------|---------|------------|---------|------------|---------|----------|
"""
    
    for item in data:
        mainop_type = "按产品" if item.get('MAINOP_TYPE') == '1' else "按地区" if item.get('MAINOP_TYPE') == '2' else "其他"
        item_name = item.get('ITEM_NAME', '--')
        income = convert_amount_unit(item.get('MAIN_BUSINESS_INCOME')) if item.get('MAIN_BUSINESS_INCOME') else '--'
        income_ratio = f"{round(item.get('MBI_RATIO', 0), 2)}%" if item.get('MBI_RATIO') else '--'
        cost = convert_amount_unit(item.get('MAIN_BUSINESS_COST')) if item.get('MAIN_BUSINESS_COST') else '--'
        cost_ratio = f"{round(item.get('MBC_RATIO', 0), 2)}%" if item.get('MBC_RATIO') else '--'
        profit = convert_amount_unit(item.get('MAIN_BUSINESS_RPOFIT')) if item.get('MAIN_BUSINESS_RPOFIT') else '--'
        profit_ratio = f"{round(item.get('MBR_RATIO', 0), 2)}%" if item.get('MBR_RATIO') else '--'
        gross_ratio = f"{round(item.get('GROSS_RPOFIT_RATIO', 0), 2)}%" if item.get('GROSS_RPOFIT_RATIO') else '--'
        
        markdown += f"| {mainop_type} | {item_name} | {income} | {income_ratio} | {cost} | {cost_ratio} | {profit} | {profit_ratio} | {gross_ratio} |\n"
    
    return markdown + "\n"


async def get_revenue_analysis_three_years_markdown(secucode="002371.SZ", stock_code=None, stock_name=None):
    """获取过去三年的主营业务构成并转换为markdown（每年4个季度）"""
    current_year = datetime.now().year
    quarters = ["12-31", "09-30", "06-30", "03-31"]
    
    markdown = ""
    for year in range(current_year - 3, current_year + 1):
        for quarter in quarters:
            date = f"{year}-{quarter}"
            try:
                md = await get_revenue_analysis_markdown(secucode, date, stock_code, stock_name)
                if md:
                    markdown += md
            except:
                pass
    
    return markdown


if __name__ == "__main__":
    import asyncio
    
    async def main():
        # 测试获取过去三年json数据
        result = await get_revenue_analysis_three_years("002371.SZ")
        print(f"过去三年数据条数: {result['count']}\n")
        
        # 测试生成过去三年markdown
        markdown = await get_revenue_analysis_three_years_markdown("002371.SZ", "002371", "北方华创")
        print(markdown)
    
    asyncio.run(main())
