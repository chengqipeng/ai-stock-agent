import requests
from typing import Dict, List, Tuple


# 指标配置：(字段名, 计数字段名, 显示名称, 格式化函数)
METRICS_CONFIG = [
    ('EPS', 'EPS_COUNT', '每股收益(元)', lambda v: f"{v:.4f}" if v else '-'),
    ('EPS_LASTMONTHS', 'EPS_LASTMONTHS_COUNT', '上一个月预测每股收益(元)', lambda v: f"{v:.4f}" if v else '-'),
    ('BVPS', 'BVPS_COUNT', '每股净资产(元)', lambda v: f"{v:.4f}" if v else '-'),
    ('ROE', 'ROE_COUNT', '净资产收益率(%)', lambda v: f"{v:.2f}" if v else '-'),
    ('PARENT_NETPROFIT', 'PARENT_NETPROFIT_COUNT', '归属于母公司股东的净利润(元)', lambda v: f"{v/100000000:.2f}亿" if v else '-'),
    ('TOTAL_OPERATE_INCOME', 'TOTAL_OPERATE_INCOME_COUNT', '营业总收入(元)', lambda v: f"{v/100000000:.2f}亿" if v else '-'),
    ('OPERATE_PROFIT', 'OPERATE_PROFIT_COUNT', '营业利润(元)', lambda v: f"{v/100000000:.2f}亿" if v else '-'),
]


def get_institution_forecast_summary(secucode: str) -> dict:
    """获取机构预测统计汇总数据"""
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_HSF10_RESPREDICT_COUNTSTATISTICS",
        "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,YEAR,YEAR_MARK,EPS,EPS_LASTMONTHS,BVPS,ROE,PARENT_NETPROFIT,TOTAL_OPERATE_INCOME,OPERATE_PROFIT,EPS_COUNT,EPS_LASTMONTHS_COUNT,BVPS_COUNT,ROE_COUNT,PARENT_NETPROFIT_COUNT,TOTAL_OPERATE_INCOME_COUNT,OPERATE_PROFIT_COUNT,RANK",
        "quoteColumns": "",
        "filter": f'(SECUCODE="{secucode}")',
        "pageNumber": 1,
        "pageSize": 200,
        "sortTypes": "1",
        "sortColumns": "RANK",
        "source": "HSF10",
        "client": "PC",
        "v": "04409410361813054"
    }
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Origin": "https://emweb.securities.eastmoney.com",
        "Pragma": "no-cache",
        "Referer": "https://emweb.securities.eastmoney.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"'
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


def _parse_raw_data(data: dict, limit_years: int = None, current_and_next_only: bool = False) -> Tuple[str, str, Dict[str, dict], List[str]]:
    """解析原始数据，返回股票信息和按年份组织的数据
    
    Args:
        data: 原始数据
        limit_years: 限制返回的年份数量，None表示返回所有年份
        current_and_next_only: 是否只返回今年和未来一年的数据
    """
    if not data.get('success') or not data.get('result', {}).get('data'):
        return '', '', {}, []
    
    items = data['result']['data']
    stock_name = items[0].get('SECURITY_NAME_ABBR', '')
    stock_code = items[0].get('SECURITY_CODE', '')
    
    year_data = {}
    for item in items:
        year = item.get('YEAR', '')
        year_mark = item.get('YEAR_MARK', '')
        year_data[year] = {'item': item, 'year_mark': year_mark}
    
    years = sorted(year_data.keys())
    
    if current_and_next_only and len(years) >= 2:
        from datetime import datetime
        current_year = datetime.now().year
        # 筛选出今年和未来一年的数据
        filtered_years = [y for y in years if y in [current_year, current_year + 1]]
        years = filtered_years if len(filtered_years) == 2 else years[:2]
    elif limit_years:
        years = years[:limit_years]
    
    return stock_name, stock_code, year_data, years


def _build_json_result(stock_name: str, stock_code: str, year_data: Dict[str, dict], years: List[str]) -> list:
    """构建JSON格式结果"""
    result = []
    for year in years:
        year_dict = {"年份": str(year)}
        for field, count_field, label, formatter in METRICS_CONFIG:
            item = year_data[year]['item']
            value = item.get(field)
            year_dict[label] = formatter(value) if value is not None else '-'
        result.append(year_dict)
    return result


def _build_markdown_result(stock_name: str, stock_code: str, year_data: Dict[str, dict], years: List[str]) -> str:
    """构建Markdown格式结果"""
    md = f"# {stock_name}({stock_code}) 机构预测统计汇总\n\n"
    md += "| 预测指标 | " + " | ".join([f"{year}年" if '预测' not in year_data[year]['year_mark'] else f"{year}年预测" for year in years]) + " |\n"
    md += "|" + "|".join(["------"] * (len(years) + 1)) + "|\n"
    
    for field, count_field, label, formatter in METRICS_CONFIG:
        row = f"| {label} |"
        for year in years:
            item = year_data[year]['item']
            value = formatter(item.get(field))
            count = item.get(count_field, 0)
            row += f" {value}({count}家) |" if count else f" {value} |"
        md += row + "\n"
    
    return md


def get_institution_forecast_summary_json(secucode: str) -> list:
    """获取机构预测统计汇总数据并转换为JSON格式"""
    data = get_institution_forecast_summary(secucode)
    stock_name, stock_code, year_data, years = _parse_raw_data(data)
    return _build_json_result(stock_name, stock_code, year_data, years) if years else []


def get_institution_forecast_summary_current_next_year_json(secucode: str) -> list:
    """获取机构预测统计汇总数据并转换为JSON格式（仅今年和未来一年）"""
    data = get_institution_forecast_summary(secucode)
    stock_name, stock_code, year_data, years = _parse_raw_data(data, current_and_next_only=True)
    return _build_json_result(stock_name, stock_code, year_data, years) if years else []


def get_institution_forecast_summary_markdown(secucode: str) -> str:
    """获取机构预测统计汇总数据并转换为Markdown格式"""
    data = get_institution_forecast_summary(secucode)
    stock_name, stock_code, year_data, years = _parse_raw_data(data)
    return _build_markdown_result(stock_name, stock_code, year_data, years) if years else "# 无数据\n"


def get_institution_forecast_summary_current_next_year_markdown(secucode: str) -> str:
    """获取机构预测统计汇总数据并转换为Markdown格式（仅今年和未来一年）"""
    data = get_institution_forecast_summary(secucode)
    stock_name, stock_code, year_data, years = _parse_raw_data(data, current_and_next_only=True)
    return _build_markdown_result(stock_name, stock_code, year_data, years) if years else "# 无数据\n"


if __name__ == "__main__":
    import json
    
    secucode = "002371.SZ"
    print(f"正在获取 {secucode} 的机构预测统计汇总数据...\n")
    
    print("\n=== JSON格式（当前年+未来一年） ===")
    json_data_recent = get_institution_forecast_summary_current_next_year_json(secucode)
    print(json.dumps(json_data_recent, ensure_ascii=False, indent=2))
    
    print("\n=== Markdown格式（所有年份） ===")
    markdown = get_institution_forecast_summary_markdown(secucode)
    print(markdown)
    
    print("\n=== Markdown格式（当前年+未来一年） ===")
    markdown_recent = get_institution_forecast_summary_current_next_year_markdown(secucode)
    print(markdown_recent)
