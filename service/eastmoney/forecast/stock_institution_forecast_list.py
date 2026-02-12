import requests
from datetime import datetime


def get_institution_forecast(secucode: str) -> dict:
    """
    获取机构预测数据（每股收益、市盈率）
    
    Args:
        secucode: 证券代码，格式如 "002371.SZ"
    
    Returns:
        dict: 机构预测数据
    """
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    
    params = {
        "reportName": "RPT_HSF10_RES_ORGPREDICT",
        "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,PUBLISH_DATE,ORG_CODE,ORG_NAME_ABBR,YEAR1,YEAR_MARK1,EPS1,PE1,YEAR2,YEAR_MARK2,EPS2,PE2,YEAR3,YEAR_MARK3,EPS3,PE3,YEAR4,YEAR_MARK4,EPS4,PE4",
        "quoteColumns": "",
        "filter": f'(SECUCODE="{secucode}")',
        "pageNumber": 1,
        "pageSize": 200,
        "sortTypes": "",
        "sortColumns": "",
        "source": "HSF10",
        "client": "PC",
        "v": "04720683911540642"
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


def _format_value(value, decimal_places: int = 2) -> str:
    """格式化数值"""
    return f"{value:.{decimal_places}f}" if value is not None else '-'


def _build_table_header(columns: list) -> str:
    """构建表格头部"""
    header = "| " + " | ".join(columns) + " |\n"
    separator = "|".join(['-' * (len(col) + 2) for col in columns])
    return header + "|" + separator + "|\n"


def _extract_year_data(item: dict, year: int) -> tuple:
    """提取指定年份的数据"""
    for i in range(1, 5):
        if item.get(f'YEAR{i}') == year:
            eps = _format_value(item.get(f'EPS{i}'))
            pe = _format_value(item.get(f'PE{i}'))
            return eps, pe
    return '-', '-'


def _process_forecast_data(data: dict, year_filter=None) -> dict:
    """处理机构预测数据的通用方法"""
    if not data.get('success') or not data.get('result', {}).get('data'):
        return None
    
    items = data['result']['data']
    stock_name = items[0].get('SECURITY_NAME_ABBR', '')
    stock_code = items[0].get('SECURITY_CODE', '')
    
    forecasts = []
    for item in items:
        forecast = {
            '发布日期': item.get('PUBLISH_DATE', '-')[:10] if item.get('PUBLISH_DATE') else '-',
            '机构名称': item.get('ORG_NAME_ABBR') or '-'
        }
        
        if year_filter:
            for year in year_filter:
                for i in range(1, 5):
                    if item.get(f'YEAR{i}') == year:
                        forecast[f'{year}年每股收益'] = item.get(f'EPS{i}')
                        forecast[f'{year}年市盈率'] = item.get(f'PE{i}')
                        break
        else:
            for i in range(1, 5):
                year = item.get(f'YEAR{i}')
                if year:
                    forecast[f'{year}年每股收益'] = item.get(f'EPS{i}')
                    forecast[f'{year}年市盈率'] = item.get(f'PE{i}')
        
        forecasts.append(forecast)
    
    return {
        'stock_name': stock_name,
        'stock_code': stock_code,
        'forecasts': forecasts
    }


def get_institution_forecast_to_json(secucode: str) -> dict:
    """将机构预测数据转换为JSON格式（显示所有年份）"""
    data = get_institution_forecast(secucode)
    return _process_forecast_data(data)


def get_institution_forecast_to_markdown(secucode: str) -> str:
    """将机构预测数据转换为Markdown格式（显示所有年份）"""
    data = get_institution_forecast(secucode)
    if not data.get('success') or not data.get('result', {}).get('data'):
        return "# 无数据\n"
    
    items = data['result']['data']
    stock_name = items[0].get('SECURITY_NAME_ABBR', '')
    stock_code = items[0].get('SECURITY_CODE', '')
    
    md = f"# {stock_name}({stock_code}) 机构预测\n\n"
    md += _build_table_header(["发布日期", "机构名称", "24年收益", "24年市盈率", "25年收益", "25年市盈率", "26年收益", "26年市盈率", "27年收益", "27年市盈率"])
    
    for item in items:
        publish_date = item.get('PUBLISH_DATE', '-')[:10] if item.get('PUBLISH_DATE') else '-'
        org_name = item.get('ORG_NAME_ABBR') or '-'
        eps1 = _format_value(item.get('EPS1'))
        pe1 = _format_value(item.get('PE1'))
        eps2 = _format_value(item.get('EPS2'))
        pe2 = _format_value(item.get('PE2'))
        eps3 = _format_value(item.get('EPS3'))
        pe3 = _format_value(item.get('PE3'))
        eps4 = _format_value(item.get('EPS4'))
        pe4 = _format_value(item.get('PE4'))
        
        md += f"| {publish_date} | {org_name} | {eps1} | {pe1} | {eps2} | {pe2} | {eps3} | {pe3} | {eps4} | {pe4} |\n"
    
    return md


def get_institution_forecast_current_next_year_to_json(secucode: str) -> dict:
    """将机构预测数据转换为JSON格式（只显示当前年和未来一年）"""
    data = get_institution_forecast(secucode)
    current_year = datetime.now().year
    next_year = current_year + 1
    return _process_forecast_data(data, year_filter=[current_year, next_year])


def get_institution_forecast_current_next_year_to_markdown(secucode: str) -> str:
    """将机构预测数据转换为Markdown格式（只显示当前年和未来一年）"""
    data = get_institution_forecast(secucode)
    if not data.get('success') or not data.get('result', {}).get('data'):
        return "# 无数据\n"
    
    items = data['result']['data']
    stock_name = items[0].get('SECURITY_NAME_ABBR', '')
    stock_code = items[0].get('SECURITY_CODE', '')
    
    current_year = datetime.now().year
    next_year = current_year + 1
    
    md = f"# {stock_name}({stock_code}) 机构预测\n\n"
    md += _build_table_header(["发布日期", "机构名称", f"{current_year}年收益", f"{current_year}年市盈率", f"{next_year}年收益", f"{next_year}年市盈率"])
    
    for item in items:
        publish_date = item.get('PUBLISH_DATE', '-')[:10] if item.get('PUBLISH_DATE') else '-'
        org_name = item.get('ORG_NAME_ABBR') or '-'
        eps_current, pe_current = _extract_year_data(item, current_year)
        eps_next, pe_next = _extract_year_data(item, next_year)
        
        md += f"| {publish_date} | {org_name} | {eps_current} | {pe_current} | {eps_next} | {pe_next} |\n"
    
    return md


if __name__ == "__main__":
    import json
    
    secucode = "002371.SZ"
    print(f"正在获取 {secucode} 的机构预测数据...\n")
    
    print("=== 显示所有年份数据（Markdown） ===")
    markdown = get_institution_forecast_to_markdown(secucode)
    print(markdown)
    
    print("\n=== 显示所有年份数据（JSON） ===")
    json_data = get_institution_forecast_to_json(secucode)
    print(json.dumps(json_data, ensure_ascii=False, indent=2))
    
    print("\n=== 只显示当前年和未来一年数据（Markdown） ===")
    markdown_filtered = get_institution_forecast_current_next_year_to_markdown(secucode)
    print(markdown_filtered)
    
    print("\n=== 只显示当前年和未来一年数据（JSON） ===")
    json_filtered = get_institution_forecast_current_next_year_to_json(secucode)
    print(json.dumps(json_filtered, ensure_ascii=False, indent=2))
