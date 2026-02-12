import requests

#评级统计
def get_institution_rating(secucode: str) -> dict:
    """
    获取机构评级数据
    
    Args:
        secucode: 证券代码，格式如 "002371.SZ"
    
    Returns:
        dict: 机构评级数据
    """
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    
    params = {
        "reportName": "RPT_HSF10_RES_ORGRATING",
        "columns": "SECUCODE,SECURITY_NAME_ABBR,SECURITY_INNER_CODE,ORG_CODE,SECURITY_TYPE_CODE,TRADE_MARKET_CODE,DATE_TYPE_CODE,DATE_TYPE,COMPRE_RATING_NUM,COMPRE_RATING,RATING_ORG_NUM,RATING_BUY_NUM,RATING_ADD_NUM,RATING_NEUTRAL_NUM,RATING_REDUCE_NUM,RATING_SALE_NUM,SECURITY_CODE",
        "quoteColumns": "",
        "filter": f'(SECUCODE="{secucode}")',
        "pageNumber": 1,
        "pageSize": 200,
        "sortTypes": 1,
        "sortColumns": "DATE_TYPE_CODE",
        "source": "HSF10",
        "client": "PC",
        "v": "08685099710860091"
    }
    
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "no-cache",
        "Origin": "https://emweb.securities.eastmoney.com",
        "Referer": "https://emweb.securities.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }
    
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    
    return response.json()


def format_to_markdown(data: dict) -> str:
    """
    将机构评级数据转换为Markdown格式
    
    Args:
        data: API返回的JSON数据
    
    Returns:
        str: Markdown格式的文本
    """
    if not data.get('success') or not data.get('result', {}).get('data'):
        return "# 无数据\n"
    
    items = data['result']['data']
    stock_name = items[0].get('SECURITY_NAME_ABBR', '')
    stock_code = items[0].get('SECURITY_CODE', '')
    
    md = f"# {stock_name}({stock_code}) 机构评级\n\n"
    md += "| 时间范围 | 综合评级 | 评级分数 | 评级机构数 | 买入 | 增持 | 中性 | 减持 | 卖出 |\n"
    md += "|---------|---------|---------|-----------|-----|-----|-----|-----|-----|\n"
    
    for item in items:
        date_type = item.get('DATE_TYPE', '-')
        rating = item.get('COMPRE_RATING') or '-'
        rating_num = f"{item.get('COMPRE_RATING_NUM', ''):.2f}" if item.get('COMPRE_RATING_NUM') else '-'
        org_num = item.get('RATING_ORG_NUM') or '-'
        buy = item.get('RATING_BUY_NUM') or '-'
        add = item.get('RATING_ADD_NUM') or '-'
        neutral = item.get('RATING_NEUTRAL_NUM') or '-'
        reduce = item.get('RATING_REDUCE_NUM') or '-'
        sale = item.get('RATING_SALE_NUM') or '-'
        
        md += f"| {date_type} | {rating} | {rating_num} | {org_num} | {buy} | {add} | {neutral} | {reduce} | {sale} |\n"
    
    return md


if __name__ == "__main__":
    secucode = "002371.SZ"
    print(f"正在获取 {secucode} 的机构评级数据...\n")
    
    result = get_institution_rating(secucode)
    markdown = format_to_markdown(result)
    
    print(markdown)
