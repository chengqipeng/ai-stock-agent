import requests
from typing import Optional


def get_billboard_daily_details(trade_date: str, security_code: str, page_number: int = 1, page_size: int = 50) -> Optional[dict]:
    """
    获取龙虎榜每日明细买入数据
    
    Args:
        trade_date: 交易日期，格式：YYYY-MM-DD
        security_code: 股票代码
        page_number: 页码，默认1
        page_size: 每页数量，默认50
    
    Returns:
        返回API响应数据，失败返回None
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    
    params = {
        "reportName": "RPT_BILLBOARD_DAILYDETAILSBUY",
        "columns": "ALL",
        "filter": f"(TRADE_DATE='{trade_date}')(SECURITY_CODE=\"{security_code}\")",
        "pageNumber": page_number,
        "pageSize": page_size,
        "sortTypes": "-1",
        "sortColumns": "BUY",
        "source": "WEB",
        "client": "WEB"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "Referer": f"https://data.eastmoney.com/stock/lhb/{security_code}.html"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"请求失败: {e}")
        return None


if __name__ == "__main__":
    result = get_billboard_daily_details("2025-09-13", "002008")
    if result:
        print(result)
    else:
        print("获取数据失败")
