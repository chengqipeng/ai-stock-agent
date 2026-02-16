from common.http.http_utils import fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo
from common.utils.cache_utils import get_cache_path, load_cache, save_cache

async def get_stock_industry_ranking(stock_info: StockInfo, page: int = 1):
    """获取股票所属行业排名数据
    
    Args:
        stock_info: 股票信息对象
        page: 页码，默认为1
    
    Returns:
        dict: 行业排名数据
    """
    cache_path = get_cache_path("industry_ranking", stock_info.stock_code)
    
    # 检查缓存
    cached_data = load_cache(cache_path)
    if cached_data:
        return cached_data
    
    # 获取数据
    url = "https://push2.eastmoney.com/api/qt/slist/get"
    params = {
        "fltt": "1",
        "invt": "2",
        "fields": "f12,f13,f14,f20,f58,f45,f132,f9,f152,f23,f49,f131,f137,f133,f134,f135,f129,f37,f1000,f3000,f2000",
        "secid": stock_info.secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "pn": str(page),
        "np": "1",
        "spt": "1",
        "wbp2u": "|0|0|0|web"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://quote.eastmoney.com/")
    if data.get("data"):
        # 保存缓存
        save_cache(cache_path, data["data"])
        return data["data"]
    else:
        raise Exception(f"未获取到股票 {stock_info.secid} 的行业排名数据")


async def get_stock_industry_ranking_json(stock_info: StockInfo, page: int = 1):
    """获取股票所属行业排名数据（格式化表格）
    
    Args:
        stock_info: 股票信息对象
        page: 页码，默认为1
    
    Returns:
        dict: 格式化的行业排名对比数据
    """
    from common.utils.amount_utils import convert_amount_unit
    
    data = await get_stock_industry_ranking(stock_info, page)
    items = data.get("diff", [])
    if len(items) < 2:
        raise Exception("数据不完整")
    
    stock_data = items[0]
    industry_data = items[1]
    total_count = industry_data.get('f134', 0)
    
    result = {
        "股票名称": stock_data.get('f14', '-'),
        "行业名称": industry_data.get('f14', '-'),
        "指标对比": {
            "总市值": {
                "股票值": convert_amount_unit(stock_data.get('f20')),
                "行业平均": convert_amount_unit(industry_data.get('f2020')),
                "行业排名": f"{stock_data.get('f1020', '-')}|{total_count}"
            },
            "净资产": {
                "股票值": convert_amount_unit(stock_data.get('f58')),
                "行业平均": convert_amount_unit(industry_data.get('f2135')),
                "行业排名": f"{stock_data.get('f1113', '-')}|{total_count}"
            },
            "净利润": {
                "股票值": convert_amount_unit(stock_data.get('f45')),
                "行业平均": convert_amount_unit(industry_data.get('f2045')),
                "行业排名": f"{stock_data.get('f1045', '-')}|{total_count}"
            },
            "市盈率(动)": {
                "股票值": round(stock_data.get('f9', 0) / 100, 2) if stock_data.get('f9') else '-',
                "行业平均": round(industry_data.get('f2009', 0), 2) if industry_data.get('f2009') else '-',
                "行业排名": f"{stock_data.get('f1009', '-')}|{total_count}"
            },
            "市净率": {
                "股票值": round(stock_data.get('f23', 0) / 100, 2) if stock_data.get('f23') else '-',
                "行业平均": round(industry_data.get('f2023', 0), 2) if industry_data.get('f2023') else '-',
                "行业排名": f"{stock_data.get('f1023', '-')}|{total_count}"
            },
            "毛利率": {
                "股票值": f"{round(stock_data.get('f49', 0), 2)}%" if stock_data.get('f49') else '-',
                "行业平均": f"{round(industry_data.get('f2049', 0), 2)}%" if industry_data.get('f2049') else '-',
                "行业排名": f"{stock_data.get('f1049', '-')}|{total_count}"
            },
            "净利率": {
                "股票值": f"{round(stock_data.get('f129', 0), 2)}%" if stock_data.get('f129') else '-',
                "行业平均": f"{round(industry_data.get('f2129', 0), 2)}%" if industry_data.get('f2129') else '-',
                "行业排名": f"{stock_data.get('f1129', '-')}|{total_count}"
            },
            "ROE": {
                "股票值": f"{round(stock_data.get('f37', 0), 2)}%" if stock_data.get('f37') else '-',
                "行业平均": f"{round(industry_data.get('f2037', 0), 2)}%" if industry_data.get('f2037') else '-',
                "行业排名": f"{stock_data.get('f1037', '-')}|{total_count}"
            }
        }
    }
    
    return result


if __name__ == "__main__":
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name
    
    async def main():
        stock_info = get_stock_info_by_name("北方华创")
        result = await get_stock_industry_ranking_json(stock_info)
        print(result)
    
    asyncio.run(main())
