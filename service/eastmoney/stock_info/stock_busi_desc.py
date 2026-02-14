import asyncio
from common.http.http_utils import fetch_eastmoney_api
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name


async def get_stock_core_theme(stock_info: StockInfo):
    """获取股票核心主题"""
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_F10_CORETHEME_CONTENT",
        "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,KEYWORD,KEY_CLASSIF,KEY_CLASSIF_CODE,IS_POINT,IS_HISTORY",
        "quoteColumns": "",
        "filter": f'(SECUCODE="{stock_info.stock_code_normalize}")(IS_POINT="1")(IS_HISTORY="0")',
        "pageNumber": "1",
        "pageSize": "",
        "sortTypes": "1,1",
        "sortColumns": "KEY_CLASSIF_CODE,MAINPOINT",
        "source": "HSF10",
        "client": "PC",
        "v": "012117110637238293"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://emweb.securities.eastmoney.com/")
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        raise Exception(f"未获取到股票 {stock_info.stock_code_normalize} 的核心主题数据")


async def get_stock_board_type(stock_info: StockInfo):
    """获取股票板块类型"""
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_F10_CORETHEME_BOARDTYPE",
        "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,NEW_BOARD_CODE,BOARD_NAME,SELECTED_BOARD_REASON,IS_PRECISE,BOARD_RANK,BOARD_YIELD,DERIVE_BOARD_CODE",
        "quoteColumns": "f3~05~NEW_BOARD_CODE~BOARD_YIELD",
        "filter": f'(SECUCODE="{stock_info.stock_code_normalize}")(IS_PRECISE="1")',
        "pageNumber": "1",
        "pageSize": "",
        "sortTypes": "1",
        "sortColumns": "BOARD_RANK",
        "source": "HSF10",
        "client": "PC",
        "v": "0732758307065009"
    }
    data = await fetch_eastmoney_api(url, params, referer="https://emweb.securities.eastmoney.com/")
    if data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    else:
        raise Exception(f"未获取到股票 {stock_info.stock_code_normalize} 的板块类型数据")


if __name__ == "__main__":
    stock_info: StockInfo = get_stock_info_by_name("北方华创")
    result = asyncio.run(get_stock_board_type(stock_info))
    print(result)
