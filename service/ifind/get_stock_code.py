import asyncio

from service.ifind import refresh_token
from get_client_token import THSTokenClient
from smart_stock_picking import SmartStockPicking

async def main():

    # 获取access_token
    client = THSTokenClient(refresh_token)
    token_result = await client.get_access_token()
    
    if token_result.get("errorcode") != 0:
        print(f"获取token失败: {token_result.get('errmsg')}")
        return
    
    access_token = token_result.get("data", {}).get("access_token")
    
    # 调用智能选股接口
    stock_picker = SmartStockPicking(access_token)
    result = await stock_picker.search(searchstring="万科A、隆基绿能、北方华创", searchtype="stock")
    
    print(f"错误码: {result.get('errorcode')}")
    print(f"错误信息: {result.get('errmsg')}")
    print(f"处理时间: {result.get('perf')}ms")
    print(f"数据量: {result.get('dataVol')}")
    print(f"返回数据: {result.get('tables')}")

if __name__ == "__main__":
    asyncio.run(main())
