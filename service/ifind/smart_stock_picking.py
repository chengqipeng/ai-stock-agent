import aiohttp
from typing import Dict, Any

class SmartStockPicking:
    """智能选股接口"""
    
    def __init__(self, access_token: str):
        self.url = "https://quantapi.51ifind.com/api/v1/smart_stock_picking"
        self.access_token = access_token
    
    async def search(self, searchstring: str, searchtype: str = "stock") -> Dict[str, Any]:
        """
        智能选股搜索
        
        Args:
            searchstring: 搜索关键词，如"个股热度"
            searchtype: 搜索类别，默认"stock"
            
        Returns:
            Dict包含:
                - errorcode: 错误码，0表示正常
                - errmsg: 错误信息
                - tables: 返回数据结构体
                - datatype: 指标格式
                - inputParams: 输入参数
                - perf: 处理时间(ms)
                - dataVol: 数据量
        """
        import json
        
        headers = {
            "access_token": self.access_token,
            "Content-Type": "application/json"
        }
        
        data = json.dumps({
            "searchstring": searchstring,
            "searchtype": searchtype
        })
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, headers=headers, data=data) as response:
                text = await response.text()
                return json.loads(text)
