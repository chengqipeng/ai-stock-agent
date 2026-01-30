import aiohttp
from typing import Dict, Any, Optional

class EastmoneyHolderService:
    def __init__(self):
        self.base_url = "https://data.eastmoney.com/dataapi/zlsj/detail"

    async def get_holder_detail(
        self,
        scode: str,
        report_date: str = "2025-12-31",
        page_num: int = 1,
        page_size: int = 30,
        sh_type: str = "",
        sh_code: str = "",
        sort_field: str = "HOLDER_CODE",
        sort_direc: int = 1
    ) -> Dict[str, Any]:
        """
        获取股票主力持仓明细
        
        Args:
            scode: 股票代码，如 "002371"
            report_date: 报告日期，如 "2025-12-31"
            page_num: 页码
            page_size: 每页数量
            sh_type: 股东类型
            sh_code: 股东代码
            sort_field: 排序字段
            sort_direc: 排序方向 1升序 -1降序
        
        Returns:
            API响应结果
        """
        params = {
            "SHType": sh_type,
            "SHCode": sh_code,
            "SCode": scode,
            "ReportDate": report_date,
            "sortField": sort_field,
            "sortDirec": sort_direc,
            "pageNum": page_num,
            "pageSize": page_size
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=params) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"请求失败: {response.status}, 响应: {text}")
                
                return await response.json()
