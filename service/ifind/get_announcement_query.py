import aiohttp
from typing import Optional, List, Dict, Any


class AnnouncementQuery:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = "https://quantapi.51ifind.com/api/v1/report_query"

    @staticmethod
    def get_field_mapping() -> Dict[str, str]:
        return {
            "reportDate": "公告日期",
            "thscode": "证券代码",
            "secName": "证券简称",
            "ctime": "发布时间",
            "reportTitle": "公告标题",
            "pdfURL": "公告链接",
            "seq": "唯一标号"
        }

    @staticmethod
    def convert_codes(codes: List[str]) -> str:
        """将股票代码列表转换为API所需格式"""
        return ",".join(codes)

    @staticmethod
    def parse_tables(data: List[Dict[str, Any]], field_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """将字段名转换为中文"""
        return [{field_mapping.get(k, k): v for k, v in row.items()} for row in data]

    async def query(
        self,
        codes: Optional[List[str]] = None,
        functionpara: Optional[Dict[str, Any]] = None,
        outputpara: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        查询公告信息
        
        Args:
            codes: 证券代码列表，如 ["300033.SZ", "600030.SH"]
            functionpara: 查询参数字典，支持的key:
                - mode: 提取方式，如 "allAStock"-全部A股
                - reportType: 公告类型，如 "903"-全部
                - beginrDate: 公告开始日期，如 "2024-09-10"
                - endrDate: 公告截止日期
                - begincTime: 发布开始时间
                - endcTime: 发布截止时间
                - beginSeq: 开始seq
                - endSeq: 截止seq
                - keyWord: 标题关键词
            outputpara: 输出指标列表，如 ["reportDate", "thscode", "secName", "ctime", "reportTitle", "pdfURL", "seq"]
        
        Returns:
            包含查询结果的字典
        """
        if outputpara is None:
            outputpara = ["reportDate", "thscode", "secName", "ctime", "reportTitle", "pdfURL", "seq"]

        data = {
            "codes": self.convert_codes(codes) if codes else "",
            "functionpara": functionpara or {},
            "outputpara": ",".join(outputpara)
        }

        headers = {
            "access_token": self.access_token,
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(self.base_url, json=data, headers=headers) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"请求失败: {response.status}, 响应内容: {text}")
                
                result = await response.json(content_type=None)
                return result
