import aiohttp
from typing import Optional, List, Dict, Any
import asyncio
from common.utils.pdf_parser import PDFParser


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
    def parse_result(result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """转换为列表形式，每个元素为一条公告记录"""
        field_mapping = AnnouncementQuery.get_field_mapping()
        
        if not result.get("tables") or len(result["tables"]) == 0:
            return []
        
        table_data = result["tables"][0].get("table", {})
        if not table_data:
            return []
        
        keys = list(table_data.keys())
        length = len(table_data[keys[0]]) if keys else 0
        
        return [
            {field_mapping.get(k, k): table_data[k][i] for k in keys}
            for i in range(length)
        ]

    @staticmethod
    async def parse_result_with_pdf(result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """转换为列表形式，并将PDF链接转换为文本"""
        records = AnnouncementQuery.parse_result(result)
        
        async def process_record(index: int, record: Dict[str, Any]):
            if "公告链接" in record:
                print(f"正在处理第 {index + 1} 条")
                pdf_text = await PDFParser.download_and_parse(record["公告链接"])
                if pdf_text:
                    record["公告内容"] = pdf_text
                print(f"第 {index + 1} 条处理完成")
        
        await asyncio.gather(*[process_record(i, record) for i, record in enumerate(records)])
        return records

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
            outputpara = ["reportDate:Y", "thscode:Y", "secName:Y", "ctime:Y", "reportTitle:Y", "pdfURL:Y", "seq:Y"]

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
