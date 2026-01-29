import aiohttp
from typing import List, Dict, Any, Optional

class DateSequenceService:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = "https://quantapi.51ifind.com/api/v1/date_sequence"

    async def get_date_sequence(
        self, 
        codes: List[str], 
        indipara: List[Dict[str, Any]],
        startdate: str,
        enddate: str,
        functionpara: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        获取日期序列数据
        
        Args:
            codes: 股票代码列表，如 ["300033.SZ", "600030.SH"]
            indipara: 指标参数列表，格式如:
                [
                    {
                        "indicator": "ths_roe_stock",
                        "indiparams": ["20241231"]
                    }
                ]
            startdate: 开始日期，支持 "YYYYMMDD"/"YYYY-MM-DD"/"YYYY/MM/DD"
            enddate: 结束日期，支持 "YYYYMMDD"/"YYYY-MM-DD"/"YYYY/MM/DD"
            functionpara: 可选参数，格式如:
                {
                    "Interval": "D",  # D-日 W-周 M-月 Q-季 S-半年 Y-年
                    "Days": "Tradedays",  # Tradedays-交易日 Alldays-日历日
                    "Fill": "Previous"  # Previous-沿用之前数据 Blank-空值
                }
        
        Returns:
            API响应结果
        """
        form_data = {
            "codes": ",".join(codes),
            "indipara": indipara,
            "startdate": startdate,
            "enddate": enddate
        }
        
        if functionpara:
            form_data["functionpara"] = functionpara

        headers = {
            "access_token": self.access_token,
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.base_url,
                json=form_data,
                headers=headers
            ) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"请求失败: {response.status}, 响应内容: {text}")

                result = await response.json(content_type=None)
                return result
