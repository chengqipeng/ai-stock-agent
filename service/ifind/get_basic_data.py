import aiohttp
from typing import List, Dict, Any

class BasicDataService:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = "https://quantapi.51ifind.com/api/v1/basic_data_service"

    async def get_basic_data(self, codes: List[str], indipara: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        获取基础数据
        
        Args:
            codes: 股票代码列表，如 ["300033.SZ", "600030.SH"]
            indipara: 指标参数列表，格式如:
                [
                    {
                        "indicator": "roe",
                        "indiparams": {"reportdate": "2024"},
                        "otherparams": {"sys": ["reportdate"]}
                    }
                ]
        
        Returns:
            API响应结果
        """
        form_data = {
            "codes": ",".join(codes),
            "indipara": indipara
        }

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

    @staticmethod
    def parse_tables(tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """解析返回的tables数据"""
        result = []
        if not tables:
            return result
        
        for table_item in tables:
            table_data = table_item.get("table", {})
            ths_code = table_data.get("thscode", [])
            
            for key, values in table_data.items():
                if key == "thscode":
                    continue
                for i, code in enumerate(ths_code):
                    if i >= len(result):
                        result.append({"code": code})
                    result[i][key] = values[i] if i < len(values) else None
        
        return result
