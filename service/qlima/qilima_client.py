"""千里马API客户端"""
import asyncio

import aiohttp
from typing import List, Dict, Any, Optional

from common.utils import signature_utils

# 千里马API配置
QILIMA_SEARCH_URL: str = "https://thirdcommon.qianlima.com/third/open/subscriber/search/v2"
QILIMA_DETAIL_URL: str = "https://thirdcommon.qianlima.com/third/open/detail/info"
QILIMA_ACCOUNT_KEY: str = "d115a7b8c93846a38ca601b1ad158b3f"
QILIMA_SECRET_USER: str = "xsy"

class QilimaClient:
    """千里马API客户端"""
    
    def __init__(self):
        self.search_url = QILIMA_SEARCH_URL
        self.detail_url = QILIMA_DETAIL_URL
        self.account_key = QILIMA_ACCOUNT_KEY
        self.secret_user = QILIMA_SECRET_USER
    
    async def search(
        self,
        search_key: str,
        more_keys: List[str] = None,
        page_no: int = 1
    ) -> List[int]:
        """搜索招标信息，返回id列表"""

        try:
            rule_list = []
            if search_key:
                rule_list.append([search_key])
            if more_keys:
                rule_list.extend(more_keys)
            
            params = {
                "accountKey": self.account_key,
                #"searchKey": search_key,
                "searchRange": 1,
                "pageIndex": page_no,
                "pageSize": 10,
                "timeType": 8,
                "searchMode": 1,
                "biddingType": 0,
                "ruleList": [[search_key]],
                "infoTypeList": [0, 1, 2]
            }
            
            headers = {
                "secretUser": self.secret_user,
                "secretContent": signature_utils.get_string(params),
                "Content-Type": "application/json"
            }
            
            timeout = aiohttp.ClientTimeout(total=30.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self.search_url,
                    headers=headers,
                    json=params
                ) as response:
                    response.raise_for_status()
                    result = await response.json()
                
                # 解析结果 - 从data.list获取id列表
                data = result.get("data", {})
                items = data.get("list", [])
                
                return [item.get("dataId") for item in items if item.get("dataId")]
        except Exception as e:
            print(f"千里马搜索API调用失败: {e}")
            return []
    
    async def get_detail(self, data_id: int) -> Optional[Dict[str, Any]]:
        """获取招标详情"""
        try:
            params = {
                "accountKey": self.account_key,
                "dataId": data_id
            }
            
            headers = {
                "secretUser": self.secret_user,
                "secretContent": signature_utils.get_string(params),
                "Content-Type": "application/json"
            }
            
            timeout = aiohttp.ClientTimeout(total=30.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self.detail_url,
                    headers=headers,
                    json=params
                ) as response:
                    response.raise_for_status()
                    response_json = await response.json()
                
                data = response_json.get("data")
                if not data:
                    return {}

                from datetime import datetime
                return_json = {}
                return_json["招标单位"] = data.get("zhaoBiaoUnit", "")

                publish_time = data.get("publishTime")
                if publish_time:
                    date_str = datetime.fromtimestamp(publish_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    return_json["标书发布时间"] = date_str
                else:
                    return_json["标书发布时间"] = "未指定"
                
                registration_end_time = data.get("registrationEndTime")
                if registration_end_time:
                    date_str = datetime.fromtimestamp(registration_end_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    return_json["标书截止时间"] = date_str
                else:
                    return_json["标书截止时间"] = "未指定"

                return_json["标书标题"] = data.get("title", "")
                return_json["联系人"] = data.get("zhaoRelationName", "")
                return_json["联系方式"] = data.get("zhaoRelationWay", "")
                return_json["招标地区"] = data.get("area", "")
                return_json["标的词"] = data.get("bdKeywords", "")
                #return_json["详细内容"] = data.get("content", "")

                return return_json
        except Exception as e:
            print(f"千里马详情API调用失败: {e}")
            return None


async def demo_qilima_search():
    """千里马搜索API demo"""
    client = QilimaClient()

    # {'name': '万华过程控制级音叉物位开关框架', 'number': 'WHYT/B-D03-2025-IA061-01-01', ', {'name': '电能质量分析仪框架
    #  ', 'number': 'WHYT / B - D03 - 2025 - IA089 - 01 - 01', 'organization': '万华化学集团物资有限公司'}, {'name
    #  ': '江苏美能RO膜元件采购S203 - CG - 28 - 01 - 042', 'organization': '江苏美能膜材料科技有限公司'}]

    # 测试查询
    search_key = "北方华创"
    print(f"🔍 千里马搜索: {search_key}")

    # 执行搜索
    results = await client.search(search_key=search_key, more_keys=["WHYT/B-D03-2025-IA061-01-01"])

    # 输出结果
    print(f"\n📊 找到 {len(results)} 个招标项目id:")
    for i, result_id in enumerate(results[:5], 1):  # 显示前5个id
        print(f"{i}. ID: {result_id}")

    # 测试获取详情
    if results:
        print(f"\n🔍 获取第一个项目详情 (ID: {results[0]}):")
        detail = await client.get_detail(results[0])
        if detail:
            print(f"   详情数据: {str(detail)}...")
        else:
            print("   获取详情失败")


if __name__ == "__main__":
    asyncio.run(demo_qilima_search())