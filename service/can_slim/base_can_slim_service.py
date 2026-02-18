"""CAN SLIM分析服务基类"""
import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any

from common.utils.stock_info_utils import StockInfo
from service.llm.deepseek_client import DeepSeekClient


class BaseCanSlimService(ABC):
    """CAN SLIM分析服务基类"""
    
    def __init__(self, stock_info: StockInfo):
        self.stock_info = stock_info
        self.data_cache: Dict[str, Any] = {}
    
    @abstractmethod
    async def collect_data(self) -> Dict[str, Any]:
        """收集分析所需的数据"""
        pass
    
    @abstractmethod
    def get_prompt_template(self) -> str:
        """获取提示词模板"""
        pass
    
    @abstractmethod
    def get_prompt_params(self) -> Dict[str, Any]:
        """获取提示词参数"""
        pass
    
    async def process_data(self) -> None:
        """处理数据（可选，子类可覆盖）"""
        pass
    
    def build_prompt(self) -> str:
        """构建提示词"""
        template = self.get_prompt_template()
        params = self.get_prompt_params()
        
        # 添加通用参数
        params.update({
            'current_date': datetime.now().strftime('%Y-%m-%d'),
            'stock_name': self.stock_info.stock_name,
            'stock_code': self.stock_info.stock_code_normalize
        })
        
        return template.format(**params)
    
    async def execute(self, deep_thinking: bool = False) -> str:
        """执行分析流程"""
        # 1. 收集数据
        self.data_cache = await self.collect_data()
        
        # 2. 处理数据
        await self.process_data()
        
        # 3. 构建提示词
        prompt = self.build_prompt()
        print(prompt)
        print("\n =============================== \n")
        
        # 4. 调用LLM
        model = "deepseek-reasoner" if deep_thinking else "deepseek-chat"
        client = DeepSeekClient()
        
        result = ""
        async for content in client.chat_stream(
            messages=[{"role": "user", "content": prompt}],
            model=model
        ):
            result += content
        
        return result
    
    def to_json(self, data: Any) -> str:
        """将数据转换为JSON字符串"""
        return json.dumps(data, ensure_ascii=False, indent=2)
