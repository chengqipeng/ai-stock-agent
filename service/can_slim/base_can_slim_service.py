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
    
    def build_prompt(self, use_score_output: bool = False) -> str:
        """构建提示词
        
        Args:
            use_score_output: 是否使用打分输出格式（True）还是完整分析输出格式（False）
        """
        template = self.get_prompt_template()
        params = self.get_prompt_params()
        
        # 添加通用参数
        params.update({
            'system_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'current_date': datetime.now().strftime('%Y-%m-%d'),
            'stock_name': self.stock_info.stock_name,
            'stock_code': self.stock_info.stock_code_normalize
        })
        
        prompt = template.format(**params)
        return self.append_final_output(prompt, use_score_output)
    
    async def execute(self, deep_thinking: bool = False) -> str:
        """执行分析流程"""
        # 1. 收集数据
        self.data_cache = await self.collect_data()
        
        # 2. 处理数据
        await self.process_data()
        
        # 3. 构建提示词（使用完整分析输出）
        prompt = self.build_prompt(use_score_output=False)
        
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
    
    def get_final_output_instruction(self) -> str:
        """获取最终输出指令（子类可覆盖以提供特定维度的输出要求）"""
        return ""
    
    def get_dimension_name(self) -> str:
        """获取维度名称（子类必须实现）"""
        return self.__class__.__name__[0]  # 默认返回类名的第一个字符
    
    def append_final_output(self, prompt: str, use_score_output: bool = False) -> str:
        """在提示词末尾追加最终输出指令
        
        Args:
            prompt: 原始提示词
            use_score_output: True使用SCORE_OUTPUT（打分），False使用维度特定的COMPLETION_OUTPUT（完整分析）
        """
        dim = self.get_dimension_name().upper()
        try:
            from common.constants import can_slim_final_outputs
            if use_score_output:
                score_output = getattr(can_slim_final_outputs, f"{dim}_SCORE_OUTPUT", None)
                if score_output:
                    return f"{prompt}\n\n{score_output}"
            else:
                completion_output = getattr(can_slim_final_outputs, f"{dim}_COMPLETION_OUTPUT", None)
                if completion_output:
                    return f"{prompt}\n\n{completion_output}"
        except (ImportError, AttributeError) as e:
            print(e)
        return prompt
