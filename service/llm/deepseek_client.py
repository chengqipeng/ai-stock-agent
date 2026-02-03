import aiohttp
import json
from typing import Optional, List, Dict, Any

class DeepSeekClient:
    def __init__(self, base_url: str = "https://api.deepseek.com/v1"):
        self.api_key = "sk-9f61d63fe121482783efc7c11a9b2239"
        self.base_url = base_url
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: str = "deepseek-chat",
        temperature: float = 1.0,
        max_tokens: Optional[int] = None,
        stream: bool = False
    ) -> Dict[str, Any]:
        """调用DeepSeek聊天接口"""
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": stream
        }
        if max_tokens:
            payload["max_tokens"] = max_tokens
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                return await response.json()
