import aiohttp
import json
import base64
from typing import Optional, List, Dict, Any, AsyncIterator

VOL_API_KEY = "YjZlY2QxZGEtYTA3Yi00YTI2LThjNTgtY2M1OGViMmU1YTk3"

class VolcengineClient:
    def __init__(self, base_url: str = "https://ark.cn-beijing.volces.com/api/v3"):
        self.api_key = base64.b64decode(VOL_API_KEY).decode('utf-8')
        self.base_url = base_url
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: str = "doubao-seed-1-6-flash-250828",
        temperature: float = 1.0,
        max_tokens: Optional[int] = None,
        stream: bool = False
    ) -> Dict[str, Any]:
        """调用火山引擎聊天接口"""
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
    
    async def chat_stream(
        self,
        messages: List[Dict[str, str]],
        model: str = "doubao-seed-1-6-flash-250828",
        temperature: float = 1.0,
        max_tokens: Optional[int] = None
    ) -> AsyncIterator[str]:
        """流式调用火山引擎聊天接口"""
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": True
        }
        if max_tokens:
            payload["max_tokens"] = max_tokens
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                async for line in response.content:
                    line = line.decode('utf-8').strip()
                    if line.startswith('data: '):
                        data = line[6:]
                        if data == '[DONE]':
                            break
                        try:
                            chunk = json.loads(data)
                            content = chunk.get('choices', [{}])[0].get('delta', {}).get('content', '')
                            if content:
                                yield content
                        except json.JSONDecodeError:
                            continue
