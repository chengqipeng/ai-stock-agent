import aiohttp
from typing import Optional, List, Dict, Any

class GeminiClient:
    def __init__(self, base_url: str = "https://api2.aigcbest.top/v1"):
        self.api_key = "sk-F6CFwjNNJPotsZqZkEVaws1d4VGUTjg7KlZEJe5dbPmFCFOb"
        self.base_url = base_url
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: str = "gemini-3-pro-all",
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        stream: bool = False
    ) -> Dict[str, Any]:
        """调用Gemini聊天接口"""
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
            timeout = aiohttp.ClientTimeout(total=60)
            async with session.post(url, headers=headers, json=payload, timeout=timeout) as response:
                return await response.json()
