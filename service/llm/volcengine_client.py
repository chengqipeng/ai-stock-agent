import aiohttp
import json
import base64
import logging
import asyncio
from typing import Optional, List, Dict, Any, AsyncIterator

logger = logging.getLogger(__name__)

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
        stream: bool = False,
        thinking: bool = False
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
            "stream": stream,
            "thinking": {
                "type": "enabled" if thinking else "disabled"
            }
        }
        if max_tokens:
            payload["max_tokens"] = max_tokens
        
        timeout = aiohttp.ClientTimeout(total=120, connect=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30, keepalive_timeout=30)
        
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        result = await response.json()
                        if response.status != 200:
                            error_msg = result.get('error', {}).get('message', '') or str(result)
                            if attempt < 2 and response.status in (429, 500, 502, 503):
                                logger.warning("VolcengineClient.chat HTTP %d (attempt %d): %s", response.status, attempt + 1, error_msg)
                                await asyncio.sleep(2 ** attempt)
                                continue
                            raise RuntimeError(f"LLM API HTTP {response.status}: {error_msg}")
                        if 'choices' not in result:
                            raise RuntimeError(f"LLM API 响应缺少 choices 字段: {str(result)[:200]}")
                        return result
            except (aiohttp.ClientPayloadError, aiohttp.ClientError, ConnectionResetError) as e:
                if attempt == 2:
                    raise e
                logger.warning("VolcengineClient.chat 请求失败 (attempt %d): %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)
    
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
        
        timeout = aiohttp.ClientTimeout(total=120, connect=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30, keepalive_timeout=30)
        
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
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
                                except json.JSONDecodeError as e:
                                    logger.debug("VolcengineClient.chat_stream JSON解析失败: %s", e)
                                    continue
                break
            except (aiohttp.ClientPayloadError, aiohttp.ClientError, ConnectionResetError) as e:
                if attempt == 2:
                    raise e
                logger.warning("VolcengineClient.chat_stream 请求失败 (attempt %d): %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)
