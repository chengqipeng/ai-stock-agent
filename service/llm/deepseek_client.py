import aiohttp
import json
import logging
import asyncio
from typing import Optional, List, Dict, Any, AsyncIterator

logger = logging.getLogger(__name__)

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
        
        timeout = aiohttp.ClientTimeout(total=300, connect=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30, keepalive_timeout=30)
        
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        return await response.json()
            except (aiohttp.ClientPayloadError, aiohttp.ClientError, ConnectionResetError) as e:
                if attempt == 2:
                    raise e
                logger.warning("DeepSeekClient.chat 请求失败 (attempt %d): %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)
    
    async def chat_stream(
        self,
        messages: List[Dict[str, str]],
        model: str = "deepseek-chat",
        temperature: float = 1.0,
        max_tokens: Optional[int] = None
    ) -> AsyncIterator[str]:
        """流式调用DeepSeek聊天接口"""
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

        timeout = aiohttp.ClientTimeout(total=600, connect=30, sock_read=120)

        for attempt in range(3):
            session = None
            accumulated = []
            try:
                session = aiohttp.ClientSession(timeout=timeout)
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status != 200:
                        body = await response.text()
                        raise aiohttp.ClientResponseError(
                            response.request_info, response.history,
                            status=response.status, message=body
                        )
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
                                    accumulated.append(content)
                                    yield content
                            except json.JSONDecodeError as e:
                                logger.debug("DeepSeekClient.chat_stream JSON解析失败: %s", e)
                                continue
                break
            except (aiohttp.ClientPayloadError, aiohttp.ClientError, 
                    ConnectionResetError, asyncio.TimeoutError) as e:
                if attempt == 2:
                    raise e
                logger.warning(
                    "DeepSeekClient.chat_stream 请求失败 (attempt %d, 已接收%d段): %s", 
                    attempt + 1, len(accumulated), e
                )
                await asyncio.sleep(2 ** attempt)
            finally:
                if session:
                    await session.close()
