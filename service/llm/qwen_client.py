import aiohttp
import json
import logging
import asyncio
from typing import Optional, List, Dict, Any, AsyncIterator

logger = logging.getLogger(__name__)


class QwenClient:
    """阿里云百炼 DashScope OpenAI 兼容接口客户端。

    支持模型：
    - qwen3-235b-a22b          : 混合思考模式（通过 enable_thinking 控制）
    - qwen3-235b-a22b-thinking-2507 : 纯思考模式（始终开启深度推理）
    - qwen-plus / qwen-turbo 等商业版模型
    """

    def __init__(self, base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"):
        self.api_key = "sk-fb7059692d5b40719330fbf1ac6d3ac5"  # TODO: 替换为实际的 DashScope API Key
        self.base_url = base_url

    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: str = "qwen3-235b-a22b",
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        stream: bool = False,
        enable_thinking: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """调用 DashScope 聊天接口。

        Args:
            messages: 对话消息列表
            model: 模型名称
            temperature: 温度参数
            max_tokens: 最大输出 token 数
            stream: 是否流式（此方法固定 False）
            enable_thinking: 是否启用深度思考
                - True:  开启思考（reasoning_content 中返回推理过程）
                - False: 关闭思考
                - None:  不传该参数，使用模型默认行为
        """
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": stream,
        }
        if max_tokens:
            payload["max_tokens"] = max_tokens
        if enable_thinking is not None:
            payload["enable_thinking"] = enable_thinking

        # 思考模型延迟较高，给更长超时
        total_timeout = 600 if enable_thinking else 300
        timeout = aiohttp.ClientTimeout(total=total_timeout, connect=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30, keepalive_timeout=30)

        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        result = await response.json()
                        if response.status != 200:
                            error_msg = result.get('error', {}).get('message', '') or str(result)
                            if attempt < 2 and response.status in (429, 500, 502, 503):
                                logger.warning(
                                    "QwenClient.chat HTTP %d (attempt %d): %s",
                                    response.status, attempt + 1, error_msg,
                                )
                                await asyncio.sleep(2 ** attempt)
                                continue
                            raise RuntimeError(f"Qwen API HTTP {response.status}: {error_msg}")
                        if 'choices' not in result:
                            raise RuntimeError(f"Qwen API 响应缺少 choices 字段: {str(result)[:200]}")
                        return result
            except (aiohttp.ClientPayloadError, aiohttp.ClientError, ConnectionResetError) as e:
                if attempt == 2:
                    raise type(e)(
                        f"{type(e).__name__}: {e or '(no message)'} "
                        f"[model={model}, 重试3次均失败]"
                    ) from e
                logger.warning("QwenClient.chat 请求失败 (attempt %d): %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)

    async def chat_stream(
        self,
        messages: List[Dict[str, str]],
        model: str = "qwen3-235b-a22b",
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        enable_thinking: Optional[bool] = None,
    ) -> AsyncIterator[str]:
        """流式调用 DashScope 聊天接口。

        对于思考模型，reasoning_content 会被跳过，只 yield 最终 content。
        """
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": True,
        }
        if max_tokens:
            payload["max_tokens"] = max_tokens
        if enable_thinking is not None:
            payload["enable_thinking"] = enable_thinking

        total_timeout = 600 if enable_thinking else 300
        timeout = aiohttp.ClientTimeout(total=total_timeout, connect=30, sock_read=120)

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
                            status=response.status, message=body,
                        )
                    async for line in response.content:
                        line = line.decode('utf-8').strip()
                        if line.startswith('data: '):
                            data = line[6:]
                            if data == '[DONE]':
                                break
                            try:
                                chunk = json.loads(data)
                                delta = chunk.get('choices', [{}])[0].get('delta', {})
                                # 只输出最终回答内容，跳过 reasoning_content
                                content = delta.get('content', '')
                                if content:
                                    accumulated.append(content)
                                    yield content
                            except json.JSONDecodeError as e:
                                logger.debug("QwenClient.chat_stream JSON解析失败: %s", e)
                                continue
                break
            except (aiohttp.ClientPayloadError, aiohttp.ClientError,
                    ConnectionResetError, asyncio.TimeoutError) as e:
                if attempt == 2:
                    raise type(e)(
                        f"{type(e).__name__}: {e or '(no message)'} "
                        f"[model={model}, 已接收{len(accumulated)}段, 重试3次均失败]"
                    ) from e
                logger.warning(
                    "QwenClient.chat_stream 请求失败 (attempt %d, 已接收%d段): %s",
                    attempt + 1, len(accumulated), e,
                )
                await asyncio.sleep(2 ** attempt)
            finally:
                if session:
                    await session.close()

    @staticmethod
    def extract_reasoning(response: Dict[str, Any]) -> Optional[str]:
        """从非流式响应中提取思考过程（reasoning_content）。

        Args:
            response: chat() 返回的完整响应

        Returns:
            推理过程文本，如果没有则返回 None
        """
        try:
            message = response['choices'][0]['message']
            return message.get('reasoning_content')
        except (KeyError, IndexError):
            return None
