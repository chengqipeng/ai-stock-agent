from service.gemini.stream_generate import GeminiService
from service.gemini.gemini_parser import parse_gemini_stream_response

DEFAULT_PROXY = "http://127.0.0.1:7890"


async def gemini_generate(prompt: str, proxy: str = DEFAULT_PROXY, timeout: int = 45) -> str:
    service = GeminiService(
        proxy=proxy,
        timeout=timeout,
        trust_env=False
    )
    
    raw_result = await service.stream_generate(prompt=prompt)
    result = parse_gemini_stream_response(raw_result)
    
    return result
