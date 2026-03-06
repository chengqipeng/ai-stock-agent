import json
import re


def parse_llm_json(content: str):
    """解析 LLM 返回的 JSON 内容，兼容 markdown 代码块包裹的情况。

    Args:
        content: LLM 返回的原始字符串

    Returns:
        解析后的 Python 对象

    Raises:
        ValueError: content 为空
        json.JSONDecodeError: JSON 解析失败
    """
    content = (content or "").strip()
    if not content:
        raise ValueError("LLM returned empty content")

    # 剥离 ```json ... ``` 或 ``` ... ``` 代码块
    match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", content)
    if match:
        content = match.group(1).strip()

    # strict=False 允许字符串内的控制字符（\n, \t 等）
    try:
        return json.loads(content, strict=False)
    except json.JSONDecodeError:
        # 移除非法控制字符后重试
        cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', content)
        return json.loads(cleaned, strict=False)
