import json
import re


def parse_gemini_stream_response(stream_data: str) -> str:
    """
    解析Gemini Pro接口返回的流式数据,提取完整的响应文本
    
    Args:
        stream_data: 原始流式响应数据
        
    Returns:
        str: 提取的完整文本内容
    """
    lines = stream_data.strip().split('\n')
    
    # 获取倒数第9行
    if len(lines) < 9:
        return ""
    
    target_line = lines[-9].strip()
    
    try:
        # 解析JSON
        data = json.loads(target_line)
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) >= 3:
            json_str = data[0][2]
            if json_str:
                inner_data = json.loads(json_str)
                # inner_data[4][0][1] 是答案文本
                if isinstance(inner_data, list) and len(inner_data) >= 5:
                    content = inner_data[4]
                    if isinstance(content, list) and len(content) > 0:
                        first_item = content[0]
                        if isinstance(first_item, list) and len(first_item) >= 2:
                            text = first_item[1]
                            if isinstance(text, str):
                                return text
                            elif isinstance(text, list):
                                return "\n\n".join(text)
                            return ""
    except Exception as e:
        print(f"解析错误: {e}")
    
    return ""


