from urllib.parse import quote

def encode_like_big_prompt(text):
    """与big_prompt.py相同的编码方式：URL编码但保留特定字符"""
    safe = '!&()*-.0123456789=ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz'
    return quote(text, safe=safe)

