"""
千里马API签名工具 - Python实现
对应Java版本的SignatureUtils
"""

import json
import time
import base64
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5

PUBLIC_KEY_STR = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDVT0rCVSoNi/5SEmL5lY6pIsccalZdSbMe0Qv+SYFlwuYVzxMCTIwVOTvOWFXt7EHEIk2ajvUnb3ARc7ALFt1uNVw6+vTPh68CIkqRC9ipuzkt5oQ8Sgv3tSiVSveu6zKNYjE83XTMIwqLBg87TC1j4+ExpmSKUGBA+d85c4tyMwIDAQAB"

def bytes_to_hex(data):
    """字节数组转16进制字符串"""
    return ''.join(['{:02X}'.format(b) for b in data])


def public_key_encrypt(text, public_key_str):
    """RSA公钥加密"""
    # 解码公钥
    public_key_bytes = base64.b64decode(public_key_str)
    public_key = RSA.import_key(public_key_bytes)
    
    # 创建加密器
    cipher = PKCS1_v1_5.new(public_key)
    
    # 分块加密（每块最大117字节）
    input_bytes = text.encode('utf-8')
    max_encrypt_block = 117
    offset = 0
    result_bytes = b''
    
    while offset < len(input_bytes):
        if len(input_bytes) - offset > max_encrypt_block:
            chunk = input_bytes[offset:offset + max_encrypt_block]
            offset += max_encrypt_block
        else:
            chunk = input_bytes[offset:]
            offset = len(input_bytes)
        
        encrypted_chunk = cipher.encrypt(chunk)
        result_bytes += encrypted_chunk
    
    # 转换为16进制字符串
    hex_str = bytes_to_hex(result_bytes)
    
    # Base64编码
    return base64.b64encode(hex_str.encode()).decode()


def get_string(request_param):
    """
    生成签名字符串
    
    Args:
        request_param: 请求参数字典
        
    Returns:
        签名后的Base64字符串
    """
    secret_content = {
        "timeStamp": int(time.time() * 1000),  # 毫秒时间戳
        "requestParam": request_param
    }
    
    secret_content_str = json.dumps(secret_content, separators=(',', ':'), ensure_ascii=False)
    
    return public_key_encrypt(secret_content_str, PUBLIC_KEY_STR)


# 使用示例
if __name__ == "__main__":
    # 示例：千里马搜索接口参数
    request_param = {
        "accountKey": "d115a7b8c93846a38ca601b1ad158b3f",
        "ruleList": [["软件开发"]],
        "pageIndex": 1,
        "pageSize": 40,
        "timeType": 8,
        "searchMode": 1,
        "biddingType": 0,
        "searchRange": 1,
        "infoTypeList": [0, 1, 2]
    }
    
    signature = get_string(request_param)
    print("签名结果:")
    print(signature)
