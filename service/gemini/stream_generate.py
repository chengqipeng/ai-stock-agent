import aiohttp
from typing import Dict, Any
from urllib.parse import quote
import time

class GeminiService:
    def __init__(self, cookies: str, proxy: str = None, timeout: int = 60, trust_env: bool = True):
        self.cookies = cookies
        self.proxy = proxy
        self.timeout = timeout
        self.trust_env = trust_env

    async def stream_generate(self, prompt: str, timestamp: str = None) -> Dict[str, Any]:
        if timestamp is None:
            timestamp = str(int(time.time() * 1000))
        url = "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate?bl=boq_assistant-bard-web-server_20260202.09_p1&f.sid=6488172965349433713&hl=zh-CN&_reqid=3161907&rt=c"
        
        encoded_prompt = quote(prompt, safe='')
        data = f'f.req=%5Bnull%2C%22%5B%5B%5C%22{encoded_prompt}%5C%22%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2C0%5D%2C%5B%5C%22zh-CN%5C%22%5D%2C%5B%5C%22%5C%22%2C%5C%22%5C%22%2C%5C%22%5C%22%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5C%22%5C%22%5D%2C%5C%22%214uGl4bnNAAYFnR50JZ1C6qnaHvT5mwI7ADQBEArZ1AJm4TKYiXJpy3LOtBthDoL0jhQjiTCNNJ4x0-U5-Y99iwo5jJ_43tEGGkq6erR0AgAAAKNSAAAABWgBB34AQUS3Egd9EBPAlgePd5vs6cLYytouBh7_515_Qz8aXuMUoDhR69Skysb_GV6ozzMQE1JirlpAPfkM3KGqpx803rmDmQNXiFX9GhBlx1_ESspi3kmgR9aHv4Pi5XKfvDhmc2e_85JCekhJkflt1u5VTbcWK6XT48CxtiT1bMoqcP_EazyetkwnBxhpyVGavokAhEV8ITBKEW2Kc3NlOQk2lzMPxee3uakjmDtbt4btu5zqgMZzyXgz4PTwpV6rS8RYLzobXll2dzFB5EAnir5ClZDN5byjaebBY7l1gd1sjYjraeUgX4C0GxcS6xxgXl62COUPFUxorEeK_2XuM8AAqv0MNs6NSF9cIUIkRXxevnlgjBF1RuFnNFoacf5RXdCC9YOzpxNEbbCYwQ2hNTYsM-iW1ROfnNXAA8W1ttN24i3en2GmYlH7l24MRIZOVwOYEMrU045FAIKMlEuLG8EAQqjZzAegrKk9lRNH8Xzr-EGpoP9i_mWiAk585FsLVr_umre8ULOluFuhzfUq5lou26tJhCEjXroCoFuU-LZYqWWkcUHe9FJ_YZOfNzw-1v4AWarcKdHtwnq8b67qcySVkH-3nOHKreLNaQq7-ozK2q5-iolRXgvs7NW5it52f6O6fpfSNrFZvLY8TQ1MTi1df5dv6TZyCyjHSWyaM97mqGnJkYOZ86ozQf6CA2EQIe1Sok-I6cEXRlMcgVvJdZmTfEC_trrP5um4Aw1_SjpXBFYwVPZvkHqn9S_oesTrsLQ3b0YmlrsvceACeUpNFEUKjtqYML3eu_8yT96wis_HTkQpVdnH-1e3FSsslmwXJ0eQjT1afXcFxjwBZCWm_OoGb6NMFW_xv1e1NDjdso7IvfyYu3a9rPQj9zkq3Q7qQbY0QjdlEufhhcbQ9_BfZNVLRYLwrJYNfiK9AYOfnTWFoC9VYwwrA6NCuQG1JZxCgF_K8W6ZkZrz5nhvevnl7hQ5I7TS0m80tMmcau2q7E8vFwXZonTpZvvMtJUUCXReHvusvsSPGsoKLUncWdIlEfXrezEqZleIGw1pjnOUsn6g6sqtIns7FUoIQb-n22hR6yBdZPb6B-cVJ0k9jT0UAB1Iu40A6IwzdB6B6x-6gF244QrxDsiXxU7dsvTvXmQhXUFQonryjNXcZ1IRMfeEuiKetMxb9uwC2w3ypT4ZCu7anuZ2VC858LP_XHw85zuV6ByGPJlMJhZSUKN-16kq%5C%22%2C%5C%22bcd3b4b49e4be4f90af493ff2a493b88%5C%22%2Cnull%2C%5B1%5D%2C1%2Cnull%2Cnull%2C1%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B0%5D%5D%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C1%2Cnull%2Cnull%2C%5B4%5D%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B1%5D%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5C%2257B17232-B16E-4E9E-8A63-F85AE072B2D4%5C%22%2Cnull%2C%5B%5D%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B1770196106%2C454000000%5D%2Cnull%2C2%5D%22%5D&at=AEHmXlEI0oZuoDyWz_6pDHJz5jdR%3A{timestamp}&'
        
        headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
            "cookie": self.cookies,
            "origin": "https://gemini.google.com",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://gemini.google.com/",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            "sec-ch-ua-arch": '"x86"',
            "sec-ch-ua-bitness": '"64"',
            "sec-ch-ua-form-factors": '"Desktop"',
            "sec-ch-ua-full-version": '"144.0.7559.96"',
            "sec-ch-ua-full-version-list": '"Not(A:Brand";v="8.0.0.0", "Chromium";v="144.0.7559.96", "Google Chrome";v="144.0.7559.96"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": '""',
            "sec-ch-ua-platform": '"macOS"',
            "sec-ch-ua-platform-version": '"13.4.0"',
            "sec-ch-ua-wow64": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
            "x-browser-channel": "stable",
            "x-browser-copyright": "Copyright 2026 Google LLC. All Rights reserved.",
            "x-browser-validation": "xnE7MBo0fDEGcagbvn9w5+SkX68=",
            "x-browser-year": "2026",
            "x-client-data": "CJO2yQEIo7bJAQipncoBCOT+ygEIlqHLAQiGoM0BCJeMzwEIipHPAQi1os8BCNajzwEIvKTPAQiapc8BCICmzwEI3qbPAQjQqc8BCNmqzwEYwaHPAQ==",
            "x-goog-ext-525001261-jspb": '[1,null,null,null,"56fdd199312815e2",null,null,0,[4],null,null,2]',
            "x-goog-ext-525005358-jspb": '["57B17232-B16E-4E9E-8A63-F85AE072B2D4",1]',
            "x-goog-ext-73010989-jspb": "[0]",
            "x-same-domain": "1"
        }
        
        timeout_config = aiohttp.ClientTimeout(total=self.timeout)
        connector = aiohttp.TCPConnector(ssl=False) if self.proxy else None
        
        async with aiohttp.ClientSession(timeout=timeout_config, connector=connector, trust_env=self.trust_env) as session:
            try:
                async with session.post(url, data=data, headers=headers, proxy=self.proxy) as response:
                    if response.status != 200:
                        text = await response.text()
                        raise Exception(f"请求失败: {response.status}, 响应: {text}")
                    
                    result = ""
                    async for line in response.content:
                        if line:
                            result += line.decode('utf-8', errors='ignore')
                    
                    return result
            except aiohttp.ClientPayloadError:
                # 忽略传输不完整错误，返回已接收的数据
                return result if result else ""
