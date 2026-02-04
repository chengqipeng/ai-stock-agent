import aiohttp
from typing import Dict, Any
from urllib.parse import quote
import time

DEFAULT_COOKIES = "_gcl_au=1.1.1516393348.1763729168; _ga=GA1.1.949038794.1763729169; SEARCH_SAMESITE=CgQIwZ8B; __Secure-BUCKET=CLkC; __Secure-ENID=30.SE=g3T85coHgVfq8Q_a0UyET_BFZ6G78c8Jgn1BLNpKNbO2T3WqmfFXjjNkJYcLNr_gW2zOlAjCcKceXPPflvFafRohA-zknxP-gph6M7Pk6PNsjzHlRkK35NLGthcrLAcpEhOaZzlGm-WForlZCE_jdqGAaQE7LgWM2HV8p9ZKZ2EFiJJxTjjVVLdq2QH6FF7jsp5-R44ZF8uP44MHR7CogxUwbwN9Q44OgJUvzDReZiz1937NYecidRVZOg; SID=g.a0006AjGdoJMV9YoY6xLNi39p65Ho4Y8J64iU6DSS97xS9-hjoXLcCb6spxTNMF8W3HVOtAkoQACgYKAWMSARESFQHGX2MibsPeQwLKHFYsqz0vPOrXtRoVAUF8yKq7llDeTOSjAoTwf9UEXZjm0076; __Secure-1PSID=g.a0006AjGdoJMV9YoY6xLNi39p65Ho4Y8J64iU6DSS97xS9-hjoXLZF75FhNUVdGxFcYYhBlJOAACgYKAc4SARESFQHGX2MifDB8wfXteZLheMopbsIBsBoVAUF8yKovlzg67k66LnqvfD-R-Zml0076; __Secure-3PSID=g.a0006AjGdoJMV9YoY6xLNi39p65Ho4Y8J64iU6DSS97xS9-hjoXLvfNrwI8KkYV46rPwfk09xgACgYKAY8SARESFQHGX2MiJvcZYNXsFqcPozTJ7QvCOBoVAUF8yKpbejFIL1B2wifKlVEaEz8G0076; HSID=A5XSQMdKvnG8uRc1m; SSID=AF0AJotjZ4lzDKDFD; APISID=wzkpoCMZ_TZdlISv/AuVlwm6ezW29PRg72; SAPISID=5f_p7D_9QEy3ETNA/AYsUb_sJ1OZFo4R6o; __Secure-1PAPISID=5f_p7D_9QEy3ETNA/AYsUb_sJ1OZFo4R6o; __Secure-3PAPISID=5f_p7D_9QEy3ETNA/AYsUb_sJ1OZFo4R6o; S=billing-ui-v3=59Id8PcXojqcFfPTpjYbe3UD-LdEFmhM:billing-ui-v3-efe=59Id8PcXojqcFfPTpjYbe3UD-LdEFmhM; COMPASS=gemini-pd=CjwACWuJV93jFYb_b6k1ZbZc5AVi75OXfwVJx6huPFdJgLZgT-iphNSBtyIyTho-2Gurv4U86El7hPmdVFUQuuiLzAYaXQAJa4lXnqJx4gXk9zvhy1q10WQrRMnG8G9fTHk2jvKIu0mTZmOiCuvFDsXH12Ir-E8p3oGWds0RuYp643WmILSrRIwMqKEtz4d2gTbBdp9qS6_WVZWX0zhA5o5i1yABMAE:gemini-hl=CkkACWuJV4Jq7gXnYGXm-CCWRGf1MNczIJ0yMsen8R98zb0fdd_v1HDcw_-Y0Gxw7WZu_GGVl89NUAGecp6EG6tM_DjudIlkdiK-EKep7csGGmoACWuJV7Lg1UsZQy6wrea7RbcYXrgIBMhT7j2dA3F7F8d80C4l18yb6WVs8qlK7QSQjj7jmPGvzUn5cebPhh4efvhlaRKsIx8742fzS6iZJ-tQOZfmiFBBZBLolITGCafRJlBZ9-u5gJGrIAEwAQ; AEC=AaJma5utKdDnuMMO6KUGnAsMcVqFXxM_BCn-w2iJaez685RdOzZg9as3xg; NID=528=hBiY1C1iKVEoE1vifRADly2ojdptOMBatZS51vheNHMU2iKFQncoC9OSaf0_kyw51HdgK_W7qrI5Owq7qQZ-3hI92aRuaQ2T3aKalLwkN7a0q6TKkD1rJ78Z3tPvD_iBHQ09Z_V7FbDeNQEs1MZTyWx0ChLTYsKrr00zdBDAxxqwU4ehZq5igYeAcwIOF3gcsoOaNIIYZxaeXBnYj1342h45cnu94AAsfmfVq7N_0kgL_KWAgoi3BGfHLV9e8dGI6cidYnjGWDGyULJXwQOsOXgo1lFuVsLHt2D1AAfmZLrxiTVVBe3PdxUJ-sS6mVUM_6v4-l5T_yJoiwzbEOEtPuEc8IWJXMJjsghyi-Ppe4SxNlAQCtkLmi9MTkZosIpqaLTDlWau9VCcY0NIP8QbCjbs6VGvIASBQsANFMnYiZqZTWmkrT5Zsju51574tGL5eb0DsZZXX24sPJWgsWrt4mATolJbRfdH6hblXGlaiO9Tt5LcwMnAAntfeM7NrVq7tO_95w73okfgRZwVpTz3W5lBxNn3HbNbPTGZ0Jbz9DTVgMPvVT5F18-n_SyEnTKDKxbpTrajezjae4p4Wv05gmNHSxXod0XYytlS7E6aCGUjUYIHr-49KqshSyLYqaTfBTJfZI8FtESlOXJg9oRmqzYE524a0ub5FxJoBGmPsWzcmFqVvGoFD1phXLADesz1nk6IigroZeBp-Alby7-yIXMzBGMj83gfwkeYagZeJXIgeMSny-MlbP3JLReSgQvptThnCz656I_1xU5y; __Secure-1PSIDTS=sidts-CjIB7I_69MojPruvrRDcVpmSM2l2z5b9Mfm0qRkDG2jqsfSUkn_7Bfww5Zp8u9nMvAwX6RAA; __Secure-3PSIDTS=sidts-CjIB7I_69MojPruvrRDcVpmSM2l2z5b9Mfm0qRkDG2jqsfSUkn_7Bfww5Zp8u9nMvAwX6RAA; _ga_BF8Q35BMLM=GS2.1.s1770193017$o87$g1$t1770196651$j60$l0$h0; _ga_WC57KJ50ZZ=GS2.1.s1770193017$o98$g1$t1770196651$j60$l0$h0; SIDCC=AKEyXzW_U3PB7amK5HdfRANxh8MWX4nhZdjWCYiI-b7V6IGDH_DxkCuZLaI91GxMSJL5-P-sX_g; __Secure-1PSIDCC=AKEyXzVhghU1D9opb0PBwEkEqrZEECQtvf8Yj9vpDGghZ6Hcd97-3GfyY2HBCfI8NKKBBx76KQ; __Secure-3PSIDCC=AKEyXzWC2dEfrd_N-ziYiLX65QpCN7mnbdxVDy7KwgfEqfOuRGrp9px6DCAIlH-P2VNhn-b0LA"
SESSION_ID = "bcd3b4b49e4be4f90af493ff2a493b88"
SNLM0E = "!4uGl4bnNAAYFnR50JZ1C6qnaHvT5mwI7ADQBEArZ1AJm4TKYiXJpy3LOtBthDoL0jhQjiTCNNJ4x0-U5-Y99iwo5jJ_43tEGGkq6erR0AgAAAKNSAAAABWgBB34AQUS3Egd9EBPAlgePd5vs6cLYytouBh7_515_Qz8aXuMUoDhR69Skysb_GV6ozzMQE1JirlpAPfkM3KGqpx803rmDmQNXiFX9GhBlx1_ESspi3kmgR9aHv4Pi5XKfvDhmc2e_85JCekhJkflt1u5VTbcWK6XT48CxtiT1bMoqcP_EazyetkwnBxhpyVGavokAhEV8ITBKEW2Kc3NlOQk2lzMPxee3uakjmDtbt4btu5zqgMZzyXgz4PTwpV6rS8RYLzobXll2dzFB5EAnir5ClZDN5byjaebBY7l1gd1sjYjraeUgX4C0GxcS6xxgXl62COUPFUxorEeK_2XuM8AAqv0MNs6NSF9cIUIkRXxevnlgjBF1RuFnNFoacf5RXdCC9YOzpxNEbbCYwQ2hNTYsM-iW1ROfnNXAA8W1ttN24i3en2GmYlH7l24MRIZOVwOYEMrU045FAIKMlEuLG8EAQqjZzAegrKk9lRNH8Xzr-EGpoP9i_mWiAk585FsLVr_umre8ULOluFuhzfUq5lou26tJhCEjXroCoFuU-LZYqWWkcUHe9FJ_YZOfNzw-1v4AWarcKdHtwnq8b67qcySVkH-3nOHKreLNaQq7-ozK2q5-iolRXgvs7NW5it52f6O6fpfSNrFZvLY8TQ1MTi1df5dv6TZyCyjHSWyaM97mqGnJkYOZ86ozQf6CA2EQIe1Sok-I6cEXRlMcgVvJdZmTfEC_trrP5um4Aw1_SjpXBFYwVPZvkHqn9S_oesTrsLQ3b0YmlrsvceACeUpNFEUKjtqYML3eu_8yT96wis_HTkQpVdnH-1e3FSsslmwXJ0eQjT1afXcFxjwBZCWm_OoGb6NMFW_xv1e1NDjdso7IvfyYu3a9rPQj9zkq3Q7qQbY0QjdlEufhhcbQ9_BfZNVLRYLwrJYNfiK9AYOfnTWFoC9VYwwrA6NCuQG1JZxCgF_K8W6ZkZrz5nhvevnl7hQ5I7TS0m80tMmcau2q7E8vFwXZonTpZvvMtJUUCXReHvusvsSPGsoKLUncWdIlEfXrezEqZleIGw1pjnOUsn6g6sqtIns7FUoIQb-n22hR6yBdZPb6B-cVJ0k9jT0UAB1Iu40A6IwzdB6B6x-6gF244QrxDsiXxU7dsvTvXmQhXUFQonryjNXcZ1IRMfeEuiKetMxb9uwC2w3ypT4ZCu7anuZ2VC858LP_XHw85zuV6ByGPJlMJhZSUKN-16kq"
AT = "AEHmXlEI0oZuoDyWz_6pDHJz5jdR"

class GeminiService:

    def __init__(self, proxy: str = None, timeout: int = 60, trust_env: bool = True):
        self.proxy = proxy
        self.timeout = timeout
        self.trust_env = trust_env

    async def stream_generate(self, prompt: str) -> Dict[str, Any]:
        timestamp = str(int(time.time() * 1000))
        current_time = time.time()
        timestamp_sec = int(current_time)
        timestamp_micro = int((current_time - timestamp_sec) * 1000000000)
        cookies: str = DEFAULT_COOKIES
        url = "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate?bl=boq_assistant-bard-web-server_20260202.09_p1&f.sid=6488172965349433713&hl=zh-CN&_reqid=3161907&rt=c"
        
        encoded_prompt = quote(prompt, safe='')
        data = f'f.req=%5Bnull%2C%22%5B%5B%5C%22{encoded_prompt}%5C%22%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2C0%5D%2C%5B%5C%22zh-CN%5C%22%5D%2C%5B%5C%22%5C%22%2C%5C%22%5C%22%2C%5C%22%5C%22%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5C%22%5C%22%5D%2C%5C%22{SNLM0E}%5C%22%2C%5C%22{SESSION_ID}%5C%22%2Cnull%2C%5B1%5D%2C1%2Cnull%2Cnull%2C1%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B0%5D%5D%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C1%2Cnull%2Cnull%2C%5B4%5D%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B1%5D%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5C%2257B17232-B16E-4E9E-8A63-F85AE072B2D4%5C%22%2Cnull%2C%5B%5D%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B{timestamp_sec}%2C{timestamp_micro}%5D%2Cnull%2C2%5D%22%5D&at={AT}%3A{timestamp}&'
        
        headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
            "cookie": cookies,
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

        async with aiohttp.ClientSession(timeout=timeout_config) as session:
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
