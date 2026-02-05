import aiohttp
from typing import Dict, Any
from urllib.parse import quote
import time

class GeminiService:

    def __init__(self, proxy: str = None, timeout: int = 60, trust_env: bool = True):
        self.proxy = proxy
        self.timeout = timeout
        self.trust_env = trust_env

    async def stream_generate(self, prompt: str) -> Dict[str, Any]:
        url = "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate?bl=boq_assistant-bard-web-server_20260203.08_p0&f.sid=-2696125158889097683&hl=zh-CN&_reqid=4954895&rt=c"
        data = 'f.req=%5Bnull%2C%22%5B%5B%5C%22%23%20%E4%BD%BF%E7%94%A8%E6%AC%A7%E5%A5%88%E5%B0%94CAN%20SLIM%E8%A7%84%E5%88%99%E5%88%86%E6%9E%90%E4%B8%80%E4%B8%8B%3C002050%20%E4%B8%89%E8%8A%B1%E6%99%BA%E6%8E%A7%3E%EF%BC%8C%E6%98%AF%E5%90%A6%E7%AC%A6%E5%90%88%E4%B9%B0%E5%85%A5%E6%9D%A1%E4%BB%B6%EF%BC%9A%E5%9F%BA%E4%BA%8E%E6%A8%A1%E5%9E%8B%E7%9A%84%E6%9C%80%E7%BB%88%E5%88%A4%E6%96%AD%EF%BC%8C%E7%A8%B3%E5%81%A5%E4%B9%B0%E5%85%A5%E4%BB%B7%E6%A0%BC%E5%8C%BA%E9%97%B4%EF%BC%9A%E5%9F%BA%E4%BA%8E%E6%8A%80%E6%9C%AF%E5%BD%A2%E6%80%81%EF%BC%88%E5%A6%82%E6%9D%AF%E6%9F%84%E5%BD%A2%E6%80%81%E3%80%81%E7%AA%81%E7%A0%B4%E7%82%B9%EF%BC%89%E7%BB%99%E5%87%BA%E7%9A%84%E5%BB%BA%E8%AE%AE%5C%22%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2C0%5D%2C%5B%5C%22zh-CN%5C%22%5D%2C%5B%5C%22%5C%22%2C%5C%22%5C%22%2C%5C%22%5C%22%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5C%22%5C%22%5D%2C%5C%22\u0021GRqlGkLNAAYFnR50JZ1CWN980d_85Ww7ADQBEArZ1JTqzcIxUMyOTDe537TE6Nj7HfyKqu0yU5YFgoi3EvSn5bHmWqhFQmyvgY9ZoKxtAgAAAERSAAAAAmgBB34AQX0BCSiI0voZfSrakqovEzcKArvMSxpdRTU8nvAgjstey2S6w7BbRZk1Umq_S2KJLigoZkxNBKBn_SF0lWhOocQjmQNlRZa-h6MKlCzHgWezJwiWgxhHy_nVFvEd4rzlVvMntTWgnPo3RbfuYEhxboaEJFDVNLkVZkURKMBHX1fdVBYhudPYQ3TWnuHMia7zf7hP8o25SGPsRcV6Bx5gDPQapOfXMh21LNe3nMNH1TDquWJOLmnrNIEoYmbrxfprODApA8aLLTo5iWP8pZQ2IXe_tGMnvbLLpjmAolaPKbSD7nuvWPGOrP6HfpCwEwOBe_JzughkOd5s0LjbEc2W8asRyxhCbdF52rh6Lq10Jcali-atZ6tRdqDe_ApTuExHEpoePTzVkFXznN0IuK14oeV3K-mxIxnO2Tyzlm2A6qSWpNU1vxuOl0caADid6Fg400hLK4AUmxqivVXptaYirWiPZTeUlEIEyLd5pUGmJ8ksb4yEnH-aF-zPXQlmDgwOyfh8z_3rIplybj_O4mkIxDFsMj2MlHcpnZS9XzK6ruLPuI1-rfX9YoDJsnGcYi42Hlot_LpYOhdaJw3viM8kKaiBLOHF8qnWB-wvfsBFso0pTc4GPIzWiaQHIQ7qvngMF128Kyjv4Z1aSYVdBLxwkiByfJbnxDAQkaYacVml8EiV7vw5TNpmKcmmHQrU0eFWTsvvOOqI-_eGeSPdVBtdqb1zdgkAmTmTwBRQtnZOI9tz5EPJlR69oqJ3mgXzzz6Px6gmXEgQJsHSeqsYI7nivL_K8VzczN1P1iYUpcvVVm7wKPAkO-3EN-H6NbGh9g-tdHgTCYlPYCKQq656YxNmRe4kWSvZMCn7FLxqYnrUh9DiRVhIspBYEWFPWqtmfzQkzaHA2AV-O7ktqWa8jjPHVRIc2ylUmYUKhR8F4qTMG7hWreweneBtJZm0dJpX4QfV4WqpKBNxJTjkdGjywPN6Nlr_YAchZxeuYwjJHpklJk0acRRJ13c0sgHb--p7kixGOoKqRp8b8PRA-go9Mg0UaxEsJK4GnDQ37gyOsLbnOAWU9YcNX3h1DVZc-4W8fkOc0X3OKufnzKfT8IjLkd4sPlmexVm2MuszRj6qG7GFN8n6hvdcTwB3rRDBpBl1WU2psYX95PpebYw3R2HX-NYo4tMheVQLUwybNOg2X5W7q4uk4GDKX787QgzXI9TwKAwhiitQL4rnl8Il2tvTBJo5bWuLSKAS21us7kk%5C%22%2C%5C%229fc1cb87f198ec3fe5693d02b41b494a%5C%22%2Cnull%2C%5B0%5D%2C1%2Cnull%2Cnull%2C1%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B0%5D%5D%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C1%2Cnull%2Cnull%2C%5B4%5D%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B1%5D%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5C%221BA2638C-4DE8-4B7D-A402-69CF67005121%5C%22%2Cnull%2C%5B%5D%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B1770275757%2C176000000%5D%2Cnull%2C1%5D%22%5D&at=AEHmXlHb8AuUBOTKxyz9B1xbXrKu%3A1770275693619&'
        
        headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
            "origin": "https://gemini.google.com",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://gemini.google.com/",
            "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "sec-ch-ua-arch": '"arm"',
            "sec-ch-ua-bitness": '"64"',
            "sec-ch-ua-form-factors": '"Desktop"',
            "sec-ch-ua-full-version": '"143.0.7499.193"',
            "sec-ch-ua-full-version-list": '"Google Chrome";v="143.0.7499.193", "Chromium";v="143.0.7499.193", "Not A(Brand";v="24.0.0.0"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": '""',
            "sec-ch-ua-platform": '"macOS"',
            "sec-ch-ua-platform-version": '"14.3.0"',
            "sec-ch-ua-wow64": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "x-browser-channel": "stable",
            "x-browser-copyright": "Copyright 2026 Google LLC. All Rights reserved.",
            "x-browser-validation": "AUXUCdutEJ+6gl6bYtz7E2kgIT4=",
            "x-browser-year": "2026",
            "x-client-data": "CI+2yQEIprbJAQipncoBCJT3ygEIlqHLAQiGoM0BCJaMzwEIhZHPAQi1os8BCNWjzwEImqXPAQjfps8BCNGpzwEI2qrPARiyhs8BGMGhzwEY/aXPAQ==",
            "x-goog-ext-525001261-jspb": '[1,null,null,null,"56fdd199312815e2",null,null,0,[4],null,null,2]',
            "x-goog-ext-525005358-jspb": '["1BA2638C-4DE8-4B7D-A402-69CF67005121",1]',
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
