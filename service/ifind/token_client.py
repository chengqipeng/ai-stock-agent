import aiohttp
import json

class THSTokenClient:
    def __init__(self, refresh_token):
        self.refresh_token = refresh_token
        self.base_url = "https://quantapi.51ifind.com/api/v1"
    
    async def get_access_token(self):
        """获取当前有效的access_token"""
        url = f"{self.base_url}/get_access_token"
        headers = {
            "Content-Type": "application/json",
            "refresh_token": self.refresh_token
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                text = await response.text()
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return {"error": "Invalid JSON response", "content": text}
    
    async def update_access_token(self):
        """获取新的access_token（会使旧token失效）"""
        url = f"{self.base_url}/update_access_token"
        headers = {
            "Content-Type": "application/json",
            "refresh_token": self.refresh_token
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                text = await response.text()
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return {"error": "Invalid JSON response", "content": text}