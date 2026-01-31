import aiohttp
import json
import os
from pathlib import Path

class THSTokenClient:
    def __init__(self, refresh_token, token_file=".ths_token.json"):
        self.refresh_token = refresh_token
        self.base_url = "https://quantapi.51ifind.com/api/v1"
        self.token_file = Path(token_file)
    
    def _load_token_from_file(self):
        """从文件加载token"""
        if self.token_file.exists():
            try:
                with open(self.token_file, 'r') as f:
                    return json.load(f)
            except:
                return None
        return None
    
    def _save_token_to_file(self, token_data):
        """保存token到文件"""
        with open(self.token_file, 'w') as f:
            json.dump(token_data, f)
    
    async def get_access_token(self):
        """获取当前有效的access_token"""
        # 优先从文件读取
        token_data = self._load_token_from_file()
        if token_data:
            return token_data
        
        # 文件不存在或读取失败，从API获取
        url = f"{self.base_url}/get_access_token"
        headers = {
            "Content-Type": "application/json",
            "refresh_token": self.refresh_token
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                text = await response.text()
                try:
                    token_data = json.loads(text)
                    if "error" not in token_data:
                        self._save_token_to_file(token_data)
                    return token_data
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
                    token_data = json.loads(text)
                    if "error" not in token_data:
                        self._save_token_to_file(token_data)
                    return token_data
                except json.JSONDecodeError:
                    return {"error": "Invalid JSON response", "content": text}