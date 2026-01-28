import aiohttp
from typing import Optional
import io
import asyncio
import PyPDF2
import os
import hashlib
import aiofiles

class PDFParser:
    @staticmethod
    async def download_pdf(pdf_url: str, save_dir: str = "/tmp") -> Optional[str]:
        """下载PDF文件"""
        try:
            file_hash = hashlib.md5(pdf_url.encode()).hexdigest()
            file_path = os.path.join(save_dir, f"{file_hash}.pdf")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "http://ft.10jqka.com.cn/",
                "Accept": "*/*",
                "Connection": "keep-alive"
            }
            
            timeout = aiohttp.ClientTimeout(total=300)
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(pdf_url) as response:
                    if response.status != 200:
                        print(f"服务器拒绝请求，状态码: {response.status}")
                        return None
                    
                    async with aiofiles.open(file_path, 'wb') as f:
                        try:
                            async for chunk in response.content.iter_chunked(4096):
                                if chunk:
                                    await f.write(chunk)
                        except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError):
                            print(f"警告：服务器连接提前断开，尝试抢救已下载的数据...")
            
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                print(f"下载完成，文件大小: {file_size} 字节")
                return file_path
            return None
        except Exception as e:
            print(f"PDF下载失败: {pdf_url}, 错误: {e}")
            return None
    
    @staticmethod
    def parse_pdf(file_path: str) -> Optional[str]:
        """解析PDF文件为文本"""
        try:
            with open(file_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                text = ""
                for page in reader.pages:
                    text += page.extract_text()
            return text
        except Exception as e:
            print(f"PDF解析失败: {file_path}, 错误: {e}")
            return None
    
    @staticmethod
    async def download_and_parse(pdf_url: str, max_retries: int = 3) -> Optional[str]:
        """下载PDF并转换为文本"""
        for attempt in range(max_retries):
            file_path = await PDFParser.download_pdf(pdf_url)
            if not file_path:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                    continue
                return None
            
            text = PDFParser.parse_pdf(file_path)
            
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"删除文件失败: {file_path}, 错误: {e}")
            
            if text:
                return text
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
        
        return None
