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
    async def download_pdf(pdf_url: str, save_dir: str = "temp_files") -> Optional[str]:
        """下载PDF文件"""
        try:
            os.makedirs(save_dir, exist_ok=True)
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
    async def parse_pdf(file_path: str) -> Optional[str]:
        """解析PDF文件为文本并保存"""
        try:
            text = ""
            with open(file_path, 'rb') as f:
                try:
                    reader = PyPDF2.PdfReader(f, strict=False)
                    for page in reader.pages:
                        try:
                            text += page.extract_text()
                        except Exception:
                            continue
                except Exception as e:
                    print(f"PDF解析警告: {file_path}, 错误: {e}, 尝试保存已读取内容")
            
            if not text:
                return None
            
            txt_path = file_path.replace('.pdf', '.txt')
            async with aiofiles.open(txt_path, 'w', encoding='utf-8') as f:
                await f.write(text)
            
            return txt_path
        except Exception as e:
            print(f"PDF解析失败: {file_path}, 错误: {e}")
            return None
    
    @staticmethod
    async def download_and_parse(pdf_url: str, max_retries: int = 3) -> tuple[Optional[str], str]:
        """下载PDF并转换为文本文件，返回(txt路径, 状态)"""
        for attempt in range(max_retries):
            file_path = await PDFParser.download_pdf(pdf_url)
            if not file_path:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                    continue
                return None, "download_failed"
            
            txt_path = await PDFParser.parse_pdf(file_path)
            
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"删除文件失败: {file_path}, 错误: {e}")
            
            if txt_path:
                return txt_path, "success"
            else:
                return None, "parse_failed"
        
        return None, "download_failed"
