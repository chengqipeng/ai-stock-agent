import aiohttp
import logging
from typing import Optional
import io
import asyncio
import PyPDF2
import os
import hashlib
import aiofiles

logger = logging.getLogger(__name__)

class PDFParser:
    @staticmethod
    async def download_pdf(pdf_url: str, save_dir: str = "temp_files", max_retries: int = 3) -> Optional[str]:
        """下载PDF文件，带重试和指数退避"""
        os.makedirs(save_dir, exist_ok=True)
        file_hash = hashlib.md5(pdf_url.encode()).hexdigest()
        file_path = os.path.join(save_dir, f"{file_hash}.pdf")

        # 根据域名设置合适的 Referer
        referer = "http://ft.10jqka.com.cn/"
        if "szse.cn" in pdf_url:
            referer = "http://www.szse.cn/"
        elif "sse.com.cn" in pdf_url:
            referer = "http://www.sse.com.cn/"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": referer,
            "Accept": "*/*",
            "Connection": "keep-alive"
        }

        for attempt in range(max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=300)
                async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                    async with session.get(pdf_url) as response:
                        if response.status != 200:
                            logger.warning("服务器拒绝请求，状态码: %d, URL: %s", response.status, pdf_url)
                            return None

                        async with aiofiles.open(file_path, 'wb') as f:
                            try:
                                async for chunk in response.content.iter_chunked(4096):
                                    if chunk:
                                        await f.write(chunk)
                            except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                                logger.warning("PDF下载连接断开 [%s]: %s", pdf_url, e)
                                logger.warning("服务器连接提前断开，尝试抢救已下载的数据...")

                if os.path.exists(file_path):
                    file_size = os.path.getsize(file_path)
                    if file_size > 0:
                        logger.info("下载完成，文件大小: %d 字节", file_size)
                        return file_path
                    # 文件为空，删除后重试
                    os.remove(file_path)

            except (ConnectionResetError, ConnectionError, OSError) as e:
                wait = 2 ** attempt
                logger.warning("PDF下载连接被重置 (第%d/%d次) [%s]: %s, %ds后重试",
                               attempt + 1, max_retries, pdf_url, e, wait)
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait)
                    continue
                logger.error("PDF下载失败（重试耗尽）: %s", pdf_url)
                return None
            except Exception as e:
                logger.error("PDF下载失败: %s, 错误: %s", pdf_url, e)
                return None

        return None
    
    @staticmethod
    async def parse_pdf(file_path: str) -> Optional[str]:
        """解析PDF文件为文本并保存"""
        try:
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                logger.warning("PDF文件为空或不存在: %s", file_path)
                return None
            
            text = ""
            with open(file_path, 'rb') as f:
                try:
                    reader = PyPDF2.PdfReader(f, strict=False)
                    for page in reader.pages:
                        try:
                            text += page.extract_text()
                        except Exception as e:
                            logger.warning("PDF页面解析失败 [%s]: %s", file_path, e)
                            continue
                except Exception as e:
                    logger.warning("PDF解析警告: %s, 错误: %s, 尝试保存已读取内容", file_path, e)
            
            if not text:
                return None
            
            txt_path = file_path.replace('.pdf', '.txt')
            async with aiofiles.open(txt_path, 'w', encoding='utf-8') as f:
                await f.write(text)
            
            return txt_path
        except Exception as e:
            logger.error("PDF解析失败: %s, 错误: %s", file_path, e)
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
                logger.warning("删除文件失败: %s, 错误: %s", file_path, e)
            
            if txt_path:
                return txt_path, "success"
            else:
                return None, "parse_failed"
        
        return None, "download_failed"
