import aiohttp
from typing import Optional
import io


class PDFParser:
    @staticmethod
    async def download_and_parse(pdf_url: str) -> Optional[str]:
        """下载PDF并转换为文本"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(pdf_url) as response:
                    if response.status != 200:
                        return None
                    
                    pdf_content = await response.read()
                    
                    try:
                        import PyPDF2
                        pdf_file = io.BytesIO(pdf_content)
                        reader = PyPDF2.PdfReader(pdf_file)
                        text = ""
                        for page in reader.pages:
                            text += page.extract_text()
                        return text
                    except ImportError:
                        return None
        except Exception:
            return None
