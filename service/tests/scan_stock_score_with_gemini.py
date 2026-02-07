import asyncio

from service.llm.gemini_client import GeminiClient
from service.tests.processor.base_stock_processor import BaseStockProcessor


class GeminiStockProcessor(BaseStockProcessor):
    def __init__(self):
        super().__init__(model_name='gemini')
    
    def create_client(self):
        return GeminiClient()


if __name__ == "__main__":
    asyncio.run(GeminiStockProcessor().run())
