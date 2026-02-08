import asyncio

from service.llm.deepseek_client import DeepSeekClient
from service.processor.base_stock_processor import BaseStockProcessor

class DeepSeekStockProcessor(BaseStockProcessor):
    def __init__(self):
        super().__init__(model_name='deepseek')
    
    def create_client(self):
        return DeepSeekClient()


if __name__ == "__main__":
    asyncio.run(DeepSeekStockProcessor().run())
