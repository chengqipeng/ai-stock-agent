import asyncio
from service.gemini.gemini_app_client import gemini_generate

async def main():
    result = await gemini_generate(prompt="CRM系统在中国最知名的前3加企业前3个")
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
