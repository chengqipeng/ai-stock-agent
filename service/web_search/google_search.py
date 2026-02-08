import aiohttp
import asyncio


async def google_search(query: str, api_key: str, location: str = "United States", hl: str = "en", gl: str = "us") -> dict:
    """使用 SerpAPI 进行谷歌搜索"""
    url = "https://serpapi.com/search"
    params = {
        "engine": "google",
        "q": query,
        "location": location,
        "google_domain": "google.com",
        "hl": hl,
        "gl": gl,
        "api_key": api_key
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()


if __name__ == "__main__":
    async def main():
        result = await google_search(
            query="销售易创始人",
            api_key="61ac5e704d09b61d45174c0d7ed881fa0658aaadc803f91912a8949634a132c3",
            location="United States"
        )
        print(result)
    
    asyncio.run(main())
