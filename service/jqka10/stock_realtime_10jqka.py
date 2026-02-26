import re
import aiohttp

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://stockpage.10jqka.com.cn/",
    "Sec-Fetch-Dest": "script",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}


async def get_today_trade_data(stock_code: str) -> dict:
    """
    获取同花顺今日交易数据
    :param stock_code: 股票代码，如 '002371'
    :return: 解析后的今日交易数据字典
    """
    url = f"https://d.10jqka.com.cn/v6/line/hs_{stock_code}/01/defer/today.js"
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            text = await resp.text()

    # 响应格式: quotebridge_v6_line_hs_002371_01_defer_today({"data":...})
    match = re.search(r"\((\{.*\})\)", text, re.DOTALL)
    if not match:
        raise ValueError(f"Unexpected response format: {text[:200]}")

    import json
    return json.loads(match.group(1))


if __name__ == "__main__":
    import asyncio

    async def main():
        data = await get_today_trade_data("002371")
        print(data)

    asyncio.run(main())
