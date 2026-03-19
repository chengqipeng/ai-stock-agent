"""测试个股消息面批量搜索"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from service.web_search.stock_news_batch_search import (
    search_stock_sentiment,
    batch_search_stock_sentiment,
    format_sentiment_for_md,
)
from common.utils.stock_info_utils import get_stock_info_by_name


async def test_single_stock():
    """测试单只股票消息面搜索"""
    stock_info = get_stock_info_by_name('桂冠电力')
    if not stock_info:
        print("未找到股票信息")
        return

    print(f"=== 测试单只股票: {stock_info.stock_name}({stock_info.stock_code_normalize}) ===\n")
    result = await search_stock_sentiment(stock_info, days=7)
    print(f"情绪: {result['sentiment']}")
    print(f"置信度: {result['confidence']}")
    print(f"摘要: {result['summary']}")
    print(f"行业趋势: {result.get('industry_trend', '')}")
    print(f"\n关键新闻:")
    for news in result.get('key_news', []):
        print(f"  - [{news['impact']}] {news['title']}: {news['reason']}")
    print(f"\nMarkdown输出:")
    print(format_sentiment_for_md(result))


async def test_batch_search():
    """测试批量消息面搜索"""
    test_stocks = [
        {'name': '桂冠电力', 'code': '600236.SH'},
        {'name': '首航新能', 'code': '301658.SZ'},
        {'name': '闰土股份', 'code': '002440.SZ'},
        {'name': '晓鸣股份', 'code': '300967.SZ'},
        {'name': '金徽股份', 'code': '603132.SH'},
    ]

    print(f"=== 测试批量搜索（{len(test_stocks)}只股票） ===\n")
    results = await batch_search_stock_sentiment(test_stocks, days=7, concurrency=2)

    print(f"\n{'='*60}")
    print(f"搜索完成，结果汇总：\n")
    for code, sentiment in results.items():
        print(f"--- {code} ---")
        print(format_sentiment_for_md(sentiment))
        print()


if __name__ == "__main__":
    # 默认测试单只股票，传参 batch 测试批量
    if len(sys.argv) > 1 and sys.argv[1] == 'batch':
        asyncio.run(test_batch_search())
    else:
        asyncio.run(test_single_stock())
