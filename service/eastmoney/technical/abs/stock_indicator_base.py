import pandas as pd
from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_history_kline_max_min, _parse_kline_fields

# 指标数据配置
INDICATOR_CONFIG = {
    # 'boll': {'kline_limit': 300, 'tail_limit': 300, 'markdown_limit': 250},
    # 'macd': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 365},
    # 'rsi': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 365},
    # 'ma': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 200},
    # 'vwma': {'kline_limit': 200, 'tail_limit': 200, 'markdown_limit': 60}
    'boll': {'kline_limit': 300, 'tail_limit': 300, 'markdown_limit': 200},
    'macd': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 200},
    'rsi': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 200},
    'ma': {'kline_limit': 400, 'tail_limit': 400, 'markdown_limit': 200},
    'atr': {'kline_limit': 200, 'tail_limit': 200, 'markdown_limit': 100},
    'vwma': {'kline_limit': 200, 'tail_limit': 200, 'markdown_limit': 60}
}

def parse_klines_to_df(klines):
    """解析K线数据为DataFrame"""
    data = [_parse_kline_fields(kline) for kline in klines]
    return pd.DataFrame(data)

def process_indicator_data(df, indicator_type):
    """处理指标数据，应用tail限制并返回倒序记录"""
    config = INDICATOR_CONFIG.get(indicator_type, {'tail_limit': 400})
    return df.tail(config['tail_limit']).to_dict('records')[::-1]


if __name__ == "__main__":
    import asyncio
    import json


    async def main():
        stock_name = "北方华创"
        stock_info: StockInfo = get_stock_info_by_name(stock_name)
        # 测试 JSON 格式
        result = await get_stock_history_kline_max_min(stock_info)
        print("股东增减持数据 (JSON格式):")
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())