import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from common.utils.stock_info_utils import StockInfo


def _build_dataframe(klines: list) -> pd.DataFrame:
    rows = []
    for kline in klines:
        fields = kline.split(',')
        rows.append({
            'date':   fields[0],
            'open':   float(fields[1]),
            'close':  float(fields[2]),
            'high':   float(fields[3]),
            'low':    float(fields[4]),
            'volume': float(fields[5]),
        })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date')


def identify_new_high_signal(df: pd.DataFrame, lookback_window=60, vol_ma_window=50, vol_ratio=2.0) -> pd.DataFrame:
    """
    量价协同突破策略：新量新价出新高
    条件 A：收盘价突破过去 lookback_window 日最高价
    条件 B：成交量 > vol_ma_window 日均量 * vol_ratio
    条件 C：实体阳线且涨幅 > 3%
    """
    df['rolling_max_high'] = df['high'].shift(1).rolling(window=lookback_window).max()
    df['ma_volume'] = df['volume'].rolling(window=vol_ma_window).mean()

    price_change = df['close'].pct_change()

    condition_a = df['close'] > df['rolling_max_high']
    condition_b = df['volume'] > df['ma_volume'] * vol_ratio
    condition_c = (df['close'] > df['open']) & (price_change > 0.03)

    df['signal'] = condition_a & condition_b & condition_c
    return df


async def get_new_high_signals(stock_info: StockInfo, limit=400, lookback_window=60, vol_ma_window=50, vol_ratio=2.0) -> pd.DataFrame:
    """获取股票日K线并返回含信号列的 DataFrame，只保留有信号的行"""
    klines = await get_stock_day_range_kline(stock_info, limit=limit)
    df = _build_dataframe(klines)
    df = identify_new_high_signal(df, lookback_window, vol_ma_window, vol_ratio)
    return df[df['signal']][['open', 'close', 'high', 'low', 'volume', 'rolling_max_high', 'ma_volume']]


if __name__ == '__main__':
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        signals = await get_new_high_signals(stock_info)
        print(signals.to_string())

    asyncio.run(main())
