import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from service.eastmoney.technical.stock_day_volume_avg import get_volume_avg
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


def identify_new_high_signal(df: pd.DataFrame, lookback_window=60, vol_ratio=2.0) -> pd.DataFrame:
    """
    量价协同突破策略：新量新价出新高
    条件 A：收盘价突破过去 lookback_window 日最高价
    条件 B：成交量 > 日均量 * vol_ratio（ma_volume 由外部传入）
    条件 C：实体阳线且涨幅 > 3%
    """
    df['rolling_max_high'] = df['high'].shift(1).rolling(window=lookback_window).max()

    price_change = df['close'].pct_change()

    condition_a = df['close'] > df['rolling_max_high']
    condition_b = df['volume'] > df['ma_volume'] * vol_ratio
    condition_c = (df['close'] > df['open']) & (price_change > 0.03)

    df['signal'] = condition_a & condition_b & condition_c
    return df


_CN_COLUMNS = {'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价', 'volume': '成交量（万）', 'rolling_max_high': '历史最高价', 'ma_volume': '日均量'}


async def get_new_high_signals(stock_info: StockInfo, limit=400, lookback_window=60, vol_ma_window=50, vol_ratio=2.0) -> pd.DataFrame:
    """获取股票日K线并返回含信号列的 DataFrame，只保留有信号的行"""
    klines, vol_avg_records = await asyncio.gather(
        get_stock_day_range_kline(stock_info, limit=limit),
        get_volume_avg(stock_info, days=vol_ma_window, page_size=limit),
    )
    df = _build_dataframe(klines)
    vol_avg = pd.Series(
        {pd.Timestamp(r['date']): r['volume_avg'] * 10000 for r in vol_avg_records},
        name='ma_volume',
    )
    df['ma_volume'] = vol_avg.reindex(df.index)
    df = identify_new_high_signal(df, lookback_window, vol_ratio)
    return df[['open', 'close', 'high', 'low', 'volume', 'rolling_max_high', 'ma_volume', 'signal']]


async def get_new_high_signals_cn(stock_info: StockInfo, limit=400, lookback_window=60, vol_ma_window=50, vol_ratio=2.0) -> dict:
    """获取股票日K线信号，返回中文 key 的 JSON 结构"""
    df = await get_new_high_signals(stock_info, limit, lookback_window, vol_ma_window, vol_ratio)
    cols = ['open', 'close', 'high', 'low', 'volume', 'rolling_max_high', 'ma_volume']
    cn = {**_CN_COLUMNS, 'ma_volume': f'{vol_ma_window}日均量（万）'}

    def to_row(date, row):
        return {'日期': date.strftime('%Y-%m-%d'), **{cn[c]: round(row[c] / 10000, 2) if c in ('volume', 'ma_volume') else row[c] for c in cols}}

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')
    return {
        '最新交易日': latest_date,
        f'新量新价出新高（{latest_date}）': bool(latest['signal']),
        '历史信号列表': [to_row(date, row) for date, row in df[df['signal']].sort_index(ascending=False).iterrows()],
    }


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name


    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        import json
        signals = await get_new_high_signals_cn(stock_info)
        print(json.dumps(signals, ensure_ascii=False, indent=2))

    asyncio.run(main())
