import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from service.eastmoney.technical.stock_day_volume_avg import get_volume_avg
from common.utils.stock_info_utils import StockInfo


def build_dataframe(klines: list) -> pd.DataFrame:
    rows = []
    for kline in klines:
        fields = kline.split(',')
        rows.append({
            'date': fields[0],
            'open': float(fields[1]),
            'close': float(fields[2]),
            'high': float(fields[3]),
            'low': float(fields[4]),
            'volume': float(fields[5]),
            'pct_change': float(fields[8]),
        })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date')


async def inject_vol_avg(df: pd.DataFrame, stock_info: StockInfo, vol_ma_window=50, limit=400) -> None:
    """向 df 注入 ma50_volume 列（原地修改）"""
    vol_avg_records = await get_volume_avg(stock_info, days=vol_ma_window, page_size=limit)
    vol_avg = pd.Series(
        {pd.Timestamp(r['date']): r['volume_avg'] * 10000 for r in vol_avg_records},
        name='ma50_volume',
    )
    df['ma50_volume'] = vol_avg.reindex(df.index)


def is_high_vol_pillar(df: pd.DataFrame, vol_ratio: float = 2.0) -> pd.Series:
    """
    判断高量柱：成交量 > MA50均量×vol_ratio倍，阳线，涨幅>3%，且为近10日最大量
    依赖 df 中已存在 volume、ma50_volume、close、open、pct_change 列
    """
    return (
        (df['volume'] > df['ma50_volume'] * vol_ratio) &
        (df['close'] > df['open']) &
        (df['pct_change'] > 3) &
        (df['volume'] == df['volume'].rolling(10).max())
    )


async def get_high_vol_pillars(stock_info: StockInfo, limit=400, vol_ma_window=50, vol_ratio=2.0) -> pd.DataFrame:
    klines = await get_stock_day_range_kline(stock_info, limit=limit)
    df = build_dataframe(klines)
    await inject_vol_avg(df, stock_info, vol_ma_window, limit)
    mask = is_high_vol_pillar(df, vol_ratio)
    return df[mask][['open', 'close', 'high', 'low', 'volume', 'pct_change', 'ma50_volume']]


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('中国卫通')
        result = await get_high_vol_pillars(stock_info)
        for date, row in result.iterrows():
            print(f"[高量柱] {date.strftime('%Y-%m-%d')} 成交量={round(row['volume']/10000,2)}万 涨幅={row['pct_change']}%")

    asyncio.run(main())
