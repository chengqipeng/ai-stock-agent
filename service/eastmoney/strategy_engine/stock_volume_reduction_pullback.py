import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
from service.eastmoney.technical.stock_day_volume_avg import get_volume_avg
from service.eastmoney.technical.stock_day_boll import calculate_bollinger_bands
from common.utils.stock_info_utils import StockInfo


def _build_dataframe(klines: list) -> pd.DataFrame:
    rows = []
    for kline in klines:
        fields = kline.split(',')
        rows.append({
            'date':  fields[0],
            'open':  float(fields[1]),
            'close': float(fields[2]),
            'high':  float(fields[3]),
            'low':   float(fields[4]),
            'volume': float(fields[5]),
            'pct_change': float(fields[8]),
        })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date')


async def identify_volume_reduction_pullback(stock_info: StockInfo, df: pd.DataFrame, vol_ma_window=50, vol_ratio=2.0, limit=400) -> pd.DataFrame:
    """
    缩量回调不用慌：识别健康洗盘形态
    条件 A：缩量达标（成交量 < 高量柱的50% 且 < 日均量）
    条件 B：防线安全（收盘价 > 高量柱最低价防守线）
    条件 C：支撑有效（收盘价 >= MA20）
    条件 D：K线可控（振幅 < 4% 且跌幅 < 2%）
    """
    vol_avg_records, boll_records = await asyncio.gather(
        get_volume_avg(stock_info, days=vol_ma_window, page_size=limit),
        calculate_bollinger_bands(stock_info),
    )
    vol_avg = pd.Series(
        {pd.Timestamp(r['date']): r['volume_avg'] * 10000 for r in vol_avg_records},
        name='ma_volume',
    )
    df['ma_volume'] = vol_avg.reindex(df.index)
    boll_mb = pd.Series(
        {pd.Timestamp(r['date']): r['boll'] for r in boll_records},
        name='boll_mb',
    )
    df['boll_mb'] = boll_mb.reindex(df.index)
    prev_close = df['close'].shift(1)

    is_high_vol_pillar = (
        (df['volume'] > df['ma_volume'] * vol_ratio) &
        (df['close'] > df['open']) &
        (df['pct_change'] > 3)
        # (df['close'] / prev_close > 1.03)
    )

    df['prev_high_vol'] = df['volume'].where(is_high_vol_pillar).ffill()
    df['defense_line'] = df['low'].where(is_high_vol_pillar).ffill()
    df['high_vol_date'] = df.index.to_series().where(is_high_vol_pillar).ffill()

    cond_a = (df['volume'] < df['prev_high_vol'] * 0.5) & (df['volume'] < df['ma_volume'])
    cond_b = (df['close'] > df['defense_line']) & df['defense_line'].notna()
    cond_c = df['close'] >= df['boll_mb']
    cond_d = (df['high'] - df['low']) / prev_close < 0.04

    df['signal'] = cond_a & cond_b & cond_c & cond_d
    return df


_CN_COLUMNS = {
    'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价',
    'volume': '成交量（万）', 'boll_mb': 'BOLL中轨', 'defense_line': '防守线',
    'prev_high_vol': '高量柱成交量（万）',
}


async def get_volume_reduction_pullback(stock_info: StockInfo, limit=400, vol_ma_window=50, vol_ratio=2.0) -> pd.DataFrame:
    """获取股票日K线并返回含信号列的 DataFrame"""
    klines = await get_stock_day_range_kline(stock_info, limit=limit)
    df = _build_dataframe(klines)
    return await identify_volume_reduction_pullback(stock_info, df, vol_ma_window, vol_ratio, limit)


async def get_volume_reduction_pullback_cn(stock_info: StockInfo, limit=400, vol_ma_window=50, vol_ratio=2.0) -> dict:
    """获取缩量回调信号，返回中文 key 的 JSON 结构"""
    df = await get_volume_reduction_pullback(stock_info, limit, vol_ma_window, vol_ratio)
    cols = ['open', 'close', 'high', 'low', 'volume', 'boll_mb', 'defense_line', 'prev_high_vol']
    cn = {**_CN_COLUMNS, 'prev_high_vol': f'高量柱成交量（万）'}

    def to_row(date, row):
        return {
            '日期': date.strftime('%Y-%m-%d'),
            **{cn[c]: round(row[c] / 10000, 2) if c in ('volume', 'prev_high_vol') else round(row[c], 2) for c in cols},
            '高量柱日期': pd.Timestamp(row['high_vol_date']).strftime('%Y-%m-%d') if pd.notna(row['high_vol_date']) else None,
        }

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')
    return {
        '最新交易日': latest_date,
        f'缩量回调（{latest_date}）': bool(latest['signal']),
        '历史信号列表（最近3次）': [to_row(date, row) for date, row in df[df['signal']].sort_index(ascending=False).head(3).iterrows()],
    }


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('中国卫通')
        import json
        result = await get_volume_reduction_pullback_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
