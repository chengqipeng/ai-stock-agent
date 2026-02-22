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
            'date':       fields[0],
            'open':       float(fields[1]),
            'close':      float(fields[2]),
            'high':       float(fields[3]),
            'low':        float(fields[4]),
            'volume':     float(fields[5]),
            'pct_change': float(fields[8]),
        })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date')


def identify_unlimited_increase(df: pd.DataFrame, vol_ma_window=50, high_pos_ratio=1.15, vol_shrink_ratio=0.8) -> pd.DataFrame:
    # 条件 A+B+C：高位无量上涨
    cond_abc = (
        (df['close'] > df['boll_mb'] * high_pos_ratio) &
        (df['pct_change'] > 0) &
        (df['volume'] < df['ma50_volume'] * vol_shrink_ratio)
    )

    # 条件 D：阶梯缩量（连续3日价涨量减）
    price_up = df['pct_change'] > 0
    cond_d = (
        price_up & price_up.shift(1) & price_up.shift(2) &
        (df['volume'] < df['volume'].shift(1)) &
        (df['volume'].shift(1) < df['volume'].shift(2))
    )

    # 条件 E：结构背离（创20日新高，但量 < 上一个20日新高日的成交量）
    max_prev19 = df['close'].shift(1).rolling(window=19, min_periods=1).max()
    is_new_high = df['close'] > max_prev19
    cond_e = pd.Series(False, index=df.index)
    prev_high_volume = None
    for idx in df.index:
        if is_new_high[idx]:
            if prev_high_volume is not None and df.loc[idx, 'volume'] < prev_high_volume:
                cond_e[idx] = True
            prev_high_volume = df.loc[idx, 'volume']

    df['cond_ab_c'] = cond_abc
    df['cond_d'] = cond_d
    df['cond_e'] = cond_e
    df['signal'] = cond_abc | cond_d | cond_e
    return df


_CN_COLUMNS = {
    'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价',
    'volume': '成交量（万）', 'pct_change': '涨跌幅（%）',
    'boll_mb': 'BOLL中轨', 'ma50_volume': '日均量（万）',
}


async def get_unlimited_increase(stock_info: StockInfo, limit=400, vol_ma_window=50, high_pos_ratio=1.15, vol_shrink_ratio=0.8) -> tuple:
    klines, vol_avg_records, boll_records = await asyncio.gather(
        get_stock_day_range_kline(stock_info, limit=limit),
        get_volume_avg(stock_info, days=vol_ma_window, page_size=limit),
        calculate_bollinger_bands(stock_info),
    )
    raw_df = _build_dataframe(klines)
    df = raw_df.copy()
    df['ma50_volume'] = pd.Series(
        {pd.Timestamp(r['date']): r['volume_avg'] * 10000 for r in vol_avg_records},
    ).reindex(df.index)
    df['boll_mb'] = pd.Series(
        {pd.Timestamp(r['date']): r['boll'] for r in boll_records},
    ).reindex(df.index)
    df = identify_unlimited_increase(df, vol_ma_window, high_pos_ratio, vol_shrink_ratio)
    return raw_df, df


async def get_unlimited_increase_cn(stock_info: StockInfo, limit=400, vol_ma_window=50, high_pos_ratio=1.15, vol_shrink_ratio=0.8) -> dict:
    raw_df, df = await get_unlimited_increase(stock_info, limit, vol_ma_window, high_pos_ratio, vol_shrink_ratio)

    def to_row(date, row):
        triggers = []
        if row.get('cond_ab_c'): triggers.append('高位无量上涨(A+B+C)')
        if row.get('cond_d'):    triggers.append('阶梯缩量(D)')
        if row.get('cond_e'):    triggers.append('结构背离(E)')
        return {
            '日期': date.strftime('%Y-%m-%d'),
            **{_CN_COLUMNS[c]: round(row[c] / 10000, 2) if c in ('volume', 'ma50_volume') else round(row[c], 2)
               for c in ('open', 'close', 'high', 'low', 'volume', 'pct_change', 'boll_mb', 'ma50_volume')},
            '触发条件': '、'.join(triggers),
        }

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')
    return {
        '最新交易日': latest_date,
        f'无量上涨诱多背离（{latest_date}）': bool(latest['signal']),
        '历史信号列表（最近3次）': [
            to_row(date, row)
            for date, row in df[df['signal']].sort_index(ascending=False).head(3).iterrows()
        ],
    }


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('中国卫通')
        import json
        result = await get_unlimited_increase_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
