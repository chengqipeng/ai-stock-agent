import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline_by_db_cache as get_stock_day_range_kline
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
            'pct_change': float(fields[8]),
        })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date')


def identify_new_high_signal(df: pd.DataFrame, lookback_window=60, vol_ratio=2.0) -> pd.DataFrame:
    """
    量价协同突破策略：新量新价出新高
    条件 A：收盘价突破过去 lookback_window 日最高价
    条件 B：成交量 > 日均量 * vol_ratio（ma_volume 由外部传入）
    条件 C：中阳/大阳线（涨幅 > 3%，上影线 < 实体的 20%，排除长上影十字星）
    """
    df['rolling_max_high'] = df['high'].shift(1).rolling(window=lookback_window).max()

    condition_a = df['close'] > df['rolling_max_high']
    condition_b = df['volume'] > df['ma_volume'] * vol_ratio
    condition_c = (
        (df['close'] > df['open']) &
        (df['pct_change'] > 3)
    )

    df['signal'] = condition_a & condition_b & condition_c
    return df


_CN_COLUMNS = {'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价', 'volume': '成交量（万）', 'pct_change': '涨跌幅（%）', 'rolling_max_high': '历史最高价', 'ma_volume': '日均量'}


def _log_result(stock_name: str, raw_df: pd.DataFrame, calc_df: pd.DataFrame, result: dict, vol_ratio: float, vol_ma_window: int, lookback_window: int) -> None:
    print("\n========== 新量新价出新高信号日志 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_name}
策略：量价协同突破，需同时满足以下3个条件，输出最新成交日是否满足和历史满足的前三个交易日：
  条件A（价格突破）：收盘价突破过去{lookback_window}日最高价
  条件B（放量配合）：成交量 > {vol_ma_window}日均量×{vol_ratio}倍
  条件C（阳线确认）：阳线且涨幅 > 3%""")
    print("\n【原始K线数据】")
    cn_rename = {'date': '日期', 'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价', 'volume': '成交量', 'pct_change': '涨跌幅', 'ma_volume': f'{vol_ma_window}日均量'}
    display_df = raw_df.tail(250).copy()
    display_df['ma_volume'] = calc_df['ma_volume'].reindex(display_df.index)
    display_df = display_df.reset_index().rename(columns=cn_rename)
    display_df['日期'] = display_df['日期'].dt.strftime('%Y-%m-%d')
    print(display_df.to_json(orient='records', force_ascii=False, indent=2))
    print("========================================\n")


async def get_new_high_signals(stock_info: StockInfo, limit=400, lookback_window=60, vol_ma_window=50, vol_ratio=2.0) -> pd.DataFrame:
    """获取股票日K线并返回含信号列的 DataFrame，只保留有信号的行"""
    klines, vol_avg_records = await asyncio.gather(
        get_stock_day_range_kline(stock_info, limit=limit),
        get_volume_avg(stock_info, days=vol_ma_window, page_size=limit),
    )
    raw_df = _build_dataframe(klines)
    df = raw_df.copy()
    vol_avg = pd.Series(
        {pd.Timestamp(r['date']): r['volume_avg'] * 10000 for r in vol_avg_records},
        name='ma_volume',
    )
    df['ma_volume'] = vol_avg.reindex(df.index)
    df = identify_new_high_signal(df, lookback_window, vol_ratio)
    return raw_df, df[['open', 'close', 'high', 'low', 'volume', 'pct_change', 'rolling_max_high', 'ma_volume', 'signal']]


async def get_new_high_signals_cn(stock_info: StockInfo, limit=400, lookback_window=60, vol_ma_window=50, vol_ratio=2.0) -> dict:
    """获取股票日K线信号，返回中文 key 的 JSON 结构"""
    raw_df, df = await get_new_high_signals(stock_info, limit, lookback_window, vol_ma_window, vol_ratio)
    cols = ['open', 'close', 'high', 'low', 'volume', 'pct_change', 'rolling_max_high', 'ma_volume']
    cn = {**_CN_COLUMNS, 'ma_volume': f'{vol_ma_window}日均量（万）'}

    def to_row(date, row):
        return {'日期': date.strftime('%Y-%m-%d'), **{cn[c]: round(row[c] / 10000, 2) if c in ('volume', 'ma_volume') else row[c] for c in cols}}

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')
    result = {
        '最新交易日': latest_date,
        f'新量新价出新高（{latest_date}）': bool(latest['signal']),
        '历史信号列表（最近3次）': [to_row(date, row) for date, row in df[df['signal']].sort_index(ascending=False).head(3).iterrows()],
    }
    _log_result(stock_info.stock_name, raw_df, df, result, vol_ratio, vol_ma_window, lookback_window)
    return result


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name


    async def main():
        stock_info: StockInfo = get_stock_info_by_name('中国卫通')
        import json
        signals = await get_new_high_signals_cn(stock_info)
        print(json.dumps(signals, ensure_ascii=False, indent=2))

    asyncio.run(main())
