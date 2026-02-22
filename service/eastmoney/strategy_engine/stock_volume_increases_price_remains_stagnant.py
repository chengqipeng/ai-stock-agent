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


async def identify_distribution_signal(stock_info: StockInfo, df: pd.DataFrame, vol_ma_window=50, vol_ratio=2.0, limit=400) -> pd.DataFrame:
    """
    放量滞涨要当心（派发）：识别主力出货形态
    条件 A：高位前提（距60日最低价涨幅 > 30% 且触及布林上轨）
    条件 B：极其放量（成交量 > 50日均量×2倍 且 > 昨日量×1.5倍）
    条件 C：价格滞涨（涨跌幅绝对值 < 1.5% 或 长上影线 或 收绿）
    """
    vol_avg_records, boll_records = await asyncio.gather(
        get_volume_avg(stock_info, days=vol_ma_window, page_size=limit),
        calculate_bollinger_bands(stock_info),
    )
    df['ma50_volume'] = pd.Series(
        {pd.Timestamp(r['date']): r['volume_avg'] * 10000 for r in vol_avg_records},
        name='ma50_volume',
    ).reindex(df.index)
    df['boll_up'] = pd.Series(
        {pd.Timestamp(r['date']): r['boll_ub'] for r in boll_records},
        name='boll_up',
    ).reindex(df.index)

    df['low_60d'] = df['low'].rolling(window=60).min()

    # 条件 A：高位前提
    cond_a = (df['close'] > df['low_60d'] * 1.3) & (df['close'] >= df['boll_up'])

    # 条件 B：极其放量
    cond_b = df['volume'] > df['ma50_volume'] * vol_ratio

    # 条件 C：价格滞涨
    # cond_c = (df['close'] - df['close'].shift(1)).abs() / df['close'].shift(1) < 0.015

    cond_c = df['pct_change'].abs() < 1.5

    df['signal'] = cond_a & cond_b & cond_c
    return df


_CN_COLUMNS = {
    'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价',
    'volume': '成交量（万）', 'boll_up': 'BOLL上轨', 'low_60d': '60日最低价',
    'ma50_volume': '50日均量（万）',
}


def _log_result(stock_name: str, raw_df: pd.DataFrame, calc_df: pd.DataFrame, result: dict, vol_ratio: float, vol_ma_window: int) -> None:
    print("\n========== 放量滞涨派发信号日志 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_name}
策略：识别「放量滞涨要当心（派发）」主力出货形态，需同时满足以下3个条件，输出最新成交日是否满足和历史满足的前三个交易日：
  条件A（高位前提）：收盘价 > 60日最低价×1.3 且 收盘价 >= BOLL上轨
  条件B（极其放量）：成交量 > {vol_ma_window}日均量×{vol_ratio}倍
  条件C（价格滞涨）：涨跌幅绝对值<1.5%""")
    print("\n【原始K线数据】")
    cn_rename = {'date': '日期', 'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价', 'volume': '成交量', 'pct_change': '涨跌幅', 'ma50_volume': '50日均量', 'low_60d': '60日最低价', 'boll_up': 'BOLL上轨'}
    display_df = raw_df.tail(250).copy()
    display_df['ma50_volume'] = calc_df['ma50_volume'].reindex(display_df.index)
    display_df['low_60d'] = calc_df['low_60d'].reindex(display_df.index)
    display_df['boll_up'] = calc_df['boll_up'].reindex(display_df.index)
    display_df = display_df.reset_index().rename(columns=cn_rename)
    display_df['日期'] = display_df['日期'].dt.strftime('%Y-%m-%d')
    print(display_df.to_json(orient='records', force_ascii=False, indent=2))
    print("==========================================\n")


async def get_distribution_signal_cn(stock_info: StockInfo, limit=400, vol_ma_window=50, vol_ratio=2.0) -> dict:
    """获取放量滞涨派发信号，返回中文 key 的 JSON 结构"""
    klines = await get_stock_day_range_kline(stock_info, limit=limit)
    raw_df = _build_dataframe(klines)
    df = raw_df.copy()
    df = await identify_distribution_signal(stock_info, df, vol_ma_window, vol_ratio, limit)

    cols = ['open', 'close', 'high', 'low', 'volume', 'boll_up', 'low_60d', 'ma50_volume']

    def to_row(date, row):
        return {
            '日期': date.strftime('%Y-%m-%d'),
            **{_CN_COLUMNS[c]: round(row[c] / 10000, 2) if c in ('volume', 'ma50_volume') else round(row[c], 2) for c in cols},
        }

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')
    result = {
        '最新交易日': latest_date,
        f'放量滞涨派发（{latest_date}）': bool(latest['signal']),
        '历史信号列表（最近3次）': [to_row(date, row) for date, row in df[df['signal']].sort_index(ascending=False).head(3).iterrows()],
    }
    _log_result(stock_info.stock_name, raw_df, df, result, vol_ratio, vol_ma_window)
    return result


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        import json
        result = await get_distribution_signal_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
