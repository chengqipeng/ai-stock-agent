import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline_by_db_cache
from service.eastmoney.technical.stock_day_boll import calculate_bollinger_bands
from common.utils.stock_info_utils import StockInfo
from service.eastmoney.strategy_engine.stock_is_high_vol_pillar import is_high_vol_pillar, build_dataframe, inject_vol_avg


async def identify_volume_reduction_pullback(stock_info: StockInfo, df: pd.DataFrame, vol_ma_window=50, vol_ratio=2.0, limit=400) -> pd.DataFrame:
    """
    缩量回调不用慌：识别健康洗盘形态
    条件 A：缩量达标（成交量 < 高量柱的50% 且 < 日均量）
    条件 B：防线安全（收盘价 > 高量柱最低价防守线）
    条件 C：支撑有效（收盘价 >= MA20）
    条件 D：K线可控（振幅 < 4% 且跌幅 < 2%）
    """
    boll_records, _ = await asyncio.gather(
        calculate_bollinger_bands(stock_info),
        inject_vol_avg(df, stock_info, vol_ma_window, limit),
    )
    boll_mb = pd.Series(
        {pd.Timestamp(r['date']): r['boll'] for r in boll_records},
        name='boll_mb',
    )
    df['boll_mb'] = boll_mb.reindex(df.index)
    prev_close = df['close'].shift(1)

    high_vol = is_high_vol_pillar(df, vol_ratio)

    print("-------------------------------")
    for date, row in df[high_vol].iterrows():
        print(f"[高量柱] {stock_info.stock_name} {date.strftime('%Y-%m-%d')} 成交量={round(row['volume']/10000,2)}万 涨幅={row['pct_change']}%")
    print("-------------------------------\n")

    df['prev_high_vol'] = df['volume'].where(high_vol).ffill()
    df['defense_line'] = df['low'].where(high_vol).ffill()
    df['high_vol_date'] = df.index.to_series().where(high_vol).ffill()

    cond_a = (df['volume'] < df['prev_high_vol'] * 0.5) & (df['volume'] < df['ma50_volume'])
    cond_b = (df['close'] > df['defense_line']) & df['defense_line'].notna()
    cond_c = df['close'] >= df['boll_mb']
    cond_d = ((df['high'] - df['low']) / prev_close) < 0.04
    df['cond_d'] = (df['high'] - df['low']) / prev_close
    df['prev_close'] = prev_close

    df['signal'] = cond_a & cond_b & cond_c & cond_d
    return df


_CN_COLUMNS = {
    'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价',
    'volume': '成交量（万）', 'boll_mb': 'BOLL中轨', 'defense_line': '防守线',
    'prev_high_vol': '高量柱成交量（万）',
}


async def get_volume_reduction_pullback(stock_info: StockInfo, limit=400, vol_ma_window=50, vol_ratio=2.0) -> pd.DataFrame:
    """获取股票日K线并返回含信号列的 DataFrame"""
    klines = await get_stock_day_range_kline_by_db_cache(stock_info, limit=limit)
    df = build_dataframe(klines)
    return await identify_volume_reduction_pullback(stock_info, df, vol_ma_window, vol_ratio, limit)


def _log_result(stock_name: str, raw_df: pd.DataFrame, calc_df: pd.DataFrame, result: dict, vol_ratio: float, vol_ma_window: int) -> None:
    import json
    print("\n========== 缩量回调信号日志 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_name}
策略：识别「缩量回调不用慌」健康洗盘形态，需同时满足以下4个条件，输出最新成交日是否满足和历史满足的前三个交易日：
  条件A（缩量达标）：当日成交量 < 高量柱成交量×50%，且 < {vol_ma_window}日均量
  条件B（防线安全）：收盘价 > 最近高量柱当日最低价（防守线），且防守线存在
  条件C（支撑有效）：收盘价 >= BOLL中轨（MA20）
  条件D（K线可控）：当日振幅（最高-最低）/ 前日收盘 < 4%
高量柱定义：成交量 > {vol_ma_window}日均量×{vol_ratio}倍，阳线，涨幅>3%，且为近10日最大量""")
    print("\n【原始K线数据】")
    cn_rename = {'date': '日期', 'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价', 'volume': '成交量', 'pct_change': '涨跌幅', 'ma50_volume': '50日均量'}
    display_df = raw_df.tail(250).copy()
    display_df['ma50_volume'] = calc_df['ma50_volume'].reindex(display_df.index)
    display_df = display_df.reset_index().rename(columns=cn_rename)
    display_df['日期'] = display_df['日期'].dt.strftime('%Y-%m-%d')
    print(display_df.to_json(orient='records', force_ascii=False))
    print("========================================\n")


async def get_volume_reduction_pullback_cn(stock_info: StockInfo, limit=400, vol_ma_window=50, vol_ratio=2.0) -> dict:
    """获取缩量回调信号，返回中文 key 的 JSON 结构"""
    klines = await get_stock_day_range_kline_by_db_cache(stock_info, limit=limit)
    raw_df = build_dataframe(klines)
    df = raw_df.copy()
    df = await identify_volume_reduction_pullback(stock_info, df, vol_ma_window, vol_ratio, limit)
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
    result = {
        '最新交易日': latest_date,
        f'缩量回调（{latest_date}）': bool(latest['signal']),
        '历史信号列表（最近3次）': [to_row(date, row) for date, row in df[df['signal']].sort_index(ascending=False).head(3).iterrows()],
    }
    _log_result(stock_info.stock_name, raw_df, df, result, vol_ratio, vol_ma_window)
    return result


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        import json
        result = await get_volume_reduction_pullback_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False))

    asyncio.run(main())
