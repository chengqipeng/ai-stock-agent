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


async def identify_boll_rule(stock_info: StockInfo, df: pd.DataFrame, vol_ma_window=50, limit=400, bw_window=60) -> pd.DataFrame:
    """
    布林线法则 (运行空间) 核心逻辑
    强势开启信号：放量突破中轨（昨收 <= 昨中轨 且 今收 > 今中轨 且 量 > 50日均量×1.5）
    波段结束信号：跌破中轨（昨收 >= 昨中轨 且 今收 < 今中轨）
    可操作区：收盘 > 中轨 且 中轨向上倾斜
    喇叭口扩张加速：上下轨反向张开 且 带宽单日放大超10%
    """
    vol_avg_records, boll_records = await asyncio.gather(
        get_volume_avg(stock_info, days=vol_ma_window, page_size=limit),
        calculate_bollinger_bands(stock_info),
    )

    vol_avg = pd.Series(
        {pd.Timestamp(r['date']): r['volume_avg'] * 10000 for r in vol_avg_records},
        name='ma50_volume',
    )
    df['ma50_volume'] = vol_avg.reindex(df.index)

    boll_df = pd.DataFrame(boll_records).set_index(
        pd.to_datetime([r['date'] for r in boll_records])
    )
    df['MB'] = pd.Series({pd.Timestamp(r['date']): r['boll']    for r in boll_records}).reindex(df.index)
    df['UP'] = pd.Series({pd.Timestamp(r['date']): r['boll_ub'] for r in boll_records}).reindex(df.index)
    df['DN'] = pd.Series({pd.Timestamp(r['date']): r['boll_lb'] for r in boll_records}).reindex(df.index)

    df['BW'] = (df['UP'] - df['DN']) / df['MB']

    df['prev_close'] = df['close'].shift(1)
    df['prev_MB']    = df['MB'].shift(1)
    df['prev_UP']    = df['UP'].shift(1)
    df['prev_DN']    = df['DN'].shift(1)
    df['prev_BW']    = df['BW'].shift(1)

    # 可操作区 / 弱势区
    df['is_operable_zone'] = (df['close'] > df['MB']) & (df['MB'] > df['prev_MB'])
    df['is_weak_zone']     = df['close'] < df['MB']

    # 带宽挤压 & 喇叭口扩张
    df['min_bw'] = df['BW'].rolling(window=bw_window).min()
    df['is_squeeze']    = df['BW'] <= (df['min_bw'] * 1.1)
    df['is_expanding']  = (
        (df['UP'] > df['prev_UP']) &
        (df['DN'] < df['prev_DN']) &
        (df['BW'] > df['prev_BW'] * 1.1)
    )
    df['is_accelerating_up']   = df['is_expanding'] & df['is_operable_zone']
    df['is_accelerating_down'] = df['is_expanding'] & df['is_weak_zone']

    # 强势开启：放量突破中轨
    df['strong_start_signal'] = (
        (df['prev_close'] <= df['prev_MB']) &
        (df['close']      >  df['MB']) &
        (df['volume']     >  df['ma50_volume'] * 1.5)
    )

    # 波段结束：跌破中轨
    df['wave_end_signal'] = (
        (df['prev_close'] >= df['prev_MB']) &
        (df['close']      <  df['MB'])
    )

    return df


async def get_boll_rule(stock_info: StockInfo, limit=400, vol_ma_window=50) -> pd.DataFrame:
    klines = await get_stock_day_range_kline(stock_info, limit=limit)
    df = _build_dataframe(klines)
    return await identify_boll_rule(stock_info, df, vol_ma_window, limit)


def _log_result(stock_name: str, df: pd.DataFrame, vol_ma_window: int) -> None:
    print("\n========== 布林线法则信号日志 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_name}
策略：布林线法则（运行空间），识别以下信号：
  强势开启：昨收 <= 昨中轨 且 今收 > 今中轨 且 成交量 > {vol_ma_window}日均量×1.5倍
  波段结束：昨收 >= 昨中轨 且 今收 < 今中轨
  可操作区：收盘 > 中轨 且 中轨向上倾斜
  喇叭口加速上行：上下轨反向张开 且 带宽单日放大>10% 且 处于可操作区""")
    print("========================================\n")


async def get_boll_rule_cn(stock_info: StockInfo, limit=400, vol_ma_window=50) -> dict:
    """获取布林线法则信号，返回中文 key 的 JSON 结构"""
    klines = await get_stock_day_range_kline(stock_info, limit=limit)
    df = _build_dataframe(klines)
    df = await identify_boll_rule(stock_info, df, vol_ma_window, limit)

    def to_row(date, row):
        return {
            '日期':    date.strftime('%Y-%m-%d'),
            '收盘价':  round(row['close'], 2),
            'BOLL中轨': round(row['MB'], 2),
            'BOLL上轨': round(row['UP'], 2),
            'BOLL下轨': round(row['DN'], 2),
            '带宽':    round(row['BW'], 4),
            '成交量（万）': round(row['volume'] / 10000, 2),
            '50日均量（万）': round(row['ma50_volume'] / 10000, 2) if pd.notna(row['ma50_volume']) else None,
            '可操作区': bool(row['is_operable_zone']),
            '喇叭口加速上行': bool(row['is_accelerating_up']),
        }

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')

    strong_start_rows = df[df['strong_start_signal']].sort_index(ascending=False).head(3)
    wave_end_rows     = df[df['wave_end_signal']].sort_index(ascending=False).head(3)

    result = {
        '最新交易日': latest_date,
        f'强势开启信号（{latest_date}）': bool(latest['strong_start_signal']),
        f'波段结束信号（{latest_date}）': bool(latest['wave_end_signal']),
        f'可操作区（{latest_date}）':    bool(latest['is_operable_zone']),
        f'喇叭口加速上行（{latest_date}）': bool(latest['is_accelerating_up']),
        '历史强势开启信号（最近3次）': [to_row(date, row) for date, row in strong_start_rows.iterrows()],
        '历史波段结束信号（最近3次）': [to_row(date, row) for date, row in wave_end_rows.iterrows()],
    }
    _log_result(stock_info.stock_name, df, vol_ma_window)
    return result


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        import json
        result = await get_boll_rule_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
