import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline_by_db_cache as get_stock_day_range_kline
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


def identify_unlimited_increase(df: pd.DataFrame, vol_ma_window=50, high_pos_ratio=1.15, vol_shrink_ratio=0.8) -> pd.DataFrame:
    """
    无量上涨必须跑（诱多/背离）识别策略
    条件 A（高位判定）：收盘价 > BOLL中轨（MA20）* 1.15，偏离15%以上视为高位
    条件 B（价格上涨）：收盘价 > 前日收盘价
    条件 C（无量萎缩）：成交量 < ma_volume * 0.8
    条件 D（阶梯缩量）：连续3日价涨量减（量依次递减）
    条件 E（结构背离）：创20日新高，但当日量 < 过去20日收盘价最高点当天的成交量
    最终信号：(A & B & C) | D | E，任一成立即触发警示
    """
    prev_close = df['close'].shift(1)

    # 条件 A：高位（偏离BOLL中轨15%以上）
    cond_a = df['close'] > (df['boll_mb'] * high_pos_ratio)

    # 条件 B：价格上涨
    price_up = df['close'] > prev_close

    # 条件 C：无量（成交量 < 均量 * 0.8）
    cond_c = df['volume'] < (df['ma50_volume'] * vol_shrink_ratio)

    # 条件 D：阶梯型缩量（连续3日价涨量减）
    cond_d = (
        price_up & price_up.shift(1) & price_up.shift(2) &
        (df['volume'] < df['volume'].shift(1)) &
        (df['volume'].shift(1) < df['volume'].shift(2))
    )

    # 条件 E：结构性背离（创20日新高，但当日量 < 阶段高点当天的成交量）
    rolling_max_close_20 = df['close'].rolling(window=20).max()
    is_new_high_20 = df['close'] >= rolling_max_close_20
    # 找到过去20日窗口内收盘价最高点的日期，取对应的成交量
    peak_vol_20 = pd.Series(
        [
            df['volume'].iloc[max(0, i - 19): i + 1].loc[
                df['close'].iloc[max(0, i - 19): i + 1].idxmax()
            ]
            for i in range(len(df))
        ],
        index=df.index,
    )
    cond_e = is_new_high_20 & (df['volume'] < peak_vol_20)

    df['cond_ab_c'] = cond_a & price_up & cond_c & cond_e
    df['cond_d'] = cond_d
    df['cond_e'] = cond_e
    df['signal'] = df['cond_ab_c'] | cond_d
    return df


_CN_COLUMNS = {
    'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价',
    'volume': '成交量（万）', 'pct_change': '涨跌幅（%）',
    'boll_mb': 'BOLL中轨', 'ma50_volume': '日均量（万）',
    'high_20': '20日最高价', 'peak_date_20': '20日最高收盘价日期', 'peak_vol_20': '20日最高收盘价当日量（万）',
}


def _log_result(stock_name: str, raw_df: pd.DataFrame, calc_df: pd.DataFrame, vol_shrink_ratio: float, vol_ma_window: int, high_pos_ratio: float) -> None:
    print("\n========== 无量上涨诱多/背离信号日志 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_name}
策略：识别「无量上涨必须跑」诱多/背离形态，满足以下任一条件即触发警示，输出最新成交日是否满足和历史满足的前三个交易日：
  条件A+B+C（高位无量上涨）：收盘价 > BOLL中轨×{high_pos_ratio} 且 价格上涨 且 成交量 < {vol_ma_window}日均量×{vol_shrink_ratio}
  条件D（阶梯缩量）：连续3日价涨，且成交量依次递减
  条件E（结构背离）：创20日新高，但成交量 < 阶段高点当天的成交量""")
    print("\n【原始K线数据（最近250日）】")
    cn_rename = {**{'date': '日期', 'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价', 'volume': '成交量', 'pct_change': '涨跌幅', 'ma50_volume': f'{vol_ma_window}日均量', 'boll_mb': 'BOLL中轨'}, **{k: v for k, v in _CN_COLUMNS.items() if k in ('high_20', 'peak_date_20', 'peak_vol_20')}}
    display_df = raw_df.tail(250).copy()
    display_df['ma50_volume'] = calc_df['ma50_volume'].reindex(display_df.index)
    display_df['boll_mb'] = calc_df['boll_mb'].reindex(display_df.index)
    display_df['high_20'] = calc_df['high_20'].reindex(display_df.index)
    display_df['peak_date_20'] = calc_df['peak_date_20'].reindex(display_df.index)
    display_df['peak_vol_20'] = calc_df['peak_vol_20'].reindex(display_df.index)
    display_df = display_df.reset_index().rename(columns=cn_rename)
    display_df['日期'] = display_df['日期'].dt.strftime('%Y-%m-%d')
    print(display_df.to_json(orient='records', force_ascii=False))
    print("==========================================\n")


async def get_unlimited_increase(stock_info: StockInfo, limit=400, vol_ma_window=50, high_pos_ratio=1.15, vol_shrink_ratio=0.8) -> tuple:
    """获取股票日K线并返回含信号列的 DataFrame"""
    klines, vol_avg_records, boll_records = await asyncio.gather(
        get_stock_day_range_kline(stock_info, limit=limit),
        get_volume_avg(stock_info, days=vol_ma_window, page_size=limit),
        calculate_bollinger_bands(stock_info),
    )
    raw_df = _build_dataframe(klines)
    df = raw_df.copy()
    df['ma50_volume'] = pd.Series(
        {pd.Timestamp(r['date']): r['volume_avg'] * 10000 for r in vol_avg_records},
        name='ma50_volume',
    ).reindex(df.index)
    df['boll_mb'] = pd.Series(
        {pd.Timestamp(r['date']): r['boll'] for r in boll_records},
        name='boll_mb',
    ).reindex(df.index)
    df = identify_unlimited_increase(df, vol_ma_window, high_pos_ratio, vol_shrink_ratio)
    df['high_20'] = raw_df['high'].rolling(window=20).max()
    peak_idx = [raw_df['close'].iloc[max(0, i - 19): i + 1].idxmax() for i in range(len(raw_df))]
    df['peak_date_20'] = [idx.strftime('%Y-%m-%d') for idx in peak_idx]
    df['peak_vol_20'] = [raw_df['volume'].loc[idx] for idx in peak_idx]
    return raw_df, df


async def get_unlimited_increase_cn(stock_info: StockInfo, limit=400, vol_ma_window=50, high_pos_ratio=1.15, vol_shrink_ratio=0.8) -> dict:
    """获取无量上涨诱多/背离信号，返回中文 key 的 JSON 结构"""
    raw_df, df = await get_unlimited_increase(stock_info, limit, vol_ma_window, high_pos_ratio, vol_shrink_ratio)
    cols = ['open', 'close', 'high', 'low', 'volume', 'pct_change', 'boll_mb', 'ma50_volume', 'high_20', 'peak_date_20', 'peak_vol_20']
    vol_cols = {'volume', 'ma50_volume', 'peak_vol_20'}

    def to_row(date, row):
        triggers = []
        if row.get('cond_ab_c'): triggers.append('高位无量上涨(A+B+C)')
        if row.get('cond_d'):    triggers.append('阶梯缩量(D)')
        if row.get('cond_e'):    triggers.append('结构背离(E)')
        return {
            '日期': date.strftime('%Y-%m-%d'),
            **{_CN_COLUMNS[c]: (row[c] if c == 'peak_date_20' else round(row[c] / 10000, 2) if c in vol_cols else round(row[c], 2)) for c in cols},
            '触发条件': '、'.join(triggers),
        }

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')
    result = {
        '最新交易日': latest_date,
        f'无量上涨诱多背离（{latest_date}）': bool(latest['signal']),
        '历史信号列表（最近3次）': [to_row(date, row) for date, row in df[df['signal']].sort_index(ascending=False).head(3).iterrows()],
    }
    _log_result(stock_info.stock_name, raw_df, df, vol_shrink_ratio, vol_ma_window, high_pos_ratio)
    return result


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        import json
        result = await get_unlimited_increase_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False))

    asyncio.run(main())
