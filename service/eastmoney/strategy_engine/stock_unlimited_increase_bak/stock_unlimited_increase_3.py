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
    """
    无量上涨必须跑（诱多/背离）识别策略
    条件 A（高位判定）：收盘价 > min(BOLL中轨（MA20）* 1.15, 布林上轨 * 0.95)，偏离15%以上或接近上轨视为高位
    条件 B（价格上涨）：涨跌幅 > 0
    条件 C（无量萎缩）：成交量 < vol_ma_window日均量 * vol_shrink_ratio
    条件 D（阶梯缩量）：连续3日价涨量减（量依次递减）
    条件 E（结构背离）：创20日新高（收盘价 > 前19日最高收盘价），但当日量 < 上一个20日新高日的成交量
                       与原版区别：原版取同一rolling窗口内峰值量；此版跨窗口追踪历史新高日，背离判断更严格
    最终信号：A AND ( (B AND C) OR D OR E )，A为前提，内层任一成立即触发警示
    """
    # 条件 A（高位判定）
    high_threshold = df['boll_mb'] * high_pos_ratio
    high_threshold = high_threshold.where(high_threshold < df['boll_ub'] * 0.95, df['boll_ub'] * 0.95)
    cond_a = df['close'] > high_threshold

    # 条件 B（价格上涨）
    cond_b = df['pct_change'] > 0

    # 条件 C（无量萎缩）
    cond_c = df['volume'] < df['ma50_volume'] * vol_shrink_ratio

    # 条件 D：阶梯缩量（连续3日价涨量减）
    price_up = df['pct_change'] > 0
    cond_d = (
        price_up & price_up.shift(1) & price_up.shift(2) &
        (df['volume'] < df['volume'].shift(1)) &
        (df['volume'].shift(1) < df['volume'].shift(2))
    )

    # 条件 E：结构背离
    max_prev19 = df['close'].shift(1).rolling(window=19, min_periods=1).max()
    is_new_high = df['close'] > max_prev19
    cond_e = pd.Series(False, index=df.index)
    prev_high_volume = None
    for idx in df.index:
        if is_new_high[idx]:
            if prev_high_volume is not None and df.loc[idx, 'volume'] < prev_high_volume:
                cond_e[idx] = True
            prev_high_volume = df.loc[idx, 'volume']

    df['cond_ab_c'] = cond_a & cond_b & cond_c
    df['cond_d'] = cond_d
    df['cond_e'] = cond_e
    df['signal'] = cond_a & ((cond_b & cond_c) | cond_d | cond_e)
    return df


_CN_COLUMNS = {
    'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价',
    'volume': '成交量（万）', 'pct_change': '涨跌幅（%）',
    'boll_mb': 'BOLL中轨', 'ma50_volume': '日均量（万）',
}


def _log_result(stock_name: str, raw_df: pd.DataFrame, calc_df: pd.DataFrame, vol_shrink_ratio: float, vol_ma_window: int, high_pos_ratio: float) -> None:
    print("\n========== 无量上涨诱多/背离信号日志 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_name}
策略：识别「无量上涨必须跑」诱多/背离形态，满足以下任一条件即触发警示，输出最新成交日是否满足和历史满足的前三个交易日：
  条件A（高位判定，必须满足）：收盘价 > BOLL中轨×{high_pos_ratio}
  条件B+C（高位无量上涨）：涨跌幅>0 且 成交量 < {vol_ma_window}日均量×{vol_shrink_ratio}
  条件D（阶梯缩量）：连续3日价涨，且成交量依次递减
  条件E（结构背离）：创20日新高，但成交量 < 上一个20日新高日的成交量
  最终信号：A AND ( (B AND C) OR D OR E )""")
    print("\n【原始K线数据（最近250日）】")
    display_df = raw_df.tail(250).copy()
    display_df['ma50_volume'] = calc_df['ma50_volume'].reindex(display_df.index)
    display_df['boll_mb'] = calc_df['boll_mb'].reindex(display_df.index)
    display_df = display_df.reset_index().rename(columns={
        'date': '日期', 'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价',
        'volume': '成交量', 'pct_change': '涨跌幅', f'ma50_volume': f'{vol_ma_window}日均量', 'boll_mb': 'BOLL中轨',
    })
    display_df['日期'] = display_df['日期'].dt.strftime('%Y-%m-%d')
    print(display_df.to_json(orient='records', force_ascii=False, indent=2))
    print("==========================================\n")


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
    df['boll_ub'] = pd.Series(
        {pd.Timestamp(r['date']): r['boll_ub'] for r in boll_records},
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
    result = {
        '最新交易日': latest_date,
        f'无量上涨诱多背离（{latest_date}）': bool(latest['signal']),
        '历史信号列表（最近3次）': [
            to_row(date, row)
            for date, row in df[df['signal']].sort_index(ascending=False).head(3).iterrows()
        ],
    }
    _log_result(stock_info.stock_name, raw_df, df, vol_shrink_ratio, vol_ma_window, high_pos_ratio)
    return result


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('三花智控')
        import json
        result = await get_unlimited_increase_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
