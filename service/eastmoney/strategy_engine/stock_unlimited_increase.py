import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline_by_db_cache
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


def identify_unlimited_increase(df: pd.DataFrame, vol_ma_window=50, atr_window=14, vol_shrink_ratio=0.8, rsi_window=14, rsi_overbought=70, watch_days=5) -> pd.DataFrame:
    """
    无量上涨必须跑（诱多/背离）识别策略 V2.0
    条件 A（高危环境）：分支1: 收盘价 > min(BOLL中轨 + 2×ATR, BOLL上轨×0.95)（高位滞涨区）
                       分支2: 收盘价 < BOLL中轨 且 BOLL中轨斜率 < 0（下跌中继反弹）
    条件 B（价格形态）：0 < 涨跌幅 < 5%；可选强化1：收盘价 < 开盘价（假阴真阳，诱多嫌疑加倍）
                                          可选强化2：上影线 > 实体×2 且 上影线 > ATR×0.5（射击之星，确定性提升至95%）
    条件 C（单日截面骤缩）：成交量 < vol_ma_window日均量×vol_shrink_ratio 或 成交量 < 昨日成交量×0.7
    条件 D（短期动能衰减）：近3日累计涨跌幅 > 0 且 T日量 < T-2日量 且 T日收 > T-2日收
    条件 E（波段结构顶背离）：收盘价 > 昨日起前19日最高收盘价（创20日新高），但当日量 < 上一个20日新高日的成交量（峰对峰威科夫顶背离）
    辅助验证 F（RSI超买背离）：RSI(rsi_window) > rsi_overbought 且 RSI < 前一个RSI高点（价格创新高但RSI未创新高）
    退出机制：signal触发后进入watch_days日观察期（watch_period倒计时），期间收盘价未跌破信号日收盘价×0.97则为横盘观察，否则确认下跌
    最终信号：(A & B) & (C | D | E)；signal_confirmed = signal & F（高置信度）
    """
    # ATR（真实波动幅度均值）
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift(1)).abs(),
        (df['low'] - df['close'].shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=atr_window, min_periods=1).mean()
    df['atr'] = atr

    # 条件 B：0 < 涨跌幅 < 5%；可选强化：阴线（收 < 开）、射击之星（上影线 > 实体×2 且 上影线 > ATR×0.5）
    cond_b = (df['pct_change'] > 0) & (df['pct_change'] < 5)
    body = (df['close'] - df['open']).abs()
    upper_shadow = df['high'] - df[['close', 'open']].max(axis=1)
    df['cond_b_bearish_candle'] = cond_b & (df['close'] < df['open'])
    df['cond_b_shooting_star'] = cond_b & (upper_shadow > body * 2) & (upper_shadow > atr * 0.5)

    # 条件 A：高危环境（两个分支任一满足）
    # 分支1：高位滞涨区 — 收盘价 > min(BOLL中轨 + 2×ATR, BOLL上轨×0.95)
    high_threshold = (df['boll_mb'] + 2 * atr).combine(df['boll_ub'] * 0.95, min)
    df['high_threshold'] = high_threshold
    cond_a_high = df['close'] > high_threshold
    # 分支2：下跌中继反弹 — 收盘价 < BOLL中轨 且 BOLL中轨斜率 < 0
    cond_a_downtrend = (df['close'] < df['boll_mb']) & (df['boll_mb'] < df['boll_mb'].shift(1))
    cond_a = cond_a_high | cond_a_downtrend

    cond_ab = cond_a & cond_b

    # 条件 C：单日截面骤缩
    cond_c = (
        (df['volume'] < df['ma50_volume'] * vol_shrink_ratio) |
        (df['volume'] < df['volume'].shift(1) * 0.7)
    )

    # 条件 D：短期动能衰减（放宽原版连续3日严格量减）
    # 近3日累计涨跌幅 > 0（重心整体上移）且 T日量 < T-2日量 且 T日收 > T-2日收
    cum_pct_3d = df['pct_change'] + df['pct_change'].shift(1) + df['pct_change'].shift(2)
    cond_d = (
        (cum_pct_3d > 0) &
        (df['volume'] < df['volume'].shift(2)) &
        (df['close'] > df['close'].shift(2))
    )

    # 条件 E：结构背离
    # 用前19日最高收盘价（shift(1)起滚动）判断当日是否创20日新高，避免将当日自身纳入比较
    # 逐行遍历记录上一个新高日的成交量，当前新高日量若更小则触发背离
    max_prev19 = df['close'].shift(1).rolling(window=19, min_periods=1).max()
    is_new_high = df['close'] > max_prev19
    cond_e = pd.Series(False, index=df.index)
    prev_new_high_volume = pd.Series(float('nan'), index=df.index)
    prev_new_high_date = pd.Series(pd.NaT, index=df.index)
    prev_high_volume = None
    prev_high_date = None
    for idx in df.index:
        if is_new_high[idx]:
            if prev_high_volume is not None and df.loc[idx, 'volume'] < prev_high_volume:
                cond_e[idx] = True
            prev_new_high_volume[idx] = prev_high_volume if prev_high_volume is not None else float('nan')
            prev_new_high_date[idx] = prev_high_date
            prev_high_volume = df.loc[idx, 'volume']
            prev_high_date = idx
    df['max_prev19_close'] = max_prev19
    df['prev_new_high_volume'] = prev_new_high_volume.ffill()
    df['prev_new_high_date'] = prev_new_high_date.ffill()

    df['cond_ab'] = cond_ab
    df['is_new_high'] = is_new_high
    df['cond_c'] = cond_c
    df['cond_d'] = cond_d
    df['cond_e'] = cond_e
    df['signal'] = cond_ab & (cond_c | cond_d | cond_e)
    df['signal_enhanced'] = cond_a & df['cond_b_bearish_candle'] & (cond_c | cond_d | cond_e)
    df['signal_shooting_star'] = cond_a & df['cond_b_shooting_star'] & (cond_c | cond_d | cond_e)

    # 辅助验证 F：RSI超买背离
    delta = df['close'].diff()
    gain = delta.clip(lower=0).rolling(window=rsi_window, min_periods=1).mean()
    loss = (-delta.clip(upper=0)).rolling(window=rsi_window, min_periods=1).mean()
    rsi = 100 - 100 / (1 + gain / loss.replace(0, float('nan')))
    df['rsi'] = rsi
    # RSI超买 且 价格创新高但RSI未创新高（背离）
    rsi_prev_high = rsi.shift(1).rolling(window=19, min_periods=1).max()
    cond_f = (rsi > rsi_overbought) & df['is_new_high'] & (rsi < rsi_prev_high)
    df['cond_f'] = cond_f
    df['signal_confirmed'] = df['signal'] & cond_f

    # 退出机制：观察期倒计时（signal触发后 watch_days 日内标记）
    signal_arr = df['signal'].to_numpy()
    watch = [0] * len(signal_arr)
    countdown = 0
    signal_close = None
    for i, (sig, close) in enumerate(zip(signal_arr, df['close'])):
        if sig:
            countdown = watch_days
            signal_close = close
        if countdown > 0:
            stop_price = signal_close * 0.97
            watch[i] = countdown if close >= stop_price else -1  # -1表示确认下跌，应清仓
            countdown -= 1
    df['watch_period'] = watch
    return df


_CN_COLUMNS = {
    'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价',
    'volume': '成交量（万）', 'pct_change': '涨跌幅（%）',
    'boll_mb': 'BOLL中轨', 'ma50_volume': '日均量（万）',
}


def _log_result(stock_name: str, raw_df: pd.DataFrame, calc_df: pd.DataFrame, vol_shrink_ratio: float, vol_ma_window: int, atr_window: int) -> None:
    print("\n========== 无量上涨诱多/背离信号日志 V2.0 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_name}
策略：识别「无量上涨必须跑」诱多/背离形态 V2.0，满足以下任一条件即触发警示，输出最新成交日是否满足和历史满足的前三个交易日：
  前提A（高危环境）：收盘价 > min(BOLL中轨 + 2×ATR{atr_window}, BOLL上轨×0.95) 或 (收盘价 < BOLL中轨 且 BOLL中轨斜率<0)
  前提B（价格形态）：0<涨跌幅<5%；可选强化：收盘价 < 开盘价（假阴真阳）
  条件C（单日截面骤缩）：成交量 < {vol_ma_window}日均量×{vol_shrink_ratio} 或 成交量 < 昨日成交量×0.7
  条件D（动能衰减）：近3日累计涨跌幅>0 且 T日量<T-2日量 且 T日收>T-2日收
  条件E（波段顶背离）：创20日新高，但成交量 < 上一个20日新高日的成交量
  最终信号：(A & B) & (C | D | E)""")
    print("\n【原始K线数据（最近250日）】")
    display_df = raw_df.tail(250).copy()
    display_df['ma50_volume'] = calc_df['ma50_volume'].reindex(display_df.index)
    display_df['boll_mb'] = calc_df['boll_mb'].reindex(display_df.index)
    display_df['boll_ub'] = calc_df['boll_ub'].reindex(display_df.index)
    display_df['atr'] = calc_df['atr'].reindex(display_df.index)
    display_df['高位阈值（中轨+2ATR vs 上轨×0.95取小）'] = calc_df['high_threshold'].reindex(display_df.index)
    display_df['20日新高基准（昨日起前19日最高收）'] = calc_df['max_prev19_close'].reindex(display_df.index)
    display_df['上一个20日新高日成交量'] = calc_df['prev_new_high_volume'].reindex(display_df.index)
    display_df['上一个20日新高日期'] = calc_df['prev_new_high_date'].reindex(display_df.index)

    # 未触发原因
    def _no_trigger_reason(row):
        if calc_df.loc[row.name, 'signal']:
            return ''
        reasons = []
        if not calc_df.loc[row.name, 'cond_ab']:
            reasons.append('未满足高位/下跌趋势条件(A)或涨跌幅不在(0,5%)范围(B)')
        else:
            missing = [label for flag, label in [
                (calc_df.loc[row.name, 'cond_c'], 'C'),
                (calc_df.loc[row.name, 'cond_d'], 'D'),
                (calc_df.loc[row.name, 'cond_e'], 'E'),
            ] if not flag]
            if missing:
                reasons.append(f'C/D/E均不满足（{"、".join(missing)}均未触发）')
        return '；'.join(reasons)

    display_df['未触发原因'] = display_df.apply(_no_trigger_reason, axis=1)
    display_df = display_df.reset_index().rename(columns={
        'date': '日期', 'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价',
        'volume': '成交量', 'pct_change': '涨跌幅', f'ma50_volume': f'{vol_ma_window}日均量', 'boll_mb': 'BOLL中轨',
    })
    display_df['日期'] = display_df['日期'].dt.strftime('%Y-%m-%d')
    print(display_df.to_json(orient='records', force_ascii=False))
    print("==========================================\n")


async def get_unlimited_increase(stock_info: StockInfo, limit=400, vol_ma_window=50, atr_window=14, vol_shrink_ratio=0.8, rsi_window=14, rsi_overbought=70, watch_days=5) -> tuple:
    klines, vol_avg_records, boll_records = await asyncio.gather(
        get_stock_day_range_kline_by_db_cache(stock_info, limit=limit),
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
    df = identify_unlimited_increase(df, vol_ma_window, atr_window, vol_shrink_ratio, rsi_window, rsi_overbought, watch_days)
    return raw_df, df


async def get_unlimited_increase_cn(stock_info: StockInfo, limit=400, vol_ma_window=50, atr_window=14, vol_shrink_ratio=0.8, rsi_window=14, rsi_overbought=70, watch_days=5) -> dict:
    raw_df, df = await get_unlimited_increase(stock_info, limit, vol_ma_window, atr_window, vol_shrink_ratio, rsi_window, rsi_overbought, watch_days)

    def to_row(date, row):
        triggers = []
        if row.get('cond_c'):    triggers.append('无量萎缩(C)')
        if row.get('cond_d'):    triggers.append('阶梯缩量(D)')
        if row.get('cond_e'):    triggers.append('结构背离(E)')
        return {
            '日期': date.strftime('%Y-%m-%d'),
            **{_CN_COLUMNS[c]: round(row[c] / 10000, 2) if c in ('volume', 'ma50_volume') else round(row[c], 2)
               for c in ('open', 'close', 'high', 'low', 'volume', 'pct_change', 'boll_mb', 'ma50_volume')},
            'BOLL上轨': round(row['boll_ub'], 2) if pd.notna(row['boll_ub']) else None,
            'ATR': round(row['atr'], 4) if pd.notna(row['atr']) else None,
            '高位阈值（中轨+2ATR vs 上轨×0.95取小）': round(row['high_threshold'], 2) if pd.notna(row['high_threshold']) else None,
            '20日新高基准（昨日起前19日最高收）': round(row['max_prev19_close'], 2) if pd.notna(row['max_prev19_close']) else None,
            '上一个20日新高日成交量（万）': round(row['prev_new_high_volume'] / 10000, 2) if pd.notna(row['prev_new_high_volume']) else None,
            '上一个20日新高日期': row['prev_new_high_date'].strftime('%Y-%m-%d') if pd.notna(row['prev_new_high_date']) else None,
            '假阴真阳强化': bool(row.get('cond_b_bearish_candle', False)),
            '射击之星强化': bool(row.get('cond_b_shooting_star', False)),
            'RSI': round(row['rsi'], 2) if pd.notna(row['rsi']) else None,
            'RSI超买背离验证': bool(row.get('cond_f', False)),
            '观察期倒计时': int(row.get('watch_period', 0)),
            '触发条件': '、'.join(triggers),
        }

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')
    result = {
        '最新交易日': latest_date,
        f'无量上涨诱多背离（{latest_date}）': bool(latest['signal']),
        f'假阴真阳强化信号（{latest_date}）': bool(latest.get('signal_enhanced', False)),
        f'射击之星强化信号（{latest_date}）': bool(latest.get('signal_shooting_star', False)),
        f'RSI超买背离确认信号（{latest_date}）': bool(latest.get('signal_confirmed', False)),
        f'观察期倒计时（{latest_date}）': int(latest.get('watch_period', 0)),
        '历史信号列表（最近3次）': [
            to_row(date, row)
            for date, row in df[df['signal']].sort_index(ascending=False).head(10).iterrows()
        ],
    }
    _log_result(stock_info.stock_name, raw_df, df, vol_shrink_ratio, vol_ma_window, atr_window)
    return result


if __name__ == '__main__':

    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        import json
        result = await get_unlimited_increase_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False))

    asyncio.run(main())
