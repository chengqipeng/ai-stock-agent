import asyncio
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline_by_db_cache
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
        })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date')


def identify_kdj_signals(df: pd.DataFrame, n=9, s1=3, s2=3, blunt_n=3, defense_line=None) -> pd.DataFrame:
    """
    KDJ 法则（微观动能）
    买入：过去5天内曾进入超卖区（K<20, D<20, J<0），且发生金叉（昨日K<=昨日D，今日K>今日D），且今日J>昨日J → Buy
    卖出（钝化）：K>80 连续 blunt_n 天（is_high_blunted），收盘跌破 MA5/MA20 或 defense_line → Sell (Blunted Exit)；防守线未破则持股死捂，死叉信号被屏蔽
    卖出（普通）：非钝化状态下，过去5天内曾进入超买区（K>80, D>80, J>100），且发生死叉（昨日K>=昨日D，今日K<今日D）→ Sell (Standard)
    """
    low_n  = df['low'].rolling(n).min()
    high_n = df['high'].rolling(n).max()
    rsv = (df['close'] - low_n) / (high_n - low_n).replace(0, 1) * 100
    df['K'] = rsv.ewm(alpha=1/s1, adjust=False).mean()
    df['D'] = df['K'].ewm(alpha=1/s2, adjust=False).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']

    df['MA5']  = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()

    df['is_oversold']  = (df['K'] < 20) & (df['D'] < 20) & (df['J'] < 0)
    df['is_overbought'] = (df['K'] > 80) & (df['D'] > 80) & (df['J'] > 100)
    df['is_high_blunted'] = (df['K'] > 80).rolling(window=blunt_n).sum() == blunt_n
    # 过去5天内曾进入超卖/超买区，宽松捕捉金叉/死叉前的极端背景
    df['recently_oversold']   = df['is_oversold'].rolling(window=5).max().astype(bool)
    df['recently_overbought'] = df['is_overbought'].rolling(window=5).max().astype(bool)

    df['signal'] = 'Hold'
    for i in range(1, len(df)):
        cond_gold_cross = (df.iloc[i-1]['K'] <= df.iloc[i-1]['D']) and (df.iloc[i]['K'] > df.iloc[i]['D'])
        cond_dead_cross = (df.iloc[i-1]['K'] >= df.iloc[i-1]['D']) and (df.iloc[i]['K'] < df.iloc[i]['D'])
        cond_j_up = df.iloc[i]['J'] > df.iloc[i-1]['J']
        in_oversold_zone = df.iloc[i]['recently_oversold']
        in_overbought_zone = df.iloc[i]['recently_overbought']

        if in_oversold_zone and cond_gold_cross and cond_j_up:
            df.iloc[i, df.columns.get_loc('signal')] = 'Buy'
        elif df.iloc[i]['is_high_blunted']:
            close = df.iloc[i]['close']
            ma_broken = close < df.iloc[i]['MA5'] or close < df.iloc[i]['MA20']
            defense_broken = (defense_line is not None) and (close < defense_line)
            if ma_broken or defense_broken:
                df.iloc[i, df.columns.get_loc('signal')] = 'Sell (Blunted Exit)'
        elif cond_dead_cross and in_overbought_zone and not df.iloc[i]['is_high_blunted']:
            df.iloc[i, df.columns.get_loc('signal')] = 'Sell (Standard)'

    return df


async def get_kdj_rule(stock_info: StockInfo, limit=800, n=9, s1=3, s2=3, blunt_n=3, defense_line=None) -> pd.DataFrame:
    """获取股票日K线并返回含 KDJ 信号列的 DataFrame"""
    klines = await get_stock_day_range_kline_by_db_cache(stock_info, limit=limit)
    df = _build_dataframe(klines)
    return identify_kdj_signals(df, n, s1, s2, blunt_n, defense_line)


async def get_kdj_rule_cn(stock_info: StockInfo, limit=800, n=9, s1=3, s2=3, blunt_n=3, defense_line=None) -> dict:
    """获取 KDJ 信号，返回中文 key 的 JSON 结构"""
    klines = await get_stock_day_range_kline_by_db_cache(stock_info, limit=limit)
    df = _build_dataframe(klines)
    df = identify_kdj_signals(df, n, s1, s2, blunt_n, defense_line)

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')

    def to_row(date, row):
        return {
            '日期': date.strftime('%Y-%m-%d'),
            'K': round(row['K'], 2),
            'D': round(row['D'], 2),
            'J': round(row['J'], 2),
            '收盘价': round(row['close'], 2),
            'MA5': round(row['MA5'], 2) if pd.notna(row['MA5']) else None,
            '信号': row['signal'],
        }

    buy_signals  = df[df['signal'] == 'Buy'].sort_index(ascending=False).head(5)
    sell_signals = df[df['signal'].str.startswith('Sell')].sort_index(ascending=False).head(5)

    result = {
        '最新交易日': latest_date,
        f'最新信号（{latest_date}）': latest['signal'],
        'K': round(latest['K'], 2),
        'D': round(latest['D'], 2),
        'J': round(latest['J'], 2),
        '历史买入信号（最近5次）': [to_row(date, row) for date, row in buy_signals.iterrows()],
        '历史卖出信号（最近5次）': [to_row(date, row) for date, row in sell_signals.iterrows()],
    }

    _log_result(stock_info.stock_name, df, result, n, s1, s2, blunt_n)
    return result


def _log_result(stock_name: str, df: pd.DataFrame, result: dict, n: int, s1: int, s2: int, blunt_n: int) -> None:
    print("\n========== KDJ 信号日志 ==========")
    print(f"""【策略逻辑说明】
股票：{stock_name}
策略：KDJ 法则（微观动能），参数 KDJ({n},{s1},{s2})，钝化判定连续天数={blunt_n}
  买入：超卖区（K<20, D<20, J<0）出现金叉且 J 勾头向上
  卖出（钝化）：K>80 连续{blunt_n}天，收盘跌破 MA5/MA20 或防守线 → Sell (Blunted Exit)（死叉失效）
  卖出（普通）：超买区（K>80, D>80, J>100）死叉 → Sell (Standard)""")
    print("\n【原始K线数据（最近250条）】")
    cn_rename = {'open': '开盘价', 'close': '收盘价', 'high': '最高价', 'low': '最低价', 'volume': '成交量'}
    display_df = df[['open', 'close', 'high', 'low', 'volume', 'K', 'D', 'J', 'signal']].tail(250).copy()
    display_df = display_df.reset_index().rename(columns={**cn_rename, 'date': '日期', 'signal': '信号'})
    display_df['日期'] = display_df['日期'].dt.strftime('%Y-%m-%d')
    print(display_df.to_json(orient='records', force_ascii=False, indent=2))
    print("==========================================\n")


async def get_kdj_rule_kdj_only(stock_info: StockInfo, n=9, s1=3, s2=3, blunt_n=3, defense_line=None) -> dict:
    """仅使用 KDJ 数据分析信号，返回中文 key 的字典"""
    from service.eastmoney.technical.stock_day_kdj import calculate_kdj

    records = await calculate_kdj(stock_info, n=n, m1=s1, m2=s2)

    df = pd.DataFrame(records)
    df.index = pd.to_datetime([r['date'] for r in records])
    df = df.rename(columns={'close_price': 'close', 'k': 'K', 'd': 'D', 'j': 'J'})
    df = df.sort_index()

    df['is_oversold']   = (df['K'] < 20) & (df['D'] < 20) & (df['J'] < 0)
    df['is_overbought'] = (df['K'] > 80) & (df['D'] > 80) & (df['J'] > 100)
    df['is_high_blunted'] = (df['K'] > 80).rolling(window=blunt_n).sum() == blunt_n
    df['recently_oversold']   = df['is_oversold'].rolling(window=5).max().astype(bool)
    df['recently_overbought'] = df['is_overbought'].rolling(window=5).max().astype(bool)

    df['signal'] = 'Hold'
    for i in range(1, len(df)):
        cond_gold = (df.iloc[i-1]['K'] <= df.iloc[i-1]['D']) and (df.iloc[i]['K'] > df.iloc[i]['D'])
        cond_dead = (df.iloc[i-1]['K'] >= df.iloc[i-1]['D']) and (df.iloc[i]['K'] < df.iloc[i]['D'])
        cond_j_up = df.iloc[i]['J'] > df.iloc[i-1]['J']
        if df.iloc[i]['recently_oversold'] and cond_gold and cond_j_up:
            df.iloc[i, df.columns.get_loc('signal')] = 'Buy'
        elif df.iloc[i]['is_high_blunted']:
            if defense_line is not None and df.iloc[i]['close'] < defense_line:
                df.iloc[i, df.columns.get_loc('signal')] = 'Sell (Blunted Exit)'
        elif cond_dead and df.iloc[i]['recently_overbought'] and not df.iloc[i]['is_high_blunted']:
            df.iloc[i, df.columns.get_loc('signal')] = 'Sell (Standard)'

    latest = df.sort_index(ascending=False).iloc[0]
    latest_date = latest.name.strftime('%Y-%m-%d')

    def to_row(date, row):
        return {
            '日期': date.strftime('%Y-%m-%d'),
            'K': round(float(row['K']), 2),
            'D': round(float(row['D']), 2),
            'J': round(float(row['J']), 2),
            '收盘价': round(float(row['close']), 2),
            '信号': row['signal'],
        }

    buy_rows  = df[df['signal'] == 'Buy'].sort_index(ascending=False).head(5)
    sell_rows = df[df['signal'].str.startswith('Sell')].sort_index(ascending=False).head(5)

    return {
        '最新交易日': latest_date,
        f'最新信号（{latest_date}）': latest['signal'],
        'K': round(float(latest['K']), 2),
        'D': round(float(latest['D']), 2),
        'J': round(float(latest['J']), 2),
        f'超卖区（{latest_date}）':  bool(latest['is_oversold']),
        f'超买区（{latest_date}）':  bool(latest['is_overbought']),
        f'高位钝化（{latest_date}）': bool(latest['is_high_blunted']),
        '历史买入信号（最近5次）': [to_row(d, r) for d, r in buy_rows.iterrows()],
        '历史卖出信号（最近5次）': [to_row(d, r) for d, r in sell_rows.iterrows()],
        '最新数据': to_row(latest.name, latest),
        '明细数据': [to_row(d, r) for d, r in df.sort_index(ascending=False).iterrows()],
    }


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('中国卫通')
        import json
        result = await get_kdj_rule_kdj_only(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
