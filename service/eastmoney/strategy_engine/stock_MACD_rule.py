import asyncio
import numpy as np
import pandas as pd
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline
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


def calculate_macd_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    基于 MACD 法则计算多空状态、交叉信号以及背离信号。
    输入 DataFrame 必须包含 'close', 'high', 'low' 列，且按时间升序排列。
    """
    data = df.copy()

    # 1. 计算 MACD 基础指标
    data['EMA12'] = data['close'].ewm(span=12, adjust=False).mean()
    data['EMA26'] = data['close'].ewm(span=26, adjust=False).mean()
    data['DIF'] = data['EMA12'] - data['EMA26']
    data['DEA'] = data['DIF'].ewm(span=9, adjust=False).mean()
    data['MACD_Hist'] = (data['DIF'] - data['DEA']) * 2

    # Rule a: 多空市场界定
    data['Market_State'] = np.where(
        (data['DIF'] > 0) & (data['DEA'] > 0), 'Bull_Strong',
        np.where(data['DIF'] > 0, 'Bull_Weak',
        np.where(data['DIF'] < 0, 'Bear', 'Neutral'))
    )

    prev_dif = data['DIF'].shift(1)
    prev_dea = data['DEA'].shift(1)

    # Rule b: 交叉信号
    data['Golden_Cross']   = (prev_dif <= prev_dea) & (data['DIF'] > data['DEA'])
    data['Death_Cross']    = (prev_dif >= prev_dea) & (data['DIF'] < data['DEA'])
    data['Zero_Above_GC']  = data['Golden_Cross'] & (data['DIF'] > 0) & (data['DEA'] > 0)
    data['Zero_Below_DC']  = data['Death_Cross']  & (data['DIF'] < 0)

    # Rule c: 背离预警（状态机，无未来函数）
    data['Bottom_Divergence'] = False
    data['Top_Divergence']    = False

    prev_bear_min_price, prev_bear_min_dif, prev_bear_min_macd = float('inf'),  float('inf'),  float('inf')
    prev_bull_max_price, prev_bull_max_dif, prev_bull_max_macd = float('-inf'), float('-inf'), float('-inf')
    curr_min_price, curr_min_dif, curr_min_macd, curr_min_idx = float('inf'),  float('inf'),  float('inf'),  -1
    curr_max_price, curr_max_dif, curr_max_macd, curr_max_idx = float('-inf'), float('-inf'), float('-inf'), -1
    state = 0
    bot_col = data.columns.get_loc('Bottom_Divergence')
    top_col = data.columns.get_loc('Top_Divergence')

    for i in range(1, len(data)):
        price_high = data['high'].iloc[i]
        price_low  = data['low'].iloc[i]
        dif        = data['DIF'].iloc[i]
        macd_hist  = data['MACD_Hist'].iloc[i]

        if state == 1:
            if price_high > curr_max_price:
                curr_max_price, curr_max_dif, curr_max_macd, curr_max_idx = price_high, dif, macd_hist, i
            elif price_high == curr_max_price:
                curr_max_dif = max(curr_max_dif, dif)
        elif state == -1:
            if price_low < curr_min_price:
                curr_min_price, curr_min_dif, curr_min_macd, curr_min_idx = price_low, dif, macd_hist, i
            elif price_low == curr_min_price:
                curr_min_dif = min(curr_min_dif, dif)

        if data['Golden_Cross'].iloc[i]:
            if state == -1:
                dif_no_new_low    = curr_min_dif > prev_bear_min_dif
                green_bar_shorter = abs(curr_min_macd) < abs(prev_bear_min_macd)
                if (curr_min_price < prev_bear_min_price) and (dif_no_new_low or green_bar_shorter):
                    if prev_bear_min_price != float('inf') and curr_min_idx != -1:
                        data.iloc[curr_min_idx, bot_col] = True
                prev_bear_min_price, prev_bear_min_dif, prev_bear_min_macd = curr_min_price, curr_min_dif, curr_min_macd
            state = 1
            curr_max_price, curr_max_dif, curr_max_macd, curr_max_idx = price_high, dif, macd_hist, i

        elif data['Death_Cross'].iloc[i]:
            if state == 1:
                dif_no_new_high = curr_max_dif < prev_bull_max_dif
                red_bar_shorter = curr_max_macd < prev_bull_max_macd
                if (curr_max_price > prev_bull_max_price) and (dif_no_new_high or red_bar_shorter):
                    if prev_bull_max_price != float('-inf') and curr_max_idx != -1:
                        data.iloc[curr_max_idx, top_col] = True
                prev_bull_max_price, prev_bull_max_dif, prev_bull_max_macd = curr_max_price, curr_max_dif, curr_max_macd
            state = -1
            curr_min_price, curr_min_dif, curr_min_macd, curr_min_idx = price_low, dif, macd_hist, i

        elif state == 0:
            if data['DIF'].iloc[i] > data['DEA'].iloc[i]:
                state = 1
                curr_max_price, curr_max_dif, curr_max_macd, curr_max_idx = price_high, dif, macd_hist, i
            elif data['DIF'].iloc[i] < data['DEA'].iloc[i]:
                state = -1
                curr_min_price, curr_min_dif, curr_min_macd, curr_min_idx = price_low, dif, macd_hist, i

    return data


async def get_macd_signals(stock_info: StockInfo, limit: int = 400) -> pd.DataFrame:
    """获取股票日K线并返回含 MACD 信号列的 DataFrame"""
    klines = await get_stock_day_range_kline(stock_info, limit=limit)
    df = _build_dataframe(klines)
    return calculate_macd_signals(df)


def _log_result(stock_name: str, df: pd.DataFrame) -> None:
    print("\n========== MACD 信号日志 ==========")
    print(f"股票：{stock_name}")
    print(f"""【策略逻辑说明】
  Rule a（多空界定）：DIF>0 且 DEA>0 → 强多头；DIF>0 → 弱多头；DIF<0 → 空头
  Rule b（交叉信号）：金叉/死叉；零轴上金叉（抓主升）；零轴下死叉（防暴跌）
  Rule c（背离预警）：底背离（股价新低但DIF未新低）；顶背离（股价新高但DIF未新高）""")

    latest = df.iloc[-1]
    print(f"\n【最新交易日】{df.index[-1].strftime('%Y-%m-%d')}")
    print(f"  DIF={latest['DIF']:.4f}  DEA={latest['DEA']:.4f}  MACD_Hist={latest['MACD_Hist']:.4f}")
    print(f"  市场状态={latest['Market_State']}")
    print(f"  金叉={latest['Golden_Cross']}  死叉={latest['Death_Cross']}")
    print(f"  零轴上金叉={latest['Zero_Above_GC']}  零轴下死叉={latest['Zero_Below_DC']}")

    bottom_div = df[df['Bottom_Divergence']].tail(3)
    top_div    = df[df['Top_Divergence']].tail(3)
    print(f"\n【近期底背离（最近3次）】")
    for date, row in bottom_div.iterrows():
        print(f"  {date.strftime('%Y-%m-%d')}  DIF={row['DIF']:.4f}  close={row['close']:.2f}")
    print(f"\n【近期顶背离（最近3次）】")
    for date, row in top_div.iterrows():
        print(f"  {date.strftime('%Y-%m-%d')}  DIF={row['DIF']:.4f}  close={row['close']:.2f}")
    print("====================================\n")


async def get_macd_signals_cn(stock_info: StockInfo, limit: int = 400) -> dict:
    """获取 MACD 信号，返回中文 key 的 JSON 结构"""
    klines = await get_stock_day_range_kline(stock_info, limit=limit)
    df = _build_dataframe(klines)
    df = calculate_macd_signals(df)

    def to_row(date, row):
        return {
            '日期': date.strftime('%Y-%m-%d'),
            '收盘价': round(row['close'], 2),
            'DIF': round(row['DIF'], 4),
            'DEA': round(row['DEA'], 4),
            'MACD柱': round(row['MACD_Hist'], 4),
        }

    latest      = df.iloc[-1]
    latest_date = df.index[-1].strftime('%Y-%m-%d')
    result = {
        '最新交易日': latest_date,
        '市场状态': latest['Market_State'],
        f'金叉（{latest_date}）':      bool(latest['Golden_Cross']),
        f'死叉（{latest_date}）':      bool(latest['Death_Cross']),
        f'零轴上金叉（{latest_date}）': bool(latest['Zero_Above_GC']),
        f'零轴下死叉（{latest_date}）': bool(latest['Zero_Below_DC']),
        '底背离历史（最近3次）': [to_row(d, r) for d, r in df[df['Bottom_Divergence']].sort_index(ascending=False).head(3).iterrows()],
        '顶背离历史（最近3次）': [to_row(d, r) for d, r in df[df['Top_Divergence']].sort_index(ascending=False).head(3).iterrows()],
    }
    _log_result(stock_info.stock_name, df)
    return result


if __name__ == '__main__':
    from common.utils.stock_info_utils import get_stock_info_by_name
    import json

    async def main():
        stock_info: StockInfo = get_stock_info_by_name('北方华创')
        result = await get_macd_signals_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
