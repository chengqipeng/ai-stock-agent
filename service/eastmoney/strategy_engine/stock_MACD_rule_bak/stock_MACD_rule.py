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
    MACD 多空状态、交叉信号与背离信号计算 V1.0

    Rule a（多空市场界定）：
      DIF>0 且 DEA>0 → Bull_Strong（强多头）
      DIF>0 且 DEA≤0 → Bull_Weak（弱多头）
      DIF<0          → Bear（空头）

    Rule b（交叉信号）：
      Golden_Cross  ：DIF 由下穿上 DEA（金叉，看涨）
      Death_Cross   ：DIF 由上穿下 DEA（死叉，看跌）
      Zero_Above_GC ：金叉且 DIF>0 & DEA>0（零轴上金叉，抓主升段）
      Zero_Below_DC ：死叉且 DIF<0（零轴下死叉，防暴跌）

    Rule c（背离预警，状态机，无未来函数）：
      底背离（Bottom_Divergence，看涨）触发条件：
        条件A：当前空头波段最低价 < 上一空头波段最低价（股价创新低）
        条件B：当前空头波段 DIF 最低值 > 上一空头波段 DIF 最低值（DIF 未创新低）
        窗口 ：两波谷索引间距在 20~60 个交易日内
        触发 ：金叉时判定，信号标记在当前波谷所在 K 线
      顶背离（Top_Divergence，看跌）触发条件：
        条件A：当前多头波段最高价 > 上一多头波段最高价（股价创新高）
        条件B：当前多头波段 DIF 最高值 < 上一多头波段 DIF 最高值（DIF 未创新高）
        窗口 ：两波峰索引间距在 20~60 个交易日内
        触发 ：死叉时判定，信号标记在当前波峰所在 K 线
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
    # 状态定义：1=多头波段（DIF>DEA），-1=空头波段（DIF<DEA），0=初始未定
    # 每个波段内持续追踪当前波峰/波谷及对应DIF极值
    # 背离判定在金叉/死叉时触发，确保波段完整后再比较，避免未来函数
    data['Bottom_Divergence'] = False
    data['Top_Divergence']    = False
    data['Curr_Bear_Min_Price'] = np.nan
    data['Curr_Bear_Min_Date'] = None
    data['Curr_Bear_Min_DIF'] = np.nan
    data['Prev_Bear_Min_Price'] = np.nan
    data['Prev_Bear_Min_Date'] = None
    data['Prev_Bear_Min_DIF'] = np.nan
    data['Curr_Bull_Max_Price'] = np.nan
    data['Curr_Bull_Max_Date'] = None
    data['Curr_Bull_Max_DIF'] = np.nan
    data['Prev_Bull_Max_Price'] = np.nan
    data['Prev_Bull_Max_Date'] = None
    data['Prev_Bull_Max_DIF'] = np.nan
    data['Bear_Start_Date'] = None
    data['Bear_Start_Price'] = np.nan
    data['Bear_Start_DIF'] = np.nan
    data['Bull_Start_Date'] = None
    data['Bull_Start_Price'] = np.nan
    data['Bull_Start_DIF'] = np.nan

    # 上一个空头波段的价格最低点、DIF最低值及其索引（Prev_Price_Trough / Prev_DIF_Trough）
    prev_bear_min_price, prev_bear_min_dif, prev_bear_min_idx = float('inf'),  float('inf'),  -1
    # 上一个多头波段的价格最高点、DIF最高值及其索引（Prev_Price_Peak / Prev_DIF_Peak）
    prev_bull_max_price, prev_bull_max_dif, prev_bull_max_idx = float('-inf'), float('-inf'), -1
    # 当前空头波段内的价格最低点、DIF最低值及其索引（Current_Low / Current_DIF for trough）
    curr_min_price, curr_min_dif, curr_min_idx = float('inf'),  float('inf'),  -1
    # 当前多头波段内的价格最高点、DIF最高值及其索引（Current_High / Current_DIF for peak）
    curr_max_price, curr_max_dif, curr_max_idx = float('-inf'), float('-inf'), -1
    # 当前空头波段开始的死叉索引
    bear_start_idx = -1
    # 当前多头波段开始的金叉索引
    bull_start_idx = -1
    # 回溯窗口：两个相邻波谷/波峰之间的交易日数须在 20~60 日内，过近或过远均不构成有效背离
    LOOKBACK_MIN, LOOKBACK_MAX = 20, 60
    state = 0
    bot_col = data.columns.get_loc('Bottom_Divergence')
    top_col = data.columns.get_loc('Top_Divergence')

    for i in range(1, len(data)):
        price_high = data['high'].iloc[i]
        price_low  = data['low'].iloc[i]
        dif        = data['DIF'].iloc[i]

        # 多头波段：持续更新当前波峰（取最高价及对应DIF最大值）
        if state == 1:
            if price_high > curr_max_price:
                curr_max_price, curr_max_dif, curr_max_idx = price_high, dif, i
            elif price_high == curr_max_price:
                curr_max_dif = max(curr_max_dif, dif)
        # 空头波段：持续更新当前波谷（取最低价及对应DIF最小值）
        elif state == -1:
            if price_low < curr_min_price:
                curr_min_price, curr_min_dif, curr_min_idx = price_low, dif, i
            elif price_low == curr_min_price:
                curr_min_dif = min(curr_min_dif, dif)

        if data['Golden_Cross'].iloc[i]:
            # 金叉：空头波段结束，检测底背离
            # 条件A：当前波谷价格 < 上一波谷价格（股价创新低）
            # 条件B：当前波谷DIF > 上一波谷DIF（DIF未创新低，指标走强）
            # 窗口：两波谷间距须在 20~60 个交易日内
            if state == -1:
                in_window = (prev_bear_min_idx != -1 and
                             LOOKBACK_MIN <= (curr_min_idx - prev_bear_min_idx) <= LOOKBACK_MAX)
                if in_window and (curr_min_price < prev_bear_min_price) and (curr_min_dif > prev_bear_min_dif):
                    data.iloc[curr_min_idx, bot_col] = True  # 标记在波谷所在K线
                # 记录当前和上一空头波段的最低价及日期（所有金叉都记录）
                if curr_min_idx != -1:
                    data.iloc[i, data.columns.get_loc('Curr_Bear_Min_Price')] = curr_min_price
                    data.iloc[i, data.columns.get_loc('Curr_Bear_Min_Date')] = data.index[curr_min_idx]
                    data.iloc[i, data.columns.get_loc('Curr_Bear_Min_DIF')] = curr_min_dif
                if prev_bear_min_idx != -1:
                    data.iloc[i, data.columns.get_loc('Prev_Bear_Min_Price')] = prev_bear_min_price
                    data.iloc[i, data.columns.get_loc('Prev_Bear_Min_Date')] = data.index[prev_bear_min_idx]
                    data.iloc[i, data.columns.get_loc('Prev_Bear_Min_DIF')] = prev_bear_min_dif
                # 记录空头波段开始的死叉信息
                if bear_start_idx != -1:
                    data.iloc[i, data.columns.get_loc('Bear_Start_Date')] = data.index[bear_start_idx]
                    data.iloc[i, data.columns.get_loc('Bear_Start_Price')] = data['close'].iloc[bear_start_idx]
                    data.iloc[i, data.columns.get_loc('Bear_Start_DIF')] = data['DIF'].iloc[bear_start_idx]
                # 当前波谷成为下一次比较的「上一波谷」
                prev_bear_min_price, prev_bear_min_dif, prev_bear_min_idx = curr_min_price, curr_min_dif, curr_min_idx
            # 切换为多头波段，重置当前波峰追踪
            state = 1
            bull_start_idx = i  # 记录多头波段开始的金叉索引
            curr_max_price, curr_max_dif, curr_max_idx = price_high, dif, i

        elif data['Death_Cross'].iloc[i]:
            # 死叉：多头波段结束，检测顶背离
            # 条件A：当前波峰价格 > 上一波峰价格（股价创新高）
            # 条件B：当前波峰DIF < 上一波峰DIF（DIF未创新高，指标走弱）
            # 窗口：两波峰间距须在 20~60 个交易日内
            if state == 1:
                in_window = (prev_bull_max_idx != -1 and
                             LOOKBACK_MIN <= (curr_max_idx - prev_bull_max_idx) <= LOOKBACK_MAX)
                if in_window and (curr_max_price > prev_bull_max_price) and (curr_max_dif < prev_bull_max_dif):
                    data.iloc[curr_max_idx, top_col] = True  # 标记在波峰所在K线
                # 记录当前和上一多头波段的最高价及日期（所有死叉都记录）
                if curr_max_idx != -1:
                    data.iloc[i, data.columns.get_loc('Curr_Bull_Max_Price')] = curr_max_price
                    data.iloc[i, data.columns.get_loc('Curr_Bull_Max_Date')] = data.index[curr_max_idx]
                    data.iloc[i, data.columns.get_loc('Curr_Bull_Max_DIF')] = curr_max_dif
                if prev_bull_max_idx != -1:
                    data.iloc[i, data.columns.get_loc('Prev_Bull_Max_Price')] = prev_bull_max_price
                    data.iloc[i, data.columns.get_loc('Prev_Bull_Max_Date')] = data.index[prev_bull_max_idx]
                    data.iloc[i, data.columns.get_loc('Prev_Bull_Max_DIF')] = prev_bull_max_dif
                # 记录多头波段开始的金叉信息
                if bull_start_idx != -1:
                    data.iloc[i, data.columns.get_loc('Bull_Start_Date')] = data.index[bull_start_idx]
                    data.iloc[i, data.columns.get_loc('Bull_Start_Price')] = data['close'].iloc[bull_start_idx]
                    data.iloc[i, data.columns.get_loc('Bull_Start_DIF')] = data['DIF'].iloc[bull_start_idx]
                # 当前波峰成为下一次比较的「上一波峰」
                prev_bull_max_price, prev_bull_max_dif, prev_bull_max_idx = curr_max_price, curr_max_dif, curr_max_idx
            # 切换为空头波段，重置当前波谷追踪
            state = -1
            bear_start_idx = i  # 记录空头波段开始的死叉索引
            curr_min_price, curr_min_dif, curr_min_idx = price_low, dif, i

        elif state == 0:
            # 初始状态：根据DIF与DEA的相对位置确定初始波段方向
            if data['DIF'].iloc[i] > data['DEA'].iloc[i]:
                state = 1
                curr_max_price, curr_max_dif, curr_max_idx = price_high, dif, i
            elif data['DIF'].iloc[i] < data['DEA'].iloc[i]:
                state = -1
                curr_min_price, curr_min_dif, curr_min_idx = price_low, dif, i

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

    # 最新状态
    latest = df.iloc[-1]
    print(f"\n【最新交易日】{df.index[-1].strftime('%Y-%m-%d')}")
    print(f"  DIF={latest['DIF']:.4f}  DEA={latest['DEA']:.4f}  MACD_Hist={latest['MACD_Hist']:.4f}")
    print(f"  市场状态={latest['Market_State']}")
    print(f"  金叉={latest['Golden_Cross']}  死叉={latest['Death_Cross']}")
    print(f"  零轴上金叉={latest['Zero_Above_GC']}  零轴下死叉={latest['Zero_Below_DC']}")

    # 打印最近20天明细
    print(f"\n【最近20天明细数据】")
    print(f"{'日期':<12} {'收盘':<8} {'DIF':<10} {'DEA':<10} {'MACD柱':<10} {'状态':<12} {'信号'}")
    print("-" * 90)
    for date, row in df.tail(20).iterrows():
        signals = []
        if row['Golden_Cross']: signals.append('金叉')
        if row['Death_Cross']: signals.append('死叉')
        if row['Zero_Above_GC']: signals.append('零轴上金叉')
        if row['Zero_Below_DC']: signals.append('零轴下死叉')
        if row['Bottom_Divergence']: signals.append('底背离')
        if row['Top_Divergence']: signals.append('顶背离')
        signal_str = ','.join(signals) if signals else '-'
        print(f"{date.strftime('%Y-%m-%d'):<12} {row['close']:<8.2f} {row['DIF']:<10.4f} {row['DEA']:<10.4f} {row['MACD_Hist']:<10.4f} {row['Market_State']:<12} {signal_str}")

    # 空头波段明细
    golden_crosses = df[df['Golden_Cross']]
    bear_segments = golden_crosses[pd.notna(golden_crosses['Curr_Bear_Min_Price'])]
    print(f"\n【空头波段明细（共{len(bear_segments)}个）】")
    if len(bear_segments) > 0:
        print(f"{'波段序号':<8} {'死叉日期':<12} {'死叉收盘价':<12} {'死叉DIF':<10} {'最低价':<10} {'最低价日期':<12} {'金叉日期':<12} {'金叉收盘价':<12} {'跌幅%':<10}")
        print("-" * 130)
        for idx, (date, row) in enumerate(bear_segments.iterrows(), 1):
            min_price = row['Curr_Bear_Min_Price']
            min_date = row['Curr_Bear_Min_Date'].strftime('%Y-%m-%d') if pd.notna(row['Curr_Bear_Min_Date']) else '-'
            bear_start_date = row['Bear_Start_Date'].strftime('%Y-%m-%d') if pd.notna(row['Bear_Start_Date']) else '-'
            bear_start_price = f"{row['Bear_Start_Price']:.2f}" if pd.notna(row['Bear_Start_Price']) else '-'
            bear_start_dif = f"{row['Bear_Start_DIF']:.4f}" if pd.notna(row['Bear_Start_DIF']) else '-'
            # 计算从死叉到最低价的跌幅
            pct_change = ((min_price - row['Bear_Start_Price']) / row['Bear_Start_Price'] * 100) if pd.notna(row['Bear_Start_Price']) and pd.notna(min_price) and row['Bear_Start_Price'] > 0 else 0
            print(f"{idx:<8} {bear_start_date:<12} {bear_start_price:<12} {bear_start_dif:<10} {min_price:<10.2f} {min_date:<12} {date.strftime('%Y-%m-%d'):<12} {row['close']:<12.2f} {pct_change:>9.2f}")
    else:
        print("  无空头波段数据")

    # 多头波段明细
    death_crosses = df[df['Death_Cross']]
    bull_segments = death_crosses[pd.notna(death_crosses['Curr_Bull_Max_Price'])]
    print(f"\n【多头波段明细（共{len(bull_segments)}个）】")
    if len(bull_segments) > 0:
        print(f"{'波段序号':<8} {'金叉日期':<12} {'金叉收盘价':<12} {'金叉DIF':<10} {'最高价':<10} {'最高价日期':<12} {'死叉日期':<12} {'死叉收盘价':<12} {'涨幅%':<10}")
        print("-" * 130)
        for idx, (date, row) in enumerate(bull_segments.iterrows(), 1):
            max_price = row['Curr_Bull_Max_Price']
            max_date = row['Curr_Bull_Max_Date'].strftime('%Y-%m-%d') if pd.notna(row['Curr_Bull_Max_Date']) else '-'
            bull_start_date = row['Bull_Start_Date'].strftime('%Y-%m-%d') if pd.notna(row['Bull_Start_Date']) else '-'
            bull_start_price = f"{row['Bull_Start_Price']:.2f}" if pd.notna(row['Bull_Start_Price']) else '-'
            bull_start_dif = f"{row['Bull_Start_DIF']:.4f}" if pd.notna(row['Bull_Start_DIF']) else '-'
            # 计算从金叉到最高价的涨幅
            pct_change = ((max_price - row['Bull_Start_Price']) / row['Bull_Start_Price'] * 100) if pd.notna(row['Bull_Start_Price']) and pd.notna(max_price) and row['Bull_Start_Price'] > 0 else 0
            print(f"{idx:<8} {bull_start_date:<12} {bull_start_price:<12} {bull_start_dif:<10} {max_price:<10.2f} {max_date:<12} {date.strftime('%Y-%m-%d'):<12} {row['close']:<12.2f} {pct_change:>9.2f}")
    else:
        print("  无多头波段数据")

    # 金叉明细
    print(f"\n【金叉明细（共{len(golden_crosses)}次）】")
    if len(golden_crosses) > 0:
        print(f"{'日期':<12} {'收盘':<8} {'DIF':<10} {'DEA':<10} {'类型':<12} {'当前空头波段最低价':<18} {'最低价DIF':<12} {'上一空头波段最低价':<18} {'最低价DIF':<12}")
        print("-" * 150)
        for date, row in golden_crosses.tail(10).iterrows():
            cross_type = '零轴上金叉' if row['Zero_Above_GC'] else '普通金叉'
            curr_price = f"{row['Curr_Bear_Min_Price']:.2f}" if pd.notna(row['Curr_Bear_Min_Price']) else '-'
            curr_dif = f"{row['Curr_Bear_Min_DIF']:.4f}" if pd.notna(row['Curr_Bear_Min_DIF']) else '-'
            prev_price = f"{row['Prev_Bear_Min_Price']:.2f}" if pd.notna(row['Prev_Bear_Min_Price']) else '-'
            prev_dif = f"{row['Prev_Bear_Min_DIF']:.4f}" if pd.notna(row['Prev_Bear_Min_DIF']) else '-'
            print(f"{date.strftime('%Y-%m-%d'):<12} {row['close']:<8.2f} {row['DIF']:<10.4f} {row['DEA']:<10.4f} {cross_type:<12} {curr_price:<18} {curr_dif:<12} {prev_price:<18} {prev_dif:<12}")

    # 死叉明细
    death_crosses = df[df['Death_Cross']]
    print(f"\n【死叉明细（共{len(death_crosses)}次）】")
    if len(death_crosses) > 0:
        print(f"{'日期':<12} {'收盘':<8} {'DIF':<10} {'DEA':<10} {'类型':<12} {'当前多头波段最高价':<18} {'最高价DIF':<12} {'上一多头波段最高价':<18} {'最高价DIF':<12}")
        print("-" * 150)
        for date, row in death_crosses.tail(10).iterrows():
            cross_type = '零轴下死叉' if row['Zero_Below_DC'] else '普通死叉'
            curr_price = f"{row['Curr_Bull_Max_Price']:.2f}" if pd.notna(row['Curr_Bull_Max_Price']) else '-'
            curr_dif = f"{row['Curr_Bull_Max_DIF']:.4f}" if pd.notna(row['Curr_Bull_Max_DIF']) else '-'
            prev_price = f"{row['Prev_Bull_Max_Price']:.2f}" if pd.notna(row['Prev_Bull_Max_Price']) else '-'
            prev_dif = f"{row['Prev_Bull_Max_DIF']:.4f}" if pd.notna(row['Prev_Bull_Max_DIF']) else '-'
            print(f"{date.strftime('%Y-%m-%d'):<12} {row['close']:<8.2f} {row['DIF']:<10.4f} {row['DEA']:<10.4f} {cross_type:<12} {curr_price:<18} {curr_dif:<12} {prev_price:<18} {prev_dif:<12}")

    # 底背离明细
    bottom_div = df[df['Bottom_Divergence']]
    print(f"\n【底背离明细（共{len(bottom_div)}次）】")
    if len(bottom_div) > 0:
        print(f"{'日期':<12} {'最低价':<8} {'收盘价':<8} {'DIF':<10} {'DEA':<10} {'当前空头波段最低价':<18} {'发生日期':<12} {'上一空头波段最低价':<18} {'发生日期':<12}")
        print("-" * 140)
        for date, row in bottom_div.iterrows():
            curr_date = row['Curr_Bear_Min_Date'].strftime('%Y-%m-%d') if pd.notna(row['Curr_Bear_Min_Date']) else '-'
            prev_date = row['Prev_Bear_Min_Date'].strftime('%Y-%m-%d') if pd.notna(row['Prev_Bear_Min_Date']) else '-'
            print(f"{date.strftime('%Y-%m-%d'):<12} {row['low']:<8.2f} {row['close']:<8.2f} {row['DIF']:<10.4f} {row['DEA']:<10.4f} {row['Curr_Bear_Min_Price']:<18.2f} {curr_date:<12} {row['Prev_Bear_Min_Price']:<18.2f} {prev_date:<12}")
    else:
        print("  无底背离信号")

    # 顶背离明细
    top_div = df[df['Top_Divergence']]
    print(f"\n【顶背离明细（共{len(top_div)}次）】")
    if len(top_div) > 0:
        print(f"{'日期':<12} {'最高价':<8} {'收盘价':<8} {'DIF':<10} {'DEA':<10}")
        print("-" * 60)
        for date, row in top_div.iterrows():
            print(f"{date.strftime('%Y-%m-%d'):<12} {row['high']:<8.2f} {row['close']:<8.2f} {row['DIF']:<10.4f} {row['DEA']:<10.4f}")
    else:
        print("  无顶背离信号")

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
        stock_info: StockInfo = get_stock_info_by_name('中国卫通')
        result = await get_macd_signals_cn(stock_info)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    asyncio.run(main())
