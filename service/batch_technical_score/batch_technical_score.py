#!/usr/bin/env python3
"""
批量技术面打分：遍历 stock_score_list.md 中的股票，
使用 get_stock_day_range_kline_by_db_cache 获取日线数据，
通过 MACD、KDJ、交易量等维度综合打分，输出 ≥50 分的股票清单。
"""
import asyncio
import re
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_range_kline_by_db_cache
from dao.stock_technical_score_dao import save_score_results

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

SCORE_LIST_PATH = Path(__file__).parent.parent.parent / "data_results/stock_to_score_list/stock_score_list.md"
OUTPUT_PATH = Path(__file__).parent.parent.parent / "data_results/stock_all_scan_score_result/batch_technical_score_result.md"


# ─── 解析股票列表 ───
def parse_stock_list(path: Path) -> list[dict]:
    """解析 stock_score_list.md，返回 [{name, code}, ...]"""
    pattern = re.compile(r'^(.+?)\s*\((\d{6}\.\w{2})\)')
    stocks = []
    for line in path.read_text(encoding='utf-8').splitlines():
        m = pattern.match(line.strip())
        if m:
            stocks.append({'name': m.group(1).strip(), 'code': m.group(2)})
    return stocks


def _make_stock_info(name: str, code: str) -> StockInfo:
    stock_code, suffix = code.split('.')
    prefix = "0" if suffix == "SZ" else "1"
    return StockInfo(secid=f"{prefix}.{stock_code}", stock_code=stock_code,
                     stock_code_normalize=code, stock_name=name)


# ─── 构建 DataFrame ───
def build_df(klines: list[str]) -> pd.DataFrame:
    rows = []
    for k in klines:
        f = k.split(',')
        rows.append({
            'date': f[0], 'open': float(f[1]), 'close': float(f[2]),
            'high': float(f[3]), 'low': float(f[4]), 'volume': float(f[5]),
            'pct_change': float(f[8]),
        })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date').sort_index()


# ─── MACD 打分 (满分 35) ───
def score_macd(df: pd.DataFrame) -> tuple[int, str]:
    """
    MACD维度打分:
    - 市场状态: Bull_Strong +15, Bull_Weak +8, Bear +0
    - 金叉信号: 零轴上金叉 +10, 普通金叉 +5, 死叉 -5
    - 底背离 +10, 顶背离 -5
    """
    data = df.copy()
    data['EMA12'] = data['close'].ewm(span=12, adjust=False).mean()
    data['EMA26'] = data['close'].ewm(span=26, adjust=False).mean()
    data['DIF'] = data['EMA12'] - data['EMA26']
    data['DEA'] = data['DIF'].ewm(span=9, adjust=False).mean()
    data['MACD_Hist'] = (data['DIF'] - data['DEA']) * 2

    prev_dif = data['DIF'].shift(1)
    prev_dea = data['DEA'].shift(1)
    data['Golden_Cross'] = (prev_dif <= prev_dea) & (data['DIF'] > data['DEA'])
    data['Death_Cross'] = (prev_dif >= prev_dea) & (data['DIF'] < data['DEA'])

    latest = data.iloc[-1]
    score = 0
    details = []

    # 市场状态
    if latest['DIF'] > 0 and latest['DEA'] > 0:
        score += 15
        details.append('强多头+15')
    elif latest['DIF'] > 0:
        score += 8
        details.append('弱多头+8')
    else:
        details.append('空头+0')

    # 最近5天交叉信号
    recent = data.tail(5)
    if recent['Golden_Cross'].any():
        gc_row = recent[recent['Golden_Cross']].iloc[-1]
        if gc_row['DIF'] > 0 and gc_row['DEA'] > 0:
            score += 10
            details.append('零轴上金叉+10')
        else:
            score += 5
            details.append('金叉+5')
    elif recent['Death_Cross'].any():
        score -= 5
        details.append('死叉-5')

    # MACD柱状图趋势（最近3天连续放大）
    # 增加最小变化阈值，避免零轴附近微小浮动误判
    hist_tail = data['MACD_Hist'].tail(3)
    if len(hist_tail) == 3:
        vals = hist_tail.values
        min_delta = latest['close'] * 0.0001  # 价格的0.01%作为最小变化量
        if (vals[-1] > vals[-2] + min_delta and vals[-2] > vals[-3] + min_delta
                and vals[-1] > 0):
            score += 10
            details.append('红柱放大+10')
        elif (vals[-1] < vals[-2] - min_delta and vals[-2] < vals[-3] - min_delta
              and vals[-1] < 0):
            score -= 5
            details.append('绿柱放大-5')

    return max(score, 0), ','.join(details)


# ─── KDJ 打分 (满分 30) ───
def score_kdj(df: pd.DataFrame, n=9, s1=3, s2=3) -> tuple[int, str]:
    """
    KDJ维度打分:
    - 超卖区金叉 +15, 超买区死叉 -10
    - J值方向: J上行 +5, J下行 -3
    - KDJ位置: 20<K<80 中性区 +5, K<20超卖 +10, K>80超买 -5
    """
    data = df.copy()
    low_n = data['low'].rolling(n).min()
    high_n = data['high'].rolling(n).max()
    rsv = (data['close'] - low_n) / (high_n - low_n).replace(0, 1) * 100
    data['K'] = rsv.ewm(alpha=1/s1, adjust=False).mean()
    data['D'] = data['K'].ewm(alpha=1/s2, adjust=False).mean()
    data['J'] = 3 * data['K'] - 2 * data['D']

    latest = data.iloc[-1]
    prev = data.iloc[-2] if len(data) > 1 else latest
    score = 0
    details = []

    k, d, j = latest['K'], latest['D'], latest['J']

    # KDJ位置
    if k < 20:
        score += 10
        details.append('超卖区+10')
    elif k > 80:
        score -= 5
        details.append('超买区-5')
    else:
        score += 5
        details.append('中性区+5')

    # 金叉/死叉（最近3天）
    recent = data.tail(3)
    for i in range(1, len(recent)):
        curr_k, curr_d = recent.iloc[i]['K'], recent.iloc[i]['D']
        prev_k, prev_d = recent.iloc[i-1]['K'], recent.iloc[i-1]['D']
        if prev_k <= prev_d and curr_k > curr_d:
            if curr_k < 30:
                score += 15
                details.append('超卖金叉+15')
            else:
                score += 8
                details.append('金叉+8')
            break
        elif prev_k >= prev_d and curr_k < curr_d:
            if curr_k > 70:
                score -= 10
                details.append('超买死叉-10')
            break

    # J值方向
    if j > prev['J']:
        score += 5
        details.append('J上行+5')
    else:
        score -= 3
        details.append('J下行-3')

    return max(score, 0), ','.join(details)


# ─── 成交量打分 (满分 20) ───
def score_volume(df: pd.DataFrame) -> tuple[int, str]:
    """
    成交量维度打分:
    - 量价配合: 价涨量增 +10, 价涨量缩(背离) -5
    - 量比(今日量/5日均量): >1.5 放量 +5, 0.8~1.5 正常 +3, <0.8 缩量 +0
    - 连续放量(3天量递增且价格上涨) +5
    """
    data = df.copy()
    data['vol_ma5'] = data['volume'].rolling(5).mean()
    data['vol_ma20'] = data['volume'].rolling(20).mean()

    latest = data.iloc[-1]
    score = 0
    details = []

    # 量比
    vol_ratio = latest['volume'] / latest['vol_ma5'] if latest['vol_ma5'] > 0 else 1
    if vol_ratio > 1.5:
        score += 5
        details.append(f'放量({vol_ratio:.1f})+5')
    elif vol_ratio >= 0.8:
        score += 3
        details.append(f'正常量({vol_ratio:.1f})+3')
    else:
        details.append(f'缩量({vol_ratio:.1f})+0')

    # 量价配合（最近一天）
    if latest['pct_change'] > 0 and latest['volume'] > latest['vol_ma5']:
        score += 10
        details.append('价涨量增+10')
    elif latest['pct_change'] > 0 and latest['volume'] < latest['vol_ma5'] * 0.7:
        score -= 5
        details.append('价涨量缩-5')
    elif latest['pct_change'] < 0 and latest['volume'] > latest['vol_ma5'] * 1.5:
        score -= 3
        details.append('放量下跌-3')

    # 连续放量上涨
    tail3 = data.tail(3)
    if len(tail3) == 3:
        vols = tail3['volume'].values
        pcts = tail3['pct_change'].values
        if vols[-1] > vols[-2] > vols[-3] and all(p > 0 for p in pcts):
            score += 5
            details.append('连续放量涨+5')

    return max(score, 0), ','.join(details)


# ─── 趋势打分 (满分 15) ───
def score_trend(df: pd.DataFrame) -> tuple[int, str]:
    """
    趋势维度打分:
    - MA5 > MA20 > MA60 多头排列 +10
    - 价格在MA5之上(含) +5
    - 价格在MA20之下(严格) -5
    """
    data = df.copy()
    data['MA5'] = data['close'].rolling(5).mean()
    data['MA20'] = data['close'].rolling(20).mean()
    data['MA60'] = data['close'].rolling(60).mean()

    latest = data.iloc[-1]
    score = 0
    details = []

    ma5 = latest.get('MA5', np.nan)
    ma20 = latest.get('MA20', np.nan)
    ma60 = latest.get('MA60', np.nan)
    close = latest['close']

    # 容差：价格的 0.1% 以内视为"持平"
    eps = close * 0.001 if close > 0 else 0.01

    if pd.notna(ma5) and pd.notna(ma20) and pd.notna(ma60):
        if ma5 > ma20 > ma60:
            score += 10
            details.append('多头排列+10')
        elif ma5 < ma20 < ma60:
            score -= 5
            details.append('空头排列-5')

    if pd.notna(ma5) and close >= ma5 - eps:
        score += 5
        details.append('站上MA5+5')
    elif pd.notna(ma20) and close < ma20 - eps:
        score -= 5
        details.append('跌破MA20-5')

    return max(score, 0), ','.join(details)


# ─── 综合打分 ───
def technical_score(df: pd.DataFrame) -> dict:
    """综合技术面打分，满分100"""
    macd_s, macd_d = score_macd(df)
    kdj_s, kdj_d = score_kdj(df)
    vol_s, vol_d = score_volume(df)
    trend_s, trend_d = score_trend(df)
    total = macd_s + kdj_s + vol_s + trend_s
    return {
        'total': total,
        'macd_score': macd_s, 'macd_detail': macd_d,
        'kdj_score': kdj_s, 'kdj_detail': kdj_d,
        'vol_score': vol_s, 'vol_detail': vol_d,
        'trend_score': trend_s, 'trend_detail': trend_d,
    }

# ─── 布林线下轨反弹打分 (满分 100) ───
def score_boll_lower_bounce(df: pd.DataFrame, period: int = 20, num_std: float = 2.0) -> dict:
    """
    布林线下轨反弹可靠性评分，从四个维度量化：
    1. 价格位置 %b 指标 (满分 30)
    2. 量能配合 (满分 25)
    3. K线形态 (满分 25)
    4. 站稳判定 (满分 20)

    返回 dict: {boll_score, boll_detail, boll_signal, upper, mid, lower, pct_b}
    """
    data = df.copy()
    data['mid'] = data['close'].rolling(period).mean()
    data['std'] = data['close'].rolling(period).std()
    data['upper'] = data['mid'] + num_std * data['std']
    data['lower'] = data['mid'] - num_std * data['std']
    bw = data['upper'] - data['lower']
    data['pct_b'] = (data['close'] - data['lower']) / bw.replace(0, np.nan)

    # 量能辅助
    data['vol_ma5'] = data['volume'].rolling(5).mean()
    # OBV
    obv = [0.0]
    closes = data['close'].values
    vols = data['volume'].values
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv.append(obv[-1] + vols[i])
        elif closes[i] < closes[i - 1]:
            obv.append(obv[-1] - vols[i])
        else:
            obv.append(obv[-1])
    data['OBV'] = obv

    # 需要足够数据
    if len(data) < period + 10:
        return {'boll_score': 0, 'boll_detail': '数据不足', 'boll_signal': False,
                'upper': np.nan, 'mid': np.nan, 'lower': np.nan, 'pct_b': np.nan}

    latest = data.iloc[-1]
    prev = data.iloc[-2]
    score = 0
    details = []

    pct_b_now = latest['pct_b']
    lower_now = round(latest['lower'], 2)
    mid_now = round(latest['mid'], 2)
    upper_now = round(latest['upper'], 2)

    # ═══════════════════════════════════════════
    # 维度1: 价格位置 %b 指标 (满分 30)
    # ═══════════════════════════════════════════

    # 1a. 超卖确认：近5日内 %b 曾 < 0（跌破下轨）
    recent_5 = data.tail(5)
    touched_lower = (recent_5['pct_b'] < 0).any()
    if touched_lower:
        score += 10
        details.append('%b曾<0(触及下轨)+10')

    # 1b. 反弹触发：%b 从 <0 回到 >=0（收回轨内）
    if pct_b_now >= 0:
        # 检查近5日是否有从轨外收回的过程
        pct_b_vals = recent_5['pct_b'].values
        was_below = False
        recovered = False
        for v in pct_b_vals:
            if v < 0:
                was_below = True
            elif was_below and v >= 0:
                recovered = True
                break
        if recovered:
            score += 10
            details.append('%b收回轨内+10')

    # 1c. 底背离：股价第二次探底时 %b 高于第一次
    recent_20 = data.tail(20)
    low_points = recent_20[recent_20['pct_b'] < 0.1]
    if len(low_points) >= 2:
        # 找两个低点区域
        first_low_price = low_points.iloc[0]['close']
        first_low_pctb = low_points.iloc[0]['pct_b']
        last_low_price = low_points.iloc[-1]['close']
        last_low_pctb = low_points.iloc[-1]['pct_b']
        if last_low_price <= first_low_price and last_low_pctb > first_low_pctb:
            score += 10
            details.append(f'%b底背离+10')

    # ═══════════════════════════════════════════
    # 维度2: 量能配合 (满分 25)
    # ═══════════════════════════════════════════

    vol_ma5 = latest['vol_ma5']
    vol_now = latest['volume']

    # 2a. 抛压枯竭（缩量）：近5日内出现成交量 < 5日均量 * 0.6
    recent_vols = recent_5['volume'].values
    recent_vol_ma5 = recent_5['vol_ma5'].values
    shrink_found = False
    for v, ma in zip(recent_vols, recent_vol_ma5):
        if ma > 0 and v < ma * 0.6:
            shrink_found = True
            break
    if shrink_found:
        score += 8
        details.append('抛压枯竭(缩量)+8')

    # 2b. 确认反弹（放量阳线）
    if latest['pct_change'] > 0 and vol_ma5 > 0:
        vol_ratio = vol_now / vol_ma5
        if vol_ratio > 1.5:
            score += 10
            details.append(f'反弹放量({vol_ratio:.1f})+10')
        elif vol_ratio > 1.2:
            score += 5
            details.append(f'反弹温和放量({vol_ratio:.1f})+5')

    # 2c. OBV量价背离：股价在下轨附近但OBV调头向上
    if pct_b_now < 0.3:
        obv_tail5 = data['OBV'].tail(5).values
        if len(obv_tail5) >= 3 and obv_tail5[-1] > obv_tail5[-3]:
            # OBV近5日上行
            price_tail5 = data['close'].tail(5).values
            if price_tail5[-1] <= price_tail5[-3]:
                score += 7
                details.append('OBV量价背离+7')

    # ═══════════════════════════════════════════
    # 维度3: K线形态 (满分 25)
    # ═══════════════════════════════════════════

    o, c, h, l = latest['open'], latest['close'], latest['high'], latest['low']
    body = abs(c - o)
    lower_shadow = min(o, c) - l

    # 3a. 长下影线：下影线 / 实体 > 2
    if body > 0 and lower_shadow / body > 2:
        score += 10
        details.append(f'长下影线({lower_shadow / body:.1f}倍)+10')

    # 3b. 阳包阴（反包）：今日阳线实体完全覆盖昨日阴线实体
    prev_o, prev_c = prev['open'], prev['close']
    if (prev_c < prev_o  # 昨日阴线
            and c > o  # 今日阳线
            and o <= prev_c  # 今开 <= 昨收
            and c >= prev_o):  # 今收 >= 昨开
        score += 10
        details.append('阳包阴+10')

    # 3c. 十字星/锤子线（实体极小 + 下影线长）
    if body > 0 and body < (h - l) * 0.15 and lower_shadow > body * 2:
        score += 5
        details.append('锤子线/十字星+5')

    # ═══════════════════════════════════════════
    # 维度4: 站稳判定 (满分 20)
    # ═══════════════════════════════════════════

    # 4a. 连续2个交易日收盘价站稳在下轨之上
    tail3 = data.tail(3)
    if len(tail3) >= 2:
        last2_close = tail3['close'].values[-2:]
        last2_lower = tail3['lower'].values[-2:]
        if all(c > lb for c, lb in zip(last2_close, last2_lower)):
            score += 10
            details.append('连续2日站稳下轨+10')

    # 4b. 收盘价从下轨下方回到下轨上方（趋势反转确认）
    tail5 = data.tail(5)
    close_vals = tail5['close'].values
    lower_vals = tail5['lower'].values
    was_below_lower = False
    back_above = False
    for cv, lv in zip(close_vals, lower_vals):
        if cv < lv:
            was_below_lower = True
        elif was_below_lower and cv > lv:
            back_above = True
            break
    if back_above:
        score += 10
        details.append('收盘回到下轨上方+10')

    # 判断是否触发下轨反弹信号（综合分 >= 40 且近期确实触及下轨）
    is_signal = score >= 40 and touched_lower

    return {
        'boll_score': score,
        'boll_detail': ','.join(details) if details else '无信号',
        'boll_signal': is_signal,
        'upper': upper_now,
        'mid': mid_now,
        'lower': lower_now,
        'pct_b': round(pct_b_now, 3) if pd.notna(pct_b_now) else None,
    }


# ─── 布林线中轨反弹打分 (满分 100) ───
def score_boll_mid_bounce(df: pd.DataFrame, period: int = 20, num_std: float = 2.0) -> dict:
    """
    布林线中轨（MA20）回踩反弹评分 — 趋势延续型二级买点。
    四个维度量化：
    1. 趋势背景 (满分 25) — 中轨斜率向上 + 价格未有效跌破中轨
    2. 回调缩量 (满分 25) — 回调量能萎缩 + 波幅压缩
    3. 反弹放量 (满分 30) — 量比确认 + 放量倍率 + 吞没形态
    4. 动能与资金 (满分 20) — ADX趋势强度 + MFM资金流量

    返回 dict: {mid_bounce_score, mid_bounce_detail, mid_bounce_signal, ...}
    """
    data = df.copy()
    data['mid'] = data['close'].rolling(period).mean()
    data['std'] = data['close'].rolling(period).std()
    data['upper'] = data['mid'] + num_std * data['std']
    data['lower'] = data['mid'] - num_std * data['std']
    bw = data['upper'] - data['lower']
    data['pct_b'] = (data['close'] - data['lower']) / bw.replace(0, np.nan)
    data['vol_ma5'] = data['volume'].rolling(5).mean()
    data['MA5'] = data['close'].rolling(5).mean()

    if len(data) < period + 20:
        return {'mid_bounce_score': 0, 'mid_bounce_detail': '数据不足',
                'mid_bounce_signal': False}

    latest = data.iloc[-1]
    prev = data.iloc[-2]
    score = 0
    details = []

    mid_now = latest['mid']
    pct_b_now = latest['pct_b']

    # ═══════════════════════════════════════════
    # 维度1: 趋势背景 (满分 25)
    # ═══════════════════════════════════════════

    # 1a. 中轨斜率向上：最近5日MA20逐步抬升
    mid_5 = data['mid'].tail(5).values
    if len(mid_5) == 5 and mid_5[-1] > mid_5[0]:
        slope_pct = (mid_5[-1] - mid_5[0]) / mid_5[0] * 100
        if slope_pct > 0.3:
            score += 10
            details.append(f'中轨上行({slope_pct:.2f}%)+10')
        elif slope_pct > 0:
            score += 5
            details.append(f'中轨微升({slope_pct:.2f}%)+5')
    else:
        # 中轨走平或向下 — 不具备支撑力，扣分警示
        score -= 5
        details.append('中轨走平/下行-5')

    # 1b. 价格未连续2天有效跌破中轨
    tail5 = data.tail(5)
    close_below_mid = tail5['close'] < tail5['mid']
    consecutive_below = 0
    for below in close_below_mid.values:
        if below:
            consecutive_below += 1
        else:
            consecutive_below = 0
    if consecutive_below < 2:
        score += 10
        details.append('未连续跌破中轨+10')
    else:
        score -= 5
        details.append(f'连续{consecutive_below}日跌破中轨-5')

    # 1c. %b 精准回踩中轨区域 (0.45 < %b < 0.55)
    if 0.45 <= pct_b_now <= 0.55:
        score += 5
        details.append(f'%b精准回踩中轨({pct_b_now:.2f})+5')

    # ═══════════════════════════════════════════
    # 维度2: 回调缩量 — Testing Supply (满分 25)
    # ═══════════════════════════════════════════

    # 识别回调期：从近期高点回落到中轨附近的阶段
    recent_10 = data.tail(10)
    # 找到近10日最高收盘价位置作为上涨波段参考
    peak_idx = recent_10['close'].idxmax()
    peak_pos = recent_10.index.get_loc(peak_idx)

    # 上涨波段均量 vs 回调波段均量
    if peak_pos < len(recent_10) - 1:
        up_phase = recent_10.iloc[:peak_pos + 1]
        pullback_phase = recent_10.iloc[peak_pos + 1:]

        if len(up_phase) > 0 and len(pullback_phase) > 0:
            up_avg_vol = up_phase['volume'].mean()
            pb_avg_vol = pullback_phase['volume'].mean()

            # 2a. 回调量能萎缩比 < 60%
            if up_avg_vol > 0:
                vol_shrink_ratio = pb_avg_vol / up_avg_vol
                if vol_shrink_ratio < 0.5:
                    score += 12
                    details.append(f'回调深度缩量({vol_shrink_ratio:.0%})+12')
                elif vol_shrink_ratio < 0.6:
                    score += 8
                    details.append(f'回调缩量({vol_shrink_ratio:.0%})+8')
                elif vol_shrink_ratio < 0.8:
                    score += 4
                    details.append(f'回调温和缩量({vol_shrink_ratio:.0%})+4')

            # 2b. 波幅压缩：回调期K线实体变短
            pb_bodies = (pullback_phase['close'] - pullback_phase['open']).abs()
            up_bodies = (up_phase['close'] - up_phase['open']).abs()
            if up_bodies.mean() > 0:
                body_ratio = pb_bodies.mean() / up_bodies.mean()
                if body_ratio < 0.5:
                    score += 8
                    details.append(f'波幅压缩({body_ratio:.0%})+8')
                elif body_ratio < 0.7:
                    score += 4
                    details.append(f'波幅收窄({body_ratio:.0%})+4')

    # 2c. 回调期无放量大阴线（假反弹警示）
    pullback_tail3 = data.tail(3)
    big_drop_with_vol = False
    for _, row in pullback_tail3.iterrows():
        if (row['pct_change'] < -3
                and row['vol_ma5'] > 0
                and row['volume'] > row['vol_ma5'] * 1.5):
            big_drop_with_vol = True
            break
    if big_drop_with_vol:
        score -= 5
        details.append('回调放量大阴-5')
    else:
        score += 5
        details.append('回调无放量大阴+5')

    # ═══════════════════════════════════════════
    # 维度3: 反弹放量确认 — Demand Entry (满分 30)
    # ═══════════════════════════════════════════

    vol_ma5 = latest['vol_ma5']
    vol_now = latest['volume']

    # 3a. 量比 > 1.5（反弹当日）
    if vol_ma5 > 0 and latest['pct_change'] > 0:
        vol_ratio = vol_now / vol_ma5
        if vol_ratio > 1.5:
            score += 10
            details.append(f'反弹量比({vol_ratio:.1f})+10')
        elif vol_ratio > 1.2:
            score += 5
            details.append(f'反弹温和放量({vol_ratio:.1f})+5')

    # 3b. 放量倍率：反弹阳线量 > 前一日缩量阴线的 1.5 倍
    if (latest['pct_change'] > 0 and prev['pct_change'] < 0
            and prev['volume'] > 0):
        vol_amplify = vol_now / prev['volume']
        if vol_amplify > 1.5:
            score += 10
            details.append(f'放量倍率({vol_amplify:.1f}x)+10')

    # 3c. 吞没形态（阳包阴）+ 收盘站上MA5
    o, c = latest['open'], latest['close']
    prev_o, prev_c = prev['open'], prev['close']
    ma5_now = latest['MA5']
    if (prev_c < prev_o  # 昨日阴线
            and c > o  # 今日阳线
            and o <= prev_c and c >= prev_o  # 阳包阴
            and pd.notna(ma5_now) and c > ma5_now):  # 收盘站上MA5
        score += 10
        details.append('阳包阴+站上MA5+10')
    elif (c > o and pd.notna(ma5_now) and c > ma5_now
          and latest['pct_change'] > 0):
        # 非吞没但阳线站上MA5
        score += 5
        details.append('阳线站上MA5+5')

    # ═══════════════════════════════════════════
    # 维度4: 动能与资金 (满分 20)
    # ═══════════════════════════════════════════

    # 4a. ADX(14) > 25 确认强趋势
    adx_val = _calc_adx(data, n=14)
    if adx_val is not None:
        if adx_val > 25:
            score += 10
            details.append(f'ADX强趋势({adx_val:.0f})+10')
        elif adx_val > 20:
            score += 5
            details.append(f'ADX中等趋势({adx_val:.0f})+5')
        else:
            details.append(f'ADX弱趋势({adx_val:.0f})+0')

    # 4b. MFM（资金流量乘数）维持在0轴上方
    mfm = _calc_mfm(data)
    if mfm is not None:
        if mfm > 0:
            score += 10
            details.append(f'MFM正向({mfm:.2f})+10')
        else:
            score -= 3
            details.append(f'MFM负向({mfm:.2f})-3')

    # ─── 假反弹警示 ───
    # 布林线开口极度收缩（Squeeze）— 需等待方向确认
    if pd.notna(latest['std']) and mid_now > 0:
        bandwidth = (latest['upper'] - latest['lower']) / mid_now
        if bandwidth < 0.04:
            score -= 5
            details.append(f'布林Squeeze({bandwidth:.3f})-5')

    # 判断是否触发中轨反弹信号
    # 条件：评分>=40 且 %b在中轨附近(0.3~0.7) 且 中轨向上
    mid_slope_up = len(mid_5) == 5 and mid_5[-1] > mid_5[0]
    near_mid = 0.3 <= pct_b_now <= 0.7 if pd.notna(pct_b_now) else False
    is_signal = score >= 40 and near_mid and mid_slope_up

    return {
        'mid_bounce_score': max(score, 0),
        'mid_bounce_detail': ','.join(details) if details else '无信号',
        'mid_bounce_signal': is_signal,
        'mid_val': round(mid_now, 2),
        'mid_pct_b': round(pct_b_now, 3) if pd.notna(pct_b_now) else None,
    }


def _calc_adx(data: pd.DataFrame, n: int = 14) -> float | None:
    """计算 ADX(n)，返回最新值"""
    if len(data) < n * 2 + 1:
        return None
    high = data['high']
    low = data['low']
    close = data['close']

    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / n, min_periods=n, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1 / n, min_periods=n, adjust=False).mean() / atr)

    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1) * 100
    adx = dx.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()

    val = adx.iloc[-1]
    return float(val) if pd.notna(val) else None


def _calc_mfm(data: pd.DataFrame, n: int = 5) -> float | None:
    """
    计算资金流量乘数 MFM (Money Flow Multiplier) 的近 n 日均值。
    MFM = [(Close - Low) - (High - Close)] / (High - Low)
    """
    hl = data['high'] - data['low']
    mfm = ((data['close'] - data['low']) - (data['high'] - data['close'])) / hl.replace(0, np.nan)
    recent = mfm.tail(n).dropna()
    if len(recent) == 0:
        return None
    return float(recent.mean())


def calc_high120_drop(df: pd.DataFrame) -> dict:
    """计算近120个交易日最高价及其与最新收盘价的涨跌幅"""
    recent = df.tail(120)
    high120 = recent['high'].max()
    high120_date = recent['high'].idxmax().strftime('%Y-%m-%d')
    latest_close = df.iloc[-1]['close']
    drop_pct = round((latest_close - high120) / high120 * 100, 2)
    return {
        'high120': round(high120, 2),
        'high120_date': high120_date,
        'high120_drop_pct': drop_pct,
    }



# ─── 单只股票分析 ───
async def analyze_stock(name: str, code: str, idx: int, total: int) -> dict | None:
    stock_info = _make_stock_info(name, code)
    try:
        klines = await get_stock_day_range_kline_by_db_cache(stock_info, limit=200)
        if not klines or len(klines) < 60:
            print(f"[{idx}/{total}] {name}({code}) - 数据不足，跳过")
            return None
        df = build_df(klines)
        result = technical_score(df)
        high120_info = calc_high120_drop(df)
        boll_info = score_boll_lower_bounce(df)
        mid_info = score_boll_mid_bounce(df)
        latest = df.iloc[-1]
        result.update({'name': name, 'code': code, 'close': round(latest['close'], 2),
                       'date': df.index[-1].strftime('%Y-%m-%d'),
                       **high120_info, **boll_info, **mid_info})
        tag = '✅' if result['total'] >= 50 else '  '
        print(f"[{idx}/{total}] {tag} {name:<8} {code:<12} 总分:{result['total']:>3} "
              f"MACD:{result['macd_score']:>2} KDJ:{result['kdj_score']:>2} "
              f"量能:{result['vol_score']:>2} 趋势:{result['trend_score']:>2} "
              f"120日高:{result['high120']} 跌幅:{result['high120_drop_pct']}%")
        return result
    except Exception as e:
        print(f"[{idx}/{total}] {name}({code}) - 错误: {e}")
        return None


# ─── 输出结果 ───
def write_result(results: list[dict], path: Path):
    qualified = sorted([r for r in results if r['total'] >= 50], key=lambda x: -x['total'])
    # 筛选下轨反弹信号股票
    bounce_stocks = [r for r in qualified if r.get('boll_signal')]
    # 筛选中轨反弹信号股票
    mid_bounce_stocks = [r for r in qualified if r.get('mid_bounce_signal')]

    lines = [
        f"# 技术面打分结果（≥50分）",
        f"",
        f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"分析股票数: {len(results)}，达标股票数: {len(qualified)}，"
        f"下轨反弹信号: {len(bounce_stocks)}，中轨反弹信号: {len(mid_bounce_stocks)}",
        f"",
        f"评分维度: MACD(35分) + KDJ(30分) + 成交量(20分) + 趋势(15分) = 满分100分",
        f"",
        f"| 排名 | 股票名称 | 代码 | 总分 | MACD | KDJ | 量能 | 趋势 | 收盘价 | 布林下轨 | 120日最高 | 距高点跌幅 | 日期 |",
        f"|------|----------|------|------|------|-----|------|------|--------|----------|-----------|-----------|------|",
    ]
    for i, r in enumerate(qualified, 1):
        lines.append(
            f"| {i} | {r['name']} | {r['code']} | {r['total']} | {r['macd_score']} | "
            f"{r['kdj_score']} | {r['vol_score']} | {r['trend_score']} | {r['close']} | "
            f"{r.get('lower', '-')} | "
            f"{r.get('high120', '-')} | {r.get('high120_drop_pct', '-')}% | {r['date']} |"
        )

    # ─── 下轨反弹信号专区 ───
    if bounce_stocks:
        bounce_sorted = sorted(bounce_stocks, key=lambda x: -x['boll_score'])
        lines.append(f"\n## 🔻 下轨反弹信号（第二轮筛选）\n")
        lines.append(f"筛选条件: 综合评分≥50 且 布林下轨反弹评分≥40 且 近期触及下轨")
        lines.append(f"反弹评分维度: 价格%b(30分) + 量能配合(25分) + K线形态(25分) + 站稳判定(20分) = 满分100分\n")
        lines.append(f"| 排名 | 股票名称 | 代码 | 综合分 | 反弹分 | %b | 收盘价 | 下轨 | 中轨 | 日期 |")
        lines.append(f"|------|----------|------|--------|--------|------|--------|------|------|------|")
        for i, r in enumerate(bounce_sorted, 1):
            lines.append(
                f"| {i} | {r['name']} | {r['code']} | {r['total']} | {r['boll_score']} | "
                f"{r.get('pct_b', '-')} | {r['close']} | {r.get('lower', '-')} | "
                f"{r.get('mid', '-')} | {r['date']} |"
            )
        lines.append(f"\n### 下轨反弹评分细则\n")
        for r in bounce_sorted:
            lines.append(f"#### {r['name']}({r['code']}) - 反弹分 {r['boll_score']}")
            lines.append(f"- 综合技术分: {r['total']} (MACD:{r['macd_score']} KDJ:{r['kdj_score']} 量能:{r['vol_score']} 趋势:{r['trend_score']})")
            lines.append(f"- 下轨反弹({r['boll_score']}): {r['boll_detail']}")
            lines.append(f"- %b={r.get('pct_b', '-')} 下轨={r.get('lower', '-')} 中轨={r.get('mid', '-')} 上轨={r.get('upper', '-')}")
            lines.append("")

    # ─── 中轨反弹信号专区 ───
    if mid_bounce_stocks:
        mid_sorted = sorted(mid_bounce_stocks, key=lambda x: -x['mid_bounce_score'])
        lines.append(f"\n## 📈 中轨反弹信号（第二轮筛选 — 趋势延续）\n")
        lines.append(f"筛选条件: 综合评分≥50 且 中轨反弹评分≥40 且 %b在0.3~0.7 且 中轨上行")
        lines.append(f"反弹评分维度: 趋势背景(25分) + 回调缩量(25分) + 反弹放量(30分) + 动能资金(20分) = 满分100分\n")
        lines.append(f"| 排名 | 股票名称 | 代码 | 综合分 | 中轨反弹分 | %b | 收盘价 | 中轨 | 日期 |")
        lines.append(f"|------|----------|------|--------|-----------|------|--------|------|------|")
        for i, r in enumerate(mid_sorted, 1):
            lines.append(
                f"| {i} | {r['name']} | {r['code']} | {r['total']} | {r['mid_bounce_score']} | "
                f"{r.get('mid_pct_b', '-')} | {r['close']} | {r.get('mid_val', '-')} | {r['date']} |"
            )
        lines.append(f"\n### 中轨反弹评分细则\n")
        for r in mid_sorted:
            lines.append(f"#### {r['name']}({r['code']}) - 中轨反弹分 {r['mid_bounce_score']}")
            lines.append(f"- 综合技术分: {r['total']} (MACD:{r['macd_score']} KDJ:{r['kdj_score']} 量能:{r['vol_score']} 趋势:{r['trend_score']})")
            lines.append(f"- 中轨反弹({r['mid_bounce_score']}): {r['mid_bounce_detail']}")
            lines.append(f"- %b={r.get('mid_pct_b', '-')} 中轨={r.get('mid_val', '-')}")
            lines.append("")

    lines.append(f"\n## 评分细则\n")
    for r in qualified:
        lines.append(f"### {r['name']}({r['code']}) - 总分 {r['total']}")
        lines.append(f"- MACD({r['macd_score']}): {r['macd_detail']}")
        lines.append(f"- KDJ({r['kdj_score']}): {r['kdj_detail']}")
        lines.append(f"- 量能({r['vol_score']}): {r['vol_detail']}")
        lines.append(f"- 趋势({r['trend_score']}): {r['trend_detail']}")
        if r.get('boll_score', 0) > 0:
            lines.append(f"- 下轨反弹({r['boll_score']}): {r['boll_detail']}")
        if r.get('mid_bounce_score', 0) > 0:
            lines.append(f"- 中轨反弹({r['mid_bounce_score']}): {r['mid_bounce_detail']}")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines), encoding='utf-8')
    print(f"\n结果已写入: {path}")


async def main():
    stocks = parse_stock_list(SCORE_LIST_PATH)
    print(f"共解析到 {len(stocks)} 只股票，开始技术面打分...\n")

    results = []
    total = len(stocks)
    for i, s in enumerate(stocks, 1):
        r = await analyze_stock(s['name'], s['code'], i, total)
        if r:
            results.append(r)

    write_result(results, OUTPUT_PATH)
    save_score_results(results)
    print(f"打分结果已保存到数据库")

    qualified = [r for r in results if r['total'] >= 50]
    bounce_signals = [r for r in qualified if r.get('boll_signal')]
    mid_bounce_signals = [r for r in qualified if r.get('mid_bounce_signal')]
    print(f"\n{'='*60}")
    print(f"分析完成: 共 {len(results)} 只有效股票，{len(qualified)} 只达到50分以上")
    if bounce_signals:
        print(f"第二轮筛选(下轨反弹): {len(bounce_signals)} 只")
        for r in sorted(bounce_signals, key=lambda x: -x['boll_score']):
            print(f"  🔻 {r['name']:<8} {r['code']:<12} 综合:{r['total']:>3} "
                  f"反弹:{r['boll_score']:>3} %b={r.get('pct_b', '-')}")
    else:
        print(f"第二轮筛选(下轨反弹): 暂无信号")
    if mid_bounce_signals:
        print(f"第二轮筛选(中轨反弹): {len(mid_bounce_signals)} 只")
        for r in sorted(mid_bounce_signals, key=lambda x: -x['mid_bounce_score']):
            print(f"  📈 {r['name']:<8} {r['code']:<12} 综合:{r['total']:>3} "
                  f"中轨反弹:{r['mid_bounce_score']:>3} %b={r.get('mid_pct_b', '-')}")
    else:
        print(f"第二轮筛选(中轨反弹): 暂无信号")
    print(f"{'='*60}")


if __name__ == '__main__':
    asyncio.run(main())
