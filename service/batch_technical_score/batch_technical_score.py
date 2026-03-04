#!/usr/bin/env python3
"""
批量技术面打分：遍历 stock_score_list.md 中的股票，
使用 get_stock_day_range_kline_by_db_cache 获取日线数据，
通过 MACD、KDJ、交易量等维度综合打分，输出 ≥50 分的股票清单。
第二轮分析：所有股票进入布林线下轨反弹、中轨反弹筛选。
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
from service.jqka10.stock_finance_data_10jqka import get_financial_data_from_db
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
    布林线下轨反弹可靠性评分 — 超跌反弹场景。

    为了量化布林线下轨反弹的可靠性，从价格位置（%b）、量能强弱、K线形态
    以及辅助指标四个维度设定具体的衡量指标，帮助客观判断这究竟是
    "超跌反弹"还是"破位下跌的开始"。

    ══════════════════════════════════════════════════════════════
    前置校验（硬性排除条件）
    ══════════════════════════════════════════════════════════════
    下轨反弹的核心定义：股价近期跌至下轨附近后反弹回来。
    必须同时满足：
    - 数据有效性: 最新收盘价和成交量 > 0（排除停牌/全0数据）。
    - 下轨正数: 布林线下轨必须为正数（下轨为负说明数据被停牌0值污染）。
    - 下轨区域: 当前 %b <= 0.5（超过0.5说明股价已远离下轨）。
    - 近期走势向上: 近3日收盘价上升，或近2日至少有1根阳线。
      两者都不满足则视为仍在下跌，不是反弹。

    ══════════════════════════════════════════════════════════════
    维度1: 价格位置指标 %b (满分 30)
    ══════════════════════════════════════════════════════════════
    %b 是布林线最直接的量化工具，衡量股价在波段中的相对位置。
    计算公式: %b = (现价 - 下轨线) / (上轨线 - 下轨线)

    - 1a. 超卖确认 (+10): %b < 0 表示股价已跌破下轨。
          近2日内 %b 曾 < 0 即触发（只捕捉最近1-2天的下轨反弹）。
    - 1b. 反弹触发 (+10): 当 %b 从 0 以下重新回到 0 以上，
          即股价由轨外收回到轨内，近2日窗口内发生。
    - 1c. 底背离 (+10): 股价第二次探底时 %b 明显高于第一次
          （即使股价新低），这是极强的反转指标。
          在近20日内寻找 %b < 0.1 的低点进行比较。

    ══════════════════════════════════════════════════════════════
    维度2: 量能配合指标 (满分 25)
    ══════════════════════════════════════════════════════════════
    量能是验证反弹真伪的核心，利用"量比"和"相对成交量"来衡量。

    - 2a. 抛压枯竭/缩量 (+8): 成交量 < 5日均量的 0.6倍。
          通常出现在连续阴线下跌后的极度缩量，暗示抛压枯竭。
    - 2b. 确认反弹/放量 (+10/+5):
          反弹阳线的成交量 > 5日均量的 1.5倍 得10分；
          > 1.2倍 得5分。量比(Volume Ratio) > 1.2 代表主动买盘介入。
    - 2c. OBV量价背离 (+7): 股价在下轨附近震荡（%b < 0.3），
          但 OBV（能量潮）指标率先调头向上（近5日OBV上行而价格未涨）。

    ══════════════════════════════════════════════════════════════
    维度3: K线形态衡量 — 反转力度 (满分 25)
    ══════════════════════════════════════════════════════════════
    通过 K 线实体的比例来衡量多头反击的强度。

    - 3a. 长下影线 (+10): (最低价 - 收盘价) / (收盘价 - 开盘价) > 2。
          长下影线代表在下轨处有强力资金介入。
    - 3b. 阳包阴/反包 (+10): 阳线实体完全覆盖前一日阴线实体的 100% 以上。
          条件: 昨日阴线(prev_c < prev_o) + 今日阳线(c > o)
          + 今开<=昨收 + 今收>=昨开。
    - 3c. 锤子线/十字星 (+5): 实体极小(< 振幅15%) + 下影线长(> 实体2倍)。

    ══════════════════════════════════════════════════════════════
    维度4: 站稳判定 (满分 20)
    ══════════════════════════════════════════════════════════════
    - 4a. 连续站稳 (+10): 股价收盘价连续 2 个交易日站稳在布林线下轨之上。
    - 4b. 趋势反转确认 (+10): 收盘价从下轨下方回到下轨上方，
          近5日内出现由下轨下→上轨上的转换过程。

    ══════════════════════════════════════════════════════════════
    长上影线警示
    ══════════════════════════════════════════════════════════════
    与中轨反弹一致的判断标准：(最高价-实体高点)/实体高度 > 2
    为长上影线，> 1.5 为偏长，提示上方抛压。
    实体为0（十字星）且有上影线也视为长上影。

    ══════════════════════════════════════════════════════════════
    信号触发条件
    ══════════════════════════════════════════════════════════════
    boll_signal = True 当且仅当: 综合评分 >= 60 且 近期确实触及下轨(%b曾<0)

    Args:
        df: 包含 open/close/high/low/volume/pct_change 的日线 DataFrame
        period: 布林线周期，默认20
        num_std: 标准差倍数，默认2.0

    Returns:
        dict: {boll_score, boll_detail, boll_signal, upper, mid, lower, pct_b}
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

    pct_b_now = latest['pct_b']
    lower_now = round(latest['lower'], 2)
    mid_now = round(latest['mid'], 2)
    upper_now = round(latest['upper'], 2)

    _zero_result = {'boll_score': 0, 'boll_signal': False,
                    'upper': upper_now, 'mid': mid_now,
                    'lower': lower_now,
                    'pct_b': round(pct_b_now, 3) if pd.notna(pct_b_now) else None}

    # ═══════════════════════════════════════════
    # 前置校验（硬性排除条件）
    # ═══════════════════════════════════════════
    # 下轨反弹的核心定义：股价近期跌至下轨附近后反弹回来。
    # 必须同时满足：
    #   1) 最新收盘价和成交量有效（排除停牌/全0数据）
    #   2) 布林线下轨为正数（下轨为负说明数据被停牌0值污染）
    #   3) 当前 %b 不能太高（%b > 0.5 说明股价已远离下轨，不是下轨反弹场景）
    #   4) 最近几日走势必须向上（近3日收盘价上升或近2日有阳线）

    # 校验1: 排除停牌/无效数据
    if latest['close'] <= 0 or latest['volume'] <= 0:
        return {**_zero_result, 'boll_detail': '停牌或无效数据'}

    # 校验2: 布林线下轨必须为正数
    if latest['lower'] <= 0:
        return {**_zero_result, 'boll_detail': '下轨异常(停牌数据污染)'}

    # 校验3: %b 不能太高，超过0.5说明股价已在中轨附近或以上，不属于下轨反弹
    if pd.notna(pct_b_now) and pct_b_now > 0.5:
        return {**_zero_result, 'boll_detail': f'%b={pct_b_now:.3f}远离下轨,非下轨反弹场景'}

    # 校验4: 最近走势必须向上（硬性条件）
    # 近3日收盘价必须呈现上升趋势：最新收盘价 > 3日前收盘价，
    # 或近2日至少有1日收阳线，证明有反弹动作。
    # 如果近几日仍在持续下跌，则不是"反弹"而是"继续下跌"。
    recent_3_close = data['close'].tail(3).values
    if len(recent_3_close) == 3:
        price_rising = recent_3_close[-1] > recent_3_close[0]
        recent_2_has_yang = any(
            data.iloc[i]['close'] > data.iloc[i]['open']
            for i in range(-2, 0)
        )
        if not price_rising and not recent_2_has_yang:
            return {**_zero_result,
                    'boll_detail': f'近期走势仍向下(近3日收盘{recent_3_close[0]:.2f}→{recent_3_close[-1]:.2f}),未出现反弹'}

    score = 0
    details = []

    # ═══════════════════════════════════════════
    # 维度1: 价格位置 %b 指标 (满分 30)
    # ═══════════════════════════════════════════

    # 1a. 超卖确认：近2日内 %b 曾 < 0（跌破下轨）— 只捕捉最近1-2天的下轨反弹
    recent_2 = data.tail(2)
    touched_lower = (recent_2['pct_b'] < 0).any()
    if touched_lower:
        score += 10
        details.append('%b曾<0(近2日触及下轨)+10')

    # 1b. 反弹触发：%b 从 <0 回到 >=0（收回轨内）— 近2日窗口
    if pct_b_now >= 0:
        pct_b_vals = recent_2['pct_b'].values
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

    # 2a. 抛压枯竭（缩量）：近3日内出现成交量 < 5日均量 * 0.6
    recent_3_vol = data.tail(3)
    recent_vols = recent_3_vol['volume'].values
    recent_vol_ma5 = recent_3_vol['vol_ma5'].values
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

    # ─── 长上影线警示 ───
    # 与中轨反弹保持一致：(最高价 - 实体高点) / 实体高度 > 2
    upper_shadow = latest['high'] - max(latest['open'], latest['close'])
    if body > 0:
        shadow_body_ratio = upper_shadow / body
        if shadow_body_ratio > 2:
            details.append(f'\n  ⚠ 反弹长上影线(上影/实体={shadow_body_ratio:.1f}),上方抛压较重,需观察确认')
        elif shadow_body_ratio > 1.5:
            details.append(f'\n  ⚠ 反弹上影线偏长(上影/实体={shadow_body_ratio:.1f}),建议观察1-2天确认')
    elif upper_shadow > 0:
        details.append(f'\n  ⚠ 十字星长上影线,反弹力度存疑')

    # 判断是否触发下轨反弹信号（综合分 >= 60 且近期确实触及下轨）
    is_signal = score >= 60 and touched_lower

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

    相比于布林线下轨的"超跌反弹"，中轨（通常是 20 日均线）的反弹更多属于
    趋势延续（Trend Following）的范畴。在威科夫理论和 CAN SLIM 法则中，
    这通常被视为"上升途中的回踩确认"或"二级买点"。
    在中轨形成有效反弹，其量价配合的衡量指标应聚焦于"回调缩量"与"启动放量"
    的对比，而非单纯的超卖修复。

    可将中轨反弹视为 CAN SLIM 中的 "底中底（Base on Base）" 或
    "手柄区（Handle）" 的低吸点。

    ══════════════════════════════════════════════════════════════
    维度1: 趋势背景指标 — 斜率与方向 (满分 25)
    ══════════════════════════════════════════════════════════════
    中轨（MB）本质上是股价的生命线。只有在中轨向上的背景下，
    回踩中轨才具有支撑意义。

    - 1a. 中轨斜率 (+10/+5): MA(20) 的导数需为正。
          最近5个交易日的中轨（MA20）重心稳步上移。
          斜率 > 0.3% 得10分，> 0 得5分。
          中轨走平或向下时扣5分（不具备支撑力，仅是震荡中枢）。
    - 1b. 价格站稳 (+10/-5): 股价回踩中轨时，收盘价不应连续2天
          有效跌破中轨。即使盘中有刺穿，收盘也需拉回。
          未连续跌破得10分，连续跌破扣5分。
    - 1c. %b 精准回踩 (+5): 0.45 < %b < 0.55 确认股价精准回踩中轨区域。

    ══════════════════════════════════════════════════════════════
    维度2: 回调期量化特征 — Testing Supply 测试供应 (满分 25)
    ══════════════════════════════════════════════════════════════
    当股价从中轨上方回落至中轨附近时，必须看到卖压的消失。

    - 2a. 量能萎缩比 (+12/+8/+4):
          回调至中轨时的平均成交量应小于前期上涨波段平均成交量的 50%-60%。
          以近10日最高收盘价为分界，划分上涨波段与回调波段。
          萎缩比 < 50% 得12分，< 60% 得8分，< 80% 得4分。
    - 2b. 波幅压缩 (+8/+4): K线的实体变短。
          回调期K线实体均值 / 上涨期K线实体均值 < 50% 得8分，< 70% 得4分。
          如果回踩中轨时仍是大阴线伴随放量，这往往是趋势转弱的信号。
    - 2c. 回调无放量大阴 (+5/-5): 近3日内无跌幅>3%且放量>1.5倍均量的大阴线。
          出现则扣5分（假反弹警示），未出现加5分。

    ══════════════════════════════════════════════════════════════
    维度3: 反弹期确认指标 — Demand Entry 需求介入 (满分 30)
    ══════════════════════════════════════════════════════════════
    当股价触碰中轨并开始调头向上时，需要"力量"的确认。

    - 3a. 量比 (+10/+5): 反弹当日的量比建议 > 1.5（得10分）或 > 1.2（得5分）。
          代表主动性买盘在 20 日均线位置产生了共识。
    - 3b. 放量倍率 (+10): 反弹阳线的成交量 > 前一日缩量阴线的 1.5 倍以上。
    - 3c. 吞没形态 (+10/+5):
          最好出现"阳包阴"或"曙光初现"形态，且阳线的收盘价需收在 5 日均线之上。
          完整阳包阴+站上MA5 得10分；仅阳线站上MA5 得5分。

    ══════════════════════════════════════════════════════════════
    维度4: 动能与资金 (满分 20)
    ══════════════════════════════════════════════════════════════
    - 4a. ADX(14) (+10/+5): ADX > 25 确保个股处于强势趋势中而非横盘震荡（得10分）。
          ADX > 20 中等趋势（得5分）。ADX < 20 弱趋势（0分）。
    - 4b. MFM 资金流量 (+10/-3):
          MFM = [(Close-Low) - (High-Close)] / (High-Low)
          回踩中轨时 MFM 近5日均值维持在 0 轴上方（得10分），
          代表机构大单并未因调整而大规模流出。负向扣3分。

    ══════════════════════════════════════════════════════════════
    假反弹警示
    ══════════════════════════════════════════════════════════════
    - 布林线开口极度收缩（Squeeze）: bandwidth < 0.04 时扣5分。
      如果布林线上下轨极度缩口，股价在中轨的震荡可能是在酝酿变盘，
      此时需等待突破方向确认。
    - 长上影线警示: (最高价-实体高点)/实体高度 > 2 为长上影线，
      > 1.5 为偏长，提示上方抛压较重，可能需要1-2天再确认。
      实体为0（十字星）且有上影线也视为长上影。
    - 中轨走平或向下: 此时的中轨不具备支撑力（维度1已处理）。
    - 无量反弹: 股价虽然在中轨止跌，但后续反弹成交量无法放大，
      通常会演变成"L型"盘整或二次下探（维度3未得分即体现）。
    - 无反弹动作: 今日收阴且跌幅为负，缺乏反弹力度，扣5分。

    ══════════════════════════════════════════════════════════════
    前置校验（硬性排除条件）
    ══════════════════════════════════════════════════════════════
    中轨反弹的核心定义：价格从中轨下方反弹到中轨上方。
    - 回踩历史验证: 近15日内至少3天收盘价在中轨上方（证明之前在上方运行）。
    - 触及中轨验证: 近5日内至少1天收盘价在中轨附近或下方（容差0.5%），
      证明确实回踩到了中轨。未触及中轨则不是中轨反弹。
    - 站回中轨验证: 今日收盘价必须站回中轨上方，证明反弹成功。
      收盘价仍在中轨下方 = 反弹未确认，直接返回0分。
    - 连续跌破排除: 连续3天以上收盘价跌破中轨 = 破位，不是回踩。
    - 反弹动作确认: 今日需收阳或涨幅>0，否则扣5分。

    ══════════════════════════════════════════════════════════════
    信号触发条件
    ══════════════════════════════════════════════════════════════
    mid_bounce_signal = True 当且仅当:
      评分 >= 60 且 %b 在中轨附近(0.4~0.65) 且 中轨斜率向上 且 有反弹动作

    Args:
        df: 包含 open/close/high/low/volume/pct_change 的日线 DataFrame
        period: 布林线周期，默认20
        num_std: 标准差倍数，默认2.0

    Returns:
        dict: {mid_bounce_score, mid_bounce_detail, mid_bounce_signal, mid_val, mid_pct_b}
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
    # 前置校验: 回踩模式验证（硬性条件）
    # ═══════════════════════════════════════════
    # 中轨反弹的核心定义：价格从中轨下方反弹到中轨上方
    # 必须同时满足：
    #   1) 近5日内至少有1天收盘价在中轨下方或触及中轨（证明曾回踩到中轨）
    #   2) 今日收盘价站回中轨上方（证明反弹成功）
    #   3) 近15日内至少有3天收盘价在中轨上方（证明之前在上方运行，是回踩而非长期弱势）

    recent_15 = data.tail(15)
    days_above_mid = (recent_15['close'] > recent_15['mid']).sum()
    has_prior_above = days_above_mid >= 3

    # 近5日是否有跌到中轨下方或触及中轨的记录（%b <= 0.55 视为触及中轨区域）
    tail5 = data.tail(5)
    had_touch_mid = False
    for i in range(len(tail5)):
        row = tail5.iloc[i]
        if row['close'] <= row['mid'] * 1.005:  # 收盘价在中轨附近或下方（容差0.5%）
            had_touch_mid = True
            break

    # 今日收盘价必须站回中轨上方
    close_above_mid = latest['close'] > mid_now

    if not has_prior_above:
        details.append(f'近15日仅{days_above_mid}日在中轨上方(需≥3),非回踩模式')
        return {
            'mid_bounce_score': 0,
            'mid_bounce_detail': ','.join(details),
            'mid_bounce_signal': False,
            'mid_val': round(mid_now, 2),
            'mid_pct_b': round(pct_b_now, 3) if pd.notna(pct_b_now) else None,
        }

    if not had_touch_mid:
        details.append('近5日未回踩中轨,非中轨反弹模式')
        return {
            'mid_bounce_score': 0,
            'mid_bounce_detail': ','.join(details),
            'mid_bounce_signal': False,
            'mid_val': round(mid_now, 2),
            'mid_pct_b': round(pct_b_now, 3) if pd.notna(pct_b_now) else None,
        }

    if not close_above_mid:
        details.append(f'收盘({latest["close"]:.2f})未站回中轨({mid_now:.2f})上方,反弹未确认')
        return {
            'mid_bounce_score': 0,
            'mid_bounce_detail': ','.join(details),
            'mid_bounce_signal': False,
            'mid_val': round(mid_now, 2),
            'mid_pct_b': round(pct_b_now, 3) if pd.notna(pct_b_now) else None,
        }

    # 连续跌破中轨天数检测（硬性排除）
    close_below_mid = tail5['close'] < tail5['mid']
    consecutive_below = 0
    for below in close_below_mid.values:
        if below:
            consecutive_below += 1
        else:
            consecutive_below = 0

    # 连续3天以上跌破中轨 = 破位，不是回踩
    if consecutive_below >= 3:
        details.append(f'连续{consecutive_below}日跌破中轨,已破位')
        return {
            'mid_bounce_score': 0,
            'mid_bounce_detail': ','.join(details),
            'mid_bounce_signal': False,
            'mid_val': round(mid_now, 2),
            'mid_pct_b': round(pct_b_now, 3) if pd.notna(pct_b_now) else None,
        }

    # 反弹确认：今日需有正向动能（收阳或涨幅>0）
    has_bounce_action = latest['pct_change'] > 0 or latest['close'] > latest['open']

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

    # 3a. 量比 > 1.5（反弹当日）— 必须是阳线才算反弹
    if vol_ma5 > 0 and has_bounce_action:
        vol_ratio = vol_now / vol_ma5
        if vol_ratio > 1.5:
            score += 10
            details.append(f'反弹量比({vol_ratio:.1f})+10')
        elif vol_ratio > 1.2:
            score += 5
            details.append(f'反弹温和放量({vol_ratio:.1f})+5')

    # 3b. 放量倍率：反弹阳线量 > 前一日缩量阴线的 1.5 倍
    if (has_bounce_action and prev['pct_change'] < 0
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

    # 长上影线警示：突破中轨当天出现较长上影线，说明上方抛压较重
    # 判断标准：(最高价 - 实体高点) / 实体高度 > 2
    upper_shadow = latest['high'] - max(latest['open'], latest['close'])
    body = abs(latest['close'] - latest['open'])
    if body > 0:
        shadow_body_ratio = upper_shadow / body
        if shadow_body_ratio > 2:
            details.append(f'\n  ⚠ 突破中轨长上影线(上影/实体={shadow_body_ratio:.1f}),可能需要1-2天再确认')
        elif shadow_body_ratio > 1.5:
            details.append(f'\n  ⚠ 突破中轨上影线偏长(上影/实体={shadow_body_ratio:.1f}),建议观察1-2天确认')
    elif upper_shadow > 0:
        # 实体为0（十字星），有上影线即视为长上影
        details.append(f'\n  ⚠ 突破中轨十字星长上影线,可能需要1-2天再确认')

    # 无反弹动作扣分：今日收阴且跌幅为负，缺乏反弹力度
    if not has_bounce_action:
        score -= 5
        details.append('今日无反弹动作-5')

    # 判断是否触发中轨反弹信号
    # 条件：评分>=40 且 %b在中轨附近(0.4~0.65) 且 中轨向上 且 有反弹动作
    mid_slope_up = len(mid_5) == 5 and mid_5[-1] > mid_5[0]
    near_mid = 0.4 <= pct_b_now <= 0.65 if pd.notna(pct_b_now) else False
    is_signal = (score >= 60 and near_mid and mid_slope_up
                 and has_bounce_action)

    return {
        'mid_bounce_score': max(score, 0),
        'mid_bounce_detail': ','.join(details) if details else '无信号',
        'mid_bounce_signal': is_signal,
        'mid_val': round(mid_now, 2),
        'mid_pct_b': round(pct_b_now, 3) if pd.notna(pct_b_now) else None,
    }


def _calc_adx(data: pd.DataFrame, n: int = 14) -> float | None:
    """
    计算 ADX (Average Directional Index) 指标，返回最新值。

    ADX 用于衡量趋势的强度（不区分方向）：
    - ADX > 25: 强趋势，中轨回踩具有支撑意义
    - ADX 20~25: 中等趋势
    - ADX < 20: 弱趋势或横盘震荡，中轨不具备可靠支撑

    计算步骤:
    1. +DM / -DM: 方向运动指标
    2. TR (True Range): 真实波幅
    3. +DI / -DI: 方向指标 = 平滑后的 DM / ATR * 100
    4. DX = |+DI - -DI| / (+DI + -DI) * 100
    5. ADX = DX 的 EMA 平滑

    Args:
        data: 包含 high/low/close 的 DataFrame
        n: ADX 周期，默认14

    Returns:
        float | None: ADX 最新值，数据不足时返回 None
    """
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

    MFM 用于衡量每日资金流入/流出的倾向：
    公式: MFM = [(Close - Low) - (High - Close)] / (High - Low)
    取值范围: [-1, 1]
    - MFM > 0: 收盘价偏向当日高点，代表买方力量占优，资金净流入
    - MFM < 0: 收盘价偏向当日低点，代表卖方力量占优，资金净流出

    在中轨反弹场景中，回踩中轨时 MFM 维持在 0 轴上方，
    代表机构大单并未因调整而大规模流出，是趋势延续的积极信号。

    Args:
        data: 包含 high/low/close 的 DataFrame
        n: 计算均值的天数，默认5

    Returns:
        float | None: MFM 近 n 日均值，数据不足时返回 None
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

def analyze_finance_growth(stock_info: StockInfo) -> dict:
    """
    分析近三个季度的财报数据，判断营收和利润是否持续增长及增长幅度。

    Returns:
        dict 包含:
        - finance_growth: bool, 是否持续增长
        - finance_summary: str, 增长概要描述
        - finance_details: list[dict], 每个季度的关键指标
    """
    records = get_financial_data_from_db(stock_info, limit=3)
    if not records or len(records) < 3:
        return {
            'finance_growth': None,
            'finance_summary': '财报数据不足(不足3个季度)',
            'finance_details': [],
        }

    # records 按报告期倒序，反转为时间正序便于判断趋势
    quarters = list(reversed(records))

    details = []
    for q in quarters:
        details.append({
            'period': q.get('报告期', ''),
            'revenue': q.get('营业总收入(元)', None),
            'parent_net_profit': q.get('归母净利润(元)', None),
            'deducted_net_profit': q.get('扣非净利润(元)', None),
            'revenue_yoy': q.get('营业总收入同比增长(%)', None),
            'profit_yoy': q.get('归属净利润同比增长(%)', None),
            'deducted_yoy': q.get('扣非净利润同比增长(%)', None),
        })

    # 判断持续增长：营收和归母净利润连续3个季度同比正增长
    revenue_yoys = [d['revenue_yoy'] for d in details]
    profit_yoys = [d['profit_yoy'] for d in details]
    deducted_yoys = [d['deducted_yoy'] for d in details]

    def _all_positive(vals):
        return all(v is not None and float(v) > 0 for v in vals)

    def _is_accelerating(vals):
        """判断增速是否加速（后一个季度增速 > 前一个季度）"""
        nums = [float(v) for v in vals if v is not None]
        if len(nums) < 2:
            return False
        return all(nums[i + 1] > nums[i] for i in range(len(nums) - 1))

    revenue_growing = _all_positive(revenue_yoys)
    profit_growing = _all_positive(profit_yoys)
    deducted_growing = _all_positive(deducted_yoys)
    all_growing = revenue_growing and profit_growing

    revenue_accel = _is_accelerating(revenue_yoys)
    profit_accel = _is_accelerating(profit_yoys)

    # 构建概要
    parts = []
    if all_growing:
        parts.append('营收+利润连续3季正增长')
        if revenue_accel:
            parts.append('营收增速加速')
        if profit_accel:
            parts.append('利润增速加速')
    else:
        if revenue_growing:
            parts.append('营收连续正增长')
        else:
            parts.append('营收增长不连续')
        if profit_growing:
            parts.append('利润连续正增长')
        else:
            parts.append('利润增长不连续')

    if deducted_growing:
        parts.append('扣非净利润连续正增长')

    # 附加最新季度增速
    latest = details[-1]
    if latest['revenue_yoy'] is not None:
        parts.append(f"最新营收同比{latest['revenue_yoy']}%")
    if latest['profit_yoy'] is not None:
        parts.append(f"最新利润同比{latest['profit_yoy']}%")

    return {
        'finance_growth': all_growing,
        'finance_summary': '；'.join(parts),
        'finance_details': details,
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

        # 对中轨反弹或下轨反弹信号的股票，分析近三个季度财报增长情况
        if result.get('mid_bounce_signal') or result.get('boll_signal'):
            finance_info = analyze_finance_growth(stock_info)
            result.update(finance_info)

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
    # 第二轮筛选：所有股票进入布林反弹分析
    mid_bounce_stocks = [r for r in results if r.get('mid_bounce_signal')]
    bounce_stocks = [r for r in results if r.get('boll_signal')]

    lines = [
        f"# 技术面打分与布林线反弹信号筛选结果",
        f"",
        f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"分析股票数: {len(results)}，"
        f"中轨反弹信号: {len(mid_bounce_stocks)}，下轨反弹信号: {len(bounce_stocks)}",
        f"",
        f"---",
        f"",
        f"## 📐 算法规则说明",
        f"",
        f"### 第一轮: 综合技术面打分 (满分100)",
        f"",
        f"由 MACD(35) + KDJ(30) + 成交量(20) + 趋势(15) 四个维度加总，各维度最低0分。",
        f"",
        f"#### MACD维度 (满分35)",
        f"- 市场状态: DIF>0且DEA>0 强多头+15, DIF>0 弱多头+8, 否则空头+0",
        f"- 金叉/死叉(近5日): 零轴上金叉+10, 普通金叉+5, 死叉-5",
        f"- MACD柱(近3日): 红柱连续放大+10, 绿柱连续放大-5 (含最小变化阈值=价格×0.01%)",
        f"",
        f"#### KDJ维度 (满分30)",
        f"- KDJ位置: K<20超卖区+10, K>80超买区-5, 20≤K≤80中性区+5",
        f"- 金叉/死叉(近3日): 超卖区金叉(K<30)+15, 普通金叉+8, 超买区死叉(K>70)-10",
        f"- J值方向: J上行+5, J下行-3",
        f"",
        f"#### 成交量维度 (满分20)",
        f"- 量比(今量/5日均量): >1.5放量+5, ≥0.8正常+3, <0.8缩量+0",
        f"- 量价配合: 价涨量增+10, 价涨量缩(量<均量70%)-5, 放量下跌(量>均量150%)-3",
        f"- 连续放量: 近3日量递增且均上涨+5",
        f"",
        f"#### 趋势维度 (满分15)",
        f"- 均线排列: MA5>MA20>MA60多头排列+10, MA5<MA20<MA60空头排列-5",
        f"- 价格位置: 站上MA5(含0.1%容差)+5, 跌破MA20(含0.1%容差)-5",
        f"",
        f"### 第二轮: 布林线反弹评分 (全量)",
        f"",
        f"#### 中轨反弹 (满分100) — 趋势延续型二级买点",
        f"",
        f"前置校验(不满足直接0分):",
        f"- 近15日至少3天收盘在中轨上方(回踩模式验证)",
        f"- 近5日有收盘价在中轨附近或下方(close≤mid×1.005)",
        f"- 今日收盘站回中轨上方",
        f"- 近5日未连续3天以上跌破中轨",
        f"",
        f"评分维度:",
        f"- 趋势背景(25): 中轨斜率(>0.3%+10/>0+5/走平下行-5) + 未连续跌破中轨+10 + %b精准回踩(0.45~0.55)+5",
        f"- 回调缩量(25): 量能萎缩比(<50%+12/<60%+8/<80%+4) + 波幅压缩(<50%+8/<70%+4) + 无放量大阴+5",
        f"- 反弹放量(30): 量比(>1.5+10/>1.2+5) + 放量倍率(>前日1.5倍+10) + 阳包阴+站上MA5+10/阳线站上MA5+5",
        f"- 动能资金(20): ADX(>25+10/>20+5) + MFM(>0+10/≤0-3)",
        f"- 假反弹警示: 布林Squeeze(bandwidth<0.04)-5, 无反弹动作-5",
        f"",
        f"信号条件: 评分≥40 且 0.4≤%b≤0.65 且 中轨上行 且 有反弹动作",
        f"",
        f"#### 下轨反弹 (满分100) — 超跌反弹",
        f"",
        f"评分维度:",
        f"- 价格%b(30): 近5日%b曾<0(触及下轨)+10 + %b从<0收回≥0+10 + 近20日%b底背离+10",
        f"- 量能配合(25): 近5日缩量(vol<均量60%)+8 + 反弹放量(>1.5倍+10/>1.2倍+5) + OBV量价背离+7",
        f"- K线形态(25): 长下影线(下影/实体>2)+10 + 阳包阴+10 + 锤子线/十字星+5",
        f"- 站稳判定(20): 连续2日站稳下轨+10 + 收盘从下轨下方回到上方+10",
        f"",
        f"信号条件: 评分≥40 且 近期触及下轨(%b曾<0)",
        f"",
        f"---",
        f"",
    ]

    # ─── 中轨反弹信号（排在前面） ───
    lines.append(f"## 📈 中轨反弹信号（趋势延续 — 二级买点）\n")
    lines.append(f"筛选条件: 中轨反弹评分≥40 且 %b在0.4~0.65 且 中轨上行 且 有反弹动作\n")
    if mid_bounce_stocks:
        mid_sorted = sorted(mid_bounce_stocks, key=lambda x: -x['mid_bounce_score'])
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
            lines.append(f"- 最新收盘价: {r['close']} | 120日高价: {r.get('high120', '-')} | 跌幅: {r.get('high120_drop_pct', '-')}%")
            lines.append(f"- 中轨反弹({r['mid_bounce_score']}): {r['mid_bounce_detail']}")
            lines.append(f"- %b={r.get('mid_pct_b', '-')} 中轨={r.get('mid_val', '-')}")
            _append_finance_lines(lines, r)
            lines.append("")
    else:
        lines.append("暂无中轨反弹信号\n")

    # ─── 下轨反弹信号 ───
    lines.append(f"## 🔻 下轨反弹信号（超跌反弹）\n")
    lines.append(f"筛选条件: 布林下轨反弹评分≥40 且 近期触及下轨\n")
    if bounce_stocks:
        bounce_sorted = sorted(bounce_stocks, key=lambda x: -x['boll_score'])
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
            lines.append(f"- 最新收盘价: {r['close']} | 120日高价: {r.get('high120', '-')} | 跌幅: {r.get('high120_drop_pct', '-')}%")
            lines.append(f"- 下轨反弹({r['boll_score']}): {r['boll_detail']}")
            lines.append(f"- %b={r.get('pct_b', '-')} 下轨={r.get('lower', '-')} 中轨={r.get('mid', '-')} 上轨={r.get('upper', '-')}")
            _append_finance_lines(lines, r)
            lines.append("")
    else:
        lines.append("暂无下轨反弹信号\n")

    # ─── 整体技术面打分结果 ───
    lines.append(f"## 📊 整体技术面打分结果\n")
    sorted_all = sorted(results, key=lambda x: -x['total'])
    lines.append(f"| 排名 | 股票名称 | 代码 | 总分 | MACD | KDJ | 量能 | 趋势 | 收盘价 | 120日高 | 跌幅 | 日期 |")
    lines.append(f"|------|----------|------|------|------|-----|------|------|--------|---------|------|------|")
    for i, r in enumerate(sorted_all, 1):
        lines.append(
            f"| {i} | {r['name']} | {r['code']} | {r['total']} | "
            f"{r['macd_score']} | {r['kdj_score']} | {r['vol_score']} | {r['trend_score']} | "
            f"{r['close']} | {r.get('high120', '-')} | {r.get('high120_drop_pct', '-')}% | {r['date']} |"
        )
    lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines), encoding='utf-8')
    print(f"\n结果已写入: {path}")

def _format_amount(val) -> str:
    """将金额格式化为亿/万单位"""
    if val is None:
        return '-'
    try:
        v = float(val)
    except (ValueError, TypeError):
        return str(val)
    if abs(v) >= 1e8:
        return f"{v / 1e8:.2f}亿"
    if abs(v) >= 1e4:
        return f"{v / 1e4:.2f}万"
    return f"{v:.2f}"


def _append_finance_lines(lines: list[str], r: dict):
    """向输出行列表追加财报增长分析内容"""
    summary = r.get('finance_summary')
    if not summary:
        return
    growth = r.get('finance_growth')
    icon = '📈' if growth else ('⚠️' if growth is None else '📉')
    lines.append(f"- {icon} 财报分析: {summary}")
    details = r.get('finance_details', [])
    if details:
        lines.append(f"  - 近三季度数据:")
        for d in details:
            rev = _format_amount(d.get('revenue'))
            profit = _format_amount(d.get('parent_net_profit'))
            rev_yoy = d.get('revenue_yoy')
            profit_yoy = d.get('profit_yoy')
            rev_yoy_str = f"{rev_yoy}%" if rev_yoy is not None else '-'
            profit_yoy_str = f"{profit_yoy}%" if profit_yoy is not None else '-'
            lines.append(
                f"    - {d['period']}: 营收{rev}(同比{rev_yoy_str}) | "
                f"归母净利润{profit}(同比{profit_yoy_str})"
            )



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
    mid_bounce_signals = [r for r in results if r.get('mid_bounce_signal')]
    bounce_signals = [r for r in results if r.get('boll_signal')]
    print(f"\n{'='*60}")
    print(f"分析完成: 共 {len(results)} 只有效股票")
    if mid_bounce_signals:
        print(f"📈 中轨反弹信号: {len(mid_bounce_signals)} 只")
        for r in sorted(mid_bounce_signals, key=lambda x: -x['mid_bounce_score']):
            print(f"    {r['name']:<8} {r['code']:<12} 综合:{r['total']:>3} "
                  f"中轨反弹:{r['mid_bounce_score']:>3} %b={r.get('mid_pct_b', '-')}")
    else:
        print(f"📈 中轨反弹信号: 暂无")
    if bounce_signals:
        print(f"🔻 下轨反弹信号: {len(bounce_signals)} 只")
        for r in sorted(bounce_signals, key=lambda x: -x['boll_score']):
            print(f"    {r['name']:<8} {r['code']:<12} 综合:{r['total']:>3} "
                  f"反弹:{r['boll_score']:>3} %b={r.get('pct_b', '-')}")
    else:
        print(f"🔻 下轨反弹信号: 暂无")
    print(f"{'='*60}")


if __name__ == '__main__':
    asyncio.run(main())
