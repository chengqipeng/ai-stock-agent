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
        latest = df.iloc[-1]
        result.update({'name': name, 'code': code, 'close': round(latest['close'], 2),
                       'date': df.index[-1].strftime('%Y-%m-%d')})
        tag = '✅' if result['total'] >= 50 else '  '
        print(f"[{idx}/{total}] {tag} {name:<8} {code:<12} 总分:{result['total']:>3} "
              f"MACD:{result['macd_score']:>2} KDJ:{result['kdj_score']:>2} "
              f"量能:{result['vol_score']:>2} 趋势:{result['trend_score']:>2}")
        return result
    except Exception as e:
        print(f"[{idx}/{total}] {name}({code}) - 错误: {e}")
        return None


# ─── 输出结果 ───
def write_result(results: list[dict], path: Path):
    qualified = sorted([r for r in results if r['total'] >= 50], key=lambda x: -x['total'])
    lines = [
        f"# 技术面打分结果（≥50分）",
        f"",
        f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"分析股票数: {len(results)}，达标股票数: {len(qualified)}",
        f"",
        f"评分维度: MACD(35分) + KDJ(30分) + 成交量(20分) + 趋势(15分) = 满分100分",
        f"",
        f"| 排名 | 股票名称 | 代码 | 总分 | MACD | KDJ | 量能 | 趋势 | 收盘价 | 日期 |",
        f"|------|----------|------|------|------|-----|------|------|--------|------|",
    ]
    for i, r in enumerate(qualified, 1):
        lines.append(
            f"| {i} | {r['name']} | {r['code']} | {r['total']} | {r['macd_score']} | "
            f"{r['kdj_score']} | {r['vol_score']} | {r['trend_score']} | {r['close']} | {r['date']} |"
        )

    lines.append(f"\n## 评分细则\n")
    for r in qualified:
        lines.append(f"### {r['name']}({r['code']}) - 总分 {r['total']}")
        lines.append(f"- MACD({r['macd_score']}): {r['macd_detail']}")
        lines.append(f"- KDJ({r['kdj_score']}): {r['kdj_detail']}")
        lines.append(f"- 量能({r['vol_score']}): {r['vol_detail']}")
        lines.append(f"- 趋势({r['trend_score']}): {r['trend_detail']}")
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
    print(f"\n{'='*60}")
    print(f"分析完成: 共 {len(results)} 只有效股票，{len(qualified)} 只达到50分以上")
    print(f"{'='*60}")


if __name__ == '__main__':
    asyncio.run(main())
