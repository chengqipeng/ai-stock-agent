"""
批量技术面打分全维度验证：
从 tmp_data/verify_sample_100.json 读取抽样股票的原始指标数据，
独立重新推导各维度得分，与原始打分结果对比，找出差异和潜在bug。

验证维度（共6个）：
  第一轮: MACD(35分)、KDJ(30分)、成交量(20分)、趋势(15分)
  第二轮: 布林下轨反弹(100分)、布林中轨反弹(100分)
"""
import json
from pathlib import Path

SAMPLE_PATH = Path(__file__).parent.parent / "tmp_data/verify_sample_100.json"
REPORT_PATH = Path(__file__).parent.parent / "data_results/stock_all_scan_score_result/verify_report.md"


# ═══════════════════════════════════════════════════════════════
# 第一轮维度验证
# ═══════════════════════════════════════════════════════════════

def verify_macd(s: dict) -> tuple[int, str, list[str]]:
    """
    独立验证 MACD 得分 (满分35)
    - 市场状态: DIF>0且DEA>0 强多头+15, DIF>0 弱多头+8, 否则空头+0
    - 金叉/死叉: 近5日零轴上金叉+10, 普通金叉+5, 死叉-5
    - MACD柱: 近3天红柱连续放大+10, 绿柱连续放大-5 (含最小变化阈值)
    """
    score = 0
    details = []
    issues = []

    dif = s["DIF"]
    dea = s["DEA"]
    hist_3 = s["hist_3"]
    gc_in_5 = s["gc_in_5"]
    dc_in_5 = s["dc_in_5"]
    gc_dif_dea = s["gc_dif_dea"]
    close = s["close"]

    # 1) 市场状态
    if dif > 0 and dea > 0:
        score += 15
        details.append("强多头+15")
    elif dif > 0:
        score += 8
        details.append("弱多头+8")
    else:
        details.append("空头+0")

    # 2) 金叉/死叉信号（最近5天）
    if gc_in_5:
        if gc_dif_dea and gc_dif_dea[0] > 0 and gc_dif_dea[1] > 0:
            score += 10
            details.append("零轴上金叉+10")
        else:
            score += 5
            details.append("金叉+5")
    elif dc_in_5:
        score -= 5
        details.append("死叉-5")

    # 3) MACD柱状图趋势（最近3天）— 使用最小变化阈值
    if len(hist_3) == 3:
        v = hist_3
        min_delta = close * 0.0001 if close > 0 else 0.001
        if v[2] > v[1] + min_delta and v[1] > v[0] + min_delta and v[2] > 0:
            score += 10
            details.append("红柱放大+10")
        elif v[2] < v[1] - min_delta and v[1] < v[0] - min_delta and v[2] < 0:
            score -= 5
            details.append("绿柱放大-5")

    final = max(score, 0)
    detail_str = ",".join(details)

    if final != s["macd_score"]:
        issues.append(f"MACD得分不一致: 验证={final} vs 原始={s['macd_score']} "
                      f"(验证={detail_str}, 原始={s['macd_detail']})")
    return final, detail_str, issues


def verify_kdj(s: dict) -> tuple[int, str, list[str]]:
    """
    独立验证 KDJ 得分 (满分30)
    - KDJ位置: K<20超卖+10, K>80超买-5, 中性区+5
    - 金叉/死叉: 近3天超卖金叉+15, 普通金叉+8, 超买死叉-10
    - J值方向: J上行+5, J下行-3
    """
    score = 0
    details = []
    issues = []

    k = s["K"]
    j = s["J"]
    prev_j = s["prev_J"]

    # 1) KDJ位置
    if k < 20:
        score += 10
        details.append("超卖区+10")
    elif k > 80:
        score -= 5
        details.append("超买区-5")
    else:
        score += 5
        details.append("中性区+5")

    # 2) 金叉/死叉（最近3天）
    kdj_3days = s.get("kdj_3days")
    if kdj_3days and len(kdj_3days) >= 2:
        for i in range(1, len(kdj_3days)):
            curr_k, curr_d = kdj_3days[i]["K"], kdj_3days[i]["D"]
            prev_k, prev_d = kdj_3days[i - 1]["K"], kdj_3days[i - 1]["D"]
            if prev_k <= prev_d and curr_k > curr_d:
                if curr_k < 30:
                    score += 15
                    details.append("超卖金叉+15")
                else:
                    score += 8
                    details.append("金叉+8")
                break
            elif prev_k >= prev_d and curr_k < curr_d:
                if curr_k > 70:
                    score -= 10
                    details.append("超买死叉-10")
                break

    # 3) J值方向
    if j > prev_j:
        score += 5
        details.append("J上行+5")
    else:
        score -= 3
        details.append("J下行-3")

    final = max(score, 0)
    detail_str = ",".join(details)

    if final != s["kdj_score"]:
        issues.append(f"KDJ得分不一致: 验证={final} vs 原始={s['kdj_score']} "
                      f"(验证={detail_str}, 原始={s['kdj_detail']})")
    return final, detail_str, issues


def verify_volume(s: dict) -> tuple[int, str, list[str]]:
    """
    独立验证 成交量 得分 (满分20)
    - 量比: >1.5放量+5, >=0.8正常+3, <0.8缩量+0
    - 量价配合: 价涨量增+10, 价涨量缩-5, 放量下跌-3
    - 连续放量: 3天量递增且价格均上涨+5
    """
    score = 0
    details = []
    issues = []

    vol = s["vol"]
    vol_ma5 = s["vol_ma5"]
    pct = s["pct"]
    tail3_vols = s["tail3_vols"]
    tail3_pcts = s["tail3_pcts"]

    # 1) 量比
    vol_ratio = vol / vol_ma5 if vol_ma5 > 0 else 1
    if vol_ratio > 1.5:
        score += 5
        details.append(f"放量({vol_ratio:.1f})+5")
    elif vol_ratio >= 0.8:
        score += 3
        details.append(f"正常量({vol_ratio:.1f})+3")
    else:
        details.append(f"缩量({vol_ratio:.1f})+0")

    # 2) 量价配合
    if pct > 0 and vol > vol_ma5:
        score += 10
        details.append("价涨量增+10")
    elif pct > 0 and vol < vol_ma5 * 0.7:
        score -= 5
        details.append("价涨量缩-5")
    elif pct < 0 and vol > vol_ma5 * 1.5:
        score -= 3
        details.append("放量下跌-3")

    # 3) 连续放量上涨
    if len(tail3_vols) == 3 and len(tail3_pcts) == 3:
        v = tail3_vols
        p = tail3_pcts
        if v[2] > v[1] > v[0] and all(x > 0 for x in p):
            score += 5
            details.append("连续放量涨+5")

    final = max(score, 0)
    detail_str = ",".join(details)

    if final != s["vol_score"]:
        issues.append(f"量能得分不一致: 验证={final} vs 原始={s['vol_score']} "
                      f"(验证={detail_str}, 原始={s['vol_detail']})")
    return final, detail_str, issues


def verify_trend(s: dict) -> tuple[int, str, list[str]]:
    """
    独立验证 趋势 得分 (满分15)
    - MA5>MA20>MA60 多头排列+10, MA5<MA20<MA60 空头排列-5
    - 价格>=MA5(含容差) 站上MA5+5, 价格<MA20(含容差) 跌破MA20-5
    """
    score = 0
    details = []
    issues = []

    ma5 = s["MA5"]
    ma20 = s["MA20"]
    ma60 = s["MA60"]
    close = s["close"]
    eps = close * 0.001 if close > 0 else 0.01

    if ma5 > ma20 > ma60:
        score += 10
        details.append("多头排列+10")
    elif ma5 < ma20 < ma60:
        score -= 5
        details.append("空头排列-5")

    if close >= ma5 - eps:
        score += 5
        details.append("站上MA5+5")
    elif close < ma20 - eps:
        score -= 5
        details.append("跌破MA20-5")

    final = max(score, 0)
    detail_str = ",".join(details)

    if final != s["trend_score"]:
        issues.append(f"趋势得分不一致: 验证={final} vs 原始={s['trend_score']} "
                      f"(验证={detail_str}, 原始={s['trend_detail']})")
    return final, detail_str, issues


# ═══════════════════════════════════════════════════════════════
# 第二轮维度验证 — 布林线下轨反弹
# ═══════════════════════════════════════════════════════════════

def verify_boll_lower_bounce(s: dict) -> tuple[int, str, list[str]]:
    """
    独立验证 布林线下轨反弹 得分 (满分100)

    维度1 价格位置 %b (满分30):
      1a. 近5日%b曾<0 (触及下轨) +10
      1b. %b从<0收回>=0 (收回轨内) +10
      1c. 近20日%b底背离 (价格新低但%b更高) +10

    维度2 量能配合 (满分25):
      2a. 近5日出现缩量(vol < vol_ma5*0.6) +8
      2b. 反弹放量阳线: vol/vol_ma5 >1.5 +10, >1.2 +5
      2c. OBV量价背离(%b<0.3且OBV上行但价格未涨) +7

    维度3 K线形态 (满分25):
      3a. 长下影线(下影/实体>2) +10
      3b. 阳包阴 +10
      3c. 锤子线/十字星(实体<振幅15%且下影>实体2倍) +5

    维度4 站稳判定 (满分20):
      4a. 连续2日收盘站稳下轨之上 +10
      4b. 收盘从下轨下方回到上方 +10

    信号: score>=40 且 touched_lower
    """
    score = 0
    details = []
    issues = []

    pct_b = s.get("pct_b")
    recent5_pctb = s.get("recent5_pctb", [])
    close = s["close"]
    open_ = s["open"]
    high = s["high"]
    low = s["low"]
    pct = s["pct"]
    vol = s["vol"]
    vol_ma5 = s["vol_ma5"]
    prev_open = s["prev_open"]
    prev_close = s["prev_close"]

    # ─── 维度1: 价格位置 %b ───

    # 1a. 超卖确认
    touched_lower = any(v is not None and v < 0 for v in recent5_pctb)
    if touched_lower:
        score += 10
        details.append("%b曾<0(触及下轨)+10")

    # 1b. 反弹触发: %b从<0收回>=0
    if pct_b is not None and pct_b >= 0:
        was_below = False
        recovered = False
        for v in recent5_pctb:
            if v is not None and v < 0:
                was_below = True
            elif was_below and v is not None and v >= 0:
                recovered = True
                break
        if recovered:
            score += 10
            details.append("%b收回轨内+10")

    # 1c. 底背离
    low_points = s.get("low_points_data", [])
    if len(low_points) >= 2:
        first = low_points[0]
        last = low_points[-1]
        if last["close"] <= first["close"] and last["pct_b"] > first["pct_b"]:
            score += 10
            details.append("%b底背离+10")

    # ─── 维度2: 量能配合 ───

    # 2a. 抛压枯竭
    recent5_vols = s.get("recent5_vols", [])
    recent5_vol_ma5 = s.get("recent5_vol_ma5", [])
    shrink_found = False
    for v, ma in zip(recent5_vols, recent5_vol_ma5):
        if ma is not None and ma > 0 and v < ma * 0.6:
            shrink_found = True
            break
    if shrink_found:
        score += 8
        details.append("抛压枯竭(缩量)+8")

    # 2b. 确认反弹放量
    if pct > 0 and vol_ma5 > 0:
        vr = vol / vol_ma5
        if vr > 1.5:
            score += 10
            details.append(f"反弹放量({vr:.1f})+10")
        elif vr > 1.2:
            score += 5
            details.append(f"反弹温和放量({vr:.1f})+5")

    # 2c. OBV量价背离
    if pct_b is not None and pct_b < 0.3:
        obv_tail5 = s.get("obv_tail5", [])
        price_tail5 = s.get("price_tail5", [])
        if len(obv_tail5) >= 3 and obv_tail5[-1] > obv_tail5[-3]:
            if len(price_tail5) >= 3 and price_tail5[-1] <= price_tail5[-3]:
                score += 7
                details.append("OBV量价背离+7")

    # ─── 维度3: K线形态 ───
    body = abs(close - open_)
    lower_shadow = min(open_, close) - low

    # 3a. 长下影线
    if body > 0 and lower_shadow / body > 2:
        score += 10
        details.append(f"长下影线({lower_shadow / body:.1f}倍)+10")

    # 3b. 阳包阴
    if (prev_close < prev_open
            and close > open_
            and open_ <= prev_close
            and close >= prev_open):
        score += 10
        details.append("阳包阴+10")

    # 3c. 锤子线/十字星
    hl_range = high - low
    if body > 0 and hl_range > 0 and body < hl_range * 0.15 and lower_shadow > body * 2:
        score += 5
        details.append("锤子线/十字星+5")

    # ─── 维度4: 站稳判定 ───

    # 4a. 连续2日站稳下轨
    tail3_close = s.get("tail3_close_boll", [])
    tail3_lower = s.get("tail3_lower_boll", [])
    if len(tail3_close) >= 2 and len(tail3_lower) >= 2:
        last2_c = tail3_close[-2:]
        last2_l = tail3_lower[-2:]
        if all(c > lb for c, lb in zip(last2_c, last2_l)):
            score += 10
            details.append("连续2日站稳下轨+10")

    # 4b. 收盘从下轨下方回到上方
    tail5_close = s.get("tail5_close_boll", [])
    tail5_lower = s.get("tail5_lower_boll", [])
    was_below_lower = False
    back_above = False
    for cv, lv in zip(tail5_close, tail5_lower):
        if cv < lv:
            was_below_lower = True
        elif was_below_lower and cv > lv:
            back_above = True
            break
    if back_above:
        score += 10
        details.append("收盘回到下轨上方+10")

    # 信号判定
    is_signal = score >= 60 and touched_lower

    final = score  # 下轨反弹不做 max(score,0)，原始代码直接返回 score
    detail_str = ",".join(details) if details else "无信号"

    orig_score = s.get("boll_score", 0)
    if final != orig_score:
        issues.append(f"下轨反弹得分不一致: 验证={final} vs 原始={orig_score} "
                      f"(验证={detail_str}, 原始={s.get('boll_detail', '')})")
    orig_signal = s.get("boll_signal", False)
    if is_signal != orig_signal:
        issues.append(f"下轨反弹信号不一致: 验证={is_signal} vs 原始={orig_signal}")

    return final, detail_str, issues


# ═══════════════════════════════════════════════════════════════
# 第二轮维度验证 — 布林线中轨反弹
# ═══════════════════════════════════════════════════════════════

def verify_boll_mid_bounce(s: dict) -> tuple[int, str, list[str]]:
    """
    独立验证 布林线中轨反弹 得分 (满分100)

    前置校验（硬性排除条件）:
      - 近15日至少3天在中轨上方（回踩模式验证），否则直接0分
      - 连续3天以上跌破中轨 = 破位，直接0分
      - 今日无反弹动作（收阴且跌幅<=0）扣5分

    维度1 趋势背景 (满分25):
      1a. 中轨斜率: 近5日MA20上行 >0.3%+10, >0+5, 走平/下行-5
      1b. 未连续2日跌破中轨 +10, 连续跌破-5
      1c. %b精准回踩中轨(0.45~0.55) +5

    维度2 回调缩量 Testing Supply (满分25):
      2a. 回调量能萎缩比: <50%+12, <60%+8, <80%+4
      2b. 波幅压缩: 回调实体/上涨实体 <50%+8, <70%+4
      2c. 回调无放量大阴(近3日无跌>3%且放量>1.5倍) +5/-5

    维度3 反弹放量 Demand Entry (满分30):
      3a. 量比(需有反弹动作): >1.5+10, >1.2+5
      3b. 放量倍率(需有反弹动作): 阳线量>前日阴线1.5倍 +10
      3c. 阳包阴+站上MA5 +10, 仅阳线站上MA5 +5

    维度4 动能与资金 (满分20):
      4a. ADX(14): >25+10, >20+5, <=20+0
      4b. MFM: >0+10, <=0-3

    假反弹警示:
      布林Squeeze(bandwidth<0.04) -5

    信号: score>=40 且 0.4<=%b<=0.65 且 中轨向上 且 有反弹动作
    """
    score = 0
    details = []
    issues = []

    pct_b = s.get("pct_b")
    close = s["close"]
    open_ = s["open"]
    pct = s["pct"]
    vol = s["vol"]
    vol_ma5 = s["vol_ma5"]
    prev_open = s["prev_open"]
    prev_close = s["prev_close"]
    prev_pct = s["prev_pct"]
    prev_vol = s["prev_vol"]
    ma5 = s["MA5"]

    # ─── 前置校验: 回踩模式验证 ───
    days_above_mid = s.get("days_above_mid")
    has_bounce_action = s.get("has_bounce_action")
    had_touch_mid = s.get("had_touch_mid")
    close_above_mid = s.get("close_above_mid")

    if days_above_mid is not None and days_above_mid < 3:
        details.append(f"近15日仅{days_above_mid}日在中轨上方(需≥3),非回踩模式")
        final = 0
        detail_str = ",".join(details)
        orig_score = s.get("mid_bounce_score", 0)
        if final != orig_score:
            issues.append(f"中轨反弹得分不一致: 验证={final} vs 原始={orig_score} "
                          f"(验证={detail_str}, 原始={s.get('mid_bounce_detail', '')})")
        orig_signal = s.get("mid_bounce_signal", False)
        if False != orig_signal:
            issues.append(f"中轨反弹信号不一致: 验证=False vs 原始={orig_signal}")
        return final, detail_str, issues

    if had_touch_mid is not None and not had_touch_mid:
        details.append("近5日未回踩中轨,非中轨反弹模式")
        final = 0
        detail_str = ",".join(details)
        orig_score = s.get("mid_bounce_score", 0)
        if final != orig_score:
            issues.append(f"中轨反弹得分不一致: 验证={final} vs 原始={orig_score} "
                          f"(验证={detail_str}, 原始={s.get('mid_bounce_detail', '')})")
        orig_signal = s.get("mid_bounce_signal", False)
        if False != orig_signal:
            issues.append(f"中轨反弹信号不一致: 验证=False vs 原始={orig_signal}")
        return final, detail_str, issues

    if close_above_mid is not None and not close_above_mid:
        boll_mid_val = s.get("boll_mid", 0)
        details.append(f"收盘({close:.2f})未站回中轨({boll_mid_val:.2f})上方,反弹未确认")
        final = 0
        detail_str = ",".join(details)
        orig_score = s.get("mid_bounce_score", 0)
        if final != orig_score:
            issues.append(f"中轨反弹得分不一致: 验证={final} vs 原始={orig_score} "
                          f"(验证={detail_str}, 原始={s.get('mid_bounce_detail', '')})")
        orig_signal = s.get("mid_bounce_signal", False)
        if False != orig_signal:
            issues.append(f"中轨反弹信号不一致: 验证=False vs 原始={orig_signal}")
        return final, detail_str, issues

    # 连续跌破中轨天数检测
    tail5_below = s.get("tail5_close_below_mid", [])
    consecutive_below = 0
    for below in tail5_below:
        if below:
            consecutive_below += 1
        else:
            consecutive_below = 0

    if consecutive_below >= 3:
        details.append(f"连续{consecutive_below}日跌破中轨,已破位")
        final = 0
        detail_str = ",".join(details)
        orig_score = s.get("mid_bounce_score", 0)
        if final != orig_score:
            issues.append(f"中轨反弹得分不一致: 验证={final} vs 原始={orig_score} "
                          f"(验证={detail_str}, 原始={s.get('mid_bounce_detail', '')})")
        orig_signal = s.get("mid_bounce_signal", False)
        if False != orig_signal:
            issues.append(f"中轨反弹信号不一致: 验证=False vs 原始={orig_signal}")
        return final, detail_str, issues

    # ─── 维度1: 趋势背景 ───

    # 1a. 中轨斜率
    mid_5 = s.get("mid_5_vals", [])
    mid_slope_up = False
    if len(mid_5) == 5 and mid_5[-1] > mid_5[0]:
        mid_slope_up = True
        slope_pct = (mid_5[-1] - mid_5[0]) / mid_5[0] * 100
        if slope_pct > 0.3:
            score += 10
            details.append(f"中轨上行({slope_pct:.2f}%)+10")
        elif slope_pct > 0:
            score += 5
            details.append(f"中轨微升({slope_pct:.2f}%)+5")
    else:
        score -= 5
        details.append("中轨走平/下行-5")

    # 1b. 未连续2日跌破中轨
    if consecutive_below < 2:
        score += 10
        details.append("未连续跌破中轨+10")
    else:
        score -= 5
        details.append(f"连续{consecutive_below}日跌破中轨-5")

    # 1c. %b精准回踩中轨
    if pct_b is not None and 0.45 <= pct_b <= 0.55:
        score += 5
        details.append(f"%b精准回踩中轨({pct_b:.2f})+5")

    # ─── 维度2: 回调缩量 ───
    peak_pos = s.get("peak_pos_r10", 0)
    r10_vols = s.get("r10_vols", [])
    r10_close = s.get("r10_close", [])
    r10_open = s.get("r10_open", [])

    if peak_pos < len(r10_vols) - 1 and len(r10_vols) == 10:
        up_vols = r10_vols[:peak_pos + 1]
        pb_vols = r10_vols[peak_pos + 1:]
        up_close = r10_close[:peak_pos + 1]
        up_open = r10_open[:peak_pos + 1]
        pb_close = r10_close[peak_pos + 1:]
        pb_open = r10_open[peak_pos + 1:]

        if len(up_vols) > 0 and len(pb_vols) > 0:
            up_avg_vol = sum(up_vols) / len(up_vols)
            pb_avg_vol = sum(pb_vols) / len(pb_vols)

            # 2a. 量能萎缩比
            if up_avg_vol > 0:
                ratio = pb_avg_vol / up_avg_vol
                if ratio < 0.5:
                    score += 12
                    details.append(f"回调深度缩量({ratio:.0%})+12")
                elif ratio < 0.6:
                    score += 8
                    details.append(f"回调缩量({ratio:.0%})+8")
                elif ratio < 0.8:
                    score += 4
                    details.append(f"回调温和缩量({ratio:.0%})+4")

            # 2b. 波幅压缩
            up_bodies = [abs(c - o) for c, o in zip(up_close, up_open)]
            pb_bodies = [abs(c - o) for c, o in zip(pb_close, pb_open)]
            up_body_avg = sum(up_bodies) / len(up_bodies) if up_bodies else 0
            pb_body_avg = sum(pb_bodies) / len(pb_bodies) if pb_bodies else 0
            if up_body_avg > 0:
                body_ratio = pb_body_avg / up_body_avg
                if body_ratio < 0.5:
                    score += 8
                    details.append(f"波幅压缩({body_ratio:.0%})+8")
                elif body_ratio < 0.7:
                    score += 4
                    details.append(f"波幅收窄({body_ratio:.0%})+4")

    # 2c. 回调无放量大阴
    tail3_pct_mid = s.get("tail3_pct_change_mid", [])
    tail3_vol_mid = s.get("tail3_vol_mid", [])
    tail3_vma5_mid = s.get("tail3_vol_ma5_mid", [])
    big_drop = False
    for p, v, ma in zip(tail3_pct_mid, tail3_vol_mid, tail3_vma5_mid):
        if ma is not None and ma > 0 and p < -3 and v > ma * 1.5:
            big_drop = True
            break
    if big_drop:
        score -= 5
        details.append("回调放量大阴-5")
    else:
        score += 5
        details.append("回调无放量大阴+5")

    # ─── 维度3: 反弹放量（需有反弹动作） ───

    # 3a. 量比（需有反弹动作才算反弹）
    if vol_ma5 > 0 and has_bounce_action:
        vr = vol / vol_ma5
        if vr > 1.5:
            score += 10
            details.append(f"反弹量比({vr:.1f})+10")
        elif vr > 1.2:
            score += 5
            details.append(f"反弹温和放量({vr:.1f})+5")

    # 3b. 放量倍率（需有反弹动作）
    if has_bounce_action and prev_pct < 0 and prev_vol > 0:
        amplify = vol / prev_vol
        if amplify > 1.5:
            score += 10
            details.append(f"放量倍率({amplify:.1f}x)+10")

    # 3c. 吞没形态 + 站上MA5
    if (prev_close < prev_open
            and close > open_
            and open_ <= prev_close and close >= prev_open
            and close > ma5):
        score += 10
        details.append("阳包阴+站上MA5+10")
    elif close > open_ and close > ma5 and pct > 0:
        score += 5
        details.append("阳线站上MA5+5")

    # ─── 维度4: 动能与资金 ───

    # 4a. ADX
    adx = s.get("adx_val")
    if adx is not None:
        if adx > 25:
            score += 10
            details.append(f"ADX强趋势({adx:.0f})+10")
        elif adx > 20:
            score += 5
            details.append(f"ADX中等趋势({adx:.0f})+5")
        else:
            details.append(f"ADX弱趋势({adx:.0f})+0")

    # 4b. MFM
    mfm = s.get("mfm_val")
    if mfm is not None:
        if mfm > 0:
            score += 10
            details.append(f"MFM正向({mfm:.2f})+10")
        else:
            score -= 3
            details.append(f"MFM负向({mfm:.2f})-3")

    # 假反弹警示: Squeeze
    bw = s.get("bandwidth")
    if bw is not None and bw < 0.04:
        score -= 5
        details.append(f"布林Squeeze({bw:.3f})-5")

    # 无反弹动作扣分
    if not has_bounce_action:
        score -= 5
        details.append("今日无反弹动作-5")

    # 信号判定: score>=40 且 %b在0.4~0.65 且 中轨向上 且 有反弹动作
    near_mid = pct_b is not None and 0.4 <= pct_b <= 0.65
    is_signal = score >= 60 and near_mid and mid_slope_up and has_bounce_action

    final = max(score, 0)
    detail_str = ",".join(details) if details else "无信号"

    orig_score = s.get("mid_bounce_score", 0)
    if final != orig_score:
        issues.append(f"中轨反弹得分不一致: 验证={final} vs 原始={orig_score} "
                      f"(验证={detail_str}, 原始={s.get('mid_bounce_detail', '')})")
    orig_signal = s.get("mid_bounce_signal", False)
    if is_signal != orig_signal:
        issues.append(f"中轨反弹信号不一致: 验证={is_signal} vs 原始={orig_signal}")

    return final, detail_str, issues


# ═══════════════════════════════════════════════════════════════
# 主验证流程
# ═══════════════════════════════════════════════════════════════

ALL_DIMENSIONS = ["MACD", "KDJ", "量能", "趋势", "下轨反弹", "中轨反弹"]


def main():
    data = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    print(f"✅ 加载 {len(data)} 只抽样股票\n")

    total_stocks = len(data)
    match_count = 0
    mismatch_stocks = []
    dimension_mismatches = {d: 0 for d in ALL_DIMENSIONS}

    for s in data:
        all_issues = []

        macd_v, macd_d, macd_issues = verify_macd(s)
        kdj_v, kdj_d, kdj_issues = verify_kdj(s)
        vol_v, vol_d, vol_issues = verify_volume(s)
        trend_v, trend_d, trend_issues = verify_trend(s)
        boll_v, boll_d, boll_issues = verify_boll_lower_bounce(s)
        mid_v, mid_d, mid_issues = verify_boll_mid_bounce(s)

        all_issues.extend(macd_issues)
        all_issues.extend(kdj_issues)
        all_issues.extend(vol_issues)
        all_issues.extend(trend_issues)
        all_issues.extend(boll_issues)
        all_issues.extend(mid_issues)

        verified_total = macd_v + kdj_v + vol_v + trend_v

        if macd_issues:
            dimension_mismatches["MACD"] += 1
        if kdj_issues:
            dimension_mismatches["KDJ"] += 1
        if vol_issues:
            dimension_mismatches["量能"] += 1
        if trend_issues:
            dimension_mismatches["趋势"] += 1
        if boll_issues:
            dimension_mismatches["下轨反弹"] += 1
        if mid_issues:
            dimension_mismatches["中轨反弹"] += 1

        if all_issues:
            mismatch_stocks.append((s, all_issues, verified_total, boll_v, mid_v))
        else:
            match_count += 1

    # ─── 打印结果 ───
    print(f"{'='*70}")
    print(f"  验证结果汇总（6个维度全面验证）")
    print(f"{'='*70}")
    print(f"  总样本数: {total_stocks}")
    print(f"  完全一致: {match_count} ({match_count/total_stocks*100:.1f}%)")
    print(f"  存在差异: {len(mismatch_stocks)} ({len(mismatch_stocks)/total_stocks*100:.1f}%)")
    print()
    print(f"  第一轮维度差异统计:")
    for dim in ["MACD", "KDJ", "量能", "趋势"]:
        cnt = dimension_mismatches[dim]
        print(f"    {dim}: {cnt} 只不一致")
    print(f"  第二轮维度差异统计:")
    for dim in ["下轨反弹", "中轨反弹"]:
        cnt = dimension_mismatches[dim]
        print(f"    {dim}: {cnt} 只不一致")
    print()

    if mismatch_stocks:
        print(f"{'='*70}")
        print(f"  差异明细（最多显示20条）")
        print(f"{'='*70}")
        for s, issues, v_total, boll_v, mid_v in mismatch_stocks[:20]:
            print(f"\n  {s['name']}({s['code']}) "
                  f"原始总分={s['our_total']} 验证总分={v_total} "
                  f"原始下轨={s.get('boll_score', 0)} 验证下轨={boll_v} "
                  f"原始中轨={s.get('mid_bounce_score', 0)} 验证中轨={mid_v}")
            for issue in issues:
                print(f"    ⚠ {issue}")

    # ─── 写入报告 ───
    write_report(data, match_count, mismatch_stocks, dimension_mismatches)
    print(f"\n  报告已写入: {REPORT_PATH}")
    print(f"{'='*70}")


def write_report(data, match_count, mismatch_stocks, dimension_mismatches):
    total = len(data)
    lines = [
        "# 技术面打分全维度验证报告",
        "",
        f"样本数: {total}，完全一致: {match_count} ({match_count/total*100:.1f}%)，"
        f"存在差异: {len(mismatch_stocks)} ({len(mismatch_stocks)/total*100:.1f}%)",
        "",
        "验证维度: MACD(35) + KDJ(30) + 成交量(20) + 趋势(15) + 下轨反弹(100) + 中轨反弹(100)",
        "",
        "## 各维度差异统计",
        "",
        "| 维度 | 不一致数 | 占比 | 轮次 |",
        "|------|----------|------|------|",
    ]
    round_map = {"MACD": "第一轮", "KDJ": "第一轮", "量能": "第一轮", "趋势": "第一轮",
                 "下轨反弹": "第二轮", "中轨反弹": "第二轮"}
    for dim in ALL_DIMENSIONS:
        cnt = dimension_mismatches[dim]
        lines.append(f"| {dim} | {cnt} | {cnt/total*100:.1f}% | {round_map[dim]} |")

    if mismatch_stocks:
        lines.append("")
        lines.append("## 差异明细")
        lines.append("")
        lines.append("| 股票 | 代码 | 原始总分 | 验证总分 | 下轨(原/验) | 中轨(原/验) | 差异维度 |")
        lines.append("|------|------|----------|----------|-------------|-------------|----------|")
        for s, issues, v_total, boll_v, mid_v in mismatch_stocks:
            dims = set()
            for issue in issues:
                for d in ALL_DIMENSIONS:
                    if d in issue:
                        dims.add(d)
            lines.append(
                f"| {s['name']} | {s['code']} | {s['our_total']} | {v_total} | "
                f"{s.get('boll_score', 0)}/{boll_v} | "
                f"{s.get('mid_bounce_score', 0)}/{mid_v} | "
                f"{','.join(sorted(dims))} |"
            )

        lines.append("")
        lines.append("## 差异详情")
        lines.append("")
        for s, issues, v_total, boll_v, mid_v in mismatch_stocks:
            lines.append(f"### {s['name']}({s['code']})")
            for issue in issues:
                lines.append(f"- {issue}")
            lines.append("")

    lines.append("")
    lines.append("## 结论")
    lines.append("")
    if len(mismatch_stocks) == 0:
        lines.append("所有抽样股票的6个维度打分结果与独立验证完全一致，打分逻辑正确。")
    else:
        pct = len(mismatch_stocks) / total * 100
        lines.append(f"共发现 {len(mismatch_stocks)} 只股票存在差异（{pct:.1f}%），"
                     "需要检查上述差异是否为打分逻辑bug或数据精度问题。")

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
