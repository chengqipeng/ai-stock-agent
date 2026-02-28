import json
import math
from datetime import datetime

import pandas as pd

from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_kline_cn
from service.eastmoney.strategy_engine.stock_BOLL_rule import get_boll_rule_boll_only
from service.eastmoney.strategy_engine.stock_KDJ_rule import get_kdj_rule_kdj_only
from service.eastmoney.strategy_engine.stock_MACD_rule import get_macd_signals_macd_only
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json_cn
from service.jqka10.stock_time_kline_data_10jqka import get_stock_time_kline_cn_10jqka


# ──────────────────────────────────────────────
# 数据预处理工具函数（在 Python 端完成，避免 LLM 计算幻觉）
# ──────────────────────────────────────────────

def _filter_valid_trading_days(kline_data: list[dict]) -> list[dict]:
    """过滤停牌日（成交量为0的交易日），仅保留有效交易日"""
    return [d for d in kline_data if d.get('成交量（手）', 0) > 0]


def _compute_macd_divergence(macd_data: dict, kline_data: list[dict]) -> dict:
    """
    预计算 MACD 背离信号（状态机，无未来函数）。
    在 Python 端完成，避免 LLM 自行推导出错。

    底背离：股价创新低，DIF 未创新低（20~60 交易日窗口）
    顶背离：股价创新高，DIF 未创新高（20~60 交易日窗口）
    """
    details = macd_data.get('明细数据', [])
    if len(details) < 20:
        return {'底背离': False, '顶背离': False, '背离详情': '数据不足，无法检测'}

    # 按日期升序排列
    details_asc = sorted(details, key=lambda x: x['日期'])

    # 构建价格查找表（日期 -> 最高价/最低价）
    kline_map = {}
    for k in kline_data:
        kline_map[k['日期']] = k

    # 识别多头/空头波段（基于 DIF 与 DEA 的关系）
    # 多头波段：DIF > DEA 的连续区间
    # 空头波段：DIF <= DEA 的连续区间
    bull_segments = []  # [(start_idx, end_idx), ...]
    bear_segments = []

    current_type = None  # 'bull' or 'bear'
    seg_start = 0

    for i, d in enumerate(details_asc):
        is_bull = d['DIF'] > d['DEA']
        seg_type = 'bull' if is_bull else 'bear'

        if current_type is None:
            current_type = seg_type
            seg_start = i
        elif seg_type != current_type:
            if current_type == 'bull':
                bull_segments.append((seg_start, i - 1))
            else:
                bear_segments.append((seg_start, i - 1))
            current_type = seg_type
            seg_start = i

    # 最后一个波段（正在进行中的也要计算）
    if current_type == 'bull':
        bull_segments.append((seg_start, len(details_asc) - 1))
    elif current_type == 'bear':
        bear_segments.append((seg_start, len(details_asc) - 1))

    # ── 底背离检测（空头波段之间比较）──
    bottom_divergence = False
    bottom_detail = ''
    if len(bear_segments) >= 2:
        for j in range(len(bear_segments) - 1, 0, -1):
            curr_seg = bear_segments[j]
            prev_seg = bear_segments[j - 1]

            # 窗口检查：两波谷间距 20~60 交易日
            gap = curr_seg[0] - prev_seg[1]
            if gap < 20 or gap > 60:
                continue

            # 当前波段最低价和 DIF 最低值
            curr_dates = [details_asc[k]['日期'] for k in range(curr_seg[0], curr_seg[1] + 1)]
            prev_dates = [details_asc[k]['日期'] for k in range(prev_seg[0], prev_seg[1] + 1)]

            curr_low_price = min(
                (kline_map[d]['最低价'] for d in curr_dates if d in kline_map),
                default=float('inf')
            )
            prev_low_price = min(
                (kline_map[d]['最低价'] for d in prev_dates if d in kline_map),
                default=float('inf')
            )
            curr_dif_min = min(details_asc[k]['DIF'] for k in range(curr_seg[0], curr_seg[1] + 1))
            prev_dif_min = min(details_asc[k]['DIF'] for k in range(prev_seg[0], prev_seg[1] + 1))

            if curr_low_price < prev_low_price and curr_dif_min > prev_dif_min:
                bottom_divergence = True
                bottom_detail = (
                    f"当前波段({curr_dates[0]}~{curr_dates[-1]})最低价{curr_low_price} < "
                    f"上一波段({prev_dates[0]}~{prev_dates[-1]})最低价{prev_low_price}，"
                    f"但DIF最低值{round(curr_dif_min, 4)} > {round(prev_dif_min, 4)}，构成底背离"
                )
                break

    # ── 顶背离检测（多头波段之间比较）──
    top_divergence = False
    top_detail = ''
    if len(bull_segments) >= 2:
        for j in range(len(bull_segments) - 1, 0, -1):
            curr_seg = bull_segments[j]
            prev_seg = bull_segments[j - 1]

            gap = curr_seg[0] - prev_seg[1]
            if gap < 20 or gap > 60:
                continue

            curr_dates = [details_asc[k]['日期'] for k in range(curr_seg[0], curr_seg[1] + 1)]
            prev_dates = [details_asc[k]['日期'] for k in range(prev_seg[0], prev_seg[1] + 1)]

            curr_high_price = max(
                (kline_map[d]['最高价'] for d in curr_dates if d in kline_map),
                default=0
            )
            prev_high_price = max(
                (kline_map[d]['最高价'] for d in prev_dates if d in kline_map),
                default=0
            )
            curr_dif_max = max(details_asc[k]['DIF'] for k in range(curr_seg[0], curr_seg[1] + 1))
            prev_dif_max = max(details_asc[k]['DIF'] for k in range(prev_seg[0], prev_seg[1] + 1))

            if curr_high_price > prev_high_price and curr_dif_max < prev_dif_max:
                top_divergence = True
                top_detail = (
                    f"当前波段({curr_dates[0]}~{curr_dates[-1]})最高价{curr_high_price} > "
                    f"上一波段({prev_dates[0]}~{prev_dates[-1]})最高价{prev_high_price}，"
                    f"但DIF最高值{round(curr_dif_max, 4)} < {round(prev_dif_max, 4)}，构成顶背离"
                )
                break

    return {
        '底背离': bottom_divergence,
        '顶背离': top_divergence,
        '底背离详情': bottom_detail if bottom_divergence else '未检测到底背离',
        '顶背离详情': top_detail if top_divergence else '未检测到顶背离',
    }


def _compute_kdj_summary(kdj_data: dict) -> dict:
    """
    预计算 KDJ 关键状态摘要，减少传入 LLM 的数据量。
    只保留最近 20 日明细 + 关键状态判断。
    """
    details = kdj_data.get('明细数据', [])
    # 只保留最近 20 日有效数据（KDJ=0 的为无效数据）
    recent = [d for d in details[:20] if d['K'] > 0 or d['D'] > 0]

    # 检测 KDJ 拐头
    kdj_turning = ''
    if len(recent) >= 2:
        curr, prev = recent[0], recent[1]
        if curr['K'] < prev['K'] and curr['J'] < prev['J'] and prev['K'] > 75:
            kdj_turning = '高位拐头向下（K从{:.1f}降至{:.1f}，J从{:.1f}降至{:.1f}）'.format(
                prev['K'], curr['K'], prev['J'], curr['J'])
        elif curr['K'] > prev['K'] and curr['J'] > prev['J'] and prev['K'] < 30:
            kdj_turning = '低位拐头向上（K从{:.1f}升至{:.1f}，J从{:.1f}升至{:.1f}）'.format(
                prev['K'], curr['K'], prev['J'], curr['J'])

    # 检测近5日是否曾进入超买/超卖区
    recent_5 = recent[:5]
    was_oversold = any(d['K'] < 20 and d['D'] < 20 and d['J'] < 0 for d in recent_5)
    was_overbought = any(d['K'] > 80 and d['D'] > 80 and d['J'] > 100 for d in recent_5)

    # 高位钝化检测（K>80 连续天数）
    k_above_80_streak = 0
    for d in recent:
        if d['K'] > 80:
            k_above_80_streak += 1
        else:
            break

    return {
        '最新K': kdj_data.get('K'),
        '最新D': kdj_data.get('D'),
        '最新J': kdj_data.get('J'),
        '最新信号': kdj_data.get(f'最新信号（{kdj_data.get("最新交易日")}）'),
        '近5日曾超卖': was_oversold,
        '近5日曾超买': was_overbought,
        'K>80连续天数': k_above_80_streak,
        '高位钝化': kdj_data.get(f'高位钝化（{kdj_data.get("最新交易日")}）', False),
        'KDJ拐头状态': kdj_turning if kdj_turning else '无明显拐头',
        '历史买入信号': kdj_data.get('历史买入信号（最近5次）', []),
        '历史卖出信号': kdj_data.get('历史卖出信号（最近5次）', []),
        '近20日明细': recent,
    }


def _compute_intraday_summary(time_data: list[dict]) -> dict:
    """
    预聚合分时数据为关键特征摘要，避免传入数百条原始分钟数据。
    """
    if not time_data:
        return {'状态': '无分时数据'}

    # 按时段分组
    morning_1 = [d for d in time_data if '09:30' <= d['时间'] <= '10:00']
    morning_2 = [d for d in time_data if '10:00' < d['时间'] <= '11:30']
    afternoon_1 = [d for d in time_data if '13:00' <= d['时间'] <= '14:00']
    afternoon_2 = [d for d in time_data if '14:00' < d['时间'] <= '14:57']
    closing = [d for d in time_data if d['时间'] > '14:57']

    def _segment_stats(segment, name):
        if not segment:
            return None
        prices = [d['价格'] for d in segment]
        volumes = [d['成交量'] for d in segment if d['成交量'] > 0]
        amounts = [d['成交额'] for d in segment if d['成交额'] > 0]
        return {
            '时段': name,
            '最高价': max(prices),
            '最低价': min(prices),
            '开始价': prices[0],
            '结束价': prices[-1],
            '总成交量': sum(volumes),
            '总成交额': round(sum(amounts) / 1e8, 2),  # 亿元
            '平均每分钟成交量': round(sum(volumes) / max(len(volumes), 1)),
        }

    segments = []
    for seg, name in [(morning_1, '早盘(9:30-10:00)'), (morning_2, '上午盘(10:00-11:30)'),
                      (afternoon_1, '午后(13:00-14:00)'), (afternoon_2, '尾盘前(14:00-14:57)'),
                      (closing, '集合竞价(14:57-15:00)')]:
        s = _segment_stats(seg, name)
        if s:
            segments.append(s)

    # 全天统计
    all_prices = [d['价格'] for d in time_data]
    all_volumes = [d['成交量'] for d in time_data if d['成交量'] > 0]
    first_valid = time_data[0] if time_data else {}
    last_valid = time_data[-1] if time_data else {}

    # 黄白线关系（价格 vs 均价）
    above_avg_count = sum(1 for d in time_data if d['价格'] >= d['均价'] and d['成交量'] > 0)
    below_avg_count = sum(1 for d in time_data if d['价格'] < d['均价'] and d['成交量'] > 0)
    valid_count = above_avg_count + below_avg_count

    # 脉冲式放量检测（单分钟成交量 > 均量的3倍）
    avg_vol = sum(all_volumes) / max(len(all_volumes), 1)
    pulse_events = []
    for d in time_data:
        if d['成交量'] > avg_vol * 3 and d['成交量'] > 0:
            pulse_events.append({
                '时间': d['时间'],
                '价格': d['价格'],
                '成交量': d['成交量'],
                '倍数': round(d['成交量'] / avg_vol, 1),
                '涨跌幅': d['涨跌幅'],
            })

    return {
        '开盘涨跌幅': first_valid.get('涨跌幅'),
        '收盘涨跌幅': last_valid.get('涨跌幅'),
        '全天最高价': max(all_prices),
        '全天最低价': min(all_prices),
        '收盘均价': last_valid.get('均价'),
        '收盘价': last_valid.get('价格'),
        '白线在黄线上方占比': f"{round(above_avg_count / max(valid_count, 1) * 100, 1)}%",
        '分时段统计': segments,
        '脉冲式放量事件': pulse_events[:10],  # 最多10个
        '分钟均量': round(avg_vol),
    }


def _compute_kline_summary(kline_data: list[dict]) -> dict:
    """
    预计算 K 线关键统计摘要：极值、量能特征、近期形态。
    """
    if not kline_data:
        return {}

    valid = _filter_valid_trading_days(kline_data)
    if not valid:
        return {}

    # 按日期降序（最新在前）
    highs = [d['最高价'] for d in valid]
    lows = [d['最低价'] for d in valid]
    volumes = [d['成交量（手）'] for d in valid]
    closes = [d['收盘价'] for d in valid]

    # 近期量能（最近5日 vs 50日均量）
    recent_5_vol = volumes[:5] if len(volumes) >= 5 else volumes
    vol_50_avg = sum(volumes[:50]) / min(len(volumes), 50) if volumes else 0

    # 近5日量比
    recent_vol_ratio = round(sum(recent_5_vol) / len(recent_5_vol) / vol_50_avg, 2) if vol_50_avg > 0 else 0

    # 120日极值
    high_120 = max(highs) if highs else 0
    low_120 = min(lows) if lows else 0
    high_date = next((d['日期'] for d in valid if d['最高价'] == high_120), '')
    low_date = next((d['日期'] for d in valid if d['最低价'] == low_120), '')

    # 近5日K线形态特征
    recent_5 = valid[:5]
    up_days = sum(1 for d in recent_5 if d['收盘价'] > d['开盘价'])
    down_days = sum(1 for d in recent_5 if d['收盘价'] < d['开盘价'])

    # 最新一日的上下影线
    latest = valid[0] if valid else {}
    upper_shadow = round(latest.get('最高价', 0) - max(latest.get('收盘价', 0), latest.get('开盘价', 0)), 2)
    lower_shadow = round(min(latest.get('收盘价', 0), latest.get('开盘价', 0)) - latest.get('最低价', 0), 2)
    body = round(abs(latest.get('收盘价', 0) - latest.get('开盘价', 0)), 2)

    return {
        '120日最高价': high_120,
        '120日最高价日期': high_date,
        '120日最低价': low_120,
        '120日最低价日期': low_date,
        '当前价距120日高点': f"{round((closes[0] - high_120) / high_120 * 100, 1)}%" if closes and high_120 else '',
        '当前价距120日低点': f"{round((closes[0] - low_120) / low_120 * 100, 1)}%" if closes and low_120 else '',
        '50日均量（手）': round(vol_50_avg),
        '近5日均量（手）': round(sum(recent_5_vol) / len(recent_5_vol)) if recent_5_vol else 0,
        '近5日量比（vs50日）': recent_vol_ratio,
        '近5日阳线数': up_days,
        '近5日阴线数': down_days,
        '最新日上影线': upper_shadow,
        '最新日下影线': lower_shadow,
        '最新日实体': body,
        '最新日K线形态': '长上影线' if upper_shadow > body * 2 else ('长下影线' if lower_shadow > body * 2 else '普通'),
    }


def _compute_ma_summary(ma_data: dict) -> dict:
    """预计算均线排列状态和关键乖离率判断"""
    data_list = ma_data.get('数据', [])
    if not data_list:
        return {}

    latest = data_list[0]  # 最新一天

    ma5 = latest.get('5日均线')
    ma10 = latest.get('10日均线')
    ma20 = latest.get('20日均线')
    ma60 = latest.get('60日均线')

    # 均线排列判断
    if ma5 and ma10 and ma20 and ma60:
        if ma5 > ma10 > ma20 > ma60:
            alignment = '多头排列（MA5>MA10>MA20>MA60）'
        elif ma5 < ma10 < ma20 < ma60:
            alignment = '空头排列（MA5<MA10<MA20<MA60）'
        else:
            # 细化纠缠状态
            parts = []
            if ma5 > ma20:
                parts.append('MA5上穿MA20')
            if ma10 < ma20:
                parts.append('MA10仍在MA20下方')
            alignment = '均线纠缠（{}）'.format('，'.join(parts) if parts else '短中期均线交织')
    else:
        alignment = '数据不足'

    # 乖离率极端值判断
    bias5 = latest.get('BIAS5', 0)
    bias10 = latest.get('BIAS10', 0)
    bias20 = latest.get('BIAS20', 0)
    bias60 = latest.get('BIAS60', 0)

    bias_warning = ''
    if bias5 and abs(bias5) > 7:
        bias_warning += f"BIAS5={bias5}%（{'短线超买' if bias5 > 0 else '短线超卖'}）；"
    if bias10 and abs(bias10) > 10:
        bias_warning += f"BIAS10={bias10}%（{'中线超买' if bias10 > 0 else '中线超卖'}）；"
    if bias20 and abs(bias20) > 12:
        bias_warning += f"BIAS20={bias20}%（{'中线超买' if bias20 > 0 else '中线超卖'}）；"
    if bias60 and abs(bias60) > 20:
        bias_warning += f"BIAS60={bias60}%（{'大幅偏离' if bias60 > 0 else '深度超跌'}）；"

    return {
        '均线排列状态': alignment,
        '乖离率预警': bias_warning if bias_warning else '各周期乖离率处于正常区间',
        'MA5': ma5,
        'MA10': ma10,
        'MA20': ma20,
        'MA60': ma60,
        'BIAS5': bias5,
        'BIAS10': bias10,
        'BIAS20': bias20,
        'BIAS60': bias60,
    }



def _trim_macd_details(macd_data: dict, keep_recent: int = 30) -> dict:
    """精简 MACD 明细数据，只保留最近 N 日，减少 token 消耗"""
    trimmed = dict(macd_data)
    if '明细数据' in trimmed:
        trimmed['明细数据'] = trimmed['明细数据'][:keep_recent]
    return trimmed


def _trim_boll_details(boll_data: dict, keep_recent: int = 20) -> dict:
    """精简 BOLL 明细数据"""
    trimmed = dict(boll_data)
    if '明细数据' in trimmed:
        trimmed['明细数据'] = trimmed['明细数据'][:keep_recent]
    return trimmed


def _trim_kline_data(kline_data: list[dict], keep_recent: int = 30) -> list[dict]:
    """精简 K 线数据，只保留最近 N 日（完整数据的极值已在 summary 中提取）"""
    valid = _filter_valid_trading_days(kline_data)
    return valid[:keep_recent]


def _trim_ma_data(ma_data: dict, keep_recent: int = 20) -> dict:
    """精简均线数据，只保留最近 N 日"""
    trimmed = dict(ma_data)
    if '数据' in trimmed:
        trimmed['数据'] = trimmed['数据'][:keep_recent]
    return trimmed


# ──────────────────────────────────────────────
# 主函数
# ──────────────────────────────────────────────

async def get_stock_indicator_prompt(stock_info: StockInfo):
    data_num = 120

    # 并发获取所有数据
    boll_rule_boll = await get_boll_rule_boll_only(stock_info)
    stock_day_kline = await get_stock_day_kline_cn(stock_info, data_num)
    kdj_rule_kdj = await get_kdj_rule_kdj_only(stock_info, data_num)
    macd_signals_macd = await get_macd_signals_macd_only(stock_info, data_num)
    stock_time_kline_10jqka = await get_stock_time_kline_cn_10jqka(stock_info, 240)

    moving_averages_json = await get_moving_averages_json_cn(
        stock_info,
        ["date", "close_5_sma", "close_10_ema", "close_20_sma", "close_60_sma",
         "bias_5", "bias_10", "bias_20", "bias_60"],
        120
    )

    # ── Python 端预计算（核心优化：把容易出错的计算从 LLM 移到代码端）──
    valid_kline = _filter_valid_trading_days(stock_day_kline)
    divergence_result = _compute_macd_divergence(macd_signals_macd, valid_kline)
    kdj_summary = _compute_kdj_summary(kdj_rule_kdj)
    intraday_summary = _compute_intraday_summary(stock_time_kline_10jqka)
    kline_summary = _compute_kline_summary(valid_kline)
    ma_summary = _compute_ma_summary(moving_averages_json)

    # ── 精简数据（减少 token，降低幻觉概率）──
    macd_trimmed = _trim_macd_details(macd_signals_macd, keep_recent=30)
    boll_trimmed = _trim_boll_details(boll_rule_boll, keep_recent=20)
    kline_trimmed = _trim_kline_data(valid_kline, keep_recent=30)
    ma_trimmed = _trim_ma_data(moving_averages_json, keep_recent=20)

    return f"""
# 当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

# Role: 资深A股技术面分析师 / 操盘手
你拥有20年实战交易经验，精通量价关系、分时盘口语言以及MACD、KDJ、BOLL等核心指标的底层逻辑。你的分析风格：客观、严谨、直击本质、拒绝模棱两可。

## Task: {stock_info.stock_name}（{stock_info.stock_code_normalize}）次日实战操作推演
请基于我提供的"预计算分析结论"与"精简后的原始数据"，进行深度交叉验证，输出具备极强实操指导意义的分析报告。

---

## ★ 重要约束（必须遵守）

1. **直接引用预计算结论**：背离信号、KDJ状态摘要、分时特征摘要、K线统计摘要、均线排列状态 均已在 Python 端预计算完成，你必须直接引用这些结论，严禁自行重新推导。
2. **禁止计算幻觉**：均线值与乖离率必须直接读取提供的数据，严禁自行计算。
3. **数据已清洗**：提供的数据已过滤停牌日（成交量为0的交易日），无需再次过滤。
4. **严禁主观臆断**：每一个结论必须紧跟数据论据，引用具体数值。
5. **精简原始数据仅供验证**：原始数据仅保留近期关键部分，用于验证预计算结论的合理性，不要试图从中推导120日全量统计。

---

## Analysis Framework - 请严格按此结构输出

### 一、 核心指标交叉验证（日线级别）

#### 1. MACD（趋势与动能）

**判定规则（仅供参考，预计算结论已给出）：**
- Rule a：DIF>0且DEA>0→强多头；DIF>0且DEA≤0→弱多头；DIF<0→空头
- Rule b：金叉/死叉/零轴上金叉/零轴下死叉
- Rule c：背离信号 → **直接引用下方预计算结论，严禁自行推导**

**★ 预计算背离结论（必须直接引用）：**
{json.dumps(divergence_result, ensure_ascii=False, indent=2)}

请结合MACD明细数据，分析当前多空状态、最近金叉/死叉的质量、MACD柱的变化趋势（放大/收窄），并引用上述背离结论。

#### 2. KDJ（极限与拐点）

**判定规则：**
- 买入：近5日曾超卖(K<20,D<20,J<0) + 金叉 + J勾头向上
- 卖出(钝化)：K>80连续N天 + 跌破MA5/MA20 → 钝化出局
- 卖出(普通)：非钝化 + 近5日曾超买(K>80,D>80,J>100) + 死叉

**★ 预计算KDJ状态摘要（必须直接引用）：**
{json.dumps(kdj_summary, ensure_ascii=False, indent=2)}

请基于上述摘要分析KDJ当前位置、是否存在买卖信号、拐头方向。

#### 3. BOLL（空间与边界）

**判定规则：**
- 强势开启：放量突破中轨（昨收<=昨中轨 且 今收>今中轨 且 量>50日均量×1.5）
- 波段结束：跌破中轨（昨收>=昨中轨 且 今收<今中轨）
- 可操作区：收盘>中轨 且 中轨向上倾斜
- 喇叭口加速：上下轨反向张开 且 带宽单日放大超10%

请结合BOLL明细数据和K线统计中的50日均量，分析当前轨道位置、突破质量、运行空间。

### 二、 中期结构与均线系统（120日大局观）

**★ 预计算均线状态（必须直接引用）：**
{json.dumps(ma_summary, ensure_ascii=False, indent=2)}

**★ 预计算K线统计摘要（必须直接引用）：**
{json.dumps(kline_summary, ensure_ascii=False, indent=2)}

请基于上述预计算结论，分析均线排列、乖离率风险、量价匹配、支撑压力位。严禁自行计算均线值。

### 三、 今日分时盘口深度解析（超短线博弈）

**★ 预计算分时特征摘要（必须直接引用）：**
{json.dumps(intraday_summary, ensure_ascii=False, indent=2)}

请基于上述摘要分析：
- 黄白线格局（白线在黄线上方占比已给出）
- 各时段量价分布特征
- 脉冲式放量事件的含义
- 尾盘资金动向

### 四、 多空力量博弈清单
以列表形式，客观陈述当前盘面的核心利多与利空因素：
- **多方筹码（有利因素）**：[✅] （提炼3-5个核心数据支撑点，每条必须引用具体数值）
- **空方筹码（不利因素）**：[❌] （提炼3-5个核心风险警示点，每条必须引用具体数值）

### 五、 综合评分体系（满分100分）

| 评分维度 | 权重 | 核心考察点 | 得分 |
|---------|------|----------|------|
| 趋势强度 | 25% | MACD位置、长短期均线排列 | /25 |
| 动能与量价 | 25% | KDJ拐点、分时量能、日线量价配合 | /25 |
| 结构边界 | 20% | BOLL轨道位置、乖离率极端值 | /20 |
| 短线情绪 | 15% | 分时均线承接力、尾盘资金动向 | /15 |
| 风险收益比 | 15% | 潜在上涨空间 vs 潜在下跌空间 | /15 |
| **总分** | **100%**| | **/100** |

**评分必须与预计算结论一致**：
- 若预计算显示顶背离，趋势强度不应超过15分
- 若KDJ高位拐头，动能得分应相应扣减
- 若分时白线长期在黄线下方，短线情绪不应超过8分

**实战评级标准**：
- 90-100：积极买入
- 75-89：逢低建仓
- 60-74：持股待涨
- 40-59：逢高减仓
- 20-39：保持观望
- <20：清仓离场

### 六、 明日实战操作策略（Strategy）

#### 1. 操作定调
[明确：积极买入 / 逢低建仓 / 持股待涨 / 逢高减仓 / 清仓离场 / 保持观望]

#### 2. 核心操盘点位（精确到±0.5元）
- **重要压力位**：[阻力位1] / [阻力位2]（必须基于K线统计摘要中的极值和均线数据）
- **强支撑位**：[支撑位1] / [支撑位2]（必须基于BOLL中轨、均线、前期低点）

#### 3. 动态情景推演与应对（If-Then逻辑）
**▶ 情景A：向上突破或高开（强势剧本）**
- **触发条件**：[具体量价与点位条件]
- **操作建议**：[持仓者/空仓者具体动作]

**▶ 情景B：向下破位或低开（弱势剧本）**
- **触发条件**：[具体量价与点位条件]
- **操作建议**：[持仓者/空仓者具体动作]

**▶ 情景C：区间震荡（平庸剧本）**
- **触发条件**：[具体量价与点位条件]
- **操作建议**：[具体动作]

### 七、 盘中关键观察哨（明日盯盘重点）

| 监控指标 | 当前状态 | 次日健康/安全阈值 | 危险破位/预警线 |
|--------|--------|----------------|---------------|
| 15分钟MACD | | | |
| 分时均量线 | | | |
| 关键均线(如MA5) | | | |

---

## 原始数据（精简版，仅供验证预计算结论）

**1. MACD数据（近30日）**：
{json.dumps(macd_trimmed, ensure_ascii=False)}

**2. BOLL数据（近20日）**：
{json.dumps(boll_trimmed, ensure_ascii=False)}

**3. 均线与乖离率数据（近20日）**：
{json.dumps(ma_trimmed, ensure_ascii=False)}

**4. 近30日交易数据**：
{json.dumps(kline_trimmed, ensure_ascii=False)}

---

[最终输出] 只能输出json格式数据：
{{
  'stock_code': '<股票代码>',
  'stock_name': '<股票名称>',
  'not_hold_grade': '<未持有建议，积极买入 / 逢低建仓 / 保持观望>',
  'hold_grade': '<持有建议，持股待涨 / 逢高减仓 / 清仓离场>',
  'content': '<深度分析关键的判断内容，输出markdown格式>'
}}
"""


if __name__ == '__main__':
    import asyncio
    from common.utils.stock_info_utils import get_stock_info_by_name

    async def main():
        stock_info = get_stock_info_by_name('生益科技')
        prompt = await get_stock_indicator_prompt(stock_info)
        print(prompt)

    asyncio.run(main())
