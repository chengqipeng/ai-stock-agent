import json
import math
from datetime import datetime

import pandas as pd

from common.utils.stock_info_utils import StockInfo
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_kline_cn
from service.eastmoney.stock_info.stock_northbound_funds import get_northbound_funds_cn
from service.eastmoney.stock_info.stock_real_fund_flow import get_real_main_fund_flow
from service.eastmoney.strategy_engine.stock_BOLL_rule import get_boll_rule_boll_only
from service.eastmoney.strategy_engine.stock_KDJ_rule import get_kdj_rule_kdj_only
from service.eastmoney.strategy_engine.stock_MACD_rule import get_macd_signals_macd_only
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json_cn
from service.jqka10.stock_time_kline_data_10jqka import get_stock_time_kline_cn_10jqka
from service.web_search.stock_news_search import search_stock_news, format_news_for_prompt


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
    k_change = 0.0
    j_change = 0.0
    if len(recent) >= 2:
        curr, prev = recent[0], recent[1]
        k_change = round(curr['K'] - prev['K'], 2)
        j_change = round(curr['J'] - prev['J'], 2)
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

    # K-D 差值（避免 LLM 自行做减法）
    latest_k = kdj_data.get('K', 0) or 0
    latest_d = kdj_data.get('D', 0) or 0
    kd_diff = round(latest_k - latest_d, 2)

    return {
        '最新K': kdj_data.get('K'),
        '最新D': kdj_data.get('D'),
        '最新J': kdj_data.get('J'),
        'K-D差值': kd_diff,
        'K日变化': k_change,
        'J日变化': j_change,
        '最新信号': kdj_data.get(f'最新信号（{kdj_data.get("最新交易日")}）'),
        '近5日曾超卖': was_oversold,
        '近5日曾超买': was_overbought,
        'K>80连续天数': k_above_80_streak,
        '高位钝化': kdj_data.get(f'高位钝化（{kdj_data.get("最新交易日")}）', False),
        'KDJ拐头状态': kdj_turning if kdj_turning else '无明显拐头',
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

    # 预先计算全天分钟均量，供各时段倍数计算使用
    all_volumes_pre = [d['成交量'] for d in time_data if d['成交量'] > 0]
    avg_vol_pre = sum(all_volumes_pre) / max(len(all_volumes_pre), 1)

    def _segment_stats(segment, name):
        if not segment:
            return None
        prices = [d['价格'] for d in segment]
        volumes = [d['成交量'] for d in segment if d['成交量'] > 0]
        amounts = [d['成交额'] for d in segment if d['成交额'] > 0]
        seg_avg_vol = round(sum(volumes) / max(len(volumes), 1))
        price_change = round(prices[-1] - prices[0], 2)
        return {
            '时段': name,
            '最高价': max(prices),
            '最低价': min(prices),
            '开始价': prices[0],
            '结束价': prices[-1],
            '时段内价格变化': price_change,
            '总成交量': sum(volumes),
            '总成交额': round(sum(amounts) / 1e8, 2),  # 亿元
            '平均每分钟成交量': seg_avg_vol,
            '分钟均量vs全天倍数': round(seg_avg_vol / avg_vol_pre, 2) if avg_vol_pre > 0 else 0,
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

    # 收盘价与均价差值（避免 LLM 自行计算）
    closing_price = last_valid.get('价格', 0)
    closing_avg_price = last_valid.get('均价', 0)
    close_vs_avg_diff = round(closing_price - closing_avg_price, 3) if closing_price and closing_avg_price else 0
    close_vs_avg_pct = round(close_vs_avg_diff / closing_avg_price * 100, 2) if closing_avg_price else 0

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

    # 脉冲事件价格变化范围（避免 LLM 自行计算）
    pulse_price_range = {}
    if pulse_events:
        pulse_prices = [e['价格'] for e in pulse_events]
        pulse_price_range = {
            '脉冲最高价': max(pulse_prices),
            '脉冲最低价': min(pulse_prices),
            '脉冲价格跌幅': round(min(pulse_prices) - max(pulse_prices), 2),
            '脉冲时间跨度': f"{pulse_events[0]['时间']}~{pulse_events[-1]['时间']}",
            '脉冲涨跌幅范围': f"{pulse_events[0]['涨跌幅']}%~{pulse_events[-1]['涨跌幅']}%",
        }

    return {
        '开盘涨跌幅': first_valid.get('涨跌幅'),
        '收盘涨跌幅': last_valid.get('涨跌幅'),
        '全天最高价': max(all_prices),
        '全天最低价': min(all_prices),
        '收盘均价': closing_avg_price,
        '收盘价': closing_price,
        '收盘价vs均价差值（元）': close_vs_avg_diff,
        '收盘价vs均价差值（%）': close_vs_avg_pct,
        '白线在黄线上方占比': f"{round(above_avg_count / max(valid_count, 1) * 100, 1)}%",
        '分时段统计': segments,
        '脉冲式放量事件': pulse_events[:10],  # 最多10个
        '脉冲事件汇总': pulse_price_range,
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

    # 放量突破阈值（50日均量×1.5），供 BOLL 突破质量判断使用
    vol_50_avg_rounded = round(vol_50_avg)
    breakout_vol_threshold = round(vol_50_avg * 1.5)

    # 近5日振幅与换手率统计（避免 LLM 忽略或自行计算）
    recent_5_amplitude = [d.get('振幅(%)', 0) for d in recent_5]
    recent_5_turnover = [d.get('换手率(%)', 0) for d in recent_5]
    avg_amplitude_5 = round(sum(recent_5_amplitude) / max(len(recent_5_amplitude), 1), 2)
    avg_turnover_5 = round(sum(recent_5_turnover) / max(len(recent_5_turnover), 1), 3)
    max_amplitude_5 = max(recent_5_amplitude) if recent_5_amplitude else 0
    max_turnover_5 = max(recent_5_turnover) if recent_5_turnover else 0

    # 最新日成交量与放量阈值的对比（避免 LLM 自行比较）
    latest_vol = latest.get('成交量（手）', 0)
    latest_vol_vs_threshold = '未达标' if latest_vol < breakout_vol_threshold else '已达标'
    latest_vol_vs_threshold_detail = f"{latest_vol}手 vs 阈值{breakout_vol_threshold}手（{'未达到' if latest_vol < breakout_vol_threshold else '已达到'}放量突破标准）"

    return {
        '120日最高价': high_120,
        '120日最高价日期': high_date,
        '120日最低价': low_120,
        '120日最低价日期': low_date,
        '当前价距120日高点': f"{round((closes[0] - high_120) / high_120 * 100, 1)}%" if closes and high_120 else '',
        '当前价距120日低点': f"{round((closes[0] - low_120) / low_120 * 100, 1)}%" if closes and low_120 else '',
        '50日均量（手）': vol_50_avg_rounded,
        '放量突破阈值（手）': breakout_vol_threshold,
        '最新日成交量（手）': latest_vol,
        '最新日成交量vs放量阈值': latest_vol_vs_threshold_detail,
        '近5日均量（手）': round(sum(recent_5_vol) / len(recent_5_vol)) if recent_5_vol else 0,
        '近5日量比（vs50日）': recent_vol_ratio,
        '近5日阳线数': up_days,
        '近5日阴线数': down_days,
        '近5日平均振幅(%)': avg_amplitude_5,
        '近5日最大振幅(%)': max_amplitude_5,
        '近5日平均换手率(%)': avg_turnover_5,
        '近5日最大换手率(%)': max_turnover_5,
        '最新日上影线': upper_shadow,
        '最新日下影线': lower_shadow,
        '最新日实体': body,
        '上影线vs实体比': round(upper_shadow / body, 2) if body > 0 else float('inf'),
        '下影线vs实体比': round(lower_shadow / body, 2) if body > 0 else float('inf'),
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
def _compute_macd_bar_trend(macd_data: dict) -> dict:
    """
    预计算 MACD 柱变化趋势（连续放大/收窄天数及方向），
    避免 LLM 自行比较序列数据。
    """
    details = macd_data.get('明细数据', [])
    if len(details) < 2:
        return {'MACD柱趋势': '数据不足'}

    # 明细数据已按日期降序（最新在前）
    bars = [d['MACD柱'] for d in details]
    latest_bar = bars[0]
    bar_color = '红柱' if latest_bar > 0 else ('绿柱' if latest_bar < 0 else '零')

    # 计算连续放大或收窄天数
    # 放大：绝对值递增；收窄：绝对值递减
    expanding_days = 0
    shrinking_days = 0
    for i in range(len(bars) - 1):
        curr_abs = abs(bars[i])
        prev_abs = abs(bars[i + 1])
        # 只在同色柱之间比较
        if (bars[i] > 0) != (bars[i + 1] > 0):
            break
        if curr_abs > prev_abs:
            expanding_days += 1
        else:
            break
    if expanding_days == 0:
        for i in range(len(bars) - 1):
            curr_abs = abs(bars[i])
            prev_abs = abs(bars[i + 1])
            if (bars[i] > 0) != (bars[i + 1] > 0):
                break
            if curr_abs < prev_abs:
                shrinking_days += 1
            else:
                break

    if expanding_days > 0:
        trend_desc = f'{bar_color}连续放大{expanding_days}日'
    elif shrinking_days > 0:
        trend_desc = f'{bar_color}连续收窄{shrinking_days}日'
    else:
        trend_desc = f'{bar_color}，无明显连续趋势'

    # 最近3日MACD柱值，供直接引用
    recent_bars = [{'日期': details[i]['日期'], 'MACD柱': details[i]['MACD柱']} for i in range(min(3, len(details)))]

    return {
        'MACD柱趋势': trend_desc,
        '最新MACD柱': latest_bar,
        '柱色': bar_color,
        '连续放大天数': expanding_days,
        '连续收窄天数': shrinking_days,
        '近3日MACD柱': recent_bars,
    }


def _compute_boll_summary(boll_data: dict, latest_close: float) -> dict:
    """
    预计算 BOLL 关键距离指标，避免 LLM 自行做减法和除法。
    """
    details = boll_data.get('明细数据', [])
    if not details:
        return {'状态': '无BOLL数据'}

    latest = details[0]
    mid = latest.get('BOLL', 0)
    upper = latest.get('BOLL_UB', 0)
    lower = latest.get('BOLL_LB', 0)

    dist_mid = round(latest_close - mid, 2) if mid else 0
    dist_mid_pct = round(dist_mid / mid * 100, 2) if mid else 0
    dist_upper = round(upper - latest_close, 2) if upper else 0
    dist_upper_pct = round(dist_upper / latest_close * 100, 2) if latest_close else 0
    dist_lower = round(latest_close - lower, 2) if lower else 0
    dist_lower_pct = round(dist_lower / latest_close * 100, 2) if latest_close else 0

    # 带宽
    bandwidth = round((upper - lower) / mid * 100, 2) if mid else 0

    # 中轨方向（对比前一日）
    mid_direction = '无法判断'
    if len(details) >= 2:
        prev_mid = details[1].get('BOLL', 0)
        if mid > prev_mid:
            mid_direction = '上倾'
        elif mid < prev_mid:
            mid_direction = '下倾'
        else:
            mid_direction = '走平'

    return {
        '收盘价': latest_close,
        'BOLL中轨': mid,
        'BOLL上轨': upper,
        'BOLL下轨': lower,
        '距中轨（元）': dist_mid,
        '距中轨（%）': dist_mid_pct,
        '距上轨（元）': dist_upper,
        '距上轨（%）': dist_upper_pct,
        '距下轨（元）': dist_lower,
        '距下轨（%）': dist_lower_pct,
        '带宽（%）': bandwidth,
        '中轨方向': mid_direction,
        '收盘价位置': '中轨上方' if dist_mid > 0 else ('中轨下方' if dist_mid < 0 else '中轨附近'),
    }

def _compute_data_consistency_check(kline_data: list[dict], intraday_summary: dict) -> dict:
    """
    预计算日线与分时数据的一致性校验，避免 LLM 引用矛盾数据。
    """
    if not kline_data or not intraday_summary or intraday_summary.get('状态') == '无分时数据':
        return {'校验状态': '数据不足，无法校验'}

    latest_kline = kline_data[0]
    kline_high = latest_kline.get('最高价', 0)
    kline_low = latest_kline.get('最低价', 0)
    kline_open = latest_kline.get('开盘价', 0)
    kline_close = latest_kline.get('收盘价', 0)

    intraday_high = intraday_summary.get('全天最高价', 0)
    intraday_low = intraday_summary.get('全天最低价', 0)
    intraday_close = intraday_summary.get('收盘价', 0)

    warnings = []
    if kline_high and intraday_high and abs(kline_high - intraday_high) > 0.5:
        warnings.append(
            f"日线最高价{kline_high} vs 分时最高价{intraday_high}，差异{round(kline_high - intraday_high, 2)}元"
            f"（日线含集合竞价，分时可能不含，以日线数据为准）"
        )
    if kline_low and intraday_low and abs(kline_low - intraday_low) > 0.5:
        warnings.append(
            f"日线最低价{kline_low} vs 分时最低价{intraday_low}，差异{round(kline_low - intraday_low, 2)}元"
        )
    if kline_close and intraday_close and abs(kline_close - intraday_close) > 0.01:
        warnings.append(
            f"日线收盘价{kline_close} vs 分时收盘价{intraday_close}，差异{round(kline_close - intraday_close, 2)}元"
        )

    return {
        '校验状态': '存在差异' if warnings else '数据一致',
        '差异详情': warnings if warnings else ['日线与分时数据一致，无矛盾'],
        '建议': '价格极值以日线数据为准（含集合竞价），分时数据用于盘口行为分析' if warnings else '',
    }


def _compute_scenario_reference(kline_summary: dict, boll_summary: dict, ma_summary: dict, intraday_summary: dict) -> dict:
    """
    预计算情景推演中的关键参考阈值，避免 LLM 自行拍脑袋。
    """
    vol_50_avg = kline_summary.get('50日均量（手）', 0)
    breakout_threshold = kline_summary.get('放量突破阈值（手）', 0)
    minute_avg_vol = intraday_summary.get('分钟均量', 0) if isinstance(intraday_summary, dict) else 0

    # 早盘30分钟量能参考（基于今日早盘数据）
    segments = intraday_summary.get('分时段统计', []) if isinstance(intraday_summary, dict) else []
    morning_vol = 0
    for seg in segments:
        if '早盘' in seg.get('时段', ''):
            morning_vol = seg.get('总成交量', 0)
            break

    ma5 = ma_summary.get('MA5', 0)
    ma10 = ma_summary.get('MA10', 0)
    ma20 = ma_summary.get('MA20', 0)
    ma60 = ma_summary.get('MA60', 0)
    boll_mid = boll_summary.get('BOLL中轨', 0)
    boll_upper = boll_summary.get('BOLL上轨', 0)
    boll_lower = boll_summary.get('BOLL下轨', 0)

    return {
        '放量标准（日线）': f"日成交量>{breakout_threshold}手（50日均量{vol_50_avg}手×1.5）",
        '今日早盘30分钟总量': f"{morning_vol}手",
        '今日分钟均量': f"{minute_avg_vol}手",
        '关键压力位参考': {
            'MA20': ma20,
            'BOLL上轨': boll_upper,
            '120日最高价': kline_summary.get('120日最高价', 0),
        },
        '关键支撑位参考': {
            'MA5': ma5,
            'BOLL中轨': boll_mid,
            'MA60': ma60,
            'BOLL下轨': boll_lower,
        },
    }

def _compute_volume_trend(kline_data: list[dict]) -> dict:
    """
    预计算量能趋势的系统分析：近30日成交量变化趋势、缩放量节奏、量价配合度。
    避免 LLM 自行从原始K线数据中推导量能趋势。
    """
    valid = _filter_valid_trading_days(kline_data)
    if len(valid) < 5:
        return {'状态': '数据不足'}

    volumes = [d['成交量（手）'] for d in valid]
    closes = [d['收盘价'] for d in valid]
    changes = [d.get('涨跌幅(%)', 0) for d in valid]

    # ── 近5日逐日量能变化 ──
    daily_vol_changes = []
    for i in range(min(5, len(volumes) - 1)):
        prev_vol = volumes[i + 1]
        curr_vol = volumes[i]
        change_pct = round((curr_vol - prev_vol) / prev_vol * 100, 1) if prev_vol > 0 else 0
        daily_vol_changes.append({
            '日期': valid[i]['日期'],
            '成交量（手）': curr_vol,
            '较前日变化(%)': change_pct,
            '放量/缩量': '放量' if change_pct > 20 else ('缩量' if change_pct < -20 else '平量'),
            '涨跌幅(%)': changes[i],
        })

    # ── 量能趋势判断（近5日） ──
    recent_5_vol = volumes[:5]
    vol_increasing = all(recent_5_vol[i] >= recent_5_vol[i + 1] for i in range(min(4, len(recent_5_vol) - 1)))
    vol_decreasing = all(recent_5_vol[i] <= recent_5_vol[i + 1] for i in range(min(4, len(recent_5_vol) - 1)))

    if vol_increasing:
        vol_trend_5d = '连续放量'
    elif vol_decreasing:
        vol_trend_5d = '连续缩量'
    else:
        vol_trend_5d = '量能不规则波动'

    # ── 量价配合度分析（近10日） ──
    recent_10 = min(10, len(volumes))
    vol_price_match = 0  # 量价同向天数
    vol_price_diverge = 0  # 量价背离天数
    for i in range(recent_10 - 1):
        price_up = changes[i] > 0
        vol_up = volumes[i] > volumes[i + 1]
        if (price_up and vol_up) or (not price_up and not vol_up):
            vol_price_match += 1
        else:
            vol_price_diverge += 1

    total_compared = vol_price_match + vol_price_diverge
    match_ratio = round(vol_price_match / total_compared * 100, 1) if total_compared > 0 else 0

    # ── 脉冲式放量检测（近20日，单日量>前5日均量2倍） ──
    pulse_vol_days = []
    for i in range(min(20, len(volumes))):
        if i + 5 < len(volumes):
            prev_5_avg = sum(volumes[i + 1:i + 6]) / 5
            if prev_5_avg > 0 and volumes[i] > prev_5_avg * 2:
                pulse_vol_days.append({
                    '日期': valid[i]['日期'],
                    '成交量（手）': volumes[i],
                    '前5日均量（手）': round(prev_5_avg),
                    '倍数': round(volumes[i] / prev_5_avg, 2),
                    '涨跌幅(%)': changes[i],
                    '量价关系': '放量上涨' if changes[i] > 0 else '放量下跌',
                })

    # ── 近10日/20日均量对比 ──
    vol_10_avg = round(sum(volumes[:min(10, len(volumes))]) / min(10, len(volumes)))
    vol_20_avg = round(sum(volumes[:min(20, len(volumes))]) / min(20, len(volumes)))
    vol_10_vs_20 = round((vol_10_avg - vol_20_avg) / vol_20_avg * 100, 1) if vol_20_avg > 0 else 0

    return {
        '近5日量能趋势': vol_trend_5d,
        '近5日逐日量能变化': daily_vol_changes,
        '近10日量价配合度': f"{match_ratio}%（{vol_price_match}天同向/{vol_price_diverge}天背离）",
        '近10日均量（手）': vol_10_avg,
        '近20日均量（手）': vol_20_avg,
        '10日均量vs20日均量': f"{vol_10_vs_20}%（{'量能扩张' if vol_10_vs_20 > 10 else ('量能萎缩' if vol_10_vs_20 < -10 else '量能平稳')}）",
        '近20日脉冲式放量': pulse_vol_days if pulse_vol_days else '无脉冲式放量',
    }


async def _fetch_market_index_summary(index_name: str, days: int = 20) -> dict:
    """
    获取大盘指数近期K线数据并预计算摘要。
    """
    try:
        index_info = get_stock_info_by_name(index_name)
        if not index_info:
            return {'指数名称': index_name, '状态': '无法获取指数信息'}

        kline_data = await get_stock_day_kline_cn(index_info, limit=days)
        if not kline_data:
            return {'指数名称': index_name, '状态': '无K线数据'}

        # kline_data 按日期降序（最新在前）
        latest = kline_data[0]
        latest_close = latest['收盘价']
        latest_change = latest.get('涨跌幅(%)', 0)

        closes = [d['收盘价'] for d in kline_data]
        highs = [d['最高价'] for d in kline_data]
        lows = [d['最低价'] for d in kline_data]
        changes = [d.get('涨跌幅(%)', 0) for d in kline_data]

        # 近N日涨跌幅累计
        if len(closes) >= 2:
            change_3d = round((closes[0] - closes[min(2, len(closes) - 1)]) / closes[min(2, len(closes) - 1)] * 100, 2) if len(closes) > 2 else latest_change
            change_5d = round((closes[0] - closes[min(4, len(closes) - 1)]) / closes[min(4, len(closes) - 1)] * 100, 2) if len(closes) > 4 else None
            change_10d = round((closes[0] - closes[min(9, len(closes) - 1)]) / closes[min(9, len(closes) - 1)] * 100, 2) if len(closes) > 9 else None
            change_20d = round((closes[0] - closes[-1]) / closes[-1] * 100, 2) if len(closes) > 1 else None
        else:
            change_3d = change_5d = change_10d = change_20d = None

        # 近N日阳线/阴线统计
        up_days = sum(1 for c in changes if c and c > 0)
        down_days = sum(1 for c in changes if c and c < 0)

        # 近期高低点
        period_high = max(highs) if highs else 0
        period_low = min(lows) if lows else 0

        # 趋势判断
        if len(closes) >= 5:
            ma5 = sum(closes[:5]) / 5
            trend_vs_ma5 = '站上5日均线' if latest_close > ma5 else '跌破5日均线'
        else:
            ma5 = None
            trend_vs_ma5 = '数据不足'

        if len(closes) >= 10:
            ma10 = sum(closes[:10]) / 10
            trend_vs_ma10 = '站上10日均线' if latest_close > ma10 else '跌破10日均线'
        else:
            ma10 = None
            trend_vs_ma10 = '数据不足'

        # 连涨/连跌天数
        streak = 0
        streak_dir = ''
        for c in changes:
            if c is None:
                break
            if streak == 0:
                streak_dir = '涨' if c > 0 else '跌'
                streak = 1
            elif (c > 0 and streak_dir == '涨') or (c < 0 and streak_dir == '跌'):
                streak += 1
            else:
                break

        # 近5日明细（供验证）
        recent_5_detail = []
        for d in kline_data[:5]:
            recent_5_detail.append({
                '日期': d['日期'],
                '收盘价': d['收盘价'],
                '涨跌幅(%)': d.get('涨跌幅(%)', 0),
                '成交额': d.get('成交额', ''),
            })

        return {
            '指数名称': index_name,
            '最新收盘价': latest_close,
            '最新涨跌幅(%)': latest_change,
            '连续状态': f"连{streak_dir}{streak}日" if streak > 1 else f"最新日{'上涨' if latest_change and latest_change > 0 else '下跌'}{abs(latest_change) if latest_change else 0}%",
            '近3日累计涨跌(%)': change_3d,
            '近5日累计涨跌(%)': change_5d,
            '近10日累计涨跌(%)': change_10d,
            '近20日累计涨跌(%)': change_20d,
            f'近{len(kline_data)}日阳线/阴线': f"{up_days}阳/{down_days}阴",
            f'近{len(kline_data)}日最高': period_high,
            f'近{len(kline_data)}日最低': period_low,
            '5日均线位置': trend_vs_ma5,
            '10日均线位置': trend_vs_ma10,
            '近5日明细': recent_5_detail,
        }
    except Exception as e:
        return {'指数名称': index_name, '状态': f'获取失败: {str(e)}'}


async def _compute_market_environment(stock_info: StockInfo) -> dict:
    """
    预计算大盘环境数据：上证指数 + 深证成指近期走势摘要。
    同时计算个股与大盘的联动性。
    """
    # 获取上证和深证指数数据
    sh_summary = await _fetch_market_index_summary('上证指数', days=20)
    sz_summary = await _fetch_market_index_summary('深证成指', days=20)

    # 判断大盘整体环境
    sh_change_5d = sh_summary.get('近5日累计涨跌(%)')
    sz_change_5d = sz_summary.get('近5日累计涨跌(%)')
    sh_latest = sh_summary.get('最新涨跌幅(%)', 0) or 0
    sz_latest = sz_summary.get('最新涨跌幅(%)', 0) or 0

    if sh_change_5d is not None and sz_change_5d is not None:
        avg_5d = (sh_change_5d + sz_change_5d) / 2
        if avg_5d > 2:
            market_sentiment = '偏多（近5日大盘整体上涨）'
        elif avg_5d < -2:
            market_sentiment = '偏空（近5日大盘整体下跌）'
        else:
            market_sentiment = '震荡（近5日大盘涨跌幅有限）'
    else:
        market_sentiment = '无法判断'

    # 当日大盘表现
    if sh_latest > 0.5 and sz_latest > 0.5:
        today_market = '当日大盘普涨'
    elif sh_latest < -0.5 and sz_latest < -0.5:
        today_market = '当日大盘普跌'
    else:
        today_market = '当日大盘分化/震荡'

    return {
        '上证指数': sh_summary,
        '深证成指': sz_summary,
        '大盘环境判断': market_sentiment,
        '当日大盘表现': today_market,
        '个股所属指数': stock_info.indices_stock_name or '未知',
    }






def _trim_macd_details(macd_data: dict, keep_recent: int = 30) -> dict:
    """精简 MACD 明细数据，只保留最近 N 日，减少 token 消耗"""
    trimmed = dict(macd_data)
    if '明细数据' in trimmed:
        trimmed['明细数据'] = trimmed['明细数据'][:keep_recent]
    # 近期金叉/死叉只保留最近2次，剔除更早的历史记录
    if '近期金叉（最近3次）' in trimmed:
        trimmed['近期金叉（最近2次）'] = trimmed.pop('近期金叉（最近3次）')[:2]
    if '近期死叉（最近3次）' in trimmed:
        trimmed['近期死叉（最近2次）'] = trimmed.pop('近期死叉（最近3次）')[:2]
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
    real_main_fund_flow = await get_real_main_fund_flow(stock_info)

    northbound_funds_cn = await get_northbound_funds_cn(stock_info, ['TRADE_DATE', 'ADD_MARKET_CAP', 'ADD_SHARES_AMP', 'ADD_SHARES_AMP'])

    moving_averages_json = await get_moving_averages_json_cn(
        stock_info,
        ["date", "close_5_sma", "close_10_ema", "close_20_sma", "close_60_sma",
         "bias_5", "bias_10", "bias_20", "bias_60"],
        120
    )

    # ── 百度搜索近期新闻/公告/事件 ──
    stock_news = await search_stock_news(stock_info, days=7)

    # ── Python 端预计算（核心优化：把容易出错的计算从 LLM 移到代码端）──
    valid_kline = _filter_valid_trading_days(stock_day_kline)
    divergence_result = _compute_macd_divergence(macd_signals_macd, valid_kline)
    macd_bar_trend = _compute_macd_bar_trend(macd_signals_macd)
    kdj_summary = _compute_kdj_summary(kdj_rule_kdj)
    intraday_summary = _compute_intraday_summary(stock_time_kline_10jqka)
    kline_summary = _compute_kline_summary(valid_kline)
    ma_summary = _compute_ma_summary(moving_averages_json)
    latest_close = valid_kline[0]['收盘价'] if valid_kline else 0
    boll_summary = _compute_boll_summary(boll_rule_boll, latest_close)

    # ── 数据一致性校验 & 情景推演参考值 ──
    data_consistency = _compute_data_consistency_check(valid_kline, intraday_summary)
    scenario_ref = _compute_scenario_reference(kline_summary, boll_summary, ma_summary, intraday_summary)
    volume_trend = _compute_volume_trend(valid_kline)

    # ── 大盘指数环境数据 ──
    market_env = await _compute_market_environment(stock_info)

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

1. **直接引用预计算结论**：背离信号、MACD柱趋势、BOLL空间摘要、KDJ状态摘要、分时特征摘要、K线统计摘要、均线排列状态、近期消息面 均已在 Python 端预计算完成，你必须直接引用这些结论，严禁自行重新推导。
2. **禁止计算幻觉**：均线值、乖离率、BOLL距离、MACD柱趋势、KDJ差值、量能倍数等必须直接读取提供的预计算数据，严禁自行做加减乘除运算。
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
{json.dumps(divergence_result, ensure_ascii=False)}

**★ 预计算MACD柱变化趋势（必须直接引用）：**
{json.dumps(macd_bar_trend, ensure_ascii=False)}

请结合MACD明细数据，分析当前多空状态、最近金叉/死叉的质量，并直接引用上述MACD柱趋势结论和背离结论。

#### 2. KDJ（极限与拐点）

**判定规则：**
- 买入：近5日曾超卖(K<20,D<20,J<0) + 金叉 + J勾头向上
- 卖出(钝化)：K>80连续N天 + 跌破MA5/MA20 → 钝化出局
- 卖出(普通)：非钝化 + 近5日曾超买(K>80,D>80,J>100) + 死叉

**★ 预计算KDJ状态摘要（必须直接引用）：**
{json.dumps(kdj_summary, ensure_ascii=False)}

请基于上述摘要分析KDJ当前位置、是否存在买卖信号、拐头方向。

#### 3. BOLL（空间与边界）

**判定规则：**
- 强势开启：放量突破中轨（昨收<=昨中轨 且 今收>今中轨 且 量>50日均量×1.5）
- 波段结束：跌破中轨（昨收>=昨中轨 且 今收<今中轨）
- 可操作区：收盘>中轨 且 中轨向上倾斜
- 喇叭口加速：上下轨反向张开 且 带宽单日放大超10%

**★ 预计算BOLL空间摘要（必须直接引用）：**
{json.dumps(boll_summary, ensure_ascii=False)}

请结合BOLL明细数据和K线统计中的放量突破阈值，分析当前轨道位置、突破质量、运行空间。直接引用上述距离数据，严禁自行计算。

**★ 预计算数据一致性校验（日线 vs 分时）：**
{json.dumps(data_consistency, ensure_ascii=False)}

### 二、 中期结构与均线系统（120日大局观）

**★ 预计算均线状态（必须直接引用）：**
{json.dumps(ma_summary, ensure_ascii=False)}

**★ 预计算K线统计摘要（必须直接引用）：**
{json.dumps(kline_summary, ensure_ascii=False)}

**★ 预计算量能趋势分析（必须直接引用）：**
{json.dumps(volume_trend, ensure_ascii=False)}

请基于上述预计算结论，分析均线排列、乖离率风险、量价匹配、量能趋势、支撑压力位。严禁自行计算均线值和量能变化。

### 三、 今日分时盘口深度解析（超短线博弈）

**★ 预计算分时特征摘要（必须直接引用）：**
{json.dumps(intraday_summary, ensure_ascii=False)}

**★ 资金流向数据（主力净流入/流出、大单/中单/小单占比：**
{json.dumps(real_main_fund_flow, ensure_ascii=False)}

### 四、大机构数据
** 北向资金 (Northbound Capital)近期增减持记录: 香港过来的外资，通常被视为"聪明钱"的风向标 **
{json.dumps(northbound_funds_cn, ensure_ascii=False)}

请基于上述摘要分析：
- 黄白线格局（白线在黄线上方占比已给出）
- 各时段量价分布特征
- 脉冲式放量事件的含义
- 尾盘资金动向

### 五、 大盘环境（系统性风险判断）

**★ 预计算大盘指数走势摘要（必须直接引用）：**
{json.dumps(market_env, ensure_ascii=False)}

请基于上述大盘数据，判断当前市场系统性风险水平，以及个股走势是跟随大盘还是独立行情。大盘环境判断应纳入多空博弈清单和综合评分。

### 六、 近期消息面（百度搜索，近7日）

**★ 以下新闻/公告/事件由百度搜索自动获取，仅供参考，请结合技术面综合判断：**

{format_news_for_prompt(stock_news)}

请基于上述消息面信息，判断是否存在影响股价的重大利好/利空事件，并在多空博弈清单中体现。若无重大消息则简要说明"消息面平淡"。

### 七、 多空力量博弈清单
以列表形式，客观陈述当前盘面的核心利多与利空因素：
- **多方筹码（有利因素）**：[✅] （提炼3-5个核心数据支撑点，每条必须引用具体数值）
- **空方筹码（不利因素）**：[❌] （提炼3-5个核心风险警示点，每条必须引用具体数值）

### 八、 综合评分体系（满分100分）

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

### 九、 明日实战操作策略（Strategy）

#### 1. 操作定调
[明确：积极买入 / 逢低建仓 / 持股待涨 / 逢高减仓 / 清仓离场 / 保持观望]

#### 2. 核心操盘点位（精确到±0.5元）

**★ 预计算情景推演参考值（必须直接引用，严禁自行计算阈值）：**
{json.dumps(scenario_ref, ensure_ascii=False)}

- **重要压力位**：[阻力位1] / [阻力位2]（必须基于上述关键压力位参考数据）
- **强支撑位**：[支撑位1] / [支撑位2]（必须基于上述关键支撑位参考数据）

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

### 十、 盘中关键观察哨（明日盯盘重点）

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
