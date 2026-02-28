import json
import logging
import math
from datetime import datetime, timedelta

import chinese_calendar
import pandas as pd

from common.utils.stock_info_utils import StockInfo, get_stock_info_by_name
from service.eastmoney.stock_info.stock_day_kline_data import get_stock_day_kline_cn
from service.eastmoney.stock_info.stock_northbound_funds import get_northbound_funds_cn

logger = logging.getLogger(__name__)
from service.eastmoney.stock_info.stock_real_fund_flow import get_real_main_fund_flow
from service.eastmoney.strategy_engine.stock_BOLL_rule import get_boll_rule_boll_only
from service.eastmoney.strategy_engine.stock_KDJ_rule import get_kdj_rule_kdj_only
from service.eastmoney.strategy_engine.stock_MACD_rule import get_macd_signals_macd_only
from service.eastmoney.technical.stock_day_range_kline import get_moving_averages_json_cn
from service.eastmoney.stock_info.stock_billboard_data import get_billboard_json
from service.eastmoney.stock_info.stock_org_realtime import get_org_realtime_snapshot, compute_org_snapshot_summary
from service.eastmoney.stock_info.stock_org_hold_by_sh_sz_hk import get_org_hold_by_sh_sz_hk_rank_cn
from service.jqka10.stock_time_kline_data_10jqka import get_stock_time_kline_cn_10jqka
from service.jqka10.stock_week_kline_data_10jqka import get_stock_week_kline_list_10jqka
from service.web_search.stock_news_search import search_stock_news, format_news_for_prompt
from service.web_search.stock_block_trade_search import search_block_trade, compute_block_trade_summary
from service.sina.stock_order_book_data import get_order_book, compute_order_book_summary


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

    # ── 下一关键阈值（避免LLM自行拍脑袋设定阈值） ──
    latest_k_val = kdj_data.get('K', 0) or 0
    if latest_k_val > 75:
        next_threshold = f"若K跌破75将确认高位回落；若K突破80进入超买区"
    elif latest_k_val < 25:
        next_threshold = f"若K升破25将确认低位回升；若K跌破20进入超卖区"
    elif 40 <= latest_k_val <= 60:
        next_threshold = f"K值处于中性区间，关注方向选择"
    else:
        next_threshold = f"K值={latest_k_val:.1f}，上方关注80超买线，下方关注20超卖线"

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
        '下一关键阈值': next_threshold,
        '近5日明细': recent[:5],
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
    pulse_total_count = len(pulse_events)
    pulse_display_limit = 10
    pulse_events_display = pulse_events[:pulse_display_limit]
    if pulse_events:
        pulse_prices = [e['价格'] for e in pulse_events]
        # 脉冲集中时段（只取首尾脉冲的时间）
        pulse_start = pulse_events[0]['时间']
        pulse_end = pulse_events[-1]['时间']
        # 判断脉冲是否集中在某个时段
        pulse_in_morning = all(e['时间'] <= '10:00' for e in pulse_events)
        pulse_in_afternoon = all(e['时间'] >= '13:00' for e in pulse_events)
        if pulse_in_morning:
            pulse_concentration = f"脉冲集中在早盘（{pulse_start}~{pulse_end}）"
        elif pulse_in_afternoon:
            pulse_concentration = f"脉冲集中在午后（{pulse_start}~{pulse_end}）"
        else:
            pulse_concentration = f"脉冲分散在全天（{pulse_start}~{pulse_end}）"

        pulse_price_range = {
            '脉冲总数': pulse_total_count,
            '明细展示数': min(pulse_total_count, pulse_display_limit),
            '明细截断说明': f"共{pulse_total_count}次脉冲，明细仅展示前{pulse_display_limit}条，汇总基于全部{pulse_total_count}条计算，请以汇总结论为准" if pulse_total_count > pulse_display_limit else f"共{pulse_total_count}次脉冲，明细已完整展示",
            '脉冲最高价': max(pulse_prices),
            '脉冲最低价': min(pulse_prices),
            '脉冲价格跌幅': round(min(pulse_prices) - max(pulse_prices), 2),
            '脉冲集中时段': pulse_concentration,
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
        '脉冲式放量事件': pulse_events_display,
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

    # 近期量能（50日均量）
    vol_50_avg = sum(volumes[:50]) / min(len(volumes), 50) if volumes else 0

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

    # 近5日振幅统计
    recent_5_amplitude = [d.get('振幅(%)', 0) for d in recent_5]
    avg_amplitude_5 = round(sum(recent_5_amplitude) / max(len(recent_5_amplitude), 1), 2)
    max_amplitude_5 = max(recent_5_amplitude) if recent_5_amplitude else 0

    # 最新日成交量与放量阈值的对比（避免 LLM 自行比较）
    latest_vol = latest.get('成交量（手）', 0)
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
        '近5日阳线数': up_days,
        '近5日阴线数': down_days,
        '近5日平均振幅(%)': avg_amplitude_5,
        '近5日最大振幅(%)': max_amplitude_5,
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
        '近5日BIAS极值': _compute_bias_extremes(data_list),
    }
def _compute_bias_extremes(data_list: list[dict]) -> dict:
    """预计算近5日BIAS极值及回归描述，避免LLM自行从明细中查找极值。"""
    recent_5 = data_list[:5]
    if not recent_5:
        return {'描述': '数据不足'}

    bias5_vals = [(d.get('日期', ''), d.get('BIAS5', 0)) for d in recent_5 if d.get('BIAS5') is not None]
    bias10_vals = [(d.get('日期', ''), d.get('BIAS10', 0)) for d in recent_5 if d.get('BIAS10') is not None]

    result = {}
    if bias5_vals:
        max_b5 = max(bias5_vals, key=lambda x: abs(x[1]))
        result['近5日BIAS5极值'] = max_b5[1]
        result['近5日BIAS5极值日期'] = max_b5[0]
        result['当前BIAS5'] = bias5_vals[0][1]
        if abs(max_b5[1]) > abs(bias5_vals[0][1]):
            result['BIAS5回归描述'] = f"BIAS5从{max_b5[0]}的{max_b5[1]}%回归至当前{bias5_vals[0][1]}%"
        else:
            result['BIAS5回归描述'] = f"BIAS5当前{bias5_vals[0][1]}%为近5日极值"

    if bias10_vals:
        max_b10 = max(bias10_vals, key=lambda x: abs(x[1]))
        result['近5日BIAS10极值'] = max_b10[1]
        result['近5日BIAS10极值日期'] = max_b10[0]
        result['当前BIAS10'] = bias10_vals[0][1]
        if abs(max_b10[1]) > abs(bias10_vals[0][1]):
            result['BIAS10回归描述'] = f"BIAS10从{max_b10[0]}的{max_b10[1]}%回归至当前{bias10_vals[0][1]}%"
        else:
            result['BIAS10回归描述'] = f"BIAS10当前{bias10_vals[0][1]}%为近5日极值"

    return result


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

    # ── DIF近期走势描述（避免LLM自行从明细中推导趋势） ──
    dif_values = [(d['日期'], d['DIF']) for d in details[:30] if 'DIF' in d]
    dif_trend_desc = ''
    if len(dif_values) >= 2:
        latest_dif = dif_values[0][1]
        # 找近30日DIF极值
        dif_max = max(dif_values, key=lambda x: x[1])
        dif_min = min(dif_values, key=lambda x: x[1])
        parts = []
        # 描述从极值到当前的变化
        if dif_max[0] != dif_values[0][0]:
            parts.append(f"近30日DIF最高{dif_max[1]}（{dif_max[0]}）")
        if dif_min[0] != dif_values[0][0]:
            parts.append(f"最低{dif_min[1]}（{dif_min[0]}）")
        parts.append(f"当前{latest_dif}")
        # 判断方向
        if len(dif_values) >= 5:
            recent_5_dif = [v[1] for v in dif_values[:5]]
            if all(recent_5_dif[i] >= recent_5_dif[i+1] for i in range(4)):
                parts.append('近5日DIF持续上升')
            elif all(recent_5_dif[i] <= recent_5_dif[i+1] for i in range(4)):
                parts.append('近5日DIF持续下降')
            else:
                parts.append('近5日DIF波动')
        dif_trend_desc = '，'.join(parts)

    return {
        'MACD柱趋势': trend_desc,
        '最新MACD柱': latest_bar,
        '柱色': bar_color,
        '连续放大天数': expanding_days,
        '连续收窄天数': shrinking_days,
        '近3日MACD柱': recent_bars,
        'DIF近期走势': dif_trend_desc if dif_trend_desc else '数据不足',
    }


def _compute_golden_cross_quality(macd_data: dict, kline_data: list[dict]) -> dict:
    """
    预计算最近一次金叉的质量评估，避免 LLM 自行推导。

    评估维度：
    1. 零轴位置：零轴上金叉（强）> 零轴附近金叉（中）> 零轴下金叉（弱）
    2. 金叉力度：DIF 上穿 DEA 的速度（金叉后3日 DIF-DEA 扩张幅度）
    3. 量能配合：金叉当日及后续3日成交量 vs 50日均量
    4. 前空头波段特征：空头持续天数、DIF最低值（越深反弹空间越大）
    5. 金叉后存活状态：金叉后是否已被死叉覆盖
    """
    details = macd_data.get('明细数据', [])
    golden_crosses = macd_data.get('近期金叉（最近3次）', macd_data.get('近期金叉（最近2次）', []))

    if not golden_crosses:
        return {'金叉质量': '近期无金叉', '质量评级': '无'}

    latest_gc = golden_crosses[0]  # 最近一次金叉
    gc_date = latest_gc['日期']
    gc_dif = latest_gc['DIF']
    gc_dea = latest_gc['DEA']

    # ── 1. 零轴位置判定 ──
    if gc_dif > 0 and gc_dea > 0:
        axis_position = '零轴上金叉（强信号）'
        axis_score = 3
    elif gc_dif > 0 and gc_dea <= 0:
        axis_position = '零轴附近金叉（DIF刚上穿零轴，中等信号）'
        axis_score = 2
    elif gc_dif > -0.5 and gc_dea > -0.5:
        axis_position = '零轴略下方金叉（接近零轴，中等偏弱信号）'
        axis_score = 1.5
    else:
        axis_position = '零轴下方金叉（弱信号，仅代表空头动能衰竭）'
        axis_score = 1

    # ── 2. 金叉力度（金叉后DIF-DEA扩张速度）──
    # 在明细数据中找到金叉日及之后的数据
    details_asc = sorted(details, key=lambda x: x['日期'])
    gc_idx = None
    for i, d in enumerate(details_asc):
        if d['日期'] == gc_date:
            gc_idx = i
            break

    spread_expansion = '数据不足'
    spread_score = 1
    if gc_idx is not None:
        post_gc = details_asc[gc_idx:gc_idx + 4]  # 金叉日 + 后3日
        if len(post_gc) >= 2:
            spreads = [round(d['DIF'] - d['DEA'], 4) for d in post_gc]
            initial_spread = spreads[0]
            latest_spread = spreads[-1]
            expansion = round(latest_spread - initial_spread, 4)
            if expansion > 0.5:
                spread_expansion = f'快速扩张（DIF-DEA从{initial_spread}扩至{latest_spread}，+{expansion}）'
                spread_score = 3
            elif expansion > 0.1:
                spread_expansion = f'温和扩张（DIF-DEA从{initial_spread}扩至{latest_spread}，+{expansion}）'
                spread_score = 2
            elif expansion > 0:
                spread_expansion = f'缓慢扩张（DIF-DEA从{initial_spread}扩至{latest_spread}，+{expansion}）'
                spread_score = 1.5
            else:
                spread_expansion = f'扩张乏力（DIF-DEA从{initial_spread}变为{latest_spread}，{expansion}）'
                spread_score = 0.5

    # ── 3. 量能配合 ──
    kline_map = {k['日期']: k for k in kline_data}
    volume_confirm = '无K线数据'
    volume_score = 1

    # 计算50日均量
    valid_klines_sorted = sorted(kline_data, key=lambda x: x['日期'], reverse=True)
    if len(valid_klines_sorted) >= 50:
        avg_vol_50 = sum(k.get('成交量（手）', 0) for k in valid_klines_sorted[:50]) / 50
    elif len(valid_klines_sorted) >= 20:
        avg_vol_50 = sum(k.get('成交量（手）', 0) for k in valid_klines_sorted[:20]) / 20
    else:
        avg_vol_50 = 0

    if gc_date in kline_map and avg_vol_50 > 0:
        gc_vol = kline_map[gc_date].get('成交量（手）', 0)
        vol_ratio = round(gc_vol / avg_vol_50, 2)
        # 金叉后3日平均量
        post_gc_dates = [d['日期'] for d in details_asc[gc_idx:gc_idx + 4]] if gc_idx is not None else []
        post_vols = [kline_map[d].get('成交量（手）', 0) for d in post_gc_dates if d in kline_map]
        avg_post_vol = round(sum(post_vols) / len(post_vols), 0) if post_vols else 0
        avg_post_ratio = round(avg_post_vol / avg_vol_50, 2) if avg_vol_50 > 0 else 0

        if vol_ratio >= 1.5:
            volume_confirm = f'金叉当日放量确认（成交量为50日均量的{vol_ratio}倍）'
            volume_score = 3
        elif vol_ratio >= 1.0:
            volume_confirm = f'金叉当日量能温和（成交量为50日均量的{vol_ratio}倍）'
            volume_score = 2
        else:
            volume_confirm = f'金叉当日缩量（成交量仅为50日均量的{vol_ratio}倍，量能不足）'
            volume_score = 1

        if avg_post_ratio >= 1.3:
            volume_confirm += f'，金叉后持续放量（后续均量为50日均量的{avg_post_ratio}倍）'
            volume_score = min(volume_score + 0.5, 3)
        elif avg_post_ratio < 0.8:
            volume_confirm += f'，金叉后量能萎缩（后续均量仅为50日均量的{avg_post_ratio}倍）'
            volume_score = max(volume_score - 0.5, 0.5)

    # ── 4. 前空头波段特征 ──
    bear_duration = 0
    bear_dif_min = 0
    bear_desc = '无法判断'
    bear_score = 1

    if gc_idx is not None and gc_idx > 0:
        # 向前回溯找空头波段（DIF < DEA 的连续区间）
        bear_days = []
        for i in range(gc_idx - 1, -1, -1):
            d = details_asc[i]
            if d['DIF'] <= d['DEA']:
                bear_days.append(d)
            else:
                break
        bear_duration = len(bear_days)
        if bear_days:
            bear_dif_min = round(min(d['DIF'] for d in bear_days), 4)
            if bear_duration >= 20:
                bear_desc = f'长期空头波段（{bear_duration}日），DIF最低{bear_dif_min}，空头充分释放'
                bear_score = 3
            elif bear_duration >= 10:
                bear_desc = f'中期空头波段（{bear_duration}日），DIF最低{bear_dif_min}'
                bear_score = 2
            else:
                bear_desc = f'短期空头波段（{bear_duration}日），DIF最低{bear_dif_min}，调整不充分'
                bear_score = 1

    # ── 5. 金叉存活状态 ──
    death_crosses = macd_data.get('近期死叉（最近3次）', macd_data.get('近期死叉（最近2次）', []))
    is_alive = True
    killed_by = None
    if death_crosses:
        for dc in death_crosses:
            if dc['日期'] > gc_date:
                is_alive = False
                killed_by = dc['日期']
                break

    if is_alive:
        # 计算金叉存活天数
        alive_days = 0
        if gc_idx is not None:
            alive_days = len(details_asc) - gc_idx
        survival = f'金叉仍有效（已存活{alive_days}个交易日）'
        survival_score = 2
    else:
        survival = f'金叉已失效（被{killed_by}死叉覆盖）'
        survival_score = 0

    # ── 综合质量评级 ──
    total_score = axis_score + spread_score + volume_score + bear_score + survival_score
    max_score = 3 + 3 + 3 + 3 + 2  # 14分满分
    quality_pct = round(total_score / max_score * 100, 1)

    if quality_pct >= 75:
        quality_grade = '高质量金叉'
    elif quality_pct >= 50:
        quality_grade = '中等质量金叉'
    elif quality_pct >= 30:
        quality_grade = '低质量金叉'
    else:
        quality_grade = '无效金叉'

    return {
        '最近金叉日期': gc_date,
        '金叉时DIF': gc_dif,
        '金叉时DEA': gc_dea,
        '零轴位置': axis_position,
        '金叉力度': spread_expansion,
        '量能配合': volume_confirm,
        '前空头波段': bear_desc,
        '前空头波段天数': bear_duration,
        '前空头DIF最低值': bear_dif_min,
        '金叉存活状态': survival,
        '质量评级': quality_grade,
        '质量得分': f'{total_score}/{max_score}（{quality_pct}%）',
        '分项得分': {
            '零轴位置': f'{axis_score}/3',
            '金叉力度': f'{spread_score}/3',
            '量能配合': f'{volume_score}/3',
            '前空头波段充分度': f'{bear_score}/3',
            '金叉存活': f'{survival_score}/2',
        },
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

    # 风险收益比（避免LLM自行用距上轨/距中轨做除法）
    if dist_mid_pct != 0 and dist_upper_pct != 0:
        risk_reward = round(dist_upper_pct / abs(dist_mid_pct), 1) if dist_mid_pct > 0 else 0
        risk_reward_desc = f"上方空间{dist_upper_pct}% vs 下方至中轨{abs(dist_mid_pct)}%，风险收益比约1:{risk_reward}" if risk_reward > 0 else "价格在中轨下方，风险收益比不利"
    else:
        risk_reward = 0
        risk_reward_desc = '数据不足'

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
        '风险收益比': risk_reward_desc,
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
    morning_minutes = 0
    for seg in segments:
        if '早盘' in seg.get('时段', ''):
            morning_vol = seg.get('总成交量', 0)
            morning_minutes = 30
            break

    # 早盘分钟均量（手），统一单位避免LLM自行换算
    morning_minute_avg = round(morning_vol / morning_minutes) if morning_minutes > 0 else 0

    ma5 = ma_summary.get('MA5', 0)
    ma20 = ma_summary.get('MA20', 0)
    ma60 = ma_summary.get('MA60', 0)
    boll_mid = boll_summary.get('BOLL中轨', 0)
    boll_upper = boll_summary.get('BOLL上轨', 0)
    boll_lower = boll_summary.get('BOLL下轨', 0)
    latest_close = boll_summary.get('收盘价', 0)

    # ── 心理价位 & 前日开盘价（避免LLM自行推导阻力位） ──
    latest_kline_open = 0
    if isinstance(intraday_summary, dict):
        # 分时数据中的开盘价即为今日开盘价
        open_change = intraday_summary.get('开盘涨跌幅')
        if open_change is not None and latest_close:
            close_change = intraday_summary.get('收盘涨跌幅', 0) or 0
            # 反推昨收 = 今收 / (1 + 收盘涨跌幅/100)
            if close_change != -100:
                prev_close = round(latest_close / (1 + close_change / 100), 2)
                latest_kline_open = round(prev_close * (1 + open_change / 100), 2)

    # 整数关口
    if latest_close:
        round_up = math.ceil(latest_close / 5) * 5  # 向上取整到5的倍数
        round_down = math.floor(latest_close / 5) * 5  # 向下取整到5的倍数
    else:
        round_up = round_down = 0

    return {
        '放量标准（日线）': f"日成交量>{breakout_threshold}手（50日均量{vol_50_avg}手×1.5）",
        '今日早盘30分钟总量（手）': morning_vol,
        '今日早盘分钟均量（手）': morning_minute_avg,
        '今日全天分钟均量（手）': minute_avg_vol,
        '关键压力位参考': {
            '今日开盘价': latest_kline_open,
            '整数关口（上方）': round_up,
            'MA20': ma20,
            'BOLL上轨': boll_upper,
            '120日最高价': kline_summary.get('120日最高价', 0),
        },
        '关键支撑位参考': {
            '整数关口（下方）': round_down,
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

    # ── 量价形态判断（避免LLM自行推导"放量冲高缩量回落"等模式） ──
    vol_price_pattern = ''
    if len(daily_vol_changes) >= 2:
        d0 = daily_vol_changes[0]  # 最新日
        d1 = daily_vol_changes[1]  # 前一日
        # 放量冲高缩量回落
        if d1['放量/缩量'] == '放量' and d1['涨跌幅(%)'] > 0 and d0['放量/缩量'] == '缩量' and d0['涨跌幅(%)'] < 0:
            vol_price_pattern = f"放量冲高缩量回落（前日放量{d1['较前日变化(%)']:+.1f}%涨{d1['涨跌幅(%)']:.2f}%，最新日缩量{d0['较前日变化(%)']:+.1f}%跌{d0['涨跌幅(%)']:.2f}%）"
        # 放量突破
        elif d0['放量/缩量'] == '放量' and d0['涨跌幅(%)'] > 2:
            vol_price_pattern = f"放量上攻（最新日放量{d0['较前日变化(%)']:+.1f}%涨{d0['涨跌幅(%)']:.2f}%）"
        # 放量下跌
        elif d0['放量/缩量'] == '放量' and d0['涨跌幅(%)'] < -2:
            vol_price_pattern = f"放量杀跌（最新日放量{d0['较前日变化(%)']:+.1f}%跌{d0['涨跌幅(%)']:.2f}%）"
        # 缩量上涨
        elif d0['放量/缩量'] == '缩量' and d0['涨跌幅(%)'] > 0:
            vol_price_pattern = f"缩量反弹（最新日缩量{d0['较前日变化(%)']:+.1f}%涨{d0['涨跌幅(%)']:.2f}%）"
        # 缩量下跌
        elif d0['放量/缩量'] == '缩量' and d0['涨跌幅(%)'] < 0:
            vol_price_pattern = f"缩量阴跌（最新日缩量{d0['较前日变化(%)']:+.1f}%跌{d0['涨跌幅(%)']:.2f}%）"

    return {
        '近5日量能趋势': vol_trend_5d,
        '近5日逐日量能变化': daily_vol_changes,
        '量价形态': vol_price_pattern if vol_price_pattern else '无明显量价形态',
        '近10日量价配合度': f"{match_ratio}%（{vol_price_match}天同向/{vol_price_diverge}天背离）",
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

        # 近5日明细（供验证，剔除成交额以减少token）
        recent_5_detail = []
        for d in kline_data[:5]:
            recent_5_detail.append({
                '日期': d['日期'],
                '收盘价': d['收盘价'],
                '涨跌幅(%)': d.get('涨跌幅(%)', 0),
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
        logger.warning(f"获取指数数据失败 [{index_name}]: {e}")
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






def _compute_fund_flow_behavior(fund_flow: list[dict]) -> dict:
    """预计算资金流向行为特征，避免LLM自行推断'主力出货散户接盘'等模式。"""
    if not fund_flow:
        return {'资金行为特征': '无资金流向数据'}

    latest = fund_flow[0] if isinstance(fund_flow, list) else fund_flow

    # 解析主力净流入（可能是字符串带"亿"）
    def _parse_amount(val):
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            val = val.replace('亿', '').replace(',', '').strip()
            try:
                return float(val)
            except ValueError:
                return 0
        return 0

    main_net = _parse_amount(latest.get('主力净流入', 0))
    small_net = _parse_amount(latest.get('小单净流入', 0))
    super_large_net = _parse_amount(latest.get('超大单净流入', 0))

    # 行为模式判断
    if main_net < -1 and small_net > 1:
        behavior = f"主力净流出{latest.get('主力净流入', '')}，小单净流入{latest.get('小单净流入', '')}，呈现主力减仓散户承接格局"
    elif main_net > 1 and small_net < -1:
        behavior = f"主力净流入{latest.get('主力净流入', '')}，小单净流出{latest.get('小单净流入', '')}，呈现主力吸筹格局"
    elif main_net < -1:
        behavior = f"主力净流出{latest.get('主力净流入', '')}，资金整体流出"
    elif main_net > 1:
        behavior = f"主力净流入{latest.get('主力净流入', '')}，资金整体流入"
    else:
        behavior = f"主力净流入{latest.get('主力净流入', '')}，资金流向不明显"

    return {
        '资金行为特征': behavior,
        '主力净流入': latest.get('主力净流入', ''),
        '主力净流入占比': latest.get('主力净流入占比', ''),
        '超大单净流入': latest.get('超大单净流入', ''),
        '超大单净比': latest.get('超大单净比', ''),
        '大单净流入': latest.get('大单净流入', ''),
        '大单净比': latest.get('大单净比', ''),
        '小单净流入': latest.get('小单净流入', ''),
        '小单净比': latest.get('小单净比', ''),
        '成交额': latest.get('成交额', ''),
    }




def _compute_weekly_kline_summary(weekly_kline: list[dict]) -> dict:
    """预计算周线级别趋势摘要，提供更大周期的趋势判断"""
    if not weekly_kline:
        return {'状态': '无周线数据'}

    # 取最近的数据（数据由旧到新排列，取末尾）
    recent = weekly_kline[-20:] if len(weekly_kline) >= 20 else weekly_kline
    latest = recent[-1]
    latest_close = latest.get('收盘', 0)

    # 周线MA计算（5周、10周、20周）
    closes = [k.get('收盘', 0) for k in weekly_kline if k.get('收盘')]
    ma5w = round(sum(closes[-5:]) / min(len(closes[-5:]), 5), 2) if len(closes) >= 5 else None
    ma10w = round(sum(closes[-10:]) / min(len(closes[-10:]), 10), 2) if len(closes) >= 10 else None
    ma20w = round(sum(closes[-20:]) / min(len(closes[-20:]), 20), 2) if len(closes) >= 20 else None

    # 周线均线排列
    if ma5w and ma10w and ma20w:
        if ma5w > ma10w > ma20w:
            weekly_alignment = '周线多头排列（MA5W>MA10W>MA20W）'
        elif ma5w < ma10w < ma20w:
            weekly_alignment = '周线空头排列（MA5W<MA10W<MA20W）'
        else:
            weekly_alignment = '周线均线纠缠'
    elif ma5w and ma10w:
        weekly_alignment = f"周线MA5W={'>' if ma5w > ma10w else '<'}MA10W（数据不足20周）"
    else:
        weekly_alignment = '周线数据不足'

    # 近4周涨跌统计
    recent_4 = recent[-4:] if len(recent) >= 4 else recent
    up_weeks = sum(1 for k in recent_4 if (k.get('涨跌幅(%)') or 0) > 0)
    down_weeks = sum(1 for k in recent_4 if (k.get('涨跌幅(%)') or 0) < 0)
    recent_4_change = sum(k.get('涨跌幅(%)', 0) or 0 for k in recent_4)

    # 近4周量能趋势
    recent_4_vols = [k.get('成交量', 0) for k in recent_4 if k.get('成交量')]
    if len(recent_4_vols) >= 2:
        vol_trend_parts = []
        for i in range(1, len(recent_4_vols)):
            if recent_4_vols[i - 1] > 0:
                change = round((recent_4_vols[i] - recent_4_vols[i - 1]) / recent_4_vols[i - 1] * 100, 1)
                vol_trend_parts.append(change)
        if all(v > 0 for v in vol_trend_parts):
            weekly_vol_trend = '周线连续放量'
        elif all(v < 0 for v in vol_trend_parts):
            weekly_vol_trend = '周线连续缩量'
        else:
            weekly_vol_trend = '周线量能不规则'
    else:
        weekly_vol_trend = '周线量能数据不足'

    # 周线级别高低点
    recent_20_highs = [k.get('最高', 0) for k in recent if k.get('最高')]
    recent_20_lows = [k.get('最低', 0) for k in recent if k.get('最低')]
    week_high = max(recent_20_highs) if recent_20_highs else None
    week_low = min(recent_20_lows) if recent_20_lows else None

    # 最新周K线形态
    latest_open = latest.get('开盘', 0)
    latest_high = latest.get('最高', 0)
    latest_low = latest.get('最低', 0)
    body = abs(latest_close - latest_open) if latest_close and latest_open else 0
    upper_shadow = (latest_high - max(latest_close, latest_open)) if latest_high else 0
    lower_shadow = (min(latest_close, latest_open) - latest_low) if latest_low else 0

    # 近4周逐周明细
    weekly_details = []
    for k in recent_4:
        weekly_details.append({
            '日期': k.get('日期', ''),
            '开盘': k.get('开盘'),
            '收盘': k.get('收盘'),
            '最高': k.get('最高'),
            '最低': k.get('最低'),
            '涨跌幅(%)': k.get('涨跌幅(%)'),
            '成交量': k.get('成交量'),
            '换手率(%)': k.get('换手率(%)'),
        })

    return {
        '周线均线排列': weekly_alignment,
        'MA5W': ma5w,
        'MA10W': ma10w,
        'MA20W': ma20w,
        '最新周收盘价': latest_close,
        '最新周涨跌幅': latest.get('涨跌幅(%)'),
        '近4周涨跌': f"{up_weeks}阳{down_weeks}阴，累计涨跌{round(recent_4_change, 2)}%",
        '近4周量能趋势': weekly_vol_trend,
        '近20周最高价': week_high,
        '近20周最低价': week_low,
        '当前价距20周高点': f"{round((latest_close - week_high) / week_high * 100, 2)}%" if week_high and latest_close else None,
        '当前价距20周低点': f"{round((latest_close - week_low) / week_low * 100, 2)}%" if week_low and latest_close else None,
        '最新周K线': {
            '实体': round(body, 2),
            '上影线': round(upper_shadow, 2),
            '下影线': round(lower_shadow, 2),
            '阴阳': '阳线' if latest_close >= latest_open else '阴线',
        },
        '近4周明细': weekly_details,
    }


def _compute_billboard_summary(billboard_data: list[dict]) -> dict:
    """预计算龙虎榜摘要，提取机构/游资/北向资金的买卖行为特征"""
    if not billboard_data:
        return {'状态': '近期无龙虎榜上榜记录', '上榜次数': 0}

    total_count = len(billboard_data)
    latest = billboard_data[0]

    # 统计各类席位的净买卖
    org_net_buy = 0  # 机构净买
    org_net_sell = 0
    north_net_buy = 0  # 北向净买
    north_net_sell = 0
    hot_money_net_buy = 0  # 游资净买
    hot_money_net_sell = 0

    all_entries_summary = []
    for entry in billboard_data:
        buy_seats = entry.get('买入席位', [])
        sell_seats = entry.get('卖出席位', [])

        entry_org_buy = 0
        entry_org_sell = 0
        entry_north_buy = 0
        entry_north_sell = 0
        entry_hot_buy = 0
        entry_hot_sell = 0

        for s in buy_seats:
            seat_type = s.get('类型', '游资')
            # 净额字段可能是字符串如"1.23亿"，这里直接保留原始席位信息
            if seat_type == '机构':
                entry_org_buy += 1
            elif seat_type == '北向资金':
                entry_north_buy += 1
            else:
                entry_hot_buy += 1

        for s in sell_seats:
            seat_type = s.get('类型', '游资')
            if seat_type == '机构':
                entry_org_sell += 1
            elif seat_type == '北向资金':
                entry_north_sell += 1
            else:
                entry_hot_sell += 1

        org_net_buy += entry_org_buy
        org_net_sell += entry_org_sell
        north_net_buy += entry_north_buy
        north_net_sell += entry_north_sell
        hot_money_net_buy += entry_hot_buy
        hot_money_net_sell += entry_hot_sell

        all_entries_summary.append({
            '上榜日期': entry.get('上榜日期', ''),
            '上榜原因': entry.get('上榜原因', ''),
            '涨跌幅(%)': entry.get('涨跌幅(%)', 0),
            '龙虎榜净买额': entry.get('龙虎榜净买额', '--'),
            '龙虎榜买入额': entry.get('龙虎榜买入额', '--'),
            '龙虎榜卖出额': entry.get('龙虎榜卖出额', '--'),
            '龙虎榜净买占比(%)': entry.get('龙虎榜净买占比(%)', 0),
            '次日涨跌(%)': entry.get('次日涨跌(%)', None),
            '5日涨跌(%)': entry.get('5日涨跌(%)', None),
            '买入席位机构数': entry_org_buy,
            '买入席位游资数': entry_hot_buy,
            '买入席位北向数': entry_north_buy,
            '卖出席位机构数': entry_org_sell,
            '卖出席位游资数': entry_hot_sell,
            '卖出席位北向数': entry_north_sell,
        })

    # 整体行为判断
    if org_net_buy > org_net_sell:
        org_behavior = f"机构偏买入（买入席位{org_net_buy}次 vs 卖出席位{org_net_sell}次）"
    elif org_net_buy < org_net_sell:
        org_behavior = f"机构偏卖出（买入席位{org_net_buy}次 vs 卖出席位{org_net_sell}次）"
    else:
        org_behavior = f"机构买卖均衡（买入席位{org_net_buy}次 vs 卖出席位{org_net_sell}次）" if org_net_buy > 0 else "无机构席位参与"

    return {
        '上榜次数': total_count,
        '最近上榜日期': latest.get('上榜日期', ''),
        '机构席位行为': org_behavior,
        '游资买入席位总次数': hot_money_net_buy,
        '游资卖出席位总次数': hot_money_net_sell,
        '北向资金买入席位总次数': north_net_buy,
        '北向资金卖出席位总次数': north_net_sell,
        '各次上榜摘要': all_entries_summary,
        '最近一次席位明细': {
            '买入席位': latest.get('买入席位', []),
            '卖出席位': latest.get('卖出席位', []),
        },
    }

def _compute_northbound_summary(northbound_data: list[dict]) -> dict:
    """预计算北向资金增减持摘要，避免LLM自行计算连续变化趋势。"""
    if not northbound_data:
        return {'状态': '未获取到北向资金数据'}

    total_count = len(northbound_data)

    # 统计连续增减持
    increase_days = 0
    decrease_days = 0
    for item in northbound_data:
        amp = item.get('增持幅度', 0) or 0
        if amp > 0:
            if decrease_days == 0:
                increase_days += 1
            else:
                break
        elif amp < 0:
            if increase_days == 0:
                decrease_days += 1
            else:
                break
        else:
            break

    # 近N日增持幅度汇总
    amp_list = [item.get('增持幅度', 0) or 0 for item in northbound_data]
    total_amp = round(sum(amp_list), 2)
    avg_amp = round(total_amp / total_count, 2) if total_count else 0

    # 增减持方向判断
    if increase_days >= 3:
        direction = f"北向资金连续{increase_days}日增持，累计增持幅度{total_amp}%"
    elif decrease_days >= 3:
        direction = f"北向资金连续{decrease_days}日减持，累计增持幅度{total_amp}%"
    elif total_amp > 0:
        direction = f"北向资金近{total_count}日整体增持，累计增持幅度{total_amp}%"
    elif total_amp < 0:
        direction = f"北向资金近{total_count}日整体减持，累计增持幅度{total_amp}%"
    else:
        direction = "北向资金近期增减持幅度极小，方向不明"

    # 最新一日
    latest = northbound_data[0]
    latest_desc = f"最新交易日{latest.get('交易日期', '--')}增持市值{latest.get('增持市值', '--')}，增持幅度{latest.get('增持幅度', 0)}%"

    return {
        '方向判断': direction,
        '最新一日': latest_desc,
        '连续增持天数': increase_days,
        '连续减持天数': decrease_days,
        f'近{total_count}日累计增持幅度(%)': total_amp,
        f'近{total_count}日日均增持幅度(%)': avg_amp,
        '逐日增持幅度(%)': amp_list,
    }


def _compute_sh_sz_hk_hold_summary(sh_sz_hk_data: list[dict]) -> dict:
    """预计算沪深港通持股变化趋势摘要，避免LLM自行推导连续变化方向。"""
    if not sh_sz_hk_data:
        return {'状态': '未获取到沪深港通持股数据'}

    total_count = len(sh_sz_hk_data)

    # 提取持股数量和占流通股比的变化序列
    hold_shares_list = []
    ratio_list = []
    for item in sh_sz_hk_data:
        hs = item.get('持股数量（万股）')
        ratio = item.get('占流通股比')
        hold_shares_list.append(hs)
        # 占流通股比可能是 "3.45%" 格式
        if isinstance(ratio, str) and ratio.endswith('%'):
            try:
                ratio_list.append(float(ratio.replace('%', '')))
            except ValueError:
                ratio_list.append(None)
        elif isinstance(ratio, (int, float)):
            ratio_list.append(float(ratio))
        else:
            ratio_list.append(None)

    # 统计连续增减持（基于增持数量字段）
    increase_days = 0
    decrease_days = 0
    for item in sh_sz_hk_data:
        change = item.get('增持数量（万股）')
        change_val = _parse_hold_change(change)
        if change_val > 0:
            if decrease_days == 0:
                increase_days += 1
            else:
                break
        elif change_val < 0:
            if increase_days == 0:
                decrease_days += 1
            else:
                break
        else:
            break

    # 占流通股比变化趋势
    valid_ratios = [r for r in ratio_list if r is not None]
    if len(valid_ratios) >= 2:
        latest_ratio = valid_ratios[0]
        oldest_ratio = valid_ratios[-1]
        ratio_change = round(latest_ratio - oldest_ratio, 2)
        if ratio_change > 0:
            ratio_trend = f"占流通股比从{oldest_ratio}%升至{latest_ratio}%（+{ratio_change}pp），持股比例上升"
        elif ratio_change < 0:
            ratio_trend = f"占流通股比从{oldest_ratio}%降至{latest_ratio}%（{ratio_change}pp），持股比例下降"
        else:
            ratio_trend = f"占流通股比持平于{latest_ratio}%"
    else:
        ratio_trend = "数据不足，无法判断趋势"

    # 方向判断
    if increase_days >= 3:
        direction = f"沪深港通连续{increase_days}日增持，{ratio_trend}"
    elif decrease_days >= 3:
        direction = f"沪深港通连续{decrease_days}日减持，{ratio_trend}"
    elif increase_days > decrease_days:
        direction = f"沪深港通近期偏增持（连续增持{increase_days}日），{ratio_trend}"
    elif decrease_days > increase_days:
        direction = f"沪深港通近期偏减持（连续减持{decrease_days}日），{ratio_trend}"
    else:
        direction = f"沪深港通增减持方向不明，{ratio_trend}"

    # 最新一日
    latest = sh_sz_hk_data[0]

    return {
        '方向判断': direction,
        '最新交易日': latest.get('交易日期', '--'),
        '最新持股数量': latest.get('持股数量（万股）', '--'),
        '最新占流通股比': latest.get('占流通股比', '--'),
        '最新增持数量': latest.get('增持数量（万股）', '--'),
        '连续增持天数': increase_days,
        '连续减持天数': decrease_days,
        '占流通股比变化趋势': ratio_trend,
    }


def _parse_hold_change(change) -> float:
    """解析持股变化值，支持数值和字符串格式"""
    if isinstance(change, (int, float)):
        return float(change)
    if isinstance(change, str):
        cleaned = change.replace('万股', '').replace(',', '').strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0




# ──────────────────────────────────────────────
# 数据精简函数
# ──────────────────────────────────────────────


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
# 综合评分函数（基于行业共识规则，Python端预计算）
# ──────────────────────────────────────────────


def _compute_comprehensive_score(
    macd_data: dict,
    macd_bar_trend: dict,
    divergence_result: dict,
    golden_cross_quality: dict,
    kdj_summary: dict,
    boll_summary: dict,
    ma_summary: dict,
    kline_summary: dict,
    volume_trend: dict,
    weekly_kline_summary: dict,
    intraday_summary: dict,
    fund_flow_behavior: dict,
    order_book_summary: dict,
    northbound_summary: dict,
    sh_sz_hk_summary: dict,
    org_holder_summary: dict,
    billboard_summary: dict,
    block_trade_summary: dict,
    market_env: dict,
    news_data: list,
) -> dict:
    """
    基于行业共识规则的综合评分体系（满分100分），7个维度。

    所有评分规则均基于A股技术分析行业共识：
    - MACD：零轴位置决定趋势强度，金叉/死叉质量影响信号可靠性
    - KDJ：超买超卖区间（20/80）为国际通用阈值，钝化需特殊处理
    - BOLL：中轨方向决定可操作性，带宽变化反映波动率周期
    - 量价关系：放量突破为有效突破的必要条件（1.5倍均量为行业惯例）
    - 均线系统：多头/空头排列为趋势判定的基础框架
    - 资金流向：主力资金方向为短期最核心变量
    """

    scores = {}
    details = {}

    # ════════════════════════════════════════════
    # 维度1：趋势强度（满分20分）
    # ════════════════════════════════════════════
    trend_score = 10  # 基准分
    trend_reasons = []

    # --- MACD位置与方向（0~8分）---
    # 行业共识：DIF/DEA均>0为强多头，DIF>0/DEA<0为弱多头，DIF<0为空头
    latest_detail = macd_data.get('明细数据', [{}])[0] if macd_data.get('明细数据') else {}
    market_state = latest_detail.get('市场状态', '')
    if market_state == '强多头':
        trend_score += 4
        trend_reasons.append('MACD强多头（DIF>0且DEA>0）+4')
    elif market_state == '弱多头':
        trend_score += 1
        trend_reasons.append('MACD弱多头（DIF>0但DEA<0）+1')
    elif market_state == '空头':
        trend_score -= 3
        trend_reasons.append('MACD空头（DIF<0）-3')

    # MACD柱方向
    bar_trend = macd_bar_trend.get('MACD柱趋势', '')
    if '红柱' in bar_trend and '放大' in bar_trend:
        trend_score += 2
        trend_reasons.append(f'MACD{bar_trend}+2')
    elif '红柱' in bar_trend and '收窄' in bar_trend:
        trend_score += 0
        trend_reasons.append(f'MACD{bar_trend}+0（多头动能边际减弱）')
    elif '绿柱' in bar_trend and '收窄' in bar_trend:
        trend_score += 1
        trend_reasons.append(f'MACD{bar_trend}+1（空头动能衰竭）')
    elif '绿柱' in bar_trend and '放大' in bar_trend:
        trend_score -= 2
        trend_reasons.append(f'MACD{bar_trend}-2')

    # 金叉质量加减分
    gc_grade = golden_cross_quality.get('质量评级', '')
    if gc_grade == '高质量金叉':
        trend_score += 2
        trend_reasons.append(f'高质量金叉+2（{golden_cross_quality.get("质量得分", "")}）')
    elif gc_grade == '中等质量金叉':
        trend_score += 1
        trend_reasons.append(f'中等质量金叉+1（{golden_cross_quality.get("质量得分", "")}）')
    elif gc_grade == '低质量金叉':
        trend_score += 0
        trend_reasons.append(f'低质量金叉+0（{golden_cross_quality.get("质量得分", "")}）')
    elif gc_grade == '无效金叉':
        trend_score -= 1
        trend_reasons.append(f'无效金叉-1')

    # --- 均线排列（0~4分）---
    # 行业共识：多头排列（MA5>MA10>MA20>MA60）为最强趋势信号
    alignment = ma_summary.get('均线排列状态', '')
    if '多头排列' in alignment:
        trend_score += 4
        trend_reasons.append('日线均线多头排列+4')
    elif '空头排列' in alignment:
        trend_score -= 4
        trend_reasons.append('日线均线空头排列-4')
    else:
        trend_score += 0
        trend_reasons.append(f'日线{alignment}+0')

    # --- 周线趋势及日周共振（-4~+4分）---
    weekly_alignment = weekly_kline_summary.get('周线均线排列', '')
    if '多头排列' in weekly_alignment:
        if '多头排列' in alignment:
            trend_score += 4
            trend_reasons.append('日周均线多头共振+4（强信号）')
        else:
            trend_score += 2
            trend_reasons.append('周线多头但日线未共振+2')
    elif '空头排列' in weekly_alignment:
        if '空头排列' in alignment:
            trend_score -= 4
            trend_reasons.append('日周均线空头共振-4（强空信号）')
        else:
            trend_score -= 2
            trend_reasons.append('周线空头但日线未共振-2')
    else:
        trend_score += 0
        trend_reasons.append(f'{weekly_alignment}+0')

    # --- 背离信号约束 ---
    if divergence_result.get('顶背离'):
        trend_score = min(trend_score, 12)
        trend_reasons.append('★顶背离约束：趋势强度上限12分')
    if divergence_result.get('底背离'):
        trend_score += 2
        trend_reasons.append('底背离信号+2（潜在反转）')

    # --- MACD零轴下死叉+均线空头排列约束 ---
    latest_macd = macd_data.get('明细数据', [{}])[0] if macd_data.get('明细数据') else {}
    is_death_cross_below_zero = (latest_macd.get('DIF', 0) < 0 and
                                  latest_macd.get('DEA', 0) < 0 and
                                  latest_macd.get('MACD柱', 0) < 0)
    if is_death_cross_below_zero and '空头排列' in alignment:
        trend_score = min(trend_score, 5)
        trend_reasons.append('★零轴下方死叉+均线空头排列约束：上限5分')

    # 日线与周线方向相反约束
    daily_bullish = '多头排列' in alignment or market_state in ('强多头', '弱多头')
    weekly_bearish = '空头排列' in weekly_alignment
    daily_bearish = '空头排列' in alignment or market_state == '空头'
    weekly_bullish = '多头排列' in weekly_alignment
    if (daily_bullish and weekly_bearish) or (daily_bearish and weekly_bullish):
        trend_score = min(trend_score, 14)
        trend_reasons.append('★日周趋势方向相反约束：上限14分')

    trend_score = max(0, min(20, trend_score))
    scores['趋势强度'] = trend_score
    details['趋势强度'] = trend_reasons

    # ════════════════════════════════════════════
    # 维度2：动能与量价（满分20分）
    # ════════════════════════════════════════════
    momentum_score = 10  # 基准分
    momentum_reasons = []

    # --- KDJ位置与信号（-4~+6分）---
    # 行业共识：K<20超卖区金叉为买入信号，K>80超买区死叉为卖出信号
    latest_k = kdj_summary.get('最新K', 50)
    latest_j = kdj_summary.get('最新J', 50)
    kdj_turning = kdj_summary.get('KDJ拐头状态', '')
    was_oversold = kdj_summary.get('近5日曾超卖', False)
    was_overbought = kdj_summary.get('近5日曾超买', False)
    is_stale = kdj_summary.get('高位钝化', False)
    k_change = kdj_summary.get('K日变化', 0)
    j_change = kdj_summary.get('J日变化', 0)

    if was_oversold and '低位拐头向上' in kdj_turning:
        momentum_score += 6
        momentum_reasons.append('KDJ超卖区低位拐头向上+6（强买入信号）')
    elif '低位拐头向上' in kdj_turning:
        momentum_score += 3
        momentum_reasons.append('KDJ低位拐头向上+3')
    elif was_overbought and '高位拐头向下' in kdj_turning:
        momentum_score -= 4
        momentum_reasons.append('KDJ超买区高位拐头向下-4（卖出信号）')
    elif '高位拐头向下' in kdj_turning:
        momentum_score -= 2
        momentum_reasons.append(f'KDJ高位拐头向下-2（K={latest_k:.1f}，J日变化{j_change:+.1f}）')
    elif latest_k > 50 and k_change > 0:
        momentum_score += 1
        momentum_reasons.append(f'KDJ中高位上行+1（K={latest_k:.1f}）')
    elif latest_k < 50 and k_change < 0:
        momentum_score -= 1
        momentum_reasons.append(f'KDJ中低位下行-1（K={latest_k:.1f}）')

    # KDJ高位钝化约束
    if is_stale and '高位拐头向下' in kdj_turning:
        momentum_score = min(momentum_score, 8)
        momentum_reasons.append('★KDJ高位钝化拐头向下约束：上限8分')

    # --- 量价配合度（-3~+4分）---
    # 行业共识：量价同向为健康，量价背离为危险信号
    vol_price_pattern = volume_trend.get('量价形态', '')
    vol_trend_5d = volume_trend.get('近5日量能趋势', '')

    if '放量上攻' in vol_price_pattern:
        momentum_score += 4
        momentum_reasons.append(f'量价形态：{vol_price_pattern}+4')
    elif '放量冲高缩量回落' in vol_price_pattern:
        momentum_score -= 1
        momentum_reasons.append(f'量价形态：{vol_price_pattern}-1')
    elif '放量杀跌' in vol_price_pattern:
        momentum_score -= 3
        momentum_reasons.append(f'量价形态：{vol_price_pattern}-3')
    elif '缩量反弹' in vol_price_pattern:
        momentum_score += 0
        momentum_reasons.append(f'量价形态：{vol_price_pattern}+0（反弹力度存疑）')
    elif '缩量阴跌' in vol_price_pattern:
        momentum_score -= 2
        momentum_reasons.append(f'量价形态：{vol_price_pattern}-2')

    # --- 量能趋势（-2~+2分）---
    if '持续放量' in vol_trend_5d or '连续放量' in vol_trend_5d:
        momentum_score += 2
        momentum_reasons.append(f'{vol_trend_5d}+2')
    elif '持续缩量' in vol_trend_5d or '连续缩量' in vol_trend_5d:
        momentum_score -= 2
        momentum_reasons.append(f'{vol_trend_5d}-2')
    else:
        momentum_reasons.append(f'{vol_trend_5d}+0')

    # 持续缩量且无放量突破约束
    latest_vol_vs_threshold = kline_summary.get('最新日成交量vs放量阈值', '')
    if ('缩量' in vol_trend_5d or '萎缩' in vol_trend_5d) and '未达到' in str(latest_vol_vs_threshold):
        momentum_score = min(momentum_score, 10)
        momentum_reasons.append('★持续缩量且未达放量突破标准约束：上限10分')

    momentum_score = max(0, min(20, momentum_score))
    scores['动能与量价'] = momentum_score
    details['动能与量价'] = momentum_reasons

    # ════════════════════════════════════════════
    # 维度3：结构边界（满分15分）
    # ════════════════════════════════════════════
    structure_score = 7  # 基准分
    structure_reasons = []

    # --- BOLL轨道位置（-3~+4分）---
    # 行业共识：收盘>中轨且中轨上倾为可操作区，跌破中轨为波段结束
    boll_position = boll_summary.get('收盘价位置', '')
    mid_direction = boll_summary.get('中轨方向', '')
    dist_to_mid_pct = boll_summary.get('距中轨（%）', 0)
    dist_to_upper_pct = boll_summary.get('距上轨（%）', 0)

    if '上方' in boll_position and '上倾' in mid_direction:
        structure_score += 4
        structure_reasons.append(f'收盘在中轨上方+中轨上倾（可操作区）+4')
    elif '上方' in boll_position and '下倾' in mid_direction:
        structure_score += 1
        structure_reasons.append(f'收盘在中轨上方但中轨下倾+1（突破可靠性存疑）')
    elif '下方' in boll_position and '下倾' in mid_direction:
        structure_score -= 3
        structure_reasons.append(f'收盘在中轨下方+中轨下倾-3（弱势格局）')
    elif '下方' in boll_position:
        structure_score -= 1
        structure_reasons.append(f'收盘在中轨下方-1')

    # --- 乖离率风险（-2~+1分）---
    # 行业共识：BIAS5>7%短线超买，BIAS5<-7%短线超卖
    bias_warning = ma_summary.get('乖离率预警', '')
    if '超买' in bias_warning:
        structure_score -= 2
        structure_reasons.append(f'乖离率超买预警-2（{bias_warning}）')
    elif '超卖' in bias_warning or '深度超跌' in bias_warning:
        structure_score += 1
        structure_reasons.append(f'乖离率超卖/超跌+1（反弹空间）')
    else:
        structure_reasons.append('乖离率正常区间+0')

    # --- 上方空间与风险收益比（-1~+3分）---
    risk_reward = boll_summary.get('风险收益比', '')
    if dist_to_upper_pct and isinstance(dist_to_upper_pct, (int, float)):
        if dist_to_upper_pct > 10:
            structure_score += 3
            structure_reasons.append(f'距上轨{dist_to_upper_pct}%空间充足+3')
        elif dist_to_upper_pct > 5:
            structure_score += 1
            structure_reasons.append(f'距上轨{dist_to_upper_pct}%空间适中+1')
        elif dist_to_upper_pct < 2:
            structure_score -= 1
            structure_reasons.append(f'距上轨仅{dist_to_upper_pct}%空间有限-1')

    structure_score = max(0, min(15, structure_score))
    scores['结构边界'] = structure_score
    details['结构边界'] = structure_reasons

    # ════════════════════════════════════════════
    # 维度4：短线情绪（满分15分）
    # ════════════════════════════════════════════
    sentiment_score = 7  # 基准分
    sentiment_reasons = []

    # --- 分时黄白线格局（-3~+3分）---
    # 行业共识：白线（股价）在黄线（均价）上方表示大资金主导上涨
    above_avg_str = intraday_summary.get('白线在黄线上方占比', '50%')
    above_avg_pct = float(above_avg_str.replace('%', '')) if isinstance(above_avg_str, str) else 50
    if above_avg_pct > 70:
        sentiment_score += 3
        sentiment_reasons.append(f'白线在黄线上方占比{above_avg_str}+3（大资金主导）')
    elif above_avg_pct > 50:
        sentiment_score += 1
        sentiment_reasons.append(f'白线在黄线上方占比{above_avg_str}+1')
    elif above_avg_pct < 30:
        sentiment_score -= 3
        sentiment_reasons.append(f'白线在黄线上方占比{above_avg_str}-3（大资金持续出货）')
    elif above_avg_pct < 50:
        sentiment_score -= 1
        sentiment_reasons.append(f'白线在黄线上方占比{above_avg_str}-1')

    # 白线长期在黄线下方约束（占比>70%在下方 = 上方占比<30%）
    if above_avg_pct < 30:
        sentiment_score = min(sentiment_score, 6)
        sentiment_reasons.append('★白线长期在黄线下方约束：上限6分')

    # --- 资金流向行为特征（-3~+3分）---
    # 行业共识：主力资金方向是短线最核心变量
    fund_behavior = fund_flow_behavior.get('资金行为特征', '')
    main_net_str = fund_flow_behavior.get('主力净流入', '0')

    def _parse_fund_amount(val):
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            cleaned = val.replace('亿', '').replace(',', '').strip()
            try:
                return float(cleaned)
            except ValueError:
                return 0
        return 0

    main_net = _parse_fund_amount(main_net_str)

    if '主力吸筹' in fund_behavior:
        sentiment_score += 3
        sentiment_reasons.append(f'资金流向：{fund_behavior}+3')
    elif '主力减仓散户承接' in fund_behavior:
        sentiment_score -= 3
        sentiment_reasons.append(f'资金流向：{fund_behavior}-3')
    elif main_net < -1:
        sentiment_score -= 2
        sentiment_reasons.append(f'主力净流出{main_net_str}-2')
    elif main_net > 1:
        sentiment_score += 2
        sentiment_reasons.append(f'主力净流入{main_net_str}+2')
    else:
        sentiment_reasons.append(f'资金流向不明显+0')

    # 主力减仓散户承接约束
    if '主力减仓散户承接' in fund_behavior:
        sentiment_score = min(sentiment_score, 7)
        sentiment_reasons.append('★主力减仓散户承接格局约束：上限7分')

    # 尾盘资金净流出+主力为负约束
    if main_net < 0:
        close_change = intraday_summary.get('收盘涨跌幅', 0) or 0
        if close_change < (intraday_summary.get('开盘涨跌幅', 0) or 0):
            sentiment_score = min(sentiment_score, 8)
            sentiment_reasons.append('★尾盘走弱+主力资金为负约束：上限8分')

    # --- 五档盘口（-2~+2分）---
    # 行业共识：买卖力量比>1.5为买盘强势，<0.67为卖盘强势
    buy_sell_ratio = order_book_summary.get('买卖力量比', 1.0)
    if isinstance(buy_sell_ratio, (int, float)):
        if buy_sell_ratio > 1.5:
            sentiment_score += 2
            sentiment_reasons.append(f'五档买卖比{buy_sell_ratio}:1+2（买盘强势）')
        elif buy_sell_ratio > 1.1:
            sentiment_score += 1
            sentiment_reasons.append(f'五档买卖比{buy_sell_ratio}:1+1')
        elif buy_sell_ratio < 0.67:
            sentiment_score -= 2
            sentiment_reasons.append(f'五档买卖比{buy_sell_ratio}:1-2（卖盘强势）')
            # 卖盘明显强于买盘约束
            sentiment_reasons.append('★五档卖盘明显强于买盘约束：额外扣减')
        elif buy_sell_ratio < 0.9:
            sentiment_score -= 1
            sentiment_reasons.append(f'五档买卖比{buy_sell_ratio}:1-1')

    sentiment_score = max(0, min(15, sentiment_score))
    scores['短线情绪'] = sentiment_score
    details['短线情绪'] = sentiment_reasons

    # ════════════════════════════════════════════
    # 维度5：资金筹码（满分15分）
    # ════════════════════════════════════════════
    capital_score = 7  # 基准分
    capital_reasons = []

    # --- 北向资金（-3~+3分）---
    # 行业共识：北向资金被称为"聪明钱"，连续增减持方向具有领先意义
    nb_direction = northbound_summary.get('方向判断', '')
    nb_increase_days = northbound_summary.get('连续增持天数', 0)
    nb_decrease_days = northbound_summary.get('连续减持天数', 0)

    if nb_increase_days >= 3:
        capital_score += 3
        capital_reasons.append(f'北向资金连续{nb_increase_days}日增持+3')
    elif nb_increase_days >= 1:
        capital_score += 1
        capital_reasons.append(f'北向资金近期增持+1')
    elif nb_decrease_days >= 3:
        capital_score -= 3
        capital_reasons.append(f'北向资金连续{nb_decrease_days}日减持-3')
    elif nb_decrease_days >= 1:
        capital_score -= 1
        capital_reasons.append(f'北向资金近期减持-1')
    else:
        capital_reasons.append('北向资金方向不明+0')

    # --- 沪深港通持股比例变化（-2~+2分）---
    hk_direction = sh_sz_hk_summary.get('方向判断', '')
    hk_ratio_trend = sh_sz_hk_summary.get('占流通股比变化趋势', '')
    hk_increase_days = sh_sz_hk_summary.get('连续增持天数', 0)
    hk_decrease_days = sh_sz_hk_summary.get('连续减持天数', 0)

    if '上升' in hk_ratio_trend:
        capital_score += 2
        capital_reasons.append(f'沪深港通持股比例上升+2（{hk_ratio_trend}）')
    elif '下降' in hk_ratio_trend:
        capital_score -= 2
        capital_reasons.append(f'沪深港通持股比例下降-2（{hk_ratio_trend}）')
    else:
        capital_reasons.append('沪深港通持股比例持平+0')

    # --- 机构持仓变化（-3~+3分）---
    # 行业共识：机构持仓占比上升+筹码集中为积极信号
    org_trend = org_holder_summary.get('持仓变化趋势', '')
    org_holder_change = org_holder_summary.get('股东人数变化', '')
    org_increase = org_holder_summary.get('增持机构', '')
    org_decrease = org_holder_summary.get('减持机构', '')

    if '升至' in org_trend or '升' in str(org_trend):
        capital_score += 2
        capital_reasons.append(f'机构持仓占比上升+2（{org_trend}）')
    elif '降至' in org_trend or '降' in str(org_trend):
        capital_score -= 2
        capital_reasons.append(f'机构持仓占比下降-2（{org_trend}）')

    if '集中' in org_holder_change and '分散' not in org_holder_change:
        capital_score += 1
        capital_reasons.append(f'筹码集中+1（{org_holder_change}）')
    elif '分散' in org_holder_change:
        capital_score -= 1
        capital_reasons.append(f'筹码分散-1（{org_holder_change}）')

    # --- 龙虎榜（-3~+2分）---
    billboard_status = billboard_summary.get('状态', '')
    if billboard_status and '无' in billboard_status:
        capital_reasons.append('近期无龙虎榜记录（中性）+0')
    else:
        org_buy_seats = billboard_summary.get('机构买入席位总次数', 0)
        org_sell_seats = billboard_summary.get('机构卖出席位总次数', 0)
        if org_buy_seats > org_sell_seats:
            capital_score += 2
            capital_reasons.append(f'龙虎榜机构净买入+2（买{org_buy_seats}次/卖{org_sell_seats}次）')
        elif org_sell_seats > org_buy_seats:
            capital_score -= 3
            capital_reasons.append(f'龙虎榜机构净卖出-3（买{org_buy_seats}次/卖{org_sell_seats}次）')

    # --- 大宗交易（-2~+1分）---
    block_status = block_trade_summary.get('状态', '')
    block_count = block_trade_summary.get('交易笔数', 0)
    if block_count == 0:
        capital_reasons.append('近期无大宗交易（中性）+0')
    else:
        block_character = block_trade_summary.get('交易特征', '')
        block_org = block_trade_summary.get('机构席位情况', '')
        if '折价' in block_character and '卖方' in block_org:
            capital_score -= 2
            capital_reasons.append(f'大宗交易折价+机构卖方-2')
        elif '溢价' in block_character:
            capital_score += 1
            capital_reasons.append(f'大宗交易溢价成交+1')

    # --- 融资融券杠杆方向（-2~+2分）---
    # 行业共识：融资余额增加反映杠杆资金看多，融券余额增加反映做空力量增强
    margin_news = [n for n in news_data if isinstance(n, dict) and n.get('类别') == '融资融券'] if news_data else []
    if margin_news:
        margin_texts = ' '.join(n.get('标题', '') + n.get('摘要', '') for n in margin_news)
        margin_bullish_kw = ['融资净买入', '融资余额增', '融资买入额增', '两融余额增']
        margin_bearish_kw = ['融资净偿还', '融资余额降', '融资余额减', '融券余额增', '融券卖出增']
        is_margin_bullish = any(kw in margin_texts for kw in margin_bullish_kw)
        is_margin_bearish = any(kw in margin_texts for kw in margin_bearish_kw)
        if is_margin_bullish and not is_margin_bearish:
            capital_score += 2
            capital_reasons.append('融资融券：杠杆资金偏多+2')
        elif is_margin_bearish and not is_margin_bullish:
            capital_score -= 2
            capital_reasons.append('融资融券：杠杆资金偏空-2')
        elif is_margin_bullish and is_margin_bearish:
            capital_reasons.append('融资融券：多空信号混合+0')
        else:
            capital_reasons.append(f'融资融券：有相关消息但方向不明+0')
    else:
        capital_reasons.append('融资融券：无近期数据（中性）+0')

    # --- 约束规则 ---
    # 机构持仓连续减持且北向资金净卖出
    if ('降' in str(org_trend)) and nb_decrease_days >= 1:
        capital_score = min(capital_score, 5)
        capital_reasons.append('★机构减持+北向减持约束：上限5分')

    # 北向连续减持≥3且沪深港通占比下降
    if nb_decrease_days >= 3 and '下降' in hk_ratio_trend:
        capital_score = min(capital_score, 6)
        capital_reasons.append('★北向连续减持≥3日+港通占比下降约束：上限6分')

    # 北向连续增持≥3且沪深港通占比上升
    if nb_increase_days >= 3 and '上升' in hk_ratio_trend:
        capital_score = max(capital_score, 10)
        capital_reasons.append('★北向连续增持≥3日+港通占比上升约束：下限10分')

    # 沪深港通连续减持≥5且机构也在减持
    if hk_decrease_days >= 5 and '降' in str(org_trend):
        capital_score = min(capital_score, 3)
        capital_reasons.append('★港通连续减持≥5日+机构减持约束：上限3分')

    capital_score = max(0, min(15, capital_score))
    scores['资金筹码'] = capital_score
    details['资金筹码'] = capital_reasons

    # ════════════════════════════════════════════
    # 维度6：外部环境（满分5分）
    # ════════════════════════════════════════════
    env_score = 2  # 基准分
    env_reasons = []

    # --- 大盘环境（0~3分）---
    market_sentiment = market_env.get('大盘环境判断', '')
    today_market = market_env.get('当日大盘表现', '')

    if '偏多' in market_sentiment:
        env_score += 2
        env_reasons.append(f'大盘环境偏多+2')
    elif '偏空' in market_sentiment:
        env_score -= 2
        env_reasons.append(f'大盘环境偏空-2')
    else:
        env_reasons.append(f'大盘震荡+0')

    if '普涨' in today_market:
        env_score += 1
        env_reasons.append('当日大盘普涨+1')
    elif '普跌' in today_market:
        env_score -= 1
        env_reasons.append('当日大盘普跌-1')

    # --- 消息面（-2~+2分）---
    # 只扫描非融资融券类消息，融资融券已在资金筹码维度处理
    has_major_positive = False
    has_major_negative = False
    if news_data:
        for news in news_data:
            if isinstance(news, dict) and news.get('类别') == '融资融券':
                continue  # 融资融券已在维度5评分
            title = ''
            if isinstance(news, dict):
                title = news.get('标题', '') or news.get('title', '')
            elif isinstance(news, str):
                title = news
            # 重大利好关键词
            if any(kw in title for kw in ['业绩大增', '净利润增长', '业绩快报', '同比增', '超预期']):
                has_major_positive = True
            # 重大利空关键词
            if any(kw in title for kw in ['业绩暴雷', '监管处罚', '立案调查', '退市', '暂停上市', 'ST']):
                has_major_negative = True

    if has_major_negative:
        env_score = 0
        env_reasons.append('★存在重大利空消息约束：外部环境0分')
    elif has_major_positive:
        env_score += 2
        env_reasons.append('存在重大利好消息+2')

    # 大盘下跌趋势约束
    sh_index = market_env.get('上证指数', {})
    sh_ma5_pos = sh_index.get('5日均线位置', '')
    sh_ma10_pos = sh_index.get('10日均线位置', '')
    if '跌破' in sh_ma5_pos and '跌破' in sh_ma10_pos:
        env_score = min(env_score, 2)
        env_reasons.append('★大盘跌破5日和10日均线约束：上限2分')

    env_score = max(0, min(5, env_score))
    scores['外部环境'] = env_score
    details['外部环境'] = env_reasons

    # ════════════════════════════════════════════
    # 维度7：风险收益比（满分10分）
    # ════════════════════════════════════════════
    rr_score = 5  # 基准分
    rr_reasons = []

    # 基于BOLL风险收益比
    dist_up = boll_summary.get('距上轨（%）', 0) or 0
    dist_mid = boll_summary.get('距中轨（%）', 0) or 0

    # 行业共识：风险收益比>3:1为优秀，2:1为良好，<1:1为不利
    if isinstance(dist_up, (int, float)) and isinstance(dist_mid, (int, float)) and dist_mid > 0:
        rr_ratio = round(dist_up / dist_mid, 1)
        if rr_ratio >= 3:
            rr_score += 4
            rr_reasons.append(f'BOLL风险收益比{rr_ratio}:1+4（上方{dist_up}% vs 下方至中轨{dist_mid}%）')
        elif rr_ratio >= 2:
            rr_score += 2
            rr_reasons.append(f'BOLL风险收益比{rr_ratio}:1+2')
        elif rr_ratio >= 1:
            rr_score += 0
            rr_reasons.append(f'BOLL风险收益比{rr_ratio}:1+0')
        else:
            rr_score -= 2
            rr_reasons.append(f'BOLL风险收益比{rr_ratio}:1-2（风险大于收益）')
    elif '下方' in boll_position:
        rr_score -= 2
        rr_reasons.append('收盘在中轨下方，风险收益比不利-2')

    # 距120日高低点的位置
    dist_high = kline_summary.get('当前价距120日高点', '')
    dist_low = kline_summary.get('当前价距120日低点', '')
    if isinstance(dist_high, str) and '%' in dist_high:
        try:
            high_pct = float(dist_high.replace('%', ''))
            if high_pct < -30:
                rr_score += 1
                rr_reasons.append(f'距120日高点{dist_high}（深度回调，反弹空间大）+1')
            elif high_pct > -5:
                rr_score -= 1
                rr_reasons.append(f'距120日高点{dist_high}（接近前高，上方压力大）-1')
        except ValueError:
            pass

    rr_score = max(0, min(10, rr_score))
    scores['风险收益比'] = rr_score
    details['风险收益比'] = rr_reasons

    # ════════════════════════════════════════════
    # 汇总
    # ════════════════════════════════════════════
    total = sum(scores.values())

    # 评级
    if total >= 85:
        grade = '积极买入'
    elif total >= 70:
        grade = '逢低建仓'
    elif total >= 55:
        grade = '持股待涨'
    elif total >= 40:
        grade = '逢高减仓'
    elif total >= 25:
        grade = '保持观望'
    else:
        grade = '清仓离场'

    # 持有/未持有建议
    if total >= 70:
        not_hold_grade = '逢低建仓' if total < 85 else '积极买入'
        hold_grade = '持股待涨'
    elif total >= 55:
        not_hold_grade = '保持观望'
        hold_grade = '持股待涨'
    elif total >= 40:
        not_hold_grade = '保持观望'
        hold_grade = '逢高减仓'
    else:
        not_hold_grade = '保持观望'
        hold_grade = '清仓离场'

    return {
        '总分': total,
        '评级': grade,
        '未持有建议': not_hold_grade,
        '持有建议': hold_grade,
        '各维度得分': {
            '趋势强度': f'{scores["趋势强度"]}/20',
            '动能与量价': f'{scores["动能与量价"]}/20',
            '结构边界': f'{scores["结构边界"]}/15',
            '短线情绪': f'{scores["短线情绪"]}/15',
            '资金筹码': f'{scores["资金筹码"]}/15',
            '外部环境': f'{scores["外部环境"]}/5',
            '风险收益比': f'{scores["风险收益比"]}/10',
        },
        '各维度评分依据': {
            '趋势强度': details['趋势强度'],
            '动能与量价': details['动能与量价'],
            '结构边界': details['结构边界'],
            '短线情绪': details['短线情绪'],
            '资金筹码': details['资金筹码'],
            '外部环境': details['外部环境'],
            '风险收益比': details['风险收益比'],
        },
    }



# ──────────────────────────────────────────────
# 主函数
# ──────────────────────────────────────────────

async def get_stock_indicator_all_prompt(stock_info: StockInfo):
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

    # ── 机构持仓快照 & 沪深港通持股 & 周线数据 ──
    org_snapshot = await get_org_realtime_snapshot(stock_info)
    sh_sz_hk_hold = await get_org_hold_by_sh_sz_hk_rank_cn(stock_info, page_size=10)
    weekly_kline_data = await get_stock_week_kline_list_10jqka(stock_info, limit=30)

    # ── 龙虎榜数据 ──
    billboard_data = await get_billboard_json(stock_info, days=30)

    # ── 百度搜索近期新闻/公告/事件 ──
    stock_news = await search_stock_news(stock_info, days=7)

    # ── 百度搜索大宗交易数据 ──
    block_trade_records = await search_block_trade(stock_info, days=30)

    # ── 五档盘口数据（新浪实时行情） ──
    order_book_data = await get_order_book(stock_info)

    # ── Python 端预计算（核心优化：把容易出错的计算从 LLM 移到代码端）──
    # 先计算下一个交易日（供大宗交易/消息面时效性判断使用）
    next_trading_day = datetime.now().date() + timedelta(days=1)
    while next_trading_day.weekday() >= 5 or chinese_calendar.is_holiday(next_trading_day):
        next_trading_day += timedelta(days=1)

    valid_kline = _filter_valid_trading_days(stock_day_kline)
    divergence_result = _compute_macd_divergence(macd_signals_macd, valid_kline)
    macd_bar_trend = _compute_macd_bar_trend(macd_signals_macd)
    golden_cross_quality = _compute_golden_cross_quality(macd_signals_macd, valid_kline)
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
    fund_flow_behavior = _compute_fund_flow_behavior(real_main_fund_flow)

    # ── 机构持仓快照 & 周线预计算 ──
    org_holder_summary = compute_org_snapshot_summary(org_snapshot)
    northbound_summary = _compute_northbound_summary(northbound_funds_cn)
    sh_sz_hk_summary = _compute_sh_sz_hk_hold_summary(sh_sz_hk_hold)
    weekly_kline_summary = _compute_weekly_kline_summary(weekly_kline_data)
    billboard_summary = _compute_billboard_summary(billboard_data)
    block_trade_summary = compute_block_trade_summary(block_trade_records, next_trading_day.strftime('%Y-%m-%d'))
    order_book_summary = compute_order_book_summary(order_book_data)

    # ── 大盘指数环境数据 ──
    market_env = await _compute_market_environment(stock_info)

    # ── 综合评分（Python端预计算，基于行业共识规则）──
    comprehensive_score = _compute_comprehensive_score(
        macd_data=macd_signals_macd,
        macd_bar_trend=macd_bar_trend,
        divergence_result=divergence_result,
        golden_cross_quality=golden_cross_quality,
        kdj_summary=kdj_summary,
        boll_summary=boll_summary,
        ma_summary=ma_summary,
        kline_summary=kline_summary,
        volume_trend=volume_trend,
        weekly_kline_summary=weekly_kline_summary,
        intraday_summary=intraday_summary,
        fund_flow_behavior=fund_flow_behavior,
        order_book_summary=order_book_summary,
        northbound_summary=northbound_summary,
        sh_sz_hk_summary=sh_sz_hk_summary,
        org_holder_summary=org_holder_summary,
        billboard_summary=billboard_summary,
        block_trade_summary=block_trade_summary,
        market_env=market_env,
        news_data=stock_news,
    )

    # ── 精简数据（减少 token，降低幻觉概率）──
    macd_trimmed = _trim_macd_details(macd_signals_macd, keep_recent=30)
    boll_trimmed = _trim_boll_details(boll_rule_boll, keep_recent=20)
    kline_trimmed = _trim_kline_data(valid_kline, keep_recent=30)
    ma_trimmed = _trim_ma_data(moving_averages_json, keep_recent=20)

    return f"""
# 当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}，下一个交易日：{next_trading_day.strftime('%Y-%m-%d')} 09:30-11:30 / 13:00-15:00

# Role: 资深A股技术面分析师 / 操盘手
你拥有20年实战交易经验，精通量价关系、分时盘口语言以及MACD、KDJ、BOLL等核心指标的底层逻辑。你的分析风格：客观、严谨、直击本质、拒绝模棱两可。

## Task: {stock_info.stock_name}（{stock_info.stock_code_normalize}）次日实战操作推演
请基于我提供的"预计算分析结论"与"精简后的原始数据"，进行深度交叉验证，输出具备极强实操指导意义的分析报告。

---

## ★ 重要约束（必须遵守）

1. **直接引用预计算结论**：背离信号、MACD柱趋势、金叉质量评估、BOLL空间摘要、KDJ状态摘要、分时特征摘要、K线统计摘要、均线排列状态、周线趋势摘要、资金流向行为特征、五档盘口摘要、北向资金增减持摘要、沪深港通持股变化摘要、机构持仓变化摘要、龙虎榜摘要、大宗交易摘要、近期消息面 均已在 Python 端预计算完成，你必须直接引用这些结论，严禁自行重新推导。
2. **禁止计算幻觉**：均线值、乖离率、BOLL距离、MACD柱趋势、KDJ差值、量能倍数、北向资金连续增减持天数、沪深港通占流通股比变化等必须直接读取提供的预计算数据，严禁自行做加减乘除运算。
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

**★ 预计算金叉质量评估（必须直接引用，严禁自行推导金叉质量）：**
{json.dumps(golden_cross_quality, ensure_ascii=False)}

请结合MACD明细数据，分析当前多空状态，并直接引用上述MACD柱趋势结论、背离结论和金叉质量评估结论。

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

### 二、 中期结构与均线系统（日线120日 + 周线大局观）

**★ 预计算均线状态（必须直接引用）：**
{json.dumps(ma_summary, ensure_ascii=False)}

**★ 预计算K线统计摘要（必须直接引用）：**
{json.dumps(kline_summary, ensure_ascii=False)}

**★ 预计算量能趋势分析（必须直接引用）：**
{json.dumps(volume_trend, ensure_ascii=False)}

**★ 预计算周线趋势摘要（必须直接引用）：**
{json.dumps(weekly_kline_summary, ensure_ascii=False)}

请基于上述预计算结论，分析：
- 日线均线排列、乖离率风险、量价匹配、量能趋势、支撑压力位（严禁自行计算均线值和量能变化）
- 周线级别趋势方向，与日线趋势是否共振（日周共振为强信号，背离则需警惕）
- 周线趋势应纳入多空博弈清单和综合评分中的"趋势强度"维度

### 三、 今日分时盘口深度解析（超短线博弈）

**★ 预计算分时特征摘要（必须直接引用）：**
{json.dumps(intraday_summary, ensure_ascii=False)}

**★ 资金流向数据（已预计算行为特征，必须直接引用）：**
{json.dumps(fund_flow_behavior, ensure_ascii=False)}

**★ 预计算五档盘口摘要（必须直接引用）：**
{json.dumps(order_book_summary, ensure_ascii=False)}

请基于上述摘要分析：
- 黄白线格局（白线在黄线上方占比已给出）
- 各时段量价分布特征
- 脉冲式放量事件的含义
- 尾盘资金动向
- 五档盘口买卖力量对比（买卖比、大单托底/压盘情况、价差宽窄）

### 四、 资金筹码面（机构行为与大资金动向）

**★ 预计算北向资金增减持摘要（必须直接引用）：**
{json.dumps(northbound_summary, ensure_ascii=False)}

**★ 预计算沪深港通持股变化摘要（必须直接引用）：**
{json.dumps(sh_sz_hk_summary, ensure_ascii=False)}

**★ 预计算机构持仓变化摘要（必须直接引用）：**
{json.dumps(org_holder_summary, ensure_ascii=False)}

**★ 预计算龙虎榜摘要（必须直接引用）：**
{json.dumps(billboard_summary, ensure_ascii=False)}

**★ 预计算大宗交易摘要（必须直接引用，含对次日影响判断）：**
大宗交易发生在A股收盘后（15:00-15:30盘后撮合），属于盘后交易。请直接引用"对下一个交易日影响"字段判断其时效性。
{json.dumps(block_trade_summary, ensure_ascii=False)}

请基于上述预计算摘要分析（严禁自行计算连续天数、累计幅度等，必须直接引用摘要中的结论）：
- 北向资金方向与力度：直接引用"方向判断"和"连续增/减持天数"，判断聪明钱态度
- 沪深港通持股趋势：直接引用"方向判断"和"占流通股比变化趋势"，判断外资中长期态度
- 机构持仓变化：直接引用"增持机构"/"减持机构"和"持仓变化趋势"，判断机构整体动向
- 龙虎榜席位特征：直接引用"机构席位行为"，若近期无上榜记录则说明"近期未触发龙虎榜"
- 大宗交易特征：直接引用"交易特征"和"机构席位情况"，并引用"对下一个交易日影响"判断时效性；若近期无记录则说明"近期无大宗交易"
- 融资融券杠杆方向：引用消息面中【融资融券动态】板块数据，判断杠杆资金偏多/偏空方向，与北向资金、主力资金流向交叉验证（该维度已在Python端评分中纳入资金筹码维度）
- 综合判断：基于以上六个子维度的预计算结论，判断大资金整体流入/流出方向和筹码集中/分散趋势

### 五、 外部环境（大盘系统性风险 + 消息面）

**★ 预计算大盘指数走势摘要（必须直接引用）：**
{json.dumps(market_env, ensure_ascii=False)}

**★ 近期消息面（百度搜索，近7日，按融资融券与其他消息分类展示）：**
**时效性判定规则（A股交易时段：09:30-11:30 / 13:00-15:00 北京时间）：**
- 上一个交易日15:00后发布的盘后消息 → 市场尚未消化，对{next_trading_day.strftime('%Y-%m-%d')}开盘有直接影响（标记为★）
- {next_trading_day.strftime('%Y-%m-%d')}当日09:30前发布的盘前消息 → 对当日开盘有直接影响（标记为★）
- 盘中发布的消息 → 已被部分消化，对次日影响减弱
- 更早的消息 → 影响逐步衰减
- 每条消息后的"→ 对次日影响"已根据上述规则预判断，请直接引用

{format_news_for_prompt(stock_news, next_trading_day.strftime('%Y-%m-%d'))}

请基于上述数据分析：
- 大盘当前系统性风险水平，个股走势是跟随大盘还是独立行情
- 消息面时效性判断：重点关注标记为★的消息（盘后/盘前发布、市场尚未消化），这些消息对{next_trading_day.strftime('%Y-%m-%d')}开盘有直接影响；盘中已发布的消息影响已减弱，需降低权重
- 融资融券动态分析（【融资融券动态】板块）：关注融资融券余额变化趋势，判断杠杆资金方向（融资余额增加偏多、融券余额增加偏空），与北向资金、主力资金流向交叉验证，该维度已在Python端评分中纳入"资金筹码"维度
- 其他重要消息分析（【其他重要消息】板块）：判断是否存在影响股价的重大利好/利空事件，若无重大消息则简要说明"消息面平淡"
- 外部环境对次日操作的约束（如大盘弱势则个股反弹空间受限）

### 六、 多空力量博弈清单
以列表形式，客观陈述当前盘面的核心利多与利空因素：
- **多方筹码（有利因素）**：[✅] （提炼4-6个核心数据支撑点，须覆盖技术面、资金面、外部环境，每条必须引用具体数值或预计算结论原文）
- **空方筹码（不利因素）**：[❌] （提炼4-6个核心风险警示点，须覆盖技术面、资金面、外部环境，每条必须引用具体数值或预计算结论原文）

**资金面引用要求**：多空清单中涉及北向资金、沪深港通、机构持仓的条目，必须直接引用对应预计算摘要中的"方向判断"或"持仓变化趋势"原文，严禁自行概括。

### 七、 综合评分体系（满分100分）

**★ 预计算综合评分（Python端基于行业共识规则计算，必须直接引用，严禁自行重新打分）：**
{json.dumps(comprehensive_score, ensure_ascii=False)}

**评分规则说明（已在Python端执行，以下仅供理解评分逻辑）：**

*趋势强度（满分20分）评分规则：*
- 基准分10分
- MACD市场状态：强多头+4 / 弱多头+1 / 空头-3
- MACD柱方向：红柱放大+2 / 红柱收窄+0 / 绿柱收窄+1 / 绿柱放大-2
- 金叉质量：高质量+2 / 中等+1 / 低质量+0 / 无效-1
- 日线均线排列：多头+4 / 空头-4 / 纠缠+0
- 周线趋势及日周共振：日周多头共振+4 / 周线多头日线未共振+2 / 日周空头共振-4 / 周线空头日线未共振-2
- 约束：顶背离上限12分 / 日周方向相反上限14分 / 零轴下死叉+均线空头上限5分

*动能与量价（满分20分）评分规则：*
- 基准分10分
- KDJ信号：超卖区低位拐头向上+6 / 低位拐头向上+3 / 超买区高位拐头向下-4 / 高位拐头向下-2 / 中高位上行+1 / 中低位下行-1
- 量价形态：放量上攻+4 / 放量冲高缩量回落-1 / 放量杀跌-3 / 缩量反弹+0 / 缩量阴跌-2
- 量能趋势：持续放量+2 / 持续缩量-2
- 约束：KDJ高位钝化拐头向下上限8分 / 持续缩量且未达放量标准上限10分

*结构边界（满分15分）评分规则：*
- 基准分7分
- BOLL位置：中轨上方+中轨上倾+4 / 中轨上方+中轨下倾+1 / 中轨下方+中轨下倾-3 / 中轨下方-1
- 乖离率：超买-2 / 超卖+1 / 正常+0
- 上方空间：距上轨>10%+3 / >5%+1 / <2%-1

*短线情绪（满分15分）评分规则：*
- 基准分7分
- 分时黄白线：白线上方占比>70%+3 / >50%+1 / <30%-3 / <50%-1
- 资金流向：主力吸筹+3 / 主力减仓散户承接-3 / 主力净流出>1亿-2 / 主力净流入>1亿+2
- 五档盘口：买卖比>1.5+2 / >1.1+1 / <0.67-2 / <0.9-1
- 约束：白线长期在黄线下方上限6分 / 主力减仓散户承接上限7分 / 尾盘走弱+主力为负上限8分

*资金筹码（满分15分）评分规则：*
- 基准分7分
- 北向资金：连续增持≥3日+3 / 增持+1 / 连续减持≥3日-3 / 减持-1
- 沪深港通占比：上升+2 / 下降-2
- 机构持仓：占比上升+2 / 下降-2 / 筹码集中+1 / 分散-1
- 龙虎榜：机构净买入+2 / 机构净卖出-3 / 无记录+0
- 大宗交易：折价+机构卖方-2 / 溢价+1 / 无记录+0
- 融资融券：杠杆资金偏多+2 / 杠杆资金偏空-2 / 多空混合+0 / 无数据+0
- 约束：机构减持+北向减持上限5分 / 北向减持≥3+港通占比下降上限6分 / 北向增持≥3+港通占比上升下限10分 / 港通减持≥5+机构减持上限3分

*外部环境（满分5分）评分规则：*
- 基准分2分
- 大盘环境：偏多+2 / 偏空-2 / 震荡+0
- 当日大盘：普涨+1 / 普跌-1
- 消息面（仅非融资融券类消息）：重大利好+2 / 重大利空=0分
- 约束：大盘跌破5日和10日均线上限2分 / 重大利空消息0分

*风险收益比（满分10分）评分规则：*
- 基准分5分
- BOLL风险收益比：≥3:1+4 / ≥2:1+2 / ≥1:1+0 / <1:1-2
- 距120日高点：<-30%+1（深度回调） / >-5%-1（接近前高）

**实战评级标准**：
- 85-100：积极买入
- 70-84：逢低建仓
- 55-69：持股待涨
- 40-54：逢高减仓
- 25-39：保持观望
- <25：清仓离场

请直接引用上述预计算评分结果，在分析报告中展示评分表格和各维度评分依据。若你认为某些特殊因素（如盘后重大消息尚未反映在技术面数据中）需要调整评级，必须明确说明调整原因和幅度。

### 八、 明日实战操作策略（Strategy）

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

### 九、 盘中关键观察哨（明日盯盘重点）

| 监控指标 | 当前状态 | 次日健康/安全阈值 | 危险破位/预警线 |
|--------|--------|----------------|---------------|
| 15分钟MACD | | | |
| 分时均量线 | | | |
| 关键均线(如MA5) | | | |
| 五档买卖力量比 | | | |
| 主力资金净流入 | | | |

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

    async def main():
        stock_info = get_stock_info_by_name('生益科技')
        prompt = await _fetch_market_index_summary("深证成指")
        print(prompt)

    asyncio.run(main())
