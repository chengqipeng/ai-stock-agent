#!/usr/bin/env python3
"""
v13 增强深度分析：行业板块资金流向 + 海外科技关联 + 预测准确率诊断

核心分析维度（不考虑打分，只关注涨跌预测准确率）：
1. 行业板块历史资金流向与次日涨跌的关联分析
2. 科技/芯片板块与美股半导体龙头的日线走势关联验证
3. 同行资金流向聚合信号 vs 个股预测准确率
4. 美股大幅波动日的A股板块联动深度分析
5. 资金流向趋势（连续流入/流出）对预测的增益验证
"""
import asyncio
import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# 加载回测结果
# ═══════════════════════════════════════════════════════════
with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']
total = len(details)


def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))


loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"{'=' * 80}")
print(f"当前基线: {loose_ok}/{total} ({loose_ok / total * 100:.1f}%)")
print(f"{'=' * 80}")

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']



# ═══════════════════════════════════════════════════════════
# 分析一：行业板块历史资金流向 vs 次日涨跌关联
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"一、行业板块资金流向 vs 次日涨跌关联分析")
print(f"{'=' * 80}")

# 从DB加载所有回测股票的资金流向数据
from dao.stock_fund_flow_dao import get_fund_flow_by_code

# 收集所有股票代码
stock_codes = list(set(d['代码'] for d in details))
stock_sector_map = {}
for d in details:
    stock_sector_map[d['代码']] = d['板块']

# 加载资金流向数据
fund_flow_cache = {}
for code in stock_codes:
    ff = get_fund_flow_by_code(code, limit=200)
    if ff:
        fund_flow_cache[code] = {str(r.get('date', '')): r for r in ff}

print(f"  已加载 {len(fund_flow_cache)} 只股票的资金流向数据")

# 分析1a: 个股资金流向方向 vs 次日涨跌
print(f"\n  1a. 个股大单净额方向 vs 次日涨跌:")
fund_direction_stats = defaultdict(lambda: {'ok': 0, 'n': 0})
fund_strength_stats = defaultdict(lambda: {'ok': 0, 'n': 0})

for d in details:
    code = d['代码']
    score_date = d['评分日']
    actual_chg = parse_chg(d['实际涨跌'])
    
    if code not in fund_flow_cache:
        continue
    ff_map = fund_flow_cache[code]
    ff_today = ff_map.get(score_date)
    if not ff_today:
        continue
    
    big_net = ff_today.get('big_net') or 0
    big_net_pct = ff_today.get('big_net_pct') or 0
    
    # 资金流向方向 vs 次日涨跌
    if big_net > 0:
        fund_dir = '净流入'
    elif big_net < 0:
        fund_dir = '净流出'
    else:
        fund_dir = '中性'
    
    fund_direction_stats[fund_dir]['n'] += 1
    if (big_net > 0 and actual_chg > 0) or (big_net < 0 and actual_chg < 0):
        fund_direction_stats[fund_dir]['ok'] += 1
    
    # 按资金流向强度分段
    if abs(big_net_pct) > 5:
        strength = '强(>5%)'
    elif abs(big_net_pct) > 2:
        strength = '中(2-5%)'
    elif abs(big_net_pct) > 0.5:
        strength = '弱(0.5-2%)'
    else:
        strength = '微(<0.5%)'
    
    fund_strength_stats[strength]['n'] += 1
    if (big_net > 0 and actual_chg > 0) or (big_net < 0 and actual_chg < 0):
        fund_strength_stats[strength]['ok'] += 1

for dir_name in ['净流入', '净流出', '中性']:
    s = fund_direction_stats[dir_name]
    if s['n'] > 0:
        rate = s['ok'] / s['n'] * 100
        print(f"    {dir_name}: 方向一致率 {s['ok']}/{s['n']} ({rate:.1f}%)")

print(f"\n  1b. 资金流向强度 vs 方向一致率:")
for strength in ['强(>5%)', '中(2-5%)', '弱(0.5-2%)', '微(<0.5%)']:
    s = fund_strength_stats[strength]
    if s['n'] > 0:
        rate = s['ok'] / s['n'] * 100
        print(f"    {strength}: {s['ok']}/{s['n']} ({rate:.1f}%)")

# 分析1c: 按板块的资金流向有效性
print(f"\n  1c. 按板块的资金流向方向一致率:")
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    inflow_ok, inflow_n = 0, 0
    outflow_ok, outflow_n = 0, 0
    
    for d in sec_data:
        code = d['代码']
        score_date = d['评分日']
        actual_chg = parse_chg(d['实际涨跌'])
        
        if code not in fund_flow_cache:
            continue
        ff_today = fund_flow_cache[code].get(score_date)
        if not ff_today:
            continue
        
        big_net = ff_today.get('big_net') or 0
        if big_net > 0:
            inflow_n += 1
            if actual_chg > 0:
                inflow_ok += 1
        elif big_net < 0:
            outflow_n += 1
            if actual_chg < 0:
                outflow_ok += 1
    
    in_rate = f"{inflow_ok}/{inflow_n} ({inflow_ok/inflow_n*100:.1f}%)" if inflow_n > 0 else "N/A"
    out_rate = f"{outflow_ok}/{outflow_n} ({outflow_ok/outflow_n*100:.1f}%)" if outflow_n > 0 else "N/A"
    print(f"    {sector:6s}: 流入→涨={in_rate} | 流出→跌={out_rate}")


# ═══════════════════════════════════════════════════════════
# 分析二：板块聚合资金流向（同行业多只股票资金流向聚合）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"二、板块聚合资金流向信号 vs 次日涨跌")
print(f"{'=' * 80}")

# 按(板块, 日期)聚合同行业所有股票的资金流向
sector_date_fund = defaultdict(lambda: {'total_big_net': 0, 'inflow_count': 0,
                                         'outflow_count': 0, 'stock_count': 0})

for d in details:
    code = d['代码']
    score_date = d['评分日']
    sector = d['板块']
    
    if code not in fund_flow_cache:
        continue
    ff_today = fund_flow_cache[code].get(score_date)
    if not ff_today:
        continue
    
    big_net = ff_today.get('big_net') or 0
    key = (sector, score_date)
    sector_date_fund[key]['total_big_net'] += big_net
    sector_date_fund[key]['stock_count'] += 1
    if big_net > 0:
        sector_date_fund[key]['inflow_count'] += 1
    elif big_net < 0:
        sector_date_fund[key]['outflow_count'] += 1

# 分析板块聚合资金流向 vs 板块内个股次日涨跌
print(f"\n  2a. 板块聚合资金净流入方向 vs 板块内个股次日涨跌:")
for sector in sectors:
    sector_inflow_ok, sector_inflow_n = 0, 0
    sector_outflow_ok, sector_outflow_n = 0, 0
    
    for d in details:
        if d['板块'] != sector:
            continue
        key = (sector, d['评分日'])
        if key not in sector_date_fund:
            continue
        
        agg = sector_date_fund[key]
        actual_chg = parse_chg(d['实际涨跌'])
        
        if agg['total_big_net'] > 0:
            sector_inflow_n += 1
            if actual_chg > 0:
                sector_inflow_ok += 1
        elif agg['total_big_net'] < 0:
            sector_outflow_n += 1
            if actual_chg < 0:
                sector_outflow_ok += 1
    
    in_rate = f"{sector_inflow_ok}/{sector_inflow_n} ({sector_inflow_ok/sector_inflow_n*100:.1f}%)" if sector_inflow_n > 0 else "N/A"
    out_rate = f"{sector_outflow_ok}/{sector_outflow_n} ({sector_outflow_ok/sector_outflow_n*100:.1f}%)" if sector_outflow_n > 0 else "N/A"
    print(f"    {sector:6s}: 板块净流入→涨={in_rate} | 板块净流出→跌={out_rate}")

# 分析2b: 板块资金流向一致性（多数股票同向流入/流出）
print(f"\n  2b. 板块资金流向一致性（>70%股票同向）vs 次日涨跌:")
for sector in sectors:
    consensus_bullish_ok, consensus_bullish_n = 0, 0
    consensus_bearish_ok, consensus_bearish_n = 0, 0
    no_consensus_ok, no_consensus_n = 0, 0
    
    for d in details:
        if d['板块'] != sector:
            continue
        key = (sector, d['评分日'])
        if key not in sector_date_fund:
            continue
        
        agg = sector_date_fund[key]
        actual_chg = parse_chg(d['实际涨跌'])
        sc = agg['stock_count']
        if sc == 0:
            continue
        
        inflow_ratio = agg['inflow_count'] / sc
        outflow_ratio = agg['outflow_count'] / sc
        
        if inflow_ratio > 0.7:
            consensus_bullish_n += 1
            if actual_chg > 0:
                consensus_bullish_ok += 1
        elif outflow_ratio > 0.7:
            consensus_bearish_n += 1
            if actual_chg < 0:
                consensus_bearish_ok += 1
        else:
            no_consensus_n += 1
            if d['宽松正确'] == '✓':
                no_consensus_ok += 1
    
    cb = f"{consensus_bullish_ok}/{consensus_bullish_n} ({consensus_bullish_ok/consensus_bullish_n*100:.1f}%)" if consensus_bullish_n > 0 else "N/A"
    cc = f"{consensus_bearish_ok}/{consensus_bearish_n} ({consensus_bearish_ok/consensus_bearish_n*100:.1f}%)" if consensus_bearish_n > 0 else "N/A"
    nc = f"{no_consensus_ok}/{no_consensus_n} ({no_consensus_ok/no_consensus_n*100:.1f}%)" if no_consensus_n > 0 else "N/A"
    print(f"    {sector:6s}: 一致看涨→涨={cb} | 一致看跌→跌={cc} | 无共识模型准确={nc}")


# ═══════════════════════════════════════════════════════════
# 分析三：连续资金流向趋势对预测的增益
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"三、连续资金流向趋势（3日/5日）vs 次日涨跌")
print(f"{'=' * 80}")

# 分析连续N日大单净流入/流出的趋势信号
for n_days in [3, 5]:
    print(f"\n  连续{n_days}日资金流向趋势:")
    trend_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0})
    
    for d in details:
        code = d['代码']
        score_date = d['评分日']
        actual_chg = parse_chg(d['实际涨跌'])
        
        if code not in fund_flow_cache:
            continue
        
        ff_map = fund_flow_cache[code]
        # 获取score_date及之前n_days天的资金流向
        dates_sorted = sorted([dt for dt in ff_map.keys() if dt <= score_date], reverse=True)
        if len(dates_sorted) < n_days:
            continue
        
        recent_nets = []
        for dt in dates_sorted[:n_days]:
            big_net = ff_map[dt].get('big_net') or 0
            recent_nets.append(big_net)
        
        if all(n > 0 for n in recent_nets):
            trend = f'连续{n_days}日净流入'
        elif all(n < 0 for n in recent_nets):
            trend = f'连续{n_days}日净流出'
        else:
            trend = '混合'
        
        trend_stats[trend]['n'] += 1
        if (trend.endswith('净流入') and actual_chg > 0) or \
           (trend.endswith('净流出') and actual_chg < 0):
            trend_stats[trend]['ok'] += 1
        if d['宽松正确'] == '✓':
            trend_stats[trend]['loose_ok'] += 1
    
    for trend_name in [f'连续{n_days}日净流入', f'连续{n_days}日净流出', '混合']:
        s = trend_stats[trend_name]
        if s['n'] > 0:
            dir_rate = s['ok'] / s['n'] * 100
            model_rate = s['loose_ok'] / s['n'] * 100
            print(f"    {trend_name}: n={s['n']:4d} 方向一致率={dir_rate:.1f}% 模型准确率={model_rate:.1f}%")

# 分析3b: 5日主力净额趋势
print(f"\n  3b. 5日主力净额(main_net_5day)方向 vs 次日涨跌:")
main5d_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'loose_ok': 0})

for d in details:
    code = d['代码']
    score_date = d['评分日']
    actual_chg = parse_chg(d['实际涨跌'])
    
    if code not in fund_flow_cache:
        continue
    ff_today = fund_flow_cache[code].get(score_date)
    if not ff_today:
        continue
    
    main_5d = ff_today.get('main_net_5day') or 0
    if main_5d > 5000:
        bucket = '强流入(>5000万)'
    elif main_5d > 1000:
        bucket = '中流入(1000-5000万)'
    elif main_5d > 0:
        bucket = '弱流入(0-1000万)'
    elif main_5d > -1000:
        bucket = '弱流出(0~-1000万)'
    elif main_5d > -5000:
        bucket = '中流出(-1000~-5000万)'
    else:
        bucket = '强流出(<-5000万)'
    
    main5d_stats[bucket]['n'] += 1
    if (main_5d > 0 and actual_chg > 0) or (main_5d < 0 and actual_chg < 0):
        main5d_stats[bucket]['ok'] += 1
    if d['宽松正确'] == '✓':
        main5d_stats[bucket]['loose_ok'] += 1

for bucket in ['强流入(>5000万)', '中流入(1000-5000万)', '弱流入(0-1000万)',
               '弱流出(0~-1000万)', '中流出(-1000~-5000万)', '强流出(<-5000万)']:
    s = main5d_stats[bucket]
    if s['n'] > 0:
        dir_rate = s['ok'] / s['n'] * 100
        model_rate = s['loose_ok'] / s['n'] * 100
        print(f"    {bucket:20s}: n={s['n']:4d} 方向一致={dir_rate:.1f}% 模型准确={model_rate:.1f}%")


# ═══════════════════════════════════════════════════════════
# 分析四：美股半导体/科技龙头 vs A股科技板块关联深度分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"四、美股科技龙头 vs A股科技板块关联深度分析")
print(f"{'=' * 80}")

from service.eastmoney.indices.us_market_db_query import (
    get_us_index_kline_range,
    preload_us_kline_map,
)
from dao.us_market_dao import get_us_stock_kline_range, get_us_stock_sector_avg_change

# 加载美股指数K线
us_indices = {}
for idx_code in ['NDX', 'SPX', 'DJIA']:
    klines = get_us_index_kline_range(idx_code, start_date='2025-11-01', end_date='2026-03-10', limit=200)
    us_indices[idx_code] = {str(k['trade_date']): float(k.get('change_pct') or 0) for k in klines}
    print(f"  美股{idx_code}: {len(us_indices[idx_code])}条K线")

# 加载美股半导体个股K线（从DB）
us_semi_stocks = ['NVDA', 'AMD', 'TSM', 'AVGO', 'ASML', 'AMAT', 'SOXX']
us_semi_klines = {}
for stock in us_semi_stocks:
    try:
        klines = get_us_stock_kline_range(stock, start_date='2025-11-01', end_date='2026-03-10', limit=200)
        if klines:
            us_semi_klines[stock] = {str(k.get('trade_date', '')): float(k.get('change_pct') or 0) for k in klines}
    except Exception as e:
        logger.warning(f"加载美股{stock}失败: {e}")

print(f"  美股半导体个股: {list(us_semi_klines.keys())}")

# 分析4a: 美股指数隔夜涨跌 vs A股各板块次日涨跌
print(f"\n  4a. 美股指数隔夜涨跌 vs A股各板块次日涨跌方向一致率:")

# 构建A股日期→美股前一交易日映射
def find_us_prev_date(a_date_str, us_date_map, lookback=7):
    """找到A股交易日前一个美股交易日"""
    dt = datetime.strptime(a_date_str, '%Y-%m-%d')
    for offset in range(1, lookback + 1):
        prev = (dt - timedelta(days=offset)).strftime('%Y-%m-%d')
        if prev in us_date_map:
            return prev
    return None

for idx_code in ['NDX', 'SPX']:
    print(f"\n    {idx_code}:")
    for sector in sectors:
        sec_data = [d for d in details if d['板块'] == sector]
        aligned, total_s = 0, 0
        # 按波动级别分
        large_aligned, large_n = 0, 0
        small_aligned, small_n = 0, 0
        
        for d in sec_data:
            us_date = find_us_prev_date(d['评分日'], us_indices[idx_code])
            if not us_date:
                continue
            us_chg = us_indices[idx_code][us_date]
            actual_chg = parse_chg(d['实际涨跌'])
            
            total_s += 1
            if (us_chg > 0 and actual_chg > 0) or (us_chg < 0 and actual_chg < 0):
                aligned += 1
            
            if abs(us_chg) >= 1.0:
                large_n += 1
                if (us_chg > 0 and actual_chg > 0) or (us_chg < 0 and actual_chg < 0):
                    large_aligned += 1
            else:
                small_n += 1
                if (us_chg > 0 and actual_chg > 0) or (us_chg < 0 and actual_chg < 0):
                    small_aligned += 1
        
        if total_s > 0:
            rate = aligned / total_s * 100
            large_rate = f"{large_aligned}/{large_n}({large_aligned/large_n*100:.1f}%)" if large_n > 0 else "N/A"
            small_rate = f"{small_aligned}/{small_n}({small_aligned/small_n*100:.1f}%)" if small_n > 0 else "N/A"
            print(f"      {sector:6s}: 全部={aligned}/{total_s}({rate:.1f}%) | 大波动(≥1%)={large_rate} | 小波动(<1%)={small_rate}")

# 分析4b: 美股半导体龙头 vs A股科技板块
print(f"\n  4b. 美股半导体龙头隔夜涨跌 vs A股科技板块次日涨跌:")
tech_data = [d for d in details if d['板块'] == '科技']

for us_stock, us_kline_map in us_semi_klines.items():
    aligned, total_s = 0, 0
    large_aligned, large_n = 0, 0
    
    for d in tech_data:
        us_date = find_us_prev_date(d['评分日'], us_kline_map)
        if not us_date:
            continue
        us_chg = us_kline_map[us_date]
        actual_chg = parse_chg(d['实际涨跌'])
        
        total_s += 1
        if (us_chg > 0 and actual_chg > 0) or (us_chg < 0 and actual_chg < 0):
            aligned += 1
        
        if abs(us_chg) >= 2.0:
            large_n += 1
            if (us_chg > 0 and actual_chg > 0) or (us_chg < 0 and actual_chg < 0):
                large_aligned += 1
    
    if total_s > 0:
        rate = aligned / total_s * 100
        large_rate = f"{large_aligned}/{large_n}({large_aligned/large_n*100:.1f}%)" if large_n > 0 else "N/A"
        print(f"    {us_stock:6s}: 全部={aligned}/{total_s}({rate:.1f}%) | 大波动(≥2%)={large_rate}")


# ═══════════════════════════════════════════════════════════
# 分析五：美股大幅波动日的A股板块联动深度分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"五、美股大幅波动日(≥1.5%)的A股板块联动分析")
print(f"{'=' * 80}")

# 找出美股大幅波动日
ndx_map = us_indices.get('NDX', {})
large_move_dates = {d: chg for d, chg in ndx_map.items() if abs(chg) >= 1.5}
print(f"  NDX大幅波动日: {len(large_move_dates)}天")

# 对每个大幅波动日，分析A股各板块次日表现
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    
    us_up_a_up, us_up_n = 0, 0
    us_down_a_down, us_down_n = 0, 0
    model_ok_on_large = 0
    model_n_on_large = 0
    
    for d in sec_data:
        us_date = find_us_prev_date(d['评分日'], ndx_map)
        if not us_date or us_date not in large_move_dates:
            continue
        
        us_chg = large_move_dates[us_date]
        actual_chg = parse_chg(d['实际涨跌'])
        
        model_n_on_large += 1
        if d['宽松正确'] == '✓':
            model_ok_on_large += 1
        
        if us_chg > 0:
            us_up_n += 1
            if actual_chg > 0:
                us_up_a_up += 1
        else:
            us_down_n += 1
            if actual_chg < 0:
                us_down_a_down += 1
    
    if model_n_on_large > 0:
        up_rate = f"{us_up_a_up}/{us_up_n}({us_up_a_up/us_up_n*100:.1f}%)" if us_up_n > 0 else "N/A"
        down_rate = f"{us_down_a_down}/{us_down_n}({us_down_a_down/us_down_n*100:.1f}%)" if us_down_n > 0 else "N/A"
        model_rate = model_ok_on_large / model_n_on_large * 100
        print(f"  {sector:6s}: 美涨→A涨={up_rate} | 美跌→A跌={down_rate} | 模型准确率={model_ok_on_large}/{model_n_on_large}({model_rate:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析六：美股隔夜信号当前使用方式的有效性验证
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"六、美股隔夜信号(us_overnight)在模型中的使用效果验证")
print(f"{'=' * 80}")

# 分析当前模型中us_overnight因子的使用效果
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    
    # 按美股隔夜涨跌分组
    us_up_ok, us_up_n = 0, 0
    us_down_ok, us_down_n = 0, 0
    us_flat_ok, us_flat_n = 0, 0
    
    for d in sec_data:
        us_chg = d.get('美股涨跌(%)')
        if us_chg is None:
            continue
        
        if us_chg > 0.3:
            us_up_n += 1
            if d['宽松正确'] == '✓':
                us_up_ok += 1
        elif us_chg < -0.3:
            us_down_n += 1
            if d['宽松正确'] == '✓':
                us_down_ok += 1
        else:
            us_flat_n += 1
            if d['宽松正确'] == '✓':
                us_flat_ok += 1
    
    up_rate = f"{us_up_ok}/{us_up_n}({us_up_ok/us_up_n*100:.1f}%)" if us_up_n > 0 else "N/A"
    down_rate = f"{us_down_ok}/{us_down_n}({us_down_ok/us_down_n*100:.1f}%)" if us_down_n > 0 else "N/A"
    flat_rate = f"{us_flat_ok}/{us_flat_n}({us_flat_ok/us_flat_n*100:.1f}%)" if us_flat_n > 0 else "N/A"
    print(f"  {sector:6s}: 美涨日模型准确={up_rate} | 美跌日模型准确={down_rate} | 美平日模型准确={flat_rate}")


# ═══════════════════════════════════════════════════════════
# 分析七：资金流向 + 美股信号 组合效果
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"七、资金流向 + 美股信号 组合效果分析")
print(f"{'=' * 80}")

# 当资金流向和美股信号同向时，预测准确率是否更高？
combo_stats = defaultdict(lambda: {'ok': 0, 'n': 0})

for d in details:
    code = d['代码']
    score_date = d['评分日']
    sector = d['板块']
    actual_chg = parse_chg(d['实际涨跌'])
    
    # 资金流向信号
    fund_signal = 0
    if code in fund_flow_cache:
        ff_today = fund_flow_cache[code].get(score_date)
        if ff_today:
            big_net = ff_today.get('big_net') or 0
            if big_net > 500:
                fund_signal = 1
            elif big_net < -500:
                fund_signal = -1
    
    # 美股隔夜信号
    us_chg = d.get('美股涨跌(%)')
    us_signal = 0
    if us_chg is not None:
        if us_chg > 0.5:
            us_signal = 1
        elif us_chg < -0.5:
            us_signal = -1
    
    # 组合分类
    if fund_signal > 0 and us_signal > 0:
        combo = '资金+美股双看涨'
    elif fund_signal < 0 and us_signal < 0:
        combo = '资金+美股双看跌'
    elif fund_signal > 0 and us_signal < 0:
        combo = '资金看涨+美股看跌'
    elif fund_signal < 0 and us_signal > 0:
        combo = '资金看跌+美股看涨'
    elif fund_signal != 0:
        combo = '仅资金有信号'
    elif us_signal != 0:
        combo = '仅美股有信号'
    else:
        combo = '双无信号'
    
    combo_stats[combo]['n'] += 1
    if d['宽松正确'] == '✓':
        combo_stats[combo]['ok'] += 1

for combo_name in ['资金+美股双看涨', '资金+美股双看跌', '资金看涨+美股看跌',
                    '资金看跌+美股看涨', '仅资金有信号', '仅美股有信号', '双无信号']:
    s = combo_stats[combo_name]
    if s['n'] > 0:
        rate = s['ok'] / s['n'] * 100
        print(f"  {combo_name:18s}: {s['ok']}/{s['n']} ({rate:.1f}%)")

# 按板块的组合效果
print(f"\n  按板块的双信号同向效果:")
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    both_bullish_ok, both_bullish_n = 0, 0
    both_bearish_ok, both_bearish_n = 0, 0
    
    for d in sec_data:
        code = d['代码']
        score_date = d['评分日']
        
        fund_signal = 0
        if code in fund_flow_cache:
            ff_today = fund_flow_cache[code].get(score_date)
            if ff_today:
                big_net = ff_today.get('big_net') or 0
                if big_net > 500:
                    fund_signal = 1
                elif big_net < -500:
                    fund_signal = -1
        
        us_chg = d.get('美股涨跌(%)')
        us_signal = 0
        if us_chg is not None:
            if us_chg > 0.5:
                us_signal = 1
            elif us_chg < -0.5:
                us_signal = -1
        
        if fund_signal > 0 and us_signal > 0:
            both_bullish_n += 1
            if d['宽松正确'] == '✓':
                both_bullish_ok += 1
        elif fund_signal < 0 and us_signal < 0:
            both_bearish_n += 1
            if d['宽松正确'] == '✓':
                both_bearish_ok += 1
    
    bb = f"{both_bullish_ok}/{both_bullish_n}({both_bullish_ok/both_bullish_n*100:.1f}%)" if both_bullish_n > 0 else "N/A"
    bc = f"{both_bearish_ok}/{both_bearish_n}({both_bearish_ok/both_bearish_n*100:.1f}%)" if both_bearish_n > 0 else "N/A"
    print(f"    {sector:6s}: 双看涨模型准确={bb} | 双看跌模型准确={bc}")


# ═══════════════════════════════════════════════════════════
# 分析八：资金流向反转信号（资金流出但次日涨 / 资金流入但次日跌）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"八、资金流向反转效应分析（资金流向与次日涨跌相反的情况）")
print(f"{'=' * 80}")

# 某些板块可能存在"主力出货后反弹"或"主力吸筹后洗盘"的模式
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    
    # 大单净流出 → 次日涨
    outflow_then_up_n, outflow_then_up = 0, 0
    # 大单净流入 → 次日跌
    inflow_then_down_n, inflow_then_down = 0, 0
    # 正常: 流入→涨, 流出→跌
    normal_n, normal_ok = 0, 0
    
    for d in sec_data:
        code = d['代码']
        score_date = d['评分日']
        actual_chg = parse_chg(d['实际涨跌'])
        
        if code not in fund_flow_cache:
            continue
        ff_today = fund_flow_cache[code].get(score_date)
        if not ff_today:
            continue
        
        big_net = ff_today.get('big_net') or 0
        big_net_pct = abs(ff_today.get('big_net_pct') or 0)
        
        if big_net_pct < 1:  # 忽略微弱信号
            continue
        
        if big_net < 0 and actual_chg > 0.3:
            outflow_then_up += 1
            outflow_then_up_n += 1
        elif big_net < 0:
            outflow_then_up_n += 1
        
        if big_net > 0 and actual_chg < -0.3:
            inflow_then_down += 1
            inflow_then_down_n += 1
        elif big_net > 0:
            inflow_then_down_n += 1
        
        if (big_net > 0 and actual_chg > 0) or (big_net < 0 and actual_chg < 0):
            normal_ok += 1
        normal_n += 1
    
    out_up = f"{outflow_then_up}/{outflow_then_up_n}({outflow_then_up/outflow_then_up_n*100:.1f}%)" if outflow_then_up_n > 0 else "N/A"
    in_down = f"{inflow_then_down}/{inflow_then_down_n}({inflow_then_down/inflow_then_down_n*100:.1f}%)" if inflow_then_down_n > 0 else "N/A"
    normal = f"{normal_ok}/{normal_n}({normal_ok/normal_n*100:.1f}%)" if normal_n > 0 else "N/A"
    print(f"  {sector:6s}: 流出→涨={out_up} | 流入→跌={in_down} | 正常方向一致={normal}")

# ═══════════════════════════════════════════════════════════
# 分析九：模型当前不足总结 + 改进建议
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"九、模型不足总结与改进建议")
print(f"{'=' * 80}")

# 计算各维度的理论增益
print(f"\n  当前基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"  目标: 65% = {int(total*0.65)}/{total}")
print(f"  差距: {int(total*0.65) - loose_ok} 个样本")

# 最弱板块
print(f"\n  各板块准确率排名:")
sector_rates = []
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    sec_ok = sum(1 for d in sec_data if d['宽松正确'] == '✓')
    rate = sec_ok / len(sec_data) * 100
    sector_rates.append((sector, sec_ok, len(sec_data), rate))
    print(f"    {sector:6s}: {sec_ok}/{len(sec_data)} ({rate:.1f}%)")

# 最弱的板块×置信度组合
print(f"\n  最弱的板块×置信度组合（准确率<55%且样本≥20）:")
conf_sec = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    key = (d['板块'], d['置信度'])
    conf_sec[key]['n'] += 1
    if d['宽松正确'] == '✓':
        conf_sec[key]['ok'] += 1

for key, s in sorted(conf_sec.items(), key=lambda x: x[1]['ok']/max(x[1]['n'],1)):
    rate = s['ok'] / s['n'] * 100
    if s['n'] >= 20 and rate < 55:
        print(f"    {key[0]:6s} {key[1]:6s}: {s['ok']}/{s['n']} ({rate:.1f}%)")

print(f"\n{'=' * 80}")
print(f"分析完成")
print(f"{'=' * 80}")
