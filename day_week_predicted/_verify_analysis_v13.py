#!/usr/bin/env python3
"""
v13 分析结论验证脚本 — 从原始数据独立计算，交叉验证分析报告的每个关键发现

验证方法：
1. 从DB直接查询K线和资金流向原始数据
2. 从回测结果JSON独立重算
3. 对比分析报告中的数字是否一致
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

print("=" * 80)
print("验证一：回测结果JSON数据完整性检查")
print("=" * 80)

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']
total = len(details)


def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

# 1. 基础数据完整性
print(f"\n  总样本数: {total}")
print(f"  回测区间: {data.get('回测区间', 'N/A')}")
print(f"  股票数: {data.get('股票数', 'N/A')}")

# 验证每条记录的必要字段
required_fields = ['代码', '名称', '板块', '评分日', '预测日', '预测方向', '实际涨跌', '宽松正确', '融合信号', '置信度']
missing_fields = defaultdict(int)
for d in details:
    for f in required_fields:
        if f not in d:
            missing_fields[f] += 1

if missing_fields:
    print(f"  ⚠️ 缺失字段: {dict(missing_fields)}")
else:
    print(f"  ✅ 所有 {total} 条记录字段完整")

# 验证宽松正确的计算逻辑
recomputed_loose = 0
recomputed_strict = 0
loose_mismatch = 0
for d in details:
    actual_chg = parse_chg(d['实际涨跌'])
    pred = d['预测方向']
    
    # 严格正确
    if actual_chg > 0.3:
        actual_dir = '上涨'
    elif actual_chg < -0.3:
        actual_dir = '下跌'
    else:
        actual_dir = '横盘震荡'
    
    strict_ok = (pred == actual_dir)
    
    # 宽松正确
    loose_ok = strict_ok
    if not strict_ok:
        if pred == '上涨' and actual_chg >= 0:
            loose_ok = True
        elif pred == '下跌' and actual_chg <= 0:
            loose_ok = True
    
    if loose_ok:
        recomputed_loose += 1
    if strict_ok:
        recomputed_strict += 1
    
    # 对比JSON中的标记
    json_loose = (d['宽松正确'] == '✓')
    if json_loose != loose_ok:
        loose_mismatch += 1

reported_loose = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"\n  JSON中宽松正确: {reported_loose}/{total} ({reported_loose/total*100:.1f}%)")
print(f"  独立重算宽松正确: {recomputed_loose}/{total} ({recomputed_loose/total*100:.1f}%)")
print(f"  独立重算严格正确: {recomputed_strict}/{total} ({recomputed_strict/total*100:.1f}%)")
if loose_mismatch == 0:
    print(f"  ✅ 宽松正确标记与独立重算完全一致")
else:
    print(f"  ⚠️ 宽松正确标记不一致: {loose_mismatch} 条")

# 验证报告中的总体准确率
reported_total_loose = data.get('总体准确率(宽松)', '')
print(f"\n  报告总体准确率(宽松): {reported_total_loose}")
print(f"  验证: {recomputed_loose}/{total} ({recomputed_loose/total*100:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 验证二：板块准确率独立重算
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证二：板块准确率独立重算 vs 分析报告")
print(f"{'=' * 80}")

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
# 分析报告中的数字
reported_sector_rates = {
    '科技': (269, 437, 61.6),
    '有色金属': (235, 382, 61.5),
    '汽车': (224, 391, 57.3),
    '新能源': (248, 447, 55.5),
    '医药': (229, 392, 58.4),
    '化工': (235, 392, 59.9),
    '制造': (204, 336, 60.7),
}

all_match = True
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    sec_ok = sum(1 for d in sec_data if d['宽松正确'] == '✓')
    sec_n = len(sec_data)
    rate = sec_ok / sec_n * 100 if sec_n > 0 else 0
    
    r_ok, r_n, r_rate = reported_sector_rates.get(sector, (0, 0, 0))
    match = (sec_ok == r_ok and sec_n == r_n)
    marker = "✅" if match else "⚠️"
    if not match:
        all_match = False
    
    print(f"  {sector:6s}: 重算={sec_ok}/{sec_n}({rate:.1f}%) | 报告={r_ok}/{r_n}({r_rate:.1f}%) {marker}")

if all_match:
    print(f"  ✅ 所有板块准确率与报告完全一致")


# ═══════════════════════════════════════════════════════════
# 验证三：从DB原始数据验证资金流向分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证三：从DB原始数据验证资金流向分析结论")
print(f"{'=' * 80}")

from dao.stock_fund_flow_dao import get_fund_flow_by_code

# 加载资金流向数据
stock_codes = list(set(d['代码'] for d in details))
fund_flow_cache = {}
loaded_count = 0
for code in stock_codes:
    ff = get_fund_flow_by_code(code, limit=200)
    if ff:
        fund_flow_cache[code] = {str(r.get('date', '')): r for r in ff}
        loaded_count += 1

print(f"  从DB加载资金流向: {loaded_count}/{len(stock_codes)} 只股票")

# 验证发现1: 个股资金流向方向一致率
print(f"\n  验证【发现1】个股大单净额方向一致率:")
inflow_aligned, inflow_total = 0, 0
outflow_aligned, outflow_total = 0, 0

for d in details:
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
        inflow_total += 1
        if actual_chg > 0:
            inflow_aligned += 1
    elif big_net < 0:
        outflow_total += 1
        if actual_chg < 0:
            outflow_aligned += 1

in_rate = inflow_aligned / inflow_total * 100 if inflow_total > 0 else 0
out_rate = outflow_aligned / outflow_total * 100 if outflow_total > 0 else 0
print(f"    净流入→次日涨: {inflow_aligned}/{inflow_total} ({in_rate:.1f}%) [报告: 48.8%]")
print(f"    净流出→次日跌: {outflow_aligned}/{outflow_total} ({out_rate:.1f}%) [报告: 54.6%]")
print(f"    {'✅' if abs(in_rate - 48.8) < 0.5 else '⚠️'} 净流入一致率验证{'通过' if abs(in_rate - 48.8) < 0.5 else '偏差'}")
print(f"    {'✅' if abs(out_rate - 54.6) < 0.5 else '⚠️'} 净流出一致率验证{'通过' if abs(out_rate - 54.6) < 0.5 else '偏差'}")

# 验证发现2: 资金流向强度
print(f"\n  验证【发现2】资金流向强度 vs 有效性:")
strength_stats = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
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
    
    if big_net_pct > 5:
        bucket = '强(>5%)'
    elif big_net_pct > 2:
        bucket = '中(2-5%)'
    elif big_net_pct > 0.5:
        bucket = '弱(0.5-2%)'
    else:
        bucket = '微(<0.5%)'
    
    strength_stats[bucket]['n'] += 1
    if (big_net > 0 and actual_chg > 0) or (big_net < 0 and actual_chg < 0):
        strength_stats[bucket]['ok'] += 1

reported_strength = {'强(>5%)': 50.6, '中(2-5%)': 52.9, '弱(0.5-2%)': 57.1, '微(<0.5%)': 53.4}
for bucket in ['强(>5%)', '中(2-5%)', '弱(0.5-2%)', '微(<0.5%)']:
    s = strength_stats[bucket]
    rate = s['ok'] / s['n'] * 100 if s['n'] > 0 else 0
    r = reported_strength[bucket]
    match = abs(rate - r) < 0.5
    print(f"    {bucket:12s}: {s['ok']}/{s['n']}({rate:.1f}%) [报告: {r}%] {'✅' if match else '⚠️'}")


# 验证发现3: 按板块的资金流向有效性
print(f"\n  验证【发现3】按板块的资金流向方向一致率:")
reported_sector_fund = {
    '科技': {'in': 40.5, 'out': 53.2},
    '有色金属': {'in': 55.3, 'out': 53.8},
    '汽车': {'in': 37.9, 'out': 57.4},
    '新能源': {'in': 50.0, 'out': 56.3},
    '医药': {'in': 48.4, 'out': 61.4},
    '化工': {'in': 56.6, 'out': 42.3},
    '制造': {'in': 50.0, 'out': 54.4},
}

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    in_ok, in_n = 0, 0
    out_ok, out_n = 0, 0
    
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
            in_n += 1
            if actual_chg > 0:
                in_ok += 1
        elif big_net < 0:
            out_n += 1
            if actual_chg < 0:
                out_ok += 1
    
    in_rate = in_ok / in_n * 100 if in_n > 0 else 0
    out_rate = out_ok / out_n * 100 if out_n > 0 else 0
    r = reported_sector_fund.get(sector, {})
    in_match = abs(in_rate - r.get('in', 0)) < 0.5
    out_match = abs(out_rate - r.get('out', 0)) < 0.5
    
    print(f"    {sector:6s}: 流入→涨={in_ok}/{in_n}({in_rate:.1f}%)[报告:{r.get('in',0)}%]{'✅' if in_match else '⚠️'} | "
          f"流出→跌={out_ok}/{out_n}({out_rate:.1f}%)[报告:{r.get('out',0)}%]{'✅' if out_match else '⚠️'}")


# ═══════════════════════════════════════════════════════════
# 验证四：从DB原始数据验证美股关联分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证四：从DB原始数据验证美股关联分析结论")
print(f"{'=' * 80}")

from service.eastmoney.indices.us_market_db_query import get_us_index_kline_range
from dao.us_market_dao import get_us_stock_kline_range

# 加载美股指数K线
us_indices = {}
for idx_code in ['NDX', 'SPX']:
    klines = get_us_index_kline_range(idx_code, start_date='2025-11-01', end_date='2026-03-10', limit=200)
    us_indices[idx_code] = {str(k['trade_date']): float(k.get('change_pct') or 0) for k in klines}
    print(f"  美股{idx_code}: {len(us_indices[idx_code])}条K线")

def find_us_prev_date(a_date_str, us_date_map, lookback=7):
    dt = datetime.strptime(a_date_str, '%Y-%m-%d')
    for offset in range(1, lookback + 1):
        prev = (dt - timedelta(days=offset)).strftime('%Y-%m-%d')
        if prev in us_date_map:
            return prev
    return None

# 验证发现7: NDX vs A股各板块方向一致率
print(f"\n  验证【发现7】NDX vs A股各板块方向一致率:")
reported_ndx = {
    '科技': 43.0, '有色金属': 50.3, '汽车': 41.4,
    '新能源': 48.1, '医药': 52.0, '化工': 49.7, '制造': 42.3,
}

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    aligned, total_s = 0, 0
    
    for d in sec_data:
        us_date = find_us_prev_date(d['评分日'], us_indices['NDX'])
        if not us_date:
            continue
        us_chg = us_indices['NDX'][us_date]
        actual_chg = parse_chg(d['实际涨跌'])
        
        total_s += 1
        if (us_chg > 0 and actual_chg > 0) or (us_chg < 0 and actual_chg < 0):
            aligned += 1
    
    rate = aligned / total_s * 100 if total_s > 0 else 0
    r = reported_ndx.get(sector, 0)
    match = abs(rate - r) < 0.5
    print(f"    NDX vs {sector:6s}: {aligned}/{total_s}({rate:.1f}%) [报告: {r}%] {'✅' if match else '⚠️'}")

# 验证发现8: SPX大波动时
print(f"\n  验证【发现8】SPX大波动(≥1%) vs A股方向一致率:")
reported_spx_large = {'科技': 34.7, '化工': 34.9}

for sector in ['科技', '化工']:
    sec_data = [d for d in details if d['板块'] == sector]
    aligned, total_s = 0, 0
    
    for d in sec_data:
        us_date = find_us_prev_date(d['评分日'], us_indices['SPX'])
        if not us_date:
            continue
        us_chg = us_indices['SPX'][us_date]
        if abs(us_chg) < 1.0:
            continue
        actual_chg = parse_chg(d['实际涨跌'])
        
        total_s += 1
        if (us_chg > 0 and actual_chg > 0) or (us_chg < 0 and actual_chg < 0):
            aligned += 1
    
    rate = aligned / total_s * 100 if total_s > 0 else 0
    r = reported_spx_large.get(sector, 0)
    match = abs(rate - r) < 0.5
    print(f"    SPX大波动 vs {sector:6s}: {aligned}/{total_s}({rate:.1f}%) [报告: {r}%] {'✅' if match else '⚠️'}")


# 验证美股半导体龙头
print(f"\n  验证【发现9】美股半导体龙头 vs A股科技板块:")
reported_us_semi = {
    'NVDA': 51.0, 'AMD': 44.4, 'TSM': 49.4, 'AVGO': 49.7, 'ASML': 50.6, 'AMAT': 51.0,
}

us_semi_klines = {}
for stock in ['NVDA', 'AMD', 'TSM', 'AVGO', 'ASML', 'AMAT']:
    try:
        klines = get_us_stock_kline_range(stock, start_date='2025-11-01', end_date='2026-03-10', limit=200)
        if klines:
            us_semi_klines[stock] = {str(k.get('trade_date', '')): float(k.get('change_pct') or 0) for k in klines}
    except Exception as e:
        logger.warning(f"加载美股{stock}失败: {e}")

tech_data = [d for d in details if d['板块'] == '科技']

for us_stock, us_kline_map in us_semi_klines.items():
    aligned, total_s = 0, 0
    
    for d in tech_data:
        us_date = find_us_prev_date(d['评分日'], us_kline_map)
        if not us_date:
            continue
        us_chg = us_kline_map[us_date]
        actual_chg = parse_chg(d['实际涨跌'])
        
        total_s += 1
        if (us_chg > 0 and actual_chg > 0) or (us_chg < 0 and actual_chg < 0):
            aligned += 1
    
    rate = aligned / total_s * 100 if total_s > 0 else 0
    r = reported_us_semi.get(us_stock, 0)
    match = abs(rate - r) < 0.5
    print(f"    {us_stock:6s} vs A股科技: {aligned}/{total_s}({rate:.1f}%) [报告: {r}%] {'✅' if match else '⚠️'}")


# ═══════════════════════════════════════════════════════════
# 验证五：组合信号效果
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证五：资金流向 + 美股信号组合效果")
print(f"{'=' * 80}")

reported_combo = {
    '资金+美股双看涨': 62.7,
    '资金+美股双看跌': 59.3,
    '资金看涨+美股看跌': 51.7,
    '资金看跌+美股看涨': 67.8,
    '仅资金有信号': 53.4,
    '仅美股有信号': 59.4,
    '双无信号': 61.1,
}

combo_stats = defaultdict(lambda: {'ok': 0, 'n': 0})

for d in details:
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
    rate = s['ok'] / s['n'] * 100 if s['n'] > 0 else 0
    r = reported_combo.get(combo_name, 0)
    match = abs(rate - r) < 0.5
    print(f"  {combo_name:18s}: {s['ok']}/{s['n']}({rate:.1f}%) [报告: {r}%] {'✅' if match else '⚠️'}")


# ═══════════════════════════════════════════════════════════
# 验证六：连续资金流向趋势
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证六：连续资金流向趋势")
print(f"{'=' * 80}")

reported_trend = {
    ('连续3日净流入', '方向'): 51.7,
    ('连续3日净流出', '方向'): 57.1,
    ('连续5日净流入', '方向'): 38.1,
    ('连续5日净流出', '方向'): 56.6,
}

for n_days in [3, 5]:
    trend_stats = defaultdict(lambda: {'ok': 0, 'n': 0})
    
    for d in details:
        code = d['代码']
        score_date = d['评分日']
        actual_chg = parse_chg(d['实际涨跌'])
        
        if code not in fund_flow_cache:
            continue
        
        ff_map = fund_flow_cache[code]
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
            continue
        
        trend_stats[trend]['n'] += 1
        if (trend.endswith('净流入') and actual_chg > 0) or \
           (trend.endswith('净流出') and actual_chg < 0):
            trend_stats[trend]['ok'] += 1
    
    for trend_name in [f'连续{n_days}日净流入', f'连续{n_days}日净流出']:
        s = trend_stats[trend_name]
        rate = s['ok'] / s['n'] * 100 if s['n'] > 0 else 0
        r = reported_trend.get((trend_name, '方向'), 0)
        match = abs(rate - r) < 0.5
        print(f"  {trend_name}: {s['ok']}/{s['n']}({rate:.1f}%) [报告: {r}%] {'✅' if match else '⚠️'}")


# ═══════════════════════════════════════════════════════════
# 验证七：从K线原始数据抽样验证实际涨跌幅
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证七：从DB K线原始数据抽样验证实际涨跌幅计算")
print(f"{'=' * 80}")

from dao.stock_kline_dao import get_kline_data

# 随机抽取10个样本，从DB查K线验证涨跌幅
import random
random.seed(42)
sample_indices = random.sample(range(total), min(20, total))

mismatch_count = 0
verified_count = 0

for idx in sample_indices:
    d = details[idx]
    code = d['代码']
    score_date = d['评分日']
    next_date = d['预测日']
    reported_chg = parse_chg(d['实际涨跌'])
    
    # 从DB查K线
    klines = get_kline_data(code, start_date=score_date, end_date=next_date)
    klines = [k for k in klines if (k.get('trading_volume') or 0) > 0]
    
    if len(klines) < 2:
        continue
    
    # 找到score_date和next_date的K线
    score_kline = None
    next_kline = None
    for k in klines:
        if k['date'] == score_date:
            score_kline = k
        if k['date'] == next_date:
            next_kline = k
    
    if not score_kline or not next_kline:
        continue
    
    base_close = score_kline['close_price']
    next_close = next_kline['close_price']
    
    if base_close <= 0:
        continue
    
    db_chg = round((next_close - base_close) / base_close * 100, 2)
    diff = abs(db_chg - reported_chg)
    verified_count += 1
    
    if diff > 0.05:
        mismatch_count += 1
        print(f"  ⚠️ {d['名称']}({code}) {score_date}→{next_date}: "
              f"报告={reported_chg:+.2f}% DB={db_chg:+.2f}% 差异={diff:.2f}%")
    else:
        print(f"  ✅ {d['名称']}({code}) {score_date}→{next_date}: "
              f"报告={reported_chg:+.2f}% DB={db_chg:+.2f}%")

print(f"\n  抽样验证: {verified_count}个样本, {verified_count - mismatch_count}个一致, {mismatch_count}个偏差")
if mismatch_count == 0:
    print(f"  ✅ 所有抽样的实际涨跌幅与DB原始数据完全一致")
else:
    print(f"  ⚠️ {mismatch_count}个样本存在偏差（可能是四舍五入差异）")


# ═══════════════════════════════════════════════════════════
# 验证八：美股隔夜信号在模型中的效果
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证八：美股隔夜信号在模型中的效果")
print(f"{'=' * 80}")

reported_us_effect = {
    '科技': {'涨': 62.8, '跌': 54.0, '平': 68.3},
    '有色金属': {'涨': 66.7, '跌': 62.6, '平': 53.2},
    '汽车': {'涨': 64.0, '跌': 50.8, '平': 56.3},
    '新能源': {'涨': 53.1, '跌': 52.5, '平': 61.1},
    '医药': {'涨': 55.0, '跌': 57.9, '平': 62.7},
    '化工': {'涨': 64.3, '跌': 59.5, '平': 55.6},
    '制造': {'涨': 58.3, '跌': 60.2, '平': 64.6},
}

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    
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
    
    up_rate = us_up_ok / us_up_n * 100 if us_up_n > 0 else 0
    down_rate = us_down_ok / us_down_n * 100 if us_down_n > 0 else 0
    flat_rate = us_flat_ok / us_flat_n * 100 if us_flat_n > 0 else 0
    
    r = reported_us_effect.get(sector, {})
    up_match = abs(up_rate - r.get('涨', 0)) < 0.5
    down_match = abs(down_rate - r.get('跌', 0)) < 0.5
    flat_match = abs(flat_rate - r.get('平', 0)) < 0.5
    
    print(f"  {sector:6s}: 美涨={us_up_ok}/{us_up_n}({up_rate:.1f}%)[{r.get('涨',0)}%]{'✅' if up_match else '⚠️'} | "
          f"美跌={us_down_ok}/{us_down_n}({down_rate:.1f}%)[{r.get('跌',0)}%]{'✅' if down_match else '⚠️'} | "
          f"美平={us_flat_ok}/{us_flat_n}({flat_rate:.1f}%)[{r.get('平',0)}%]{'✅' if flat_match else '⚠️'}")


# ═══════════════════════════════════════════════════════════
# 验证九：板块聚合资金流向
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证九：板块聚合资金流向信号")
print(f"{'=' * 80}")

reported_sector_agg = {
    '科技': {'in': 41.4, 'out': 53.7},
    '有色金属': {'in': 59.0, 'out': 57.3},
    '汽车': {'in': 51.8, 'out': 63.3},
    '新能源': {'in': 46.3, 'out': 53.9},
    '医药': {'in': 35.7, 'out': 54.8},
    '化工': {'in': 53.1, 'out': 38.8},
    '制造': {'in': 53.6, 'out': 59.5},
}

# 按(板块, 日期)聚合
sector_date_fund = defaultdict(lambda: {'total_big_net': 0, 'stock_count': 0})
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

for sector in sectors:
    in_ok, in_n = 0, 0
    out_ok, out_n = 0, 0
    
    for d in details:
        if d['板块'] != sector:
            continue
        key = (sector, d['评分日'])
        if key not in sector_date_fund:
            continue
        
        agg = sector_date_fund[key]
        actual_chg = parse_chg(d['实际涨跌'])
        
        if agg['total_big_net'] > 0:
            in_n += 1
            if actual_chg > 0:
                in_ok += 1
        elif agg['total_big_net'] < 0:
            out_n += 1
            if actual_chg < 0:
                out_ok += 1
    
    in_rate = in_ok / in_n * 100 if in_n > 0 else 0
    out_rate = out_ok / out_n * 100 if out_n > 0 else 0
    r = reported_sector_agg.get(sector, {})
    in_match = abs(in_rate - r.get('in', 0)) < 0.5
    out_match = abs(out_rate - r.get('out', 0)) < 0.5
    
    print(f"  {sector:6s}: 板块净流入→涨={in_ok}/{in_n}({in_rate:.1f}%)[{r.get('in',0)}%]{'✅' if in_match else '⚠️'} | "
          f"板块净流出→跌={out_ok}/{out_n}({out_rate:.1f}%)[{r.get('out',0)}%]{'✅' if out_match else '⚠️'}")


# ═══════════════════════════════════════════════════════════
# 验证十：资金流向反转效应
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证十：资金流向反转效应")
print(f"{'=' * 80}")

reported_reversal = {
    '科技': {'out_up': 42.7, 'in_down': 52.1, 'normal': 49.0},
    '有色金属': {'out_up': 44.4, 'in_down': 45.6, 'normal': 53.3},
    '汽车': {'out_up': 36.9, 'in_down': 63.0, 'normal': 50.0},
    '新能源': {'out_up': 39.8, 'in_down': 46.2, 'normal': 53.9},
    '医药': {'out_up': 29.5, 'in_down': 39.6, 'normal': 57.1},
    '化工': {'out_up': 56.2, 'in_down': 43.2, 'normal': 48.6},
    '制造': {'out_up': 41.4, 'in_down': 47.9, 'normal': 51.9},
}

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    
    outflow_then_up_n, outflow_then_up = 0, 0
    inflow_then_down_n, inflow_then_down = 0, 0
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
        
        if big_net_pct < 1:
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
    
    out_up_rate = outflow_then_up / outflow_then_up_n * 100 if outflow_then_up_n > 0 else 0
    in_down_rate = inflow_then_down / inflow_then_down_n * 100 if inflow_then_down_n > 0 else 0
    normal_rate = normal_ok / normal_n * 100 if normal_n > 0 else 0
    
    r = reported_reversal.get(sector, {})
    out_match = abs(out_up_rate - r.get('out_up', 0)) < 0.5
    in_match = abs(in_down_rate - r.get('in_down', 0)) < 0.5
    norm_match = abs(normal_rate - r.get('normal', 0)) < 0.5
    
    print(f"  {sector:6s}: 流出→涨={outflow_then_up}/{outflow_then_up_n}({out_up_rate:.1f}%)[{r.get('out_up',0)}%]{'✅' if out_match else '⚠️'} | "
          f"流入→跌={inflow_then_down}/{inflow_then_down_n}({in_down_rate:.1f}%)[{r.get('in_down',0)}%]{'✅' if in_match else '⚠️'} | "
          f"正常={normal_ok}/{normal_n}({normal_rate:.1f}%)[{r.get('normal',0)}%]{'✅' if norm_match else '⚠️'}")


# ═══════════════════════════════════════════════════════════
# 验证十一：美股大幅波动日联动分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证十一：美股大幅波动日(NDX≥1.5%)联动分析")
print(f"{'=' * 80}")

ndx_map = us_indices.get('NDX', {})
large_move_dates = {d: chg for d, chg in ndx_map.items() if abs(chg) >= 1.5}
print(f"  NDX大幅波动日: {len(large_move_dates)}天")
for dt, chg in sorted(large_move_dates.items()):
    print(f"    {dt}: {chg:+.2f}%")

reported_large_move = {
    '科技': {'model': 60.9},
    '有色金属': {'model': 61.8},
    '汽车': {'model': 53.6},
    '新能源': {'model': 39.1},
    '医药': {'model': 55.4},
    '化工': {'model': 75.0},
    '制造': {'model': 47.9},
}

for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    model_ok, model_n = 0, 0
    
    for d in sec_data:
        us_date = find_us_prev_date(d['评分日'], ndx_map)
        if not us_date or us_date not in large_move_dates:
            continue
        
        model_n += 1
        if d['宽松正确'] == '✓':
            model_ok += 1
    
    rate = model_ok / model_n * 100 if model_n > 0 else 0
    r = reported_large_move.get(sector, {}).get('model', 0)
    match = abs(rate - r) < 0.5
    print(f"  {sector:6s}: 模型准确={model_ok}/{model_n}({rate:.1f}%) [报告: {r}%] {'✅' if match else '⚠️'}")


# ═══════════════════════════════════════════════════════════
# 最终汇总
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"验证汇总")
print(f"{'=' * 80}")
print(f"""
验证维度:
  1. 回测结果JSON数据完整性 — 字段完整性 + 宽松正确标记重算
  2. 板块准确率独立重算 — 7个板块逐一对比
  3. DB资金流向原始数据 — 发现1/2/3 资金流向方向一致率
  4. DB美股K线原始数据 — 发现7/8/9 美股关联分析
  5. 组合信号效果 — 发现11 资金+美股组合
  6. 连续资金流向趋势 — 发现5 连续流入/流出
  7. K线原始数据抽样 — 20个样本涨跌幅交叉验证
  8. 美股隔夜信号效果 — 发现10 美涨/美跌/美平日
  9. 板块聚合资金流向 — 发现4 板块级聚合信号
  10. 资金流向反转效应 — 发现13 反转信号
  11. 美股大幅波动日联动 — 发现8 大波动日分析

数据来源:
  - 回测结果: data_results/backtest_prediction_enhanced_v9_50stocks_result.json
  - K线数据: stock_kline 表 (MySQL)
  - 资金流向: stock_fund_flow 表 (MySQL)
  - 美股指数: us_index_kline 表 (MySQL)
  - 美股个股: us_stock_kline 表 (MySQL)
""")
