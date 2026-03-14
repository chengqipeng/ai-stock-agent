#!/usr/bin/env python3
"""
v18 融资融券+大宗交易 深度分析：
分析新增融资融券和大宗交易数据是否能将预测准确率提升至65%以上。

核心思路：
1. 从东方财富API批量获取50只回测股票的融资融券历史数据
2. 从同花顺获取大宗交易数据（页面仅展示最近几条，覆盖有限）
3. 计算融资融券衍生信号（融资净买入变化、融资余额趋势、融券余量变化等）
4. 分析各信号维度的方向预测力（按板块）
5. 组合信号分析（融资融券 × combined × 板块）
6. 时间序列验证（前半训练→后半测试）+ 留一日交叉验证
7. 评估是否能达到65%目标
"""
import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime

logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

print(f"{'=' * 80}")
print(f"v18 融资融券+大宗交易 深度分析")
print(f"{'=' * 80}")

# ═══════════════════════════════════════════════════════════
# 加载回测数据
# ═══════════════════════════════════════════════════════════
with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    bt_data = json.load(f)
details = bt_data['逐日详情']
total = len(details)

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_ge0'] = d['_actual'] >= 0
    d['_le0'] = d['_actual'] <= 0
    try:
        d['_wd'] = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
    except:
        d['_wd'] = -1

loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"当前基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"目标65%: {int(total*0.65)}/{total}, 差距: {int(total*0.65) - loose_ok}个样本")

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
all_dates = sorted(set(d['评分日'] for d in details))
bt_codes = sorted(set(d['代码'] for d in details))
code_to_name = {}
code_to_sector = {}
for d in details:
    code_to_name[d['代码']] = d['名称']
    code_to_sector[d['代码']] = d['板块']

print(f"回测股票: {len(bt_codes)}只, 日期: {all_dates[0]}~{all_dates[-1]}, 共{len(all_dates)}个交易日")


# ═══════════════════════════════════════════════════════════
# 第1步: 批量获取融资融券历史数据（东方财富API）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第1步: 批量获取融资融券历史数据")
print(f"{'=' * 80}")

from common.utils.stock_info_utils import get_stock_info_by_code
from service.eastmoney.stock_info.stock_margin_trading import get_margin_trading_data

import time

# 融资融券数据: {stock_code: {date: row_dict}}
margin_data_map = {}
margin_fetch_ok = 0
margin_fetch_fail = 0

async def fetch_all_margin():
    global margin_fetch_ok, margin_fetch_fail
    for i, code in enumerate(bt_codes):
        try:
            stock_info = get_stock_info_by_code(code)
            if not stock_info:
                margin_fetch_fail += 1
                continue
            data = await get_margin_trading_data(stock_info, page_size=100)
            if data:
                date_map = {}
                for row in data:
                    dt = row.get('交易日期', '')
                    if dt:
                        date_map[dt] = row
                margin_data_map[code] = date_map
                margin_fetch_ok += 1
            else:
                margin_fetch_fail += 1
            if (i + 1) % 10 == 0:
                print(f"  进度: {i+1}/{len(bt_codes)}, 成功: {margin_fetch_ok}, 失败: {margin_fetch_fail}")
            await asyncio.sleep(0.2)  # 限速
        except Exception as e:
            margin_fetch_fail += 1
            print(f"  获取失败 {code}: {e}")

asyncio.run(fetch_all_margin())
print(f"融资融券数据获取完成: 成功{margin_fetch_ok}只, 失败{margin_fetch_fail}只")

# 统计覆盖率
margin_coverage = 0
for d in details:
    code = d['代码']
    score_date = d['评分日']
    if code in margin_data_map and score_date in margin_data_map[code]:
        margin_coverage += 1
print(f"融资融券数据覆盖率: {margin_coverage}/{total} ({margin_coverage/total*100:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 第2步: 计算融资融券衍生信号
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第2步: 计算融资融券衍生信号")
print(f"{'=' * 80}")

for d in details:
    code = d['代码']
    score_date = d['评分日']
    
    # 默认值
    d['_rz_net_buy'] = None        # 融资净买入(元)
    d['_rz_balance'] = None        # 融资余额(元)
    d['_rz_net_buy_pct'] = None    # 融资净买入/融资余额 (%)
    d['_rq_volume'] = None         # 融券余量(股)
    d['_rzrq_balance'] = None      # 融资融券余额(元)
    d['_rz_trend_3d'] = None       # 融资余额3日变化方向
    d['_rz_trend_5d'] = None       # 融资余额5日变化方向
    d['_rz_buy_intensity'] = None  # 融资买入强度(买入额/余额)
    d['_margin_signal'] = 0        # 综合融资融券信号 (-2 ~ +2)
    
    if code not in margin_data_map:
        continue
    
    date_map = margin_data_map[code]
    today = date_map.get(score_date)
    if not today:
        continue
    
    rz_balance = today.get('融资余额(元)')
    rz_buy = today.get('融资买入额(元)')
    rz_repay = today.get('融资偿还额(元)')
    rz_net = today.get('融资净买入(元)')
    rq_volume = today.get('融券余量(股)')
    rzrq_balance = today.get('融资融券余额(元)')
    
    if rz_balance and rz_balance > 0:
        d['_rz_balance'] = rz_balance
        d['_rz_net_buy'] = rz_net
        if rz_net is not None:
            d['_rz_net_buy_pct'] = rz_net / rz_balance * 100
        if rz_buy is not None:
            d['_rz_buy_intensity'] = rz_buy / rz_balance * 100
    
    d['_rq_volume'] = rq_volume
    d['_rzrq_balance'] = rzrq_balance
    
    # 计算融资余额趋势（需要历史数据）
    # 找到score_date在排序日期中的位置
    sorted_dates = sorted(date_map.keys())
    try:
        idx = sorted_dates.index(score_date)
    except ValueError:
        continue
    
    # 3日趋势
    if idx >= 3:
        rz_3d_ago = date_map.get(sorted_dates[idx - 3], {}).get('融资余额(元)')
        if rz_3d_ago and rz_balance:
            d['_rz_trend_3d'] = (rz_balance - rz_3d_ago) / rz_3d_ago * 100
    
    # 5日趋势
    if idx >= 5:
        rz_5d_ago = date_map.get(sorted_dates[idx - 5], {}).get('融资余额(元)')
        if rz_5d_ago and rz_balance:
            d['_rz_trend_5d'] = (rz_balance - rz_5d_ago) / rz_5d_ago * 100
    
    # 综合融资融券信号
    signal = 0
    if d['_rz_net_buy_pct'] is not None:
        if d['_rz_net_buy_pct'] > 1.0:
            signal += 1  # 融资大幅净买入 → 看涨
        elif d['_rz_net_buy_pct'] < -1.0:
            signal -= 1  # 融资大幅净卖出 → 看跌
    
    if d['_rz_trend_5d'] is not None:
        if d['_rz_trend_5d'] > 2.0:
            signal += 1  # 融资余额5日上升 → 看涨
        elif d['_rz_trend_5d'] < -2.0:
            signal -= 1  # 融资余额5日下降 → 看跌
    
    d['_margin_signal'] = signal

# 统计信号分布
signal_dist = defaultdict(int)
for d in details:
    signal_dist[d['_margin_signal']] += 1
print(f"融资融券信号分布:")
for s in sorted(signal_dist.keys()):
    print(f"  signal={s}: {signal_dist[s]}个样本")

has_margin = sum(1 for d in details if d['_rz_balance'] is not None)
print(f"有融资融券数据的样本: {has_margin}/{total} ({has_margin/total*100:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 第3步: 获取大宗交易数据（同花顺 + 东方财富）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"第3步: 获取大宗交易数据")
print(f"{'=' * 80}")

from service.jqka10.stock_block_trade_10jqka import get_block_trade_10jqka

# 大宗交易数据: {stock_code: [records]}
block_trade_map = {}
bt_fetch_ok = 0
bt_fetch_fail = 0

async def fetch_all_block_trade():
    global bt_fetch_ok, bt_fetch_fail
    for i, code in enumerate(bt_codes):
        try:
            stock_info = get_stock_info_by_code(code)
            if not stock_info:
                bt_fetch_fail += 1
                continue
            records = await get_block_trade_10jqka(stock_info)
            block_trade_map[code] = records
            if records:
                bt_fetch_ok += 1
            if (i + 1) % 10 == 0:
                print(f"  进度: {i+1}/{len(bt_codes)}, 有数据: {bt_fetch_ok}")
            await asyncio.sleep(0.3)  # 限速（同花顺反爬更严）
        except Exception as e:
            bt_fetch_fail += 1
            if (i + 1) % 10 == 0:
                print(f"  进度: {i+1}/{len(bt_codes)}, 失败: {bt_fetch_fail}")

asyncio.run(fetch_all_block_trade())
print(f"大宗交易数据获取完成: 有数据{bt_fetch_ok}只, 无数据/失败{bt_fetch_fail}只")

# 统计大宗交易覆盖情况
total_bt_records = sum(len(v) for v in block_trade_map.values())
print(f"大宗交易总记录数: {total_bt_records}")

# 为每个样本标记是否有近期大宗交易
for d in details:
    code = d['代码']
    score_date = d['评分日']
    d['_has_recent_bt'] = False
    d['_bt_premium_avg'] = None
    d['_bt_has_org'] = False
    d['_bt_count_30d'] = 0
    d['_bt_total_amount'] = 0
    
    records = block_trade_map.get(code, [])
    if not records:
        continue
    
    # 找30天内的大宗交易
    from datetime import datetime as dt, timedelta
    try:
        sd = dt.strptime(score_date, '%Y-%m-%d')
    except:
        continue
    
    recent = []
    for r in records:
        try:
            td = dt.strptime(r['trade_date'], '%Y-%m-%d')
            if 0 <= (sd - td).days <= 30:
                recent.append(r)
        except:
            continue
    
    if recent:
        d['_has_recent_bt'] = True
        d['_bt_count_30d'] = len(recent)
        d['_bt_total_amount'] = sum(r.get('amount', 0) or 0 for r in recent)
        premiums = [r['premium_rate'] for r in recent if r.get('premium_rate') is not None]
        if premiums:
            d['_bt_premium_avg'] = sum(premiums) / len(premiums)
        d['_bt_has_org'] = any('机构' in (r.get('buyer', '') + r.get('seller', '')) for r in recent)

has_bt = sum(1 for d in details if d['_has_recent_bt'])
print(f"有近30天大宗交易的样本: {has_bt}/{total} ({has_bt/total*100:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 分析1: 融资融券各维度的方向预测力（按板块）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析1: 融资融券各维度的方向预测力")
print(f"{'=' * 80}")

# 融资净买入方向 → 次日方向
print(f"\n── 融资净买入方向 → 次日方向 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_rz_net_buy'] is not None]
    if len(sec_data) < 20:
        continue
    
    rz_buy = [d for d in sec_data if d['_rz_net_buy'] > 0]
    rz_sell = [d for d in sec_data if d['_rz_net_buy'] < 0]
    
    for label, group in [('融资净买入', rz_buy), ('融资净卖出', rz_sell)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            le0 = sum(1 for d in group if d['_le0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%) "
                  f"次日<=0%={le0}({le0/len(group)*100:.1f}%)")

# 融资净买入强度 → 次日方向
print(f"\n── 融资净买入强度(净买入/余额%) → 次日方向 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_rz_net_buy_pct'] is not None]
    if len(sec_data) < 20:
        continue
    
    strong_buy = [d for d in sec_data if d['_rz_net_buy_pct'] > 1.0]
    mild_buy = [d for d in sec_data if 0 < d['_rz_net_buy_pct'] <= 1.0]
    mild_sell = [d for d in sec_data if -1.0 <= d['_rz_net_buy_pct'] < 0]
    strong_sell = [d for d in sec_data if d['_rz_net_buy_pct'] < -1.0]
    
    for label, group in [('强买>1%', strong_buy), ('弱买0~1%', mild_buy),
                          ('弱卖-1~0%', mild_sell), ('强卖<-1%', strong_sell)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")

# 融资余额5日趋势 → 次日方向
print(f"\n── 融资余额5日趋势 → 次日方向 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_rz_trend_5d'] is not None]
    if len(sec_data) < 20:
        continue
    
    up_strong = [d for d in sec_data if d['_rz_trend_5d'] > 2.0]
    up_mild = [d for d in sec_data if 0 < d['_rz_trend_5d'] <= 2.0]
    down_mild = [d for d in sec_data if -2.0 <= d['_rz_trend_5d'] < 0]
    down_strong = [d for d in sec_data if d['_rz_trend_5d'] < -2.0]
    
    for label, group in [('余额升>2%', up_strong), ('余额升0~2%', up_mild),
                          ('余额降-2~0%', down_mild), ('余额降<-2%', down_strong)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")

# 融资买入强度 → 次日方向
print(f"\n── 融资买入强度(买入额/余额%) → 次日方向 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_rz_buy_intensity'] is not None]
    if len(sec_data) < 20:
        continue
    
    # 按分位数分组
    intensities = sorted(d['_rz_buy_intensity'] for d in sec_data)
    q25 = intensities[len(intensities)//4]
    q75 = intensities[3*len(intensities)//4]
    
    high = [d for d in sec_data if d['_rz_buy_intensity'] > q75]
    low = [d for d in sec_data if d['_rz_buy_intensity'] < q25]
    mid = [d for d in sec_data if q25 <= d['_rz_buy_intensity'] <= q75]
    
    for label, group in [('买入强度高(Q4)', high), ('买入强度中(Q2-3)', mid), ('买入强度低(Q1)', low)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 分析2: 大宗交易信号的方向预测力
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析2: 大宗交易信号的方向预测力")
print(f"{'=' * 80}")

print(f"\n── 有/无近期大宗交易 → 次日方向 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    has = [d for d in sec_data if d['_has_recent_bt']]
    no = [d for d in sec_data if not d['_has_recent_bt']]
    
    for label, group in [('有大宗交易', has), ('无大宗交易', no)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")

print(f"\n── 大宗交易溢价/折价 → 次日方向 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_bt_premium_avg'] is not None]
    if len(sec_data) < 10:
        continue
    
    premium = [d for d in sec_data if d['_bt_premium_avg'] > 0]
    discount = [d for d in sec_data if d['_bt_premium_avg'] < 0]
    flat = [d for d in sec_data if d['_bt_premium_avg'] == 0]
    
    for label, group in [('溢价成交', premium), ('折价成交', discount), ('平价成交', flat)]:
        if len(group) >= 5:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")

print(f"\n── 机构参与大宗交易 → 次日方向 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_has_recent_bt']]
    if len(sec_data) < 10:
        continue
    
    org = [d for d in sec_data if d['_bt_has_org']]
    no_org = [d for d in sec_data if not d['_bt_has_org']]
    
    for label, group in [('机构参与', org), ('非机构', no_org)]:
        if len(group) >= 5:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"  {sec} {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 分析3: 融资融券 × combined信号 组合效果（关键分析）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析3: 融资融券 × combined信号 组合效果")
print(f"{'=' * 80}")

def get_combined_dir(d):
    c = d.get('融合信号', 0)
    if c > 0.5: return 'comb+'
    if c < -0.5: return 'comb-'
    return 'comb0'

def get_margin_dir(d):
    s = d['_margin_signal']
    if s > 0: return '融资看涨'
    if s < 0: return '融资看跌'
    return '融资中性'

def get_rz_net_dir(d):
    v = d.get('_rz_net_buy_pct')
    if v is None: return '无数据'
    if v > 0.5: return '净买入'
    if v < -0.5: return '净卖出'
    return '净中性'

def get_rz_trend_dir(d):
    v = d.get('_rz_trend_5d')
    if v is None: return '无数据'
    if v > 1.0: return '余额升'
    if v < -1.0: return '余额降'
    return '余额平'

for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_rz_balance'] is not None]
    if len(sec_data) < 30:
        print(f"\n  {sec}: 数据不足({len(sec_data)})")
        continue
    print(f"\n  {sec} (有融资融券数据: {len(sec_data)}):")
    
    # 融资净买入方向 × combined
    combos = defaultdict(lambda: {'n': 0, 'ge0': 0, 'le0': 0, 'model_ok': 0})
    for d in sec_data:
        rd = get_rz_net_dir(d)
        cb = get_combined_dir(d)
        key = f"{rd}+{cb}"
        combos[key]['n'] += 1
        if d['_ge0']: combos[key]['ge0'] += 1
        if d['_le0']: combos[key]['le0'] += 1
        if d['宽松正确'] == '✓': combos[key]['model_ok'] += 1
    
    for key in sorted(combos.keys()):
        s = combos[key]
        if s['n'] >= 10:
            best = max(s['ge0'], s['le0'])
            best_dir = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            model_rate = s['model_ok'] / s['n'] * 100
            optimal_rate = best / s['n'] * 100
            gap = optimal_rate - model_rate
            marker = '★' if gap > 5 else ''
            print(f"    {key}(n={s['n']}): 最优→{best_dir} {optimal_rate:.1f}% | 模型 {model_rate:.1f}% | gap={gap:+.1f}pp {marker}")

# 融资余额趋势 × combined
print(f"\n── 融资余额5日趋势 × combined ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_rz_trend_5d'] is not None]
    if len(sec_data) < 30:
        continue
    print(f"\n  {sec}:")
    
    combos = defaultdict(lambda: {'n': 0, 'ge0': 0, 'le0': 0, 'model_ok': 0})
    for d in sec_data:
        td = get_rz_trend_dir(d)
        cb = get_combined_dir(d)
        key = f"{td}+{cb}"
        combos[key]['n'] += 1
        if d['_ge0']: combos[key]['ge0'] += 1
        if d['_le0']: combos[key]['le0'] += 1
        if d['宽松正确'] == '✓': combos[key]['model_ok'] += 1
    
    for key in sorted(combos.keys()):
        s = combos[key]
        if s['n'] >= 10:
            best = max(s['ge0'], s['le0'])
            best_dir = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            model_rate = s['model_ok'] / s['n'] * 100
            optimal_rate = best / s['n'] * 100
            gap = optimal_rate - model_rate
            marker = '★' if gap > 5 else ''
            print(f"    {key}(n={s['n']}): 最优→{best_dir} {optimal_rate:.1f}% | 模型 {model_rate:.1f}% | gap={gap:+.1f}pp {marker}")


# ═══════════════════════════════════════════════════════════
# 分析4: 融资融券反转效应分析（关键：融资大幅买入后是否反转）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析4: 融资融券反转效应分析")
print(f"{'=' * 80}")

print(f"\n── 融资净买入强度分位 × 次日方向（反转效应检验）──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_rz_net_buy_pct'] is not None]
    if len(sec_data) < 30:
        continue
    
    # 按净买入强度排序，分5组
    sorted_data = sorted(sec_data, key=lambda x: x['_rz_net_buy_pct'])
    n = len(sorted_data)
    quintiles = [sorted_data[i*n//5:(i+1)*n//5] for i in range(5)]
    
    print(f"\n  {sec} (n={n}):")
    labels = ['Q1(最强卖出)', 'Q2', 'Q3(中性)', 'Q4', 'Q5(最强买入)']
    for i, (label, group) in enumerate(zip(labels, quintiles)):
        if len(group) >= 5:
            ge0 = sum(1 for d in group if d['_ge0'])
            avg_pct = sum(d['_rz_net_buy_pct'] for d in group) / len(group)
            print(f"    {label}(n={len(group)}, avg={avg_pct:.2f}%): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析5: 融资余额变化率 vs 股价涨跌的领先/滞后关系
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析5: 融资余额变化率 vs 股价涨跌的领先/滞后关系")
print(f"{'=' * 80}")

for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_rz_trend_3d'] is not None]
    if len(sec_data) < 30:
        continue
    
    # 融资余额3日上升 + 当日股价下跌 → 次日？（融资抄底信号）
    rz_up_price_down = [d for d in sec_data if d['_rz_trend_3d'] > 0 and d['_actual'] < 0]
    # 融资余额3日下降 + 当日股价上涨 → 次日？（融资出逃信号）
    rz_down_price_up = [d for d in sec_data if d['_rz_trend_3d'] < 0 and d['_actual'] > 0]
    # 融资余额3日上升 + 当日股价上涨 → 次日？（趋势延续）
    rz_up_price_up = [d for d in sec_data if d['_rz_trend_3d'] > 0 and d['_actual'] > 0]
    # 融资余额3日下降 + 当日股价下跌 → 次日？（趋势延续）
    rz_down_price_down = [d for d in sec_data if d['_rz_trend_3d'] < 0 and d['_actual'] < 0]
    
    print(f"\n  {sec}:")
    # 注意：这里的"次日"是指评分日的次日，即预测日
    # 但_actual已经是预测日的涨跌了，所以这里分析的是：
    # 融资趋势 + 评分日当天的实际涨跌 → 但我们需要的是预测日的涨跌
    # 修正：_actual就是预测日的涨跌，_rz_trend是评分日的融资趋势
    # 所以直接用 _ge0/_le0 就是预测日的方向
    
    for label, group in [('融资升+价跌(抄底?)', rz_up_price_down),
                          ('融资降+价涨(出逃?)', rz_down_price_up),
                          ('融资升+价涨(趋势)', rz_up_price_up),
                          ('融资降+价跌(趋势)', rz_down_price_down)]:
        # 这里有个问题：_actual是预测日的涨跌，不是评分日的
        # 需要重新理解数据结构
        pass

# 重新理解：details中的_actual是"预测日"的实际涨跌
# 评分日的涨跌需要从K线数据获取，但我们没有
# 所以改为分析：融资趋势 → 预测日涨跌
print(f"\n── 融资3日趋势 → 预测日涨跌 ──")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec and d['_rz_trend_3d'] is not None]
    if len(sec_data) < 30:
        continue
    
    rz_up = [d for d in sec_data if d['_rz_trend_3d'] > 1.0]
    rz_flat = [d for d in sec_data if -1.0 <= d['_rz_trend_3d'] <= 1.0]
    rz_down = [d for d in sec_data if d['_rz_trend_3d'] < -1.0]
    
    print(f"  {sec}:")
    for label, group in [('融资3日升>1%', rz_up), ('融资3日平', rz_flat), ('融资3日降<-1%', rz_down)]:
        if len(group) >= 10:
            ge0 = sum(1 for d in group if d['_ge0'])
            print(f"    {label}(n={len(group)}): 次日>=0%={ge0}({ge0/len(group)*100:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 分析6: In-sample最优策略（含融资融券信号）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析6: In-sample最优策略（含融资融券信号）")
print(f"{'=' * 80}")

# 策略: 对每个(板块, margin_dir, combined_dir)组合，选择最优方向
for strategy_name, key_fn, filter_fn in [
    ('sector+combined(基线)', 
     lambda d: f"{d['板块']}_{get_combined_dir(d)}", 
     lambda d: True),
    ('sector+rz_net+combined', 
     lambda d: f"{d['板块']}_{get_rz_net_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_balance'] is not None),
    ('sector+rz_trend+combined', 
     lambda d: f"{d['板块']}_{get_rz_trend_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_trend_5d'] is not None),
    ('sector+margin_signal+combined', 
     lambda d: f"{d['板块']}_{get_margin_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_balance'] is not None),
    ('sector+rz_net+rz_trend+combined', 
     lambda d: f"{d['板块']}_{get_rz_net_dir(d)}_{get_rz_trend_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_trend_5d'] is not None),
]:
    filtered = [d for d in details if filter_fn(d)]
    combos = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in filtered:
        key = key_fn(d)
        combos[key]['n'] += 1
        if d['_ge0']: combos[key]['ge0'] += 1
        if d['_le0']: combos[key]['le0'] += 1
    
    ok = sum(max(s['ge0'], s['le0']) for s in combos.values())
    n = len(filtered)
    
    # 按板块统计
    sec_ok = defaultdict(int)
    sec_n = defaultdict(int)
    for d in filtered:
        key = key_fn(d)
        s = combos[key]
        pred = '上涨' if s['ge0'] >= s['le0'] else '下跌'
        if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
            sec_ok[d['板块']] += 1
        sec_n[d['板块']] += 1
    
    sec_detail = ', '.join(f"{sec}:{sec_ok[sec]}/{sec_n[sec]}({sec_ok[sec]/sec_n[sec]*100:.1f}%)" 
                           for sec in sectors if sec_n[sec] > 0)
    print(f"  {strategy_name}: {ok}/{n} ({ok/n*100:.1f}%) | {sec_detail}")


# ═══════════════════════════════════════════════════════════
# 分析7: 时间序列验证（前半训练→后半测试）★关键★
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析7: 时间序列验证（前半训练→后半测试）★关键★")
print(f"{'=' * 80}")

mid_date = all_dates[len(all_dates) // 2]
train_all = [d for d in details if d['评分日'] <= mid_date]
test_all = [d for d in details if d['评分日'] > mid_date]
print(f"训练集: {len(train_all)}, 测试集: {len(test_all)}, 分割日: {mid_date}")

for strategy_name, key_fn, filter_fn in [
    ('sector+combined(基线)', 
     lambda d: f"{d['板块']}_{get_combined_dir(d)}", 
     lambda d: True),
    ('sector+rz_net+combined', 
     lambda d: f"{d['板块']}_{get_rz_net_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_balance'] is not None),
    ('sector+rz_trend+combined', 
     lambda d: f"{d['板块']}_{get_rz_trend_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_trend_5d'] is not None),
    ('sector+margin_signal+combined', 
     lambda d: f"{d['板块']}_{get_margin_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_balance'] is not None),
    ('sector+rz_net+rz_trend+combined', 
     lambda d: f"{d['板块']}_{get_rz_net_dir(d)}_{get_rz_trend_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_trend_5d'] is not None),
]:
    train = [d for d in train_all if filter_fn(d)]
    test = [d for d in test_all if filter_fn(d)]
    
    # 训练集学习最优方向
    stats = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
    for d in train:
        key = key_fn(d)
        stats[key]['n'] += 1
        if d['_ge0']: stats[key]['ge0'] += 1
        if d['_le0']: stats[key]['le0'] += 1
    
    # 测试集评估
    ok = 0
    for d in test:
        key = key_fn(d)
        s = stats.get(key, {'ge0': 1, 'le0': 1, 'n': 2})
        pred = '上涨' if s['ge0'] >= s['le0'] else '下跌'
        if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
            ok += 1
    
    n_test = len(test)
    
    # 按板块统计测试集
    sec_ok = defaultdict(int)
    sec_n = defaultdict(int)
    for d in test:
        key = key_fn(d)
        s = stats.get(key, {'ge0': 1, 'le0': 1, 'n': 2})
        pred = '上涨' if s['ge0'] >= s['le0'] else '下跌'
        if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
            sec_ok[d['板块']] += 1
        sec_n[d['板块']] += 1
    
    sec_detail = ', '.join(f"{sec}:{sec_ok[sec]/sec_n[sec]*100:.0f}%" 
                           for sec in sectors if sec_n[sec] > 0)
    print(f"  {strategy_name}: {ok}/{n_test} ({ok/n_test*100:.1f}%) | {sec_detail}")


# ═══════════════════════════════════════════════════════════
# 分析8: 留一日交叉验证 ★关键★
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析8: 留一日交叉验证")
print(f"{'=' * 80}")

for strategy_name, key_fn, filter_fn in [
    ('sector+combined(基线)', 
     lambda d: f"{d['板块']}_{get_combined_dir(d)}", 
     lambda d: True),
    ('sector+rz_net+combined', 
     lambda d: f"{d['板块']}_{get_rz_net_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_balance'] is not None),
    ('sector+rz_trend+combined', 
     lambda d: f"{d['板块']}_{get_rz_trend_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_trend_5d'] is not None),
    ('sector+margin_signal+combined', 
     lambda d: f"{d['板块']}_{get_margin_dir(d)}_{get_combined_dir(d)}", 
     lambda d: d['_rz_balance'] is not None),
]:
    filtered = [d for d in details if filter_fn(d)]
    filtered_dates = sorted(set(d['评分日'] for d in filtered))
    
    total_ok = 0
    total_n = 0
    for test_date in filtered_dates:
        train_data = [d for d in filtered if d['评分日'] != test_date]
        test_data = [d for d in filtered if d['评分日'] == test_date]
        
        stats = defaultdict(lambda: {'ge0': 0, 'le0': 0, 'n': 0})
        for d in train_data:
            key = key_fn(d)
            stats[key]['n'] += 1
            if d['_ge0']: stats[key]['ge0'] += 1
            if d['_le0']: stats[key]['le0'] += 1
        
        for d in test_data:
            key = key_fn(d)
            s = stats.get(key, {'ge0': 1, 'le0': 1, 'n': 2})
            pred = '上涨' if s['ge0'] >= s['le0'] else '下跌'
            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                total_ok += 1
            total_n += 1
    
    print(f"  {strategy_name}: {total_ok}/{total_n} ({total_ok/total_n*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析9: 融资融券信号对当前模型错误样本的修正能力
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析9: 融资融券信号对模型错误样本的修正能力")
print(f"{'=' * 80}")

wrong = [d for d in details if d['宽松正确'] != '✓' and d['_rz_balance'] is not None]
right = [d for d in details if d['宽松正确'] == '✓' and d['_rz_balance'] is not None]
print(f"有融资融券数据的错误样本: {len(wrong)}, 正确样本: {len(right)}")

# 错误样本中，融资融券信号是否能给出正确方向？
for sec in sectors:
    sec_wrong = [d for d in wrong if d['板块'] == sec]
    if len(sec_wrong) < 10:
        continue
    
    # 模型预测上涨但实际下跌的错误
    wrong_up = [d for d in sec_wrong if d['预测方向'] == '上涨']
    # 模型预测下跌但实际上涨的错误
    wrong_down = [d for d in sec_wrong if d['预测方向'] == '下跌']
    
    print(f"\n  {sec} (错误样本: {len(sec_wrong)}):")
    
    # 在预测上涨但错误的样本中，融资信号是否偏空？
    if len(wrong_up) >= 5:
        rz_sell = sum(1 for d in wrong_up if d['_margin_signal'] < 0)
        rz_buy = sum(1 for d in wrong_up if d['_margin_signal'] > 0)
        rz_neutral = sum(1 for d in wrong_up if d['_margin_signal'] == 0)
        print(f"    预测上涨但错(n={len(wrong_up)}): 融资看跌={rz_sell} 融资看涨={rz_buy} 中性={rz_neutral}")
        if rz_sell > 0:
            print(f"      → 如果用融资看跌修正为下跌，可修正{rz_sell}个样本")
    
    if len(wrong_down) >= 5:
        rz_sell = sum(1 for d in wrong_down if d['_margin_signal'] < 0)
        rz_buy = sum(1 for d in wrong_down if d['_margin_signal'] > 0)
        rz_neutral = sum(1 for d in wrong_down if d['_margin_signal'] == 0)
        print(f"    预测下跌但错(n={len(wrong_down)}): 融资看跌={rz_sell} 融资看涨={rz_buy} 中性={rz_neutral}")
        if rz_buy > 0:
            print(f"      → 如果用融资看涨修正为上涨，可修正{rz_buy}个样本")

# ═══════════════════════════════════════════════════════════
# 分析10: 综合结论
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析10: 综合结论")
print(f"{'=' * 80}")
print(f"""
当前基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)
目标: 65% = {int(total*0.65)}/{total}
差距: {int(total*0.65) - loose_ok}个样本需要额外正确

以上分析结果将决定融资融券+大宗交易信号是否能帮助达到65%目标。
关键看分析7（时间序列验证）和分析8（留一日交叉验证）的结果。
""")
