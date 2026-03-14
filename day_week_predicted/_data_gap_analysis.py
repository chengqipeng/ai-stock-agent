#!/usr/bin/env python3
"""
数据侧深度分析：找出从数据角度可以提升准确率的具体方向
目标：从58.9%提升到65%+
"""
import json
from collections import defaultdict
from datetime import datetime, timedelta

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']
total = len(details)
loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

print(f"=" * 80)
print(f"数据侧深度分析 — 从58.9%到65%需要多对{int(total*0.65)-loose_ok}个样本")
print(f"=" * 80)

# ═══════════════════════════════════════════════════════════
# 1. 同行信号的真正问题：科技和化工应该用反转信号
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"一、同行信号修正潜力（科技/化工应用反转信号）")
print(f"{'='*80}")

# 当前同行信号一致/矛盾的准确率
for sector in ['科技', '化工', '有色金属']:
    sec_data = [d for d in details if d['板块'] == sector]
    # 同行一致 = 同行信号方向与combined方向一致
    aligned = [d for d in sec_data if d.get('同行一致')]
    misaligned = [d for d in sec_data if not d.get('同行一致')]
    
    a_ok = sum(1 for d in aligned if d['宽松正确'] == '✓')
    m_ok = sum(1 for d in misaligned if d['宽松正确'] == '✓')
    
    print(f"\n  [{sector}]")
    if aligned:
        print(f"    同行一致时: {a_ok}/{len(aligned)} ({a_ok/len(aligned)*100:.1f}%)")
    if misaligned:
        print(f"    同行矛盾时: {m_ok}/{len(misaligned)} ({m_ok/len(misaligned)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 2. 缺失数据分析：哪些因子数据覆盖率低
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"二、因子数据覆盖率分析")
print(f"{'='*80}")

# 检查逐日详情中各因子的非零率
factor_names = ['reversion', 'rsi', 'kdj', 'macd', 'boll', 'vp', 'fund', 
                'market', 'streak', 'trend_bias', 'us_overnight', 
                'vol_regime', 'momentum_persist', 'gap_signal', 'intraday_pos']

# 从factors字段获取
if details and 'factors' in details[0]:
    print(f"\n  因子非零率（非零=有信号）:")
    for fname in factor_names:
        nonzero = sum(1 for d in details if abs(d.get('factors', {}).get(fname, 0)) > 0.01)
        print(f"    {fname:20s}: {nonzero}/{total} ({nonzero/total*100:.1f}%)")
else:
    print(f"  逐日详情中无factors字段，使用决策信息分析")

# ═══════════════════════════════════════════════════════════
# 3. 连续预测错误模式分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"三、连续预测错误模式（同一股票连续错误）")
print(f"{'='*80}")

# 按股票+日期排序
stock_days = defaultdict(list)
for d in details:
    stock_days[d['代码']].append(d)

total_streak_wrong = 0
for code, days in stock_days.items():
    days.sort(key=lambda x: x['评分日'])
    streak = 0
    max_streak = 0
    for d in days:
        if d['宽松正确'] == '✗':
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    if max_streak >= 4:
        name = days[0]['名称']
        sector = days[0]['板块']
        total_wrong = sum(1 for d in days if d['宽松正确'] == '✗')
        print(f"  {name:8s}[{sector}]: 最长连错{max_streak}天, 总错{total_wrong}/{len(days)}")
        total_streak_wrong += 1

# ═══════════════════════════════════════════════════════════
# 4. 前一日预测结果对次日的影响
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"四、前一日预测结果对次日准确率的影响")
print(f"{'='*80}")

prev_right_next = {'ok': 0, 'n': 0}
prev_wrong_next = {'ok': 0, 'n': 0}

for code, days in stock_days.items():
    days.sort(key=lambda x: x['评分日'])
    for i in range(1, len(days)):
        prev = days[i-1]
        curr = days[i]
        if prev['宽松正确'] == '✓':
            prev_right_next['n'] += 1
            if curr['宽松正确'] == '✓':
                prev_right_next['ok'] += 1
        else:
            prev_wrong_next['n'] += 1
            if curr['宽松正确'] == '✓':
                prev_wrong_next['ok'] += 1

if prev_right_next['n'] > 0:
    print(f"  前日预测正确→次日: {prev_right_next['ok']}/{prev_right_next['n']} ({prev_right_next['ok']/prev_right_next['n']*100:.1f}%)")
if prev_wrong_next['n'] > 0:
    print(f"  前日预测错误→次日: {prev_wrong_next['ok']}/{prev_wrong_next['n']} ({prev_wrong_next['ok']/prev_wrong_next['n']*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 5. 涨跌幅区间 vs 次日预测准确率
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"五、当日涨跌幅区间 vs 次日预测准确率")
print(f"{'='*80}")

# 用z_today来分析
z_bins = [
    ('z<-2.0(大跌)', lambda d: d.get('z_today', 0) < -2.0),
    ('z:-2~-1(中跌)', lambda d: -2.0 <= d.get('z_today', 0) < -1.0),
    ('z:-1~-0.5(小跌)', lambda d: -1.0 <= d.get('z_today', 0) < -0.5),
    ('z:-0.5~0(微跌)', lambda d: -0.5 <= d.get('z_today', 0) < 0),
    ('z:0~0.5(微涨)', lambda d: 0 <= d.get('z_today', 0) < 0.5),
    ('z:0.5~1(小涨)', lambda d: 0.5 <= d.get('z_today', 0) < 1.0),
    ('z:1~2(中涨)', lambda d: 1.0 <= d.get('z_today', 0) < 2.0),
    ('z>2.0(大涨)', lambda d: d.get('z_today', 0) >= 2.0),
]

for label, cond in z_bins:
    group = [d for d in details if cond(d)]
    if not group:
        continue
    ok = sum(1 for d in group if d['宽松正确'] == '✓')
    # 实际涨跌分布
    actual_up = sum(1 for d in group if parse_chg(d['实际涨跌']) >= 0)
    print(f"  {label:20s}: n={len(group):4d} 准确率={ok/len(group)*100:.1f}% 实际>=0%={actual_up/len(group)*100:.1f}%")

# ═══════════════════════════════════════════════════════════
# 6. 美股隔夜信号的实际效果
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"六、美股隔夜信号效果分析")
print(f"{'='*80}")

us_bins = [
    ('美股大跌(<-1.5)', lambda d: d.get('美股隔夜', 0) < -1.5),
    ('美股中跌(-1.5~-0.5)', lambda d: -1.5 <= d.get('美股隔夜', 0) < -0.5),
    ('美股微跌(-0.5~0)', lambda d: -0.5 <= d.get('美股隔夜', 0) < 0),
    ('美股无信号(=0)', lambda d: d.get('美股隔夜', 0) == 0),
    ('美股微涨(0~0.5)', lambda d: 0 < d.get('美股隔夜', 0) <= 0.5),
    ('美股中涨(0.5~1.5)', lambda d: 0.5 < d.get('美股隔夜', 0) <= 1.5),
    ('美股大涨(>1.5)', lambda d: d.get('美股隔夜', 0) > 1.5),
]

for label, cond in us_bins:
    group = [d for d in details if cond(d)]
    if not group:
        continue
    ok = sum(1 for d in group if d['宽松正确'] == '✓')
    actual_up = sum(1 for d in group if parse_chg(d['实际涨跌']) >= 0)
    print(f"  {label:25s}: n={len(group):4d} 准确率={ok/len(group)*100:.1f}% 实际>=0%={actual_up/len(group)*100:.1f}%")

# 按板块分析美股信号
print(f"\n  美股信号按板块效果:")
for sector in ['科技', '有色金属', '新能源', '化工', '制造', '汽车', '医药']:
    sec_data = [d for d in details if d['板块'] == sector]
    us_pos = [d for d in sec_data if d.get('美股隔夜', 0) > 0.5]
    us_neg = [d for d in sec_data if d.get('美股隔夜', 0) < -0.5]
    us_zero = [d for d in sec_data if abs(d.get('美股隔夜', 0)) <= 0.5]
    
    parts = []
    if us_pos:
        ok = sum(1 for d in us_pos if d['宽松正确'] == '✓')
        actual_up = sum(1 for d in us_pos if parse_chg(d['实际涨跌']) >= 0)
        parts.append(f"美股涨:{ok}/{len(us_pos)}({ok/len(us_pos)*100:.0f}%) 实际涨{actual_up/len(us_pos)*100:.0f}%")
    if us_neg:
        ok = sum(1 for d in us_neg if d['宽松正确'] == '✓')
        actual_up = sum(1 for d in us_neg if parse_chg(d['实际涨跌']) >= 0)
        parts.append(f"美股跌:{ok}/{len(us_neg)}({ok/len(us_neg)*100:.0f}%) 实际涨{actual_up/len(us_neg)*100:.0f}%")
    print(f"    {sector:6s}: {' | '.join(parts)}")

# ═══════════════════════════════════════════════════════════
# 7. 评分维度对预测的贡献
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"七、评分区间 × 板块 × 准确率")
print(f"{'='*80}")

for sector in ['科技', '有色金属', '新能源', '化工', '制造', '汽车', '医药']:
    sec_data = [d for d in details if d['板块'] == sector]
    score_bins = [
        ('<40', lambda d: d['评分'] < 40),
        ('40-47', lambda d: 40 <= d['评分'] <= 47),
        ('48-54', lambda d: 48 <= d['评分'] <= 54),
        ('55-60', lambda d: 55 <= d['评分'] <= 60),
        ('>60', lambda d: d['评分'] > 60),
    ]
    parts = []
    for label, cond in score_bins:
        group = [d for d in sec_data if cond(d)]
        if group:
            ok = sum(1 for d in group if d['宽松正确'] == '✓')
            parts.append(f"{label}:{ok}/{len(group)}({ok/len(group)*100:.0f}%)")
    print(f"  {sector:6s}: {' | '.join(parts)}")

# ═══════════════════════════════════════════════════════════
# 8. 龙虎榜/大单数据缺失分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"八、资金流数据覆盖分析")
print(f"{'='*80}")

fund_nonzero = 0
fund_zero = 0
for d in details:
    # 从决策信息中获取
    if 'factors' in d:
        if abs(d['factors'].get('fund', 0)) > 0.01:
            fund_nonzero += 1
        else:
            fund_zero += 1

if fund_nonzero + fund_zero > 0:
    print(f"  资金流有信号: {fund_nonzero}/{fund_nonzero+fund_zero} ({fund_nonzero/(fund_nonzero+fund_zero)*100:.1f}%)")
    print(f"  资金流无信号: {fund_zero}/{fund_nonzero+fund_zero} ({fund_zero/(fund_nonzero+fund_zero)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 9. 板块内个股相关性 — 同板块个股同日涨跌一致性
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"九、板块内个股同日涨跌一致性")
print(f"{'='*80}")

# 按日期+板块分组
date_sector = defaultdict(list)
for d in details:
    date_sector[(d['评分日'], d['板块'])].append(d)

sector_consistency = defaultdict(lambda: {'consistent': 0, 'total': 0})
for (date, sector), group in date_sector.items():
    if len(group) < 3:
        continue
    # 实际涨跌方向一致性
    ups = sum(1 for d in group if parse_chg(d['实际涨跌']) > 0)
    downs = sum(1 for d in group if parse_chg(d['实际涨跌']) < 0)
    majority = max(ups, downs)
    consistency = majority / len(group)
    sector_consistency[sector]['total'] += 1
    if consistency >= 0.7:
        sector_consistency[sector]['consistent'] += 1

print(f"  板块内70%+个股同向的交易日占比:")
for sector in ['科技', '有色金属', '新能源', '化工', '制造', '汽车', '医药']:
    s = sector_consistency[sector]
    if s['total'] > 0:
        print(f"    {sector:6s}: {s['consistent']}/{s['total']} ({s['consistent']/s['total']*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 10. 最大改进空间量化
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"十、改进空间量化分析")
print(f"{'='*80}")

# 当前各板块的错误数
print(f"\n  各板块错误数和改进空间:")
total_wrong = total - loose_ok
need_fix = int(total * 0.65) - loose_ok
print(f"  当前错误: {total_wrong}个, 需要修正至少{need_fix}个")

for sector in ['新能源', '化工', '科技', '医药', '有色金属', '汽车', '制造']:
    sec_data = [d for d in details if d['板块'] == sector]
    sec_wrong = [d for d in sec_data if d['宽松正确'] == '✗']
    sec_ok = len(sec_data) - len(sec_wrong)
    target_65 = int(len(sec_data) * 0.65)
    gap = target_65 - sec_ok
    
    # 分析错误样本的特征
    wrong_pred_up = [d for d in sec_wrong if d['预测方向'] == '上涨']
    wrong_pred_down = [d for d in sec_wrong if d['预测方向'] == '下跌']
    
    print(f"\n  [{sector}] 当前{sec_ok}/{len(sec_data)}({sec_ok/len(sec_data)*100:.1f}%) "
          f"目标{target_65} 差{gap}个")
    print(f"    错误中: 预测涨但跌={len(wrong_pred_up)} 预测跌但涨={len(wrong_pred_down)}")
    
    # 错误样本的combined信号强度分布
    weak_wrong = [d for d in sec_wrong if abs(d['融合信号']) < 0.5]
    mid_wrong = [d for d in sec_wrong if 0.5 <= abs(d['融合信号']) < 1.5]
    strong_wrong = [d for d in sec_wrong if abs(d['融合信号']) >= 1.5]
    print(f"    错误样本信号强度: 弱(<0.5)={len(weak_wrong)} 中(0.5-1.5)={len(mid_wrong)} 强(>1.5)={len(strong_wrong)}")

# ═══════════════════════════════════════════════════════════
# 11. 如果完美利用同行信号的理论上限
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"十一、同行信号完美利用的理论上限")
print(f"{'='*80}")

# 对每个板块，如果同行信号>0.5时全预测涨，<-0.5时全预测跌
for sector in ['科技', '有色金属', '新能源', '化工', '制造', '汽车', '医药']:
    sec_data = [d for d in details if d['板块'] == sector]
    
    peer_up = [d for d in sec_data if d.get('同行信号', 0) > 0.5]
    peer_down = [d for d in sec_data if d.get('同行信号', 0) < -0.5]
    peer_neutral = [d for d in sec_data if abs(d.get('同行信号', 0)) <= 0.5]
    
    parts = []
    for label, group in [('同行涨', peer_up), ('同行跌', peer_down), ('同行中性', peer_neutral)]:
        if group:
            actual_up = sum(1 for d in group if parse_chg(d['实际涨跌']) >= 0)
            actual_down = sum(1 for d in group if parse_chg(d['实际涨跌']) <= 0)
            best = max(actual_up, actual_down)
            best_dir = '涨' if actual_up >= actual_down else '跌'
            parts.append(f"{label}:{best}/{len(group)}({best/len(group)*100:.0f}%){best_dir}")
    print(f"  {sector:6s}: {' | '.join(parts)}")

# ═══════════════════════════════════════════════════════════
# 12. 多因子组合信号分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"十二、关键因子组合效果")
print(f"{'='*80}")

# 分析: 当多个有效因子同时看涨/看跌时的准确率
if details and 'factors' in details[0]:
    for sector in ['科技', '制造', '新能源']:
        sec_data = [d for d in details if d['板块'] == sector]
        
        # 统计因子投票
        for d in sec_data:
            factors = d.get('factors', {})
            bullish_count = sum(1 for f in factor_names if factors.get(f, 0) > 0.3)
            bearish_count = sum(1 for f in factor_names if factors.get(f, 0) < -0.3)
            d['_bull_count'] = bullish_count
            d['_bear_count'] = bearish_count
        
        # 强看涨(>=5个因子看涨)
        strong_bull = [d for d in sec_data if d['_bull_count'] >= 5 and d['_bear_count'] <= 2]
        strong_bear = [d for d in sec_data if d['_bear_count'] >= 5 and d['_bull_count'] <= 2]
        
        parts = []
        if strong_bull:
            ok = sum(1 for d in strong_bull if parse_chg(d['实际涨跌']) >= 0)
            parts.append(f"强看涨:{ok}/{len(strong_bull)}({ok/len(strong_bull)*100:.0f}%)")
        if strong_bear:
            ok = sum(1 for d in strong_bear if parse_chg(d['实际涨跌']) <= 0)
            parts.append(f"强看跌:{ok}/{len(strong_bear)}({ok/len(strong_bear)*100:.0f}%)")
        if parts:
            print(f"  {sector:6s}: {' | '.join(parts)}")

print(f"\n{'='*80}")
print(f"分析完成")
print(f"{'='*80}")
