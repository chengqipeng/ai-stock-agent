#!/usr/bin/env python3
"""
v12 深度分析：全面诊断预测模型的不足之处
不考虑打分，只关注预测涨跌准确率
"""
import json
from collections import defaultdict
from datetime import datetime

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']
total = len(details)
loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"=" * 80)
print(f"当前基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"需要达到65%: {int(total*0.65)}/{total}")
print(f"差距: 需要再多对 {int(total*0.65) - loose_ok} 个样本")
print(f"=" * 80)

# ═══════════════════════════════════════════════════════════
# 1. 错误预测的全景分析
# ═══════════════════════════════════════════════════════════
wrong = [d for d in details if d['宽松正确'] == '✗']
correct = [d for d in details if d['宽松正确'] == '✓']
print(f"\n{'='*80}")
print(f"一、错误预测全景 ({len(wrong)}个)")
print(f"{'='*80}")

# 1a. 错误预测中，预测上涨但实际跌 vs 预测下跌但实际涨
wrong_up = [d for d in wrong if d['预测方向'] == '上涨']
wrong_down = [d for d in wrong if d['预测方向'] == '下跌']
print(f"\n  预测上涨但错误: {len(wrong_up)}个 (占错误的{len(wrong_up)/len(wrong)*100:.1f}%)")
print(f"  预测下跌但错误: {len(wrong_down)}个 (占错误的{len(wrong_down)/len(wrong)*100:.1f}%)")

# 1b. 错误预测的实际涨跌幅分布
def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

wrong_chgs = [parse_chg(d['实际涨跌']) for d in wrong]
print(f"\n  错误预测的实际涨跌幅分布:")
print(f"    平均: {sum(wrong_chgs)/len(wrong_chgs):+.2f}%")
bins = {'大跌(<-2%)': 0, '中跌(-2~-1%)': 0, '小跌(-1~-0.3%)': 0,
        '微跌(-0.3~0%)': 0, '微涨(0~0.3%)': 0, '小涨(0.3~1%)': 0,
        '中涨(1~2%)': 0, '大涨(>2%)': 0}
for c in wrong_chgs:
    if c < -2: bins['大跌(<-2%)'] += 1
    elif c < -1: bins['中跌(-2~-1%)'] += 1
    elif c < -0.3: bins['小跌(-1~-0.3%)'] += 1
    elif c < 0: bins['微跌(-0.3~0%)'] += 1
    elif c < 0.3: bins['微涨(0~0.3%)'] += 1
    elif c < 1: bins['小涨(0.3~1%)'] += 1
    elif c < 2: bins['中涨(1~2%)'] += 1
    else: bins['大涨(>2%)'] += 1
for k, v in bins.items():
    print(f"    {k:18s}: {v:4d} ({v/len(wrong)*100:.1f}%)")

# 1c. 错误预测中"差一点就对了"的样本（宽松模式下）
# 预测上涨但实际微跌(0~-0.3%)，预测下跌但实际微涨(0~0.3%)
near_miss_up = [d for d in wrong_up if -0.3 <= parse_chg(d['实际涨跌']) < 0]
near_miss_down = [d for d in wrong_down if 0 < parse_chg(d['实际涨跌']) <= 0.3]
print(f"\n  '差一点就对'的样本（严格模式下错，但接近0%）:")
print(f"    预测上涨,实际微跌(0~-0.3%): {len(near_miss_up)}个")
print(f"    预测下跌,实际微涨(0~0.3%): {len(near_miss_down)}个")
print(f"    合计: {len(near_miss_up)+len(near_miss_down)}个 — 这些在宽松模式下已经算对了")

# ═══════════════════════════════════════════════════════════
# 2. 板块维度深度分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"二、板块维度深度分析")
print(f"{'='*80}")

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    if not sec_data:
        continue
    sec_ok = sum(1 for d in sec_data if d['宽松正确'] == '✓')
    sec_wrong = [d for d in sec_data if d['宽松正确'] == '✗']
    
    # 全涨/全跌基准
    all_up = sum(1 for d in sec_data if parse_chg(d['实际涨跌']) >= 0)
    all_down = sum(1 for d in sec_data if parse_chg(d['实际涨跌']) <= 0)
    
    # 预测上涨/下跌的准确率
    pred_up = [d for d in sec_data if d['预测方向'] == '上涨']
    pred_down = [d for d in sec_data if d['预测方向'] == '下跌']
    up_ok = sum(1 for d in pred_up if d['宽松正确'] == '✓')
    down_ok = sum(1 for d in pred_down if d['宽松正确'] == '✓')
    
    print(f"\n  [{sector}] {sec_ok}/{len(sec_data)} ({sec_ok/len(sec_data)*100:.1f}%)")
    print(f"    全涨基准: {all_up}/{len(sec_data)} ({all_up/len(sec_data)*100:.1f}%)")
    print(f"    全跌基准: {all_down}/{len(sec_data)} ({all_down/len(sec_data)*100:.1f}%)")
    best_base = max(all_up, all_down)
    print(f"    模型超越基准: {sec_ok - best_base:+d} ({(sec_ok/len(sec_data) - best_base/len(sec_data))*100:+.1f}pp)")
    if pred_up:
        print(f"    预测上涨: {up_ok}/{len(pred_up)} ({up_ok/len(pred_up)*100:.1f}%)")
    if pred_down:
        print(f"    预测下跌: {down_ok}/{len(pred_down)} ({down_ok/len(pred_down)*100:.1f}%)")
    
    # 错误预测的特征
    if sec_wrong:
        wrong_combined = [d['融合信号'] for d in sec_wrong]
        correct_combined = [d['融合信号'] for d in sec_data if d['宽松正确'] == '✓']
        print(f"    错误样本融合信号均值: {sum(wrong_combined)/len(wrong_combined):.3f}")
        if correct_combined:
            print(f"    正确样本融合信号均值: {sum(correct_combined)/len(correct_combined):.3f}")

# ═══════════════════════════════════════════════════════════
# 3. 置信度 × 板块 × 预测方向 三维交叉分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"三、置信度×板块×预测方向 三维交叉（只显示准确率<55%且样本≥10的）")
print(f"{'='*80}")

cross_stats = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    key = (d['板块'], d['置信度'], d['预测方向'])
    cross_stats[key]['n'] += 1
    if d['宽松正确'] == '✓':
        cross_stats[key]['ok'] += 1

low_acc_groups = []
for key, stats in sorted(cross_stats.items(), key=lambda x: x[1]['ok']/max(x[1]['n'],1)):
    sec, conf, pred = key
    rate = stats['ok'] / stats['n'] * 100 if stats['n'] > 0 else 0
    if stats['n'] >= 10 and rate < 55:
        low_acc_groups.append((key, stats, rate))
        flip_potential = stats['n'] - 2 * stats['ok']  # 翻转后净增
        print(f"  {sec:6s} {conf:6s} 预测{pred}: {stats['ok']:3d}/{stats['n']:3d} ({rate:.1f}%) 翻转净增={flip_potential:+d}")

print(f"\n  低准确率组合总样本: {sum(s['n'] for _, s, _ in low_acc_groups)}")
print(f"  如果全部翻转，理论净增: {sum(s['n'] - 2*s['ok'] for _, s, _ in low_acc_groups)}")

# ═══════════════════════════════════════════════════════════
# 4. 融合信号(combined)的有效性分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"四、融合信号(combined)有效性分析")
print(f"{'='*80}")

# 4a. combined方向 vs 实际方向
combined_right = sum(1 for d in details 
                     if (d['融合信号'] > 0 and parse_chg(d['实际涨跌']) > 0) or
                        (d['融合信号'] < 0 and parse_chg(d['实际涨跌']) < 0) or
                        (d['融合信号'] == 0))
print(f"\n  combined方向与实际方向一致率(严格): {combined_right}/{total} ({combined_right/total*100:.1f}%)")

combined_loose_right = sum(1 for d in details 
                     if (d['融合信号'] > 0 and parse_chg(d['实际涨跌']) >= 0) or
                        (d['融合信号'] <= 0 and parse_chg(d['实际涨跌']) <= 0))
print(f"  combined方向与实际方向一致率(宽松): {combined_loose_right}/{total} ({combined_loose_right/total*100:.1f}%)")

# 4b. 按板块分析combined有效性
print(f"\n  按板块的combined方向一致率(宽松):")
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    if not sec_data:
        continue
    c_right = sum(1 for d in sec_data 
                  if (d['融合信号'] > 0 and parse_chg(d['实际涨跌']) >= 0) or
                     (d['融合信号'] <= 0 and parse_chg(d['实际涨跌']) <= 0))
    # 反向一致率
    c_reverse = sum(1 for d in sec_data 
                    if (d['融合信号'] > 0 and parse_chg(d['实际涨跌']) <= 0) or
                       (d['融合信号'] < 0 and parse_chg(d['实际涨跌']) >= 0))
    print(f"    {sector:6s}: 正向{c_right}/{len(sec_data)} ({c_right/len(sec_data)*100:.1f}%) "
          f"| 反向{c_reverse}/{len(sec_data)} ({c_reverse/len(sec_data)*100:.1f}%)")

# 4c. combined强度分段准确率
print(f"\n  combined信号强度 vs 实际方向一致率(宽松):")
strength_bins = [
    ('<-2.0', lambda c: c < -2.0),
    ('-2.0~-1.0', lambda c: -2.0 <= c < -1.0),
    ('-1.0~-0.5', lambda c: -1.0 <= c < -0.5),
    ('-0.5~0', lambda c: -0.5 <= c < 0),
    ('0~0.5', lambda c: 0 <= c < 0.5),
    ('0.5~1.0', lambda c: 0.5 <= c < 1.0),
    ('1.0~2.0', lambda c: 1.0 <= c < 2.0),
    ('>2.0', lambda c: c >= 2.0),
]
for label, cond in strength_bins:
    group = [d for d in details if cond(d['融合信号'])]
    if not group:
        continue
    # combined>0 → 实际>=0 或 combined<0 → 实际<=0
    right = sum(1 for d in group 
                if (d['融合信号'] > 0 and parse_chg(d['实际涨跌']) >= 0) or
                   (d['融合信号'] <= 0 and parse_chg(d['实际涨跌']) <= 0))
    # 模型实际预测的准确率
    model_ok = sum(1 for d in group if d['宽松正确'] == '✓')
    print(f"    combined {label:10s}: n={len(group):4d} "
          f"combined方向准确={right/len(group)*100:.1f}% "
          f"模型预测准确={model_ok/len(group)*100:.1f}%")

# ═══════════════════════════════════════════════════════════
# 5. 个股维度分析 — 哪些股票拖后腿
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"五、个股维度分析")
print(f"{'='*80}")

stock_stats = defaultdict(lambda: {'ok': 0, 'n': 0, 'sector': '', 'name': ''})
for d in details:
    key = d['代码']
    stock_stats[key]['n'] += 1
    stock_stats[key]['sector'] = d['板块']
    stock_stats[key]['name'] = d['名称']
    if d['宽松正确'] == '✓':
        stock_stats[key]['ok'] += 1

sorted_stocks = sorted(stock_stats.items(), key=lambda x: x[1]['ok']/max(x[1]['n'],1))
print(f"\n  最差的10只股票:")
for code, stats in sorted_stocks[:10]:
    rate = stats['ok'] / stats['n'] * 100
    # 全涨/全跌基准
    stock_data = [d for d in details if d['代码'] == code]
    all_up = sum(1 for d in stock_data if parse_chg(d['实际涨跌']) >= 0)
    all_down = sum(1 for d in stock_data if parse_chg(d['实际涨跌']) <= 0)
    print(f"    {stats['name']:8s}({code})[{stats['sector']}]: "
          f"{stats['ok']}/{stats['n']} ({rate:.1f}%) "
          f"全涨={all_up/stats['n']*100:.0f}% 全跌={all_down/stats['n']*100:.0f}%")

print(f"\n  最好的10只股票:")
for code, stats in sorted_stocks[-10:]:
    rate = stats['ok'] / stats['n'] * 100
    print(f"    {stats['name']:8s}({code})[{stats['sector']}]: "
          f"{stats['ok']}/{stats['n']} ({rate:.1f}%)")

# 如果排除最差的5只股票
worst5 = set(code for code, _ in sorted_stocks[:5])
filtered = [d for d in details if d['代码'] not in worst5]
f_ok = sum(1 for d in filtered if d['宽松正确'] == '✓')
print(f"\n  排除最差5只后: {f_ok}/{len(filtered)} ({f_ok/len(filtered)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 6. 时间维度分析 — 星期效应 + 时间段效应
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"六、时间维度分析")
print(f"{'='*80}")

# 6a. 星期效应
print(f"\n  星期效应（评分日→预测日）:")
wd_stats = defaultdict(lambda: {'ok': 0, 'n': 0})
wd_names = {0: '周一→周二', 1: '周二→周三', 2: '周三→周四', 3: '周四→周五', 4: '周五→周一'}
for d in details:
    try:
        wd = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
        wd_stats[wd_names[wd]]['n'] += 1
        if d['宽松正确'] == '✓':
            wd_stats[wd_names[wd]]['ok'] += 1
    except: pass

for name in ['周一→周二', '周二→周三', '周三→周四', '周四→周五', '周五→周一']:
    s = wd_stats[name]
    if s['n'] > 0:
        print(f"    {name}: {s['ok']}/{s['n']} ({s['ok']/s['n']*100:.1f}%)")

# 6b. 月份效应
print(f"\n  月份效应:")
month_stats = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    m = d['评分日'][:7]
    month_stats[m]['n'] += 1
    if d['宽松正确'] == '✓':
        month_stats[m]['ok'] += 1
for m in sorted(month_stats):
    s = month_stats[m]
    print(f"    {m}: {s['ok']}/{s['n']} ({s['ok']/s['n']*100:.1f}%)")

# 6c. 星期×板块交叉（极端值）
print(f"\n  星期×板块交叉（准确率<50%或>65%的组合）:")
wd_sec = defaultdict(lambda: {'ok': 0, 'n': 0})
for d in details:
    try:
        wd = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
        wd_short = {0:'周一',1:'周二',2:'周三',3:'周四',4:'周五'}[wd]
        wd_sec[(wd_short, d['板块'])]['n'] += 1
        if d['宽松正确'] == '✓':
            wd_sec[(wd_short, d['板块'])]['ok'] += 1
    except: pass

for key, s in sorted(wd_sec.items(), key=lambda x: x[1]['ok']/max(x[1]['n'],1)):
    rate = s['ok'] / s['n'] * 100
    if s['n'] >= 10 and (rate < 50 or rate > 65):
        marker = "⚠️差" if rate < 50 else "✅好"
        print(f"    {key[0]} {key[1]:6s}: {s['ok']}/{s['n']} ({rate:.1f}%) {marker}")

# ═══════════════════════════════════════════════════════════
# 7. 因子有效性深度分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"七、各因子对预测的实际贡献度")
print(f"{'='*80}")

factor_names = ['reversion', 'rsi', 'kdj', 'macd', 'boll', 'vp', 'fund', 
                'market', 'streak', 'trend_bias', 'us_overnight', 
                'vol_regime', 'momentum_persist', 'gap_signal', 'intraday_pos']

# 全局因子有效性
print(f"\n  全局因子方向一致率（因子信号方向 vs 实际涨跌方向）:")
for fname in factor_names:
    aligned = 0
    total_f = 0
    for d in details:
        fval = d.get('factors', {}).get(fname, 0) if 'factors' in d else 0
        # 从逐日详情中没有factors，需要从其他地方获取
        # 跳过，用因子有效性分析数据
        pass

# 使用结果文件中的因子有效性分析
factor_analysis = data.get('因子有效性分析(按板块)', {})
print(f"\n  按板块的因子有效性（方向一致率）:")
for sector in sectors:
    sec_factors = factor_analysis.get(sector, {})
    if not sec_factors:
        continue
    effective = []
    ineffective = []
    neutral = []
    for fname, info in sec_factors.items():
        rate_str = info.get('方向一致率', '50%')
        rate = float(rate_str.replace('%', ''))
        n = info.get('样本数', 0)
        if rate > 55:
            effective.append(f"{fname}({rate:.0f}%,n={n})")
        elif rate < 45:
            ineffective.append(f"{fname}({rate:.0f}%,n={n})")
        else:
            neutral.append(fname)
    print(f"\n    [{sector}]")
    print(f"      有效(>55%): {', '.join(effective) if effective else '无'}")
    print(f"      无效(<45%): {', '.join(ineffective) if ineffective else '无'}")
    print(f"      中性(45-55%): {', '.join(neutral)}")

# ═══════════════════════════════════════════════════════════
# 8. 同行信号有效性分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"八、同行信号有效性分析")
print(f"{'='*80}")

peer_analysis = data.get('板块同行信号分析', {})
print(f"\n  同行看涨时: {peer_analysis.get('同行看涨时', {})}")
print(f"  同行看跌时: {peer_analysis.get('同行看跌时', {})}")
print(f"  同行中性时: {peer_analysis.get('同行中性时', {})}")

# 按板块的同行信号
print(f"\n  按板块同行信号一致/矛盾时准确率:")
sec_peer = peer_analysis.get('按板块同行信号', {})
for sec, info in sec_peer.items():
    print(f"    {sec:6s}: 一致时={info.get('信号一致时','N/A')} | 矛盾时={info.get('信号矛盾时','N/A')}")

# ═══════════════════════════════════════════════════════════
# 9. 决策逻辑缺陷分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"九、决策逻辑缺陷分析")
print(f"{'='*80}")

# 9a. combined方向正确但模型预测错误（决策逻辑翻转了正确的信号）
combined_right_model_wrong = []
combined_wrong_model_right = []
for d in details:
    c = d['融合信号']
    actual = parse_chg(d['实际涨跌'])
    combined_dir_right = (c > 0 and actual >= 0) or (c <= 0 and actual <= 0)
    model_right = d['宽松正确'] == '✓'
    
    if combined_dir_right and not model_right:
        combined_right_model_wrong.append(d)
    elif not combined_dir_right and model_right:
        combined_wrong_model_right.append(d)

print(f"\n  combined方向对但模型预测错: {len(combined_right_model_wrong)}个")
print(f"    → 决策逻辑不必要地翻转了正确的combined信号")
print(f"  combined方向错但模型预测对: {len(combined_wrong_model_right)}个")
print(f"    → 决策逻辑成功修正了错误的combined信号")
print(f"  净效果: {len(combined_wrong_model_right) - len(combined_right_model_wrong):+d}")
if len(combined_right_model_wrong) > len(combined_wrong_model_right):
    print(f"  ⚠️ 决策逻辑整体在伤害准确率！翻转了太多正确的combined信号")
else:
    print(f"  ✅ 决策逻辑整体在帮助准确率")

# 9b. 按板块分析决策逻辑的净效果
print(f"\n  按板块的决策逻辑净效果:")
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    crw = sum(1 for d in sec_data 
              if ((d['融合信号'] > 0 and parse_chg(d['实际涨跌']) >= 0) or 
                  (d['融合信号'] <= 0 and parse_chg(d['实际涨跌']) <= 0))
              and d['宽松正确'] == '✗')
    cwr = sum(1 for d in sec_data 
              if not ((d['融合信号'] > 0 and parse_chg(d['实际涨跌']) >= 0) or 
                      (d['融合信号'] <= 0 and parse_chg(d['实际涨跌']) <= 0))
              and d['宽松正确'] == '✓')
    net = cwr - crw
    marker = "✅" if net >= 0 else "⚠️"
    print(f"    {sector:6s}: 翻转正确→错误={crw}, 翻转错误→正确={cwr}, 净效果={net:+d} {marker}")

# ═══════════════════════════════════════════════════════════
# 10. 理论上限分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"十、理论上限分析")
print(f"{'='*80}")

# 10a. 如果直接用combined方向（不经过决策逻辑）
direct_combined = sum(1 for d in details 
                      if (d['融合信号'] > 0 and parse_chg(d['实际涨跌']) >= 0) or
                         (d['融合信号'] <= 0 and parse_chg(d['实际涨跌']) <= 0))
print(f"\n  直接用combined方向(宽松): {direct_combined}/{total} ({direct_combined/total*100:.1f}%)")

# 10b. 如果每个板块用最优的全涨/全跌
optimal_sector = 0
for sector in sectors:
    sec_data = [d for d in details if d['板块'] == sector]
    all_up = sum(1 for d in sec_data if parse_chg(d['实际涨跌']) >= 0)
    all_down = sum(1 for d in sec_data if parse_chg(d['实际涨跌']) <= 0)
    optimal_sector += max(all_up, all_down)
print(f"  板块最优全涨/全跌: {optimal_sector}/{total} ({optimal_sector/total*100:.1f}%)")

# 10c. 如果每个(板块,置信度)用最优方向
optimal_cross = 0
for key, stats in cross_stats.items():
    sec, conf, pred = key
    # 找同(板块,置信度)的所有样本
    pass

# 重新计算
conf_sec_stats = defaultdict(lambda: {'up': 0, 'down': 0, 'n': 0})
for d in details:
    key = (d['板块'], d['置信度'])
    conf_sec_stats[key]['n'] += 1
    if parse_chg(d['实际涨跌']) >= 0:
        conf_sec_stats[key]['up'] += 1
    if parse_chg(d['实际涨跌']) <= 0:
        conf_sec_stats[key]['down'] += 1

optimal_conf_sec = sum(max(s['up'], s['down']) for s in conf_sec_stats.values())
print(f"  (板块,置信度)最优方向: {optimal_conf_sec}/{total} ({optimal_conf_sec/total*100:.1f}%)")

# 10d. 如果每个(板块,星期)用最优方向
wd_sec_optimal = defaultdict(lambda: {'up': 0, 'down': 0})
for d in details:
    try:
        wd = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
        key = (d['板块'], wd)
        if parse_chg(d['实际涨跌']) >= 0:
            wd_sec_optimal[key]['up'] += 1
        if parse_chg(d['实际涨跌']) <= 0:
            wd_sec_optimal[key]['down'] += 1
    except: pass

optimal_wd_sec = sum(max(s['up'], s['down']) for s in wd_sec_optimal.values())
print(f"  (板块,星期)最优方向: {optimal_wd_sec}/{total} ({optimal_wd_sec/total*100:.1f}%)")

# 10e. 实际涨跌分布
flat = sum(1 for d in details if abs(parse_chg(d['实际涨跌'])) < 0.3)
up_real = sum(1 for d in details if parse_chg(d['实际涨跌']) > 0.3)
down_real = sum(1 for d in details if parse_chg(d['实际涨跌']) < -0.3)
print(f"\n  实际涨跌分布:")
print(f"    明确上涨(>0.3%): {up_real} ({up_real/total*100:.1f}%)")
print(f"    明确下跌(<-0.3%): {down_real} ({down_real/total*100:.1f}%)")
print(f"    横盘(±0.3%): {flat} ({flat/total*100:.1f}%)")
print(f"    实际>=0%: {sum(1 for d in details if parse_chg(d['实际涨跌'])>=0)} ({sum(1 for d in details if parse_chg(d['实际涨跌'])>=0)/total*100:.1f}%)")

print(f"\n{'='*80}")
print(f"分析完成")
print(f"{'='*80}")
