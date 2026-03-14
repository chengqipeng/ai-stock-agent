#!/usr/bin/env python3
"""
v16b 全维度最优策略搜索：
对每个板块，使用所有可用信号维度找到最优决策规则组合。
"""
import json
from collections import defaultdict
from datetime import datetime

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    data = json.load(f)

details = data['逐日详情']
total = len(details)

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']

# 为每个样本预计算所有特征
for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_ge0'] = d['_actual'] >= 0
    d['_le0'] = d['_actual'] <= 0
    try:
        d['_wd'] = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
    except:
        d['_wd'] = -1

# ═══════════════════════════════════════════════════════════
# 策略1: 板块基准 + 星期效应覆盖
# ═══════════════════════════════════════════════════════════
print(f"{'=' * 80}")
print(f"策略1: 板块基准 + 星期效应覆盖")
print(f"{'=' * 80}")

total_ok = 0
for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    n = len(sec)
    
    # 找每个星期几的最优方向
    best_ok = 0
    best_wd_dirs = {}
    
    for wd in range(5):
        wd_data = [d for d in sec if d['_wd'] == wd]
        if not wd_data:
            continue
        up_ok = sum(1 for d in wd_data if d['_ge0'])
        down_ok = sum(1 for d in wd_data if d['_le0'])
        if up_ok >= down_ok:
            best_wd_dirs[wd] = '上涨'
            best_ok += up_ok
        else:
            best_wd_dirs[wd] = '下跌'
            best_ok += down_ok
    
    total_ok += best_ok
    print(f"  {sector}: {best_ok}/{n} ({best_ok/n*100:.1f}%) dirs={best_wd_dirs}")

print(f"\n  总计: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 策略2: 板块×星期 + combined信号
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"策略2: 板块×星期基准 + combined信号覆盖")
print(f"{'=' * 80}")

# 先确定每个板块×星期的基准方向
sector_wd_base = {}
for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    for wd in range(5):
        wd_data = [d for d in sec if d['_wd'] == wd]
        if not wd_data:
            continue
        up_ok = sum(1 for d in wd_data if d['_ge0'])
        down_ok = sum(1 for d in wd_data if d['_le0'])
        sector_wd_base[(sector, wd)] = '上涨' if up_ok >= down_ok else '下跌'

# 在基准上叠加combined信号
for c_thresh in [0.0, 0.3, 0.5, 0.8, 1.0, 1.5]:
    total_ok = 0
    for sector in sectors:
        sec = [d for d in details if d['板块'] == sector]
        ok = 0
        for d in sec:
            base = sector_wd_base.get((sector, d['_wd']), '上涨')
            combined = d.get('融合信号', 0)
            
            if combined > c_thresh:
                pred = '上涨'
            elif combined < -c_thresh:
                pred = '下跌'
            else:
                pred = base
            
            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                ok += 1
        total_ok += ok
    print(f"  combined阈值{c_thresh}: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 策略3: 板块×星期 + combined + 美股信号
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"策略3: 板块×星期 + combined + 美股信号")
print(f"{'=' * 80}")

for c_thresh in [0.3, 0.5, 0.8]:
    for us_thresh in [0.3, 0.5, 1.0]:
        total_ok = 0
        for sector in sectors:
            sec = [d for d in details if d['板块'] == sector]
            ok = 0
            for d in sec:
                base = sector_wd_base.get((sector, d['_wd']), '上涨')
                combined = d.get('融合信号', 0)
                us = d.get('美股涨跌(%)', None)
                
                pred = base
                
                # combined覆盖
                if combined > c_thresh:
                    pred = '上涨'
                elif combined < -c_thresh:
                    pred = '下跌'
                
                # 美股大幅波动覆盖（反转信号）
                if us is not None and abs(us) > us_thresh:
                    if sector in ('科技', '汽车'):
                        # 科技/汽车: 美跌→A涨（反转）
                        if us < -us_thresh:
                            pred = '上涨'
                        elif us > us_thresh:
                            pred = '下跌'
                
                if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                    ok += 1
            total_ok += ok
        print(f"  c={c_thresh} us={us_thresh}: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 策略4: 全维度最优搜索（板块×星期 + combined + peer + US）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"策略4: 全维度最优搜索")
print(f"{'=' * 80}")

best_grand_total = 0
best_grand_config = {}

for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    n = len(sec)
    best_ok = 0
    best_cfg = None
    
    # 搜索参数空间
    for use_wd in [True, False]:
        for c_thresh in [0.0, 0.3, 0.5, 0.8, 1.0, 999]:
            for peer_mode in ['none', 'follow', 'contrarian']:
                for peer_thresh in [0.5, 1.0, 1.5]:
                    for us_mode in ['none', 'follow', 'contrarian']:
                        for us_thresh in [0.3, 0.5, 1.0]:
                            ok = 0
                            for d in sec:
                                # 基准方向
                                if use_wd:
                                    base = sector_wd_base.get((sector, d['_wd']), '上涨')
                                else:
                                    up_base = sum(1 for x in sec if x['_ge0'])
                                    base = '上涨' if up_base >= n/2 else '下跌'
                                
                                pred = base
                                combined = d.get('融合信号', 0)
                                peer = d.get('同行信号', 0)
                                us = d.get('美股涨跌(%)', None)
                                
                                # combined覆盖
                                if c_thresh < 999:
                                    if combined > c_thresh:
                                        pred = '上涨'
                                    elif combined < -c_thresh:
                                        pred = '下跌'
                                
                                # peer覆盖
                                if peer_mode == 'follow' and abs(peer) > peer_thresh:
                                    if peer > peer_thresh:
                                        pred = '上涨'
                                    else:
                                        pred = '下跌'
                                elif peer_mode == 'contrarian' and abs(peer) > peer_thresh:
                                    if peer > peer_thresh:
                                        pred = '下跌'
                                    else:
                                        pred = '上涨'
                                
                                # US覆盖
                                if us is not None and us_mode != 'none' and abs(us) > us_thresh:
                                    if us_mode == 'follow':
                                        pred = '上涨' if us > 0 else '下跌'
                                    else:  # contrarian
                                        pred = '下跌' if us > 0 else '上涨'
                                
                                if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                                    ok += 1
                            
                            if ok > best_ok:
                                best_ok = ok
                                best_cfg = {
                                    'use_wd': use_wd,
                                    'c_thresh': c_thresh,
                                    'peer_mode': peer_mode,
                                    'peer_thresh': peer_thresh,
                                    'us_mode': us_mode,
                                    'us_thresh': us_thresh,
                                }
    
    best_grand_total += best_ok
    best_grand_config[sector] = best_cfg
    print(f"  {sector}: {best_ok}/{n} ({best_ok/n*100:.1f}%) cfg={best_cfg}")

print(f"\n  总计: {best_grand_total}/{total} ({best_grand_total/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 策略5: 优先级链式决策（最强信号优先）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"策略5: 优先级链式决策（信号强度优先）")
print(f"{'=' * 80}")

# 对每个板块，按信号强度排序决策
# 优先级: 极端z_today > 强peer信号 > 强combined > 美股大波动 > 星期基准
for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    n = len(sec)
    
    # 确定peer是follow还是contrarian
    # 测试两种模式
    for peer_mode in ['follow', 'contrarian']:
        ok = 0
        for d in sec:
            combined = d.get('融合信号', 0)
            peer = d.get('同行信号', 0)
            us = d.get('美股涨跌(%)', 0) or 0
            z = d.get('z_today', 0)
            score = d.get('评分', 50)
            base = sector_wd_base.get((sector, d['_wd']), '上涨')
            
            pred = base  # 默认
            
            # 层级4: combined信号
            if abs(combined) > 0.5:
                pred = '上涨' if combined > 0 else '下跌'
            
            # 层级3: 美股大波动（反转）
            if abs(us) > 1.0:
                if sector in ('科技', '汽车', '制造'):
                    pred = '上涨' if us < 0 else '下跌'
            
            # 层级2: 强peer信号
            if abs(peer) > 1.5:
                if peer_mode == 'follow':
                    pred = '上涨' if peer > 0 else '下跌'
                else:
                    pred = '下跌' if peer > 0 else '上涨'
            
            # 层级1: 极端z_today反转
            if abs(z) > 2.0:
                pred = '下跌' if z > 0 else '上涨'
            
            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                ok += 1
        
        print(f"  {sector} peer={peer_mode}: {ok}/{n} ({ok/n*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 策略6: 投票制（多信号投票）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"策略6: 多信号投票制")
print(f"{'=' * 80}")

for vote_thresh in [1, 2, 3]:
    total_ok = 0
    for sector in sectors:
        sec = [d for d in details if d['板块'] == sector]
        n = len(sec)
        
        # 确定该板块的peer模式
        peer_contrarian = sector in ('科技', '化工', '有色金属', '新能源')
        
        ok = 0
        for d in sec:
            combined = d.get('融合信号', 0)
            peer = d.get('同行信号', 0)
            us = d.get('美股涨跌(%)', 0) or 0
            z = d.get('z_today', 0)
            base = sector_wd_base.get((sector, d['_wd']), '上涨')
            
            # 投票
            votes_up = 0
            votes_down = 0
            
            # 信号1: 星期基准
            if base == '上涨':
                votes_up += 1
            else:
                votes_down += 1
            
            # 信号2: combined
            if combined > 0.3:
                votes_up += 1
            elif combined < -0.3:
                votes_down += 1
            
            # 信号3: peer
            if peer_contrarian:
                if peer > 0.5:
                    votes_down += 1
                elif peer < -0.5:
                    votes_up += 1
            else:
                if peer > 0.5:
                    votes_up += 1
                elif peer < -0.5:
                    votes_down += 1
            
            # 信号4: 美股反转（科技/汽车/制造）
            if sector in ('科技', '汽车', '制造'):
                if us > 0.5:
                    votes_down += 1
                elif us < -0.5:
                    votes_up += 1
            elif sector in ('有色金属', '化工'):
                if us > 0.5:
                    votes_up += 1
                elif us < -0.5:
                    votes_down += 1
            
            # 信号5: z_today反转
            if z > 1.0:
                votes_down += 1
            elif z < -1.0:
                votes_up += 1
            
            # 决策
            net_votes = votes_up - votes_down
            if net_votes >= vote_thresh:
                pred = '上涨'
            elif net_votes <= -vote_thresh:
                pred = '下跌'
            else:
                pred = base
            
            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                ok += 1
        
        total_ok += ok
    print(f"  投票阈值{vote_thresh}: {total_ok}/{total} ({total_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 策略7: 板块独立最优投票配置
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"策略7: 板块独立最优投票配置")
print(f"{'=' * 80}")

grand_total = 0
for sector in sectors:
    sec = [d for d in details if d['板块'] == sector]
    n = len(sec)
    best_ok = 0
    best_cfg = None
    
    for peer_mode in ['follow', 'contrarian', 'none']:
        for us_mode in ['follow', 'contrarian', 'none']:
            for c_thresh in [0.3, 0.5, 0.8]:
                for z_thresh in [0.8, 1.0, 1.5, 2.0]:
                    for vote_thresh in [1, 2]:
                        ok = 0
                        for d in sec:
                            combined = d.get('融合信号', 0)
                            peer = d.get('同行信号', 0)
                            us = d.get('美股涨跌(%)', 0) or 0
                            z = d.get('z_today', 0)
                            base = sector_wd_base.get((sector, d['_wd']), '上涨')
                            
                            votes_up, votes_down = 0, 0
                            
                            if base == '上涨': votes_up += 1
                            else: votes_down += 1
                            
                            if combined > c_thresh: votes_up += 1
                            elif combined < -c_thresh: votes_down += 1
                            
                            if peer_mode == 'follow':
                                if peer > 0.5: votes_up += 1
                                elif peer < -0.5: votes_down += 1
                            elif peer_mode == 'contrarian':
                                if peer > 0.5: votes_down += 1
                                elif peer < -0.5: votes_up += 1
                            
                            if us_mode == 'follow':
                                if us > 0.5: votes_up += 1
                                elif us < -0.5: votes_down += 1
                            elif us_mode == 'contrarian':
                                if us > 0.5: votes_down += 1
                                elif us < -0.5: votes_up += 1
                            
                            if z > z_thresh: votes_down += 1
                            elif z < -z_thresh: votes_up += 1
                            
                            net = votes_up - votes_down
                            if net >= vote_thresh: pred = '上涨'
                            elif net <= -vote_thresh: pred = '下跌'
                            else: pred = base
                            
                            if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
                                ok += 1
                        
                        if ok > best_ok:
                            best_ok = ok
                            best_cfg = {
                                'peer': peer_mode, 'us': us_mode,
                                'c': c_thresh, 'z': z_thresh, 'vote': vote_thresh
                            }
    
    grand_total += best_ok
    print(f"  {sector}: {best_ok}/{n} ({best_ok/n*100:.1f}%) cfg={best_cfg}")

print(f"\n  总计: {grand_total}/{total} ({grand_total/total*100:.1f}%)")

print(f"\n{'=' * 80}")
print(f"分析完成")
print(f"{'=' * 80}")
