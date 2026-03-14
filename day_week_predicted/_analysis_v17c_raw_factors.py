#!/usr/bin/env python3
"""
v17c: 分析原始因子值的预测能力。
不使用combined信号，直接分析每个因子对方向的预测力。
寻找"高置信度"条件组合。
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

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_ge0'] = d['_actual'] >= 0
    d['_le0'] = d['_actual'] <= 0
    try:
        d['_wd'] = datetime.strptime(d['评分日'], '%Y-%m-%d').weekday()
    except:
        d['_wd'] = -1

# ═══════════════════════════════════════════════════════════
# 分析1: 每个因子的方向预测力（按板块）
# ═══════════════════════════════════════════════════════════
print(f"{'=' * 80}")
print(f"分析1: 各因子方向预测力")
print(f"{'=' * 80}")

# 逐日详情中没有原始因子值，只有融合信号等
# 但我们有 z_today, 融合信号, 技术信号, 同行信号, RS信号, 美股隔夜, 波动率状态, 评分
# 让我们分析这些可用维度的组合

# 分析: z_today 的方向预测力
print(f"\n--- z_today 方向预测力 ---")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    
    # z_today > 0 → 今天涨了 → 明天？
    z_pos = [d for d in sec_data if d.get('z_today', 0) > 0]
    z_neg = [d for d in sec_data if d.get('z_today', 0) < 0]
    z_zero = [d for d in sec_data if d.get('z_today', 0) == 0]
    
    if z_pos:
        z_pos_ge0 = sum(1 for d in z_pos if d['_ge0'])
        z_pos_le0 = sum(1 for d in z_pos if d['_le0'])
        print(f"  {sec} z>0(n={len(z_pos)}): 次日>=0%={z_pos_ge0}({z_pos_ge0/len(z_pos)*100:.1f}%) "
              f"次日<=0%={z_pos_le0}({z_pos_le0/len(z_pos)*100:.1f}%)")
    if z_neg:
        z_neg_ge0 = sum(1 for d in z_neg if d['_ge0'])
        z_neg_le0 = sum(1 for d in z_neg if d['_le0'])
        print(f"  {sec} z<0(n={len(z_neg)}): 次日>=0%={z_neg_ge0}({z_neg_ge0/len(z_neg)*100:.1f}%) "
              f"次日<=0%={z_neg_le0}({z_neg_le0/len(z_neg)*100:.1f}%)")

# 分析: 同行信号 的方向预测力
print(f"\n--- 同行信号 方向预测力 ---")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    
    peer_pos = [d for d in sec_data if d.get('同行信号', 0) > 0]
    peer_neg = [d for d in sec_data if d.get('同行信号', 0) < 0]
    
    if peer_pos:
        pp_ge0 = sum(1 for d in peer_pos if d['_ge0'])
        print(f"  {sec} peer>0(n={len(peer_pos)}): 次日>=0%={pp_ge0}({pp_ge0/len(peer_pos)*100:.1f}%)")
    if peer_neg:
        pn_ge0 = sum(1 for d in peer_neg if d['_ge0'])
        print(f"  {sec} peer<0(n={len(peer_neg)}): 次日>=0%={pn_ge0}({pn_ge0/len(peer_neg)*100:.1f}%)")

# 分析: 美股隔夜 的方向预测力
print(f"\n--- 美股隔夜 方向预测力 ---")
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    
    us_pos = [d for d in sec_data if (d.get('美股涨跌(%)') or 0) > 0]
    us_neg = [d for d in sec_data if (d.get('美股涨跌(%)') or 0) < 0]
    us_none = [d for d in sec_data if d.get('美股涨跌(%)') is None]
    
    if us_pos:
        up_ge0 = sum(1 for d in us_pos if d['_ge0'])
        print(f"  {sec} US>0(n={len(us_pos)}): 次日>=0%={up_ge0}({up_ge0/len(us_pos)*100:.1f}%)")
    if us_neg:
        un_ge0 = sum(1 for d in us_neg if d['_ge0'])
        print(f"  {sec} US<0(n={len(us_neg)}): 次日>=0%={un_ge0}({un_ge0/len(us_neg)*100:.1f}%)")
    if us_none:
        print(f"  {sec} US=None: {len(us_none)}")

# ═══════════════════════════════════════════════════════════
# 分析2: 高置信度条件组合
# 找出 >=65% 准确率的条件组合
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析2: 高置信度条件组合 (>=65%准确率, >=20样本)")
print(f"{'=' * 80}")

def check_rule(data_list, pred_dir):
    """检查一组数据如果全部预测pred_dir的准确率"""
    n = len(data_list)
    if n < 20:
        return None
    if pred_dir == '上涨':
        ok = sum(1 for d in data_list if d['_ge0'])
    else:
        ok = sum(1 for d in data_list if d['_le0'])
    return ok / n * 100

# 条件维度
def get_z_cat(d):
    z = d.get('z_today', 0)
    if z > 1.0: return 'z>1'
    if z > 0.3: return 'z>0.3'
    if z > -0.3: return 'z~0'
    if z > -1.0: return 'z<-0.3'
    return 'z<-1'

def get_peer_cat(d):
    p = d.get('同行信号', 0)
    if p > 1.0: return 'peer+'
    if p > -1.0: return 'peer0'
    return 'peer-'

def get_us_cat(d):
    u = d.get('美股涨跌(%)')
    if u is None: return 'us?'
    if u > 0.5: return 'us+'
    if u > -0.5: return 'us0'
    return 'us-'

def get_score_cat(d):
    s = d.get('评分', 50)
    if s >= 55: return 'score+'
    if s >= 45: return 'score0'
    return 'score-'

def get_wd_cat(d):
    return f"wd{d['_wd']}"

# 搜索所有2维组合
dims = [
    ('z', get_z_cat),
    ('peer', get_peer_cat),
    ('us', get_us_cat),
    ('score', get_score_cat),
    ('wd', get_wd_cat),
]

found_rules = []

for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    
    # 单维度
    for dim_name, dim_fn in dims:
        groups = defaultdict(list)
        for d in sec_data:
            groups[dim_fn(d)].append(d)
        
        for cat, group in groups.items():
            for pred_dir in ['上涨', '下跌']:
                rate = check_rule(group, pred_dir)
                if rate and rate >= 62:
                    found_rules.append({
                        'sector': sec, 'dims': f"{dim_name}={cat}",
                        'pred': pred_dir, 'rate': rate, 'n': len(group)
                    })
    
    # 2维组合
    for i, (d1_name, d1_fn) in enumerate(dims):
        for j, (d2_name, d2_fn) in enumerate(dims):
            if j <= i:
                continue
            groups = defaultdict(list)
            for d in sec_data:
                key = f"{d1_fn(d)}_{d2_fn(d)}"
                groups[key].append(d)
            
            for cat, group in groups.items():
                for pred_dir in ['上涨', '下跌']:
                    rate = check_rule(group, pred_dir)
                    if rate and rate >= 65:
                        found_rules.append({
                            'sector': sec, 'dims': f"{d1_name}+{d2_name}={cat}",
                            'pred': pred_dir, 'rate': rate, 'n': len(group)
                        })

# 排序输出
found_rules.sort(key=lambda x: (-x['rate'], -x['n']))
print(f"\n找到 {len(found_rules)} 条规则:")
for r in found_rules[:50]:
    print(f"  {r['sector']} | {r['dims']} → {r['pred']} | "
          f"{r['rate']:.1f}% (n={r['n']})")

# ═══════════════════════════════════════════════════════════
# 分析3: 时间序列验证高置信度规则
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析3: 时间序列验证（前半训练→后半测试）")
print(f"{'=' * 80}")

dates = sorted(set(d['评分日'] for d in details))
mid_date = dates[len(dates) // 2]
train = [d for d in details if d['评分日'] <= mid_date]
test = [d for d in details if d['评分日'] > mid_date]

# 在训练集上找规则，在测试集上验证
print(f"训练集: {len(train)}, 测试集: {len(test)}")

# 策略: 对每个(板块, z_cat)找最优方向
train_rules = {}
for sec in sectors:
    sec_train = [d for d in train if d['板块'] == sec]
    
    for z_cat in ['z>1', 'z>0.3', 'z~0', 'z<-0.3', 'z<-1']:
        group = [d for d in sec_train if get_z_cat(d) == z_cat]
        if not group:
            continue
        ge0 = sum(1 for d in group if d['_ge0'])
        le0 = sum(1 for d in group if d['_le0'])
        best_dir = '上涨' if ge0 >= le0 else '下跌'
        train_rules[f"{sec}_{z_cat}"] = best_dir

# 测试
ok = 0
for d in test:
    sec = d['板块']
    z_cat = get_z_cat(d)
    key = f"{sec}_{z_cat}"
    pred = train_rules.get(key, '上涨')
    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
        ok += 1
print(f"  sector+z_cat: {ok}/{len(test)} ({ok/len(test)*100:.1f}%)")

# 策略: 对每个(板块, z_cat, peer_cat)找最优方向
train_rules2 = {}
for sec in sectors:
    sec_train = [d for d in train if d['板块'] == sec]
    
    for z_cat in ['z>1', 'z>0.3', 'z~0', 'z<-0.3', 'z<-1']:
        for p_cat in ['peer+', 'peer0', 'peer-']:
            group = [d for d in sec_train if get_z_cat(d) == z_cat and get_peer_cat(d) == p_cat]
            if not group:
                continue
            ge0 = sum(1 for d in group if d['_ge0'])
            le0 = sum(1 for d in group if d['_le0'])
            best_dir = '上涨' if ge0 >= le0 else '下跌'
            train_rules2[f"{sec}_{z_cat}_{p_cat}"] = best_dir

ok = 0
for d in test:
    sec = d['板块']
    z_cat = get_z_cat(d)
    p_cat = get_peer_cat(d)
    key = f"{sec}_{z_cat}_{p_cat}"
    pred = train_rules2.get(key, '上涨')
    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
        ok += 1
print(f"  sector+z_cat+peer: {ok}/{len(test)} ({ok/len(test)*100:.1f}%)")

# 策略: 对每个(板块, z_cat, wd)找最优方向
train_rules3 = {}
for sec in sectors:
    sec_train = [d for d in train if d['板块'] == sec]
    
    for z_cat in ['z>1', 'z>0.3', 'z~0', 'z<-0.3', 'z<-1']:
        for wd in range(5):
            group = [d for d in sec_train if get_z_cat(d) == z_cat and d['_wd'] == wd]
            if not group:
                continue
            ge0 = sum(1 for d in group if d['_ge0'])
            le0 = sum(1 for d in group if d['_le0'])
            best_dir = '上涨' if ge0 >= le0 else '下跌'
            train_rules3[f"{sec}_{z_cat}_{wd}"] = best_dir

ok = 0
for d in test:
    sec = d['板块']
    z_cat = get_z_cat(d)
    wd = d['_wd']
    key = f"{sec}_{z_cat}_{wd}"
    pred = train_rules3.get(key, '上涨')
    if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
        ok += 1
print(f"  sector+z_cat+wd: {ok}/{len(test)} ({ok/len(test)*100:.1f}%)")

# 策略: 纯z反转（不分板块）
for z_t in [0.3, 0.5, 0.8, 1.0]:
    ok = 0
    for d in test:
        z = d.get('z_today', 0)
        sec = d['板块']
        base_rates = {'化工': 0.61, '有色金属': 0.579, '新能源': 0.51,
                     '制造': 0.479, '科技': 0.458, '汽车': 0.453, '医药': 0.446}
        if z > z_t:
            pred = '下跌'
        elif z < -z_t:
            pred = '上涨'
        else:
            pred = '上涨' if base_rates.get(sec, 0.5) > 0.5 else '下跌'
        if (pred == '上涨' and d['_ge0']) or (pred == '下跌' and d['_le0']):
            ok += 1
    print(f"  z反转(z={z_t})+基准: {ok}/{len(test)} ({ok/len(test)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析4: 新特征 - 连续涨跌天数
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析4: 连续涨跌天数的预测力")
print(f"{'=' * 80}")

# 我们没有直接的连续涨跌天数，但可以从z_today推断
# z_today > 0 表示今天涨，z_today < 0 表示今天跌
# 但我们需要连续天数信息...

# 用评分作为proxy
for sec in sectors:
    sec_data = [d for d in details if d['板块'] == sec]
    n = len(sec_data)
    
    # 按评分分组
    for score_range, lo, hi in [('<40', 0, 40), ('40-45', 40, 45), ('45-50', 45, 50),
                                  ('50-55', 50, 55), ('55-60', 55, 60), ('>=60', 60, 100)]:
        group = [d for d in sec_data if lo <= d.get('评分', 50) < hi]
        if len(group) < 10:
            continue
        ge0 = sum(1 for d in group if d['_ge0'])
        le0 = sum(1 for d in group if d['_le0'])
        best = max(ge0, le0)
        best_dir = '上涨' if ge0 >= le0 else '下跌'
        rate = best / len(group) * 100
        marker = '✓' if rate >= 60 else ''
        print(f"  {sec} score{score_range}(n={len(group)}): "
              f">=0%={ge0}({ge0/len(group)*100:.1f}%) → {best_dir} {rate:.1f}% {marker}")

print(f"\n{'=' * 80}")
print(f"分析完成")
print(f"{'=' * 80}")
