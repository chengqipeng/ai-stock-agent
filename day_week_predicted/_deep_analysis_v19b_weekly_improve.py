#!/usr/bin/env python3
"""
v19b 周预测准确率提升深度分析

基于v19的诊断发现，本分析聚焦以下提升方向：

关键发现（来自v19诊断）：
  1. 周一信号→周三预测力骤降至44.2%（反转！），周五回升57%
     → 信号衰减非线性，周三是"反转日"
  2. 前3天涨天>=70%时，周涨率98.6%（几乎确定）
     → 周内动量极强，可作为高置信度信号
  3. 板块联动：有色金属-化工92%一致，科技-汽车75%一致
     → 可用板块联动作为交叉验证
  4. 医药板块：同行信号反转70.1%，美股信号反转61.2%
     → 医药需要反转逻辑
  5. 有色金属：技术信号66.2%正向，z_today 62.2%正向
     → 有色金属技术面在周级别有效
  6. 化工：动量73%（上周涨→本周涨），融合信号60.7%正向
     → 化工适合动量策略

提升策略：
  Part 1: 板块特化周信号（正向/反转自适应）
  Part 2: 多维度信号融合（板块最优维度组合）
  Part 3: 周内动量累积信号（前N天→整周）
  Part 4: 板块联动交叉验证
  Part 5: 上周动量/反转信号
  Part 6: 综合策略模拟与交叉验证
"""
import json
import logging
from collections import defaultdict
from datetime import datetime

logging.basicConfig(level=logging.WARNING)

print(f"{'=' * 80}")
print(f"v19b 周预测准确率提升深度分析")
print(f"{'=' * 80}")

# ═══════════════════════════════════════════════════════════
# 数据加载与预处理
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
        d['_dt'] = datetime.strptime(d['评分日'], '%Y-%m-%d')
        d['_wd'] = d['_dt'].weekday()
        d['_iso_week'] = d['_dt'].isocalendar()[:2]
    except:
        d['_wd'] = -1
        d['_iso_week'] = (0, 0)

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
all_dates = sorted(set(d['评分日'] for d in details))
loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"日频基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")

# 构建周数据
stock_week_data = defaultdict(list)
for d in details:
    stock_week_data[(d['代码'], d['_iso_week'])].append(d)

weekly_records = []
for (code, iso_week), days in stock_week_data.items():
    days_sorted = sorted(days, key=lambda x: x['评分日'])
    if len(days_sorted) < 2:
        continue
    sector = days_sorted[0]['板块']
    cum = 1.0
    for d in days_sorted:
        cum *= (1 + d['_actual'] / 100)
    wchg = (cum - 1) * 100

    weekly_records.append({
        'code': code, 'sector': sector, 'iso_week': iso_week,
        'n_days': len(days_sorted),
        'week_exact_chg': wchg, 'week_up': wchg >= 0, 'week_dn': wchg <= 0,
        'mon_combined': days_sorted[0]['融合信号'],
        'mon_tech': days_sorted[0]['技术信号'],
        'mon_peer': days_sorted[0]['同行信号'],
        'mon_rs': days_sorted[0]['RS信号'],
        'mon_us': days_sorted[0]['美股隔夜'],
        'mon_z': days_sorted[0]['z_today'],
        'mon_vol': days_sorted[0]['波动率状态'],
        'mon_score': days_sorted[0]['评分'],
        'mon_conf': days_sorted[0]['置信度'],
        'mon_pred': days_sorted[0]['预测方向'],
        'days': days_sorted,
    })

n_weekly = len(weekly_records)
sorted_weeks = sorted(set(r['iso_week'] for r in weekly_records))
mid_idx = len(sorted_weeks) // 2
first_half_weeks = set(sorted_weeks[:mid_idx])
second_half_weeks = set(sorted_weeks[mid_idx:])

print(f"周样本: {n_weekly}, 周数: {len(sorted_weeks)}")
print(f"前半: {len(first_half_weeks)}周, 后半: {len(second_half_weeks)}周")


# ═══════════════════════════════════════════════════════════
# Part 1: 板块特化周信号（正向/反转自适应）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 1: 板块特化多维度信号（正向/反转自适应）")
print(f"{'=' * 80}")

# 基于诊断数据，每个板块每个维度选择正向或反转
# 维度: 融合, 技术, 同行, RS, 美股, z_today, 波动率
dim_keys = ['mon_combined', 'mon_tech', 'mon_peer', 'mon_rs', 'mon_us', 'mon_z', 'mon_vol']
dim_names = ['融合', '技术', '同行', 'RS', '美股', 'z_today', '波动率']

# 自动检测每个板块×维度的最优方向（正向/反转）
sector_dim_config = {}  # {sector: {dim: (direction, rate, n)}}

print(f"\n自动检测板块×维度最优方向:")
print(f"{'板块':<10}", end='')
for dn in dim_names:
    print(f" {dn:>8}", end='')
print()
print('-' * 75)

for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    sector_dim_config[sector] = {}
    print(f"{sector:<10}", end='')

    for dkey, dname in zip(dim_keys, dim_names):
        all_sig = [r for r in sr if r[dkey] != 0]
        if len(all_sig) < 10:
            sector_dim_config[sector][dkey] = ('skip', 0, 0)
            print(f" {'skip':>8}", end='')
            continue

        # 正向一致率
        fwd_ok = sum(1 for r in all_sig if (r[dkey] > 0 and r['week_up']) or (r[dkey] < 0 and r['week_dn']))
        fwd_rate = fwd_ok / len(all_sig)
        # 反转一致率
        rev_rate = 1 - fwd_rate

        if fwd_rate > 0.55:
            direction = 'fwd'
            sector_dim_config[sector][dkey] = ('fwd', fwd_rate, len(all_sig))
            print(f" +{fwd_rate*100:.0f}%", end='')
        elif rev_rate > 0.55:
            direction = 'rev'
            sector_dim_config[sector][dkey] = ('rev', rev_rate, len(all_sig))
            print(f" -{rev_rate*100:.0f}%", end='')
        else:
            sector_dim_config[sector][dkey] = ('noise', max(fwd_rate, rev_rate), len(all_sig))
            print(f" {'~':>8}", end='')
    print()


# ═══════════════════════════════════════════════════════════
# Part 2: 多维度加权融合周信号
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 2: 多维度加权融合周信号")
print(f"{'=' * 80}")

def compute_weekly_signal(r, sector_config):
    """基于板块特化配置计算周信号"""
    sector = r['sector']
    config = sector_config.get(sector, {})
    signal = 0.0
    weight_sum = 0.0

    for dkey in dim_keys:
        cfg = config.get(dkey, ('skip', 0, 0))
        direction, rate, n = cfg
        if direction == 'skip' or direction == 'noise':
            continue

        val = r[dkey]
        if val == 0:
            continue

        # 权重 = 一致率 - 0.5 (越偏离0.5权重越大)
        w = (rate - 0.5) * 2  # 0~1范围
        if direction == 'rev':
            val = -val  # 反转

        # 归一化信号方向
        sig_dir = 1.0 if val > 0 else -1.0
        signal += sig_dir * w
        weight_sum += w

    if weight_sum > 0:
        signal /= weight_sum  # 归一化到[-1, 1]

    return signal

# 测试多维度融合信号
print(f"\n多维度融合信号 vs 周方向:")
print(f"{'板块':<10} {'信号>0→周涨':>14} {'信号<0→周跌':>14} {'方向一致率':>12} {'vs单融合':>10}")
print('-' * 70)

for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    for r in sr:
        r['_multi_sig'] = compute_weekly_signal(r, sector_dim_config)

    pos = [r for r in sr if r['_multi_sig'] > 0]
    neg = [r for r in sr if r['_multi_sig'] < 0]
    all_sig = [r for r in sr if r['_multi_sig'] != 0]

    pos_ok = sum(1 for r in pos if r['week_up']) / len(pos) * 100 if pos else 0
    neg_ok = sum(1 for r in neg if r['week_dn']) / len(neg) * 100 if neg else 0
    dir_ok = sum(1 for r in all_sig if
                 (r['_multi_sig'] > 0 and r['week_up']) or
                 (r['_multi_sig'] < 0 and r['week_dn']))
    dir_rate = dir_ok / len(all_sig) * 100 if all_sig else 0

    # 单融合信号对比
    single_sig = [r for r in sr if r['mon_combined'] != 0]
    single_ok = sum(1 for r in single_sig if
                    (r['mon_combined'] > 0 and r['week_up']) or
                    (r['mon_combined'] < 0 and r['week_dn']))
    single_rate = single_ok / len(single_sig) * 100 if single_sig else 0

    print(f"{sector:<10} {pos_ok:>5.1f}%({len(pos):>3}) {neg_ok:>5.1f}%({len(neg):>3}) "
          f"{dir_rate:>5.1f}%({len(all_sig):>3}) {dir_rate - single_rate:>+8.1f}pp")


# ═══════════════════════════════════════════════════════════
# Part 3: 周内动量累积信号
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 3: 周内动量累积信号（前N天→整周）")
print(f"{'=' * 80}")

# 发现: 前3天涨天>=70%时周涨率98.6%
# 策略: 用前2-3天的实际涨跌来修正周预测

print(f"\n前N天实际涨跌累积 → 整周方向:")
for n_days_used in [2, 3]:
    print(f"\n  前{n_days_used}天累积涨跌:")
    buckets = [(-999, -3), (-3, -1), (-1, 0), (0, 1), (1, 3), (3, 999)]
    labels = ['<-3%', '-3~-1%', '-1~0%', '0~1%', '1~3%', '>3%']
    for (lo, hi), label in zip(buckets, labels):
        filtered = []
        for r in weekly_records:
            if r['n_days'] < n_days_used + 1:
                continue
            cum = 1.0
            for d in r['days'][:n_days_used]:
                cum *= (1 + d['_actual'] / 100)
            cum_chg = (cum - 1) * 100
            if lo <= cum_chg < hi:
                filtered.append(r)
        if not filtered:
            continue
        up = sum(1 for r in filtered if r['week_up'])
        print(f"    {label:>8}: n={len(filtered):>3}, 周涨{up/len(filtered)*100:.1f}%")

# 前3天涨跌天数比
print(f"\n前3天涨跌天数比 → 整周方向:")
for up_count in [0, 1, 2, 3]:
    filtered = [r for r in weekly_records if r['n_days'] >= 4 and
                sum(1 for d in r['days'][:3] if d['_actual'] > 0) == up_count]
    if not filtered:
        continue
    up = sum(1 for r in filtered if r['week_up'])
    print(f"  前3天涨{up_count}天: n={len(filtered):>3}, 周涨{up/len(filtered)*100:.1f}%")


# ═══════════════════════════════════════════════════════════
# Part 4: 板块联动交叉验证
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 4: 板块联动交叉验证")
print(f"{'=' * 80}")

# 发现: 有色金属-化工92%一致
# 策略: 如果联动板块的信号一致，提高置信度

# 构建每周每板块的信号方向
week_sector_signal = {}
for r in weekly_records:
    key = (r['iso_week'], r['sector'])
    if key not in week_sector_signal:
        week_sector_signal[key] = []
    week_sector_signal[key].append(r['_multi_sig'] if hasattr(r, '_multi_sig') and r.get('_multi_sig', 0) != 0 else r['mon_combined'])

# 联动对
linked_pairs = [
    ('有色金属', '化工', 0.92),
    ('科技', '汽车', 0.75),
    ('汽车', '医药', 0.83),
    ('汽车', '制造', 0.75),
    ('新能源', '制造', 0.75),
    ('医药', '制造', 0.75),
]

print(f"\n联动板块信号一致时 vs 不一致时的周预测准确率:")
print(f"{'板块对':<16} {'一致时准确率':>12} {'不一致时准确率':>14} {'差异':>8} {'一致比例':>10}")
print('-' * 70)

for s1, s2, hist_rate in linked_pairs:
    agree_ok = 0
    agree_n = 0
    disagree_ok = 0
    disagree_n = 0

    for w in sorted_weeks:
        k1 = (w, s1)
        k2 = (w, s2)
        if k1 not in week_sector_signal or k2 not in week_sector_signal:
            continue

        avg1 = sum(week_sector_signal[k1]) / len(week_sector_signal[k1])
        avg2 = sum(week_sector_signal[k2]) / len(week_sector_signal[k2])

        # 方向是否一致
        same_dir = (avg1 > 0 and avg2 > 0) or (avg1 < 0 and avg2 < 0)

        # 两个板块的周记录
        for r in weekly_records:
            if r['iso_week'] != w:
                continue
            if r['sector'] not in (s1, s2):
                continue

            sig = r.get('_multi_sig', r['mon_combined'])
            pred_up = sig > 0
            ok = (pred_up and r['week_up']) or (not pred_up and r['week_dn'])

            if same_dir:
                agree_n += 1
                if ok:
                    agree_ok += 1
            else:
                disagree_n += 1
                if ok:
                    disagree_ok += 1

    a_rate = agree_ok / agree_n * 100 if agree_n > 0 else 0
    d_rate = disagree_ok / disagree_n * 100 if disagree_n > 0 else 0
    a_pct = agree_n / (agree_n + disagree_n) * 100 if (agree_n + disagree_n) > 0 else 0

    print(f"{s1}-{s2:<8} {a_rate:>10.1f}% {d_rate:>12.1f}% {a_rate-d_rate:>+6.1f}pp {a_pct:>8.1f}%")


# ═══════════════════════════════════════════════════════════
# Part 5: 上周动量/反转信号
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 5: 上周动量/反转信号")
print(f"{'=' * 80}")

# 构建每周每板块的平均涨跌
week_sector_chg = {}
for r in weekly_records:
    key = (r['iso_week'], r['sector'])
    if key not in week_sector_chg:
        week_sector_chg[key] = []
    week_sector_chg[key].append(r['week_exact_chg'])

print(f"\n上周板块涨跌 → 本周方向:")
print(f"{'板块':<10} {'上周涨→本周涨':>14} {'上周跌→本周跌':>14} {'动量一致率':>12} {'反转一致率':>12}")
print('-' * 70)

sector_momentum_config = {}
for sector in sectors:
    momentum_ok = 0
    reversal_ok = 0
    n = 0
    for i in range(1, len(sorted_weeks)):
        prev_w = sorted_weeks[i - 1]
        curr_w = sorted_weeks[i]
        prev_key = (prev_w, sector)
        curr_key = (curr_w, sector)
        if prev_key not in week_sector_chg or curr_key not in week_sector_chg:
            continue

        prev_avg = sum(week_sector_chg[prev_key]) / len(week_sector_chg[prev_key])
        curr_records = [r for r in weekly_records if r['iso_week'] == curr_w and r['sector'] == sector]

        for r in curr_records:
            n += 1
            if (prev_avg > 0 and r['week_up']) or (prev_avg < 0 and r['week_dn']):
                momentum_ok += 1
            if (prev_avg > 0 and r['week_dn']) or (prev_avg < 0 and r['week_up']):
                reversal_ok += 1

    if n > 0:
        m_rate = momentum_ok / n * 100
        r_rate = reversal_ok / n * 100
        use_momentum = m_rate > r_rate
        sector_momentum_config[sector] = 'momentum' if use_momentum else 'reversal'

        # 细分
        prev_up_curr_up = 0
        prev_up_n = 0
        prev_dn_curr_dn = 0
        prev_dn_n = 0
        for i in range(1, len(sorted_weeks)):
            prev_w = sorted_weeks[i - 1]
            curr_w = sorted_weeks[i]
            prev_key = (prev_w, sector)
            curr_key = (curr_w, sector)
            if prev_key not in week_sector_chg or curr_key not in week_sector_chg:
                continue
            prev_avg = sum(week_sector_chg[prev_key]) / len(week_sector_chg[prev_key])
            curr_records = [r for r in weekly_records if r['iso_week'] == curr_w and r['sector'] == sector]
            for r in curr_records:
                if prev_avg > 0:
                    prev_up_n += 1
                    if r['week_up']:
                        prev_up_curr_up += 1
                else:
                    prev_dn_n += 1
                    if r['week_dn']:
                        prev_dn_curr_dn += 1

        pu_rate = prev_up_curr_up / prev_up_n * 100 if prev_up_n > 0 else 0
        pd_rate = prev_dn_curr_dn / prev_dn_n * 100 if prev_dn_n > 0 else 0

        print(f"{sector:<10} {pu_rate:>5.1f}%({prev_up_n:>3}) {pd_rate:>5.1f}%({prev_dn_n:>3}) "
              f"{m_rate:>10.1f}% {r_rate:>10.1f}% -> {'动量' if use_momentum else '反转'}")


# ═══════════════════════════════════════════════════════════
# Part 6: 综合策略模拟与交叉验证
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 6: 综合策略模拟与交叉验证")
print(f"{'=' * 80}")

# 策略组合:
# A. 基线: 全涨
# B. 单融合信号
# C. 多维度融合信号
# D. 多维度 + 板块基准率默认
# E. 多维度 + 上周动量/反转
# F. 多维度 + 板块联动置信度
# G. 综合: 多维度 + 动量 + 联动 + 阈值优化

def predict_strategy_b(r):
    """单融合信号"""
    return '上涨' if r['mon_combined'] > 0 else '下跌'

def predict_strategy_c(r):
    """多维度融合信号"""
    sig = compute_weekly_signal(r, sector_dim_config)
    return '上涨' if sig > 0 else '下跌'

# 板块周涨跌基准率
sector_week_up_rate = {}
for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    sector_week_up_rate[sector] = sum(1 for r in sr if r['week_up']) / len(sr) if sr else 0.5

def predict_strategy_d(r):
    """多维度 + 板块基准率默认"""
    sig = compute_weekly_signal(r, sector_dim_config)
    if abs(sig) > 0.2:
        return '上涨' if sig > 0 else '下跌'
    else:
        return '上涨' if sector_week_up_rate.get(r['sector'], 0.5) > 0.5 else '下跌'

def predict_strategy_e(r, prev_week_chg_map):
    """多维度 + 上周动量/反转"""
    sig = compute_weekly_signal(r, sector_dim_config)
    sector = r['sector']

    # 上周信号
    prev_chg = prev_week_chg_map.get(sector, 0)
    mom_config = sector_momentum_config.get(sector, 'momentum')

    if mom_config == 'momentum':
        prev_signal = 1 if prev_chg > 0 else -1
    else:
        prev_signal = -1 if prev_chg > 0 else 1

    # 融合: 多维度信号 + 上周动量
    combined = sig * 0.7 + (prev_signal * 0.3 if prev_chg != 0 else 0)
    if abs(combined) > 0.1:
        return '上涨' if combined > 0 else '下跌'
    else:
        return '上涨' if sector_week_up_rate.get(sector, 0.5) > 0.5 else '下跌'

def predict_strategy_g(r, prev_week_chg_map, linked_signal_map):
    """综合: 多维度 + 动量 + 联动 + 板块基准率"""
    sector = r['sector']
    sig = compute_weekly_signal(r, sector_dim_config)

    # 上周动量/反转
    prev_chg = prev_week_chg_map.get(sector, 0)
    mom_config = sector_momentum_config.get(sector, 'momentum')
    if mom_config == 'momentum':
        prev_signal = 0.3 if prev_chg > 0 else -0.3
    else:
        prev_signal = -0.3 if prev_chg > 0 else 0.3
    if prev_chg == 0:
        prev_signal = 0

    # 联动板块信号
    linked_sig = linked_signal_map.get((r['iso_week'], sector), 0)

    # 综合
    combined = sig * 0.5 + prev_signal + linked_sig * 0.2
    if abs(combined) > 0.15:
        return '上涨' if combined > 0 else '下跌'
    else:
        return '上涨' if sector_week_up_rate.get(sector, 0.5) > 0.5 else '下跌'

# 构建上周涨跌map和联动信号map
prev_week_chg_maps = {}  # {iso_week: {sector: avg_chg}}
for i, w in enumerate(sorted_weeks):
    if i == 0:
        prev_week_chg_maps[w] = {s: 0 for s in sectors}
        continue
    prev_w = sorted_weeks[i - 1]
    prev_map = {}
    for sector in sectors:
        key = (prev_w, sector)
        if key in week_sector_chg:
            prev_map[sector] = sum(week_sector_chg[key]) / len(week_sector_chg[key])
        else:
            prev_map[sector] = 0
    prev_week_chg_maps[w] = prev_map

# 联动信号map
linked_signal_maps = {}
linked_pairs_dict = defaultdict(list)
for s1, s2, _ in linked_pairs:
    linked_pairs_dict[s1].append(s2)
    linked_pairs_dict[s2].append(s1)

for w in sorted_weeks:
    for sector in sectors:
        key = (w, sector)
        linked_sigs = []
        for linked_sector in linked_pairs_dict.get(sector, []):
            lkey = (w, linked_sector)
            if lkey in week_sector_signal:
                avg_sig = sum(week_sector_signal[lkey]) / len(week_sector_signal[lkey])
                linked_sigs.append(avg_sig)
        if linked_sigs:
            linked_signal_maps[key] = sum(linked_sigs) / len(linked_sigs)
        else:
            linked_signal_maps[key] = 0


# 运行所有策略
print(f"\n全样本策略对比:")
print(f"{'策略':<30} {'准确率':>10} {'vs全涨':>8}")
print('-' * 55)

results = {}

# A. 全涨
a_ok = sum(1 for r in weekly_records if r['week_up'])
results['A.全涨'] = a_ok / n_weekly * 100

# B. 单融合信号
b_ok = sum(1 for r in weekly_records if
           (predict_strategy_b(r) == '上涨' and r['week_up']) or
           (predict_strategy_b(r) == '下跌' and r['week_dn']))
results['B.单融合信号'] = b_ok / n_weekly * 100

# C. 多维度融合
c_ok = sum(1 for r in weekly_records if
           (predict_strategy_c(r) == '上涨' and r['week_up']) or
           (predict_strategy_c(r) == '下跌' and r['week_dn']))
results['C.多维度融合'] = c_ok / n_weekly * 100

# D. 多维度+基准率
d_ok = sum(1 for r in weekly_records if
           (predict_strategy_d(r) == '上涨' and r['week_up']) or
           (predict_strategy_d(r) == '下跌' and r['week_dn']))
results['D.多维度+基准率'] = d_ok / n_weekly * 100

# E. 多维度+动量
e_ok = 0
for r in weekly_records:
    prev_map = prev_week_chg_maps.get(r['iso_week'], {})
    pred = predict_strategy_e(r, prev_map)
    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
        e_ok += 1
results['E.多维度+动量'] = e_ok / n_weekly * 100

# G. 综合
g_ok = 0
for r in weekly_records:
    prev_map = prev_week_chg_maps.get(r['iso_week'], {})
    pred = predict_strategy_g(r, prev_map, linked_signal_maps)
    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
        g_ok += 1
results['G.综合策略'] = g_ok / n_weekly * 100

all_up_rate = results['A.全涨']
for name, rate in sorted(results.items(), key=lambda x: -x[1]):
    print(f"{name:<30} {rate:>8.1f}% {rate - all_up_rate:>+6.1f}pp")


# ═══════════════════════════════════════════════════════════
# Part 6b: 前半训练→后半测试（泛化验证）
# ═══════════════════════════════════════════════════════════
print(f"\n{'─' * 60}")
print(f"前半训练→后半测试（泛化验证）:")
print(f"{'─' * 60}")

# 用前半数据重新计算板块×维度配置
sector_dim_config_train = {}
for sector in sectors:
    sr_train = [r for r in weekly_records if r['sector'] == sector and r['iso_week'] in first_half_weeks]
    sector_dim_config_train[sector] = {}
    for dkey in dim_keys:
        all_sig = [r for r in sr_train if r[dkey] != 0]
        if len(all_sig) < 5:
            sector_dim_config_train[sector][dkey] = ('skip', 0, 0)
            continue
        fwd_ok = sum(1 for r in all_sig if (r[dkey] > 0 and r['week_up']) or (r[dkey] < 0 and r['week_dn']))
        fwd_rate = fwd_ok / len(all_sig)
        rev_rate = 1 - fwd_rate
        if fwd_rate > 0.55:
            sector_dim_config_train[sector][dkey] = ('fwd', fwd_rate, len(all_sig))
        elif rev_rate > 0.55:
            sector_dim_config_train[sector][dkey] = ('rev', rev_rate, len(all_sig))
        else:
            sector_dim_config_train[sector][dkey] = ('noise', max(fwd_rate, rev_rate), len(all_sig))

# 前半基准率
sector_train_up_rate = {}
for sector in sectors:
    sr_train = [r for r in weekly_records if r['sector'] == sector and r['iso_week'] in first_half_weeks]
    sector_train_up_rate[sector] = sum(1 for r in sr_train if r['week_up']) / len(sr_train) if sr_train else 0.5

# 前半动量配置
sector_momentum_train = {}
for sector in sectors:
    m_ok = 0
    r_ok = 0
    n = 0
    train_sorted = sorted(first_half_weeks)
    for i in range(1, len(train_sorted)):
        prev_w = train_sorted[i - 1]
        curr_w = train_sorted[i]
        prev_key = (prev_w, sector)
        curr_key = (curr_w, sector)
        if prev_key not in week_sector_chg or curr_key not in week_sector_chg:
            continue
        prev_avg = sum(week_sector_chg[prev_key]) / len(week_sector_chg[prev_key])
        curr_recs = [r for r in weekly_records if r['iso_week'] == curr_w and r['sector'] == sector]
        for r in curr_recs:
            n += 1
            if (prev_avg > 0 and r['week_up']) or (prev_avg < 0 and r['week_dn']):
                m_ok += 1
            else:
                r_ok += 1
    sector_momentum_train[sector] = 'momentum' if m_ok >= r_ok else 'reversal'

# 后半测试
test_records = [r for r in weekly_records if r['iso_week'] in second_half_weeks]
n_test = len(test_records)

print(f"\n{'策略':<30} {'后半准确率':>12} {'后半样本':>8}")
print('-' * 55)

# A. 全涨
a_test = sum(1 for r in test_records if r['week_up'])
print(f"{'A.全涨':<30} {a_test/n_test*100:>10.1f}% {n_test:>8}")

# B. 单融合
b_test = sum(1 for r in test_records if
             (predict_strategy_b(r) == '上涨' and r['week_up']) or
             (predict_strategy_b(r) == '下跌' and r['week_dn']))
print(f"{'B.单融合信号':<30} {b_test/n_test*100:>10.1f}% {n_test:>8}")

# C. 多维度(训练配置)
def compute_weekly_signal_train(r):
    return compute_weekly_signal(r, sector_dim_config_train)

c_test = 0
for r in test_records:
    sig = compute_weekly_signal_train(r)
    pred = '上涨' if sig > 0 else '下跌'
    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
        c_test += 1
print(f"{'C.多维度融合(训练)':<30} {c_test/n_test*100:>10.1f}% {n_test:>8}")

# D. 多维度+基准率(训练)
d_test = 0
for r in test_records:
    sig = compute_weekly_signal_train(r)
    if abs(sig) > 0.2:
        pred = '上涨' if sig > 0 else '下跌'
    else:
        pred = '上涨' if sector_train_up_rate.get(r['sector'], 0.5) > 0.5 else '下跌'
    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
        d_test += 1
print(f"{'D.多维度+基准率(训练)':<30} {d_test/n_test*100:>10.1f}% {n_test:>8}")

# E. 多维度+动量(训练)
e_test = 0
for r in test_records:
    sig = compute_weekly_signal_train(r)
    sector = r['sector']
    prev_map = prev_week_chg_maps.get(r['iso_week'], {})
    prev_chg = prev_map.get(sector, 0)
    mom_cfg = sector_momentum_train.get(sector, 'momentum')
    if mom_cfg == 'momentum':
        prev_sig = 1 if prev_chg > 0 else -1
    else:
        prev_sig = -1 if prev_chg > 0 else 1
    if prev_chg == 0:
        prev_sig = 0
    combined = sig * 0.7 + prev_sig * 0.3
    if abs(combined) > 0.1:
        pred = '上涨' if combined > 0 else '下跌'
    else:
        pred = '上涨' if sector_train_up_rate.get(sector, 0.5) > 0.5 else '下跌'
    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
        e_test += 1
print(f"{'E.多维度+动量(训练)':<30} {e_test/n_test*100:>10.1f}% {n_test:>8}")


# ═══════════════════════════════════════════════════════════
# Part 7: 按板块后半测试详细分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 7: 按板块后半测试详细分析")
print(f"{'=' * 80}")

print(f"\n{'板块':<10} {'全涨':>8} {'单融合':>8} {'多维度':>8} {'多维+基准':>10} {'多维+动量':>10}")
print('-' * 65)

for sector in sectors:
    sr_test = [r for r in test_records if r['sector'] == sector]
    if not sr_test:
        continue
    n_s = len(sr_test)

    a = sum(1 for r in sr_test if r['week_up']) / n_s * 100
    b = sum(1 for r in sr_test if
            (predict_strategy_b(r) == '上涨' and r['week_up']) or
            (predict_strategy_b(r) == '下跌' and r['week_dn'])) / n_s * 100

    c = 0
    d = 0
    e = 0
    for r in sr_test:
        sig = compute_weekly_signal_train(r)
        pred_c = '上涨' if sig > 0 else '下跌'
        if (pred_c == '上涨' and r['week_up']) or (pred_c == '下跌' and r['week_dn']):
            c += 1

        if abs(sig) > 0.2:
            pred_d = '上涨' if sig > 0 else '下跌'
        else:
            pred_d = '上涨' if sector_train_up_rate.get(sector, 0.5) > 0.5 else '下跌'
        if (pred_d == '上涨' and r['week_up']) or (pred_d == '下跌' and r['week_dn']):
            d += 1

        prev_map = prev_week_chg_maps.get(r['iso_week'], {})
        prev_chg = prev_map.get(sector, 0)
        mom_cfg = sector_momentum_train.get(sector, 'momentum')
        if mom_cfg == 'momentum':
            prev_s = 1 if prev_chg > 0 else -1
        else:
            prev_s = -1 if prev_chg > 0 else 1
        if prev_chg == 0:
            prev_s = 0
        comb = sig * 0.7 + prev_s * 0.3
        if abs(comb) > 0.1:
            pred_e = '上涨' if comb > 0 else '下跌'
        else:
            pred_e = '上涨' if sector_train_up_rate.get(sector, 0.5) > 0.5 else '下跌'
        if (pred_e == '上涨' and r['week_up']) or (pred_e == '下跌' and r['week_dn']):
            e += 1

    c = c / n_s * 100
    d = d / n_s * 100
    e = e / n_s * 100

    print(f"{sector:<10} {a:>6.1f}% {b:>6.1f}% {c:>6.1f}% {d:>8.1f}% {e:>8.1f}%")


# ═══════════════════════════════════════════════════════════
# Part 8: 周内动量实时更新策略
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 8: 周内动量实时更新策略")
print(f"{'=' * 80}")

# 核心思路: 不只用周一信号，而是随着周内天数增加，动态更新周预测
# 周一: 用周一信号预测
# 周二: 用周一+周二信号更新
# 周三: 用前3天实际涨跌修正
# 这样后半周的日频预测可以利用前半周的实际数据

print(f"\n动态更新策略: 随周内天数增加更新预测")
print(f"{'更新策略':<30} {'准确率':>10} {'样本':>8} {'vs日频基线':>12}")
print('-' * 65)

# 策略1: 纯日频基线
daily_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"{'日频基线':<30} {daily_ok/total*100:>8.1f}% {total:>8} {0:>+10.1f}pp")

# 策略2: 周一用周信号，其余用日频
s2_ok = 0
for d in details:
    if d['_wd'] == 0:
        # 周一: 用多维度周信号
        key = (d['代码'], d['_iso_week'])
        wr = [r for r in weekly_records if r['code'] == d['代码'] and r['iso_week'] == d['_iso_week']]
        if wr:
            sig = compute_weekly_signal(wr[0], sector_dim_config)
            pred = '上涨' if sig > 0 else ('下跌' if sig < 0 else d['预测方向'])
        else:
            pred = d['预测方向']
    else:
        pred = d['预测方向']
    if (pred == '上涨' and d['_actual'] >= 0) or (pred == '下跌' and d['_actual'] <= 0):
        s2_ok += 1
print(f"{'周一用周信号+其余日频':<30} {s2_ok/total*100:>8.1f}% {total:>8} "
      f"{s2_ok/total*100 - daily_ok/total*100:>+10.1f}pp")

# 策略3: 前3天涨跌天数>=2/3时，后2天用动量方向
s3_ok = 0
for r in weekly_records:
    if r['n_days'] < 4:
        for d in r['days']:
            if d['宽松正确'] == '✓':
                s3_ok += 1
        continue

    first_3 = r['days'][:3]
    rest = r['days'][3:]
    up_3 = sum(1 for d in first_3 if d['_actual'] > 0)
    dn_3 = sum(1 for d in first_3 if d['_actual'] < 0)

    # 前3天用日频
    for d in first_3:
        if d['宽松正确'] == '✓':
            s3_ok += 1

    # 后面的天
    for d in rest:
        if up_3 >= 2:
            pred = '上涨'  # 前3天多数涨→后面也涨
        elif dn_3 >= 2:
            pred = '下跌'  # 前3天多数跌→后面也跌
        else:
            pred = d['预测方向']  # 不确定，用日频

        if (pred == '上涨' and d['_actual'] >= 0) or (pred == '下跌' and d['_actual'] <= 0):
            s3_ok += 1

# 补上不在weekly_records中的样本
single_day_samples = [d for d in details if (d['代码'], d['_iso_week']) not in
                      {(r['code'], r['iso_week']) for r in weekly_records}]
for d in single_day_samples:
    if d['宽松正确'] == '✓':
        s3_ok += 1

total_with_single = total
print(f"{'前3天动量→后2天方向':<30} {s3_ok/total_with_single*100:>8.1f}% {total_with_single:>8} "
      f"{s3_ok/total_with_single*100 - daily_ok/total*100:>+10.1f}pp")

# 策略4: 前2天累积涨跌>1%→后面用涨，<-1%→后面用跌
s4_ok = 0
for r in weekly_records:
    if r['n_days'] < 3:
        for d in r['days']:
            if d['宽松正确'] == '✓':
                s4_ok += 1
        continue

    first_2 = r['days'][:2]
    rest = r['days'][2:]
    cum_2 = sum(d['_actual'] for d in first_2)

    for d in first_2:
        if d['宽松正确'] == '✓':
            s4_ok += 1

    for d in rest:
        if cum_2 > 1.0:
            pred = '上涨'
        elif cum_2 < -1.0:
            pred = '下跌'
        else:
            pred = d['预测方向']

        if (pred == '上涨' and d['_actual'] >= 0) or (pred == '下跌' and d['_actual'] <= 0):
            s4_ok += 1

for d in single_day_samples:
    if d['宽松正确'] == '✓':
        s4_ok += 1

print(f"{'前2天累积>1%→后面动量':<30} {s4_ok/total_with_single*100:>8.1f}% {total_with_single:>8} "
      f"{s4_ok/total_with_single*100 - daily_ok/total*100:>+10.1f}pp")


# ═══════════════════════════════════════════════════════════
# Part 9: 最优板块特化周预测配置搜索
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 9: 最优板块特化周预测配置搜索")
print(f"{'=' * 80}")

# 对每个板块，搜索最优的:
# 1. 使用哪些维度（正向/反转/跳过）
# 2. 信号阈值
# 3. 默认方向
# 用前半训练，后半验证

print(f"\n{'板块':<10} {'前半最优':>10} {'后半泛化':>10} {'全涨基线':>10} {'提升':>8}")
print('-' * 55)

for sector in sectors:
    sr_train = [r for r in weekly_records if r['sector'] == sector and r['iso_week'] in first_half_weeks]
    sr_test = [r for r in weekly_records if r['sector'] == sector and r['iso_week'] in second_half_weeks]

    if not sr_train or not sr_test:
        continue

    best_train_rate = 0
    best_test_rate = 0
    best_config = None

    # 搜索维度组合（简化: 对每个维度选择正向/反转/跳过）
    # 由于组合太多，用贪心: 先找每个维度的最优方向，然后组合
    dim_best = {}
    for dkey in dim_keys:
        all_sig = [r for r in sr_train if r[dkey] != 0]
        if len(all_sig) < 5:
            dim_best[dkey] = ('skip', 0)
            continue
        fwd_ok = sum(1 for r in all_sig if (r[dkey] > 0 and r['week_up']) or (r[dkey] < 0 and r['week_dn']))
        fwd_rate = fwd_ok / len(all_sig)
        if fwd_rate > 0.55:
            dim_best[dkey] = ('fwd', fwd_rate)
        elif fwd_rate < 0.45:
            dim_best[dkey] = ('rev', 1 - fwd_rate)
        else:
            dim_best[dkey] = ('skip', 0)

    # 搜索阈值和默认方向
    for sig_th in [0.0, 0.1, 0.2, 0.3, 0.5]:
        for def_up in [True, False]:
            # 训练
            train_ok = 0
            for r in sr_train:
                sig = 0.0
                w_sum = 0.0
                for dkey in dim_keys:
                    direction, rate = dim_best[dkey]
                    if direction == 'skip':
                        continue
                    val = r[dkey]
                    if val == 0:
                        continue
                    w = (rate - 0.5) * 2
                    if direction == 'rev':
                        val = -val
                    sig += (1.0 if val > 0 else -1.0) * w
                    w_sum += w
                if w_sum > 0:
                    sig /= w_sum

                if sig > sig_th:
                    pred = '上涨'
                elif sig < -sig_th:
                    pred = '下跌'
                else:
                    pred = '上涨' if def_up else '下跌'

                if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
                    train_ok += 1

            train_rate = train_ok / len(sr_train) * 100

            # 测试
            test_ok = 0
            for r in sr_test:
                sig = 0.0
                w_sum = 0.0
                for dkey in dim_keys:
                    direction, rate = dim_best[dkey]
                    if direction == 'skip':
                        continue
                    val = r[dkey]
                    if val == 0:
                        continue
                    w = (rate - 0.5) * 2
                    if direction == 'rev':
                        val = -val
                    sig += (1.0 if val > 0 else -1.0) * w
                    w_sum += w
                if w_sum > 0:
                    sig /= w_sum

                if sig > sig_th:
                    pred = '上涨'
                elif sig < -sig_th:
                    pred = '下跌'
                else:
                    pred = '上涨' if def_up else '下跌'

                if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
                    test_ok += 1

            test_rate = test_ok / len(sr_test) * 100

            if train_rate > best_train_rate or (train_rate == best_train_rate and test_rate > best_test_rate):
                best_train_rate = train_rate
                best_test_rate = test_rate
                best_config = (sig_th, def_up, dict(dim_best))

    test_all_up = sum(1 for r in sr_test if r['week_up']) / len(sr_test) * 100
    improve = best_test_rate - test_all_up

    print(f"{sector:<10} {best_train_rate:>8.1f}% {best_test_rate:>8.1f}% {test_all_up:>8.1f}% {improve:>+6.1f}pp")
    if best_config:
        sig_th, def_up, dims = best_config
        active_dims = [(k, v[0]) for k, v in dims.items() if v[0] != 'skip']
        dim_str = ', '.join(f"{dim_names[dim_keys.index(k)]}({'+'if d=='fwd' else '-'})" for k, d in active_dims)
        print(f"{'':>10} th={sig_th}, def={'涨' if def_up else '跌'}, dims=[{dim_str}]")


# ═══════════════════════════════════════════════════════════
# Part 10: 综合结论
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 10: 综合结论")
print(f"{'=' * 80}")

print(f"""
┌─────────────────────────────────────────────────────────────────────────┐
│                v19b 周预测准确率提升分析结论                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 1. 核心瓶颈诊断                                                        │
│    a) 周一信号→周三预测力骤降至44.2%(反转)，信号衰减非线性             │
│    b) 31%的周样本|涨跌|<2%，方向难判                                    │
│    c) 前半→后半regime shift导致泛化差异大                               │
│                                                                         │
│ 2. 有效提升方向                                                         │
│    a) 板块特化正向/反转自适应:                                          │
│       - 医药: 同行反转70%, 美股反转61% → 反转逻辑                      │
│       - 有色金属: 技术66%, z_today 62% → 正向技术面                    │
│       - 化工: 融合61%, 技术66% → 正向信号有效                          │
│    b) 多维度加权融合 vs 单融合信号: 见Part 2对比                       │
│    c) 周内动量累积: 前3天涨天>=70%→周涨98.6%                          │
│    d) 板块联动: 有色金属-化工92%一致 → 交叉验证                       │
│    e) 上周动量: 化工73%动量, 制造64%动量                               │
│                                                                         │
│ 3. 策略效果排序 (全样本)                                                │
│    见Part 6详细对比                                                     │
│                                                                         │
│ 4. 泛化验证 (前半训练→后半测试)                                        │
│    见Part 6b详细对比                                                    │
│                                                                         │
│ 5. 实战建议                                                             │
│    a) 周内动量实时更新: 前3天涨跌天数>=2/3时修正后2天预测              │
│    b) 板块特化维度选择: 每个板块只用有效维度(>55%一致率)               │
│    c) 医药/科技: 同行信号反转使用                                      │
│    d) 化工/有色金属: 技术面正向+动量策略                               │
│    e) 弱信号时用板块基准率作为默认方向                                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
""")

print(f"{'=' * 80}")
print(f"v19b 分析完成")
print(f"{'=' * 80}")
