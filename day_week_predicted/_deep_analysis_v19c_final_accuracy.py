#!/usr/bin/env python3
"""
v19c 周预测最终准确率汇总

将v19/v19b的所有策略统一计算，给出最终的周预测准确率数字。
包含：全样本、前半训练→后半测试、滚动验证三种评估方式。
"""
import json
from collections import defaultdict
from datetime import datetime

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    bt_data = json.load(f)
details = bt_data['逐日详情']
total = len(details)

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

for d in details:
    d['_actual'] = parse_chg(d['实际涨跌'])
    d['_dt'] = datetime.strptime(d['评分日'], '%Y-%m-%d')
    d['_wd'] = d['_dt'].weekday()
    d['_iso_week'] = d['_dt'].isocalendar()[:2]

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')

# ── 构建周数据 ──
stock_week = defaultdict(list)
for d in details:
    stock_week[(d['代码'], d['_iso_week'])].append(d)

weekly = []
for (code, iw), days in stock_week.items():
    days.sort(key=lambda x: x['评分日'])
    if len(days) < 2:
        continue
    cum = 1.0
    for d in days:
        cum *= (1 + d['_actual'] / 100)
    wchg = (cum - 1) * 100
    weekly.append({
        'code': code, 'sector': days[0]['板块'], 'iw': iw,
        'n': len(days), 'wchg': wchg, 'wup': wchg >= 0, 'wdn': wchg <= 0,
        'mon_comb': days[0]['融合信号'], 'mon_tech': days[0]['技术信号'],
        'mon_peer': days[0]['同行信号'], 'mon_rs': days[0]['RS信号'],
        'mon_us': days[0]['美股隔夜'], 'mon_z': days[0]['z_today'],
        'mon_vol': days[0]['波动率状态'], 'mon_score': days[0]['评分'],
        'mon_conf': days[0]['置信度'], 'mon_pred': days[0]['预测方向'],
        'days': days,
    })

nw = len(weekly)
sorted_weeks = sorted(set(r['iw'] for r in weekly))
mid = len(sorted_weeks) // 2
first_half = set(sorted_weeks[:mid])
second_half = set(sorted_weeks[mid:])

dim_keys = ['mon_comb', 'mon_tech', 'mon_peer', 'mon_rs', 'mon_us', 'mon_z', 'mon_vol']
dim_names = ['融合', '技术', '同行', 'RS', '美股', 'z_today', '波动率']

# ── 工具函数 ──
def learn_dim_config(records):
    """从训练数据学习板块×维度配置"""
    cfg = {}
    for sector in sectors:
        sr = [r for r in records if r['sector'] == sector]
        cfg[sector] = {}
        for dk in dim_keys:
            sigs = [r for r in sr if r[dk] != 0]
            if len(sigs) < 5:
                cfg[sector][dk] = ('skip', 0)
                continue
            fwd = sum(1 for r in sigs if (r[dk] > 0 and r['wup']) or (r[dk] < 0 and r['wdn']))
            rate = fwd / len(sigs)
            if rate > 0.55:
                cfg[sector][dk] = ('fwd', rate)
            elif rate < 0.45:
                cfg[sector][dk] = ('rev', 1 - rate)
            else:
                cfg[sector][dk] = ('skip', 0)
    return cfg

def learn_base_rate(records):
    """学习板块周涨跌基准率"""
    rates = {}
    for sector in sectors:
        sr = [r for r in records if r['sector'] == sector]
        rates[sector] = sum(1 for r in sr if r['wup']) / len(sr) if sr else 0.5
    return rates

def multi_dim_signal(r, dim_cfg):
    """计算多维度融合信号"""
    cfg = dim_cfg.get(r['sector'], {})
    sig = 0.0
    ws = 0.0
    for dk in dim_keys:
        d, rate = cfg.get(dk, ('skip', 0))
        if d == 'skip':
            continue
        v = r[dk]
        if v == 0:
            continue
        w = (rate - 0.5) * 2
        if d == 'rev':
            v = -v
        sig += (1.0 if v > 0 else -1.0) * w
        ws += w
    return sig / ws if ws > 0 else 0.0

def predict_multi_base(r, dim_cfg, base_rates, th=0.2):
    """多维度+基准率策略"""
    sig = multi_dim_signal(r, dim_cfg)
    if sig > th:
        return '上涨'
    elif sig < -th:
        return '下跌'
    return '上涨' if base_rates.get(r['sector'], 0.5) > 0.5 else '下跌'

def learn_sector_optimal(records):
    """学习每个板块的最优配置(维度+阈值+默认方向)"""
    configs = {}
    for sector in sectors:
        sr = [r for r in records if r['sector'] == sector]
        if len(sr) < 5:
            configs[sector] = {'dims': {}, 'th': 0.0, 'def_up': True}
            continue
        # 学习维度
        dims = {}
        for dk in dim_keys:
            sigs = [r for r in sr if r[dk] != 0]
            if len(sigs) < 5:
                dims[dk] = ('skip', 0)
                continue
            fwd = sum(1 for r in sigs if (r[dk] > 0 and r['wup']) or (r[dk] < 0 and r['wdn']))
            rate = fwd / len(sigs)
            if rate > 0.55:
                dims[dk] = ('fwd', rate)
            elif rate < 0.45:
                dims[dk] = ('rev', 1 - rate)
            else:
                dims[dk] = ('skip', 0)
        # 搜索阈值
        best_rate = 0
        best_th = 0.0
        best_def = True
        for th in [0.0, 0.1, 0.2, 0.3, 0.5]:
            for def_up in [True, False]:
                ok = 0
                for r in sr:
                    sig = 0.0
                    ws = 0.0
                    for dk in dim_keys:
                        d, rate = dims.get(dk, ('skip', 0))
                        if d == 'skip':
                            continue
                        v = r[dk]
                        if v == 0:
                            continue
                        w = (rate - 0.5) * 2
                        if d == 'rev':
                            v = -v
                        sig += (1.0 if v > 0 else -1.0) * w
                        ws += w
                    if ws > 0:
                        sig /= ws
                    if sig > th:
                        pred = '上涨'
                    elif sig < -th:
                        pred = '下跌'
                    else:
                        pred = '上涨' if def_up else '下跌'
                    if (pred == '上涨' and r['wup']) or (pred == '下跌' and r['wdn']):
                        ok += 1
                rate = ok / len(sr) * 100
                if rate > best_rate:
                    best_rate = rate
                    best_th = th
                    best_def = def_up
        configs[sector] = {'dims': dims, 'th': best_th, 'def_up': best_def}
    return configs

def predict_sector_optimal(r, sector_configs):
    """板块最优配置预测"""
    cfg = sector_configs.get(r['sector'], {'dims': {}, 'th': 0.0, 'def_up': True})
    dims = cfg['dims']
    th = cfg['th']
    def_up = cfg['def_up']
    sig = 0.0
    ws = 0.0
    for dk in dim_keys:
        d, rate = dims.get(dk, ('skip', 0))
        if d == 'skip':
            continue
        v = r[dk]
        if v == 0:
            continue
        w = (rate - 0.5) * 2
        if d == 'rev':
            v = -v
        sig += (1.0 if v > 0 else -1.0) * w
        ws += w
    if ws > 0:
        sig /= ws
    if sig > th:
        return '上涨'
    elif sig < -th:
        return '下跌'
    return '上涨' if def_up else '下跌'

def check_ok(pred, r):
    return (pred == '上涨' and r['wup']) or (pred == '下跌' and r['wdn'])


# ═══════════════════════════════════════════════════════════════
# 主评估逻辑
# ═══════════════════════════════════════════════════════════════

print("=" * 70)
print("  v19c 周预测最终准确率汇总报告")
print("=" * 70)
print(f"\n日频基线: {loose_ok}/{total} = {loose_ok/total*100:.1f}%")
print(f"周样本数: {nw} (50只股票 × {len(sorted_weeks)}周)")
print(f"周列表: {sorted_weeks}")
print(f"前半(训练): {sorted(first_half)} ({sum(1 for r in weekly if r['iw'] in first_half)}样本)")
print(f"后半(测试): {sorted(second_half)} ({sum(1 for r in weekly if r['iw'] in second_half)}样本)")

# ── 1. 全样本评估 ──
print("\n" + "=" * 70)
print("  Part 1: 全样本评估 (所有策略在全部596周样本上)")
print("=" * 70)

# 全样本学习配置
dim_cfg_all = learn_dim_config(weekly)
base_rates_all = learn_base_rate(weekly)
sector_opt_all = learn_sector_optimal(weekly)

strategies_full = {}

# 策略1: 全涨
ok = sum(1 for r in weekly if r['wup'])
strategies_full['全涨'] = (ok, nw)

# 策略2: 全跌
ok = sum(1 for r in weekly if r['wdn'])
strategies_full['全跌'] = (ok, nw)

# 策略3: 单融合信号 (周一融合信号直接用)
ok = sum(1 for r in weekly if
         (r['mon_comb'] > 0 and r['wup']) or
         (r['mon_comb'] < 0 and r['wdn']) or
         (r['mon_comb'] == 0 and r['wup']))
strategies_full['单融合信号'] = (ok, nw)

# 策略4: 多维度融合
ok = 0
for r in weekly:
    sig = multi_dim_signal(r, dim_cfg_all)
    pred = '上涨' if sig >= 0 else '下跌'
    if check_ok(pred, r):
        ok += 1
strategies_full['多维度融合'] = (ok, nw)

# 策略5: 多维度+基准率
ok = 0
for r in weekly:
    pred = predict_multi_base(r, dim_cfg_all, base_rates_all)
    if check_ok(pred, r):
        ok += 1
strategies_full['多维度+基准率'] = (ok, nw)

# 策略6: 板块最优配置
ok = 0
for r in weekly:
    pred = predict_sector_optimal(r, sector_opt_all)
    if check_ok(pred, r):
        ok += 1
strategies_full['板块最优配置'] = (ok, nw)

print(f"\n{'策略':<16} {'正确':>6} {'总数':>6} {'准确率':>8}")
print("-" * 40)
for name, (ok, tot) in strategies_full.items():
    print(f"{name:<16} {ok:>6} {tot:>6} {ok/tot*100:>7.1f}%")


# ── 2. 前半训练→后半测试 ──
print("\n" + "=" * 70)
print("  Part 2: 前半训练→后半测试 (泛化能力)")
print("=" * 70)

train_set = [r for r in weekly if r['iw'] in first_half]
test_set = [r for r in weekly if r['iw'] in second_half]
print(f"训练集: {len(train_set)}样本, 测试集: {len(test_set)}样本")

dim_cfg_train = learn_dim_config(train_set)
base_rates_train = learn_base_rate(train_set)
sector_opt_train = learn_sector_optimal(train_set)

strategies_test = {}

# 全涨
ok = sum(1 for r in test_set if r['wup'])
strategies_test['全涨'] = (ok, len(test_set))

# 全跌
ok = sum(1 for r in test_set if r['wdn'])
strategies_test['全跌'] = (ok, len(test_set))

# 单融合信号
ok = sum(1 for r in test_set if
         (r['mon_comb'] > 0 and r['wup']) or
         (r['mon_comb'] < 0 and r['wdn']) or
         (r['mon_comb'] == 0 and r['wup']))
strategies_test['单融合信号'] = (ok, len(test_set))

# 多维度融合 (用训练集配置)
ok = 0
for r in test_set:
    sig = multi_dim_signal(r, dim_cfg_train)
    pred = '上涨' if sig >= 0 else '下跌'
    if check_ok(pred, r):
        ok += 1
strategies_test['多维度融合'] = (ok, len(test_set))

# 多维度+基准率
ok = 0
for r in test_set:
    pred = predict_multi_base(r, dim_cfg_train, base_rates_train)
    if check_ok(pred, r):
        ok += 1
strategies_test['多维度+基准率'] = (ok, len(test_set))

# 板块最优配置
ok = 0
for r in test_set:
    pred = predict_sector_optimal(r, sector_opt_train)
    if check_ok(pred, r):
        ok += 1
strategies_test['板块最优配置'] = (ok, len(test_set))

print(f"\n{'策略':<16} {'正确':>6} {'总数':>6} {'准确率':>8}")
print("-" * 40)
for name, (ok, tot) in strategies_test.items():
    print(f"{name:<16} {ok:>6} {tot:>6} {ok/tot*100:>7.1f}%")


# ── 3. 滚动验证 ──
print("\n" + "=" * 70)
print("  Part 3: 滚动验证 (前N周训练→第N+1周预测)")
print("=" * 70)

rolling_results = defaultdict(lambda: [0, 0])  # strategy -> [ok, total]

for i in range(2, len(sorted_weeks)):  # 至少2周训练
    train_weeks = set(sorted_weeks[:i])
    pred_week = sorted_weeks[i]
    r_train = [r for r in weekly if r['iw'] in train_weeks]
    r_pred = [r for r in weekly if r['iw'] == pred_week]
    if not r_pred:
        continue

    dc = learn_dim_config(r_train)
    br = learn_base_rate(r_train)
    so = learn_sector_optimal(r_train)

    for r in r_pred:
        rolling_results['全涨'][1] += 1
        rolling_results['全跌'][1] += 1
        rolling_results['单融合信号'][1] += 1
        rolling_results['多维度融合'][1] += 1
        rolling_results['多维度+基准率'][1] += 1
        rolling_results['板块最优配置'][1] += 1

        if r['wup']:
            rolling_results['全涨'][0] += 1
        if r['wdn']:
            rolling_results['全跌'][0] += 1
        if (r['mon_comb'] > 0 and r['wup']) or (r['mon_comb'] < 0 and r['wdn']) or (r['mon_comb'] == 0 and r['wup']):
            rolling_results['单融合信号'][0] += 1

        sig = multi_dim_signal(r, dc)
        pred = '上涨' if sig >= 0 else '下跌'
        if check_ok(pred, r):
            rolling_results['多维度融合'][0] += 1

        pred = predict_multi_base(r, dc, br)
        if check_ok(pred, r):
            rolling_results['多维度+基准率'][0] += 1

        pred = predict_sector_optimal(r, so)
        if check_ok(pred, r):
            rolling_results['板块最优配置'][0] += 1

print(f"\n滚动窗口: 第3周起预测 (前2周训练), 共{rolling_results['全涨'][1]}样本")
print(f"\n{'策略':<16} {'正确':>6} {'总数':>6} {'准确率':>8}")
print("-" * 40)
for name in ['全涨', '全跌', '单融合信号', '多维度融合', '多维度+基准率', '板块最优配置']:
    ok, tot = rolling_results[name]
    print(f"{name:<16} {ok:>6} {tot:>6} {ok/tot*100:>7.1f}%")


# ── 4. 按板块明细 ──
print("\n" + "=" * 70)
print("  Part 4: 按板块明细 (全样本 + 前半训练→后半测试)")
print("=" * 70)

for eval_name, eval_set, dcfg, brates, sopt in [
    ("全样本", weekly, dim_cfg_all, base_rates_all, sector_opt_all),
    ("后半测试", test_set, dim_cfg_train, base_rates_train, sector_opt_train),
]:
    print(f"\n── {eval_name} ──")
    print(f"{'板块':<8} {'样本':>4} {'全涨':>6} {'融合':>6} {'多维度':>6} {'多维+基准':>8} {'板块最优':>8}")
    print("-" * 56)
    for sector in sectors:
        sr = [r for r in eval_set if r['sector'] == sector]
        if not sr:
            continue
        n = len(sr)
        # 全涨
        a_up = sum(1 for r in sr if r['wup'])
        # 单融合
        a_comb = sum(1 for r in sr if
                     (r['mon_comb'] > 0 and r['wup']) or
                     (r['mon_comb'] < 0 and r['wdn']) or
                     (r['mon_comb'] == 0 and r['wup']))
        # 多维度融合
        a_multi = 0
        for r in sr:
            sig = multi_dim_signal(r, dcfg)
            pred = '上涨' if sig >= 0 else '下跌'
            if check_ok(pred, r):
                a_multi += 1
        # 多维度+基准率
        a_mb = 0
        for r in sr:
            pred = predict_multi_base(r, dcfg, brates)
            if check_ok(pred, r):
                a_mb += 1
        # 板块最优
        a_so = 0
        for r in sr:
            pred = predict_sector_optimal(r, sopt)
            if check_ok(pred, r):
                a_so += 1
        print(f"{sector:<8} {n:>4} {a_up/n*100:>5.1f}% {a_comb/n*100:>5.1f}% {a_multi/n*100:>5.1f}% {a_mb/n*100:>7.1f}% {a_so/n*100:>7.1f}%")


# ── 5. 最终结论表 ──
print("\n" + "=" * 70)
print("  Part 5: 最终结论")
print("=" * 70)

print(f"""
┌─────────────────────────────────────────────────────────────────┐
│                    周预测最终准确率汇总                          │
├─────────────────────────────────────────────────────────────────┤
│ 日频基线 (宽松正确):  {loose_ok}/{total} = {loose_ok/total*100:.1f}%                       │
│ 周样本数:             {nw}                                      │
├─────────────────────────────────────────────────────────────────┤""")

# 全样本最优
best_full_name = max(strategies_full, key=lambda k: strategies_full[k][0] / strategies_full[k][1])
bf_ok, bf_tot = strategies_full[best_full_name]

# 测试集最优
best_test_name = max(strategies_test, key=lambda k: strategies_test[k][0] / strategies_test[k][1])
bt_ok, bt_tot = strategies_test[best_test_name]

# 滚动最优
best_roll_name = max(rolling_results, key=lambda k: rolling_results[k][0] / rolling_results[k][1] if rolling_results[k][1] > 0 else 0)
br_ok, br_tot = rolling_results[best_roll_name]

print(f"│ 全样本最优:  {best_full_name:<12} {bf_ok}/{bf_tot} = {bf_ok/bf_tot*100:.1f}%              │")
print(f"│ 泛化测试最优: {best_test_name:<12} {bt_ok}/{bt_tot} = {bt_ok/bt_tot*100:.1f}%              │")
print(f"│ 滚动验证最优: {best_roll_name:<12} {br_ok}/{br_tot} = {br_ok/br_tot*100:.1f}%              │")
print(f"├─────────────────────────────────────────────────────────────────┤")

# 各策略对比
print(f"│ 策略对比:                                                      │")
print(f"│ {'策略':<12} {'全样本':>8} {'泛化测试':>8} {'滚动验证':>8}          │")
for name in ['全涨', '全跌', '单融合信号', '多维度融合', '多维度+基准率', '板块最优配置']:
    f_ok, f_tot = strategies_full[name]
    t_ok, t_tot = strategies_test[name]
    r_ok, r_tot = rolling_results[name]
    print(f"│ {name:<12} {f_ok/f_tot*100:>7.1f}% {t_ok/t_tot*100:>7.1f}% {r_ok/r_tot*100:>7.1f}%          │")

print(f"├─────────────────────────────────────────────────────────────────┤")
print(f"│ 结论:                                                          │")
print(f"│ - 全样本最优策略: {best_full_name} ({bf_ok/bf_tot*100:.1f}%)                     │")
print(f"│ - 真实泛化能力:   {best_test_name} ({bt_ok/bt_tot*100:.1f}%)                     │")
print(f"│ - 滚动验证能力:   {best_roll_name} ({br_ok/br_tot*100:.1f}%)                     │")
print(f"│ - 日频基线对比:   {loose_ok/total*100:.1f}%                                      │")
print(f"└─────────────────────────────────────────────────────────────────┘")

print("\n完成。")
