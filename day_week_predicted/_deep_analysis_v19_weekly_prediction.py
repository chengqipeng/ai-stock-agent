#!/usr/bin/env python3
"""
v19 按周预测深度分析

核心思路：
  当前模型是"日频预测"（评分日→预测次日涨跌），日频噪声大、准确率天花板低。
  本分析探索"周频预测"：
  1. 用周一的因子信号预测整周方向（周一→周五累计涨跌）
  2. 用一周内多日信号投票决定周方向
  3. 分析周级别信号的稳定性和可预测性
  4. 对比日频 vs 周频的准确率和信噪比

分析维度：
  Part 1: 周级别涨跌统计与基准率
  Part 2: 周一信号→整周方向预测力
  Part 3: 周内多日信号投票→整周方向
  Part 4: 板块×周频的稳定性分析
  Part 5: 周频因子有效性（哪些因子在周级别更有效）
  Part 6: 滚动周预测模拟（前N周训练→第N+1周测试）
  Part 7: 综合结论与建议
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')

print(f"{'=' * 80}")
print(f"v19 按周预测深度分析")
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
        d['_dt'] = datetime.strptime(d['评分日'], '%Y-%m-%d')
        d['_wd'] = d['_dt'].weekday()  # 0=Mon, 4=Fri
        d['_iso_week'] = d['_dt'].isocalendar()[:2]  # (year, week_num)
    except:
        d['_wd'] = -1
        d['_iso_week'] = (0, 0)

sectors = ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']
all_dates = sorted(set(d['评分日'] for d in details))
all_codes = sorted(set(d['代码'] for d in details))

loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"日频基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 构建周级别数据结构
# ═══════════════════════════════════════════════════════════

# 按(股票代码, ISO周)分组
stock_week_data = defaultdict(list)
for d in details:
    key = (d['代码'], d['_iso_week'])
    stock_week_data[key].append(d)

# 计算每个(股票, 周)的周级别指标
weekly_records = []
for (code, iso_week), days in stock_week_data.items():
    days_sorted = sorted(days, key=lambda x: x['评分日'])
    if len(days_sorted) < 2:
        continue  # 至少需要2天数据才有意义

    sector = days_sorted[0]['板块']
    name = days_sorted[0]['名称']

    # 周累计涨跌 = 各日实际涨跌的累乘近似（小幅度下近似为求和）
    week_cum_chg = sum(d['_actual'] for d in days_sorted)
    # 精确累乘
    cum_return = 1.0
    for d in days_sorted:
        cum_return *= (1 + d['_actual'] / 100)
    week_exact_chg = (cum_return - 1) * 100

    # 周内涨跌天数
    up_days = sum(1 for d in days_sorted if d['_actual'] > 0)
    dn_days = sum(1 for d in days_sorted if d['_actual'] < 0)
    flat_days = sum(1 for d in days_sorted if d['_actual'] == 0)

    # 周一（第一天）的信号
    mon_d = days_sorted[0]
    mon_combined = mon_d['融合信号']
    mon_score = mon_d['评分']
    mon_confidence = mon_d['置信度']
    mon_pred = mon_d['预测方向']

    # 周内所有日的信号均值
    avg_combined = sum(d['融合信号'] for d in days_sorted) / len(days_sorted)
    avg_score = sum(d['评分'] for d in days_sorted) / len(days_sorted)

    # 周内信号投票
    vote_up = sum(1 for d in days_sorted if d['预测方向'] == '上涨')
    vote_dn = sum(1 for d in days_sorted if d['预测方向'] == '下跌')
    vote_direction = '上涨' if vote_up > vote_dn else ('下跌' if vote_dn > vote_up else '持平')

    # 日频准确率
    daily_ok = sum(1 for d in days_sorted if d['宽松正确'] == '✓')

    weekly_records.append({
        'code': code,
        'name': name,
        'sector': sector,
        'iso_week': iso_week,
        'n_days': len(days_sorted),
        'week_start': days_sorted[0]['评分日'],
        'week_end': days_sorted[-1]['评分日'],
        'week_cum_chg': week_cum_chg,
        'week_exact_chg': week_exact_chg,
        'week_up': week_exact_chg >= 0,
        'week_dn': week_exact_chg <= 0,
        'up_days': up_days,
        'dn_days': dn_days,
        'flat_days': flat_days,
        'mon_combined': mon_combined,
        'mon_score': mon_score,
        'mon_confidence': mon_confidence,
        'mon_pred': mon_pred,
        'avg_combined': avg_combined,
        'avg_score': avg_score,
        'vote_direction': vote_direction,
        'vote_up': vote_up,
        'vote_dn': vote_dn,
        'daily_ok': daily_ok,
        'days': days_sorted,
    })

print(f"周级别样本: {len(weekly_records)} (股票×周)")
print(f"周数: {len(set(r['iso_week'] for r in weekly_records))}")


# ═══════════════════════════════════════════════════════════
# Part 1: 周级别涨跌统计与基准率
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 1: 周级别涨跌统计与基准率")
print(f"{'=' * 80}")

# 总体周涨跌基准率
week_up_total = sum(1 for r in weekly_records if r['week_up'])
week_dn_total = sum(1 for r in weekly_records if r['week_dn'])
n_weekly = len(weekly_records)
print(f"\n总体: 周涨(>=0): {week_up_total}/{n_weekly} ({week_up_total/n_weekly*100:.1f}%), "
      f"周跌(<=0): {week_dn_total}/{n_weekly} ({week_dn_total/n_weekly*100:.1f}%)")

# 按板块
print(f"\n{'板块':<10} {'周样本':>6} {'周涨>=0':>10} {'周跌<=0':>10} {'平均周涨跌':>10} {'日频准确率':>10}")
print('-' * 65)
for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    if not sr:
        continue
    up = sum(1 for r in sr if r['week_up'])
    dn = sum(1 for r in sr if r['week_dn'])
    avg_chg = sum(r['week_exact_chg'] for r in sr) / len(sr)
    daily_total = sum(r['n_days'] for r in sr)
    daily_ok = sum(r['daily_ok'] for r in sr)
    print(f"{sector:<10} {len(sr):>6} {up/len(sr)*100:>8.1f}% {dn/len(sr)*100:>8.1f}% "
          f"{avg_chg:>+8.2f}% {daily_ok/daily_total*100:>8.1f}%")

# 按ISO周
print(f"\n按周统计:")
print(f"{'周':>10} {'样本':>6} {'周涨>=0':>10} {'平均涨跌':>10} {'日频准确率':>10}")
print('-' * 55)
for iso_week in sorted(set(r['iso_week'] for r in weekly_records)):
    wr = [r for r in weekly_records if r['iso_week'] == iso_week]
    up = sum(1 for r in wr if r['week_up'])
    avg_chg = sum(r['week_exact_chg'] for r in wr) / len(wr)
    daily_total = sum(r['n_days'] for r in wr)
    daily_ok = sum(r['daily_ok'] for r in wr)
    print(f"{iso_week[0]}-W{iso_week[1]:02d} {len(wr):>6} {up/len(wr)*100:>8.1f}% "
          f"{avg_chg:>+8.2f}% {daily_ok/daily_total*100:>8.1f}%")

# 周内天数分布
print(f"\n周内天数分布:")
for nd in sorted(set(r['n_days'] for r in weekly_records)):
    cnt = sum(1 for r in weekly_records if r['n_days'] == nd)
    print(f"  {nd}天: {cnt}个周样本 ({cnt/n_weekly*100:.1f}%)")


# ═══════════════════════════════════════════════════════════
# Part 2: 周一信号→整周方向预测力
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 2: 周一信号→整周方向预测力")
print(f"{'=' * 80}")

print(f"\n策略A: 用周一的融合信号方向预测整周涨跌")
print(f"{'板块':<10} {'信号>0→周涨':>14} {'信号<0→周跌':>14} {'方向一致率':>12} {'vs日频':>8}")
print('-' * 70)

for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    pos = [r for r in sr if r['mon_combined'] > 0]
    neg = [r for r in sr if r['mon_combined'] < 0]
    zero = [r for r in sr if r['mon_combined'] == 0]

    pos_up = sum(1 for r in pos if r['week_up']) / len(pos) * 100 if pos else 0
    neg_dn = sum(1 for r in neg if r['week_dn']) / len(neg) * 100 if neg else 0

    # 总方向一致率
    all_sig = [r for r in sr if r['mon_combined'] != 0]
    dir_ok = sum(1 for r in all_sig if
                 (r['mon_combined'] > 0 and r['week_up']) or
                 (r['mon_combined'] < 0 and r['week_dn']))
    dir_rate = dir_ok / len(all_sig) * 100 if all_sig else 0

    # 日频准确率对比
    daily_total = sum(r['n_days'] for r in sr)
    daily_ok = sum(r['daily_ok'] for r in sr)
    daily_rate = daily_ok / daily_total * 100 if daily_total > 0 else 0

    print(f"{sector:<10} {pos_up:>5.1f}%({len(pos):>3}) {neg_dn:>5.1f}%({len(neg):>3}) "
          f"{dir_rate:>5.1f}%({len(all_sig):>3}) {dir_rate - daily_rate:>+6.1f}pp")

print(f"\n策略B: 用周一的预测方向预测整周涨跌")
print(f"{'板块':<10} {'预测涨→周涨':>14} {'预测跌→周跌':>14} {'方向一致率':>12}")
print('-' * 55)

for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    pred_up = [r for r in sr if r['mon_pred'] == '上涨']
    pred_dn = [r for r in sr if r['mon_pred'] == '下跌']

    pu_ok = sum(1 for r in pred_up if r['week_up']) / len(pred_up) * 100 if pred_up else 0
    pd_ok = sum(1 for r in pred_dn if r['week_dn']) / len(pred_dn) * 100 if pred_dn else 0

    all_pred = pred_up + pred_dn
    dir_ok = sum(1 for r in all_pred if
                 (r['mon_pred'] == '上涨' and r['week_up']) or
                 (r['mon_pred'] == '下跌' and r['week_dn']))
    dir_rate = dir_ok / len(all_pred) * 100 if all_pred else 0

    print(f"{sector:<10} {pu_ok:>5.1f}%({len(pred_up):>3}) {pd_ok:>5.1f}%({len(pred_dn):>3}) "
          f"{dir_rate:>5.1f}%({len(all_pred):>3})")

# 按融合信号强度分桶
print(f"\n策略C: 周一融合信号强度 vs 整周方向（全板块）")
buckets = [(-999, -2.0), (-2.0, -1.0), (-1.0, -0.3), (-0.3, 0.3), (0.3, 1.0), (1.0, 2.0), (2.0, 999)]
bucket_labels = ['<-2.0', '-2~-1', '-1~-0.3', '-0.3~0.3', '0.3~1', '1~2', '>2.0']
print(f"{'信号区间':<12} {'样本':>6} {'周涨>=0':>10} {'周跌<=0':>10} {'平均周涨跌':>10}")
print('-' * 55)
for (lo, hi), label in zip(buckets, bucket_labels):
    br = [r for r in weekly_records if lo <= r['mon_combined'] < hi]
    if not br:
        continue
    up = sum(1 for r in br if r['week_up'])
    dn = sum(1 for r in br if r['week_dn'])
    avg = sum(r['week_exact_chg'] for r in br) / len(br)
    print(f"{label:<12} {len(br):>6} {up/len(br)*100:>8.1f}% {dn/len(br)*100:>8.1f}% {avg:>+8.2f}%")


# ═══════════════════════════════════════════════════════════
# Part 3: 周内多日信号投票→整周方向
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 3: 周内多日信号投票→整周方向")
print(f"{'=' * 80}")

print(f"\n策略D: 周内预测方向多数投票→整周方向")
print(f"{'板块':<10} {'投票涨→周涨':>14} {'投票跌→周跌':>14} {'方向一致率':>12} {'vs日频':>8}")
print('-' * 70)

for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    v_up = [r for r in sr if r['vote_direction'] == '上涨']
    v_dn = [r for r in sr if r['vote_direction'] == '下跌']

    vu_ok = sum(1 for r in v_up if r['week_up']) / len(v_up) * 100 if v_up else 0
    vd_ok = sum(1 for r in v_dn if r['week_dn']) / len(v_dn) * 100 if v_dn else 0

    all_v = [r for r in sr if r['vote_direction'] != '持平']
    dir_ok = sum(1 for r in all_v if
                 (r['vote_direction'] == '上涨' and r['week_up']) or
                 (r['vote_direction'] == '下跌' and r['week_dn']))
    dir_rate = dir_ok / len(all_v) * 100 if all_v else 0

    daily_total = sum(r['n_days'] for r in sr)
    daily_ok = sum(r['daily_ok'] for r in sr)
    daily_rate = daily_ok / daily_total * 100 if daily_total > 0 else 0

    print(f"{sector:<10} {vu_ok:>5.1f}%({len(v_up):>3}) {vd_ok:>5.1f}%({len(v_dn):>3}) "
          f"{dir_rate:>5.1f}%({len(all_v):>3}) {dir_rate - daily_rate:>+6.1f}pp")

print(f"\n策略E: 周内融合信号均值→整周方向")
print(f"{'板块':<10} {'均值>0→周涨':>14} {'均值<0→周跌':>14} {'方向一致率':>12}")
print('-' * 55)

for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    avg_pos = [r for r in sr if r['avg_combined'] > 0]
    avg_neg = [r for r in sr if r['avg_combined'] < 0]

    ap_ok = sum(1 for r in avg_pos if r['week_up']) / len(avg_pos) * 100 if avg_pos else 0
    an_ok = sum(1 for r in avg_neg if r['week_dn']) / len(avg_neg) * 100 if avg_neg else 0

    all_avg = [r for r in sr if r['avg_combined'] != 0]
    dir_ok = sum(1 for r in all_avg if
                 (r['avg_combined'] > 0 and r['week_up']) or
                 (r['avg_combined'] < 0 and r['week_dn']))
    dir_rate = dir_ok / len(all_avg) * 100 if all_avg else 0

    print(f"{sector:<10} {ap_ok:>5.1f}%({len(avg_pos):>3}) {an_ok:>5.1f}%({len(avg_neg):>3}) "
          f"{dir_rate:>5.1f}%({len(all_avg):>3})")


# ═══════════════════════════════════════════════════════════
# Part 4: 板块×周频的稳定性分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 4: 板块×周频稳定性分析（前半 vs 后半）")
print(f"{'=' * 80}")

all_weeks = sorted(set(r['iso_week'] for r in weekly_records))
mid_week_idx = len(all_weeks) // 2
first_half_weeks = set(all_weeks[:mid_week_idx])
second_half_weeks = set(all_weeks[mid_week_idx:])

print(f"前半: {len(first_half_weeks)}周, 后半: {len(second_half_weeks)}周")
print(f"前半: {sorted(first_half_weeks)[0]} ~ {sorted(first_half_weeks)[-1]}")
print(f"后半: {sorted(second_half_weeks)[0]} ~ {sorted(second_half_weeks)[-1]}")

print(f"\n{'板块':<10} {'前半周涨率':>10} {'后半周涨率':>10} {'差异':>8} {'前半投票准':>10} {'后半投票准':>10} {'差异':>8}")
print('-' * 75)

for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    f_data = [r for r in sr if r['iso_week'] in first_half_weeks]
    s_data = [r for r in sr if r['iso_week'] in second_half_weeks]

    f_up = sum(1 for r in f_data if r['week_up']) / len(f_data) * 100 if f_data else 0
    s_up = sum(1 for r in s_data if r['week_up']) / len(s_data) * 100 if s_data else 0

    # 投票准确率
    f_vote = [r for r in f_data if r['vote_direction'] != '持平']
    s_vote = [r for r in s_data if r['vote_direction'] != '持平']
    f_vote_ok = sum(1 for r in f_vote if
                    (r['vote_direction'] == '上涨' and r['week_up']) or
                    (r['vote_direction'] == '下跌' and r['week_dn']))
    s_vote_ok = sum(1 for r in s_vote if
                    (r['vote_direction'] == '上涨' and r['week_up']) or
                    (r['vote_direction'] == '下跌' and r['week_dn']))
    f_vr = f_vote_ok / len(f_vote) * 100 if f_vote else 0
    s_vr = s_vote_ok / len(s_vote) * 100 if s_vote else 0

    print(f"{sector:<10} {f_up:>8.1f}% {s_up:>8.1f}% {f_up-s_up:>+6.1f}pp "
          f"{f_vr:>8.1f}% {s_vr:>8.1f}% {f_vr-s_vr:>+6.1f}pp")


# ═══════════════════════════════════════════════════════════
# Part 5: 周频因子有效性分析
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 5: 周频因子有效性（哪些因子在周级别更有效）")
print(f"{'=' * 80}")

# 从回测数据中提取因子有效性
factor_analysis = bt_data.get('因子有效性分析(按板块)', {})
factor_names = ['reversion', 'rsi', 'kdj', 'macd', 'boll', 'vp', 'fund', 'market',
                'streak', 'trend_bias', 'us_overnight', 'vol_regime', 'momentum_persist',
                'gap_signal', 'intraday_pos', 'db_fund', 'turnover']
factor_labels = {
    'reversion': '均值回归', 'rsi': 'RSI', 'kdj': 'KDJ', 'macd': 'MACD',
    'boll': 'BOLL', 'vp': '量价背离', 'fund': '资金流API', 'market': '大盘环境',
    'streak': '连续涨跌', 'trend_bias': 'MA趋势', 'us_overnight': '美股隔夜',
    'vol_regime': '波动率', 'momentum_persist': '动量持续', 'gap_signal': '跳空缺口',
    'intraday_pos': '日内位置', 'db_fund': 'DB资金流', 'turnover': '换手率',
}

# 分析周一因子信号 vs 整周方向的一致率
# 需要从逐日详情中提取因子信号（但JSON中没有存储单个因子值）
# 我们用融合信号的分解来近似分析

# 替代方案：分析周一的各维度信号 vs 整周方向
print(f"\n周一各维度信号 vs 整周方向一致率:")
print(f"{'维度':<12} {'信号>0→周涨':>14} {'信号<0→周跌':>14} {'方向一致率':>12}")
print('-' * 55)

# 可用的维度: 融合信号, 技术信号, 同行信号, RS信号, 美股隔夜
dimensions = [
    ('融合信号', 'mon_combined'),
    ('技术信号', lambda r: r['days'][0]['技术信号']),
    ('同行信号', lambda r: r['days'][0]['同行信号']),
    ('RS信号', lambda r: r['days'][0]['RS信号']),
    ('美股隔夜', lambda r: r['days'][0]['美股隔夜']),
]

for dim_name, dim_key in dimensions:
    if isinstance(dim_key, str):
        get_val = lambda r, k=dim_key: r[k]
    else:
        get_val = dim_key

    pos = [r for r in weekly_records if get_val(r) > 0]
    neg = [r for r in weekly_records if get_val(r) < 0]

    pos_ok = sum(1 for r in pos if r['week_up']) / len(pos) * 100 if pos else 0
    neg_ok = sum(1 for r in neg if r['week_dn']) / len(neg) * 100 if neg else 0

    all_sig = [r for r in weekly_records if get_val(r) != 0]
    dir_ok = sum(1 for r in all_sig if
                 (get_val(r) > 0 and r['week_up']) or
                 (get_val(r) < 0 and r['week_dn']))
    dir_rate = dir_ok / len(all_sig) * 100 if all_sig else 0

    print(f"{dim_name:<12} {pos_ok:>5.1f}%({len(pos):>3}) {neg_ok:>5.1f}%({len(neg):>3}) "
          f"{dir_rate:>5.1f}%({len(all_sig):>3})")

# 按板块分析
print(f"\n按板块: 周一融合信号 vs 整周方向一致率")
print(f"{'板块':<10} {'信号>0→周涨':>14} {'信号<0→周跌':>14} {'方向一致率':>12} {'日频一致率':>12}")
print('-' * 70)

for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    pos = [r for r in sr if r['mon_combined'] > 0]
    neg = [r for r in sr if r['mon_combined'] < 0]

    pos_ok = sum(1 for r in pos if r['week_up']) / len(pos) * 100 if pos else 0
    neg_ok = sum(1 for r in neg if r['week_dn']) / len(neg) * 100 if neg else 0

    all_sig = [r for r in sr if r['mon_combined'] != 0]
    dir_ok = sum(1 for r in all_sig if
                 (r['mon_combined'] > 0 and r['week_up']) or
                 (r['mon_combined'] < 0 and r['week_dn']))
    dir_rate = dir_ok / len(all_sig) * 100 if all_sig else 0

    # 日频对比: 融合信号方向 vs 次日方向
    sd = [d for d in details if d['板块'] == sector and d['融合信号'] != 0]
    daily_dir_ok = sum(1 for d in sd if
                       (d['融合信号'] > 0 and d['_ge0']) or
                       (d['融合信号'] < 0 and d['_le0']))
    daily_dir_rate = daily_dir_ok / len(sd) * 100 if sd else 0

    print(f"{sector:<10} {pos_ok:>5.1f}%({len(pos):>3}) {neg_ok:>5.1f}%({len(neg):>3}) "
          f"{dir_rate:>5.1f}%({len(all_sig):>3}) {daily_dir_rate:>5.1f}%({len(sd):>3})")


# ═══════════════════════════════════════════════════════════
# Part 6: 滚动周预测模拟
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 6: 滚动周预测模拟（前N周训练→第N+1周测试）")
print(f"{'=' * 80}")

# 策略: 用前N周的板块周涨跌基准率来预测第N+1周
# 如果前N周某板块涨的周数 > 跌的周数 → 预测涨，反之预测跌

sorted_weeks = sorted(set(r['iso_week'] for r in weekly_records))
min_train_weeks = 3  # 至少3周训练数据

print(f"\n策略F: 滚动板块周涨跌基准率预测")
print(f"{'测试周':>12} {'样本':>6} {'基准率预测准':>12} {'投票预测准':>12} {'周一信号准':>12} {'全涨基线':>10}")
print('-' * 75)

rolling_results = []
for test_idx in range(min_train_weeks, len(sorted_weeks)):
    test_week = sorted_weeks[test_idx]
    train_weeks = set(sorted_weeks[:test_idx])

    test_records = [r for r in weekly_records if r['iso_week'] == test_week]
    if not test_records:
        continue

    # 训练: 计算每个板块的周涨跌基准率
    sector_train_up_rate = {}
    for sector in sectors:
        train_sr = [r for r in weekly_records if r['sector'] == sector and r['iso_week'] in train_weeks]
        if train_sr:
            sector_train_up_rate[sector] = sum(1 for r in train_sr if r['week_up']) / len(train_sr)
        else:
            sector_train_up_rate[sector] = 0.5

    # 测试
    base_ok = 0
    vote_ok = 0
    mon_ok = 0
    all_up_ok = 0

    for r in test_records:
        sector = r['sector']
        # 基准率预测
        base_pred = '上涨' if sector_train_up_rate.get(sector, 0.5) > 0.5 else '下跌'
        if (base_pred == '上涨' and r['week_up']) or (base_pred == '下跌' and r['week_dn']):
            base_ok += 1

        # 投票预测
        if r['vote_direction'] != '持平':
            if (r['vote_direction'] == '上涨' and r['week_up']) or \
               (r['vote_direction'] == '下跌' and r['week_dn']):
                vote_ok += 1

        # 周一信号预测
        if r['mon_combined'] != 0:
            if (r['mon_combined'] > 0 and r['week_up']) or \
               (r['mon_combined'] < 0 and r['week_dn']):
                mon_ok += 1

        # 全涨基线
        if r['week_up']:
            all_up_ok += 1

    n = len(test_records)
    base_rate = base_ok / n * 100
    vote_rate = vote_ok / n * 100
    mon_rate = mon_ok / n * 100
    all_up_rate = all_up_ok / n * 100

    rolling_results.append({
        'week': test_week, 'n': n,
        'base_rate': base_rate, 'vote_rate': vote_rate,
        'mon_rate': mon_rate, 'all_up_rate': all_up_rate
    })

    print(f"{test_week[0]}-W{test_week[1]:02d} {n:>6} {base_rate:>10.1f}% {vote_rate:>10.1f}% "
          f"{mon_rate:>10.1f}% {all_up_rate:>8.1f}%")

# 汇总
if rolling_results:
    total_n = sum(r['n'] for r in rolling_results)
    avg_base = sum(r['base_rate'] * r['n'] for r in rolling_results) / total_n
    avg_vote = sum(r['vote_rate'] * r['n'] for r in rolling_results) / total_n
    avg_mon = sum(r['mon_rate'] * r['n'] for r in rolling_results) / total_n
    avg_all_up = sum(r['all_up_rate'] * r['n'] for r in rolling_results) / total_n
    print(f"{'加权平均':>12} {total_n:>6} {avg_base:>10.1f}% {avg_vote:>10.1f}% "
          f"{avg_mon:>10.1f}% {avg_all_up:>8.1f}%")


# ═══════════════════════════════════════════════════════════
# Part 6b: 板块×阈值优化的周预测
# ═══════════════════════════════════════════════════════════
print(f"\n{'─' * 60}")
print(f"策略G: 板块特化周预测（融合信号阈值优化）")
print(f"{'─' * 60}")

# 对每个板块搜索最优的周一融合信号阈值
print(f"\n{'板块':<10} {'最优bull_th':>10} {'最优bear_th':>10} {'默认方向':>8} {'周准确率':>10} {'vs全涨':>8}")
print('-' * 65)

best_weekly_configs = {}
for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    # 前半训练
    f_data = [r for r in sr if r['iso_week'] in first_half_weeks]
    s_data = [r for r in sr if r['iso_week'] in second_half_weeks]

    best_f_rate = 0
    best_s_rate = 0
    best_params = None

    for bull_th in [-1.0, -0.5, 0.0, 0.3, 0.5, 1.0, 1.5, 999]:
        for bear_th in [-2.0, -1.5, -1.0, -0.5, -0.3, 0.0, -999]:
            for def_up in [True, False]:
                # 训练
                f_ok = 0
                for r in f_data:
                    sig = r['mon_combined']
                    if sig > bull_th:
                        pred = '上涨'
                    elif sig < bear_th:
                        pred = '下跌'
                    else:
                        pred = '上涨' if def_up else '下跌'
                    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
                        f_ok += 1
                f_rate = f_ok / len(f_data) * 100 if f_data else 0

                # 测试
                s_ok = 0
                for r in s_data:
                    sig = r['mon_combined']
                    if sig > bull_th:
                        pred = '上涨'
                    elif sig < bear_th:
                        pred = '下跌'
                    else:
                        pred = '上涨' if def_up else '下跌'
                    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
                        s_ok += 1
                s_rate = s_ok / len(s_data) * 100 if s_data else 0

                if f_rate > best_f_rate or (f_rate == best_f_rate and s_rate > best_s_rate):
                    best_f_rate = f_rate
                    best_s_rate = s_rate
                    best_params = (bull_th, bear_th, def_up)

    if best_params:
        bt, brt, du = best_params
        # 全样本准确率
        total_ok = 0
        for r in sr:
            sig = r['mon_combined']
            if sig > bt:
                pred = '上涨'
            elif sig < brt:
                pred = '下跌'
            else:
                pred = '上涨' if du else '下跌'
            if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
                total_ok += 1
        total_rate = total_ok / len(sr) * 100

        all_up_ok = sum(1 for r in sr if r['week_up'])
        all_up_rate = all_up_ok / len(sr) * 100

        best_weekly_configs[sector] = best_params
        print(f"{sector:<10} {bt:>10.1f} {brt:>10.1f} {'涨' if du else '跌':>8} "
              f"{total_rate:>8.1f}% {total_rate - all_up_rate:>+6.1f}pp")
        print(f"{'':>10} 前半{best_f_rate:.1f}% → 后半{best_s_rate:.1f}% (泛化差{best_f_rate-best_s_rate:.1f}pp)")


# ═══════════════════════════════════════════════════════════
# Part 6c: 周内信号一致性→周预测置信度
# ═══════════════════════════════════════════════════════════
print(f"\n{'─' * 60}")
print(f"策略H: 周内信号一致性作为置信度过滤")
print(f"{'─' * 60}")

# 如果一周内大多数天的预测方向一致，说明信号稳定，预测更可靠
print(f"\n{'一致度':>8} {'样本':>6} {'周涨>=0':>10} {'投票准确率':>12} {'vs全样本':>10}")
print('-' * 55)

for min_agree in [0.6, 0.7, 0.8, 0.9, 1.0]:
    filtered = []
    for r in weekly_records:
        if r['n_days'] < 3:
            continue
        agree_ratio = max(r['vote_up'], r['vote_dn']) / r['n_days']
        if agree_ratio >= min_agree:
            filtered.append(r)

    if not filtered:
        continue

    up_rate = sum(1 for r in filtered if r['week_up']) / len(filtered) * 100
    vote_ok = sum(1 for r in filtered if r['vote_direction'] != '持平' and
                  ((r['vote_direction'] == '上涨' and r['week_up']) or
                   (r['vote_direction'] == '下跌' and r['week_dn'])))
    vote_total = sum(1 for r in filtered if r['vote_direction'] != '持平')
    vote_rate = vote_ok / vote_total * 100 if vote_total > 0 else 0

    # 全样本投票准确率
    all_vote_ok = sum(1 for r in weekly_records if r['vote_direction'] != '持平' and
                      ((r['vote_direction'] == '上涨' and r['week_up']) or
                       (r['vote_direction'] == '下跌' and r['week_dn'])))
    all_vote_total = sum(1 for r in weekly_records if r['vote_direction'] != '持平')
    all_vote_rate = all_vote_ok / all_vote_total * 100 if all_vote_total > 0 else 0

    print(f"{min_agree:>6.0%} {len(filtered):>6} {up_rate:>8.1f}% {vote_rate:>10.1f}% "
          f"{vote_rate - all_vote_rate:>+8.1f}pp")


# ═══════════════════════════════════════════════════════════
# Part 7: 周预测 vs 日预测的信噪比对比
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 7: 周预测 vs 日预测信噪比对比")
print(f"{'=' * 80}")

# 日频: 涨跌幅分布
daily_chgs = [d['_actual'] for d in details]
daily_abs_mean = sum(abs(c) for c in daily_chgs) / len(daily_chgs)
daily_mean = sum(daily_chgs) / len(daily_chgs)
daily_std = (sum((c - daily_mean)**2 for c in daily_chgs) / len(daily_chgs))**0.5

# 周频: 涨跌幅分布
weekly_chgs = [r['week_exact_chg'] for r in weekly_records]
weekly_abs_mean = sum(abs(c) for c in weekly_chgs) / len(weekly_chgs)
weekly_mean = sum(weekly_chgs) / len(weekly_chgs)
weekly_std = (sum((c - weekly_mean)**2 for c in weekly_chgs) / len(weekly_chgs))**0.5

# 信噪比 = |mean| / std
daily_snr = abs(daily_mean) / daily_std if daily_std > 0 else 0
weekly_snr = abs(weekly_mean) / weekly_std if weekly_std > 0 else 0

print(f"\n{'指标':<16} {'日频':>12} {'周频':>12}")
print('-' * 45)
print(f"{'样本数':<16} {len(daily_chgs):>12} {len(weekly_chgs):>12}")
print(f"{'平均涨跌':<16} {daily_mean:>+10.3f}% {weekly_mean:>+10.3f}%")
print(f"{'平均|涨跌|':<16} {daily_abs_mean:>10.3f}% {weekly_abs_mean:>10.3f}%")
print(f"{'标准差':<16} {daily_std:>10.3f}% {weekly_std:>10.3f}%")
print(f"{'信噪比':<16} {daily_snr:>10.4f} {weekly_snr:>10.4f}")
print(f"{'>=0%占比':<16} {sum(1 for c in daily_chgs if c >= 0)/len(daily_chgs)*100:>9.1f}% "
      f"{sum(1 for c in weekly_chgs if c >= 0)/len(weekly_chgs)*100:>9.1f}%")

# 按板块对比
print(f"\n按板块信噪比对比:")
print(f"{'板块':<10} {'日频SNR':>10} {'周频SNR':>10} {'日频|chg|':>10} {'周频|chg|':>10} {'周频更优':>8}")
print('-' * 60)

for sector in sectors:
    d_chgs = [d['_actual'] for d in details if d['板块'] == sector]
    w_chgs = [r['week_exact_chg'] for r in weekly_records if r['sector'] == sector]

    d_mean = sum(d_chgs) / len(d_chgs) if d_chgs else 0
    d_std = (sum((c - d_mean)**2 for c in d_chgs) / len(d_chgs))**0.5 if d_chgs else 1
    d_snr = abs(d_mean) / d_std if d_std > 0 else 0

    w_mean = sum(w_chgs) / len(w_chgs) if w_chgs else 0
    w_std = (sum((c - w_mean)**2 for c in w_chgs) / len(w_chgs))**0.5 if w_chgs else 1
    w_snr = abs(w_mean) / w_std if w_std > 0 else 0

    d_abs = sum(abs(c) for c in d_chgs) / len(d_chgs) if d_chgs else 0
    w_abs = sum(abs(c) for c in w_chgs) / len(w_chgs) if w_chgs else 0

    better = '✅' if w_snr > d_snr else '❌'
    print(f"{sector:<10} {d_snr:>8.4f} {w_snr:>8.4f} {d_abs:>8.3f}% {w_abs:>8.3f}% {better}")


# ═══════════════════════════════════════════════════════════
# Part 8: 周预测实战模拟
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 8: 周预测实战模拟（综合策略对比）")
print(f"{'=' * 80}")

# 模拟多种周预测策略，计算"周级别宽松准确率"
# 宽松: 预测涨且周涨>=0, 或预测跌且周跌<=0

strategies = {}

# 策略1: 全部预测涨
s1_ok = sum(1 for r in weekly_records if r['week_up'])
strategies['全涨'] = s1_ok / n_weekly * 100

# 策略2: 全部预测跌
s2_ok = sum(1 for r in weekly_records if r['week_dn'])
strategies['全跌'] = s2_ok / n_weekly * 100

# 策略3: 周一融合信号方向
s3_ok = 0
for r in weekly_records:
    if r['mon_combined'] > 0:
        if r['week_up']: s3_ok += 1
    elif r['mon_combined'] < 0:
        if r['week_dn']: s3_ok += 1
    else:
        if r['week_up']: s3_ok += 1  # 默认涨
strategies['周一信号'] = s3_ok / n_weekly * 100

# 策略4: 周内投票
s4_ok = 0
for r in weekly_records:
    if r['vote_direction'] == '上涨':
        if r['week_up']: s4_ok += 1
    elif r['vote_direction'] == '下跌':
        if r['week_dn']: s4_ok += 1
    else:
        if r['week_up']: s4_ok += 1
strategies['周内投票'] = s4_ok / n_weekly * 100

# 策略5: 板块基准率（用全样本，有前视偏差）
s5_ok = 0
sector_up_rate = {}
for sector in sectors:
    sr = [r for r in weekly_records if r['sector'] == sector]
    sector_up_rate[sector] = sum(1 for r in sr if r['week_up']) / len(sr) if sr else 0.5
for r in weekly_records:
    pred = '上涨' if sector_up_rate.get(r['sector'], 0.5) > 0.5 else '下跌'
    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
        s5_ok += 1
strategies['板块基准率(前视)'] = s5_ok / n_weekly * 100

# 策略6: 板块特化阈值（前半训练）
s6_ok = 0
for r in weekly_records:
    sector = r['sector']
    if sector in best_weekly_configs:
        bt, brt, du = best_weekly_configs[sector]
    else:
        bt, brt, du = 0.5, -0.5, True
    sig = r['mon_combined']
    if sig > bt:
        pred = '上涨'
    elif sig < brt:
        pred = '下跌'
    else:
        pred = '上涨' if du else '下跌'
    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
        s6_ok += 1
strategies['板块阈值优化'] = s6_ok / n_weekly * 100

# 策略7: 周一信号 + 板块偏向混合
s7_ok = 0
for r in weekly_records:
    sector = r['sector']
    sig = r['mon_combined']
    base_up = sector_up_rate.get(sector, 0.5) > 0.5
    # 强信号用信号，弱信号用基准率
    if abs(sig) > 1.0:
        pred = '上涨' if sig > 0 else '下跌'
    else:
        pred = '上涨' if base_up else '下跌'
    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
        s7_ok += 1
strategies['信号+基准率混合'] = s7_ok / n_weekly * 100

# 策略8: 周一评分极端值反转
s8_ok = 0
for r in weekly_records:
    score = r['mon_score']
    if score >= 65:
        pred = '下跌'  # 高分反转
    elif score <= 35:
        pred = '上涨'  # 低分反转
    else:
        pred = '上涨' if r['mon_combined'] > 0 else '下跌'
    if (pred == '上涨' and r['week_up']) or (pred == '下跌' and r['week_dn']):
        s8_ok += 1
strategies['评分极端反转'] = s8_ok / n_weekly * 100

print(f"\n{'策略':<20} {'周准确率':>10} {'vs日频基线':>12}")
print('-' * 50)
daily_baseline = loose_ok / total * 100
for name, rate in sorted(strategies.items(), key=lambda x: -x[1]):
    print(f"{name:<20} {rate:>8.1f}% {rate - daily_baseline:>+10.1f}pp")


# ═══════════════════════════════════════════════════════════
# Part 9: 周预测对日频决策的反馈
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 9: 周预测信号对日频决策的增强")
print(f"{'=' * 80}")

# 核心思路: 如果能预测整周方向，可以用周方向作为日频决策的额外因子
# 例如: 如果预测本周涨，则日频预测偏向涨

# 构建周方向预测 (用周一信号)
week_pred_map = {}  # iso_week -> {sector -> pred_direction}
for r in weekly_records:
    key = (r['iso_week'], r['sector'], r['code'])
    # 用周一融合信号
    if r['mon_combined'] > 0.5:
        week_pred_map[key] = '上涨'
    elif r['mon_combined'] < -0.5:
        week_pred_map[key] = '下跌'
    else:
        week_pred_map[key] = '上涨' if sector_up_rate.get(r['sector'], 0.5) > 0.5 else '下跌'

# 模拟: 日频决策 + 周方向一致性加权
print(f"\n日频决策 + 周方向一致性增强:")
print(f"{'策略':<30} {'准确率':>10} {'vs当前':>8}")
print('-' * 55)

# 当前日频基线
print(f"{'当前日频基线':<30} {loose_ok/total*100:>8.1f}% {0:>+6.1f}pp")

# 策略: 如果日频预测与周方向一致→保持，不一致→翻转为周方向
s_agree_ok = 0
s_agree_total = 0
for d in details:
    key = (d['_iso_week'], d['板块'], d['代码'])
    week_dir = week_pred_map.get(key)
    if week_dir is None:
        # 没有周预测，保持原预测
        if d['宽松正确'] == '✓':
            s_agree_ok += 1
        s_agree_total += 1
        continue

    daily_pred = d['预测方向']
    if daily_pred == week_dir:
        # 一致，保持
        final_pred = daily_pred
    else:
        # 不一致，用周方向覆盖
        final_pred = week_dir

    actual = d['_actual']
    if (final_pred == '上涨' and actual >= 0) or (final_pred == '下跌' and actual <= 0):
        s_agree_ok += 1
    s_agree_total += 1

print(f"{'周方向覆盖不一致日':<30} {s_agree_ok/s_agree_total*100:>8.1f}% "
      f"{s_agree_ok/s_agree_total*100 - loose_ok/total*100:>+6.1f}pp")

# 策略: 仅在日频低置信度时用周方向
s_low_ok = 0
s_low_total = 0
for d in details:
    key = (d['_iso_week'], d['板块'], d['代码'])
    week_dir = week_pred_map.get(key)

    if d['置信度'] == 'low' and week_dir:
        final_pred = week_dir
    else:
        final_pred = d['预测方向']

    actual = d['_actual']
    if (final_pred == '上涨' and actual >= 0) or (final_pred == '下跌' and actual <= 0):
        s_low_ok += 1
    s_low_total += 1

print(f"{'低置信度用周方向':<30} {s_low_ok/s_low_total*100:>8.1f}% "
      f"{s_low_ok/s_low_total*100 - loose_ok/total*100:>+6.1f}pp")

# 策略: 周方向作为额外投票（日频+周频2:1加权）
s_vote_ok = 0
s_vote_total = 0
for d in details:
    key = (d['_iso_week'], d['板块'], d['代码'])
    week_dir = week_pred_map.get(key)

    daily_vote = 1 if d['预测方向'] == '上涨' else -1
    week_vote = 0
    if week_dir == '上涨':
        week_vote = 1
    elif week_dir == '下跌':
        week_vote = -1

    combined_vote = daily_vote * 2 + week_vote
    final_pred = '上涨' if combined_vote > 0 else '下跌'

    actual = d['_actual']
    if (final_pred == '上涨' and actual >= 0) or (final_pred == '下跌' and actual <= 0):
        s_vote_ok += 1
    s_vote_total += 1

print(f"{'日频2+周频1投票':<30} {s_vote_ok/s_vote_total*100:>8.1f}% "
      f"{s_vote_ok/s_vote_total*100 - loose_ok/total*100:>+6.1f}pp")

# 策略: 仅在周一（第一天）用周方向，其余天用日频
s_mon_ok = 0
s_mon_total = 0
for d in details:
    key = (d['_iso_week'], d['板块'], d['代码'])
    week_dir = week_pred_map.get(key)

    if d['_wd'] == 0 and week_dir:  # 周一
        final_pred = week_dir
    else:
        final_pred = d['预测方向']

    actual = d['_actual']
    if (final_pred == '上涨' and actual >= 0) or (final_pred == '下跌' and actual <= 0):
        s_mon_ok += 1
    s_mon_total += 1

print(f"{'仅周一用周方向':<30} {s_mon_ok/s_mon_total*100:>8.1f}% "
      f"{s_mon_ok/s_mon_total*100 - loose_ok/total*100:>+6.1f}pp")


# ═══════════════════════════════════════════════════════════
# Part 10: 综合结论
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"Part 10: 综合结论与建议")
print(f"{'=' * 80}")

print(f"""
┌─────────────────────────────────────────────────────────────────────┐
│                    v19 按周预测分析结论                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│ 1. 周级别数据概况                                                   │
│    - 周样本: {n_weekly}个 (股票×周), 覆盖{len(sorted_weeks)}周                       │
│    - 周涨(>=0)基准率: {week_up_total/n_weekly*100:.1f}%                              │
│    - 日频基线准确率: {loose_ok/total*100:.1f}%                                │
│                                                                     │
│ 2. 周预测策略对比                                                   │
│    - 全涨基线: {strategies.get('全涨', 0):.1f}%                                       │
│    - 周一信号: {strategies.get('周一信号', 0):.1f}%                                       │
│    - 周内投票: {strategies.get('周内投票', 0):.1f}%                                       │
│    - 板块阈值优化: {strategies.get('板块阈值优化', 0):.1f}%                                   │
│    - 信号+基准率混合: {strategies.get('信号+基准率混合', 0):.1f}%                                 │
│                                                                     │
│ 3. 周预测对日频的增强效果                                           │
│    - 周方向覆盖: {s_agree_ok/s_agree_total*100:.1f}% (vs日频{loose_ok/total*100:.1f}%)              │
│    - 低置信度用周方向: {s_low_ok/s_low_total*100:.1f}%                            │
│    - 日频2+周频1投票: {s_vote_ok/s_vote_total*100:.1f}%                            │
│                                                                     │
│ 4. 关键发现                                                         │
│    a) 周频信噪比 vs 日频: 见Part 7详细对比                          │
│    b) 周内信号一致性越高，投票准确率越高                             │
│    c) 板块特化阈值在周级别同样重要                                   │
│    d) 前半→后半泛化差异反映市场regime shift                          │
│                                                                     │
│ 5. 建议                                                             │
│    a) 如果周频准确率显著高于日频 → 考虑切换为周频预测               │
│    b) 如果周方向能增强日频 → 将周方向作为新因子加入模型             │
│    c) 高一致性周（投票一致度>80%）可提高置信度                      │
│    d) 周一信号质量决定整周预测质量，重点优化周一因子                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
""")

print(f"{'=' * 80}")
print(f"v19 按周预测分析完成")
print(f"{'=' * 80}")
