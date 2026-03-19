#!/usr/bin/env python3
"""在51条DOWN预测上穷举过滤组合，找到80%+准确率的方案"""
import json
import sys
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent.parent))

from day_week_predicted.backtest.deepseek_50stocks_16weeks_backtest import (
    _select_stocks, _load_kline_data, _extract_features, _next_iso_week,
)

data = json.load(open('data_results/four_way_50stocks_16w_result.json'))
stock_list = _select_stocks()
klines, latest_date = _load_kline_data(stock_list)
dt_latest = datetime.strptime(latest_date, '%Y-%m-%d')
current_iso = dt_latest.isocalendar()

test_weeks = []
for offset in range(2, 18):
    y, w = current_iso[0], current_iso[1] - offset
    while w <= 0:
        y -= 1
        dec28 = datetime(y, 12, 28)
        max_w = dec28.isocalendar()[1]
        w += max_w
    test_weeks.append((y, w))

code_name = {c: n for c, n in stock_list}
all_preds = []

for wr in data['weekly_results']:
    week = wr['week']
    pred_w = int(week.split('→')[0].replace('W', ''))
    matched = None
    for py, pw in test_weeks:
        if pw == pred_w:
            matched = (py, pw)
            break
    if not matched:
        continue

    for s in wr['stocks']:
        dr = s.get('deepseek')
        if not dr or dr.get('direction') != 'DOWN':
            continue
        code = s['code']
        feat = _extract_features(code, code_name.get(code, ''), klines, matched[0], matched[1])
        if not feat:
            continue
        all_preds.append({
            'week': week, 'code': code,
            'actual': s['actual'], 'correct': s['actual'] < 0,
            'this_chg': feat['this_week_chg'],
            'mkt_chg': feat['market_chg'],
            'prev_chg': feat.get('_prev_week_chg'),
            'pos60': feat.get('_price_pos_60'),
            'vol_ratio': feat.get('vol_ratio'),
            'last_day': feat.get('last_day_chg', 0),
            'consec_up': feat.get('consec_up', 0),
            'mkt_prev': feat.get('_market_prev_week_chg'),
        })

print("总DOWN预测: {}, 正确: {}, 准确率: {:.1f}%".format(
    len(all_preds),
    sum(1 for r in all_preds if r['correct']),
    sum(1 for r in all_preds if r['correct']) / len(all_preds) * 100
))

# 穷举：涨幅阈值 × 尾日阈值 × 60日位阈值 × 前周阈值 × 大盘阈值
print("\n=== 穷举过滤组合（准确率≥80%且样本≥5）===")
results = []

chg_thresholds = [8, 10, 12, 15]
tail_upper = [999, 5, 3, 2, 1, 0]  # 尾日 < X
pos_lower = [0, 0.5, 0.6, 0.7, 0.8]  # 60日位 >= X
prev_upper = [999, 0, -1, -2]  # 前周 < X
mkt_upper = [999, 0, -0.5, -1, -1.5]  # 大盘 < X
vol_lower = [0, 1.0, 1.2, 1.5]  # 量比 >= X

for chg_t in chg_thresholds:
    for tail_t in tail_upper:
        for pos_t in pos_lower:
            for prev_t in prev_upper:
                for mkt_t in mkt_upper:
                    for vol_t in vol_lower:
                        subset = []
                        for r in all_preds:
                            if r['this_chg'] < chg_t:
                                continue
                            if tail_t < 999 and r['last_day'] >= tail_t:
                                continue
                            if pos_t > 0 and (r['pos60'] is None or r['pos60'] < pos_t):
                                continue
                            if prev_t < 999 and (r['prev_chg'] is None or r['prev_chg'] >= prev_t):
                                continue
                            if mkt_t < 999 and r['mkt_chg'] >= mkt_t:
                                continue
                            if vol_t > 0 and (r['vol_ratio'] is None or r['vol_ratio'] < vol_t):
                                continue
                            subset.append(r)

                        if len(subset) >= 5:
                            c = sum(1 for r in subset if r['correct'])
                            t = len(subset)
                            acc = c / t * 100
                            if acc >= 80:
                                conds = ["涨>={:.0f}%".format(chg_t)]
                                if tail_t < 999:
                                    conds.append("尾日<{:.0f}%".format(tail_t))
                                if pos_t > 0:
                                    conds.append("60日位>={:.0f}%".format(pos_t*100))
                                if prev_t < 999:
                                    conds.append("前周<{:.0f}%".format(prev_t))
                                if mkt_t < 999:
                                    conds.append("大盘<{:.1f}%".format(mkt_t))
                                if vol_t > 0:
                                    conds.append("量比>={:.1f}".format(vol_t))
                                desc = " + ".join(conds)
                                results.append((desc, c, t, acc))

# 去重并排序
seen = set()
unique = []
for desc, c, t, acc in sorted(results, key=lambda x: (-x[3], -x[2])):
    if desc not in seen:
        seen.add(desc)
        unique.append((desc, c, t, acc))

for desc, c, t, acc in unique[:60]:
    print("  {:55s} → {:.1f}% ({}/{})".format(desc, acc, c, t))

# 特别关注：简单条件，样本≥8
print("\n=== 简单高效条件（样本≥8，准确率≥75%）===")
simple = [(d, c, t, a) for d, c, t, a in unique if t >= 8 and a >= 75]
for desc, c, t, acc in simple[:30]:
    print("  {:55s} → {:.1f}% ({}/{})".format(desc, acc, c, t))
