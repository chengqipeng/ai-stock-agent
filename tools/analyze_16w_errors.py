#!/usr/bin/env python3
"""分析50只/16周回测中DeepSeek错误案例的特征"""
import json
import sys
import random
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from day_week_predicted.backtest.deepseek_50stocks_16weeks_backtest import (
    _select_stocks, _load_kline_data, _extract_features,
    _next_iso_week,
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

errors = []
corrects = []

for wr in data['weekly_results']:
    week = wr['week']
    parts = week.split('→')
    pred_w = int(parts[0].replace('W', ''))

    matched = None
    for py, pw in test_weeks:
        if pw == pred_w:
            matched = (py, pw)
            break
    if not matched:
        continue

    pred_y, pred_w = matched

    for s in wr['stocks']:
        dr = s.get('deepseek')
        if not dr or dr.get('direction') != 'DOWN':
            continue

        code = s['code']
        actual = s['actual']
        feat = _extract_features(code, code_name.get(code, ''), klines, pred_y, pred_w)
        if not feat:
            continue

        rec = {
            'week': week, 'code': code, 'name': code_name.get(code, ''),
            'actual': actual,
            'this_chg': feat['this_week_chg'],
            'mkt_chg': feat['market_chg'],
            'prev_chg': feat.get('_prev_week_chg'),
            'pos60': feat.get('_price_pos_60'),
            'vol_ratio': feat.get('vol_ratio'),
            'last_day': feat.get('last_day_chg', 0),
            'consec_up': feat.get('consec_up', 0),
            'mkt_prev': feat.get('_market_prev_week_chg'),
        }

        if actual >= 0:
            errors.append(rec)
        else:
            corrects.append(rec)

print("=== 错误案例（预测DOWN但实际涨了）===")
fmt = "{:12s} {:12s} {:8s} {:>7s} {:>7s} {:>7s} {:>7s} {:>7s} {:>6s} {:>7s} {:>4s} {:>7s}"
print(fmt.format('周', '代码', '名称', '实际', '本周涨', '大盘', '前周涨', '60日位', '量比', '尾日', '连涨', '前周盘'))
print("-" * 110)

for r in errors:
    pp = "{:.2f}".format(r['pos60']) if r['pos60'] is not None else "N/A"
    vr = "{:.2f}".format(r['vol_ratio']) if r['vol_ratio'] is not None else "N/A"
    pv = "{:+.1f}".format(r['prev_chg']) if r['prev_chg'] is not None else "N/A"
    mp = "{:+.1f}".format(r['mkt_prev']) if r['mkt_prev'] is not None else "N/A"
    print(fmt.format(
        r['week'], r['code'], r['name'],
        "{:+.2f}".format(r['actual']),
        "{:+.2f}".format(r['this_chg']),
        "{:+.2f}".format(r['mkt_chg']),
        pv, pp, vr,
        "{:+.2f}".format(r['last_day']),
        str(r['consec_up']),
        mp
    ))

print("\n总计: {} 个错误, {} 个正确".format(len(errors), len(corrects)))
print("准确率: {}/{} = {:.1f}%".format(
    len(corrects), len(corrects)+len(errors),
    len(corrects)/(len(corrects)+len(errors))*100))

# 统计分析
print("\n=== 特征对比（错误 vs 正确）===")

def avg(lst):
    valid = [x for x in lst if x is not None]
    return sum(valid)/len(valid) if valid else 0

def median(lst):
    valid = sorted([x for x in lst if x is not None])
    if not valid:
        return 0
    n = len(valid)
    return valid[n//2]

for label, group in [("错误", errors), ("正确", corrects)]:
    print("\n--- {} ({}) ---".format(label, len(group)))
    print("  本周涨幅: avg={:.1f}% median={:.1f}%".format(
        avg([r['this_chg'] for r in group]),
        median([r['this_chg'] for r in group])))
    print("  大盘涨跌: avg={:.1f}% median={:.1f}%".format(
        avg([r['mkt_chg'] for r in group]),
        median([r['mkt_chg'] for r in group])))
    print("  60日位置: avg={:.2f} median={:.2f}".format(
        avg([r['pos60'] for r in group]),
        median([r['pos60'] for r in group])))
    print("  量比:     avg={:.2f} median={:.2f}".format(
        avg([r['vol_ratio'] for r in group]),
        median([r['vol_ratio'] for r in group])))
    print("  尾日涨跌: avg={:.1f}% median={:.1f}%".format(
        avg([r['last_day'] for r in group]),
        median([r['last_day'] for r in group])))
    print("  连涨天数: avg={:.1f} median={:.1f}".format(
        avg([r['consec_up'] for r in group]),
        median([r['consec_up'] for r in group])))
    print("  前周涨跌: avg={:.1f}% median={:.1f}%".format(
        avg([r['prev_chg'] for r in group]),
        median([r['prev_chg'] for r in group])))
    print("  前周大盘: avg={:.1f}% median={:.1f}%".format(
        avg([r['mkt_prev'] for r in group]),
        median([r['mkt_prev'] for r in group])))

# 逐条件过滤测试
print("\n=== 过滤条件测试（在现有51条DOWN预测上）===")
all_preds = errors + corrects

conditions = [
    ("涨≥10%", lambda r: r['this_chg'] >= 10),
    ("涨≥12%", lambda r: r['this_chg'] >= 12),
    ("涨≥15%", lambda r: r['this_chg'] >= 15),
    ("大盘<0%", lambda r: r['mkt_chg'] < 0),
    ("大盘<-0.5%", lambda r: r['mkt_chg'] < -0.5),
    ("大盘<-1%", lambda r: r['mkt_chg'] < -1),
    ("60日位>0.8", lambda r: r['pos60'] is not None and r['pos60'] > 0.8),
    ("60日位>0.7", lambda r: r['pos60'] is not None and r['pos60'] > 0.7),
    ("60日位<0.8", lambda r: r['pos60'] is not None and r['pos60'] < 0.8),
    ("量比>1.5", lambda r: r['vol_ratio'] is not None and r['vol_ratio'] > 1.5),
    ("量比>1.2", lambda r: r['vol_ratio'] is not None and r['vol_ratio'] > 1.2),
    ("尾日<0%", lambda r: r['last_day'] < 0),
    ("尾日<-1%", lambda r: r['last_day'] < -1),
    ("尾日<-2%", lambda r: r['last_day'] < -2),
    ("前周<0%", lambda r: r['prev_chg'] is not None and r['prev_chg'] < 0),
    ("前周<-2%", lambda r: r['prev_chg'] is not None and r['prev_chg'] < -2),
    ("连涨<3", lambda r: r['consec_up'] < 3),
    ("连涨<2", lambda r: r['consec_up'] < 2),
    ("前周大盘<0%", lambda r: r['mkt_prev'] is not None and r['mkt_prev'] < 0),
    # 组合条件
    ("涨≥10% + 大盘<0%", lambda r: r['this_chg'] >= 10 and r['mkt_chg'] < 0),
    ("涨≥10% + 60日位>0.8", lambda r: r['this_chg'] >= 10 and r['pos60'] is not None and r['pos60'] > 0.8),
    ("涨≥8% + 尾日<0%", lambda r: r['this_chg'] >= 8 and r['last_day'] < 0),
    ("涨≥8% + 尾日<-2%", lambda r: r['this_chg'] >= 8 and r['last_day'] < -2),
    ("涨≥8% + 大盘<-1%", lambda r: r['this_chg'] >= 8 and r['mkt_chg'] < -1),
    ("涨≥8% + 前周<0%", lambda r: r['this_chg'] >= 8 and r['prev_chg'] is not None and r['prev_chg'] < 0),
    ("涨≥8% + 量比>1.5", lambda r: r['this_chg'] >= 8 and r['vol_ratio'] is not None and r['vol_ratio'] > 1.5),
    ("涨≥8% + 60日位>0.7", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] > 0.7),
    ("涨≥8% + 连涨<2", lambda r: r['this_chg'] >= 8 and r['consec_up'] < 2),
    ("涨≥8% + 60日位<0.8", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] < 0.8),
    ("涨≥8% + 60日位[0.5,0.8]", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and 0.5 <= r['pos60'] <= 0.8),
    ("涨≥10% + 尾日<0%", lambda r: r['this_chg'] >= 10 and r['last_day'] < 0),
    ("涨≥10% + 前周<0%", lambda r: r['this_chg'] >= 10 and r['prev_chg'] is not None and r['prev_chg'] < 0),
    ("涨≥10% + 连涨<2", lambda r: r['this_chg'] >= 10 and r['consec_up'] < 2),
    ("涨≥8% + 大盘<0% + 尾日<0%", lambda r: r['this_chg'] >= 8 and r['mkt_chg'] < 0 and r['last_day'] < 0),
    ("涨≥8% + 前周<0% + 尾日<0%", lambda r: r['this_chg'] >= 8 and r['prev_chg'] is not None and r['prev_chg'] < 0 and r['last_day'] < 0),
]

for desc, cond in conditions:
    subset = [r for r in all_preds if cond(r)]
    if len(subset) >= 3:
        c = sum(1 for r in subset if r['actual'] < 0)
        t = len(subset)
        acc = c / t * 100
        marker = " ★★" if acc >= 80 and t >= 5 else " ★" if acc >= 75 else ""
        print("  {:40s} → {:.1f}% ({}/{}){}".format(desc, acc, c, t, marker))
