#!/usr/bin/env python3
"""分析16周回测数据，设计新的预过滤路径"""
import json, sys
from pathlib import Path
from collections import defaultdict
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
    pred_w = int(week.split('\u2192')[0].replace('W', ''))
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
            'week': week, 'code': code, 'name': s['name'],
            'actual': s['actual'], 'correct': s['actual'] < 0,
            'this_chg': feat['this_week_chg'],
            'mkt_chg': feat['market_chg'],
            'prev_chg': feat.get('_prev_week_chg'),
            'pos60': feat.get('_price_pos_60'),
            'vol_ratio': feat.get('vol_ratio'),
            'last_day': feat.get('last_day_chg', 0),
            'consec_up': feat.get('consec_up', 0),
            'justification': dr.get('justification', ''),
        })

print(f"Total DOWN: {len(all_preds)}, Correct: {sum(1 for r in all_preds if r['correct'])}")

# Identify D2 predictions (justification mentions D2 or 连涨)
d2_preds = [r for r in all_preds if 'D2' in r['justification'] or '\u8fde\u6da8' in r['justification']]
d1_preds = [r for r in all_preds if r not in d2_preds]
print(f"\nD1 preds: {len(d1_preds)}, correct: {sum(1 for r in d1_preds if r['correct'])}, "
      f"acc: {sum(1 for r in d1_preds if r['correct'])/len(d1_preds)*100:.1f}%")
print(f"D2 preds: {len(d2_preds)}, correct: {sum(1 for r in d2_preds if r['correct'])}, "
      f"acc: {sum(1 for r in d2_preds if r['correct'])/len(d2_preds)*100:.1f}%" if d2_preds else "D2: 0")

# Current V14 path analysis
for r in all_preds:
    mkt, chg = r['mkt_chg'], r['this_chg']
    pos, prev = r['pos60'], r['prev_chg']
    vol, last, cu = r['vol_ratio'], r['last_day'], r['consec_up']
    path = 'NONE'
    if mkt < -1.5 and chg >= 5.0:
        path = 'A'
    elif pos is not None and 0.5 <= pos <= 0.8 and chg >= 5.0:
        path = 'B'
    elif prev is not None and -3.0 <= prev < 0 and chg >= 5.0:
        path = 'C'
    elif 5.0 <= chg < 8.0 and last > 3.0:
        path = 'D'
    elif vol is not None and 0.8 <= vol <= 1.2 and pos is not None and 0.5 <= pos <= 0.8:
        path = 'E'
    r['path'] = path

path_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
for r in all_preds:
    path_stats[r['path']]['total'] += 1
    if r['correct']:
        path_stats[r['path']]['correct'] += 1

print("\nCurrent V14 path accuracy:")
for path, s in sorted(path_stats.items()):
    acc = s['correct']/s['total']*100 if s['total'] > 0 else 0
    print(f"  {path}: {acc:.1f}% ({s['correct']}/{s['total']})")

# Test new filter combinations
print("\n=== New filter tests (post-LLM, on 51 DOWN predictions) ===")
filters = [
    ("chg>=8 + pos60>=0.7 + vol>=1.0", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7 and r['vol_ratio'] is not None and r['vol_ratio'] >= 1.0),
    ("chg>=8 + pos60>=0.7 + mkt<0", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7 and r['mkt_chg'] < 0),
    ("chg>=10 + pos60>=0.7", lambda r: r['this_chg'] >= 10 and r['pos60'] is not None and r['pos60'] >= 0.7),
    ("chg>=10 + pos60>=0.8", lambda r: r['this_chg'] >= 10 and r['pos60'] is not None and r['pos60'] >= 0.8),
    ("chg>=8 + pos60>=0.8", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.8),
    ("chg>=8 + mkt<-1", lambda r: r['this_chg'] >= 8 and r['mkt_chg'] < -1),
    ("chg>=8 + mkt<-1 + pos60>=0.5", lambda r: r['this_chg'] >= 8 and r['mkt_chg'] < -1 and r['pos60'] is not None and r['pos60'] >= 0.5),
    ("chg>=15", lambda r: r['this_chg'] >= 15),
    ("chg>=12 + pos60>=0.6", lambda r: r['this_chg'] >= 12 and r['pos60'] is not None and r['pos60'] >= 0.6),
    ("chg>=8 + pos60>=0.7 + prev<0", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7 and r['prev_chg'] is not None and r['prev_chg'] < 0),
    ("chg>=8 + pos60>=0.6 + mkt<0", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.6 and r['mkt_chg'] < 0),
    ("chg>=8 + pos60>=0.5 + mkt<-1.5", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.5 and r['mkt_chg'] < -1.5),
    ("chg>=10 + pos60>=0.6", lambda r: r['this_chg'] >= 10 and r['pos60'] is not None and r['pos60'] >= 0.6),
    ("chg>=8 + pos60>=0.7", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7),
    ("D1_only (chg>=8)", lambda r: r['this_chg'] >= 8),
    ("D1_only + NOT_D2", lambda r: r['this_chg'] >= 8 and r not in d2_preds),
    ("chg>=8 + pos60>=0.7 + vol>=1.0 + mkt<0", lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7 and r['vol_ratio'] is not None and r['vol_ratio'] >= 1.0 and r['mkt_chg'] < 0),
    ("chg>=10 + pos60>=0.7 + vol>=1.0", lambda r: r['this_chg'] >= 10 and r['pos60'] is not None and r['pos60'] >= 0.7 and r['vol_ratio'] is not None and r['vol_ratio'] >= 1.0),
]

for name, fn in filters:
    passed = [r for r in all_preds if fn(r)]
    if not passed:
        print(f"  {name}: NO MATCHES")
        continue
    c = sum(1 for r in passed if r['correct'])
    t = len(passed)
    acc = c/t*100
    print(f"  {name}: {acc:.1f}% ({c}/{t})")

# Error analysis
print("\n=== Errors ===")
for r in all_preds:
    if not r['correct']:
        print(f"  {r['week']} {r['code']} {r['name']}: chg={r['this_chg']:+.1f}% mkt={r['mkt_chg']:+.1f}% "
              f"pos60={r['pos60']} vol={r['vol_ratio']} prev={r['prev_chg']} cu={r['consec_up']} path={r['path']} "
              f"just={r['justification'][:40]}")
