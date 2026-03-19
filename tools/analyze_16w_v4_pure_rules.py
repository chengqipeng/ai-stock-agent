#!/usr/bin/env python3
"""
纯规则方案分析：不调用LLM，直接用预过滤条件决定DOWN
在50股×16周全量数据上测试各种条件组合
"""
import json, sys, random
from pathlib import Path
from collections import defaultdict
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent.parent))

from day_week_predicted.backtest.deepseek_50stocks_16weeks_backtest import (
    _select_stocks, _load_kline_data, _extract_features,
    _next_iso_week, _check_next_week_actual,
)

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

# Collect ALL samples with features
all_samples = []
for pred_y, pred_w in test_weeks:
    for code, name in stock_list:
        feat = _extract_features(code, name, klines, pred_y, pred_w)
        actual = _check_next_week_actual(code, klines, pred_y, pred_w)
        if feat and actual is not None:
            nw_y, nw_w = _next_iso_week(pred_y, pred_w)
            all_samples.append({
                'week': f"W{pred_w}\u2192W{nw_w}",
                'code': code, 'name': name,
                'actual': actual, 'down': actual < 0,
                'this_chg': feat['this_week_chg'],
                'mkt_chg': feat['market_chg'],
                'prev_chg': feat.get('_prev_week_chg'),
                'pos60': feat.get('_price_pos_60'),
                'vol_ratio': feat.get('vol_ratio'),
                'last_day': feat.get('last_day_chg', 0),
                'consec_up': feat.get('consec_up', 0),
            })

print(f"Total samples: {len(all_samples)}")
print(f"Base rate DOWN: {sum(1 for s in all_samples if s['down'])}/{len(all_samples)} = "
      f"{sum(1 for s in all_samples if s['down'])/len(all_samples)*100:.1f}%")


def test(name, fn):
    passed = [s for s in all_samples if fn(s)]
    if not passed:
        print(f"  {name}: NO MATCHES")
        return 0, 0, 0
    c = sum(1 for s in passed if s['down'])
    t = len(passed)
    acc = c / t * 100
    cov = t / len(all_samples) * 100
    print(f"  {name}: {acc:.1f}% ({c}/{t}) cov={cov:.1f}%")
    return c, t, acc


print("\n=== Single condition tests ===")
test("chg>=8", lambda s: s['this_chg'] >= 8)
test("chg>=10", lambda s: s['this_chg'] >= 10)
test("chg>=12", lambda s: s['this_chg'] >= 12)
test("chg>=15", lambda s: s['this_chg'] >= 15)

print("\n=== chg>=8 + pos60 tests ===")
test("chg>=8 + pos>=0.7", lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7)
test("chg>=8 + pos>=0.8", lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.8)
test("chg>=8 + pos>=0.9", lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.9)

print("\n=== chg>=8 + pos + vol tests ===")
test("chg>=8 + pos>=0.7 + vol>=1.0",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0)
test("chg>=8 + pos>=0.7 + vol[1.0,2.0]",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and 1.0 <= s['vol_ratio'] <= 2.0)
test("chg>=8 + pos>=0.7 + vol[1.0,3.0]",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and 1.0 <= s['vol_ratio'] <= 3.0)
test("chg>=8 + pos>=0.8 + vol>=1.0",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.8
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0)
test("chg>=8 + pos>=0.8 + vol[1.0,2.0]",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.8
     and s['vol_ratio'] is not None and 1.0 <= s['vol_ratio'] <= 2.0)

print("\n=== chg>=8 + pos + vol + mkt tests ===")
test("chg>=8 + pos>=0.7 + vol>=1.0 + mkt>-3",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0 and s['mkt_chg'] > -3)
test("chg>=8 + pos>=0.7 + vol>=1.0 + mkt<2",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0 and s['mkt_chg'] < 2)
test("chg>=8 + pos>=0.7 + vol>=1.0 + mkt[-3,2]",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0 and -3 < s['mkt_chg'] < 2)
test("chg>=8 + pos>=0.8 + vol>=1.0 + mkt[-3,2]",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.8
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0 and -3 < s['mkt_chg'] < 2)

print("\n=== chg>=8 + pos + vol + prev tests ===")
test("chg>=8 + pos>=0.7 + vol>=1.0 + prev<3",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0
     and (s['prev_chg'] is None or s['prev_chg'] < 3))
test("chg>=8 + pos>=0.7 + vol>=1.0 + prev<0",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0
     and (s['prev_chg'] is None or s['prev_chg'] < 0))
test("chg>=8 + pos>=0.8 + vol>=1.0 + prev<3",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.8
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0
     and (s['prev_chg'] is None or s['prev_chg'] < 3))

print("\n=== 4-condition combos ===")
test("chg>=8 + pos>=0.7 + vol[1.0,3.0] + mkt[-3,2]",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and 1.0 <= s['vol_ratio'] <= 3.0
     and -3 < s['mkt_chg'] < 2)
test("chg>=8 + pos>=0.7 + vol[1.0,2.0] + mkt[-3,2]",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and 1.0 <= s['vol_ratio'] <= 2.0
     and -3 < s['mkt_chg'] < 2)
test("chg>=8 + pos>=0.7 + vol>=1.0 + mkt[-3,2] + prev<3",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0
     and -3 < s['mkt_chg'] < 2 and (s['prev_chg'] is None or s['prev_chg'] < 3))
test("chg>=8 + pos>=0.7 + vol>=1.0 + mkt[-3,2] + prev<5",
     lambda s: s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0
     and -3 < s['mkt_chg'] < 2 and (s['prev_chg'] is None or s['prev_chg'] < 5))
test("chg>=10 + pos>=0.7 + vol>=1.0 + mkt>-3",
     lambda s: s['this_chg'] >= 10 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0 and s['mkt_chg'] > -3)
test("chg>=10 + pos>=0.8 + vol>=1.0",
     lambda s: s['this_chg'] >= 10 and s['pos60'] is not None and s['pos60'] >= 0.8
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0)
test("chg>=10 + pos>=0.7 + vol[1.0,3.0]",
     lambda s: s['this_chg'] >= 10 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and 1.0 <= s['vol_ratio'] <= 3.0)

print("\n=== Best candidates detail ===")
# Show detail for the best combo
best_fn = lambda s: (s['this_chg'] >= 8 and s['pos60'] is not None and s['pos60'] >= 0.7
     and s['vol_ratio'] is not None and s['vol_ratio'] >= 1.0
     and -3 < s['mkt_chg'] < 2 and (s['prev_chg'] is None or s['prev_chg'] < 3))
passed = [s for s in all_samples if best_fn(s)]
c = sum(1 for s in passed if s['down'])
print(f"\nchg>=8 + pos>=0.7 + vol>=1.0 + mkt[-3,2] + prev<3: {c}/{len(passed)} = {c/len(passed)*100:.1f}%")
for s in passed:
    mark = "OK" if s['down'] else "ERR"
    w = s['week']
    code = s['code']
    name = s['name']
    act = s['actual']
    chg = s['this_chg']
    mkt = s['mkt_chg']
    pos = s['pos60']
    vol = s['vol_ratio']
    prev = s['prev_chg']
    print(f"  {mark} {w} {code} {name}: actual={act:+.2f}% chg={chg:+.1f}% mkt={mkt:+.1f}% pos={pos:.2f} vol={vol:.2f} prev={prev}")
