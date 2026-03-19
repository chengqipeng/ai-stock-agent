#!/usr/bin/env python3
"""分析16周回测数据，找到80%+准确率的预过滤组合"""
import json, sys
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
        })


def test(name, fn):
    passed = [r for r in all_preds if fn(r)]
    if not passed:
        print(f"{name}: NO MATCHES")
        return 0, 0
    c = sum(1 for r in passed if r['correct'])
    t = len(passed)
    print(f"{name}: {c/t*100:.1f}% ({c}/{t})")
    return c, t


def show_detail(fn):
    for r in all_preds:
        if fn(r):
            mark = "OK" if r['correct'] else "ERR"
            w = r['week']
            c = r['code']
            n = r['name']
            chg = r['this_chg']
            mkt = r['mkt_chg']
            p = r['pos60']
            v = r['vol_ratio']
            prev = r['prev_chg']
            print(f"  {mark} {w} {c} {n}: chg={chg:+.1f}% mkt={mkt:+.1f}% pos60={p} vol={v} prev={prev}")


print("=== Volume cap tests ===")
test("chg>=8 + pos70 + vol[1.0,3.0]",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and 1.0 <= r['vol_ratio'] <= 3.0)

test("chg>=8 + pos70 + vol[0.8,3.0]",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and 0.8 <= r['vol_ratio'] <= 3.0)

test("chg>=8 + pos70 + vol[1.0,5.0]",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and 1.0 <= r['vol_ratio'] <= 5.0)

test("chg>=8 + pos70 + vol[0.8,2.0]",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and 0.8 <= r['vol_ratio'] <= 2.0)

test("chg>=8 + pos70 + vol[1.0,2.0]",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and 1.0 <= r['vol_ratio'] <= 2.0)

print("\n=== Market filter tests ===")
test("chg>=8 + pos70 + vol>=1.0 + mkt>-3",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and r['vol_ratio'] >= 1.0 and r['mkt_chg'] > -3)

test("chg>=8 + pos70 + vol>=1.0 + mkt>-4",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and r['vol_ratio'] >= 1.0 and r['mkt_chg'] > -4)

print("\n=== Prev week filter tests ===")
test("chg>=8 + pos70 + vol>=1.0 + prev<5",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and r['vol_ratio'] >= 1.0
     and (r['prev_chg'] is None or r['prev_chg'] < 5))

test("chg>=8 + pos70 + vol>=1.0 + prev<3",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and r['vol_ratio'] >= 1.0
     and (r['prev_chg'] is None or r['prev_chg'] < 3))

print("\n=== Combined exclusion tests ===")
test("chg>=8 + pos70 + vol>=1.0 + NOT(mkt<-3 & vol>5)",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and r['vol_ratio'] >= 1.0
     and not (r['mkt_chg'] < -3 and r['vol_ratio'] > 5))

test("chg>=8 + pos70 + vol[1.0,5.0] + mkt>-4",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and 1.0 <= r['vol_ratio'] <= 5.0
     and r['mkt_chg'] > -4)

test("chg>=8 + pos70 + vol[1.0,3.0] + prev<5",
     lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
     and r['vol_ratio'] is not None and 1.0 <= r['vol_ratio'] <= 3.0
     and (r['prev_chg'] is None or r['prev_chg'] < 5))

print("\n=== Best candidate detail: chg>=8 + pos70 + vol[1.0,2.0] ===")
show_detail(lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
            and r['vol_ratio'] is not None and 1.0 <= r['vol_ratio'] <= 2.0)

print("\n=== Best candidate detail: chg>=8 + pos70 + vol>=1.0 + mkt>-3 ===")
show_detail(lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
            and r['vol_ratio'] is not None and r['vol_ratio'] >= 1.0 and r['mkt_chg'] > -3)

print("\n=== Best candidate detail: chg>=8 + pos70 + vol[1.0,3.0] + prev<5 ===")
show_detail(lambda r: r['this_chg'] >= 8 and r['pos60'] is not None and r['pos60'] >= 0.7
            and r['vol_ratio'] is not None and 1.0 <= r['vol_ratio'] <= 3.0
            and (r['prev_chg'] is None or r['prev_chg'] < 5))
