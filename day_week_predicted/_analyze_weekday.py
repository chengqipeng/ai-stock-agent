"""Analyze day-of-week effect in the backtest data."""
import json
from collections import defaultdict
from datetime import datetime

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json', 'r') as f:
    data = json.load(f)

results = data.get('逐日详情', [])

# Analyze by weekday of prediction date (next_date)
weekday_stats = defaultdict(lambda: {'up': 0, 'dn': 0, 'n': 0, 'ok': 0})
for r in results:
    next_date = r['预测日']
    dt = datetime.strptime(next_date, '%Y-%m-%d')
    wd = dt.weekday()  # 0=Mon, 4=Fri
    actual = float(r['实际涨跌'].rstrip('%'))
    weekday_stats[wd]['n'] += 1
    if actual >= 0:
        weekday_stats[wd]['up'] += 1
    if actual <= 0:
        weekday_stats[wd]['dn'] += 1
    if r['宽松正确'] == '✓':
        weekday_stats[wd]['ok'] += 1

print("=== Day-of-week effect (prediction target day) ===")
day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
for wd in range(5):
    d = weekday_stats[wd]
    if d['n'] > 0:
        up_rate = d['up'] / d['n'] * 100
        ok_rate = d['ok'] / d['n'] * 100
        print(f"  {day_names[wd]}: n={d['n']}, actual>=0: {up_rate:.1f}%, current_accuracy: {ok_rate:.1f}%")

# Analyze by score_date weekday
print("\n=== Day-of-week effect (score date) ===")
score_wd_stats = defaultdict(lambda: {'up': 0, 'dn': 0, 'n': 0})
for r in results:
    score_date = r['评分日']
    dt = datetime.strptime(score_date, '%Y-%m-%d')
    wd = dt.weekday()
    actual = float(r['实际涨跌'].rstrip('%'))
    score_wd_stats[wd]['n'] += 1
    if actual >= 0:
        score_wd_stats[wd]['up'] += 1
    if actual <= 0:
        score_wd_stats[wd]['dn'] += 1

for wd in range(5):
    d = score_wd_stats[wd]
    if d['n'] > 0:
        up_rate = d['up'] / d['n'] * 100
        print(f"  {day_names[wd]}: n={d['n']}, next_day_actual>=0: {up_rate:.1f}%")

# Analyze by total_score ranges more granularly
print("\n=== Score range analysis ===")
score_ranges = [(0, 30), (30, 35), (35, 40), (40, 45), (45, 48), (48, 52), (52, 55), (55, 60), (60, 65), (65, 100)]
for lo, hi in score_ranges:
    items = [r for r in results if lo <= r['评分'] < hi]
    if not items:
        continue
    up_ok = sum(1 for r in items if float(r['实际涨跌'].rstrip('%')) >= 0)
    dn_ok = sum(1 for r in items if float(r['实际涨跌'].rstrip('%')) <= 0)
    curr_ok = sum(1 for r in items if r['宽松正确'] == '✓')
    print(f"  score [{lo}-{hi}): n={len(items)}, actual>=0: {up_ok/len(items)*100:.1f}%, actual<=0: {dn_ok/len(items)*100:.1f}%, current: {curr_ok/len(items)*100:.1f}%")

# Analyze by actual change magnitude
print("\n=== Actual change magnitude analysis ===")
flat = [r for r in results if abs(float(r['实际涨跌'].rstrip('%'))) < 0.3]
small_up = [r for r in results if 0 <= float(r['实际涨跌'].rstrip('%')) < 0.3]
small_dn = [r for r in results if -0.3 < float(r['实际涨跌'].rstrip('%')) < 0]
print(f"  Flat (|actual|<0.3%): {len(flat)} samples ({len(flat)/len(results)*100:.1f}%)")
print(f"  Small up (0-0.3%): {len(small_up)} samples")
print(f"  Small down (-0.3-0%): {len(small_dn)} samples")

# For flat samples, what's the prediction accuracy?
flat_ok = sum(1 for r in flat if r['宽松正确'] == '✓')
print(f"  Flat accuracy: {flat_ok}/{len(flat)} ({flat_ok/len(flat)*100:.1f}%)")
flat_pred_up = [r for r in flat if r['预测方向'] == '上涨']
flat_pred_dn = [r for r in flat if r['预测方向'] == '下跌']
if flat_pred_up:
    ok = sum(1 for r in flat_pred_up if r['宽松正确'] == '✓')
    print(f"  Flat+pred_up: {ok}/{len(flat_pred_up)} ({ok/len(flat_pred_up)*100:.1f}%)")
if flat_pred_dn:
    ok = sum(1 for r in flat_pred_dn if r['宽松正确'] == '✓')
    print(f"  Flat+pred_dn: {ok}/{len(flat_pred_dn)} ({ok/len(flat_pred_dn)*100:.1f}%)")
