import json
from collections import defaultdict

with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json', 'r') as f:
    data = json.load(f)

results = data.get('逐日详情', [])

def analyze_sector_tier(sector, tier):
    items = [r for r in results if r['板块'] == sector and r.get('置信度') == tier]
    if not items:
        print(f'  No data for {sector} {tier}')
        return
    
    up_items = [r for r in items if r['预测方向'] == '上涨']
    dn_items = [r for r in items if r['预测方向'] == '下跌']
    
    up_ok = sum(1 for r in up_items if r['宽松正确'] == '✓')
    dn_ok = sum(1 for r in dn_items if r['宽松正确'] == '✓')
    
    if up_items:
        print(f'  预测上涨: {up_ok}/{len(up_items)} ({up_ok/len(up_items)*100:.1f}%)')
    if dn_items:
        print(f'  预测下跌: {dn_ok}/{len(dn_items)} ({dn_ok/len(dn_items)*100:.1f}%)')
    
    # What if all predicted one direction?
    all_up = sum(1 for r in items if float(r['实际涨跌'].rstrip('%')) >= 0)
    all_dn = sum(1 for r in items if float(r['实际涨跌'].rstrip('%')) <= 0)
    print(f'  全涨baseline: {all_up}/{len(items)} ({all_up/len(items)*100:.1f}%)')
    print(f'  全跌baseline: {all_dn}/{len(items)} ({all_dn/len(items)*100:.1f}%)')
    
    # Combined signal direction analysis
    pos_combined = [r for r in items if r['融合信号'] > 0]
    neg_combined = [r for r in items if r['融合信号'] < 0]
    if pos_combined:
        pos_up = sum(1 for r in pos_combined if float(r['实际涨跌'].rstrip('%')) >= 0)
        print(f'  combined>0时actual>=0: {pos_up}/{len(pos_combined)} ({pos_up/len(pos_combined)*100:.1f}%)')
    if neg_combined:
        neg_dn = sum(1 for r in neg_combined if float(r['实际涨跌'].rstrip('%')) <= 0)
        print(f'  combined<0时actual<=0: {neg_dn}/{len(neg_combined)} ({neg_dn/len(neg_combined)*100:.1f}%)')

# Analyze weak spots
weak_spots = [
    ('化工', 'high'),
    ('化工', 'low'),
    ('有色金属', 'high'),
    ('有色金属', 'medium'),
    ('医药', 'medium'),
    ('医药', 'low'),
    ('科技', 'medium'),
    ('科技', 'low'),
    ('新能源', 'low'),
    ('新能源', 'medium'),
    ('汽车', 'low'),
    ('汽车', 'medium'),
    ('制造', 'low'),
    ('制造', 'medium'),
]

for sector, tier in weak_spots:
    items = [r for r in results if r['板块'] == sector and r.get('置信度') == tier]
    if not items:
        continue
    ok = sum(1 for r in items if r['宽松正确'] == '✓')
    print(f'\n=== {sector} {tier} ({ok}/{len(items)} = {ok/len(items)*100:.1f}%) ===')
    analyze_sector_tier(sector, tier)

# Overall: what if we flip predictions for weak tiers?
print('\n\n=== SIMULATION: Flip weak tier predictions ===')
total_ok = sum(1 for r in results if r['宽松正确'] == '✓')
print(f'Current: {total_ok}/{len(results)} ({total_ok/len(results)*100:.1f}%)')

# Simulate: for each sector/tier, check if flipping helps
for sector, tier in weak_spots:
    items = [r for r in results if r['板块'] == sector and r.get('置信度') == tier]
    if not items:
        continue
    current_ok = sum(1 for r in items if r['宽松正确'] == '✓')
    # If we flipped all predictions
    flipped_ok = 0
    for r in items:
        actual = float(r['实际涨跌'].rstrip('%'))
        pred = r['预测方向']
        if pred == '上涨':
            flipped_ok += 1 if actual <= 0 else 0
        else:
            flipped_ok += 1 if actual >= 0 else 0
    delta = flipped_ok - current_ok
    if delta > 0:
        print(f'  {sector} {tier}: flip would gain +{delta} ({current_ok}->{flipped_ok})')
    elif delta < 0:
        print(f'  {sector} {tier}: flip would lose {delta} ({current_ok}->{flipped_ok})')
