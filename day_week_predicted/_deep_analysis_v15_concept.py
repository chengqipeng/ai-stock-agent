#!/usr/bin/env python3
"""
v15 概念板块深度分析：验证概念板块成分股走势/资金流是否能提升预测准确率

核心问题：
  当前模型只用了7个行业板块(科技/有色金属/汽车/新能源/医药/化工/制造)的同行信号，
  但每只股票还属于多个概念板块(如"半导体概念"、"华为概念"、"新能源车"等)。
  概念板块的成分股走势可能提供额外的预测信号。

分析维度：
  1. 概念板块成分股当日涨跌比 vs 回测股票次日涨跌（方向一致率）
  2. 概念板块成分股资金流向聚合 vs 次日涨跌
  3. 概念板块信号 vs 行业板块信号的互补性（概念信号能否修正行业信号的错误）
  4. 多概念板块投票信号（一只股票属于多个概念，多数概念看涨→更可靠？）
  5. 概念板块信号在模型预测错误样本上的表现（能否修正错误？）
  6. 按行业板块分组的概念信号有效性差异
"""
import json
import logging
from collections import defaultdict
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# 加载数据
# ═══════════════════════════════════════════════════════════
print(f"{'=' * 80}")
print(f"v15 概念板块深度分析")
print(f"{'=' * 80}")

# 1. 回测结果
with open('data_results/backtest_prediction_enhanced_v9_50stocks_result.json') as f:
    bt_data = json.load(f)
details = bt_data['逐日详情']
total = len(details)

def parse_chg(s):
    return float(s.replace('%', '').replace('+', ''))

loose_ok = sum(1 for d in details if d['宽松正确'] == '✓')
print(f"当前基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"目标65%: {int(total*0.65)}/{total}, 差距: {int(total*0.65) - loose_ok}个样本")

# 2. 概念板块映射
with open('data_results/industry_analysis/stock_boards_map.json') as f:
    boards_data = json.load(f)

stock_concepts = {}  # code -> [概念1, 概念2, ...]
for code, info in boards_data['stocks'].items():
    stock_concepts[code] = info.get('concept_boards', [])

concept_stocks = boards_data['concept_boards']  # 概念名 -> [code1, code2, ...]

# 检查回测股票的概念覆盖
bt_codes = set(d['代码'] for d in details)
covered = sum(1 for c in bt_codes if c in stock_concepts and stock_concepts[c])
print(f"\n回测股票概念覆盖: {covered}/{len(bt_codes)}")

# 统计每只回测股票的概念数量
for code in sorted(bt_codes):
    concepts = stock_concepts.get(code, [])
    name = next((d['名称'] for d in details if d['代码'] == code), code)
    sector = next((d['板块'] for d in details if d['代码'] == code), '?')
    if concepts:
        print(f"  {name}[{sector}]: {len(concepts)}个概念 → {concepts[:5]}{'...' if len(concepts)>5 else ''}")


# ═══════════════════════════════════════════════════════════
# 从DB加载概念板块成分股的K线和资金流聚合数据
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"加载概念板块成分股聚合数据...")
print(f"{'=' * 80}")

from dao import get_connection

conn = get_connection(use_dict_cursor=True)
cursor = conn.cursor()

# 回测日期范围
all_dates = sorted(set(d['评分日'] for d in details))
date_min = all_dates[0]
date_max = all_dates[-1]
print(f"回测日期范围: {date_min} ~ {date_max}")

# 收集所有回测股票涉及的概念板块及其成分股
# 只取DB中有数据的成分股
relevant_concepts = set()
for code in bt_codes:
    for c in stock_concepts.get(code, []):
        relevant_concepts.add(c)
print(f"涉及概念板块: {len(relevant_concepts)}个")

# 收集所有概念板块成分股代码
all_concept_stock_codes = set()
for cname in relevant_concepts:
    for scode in concept_stocks.get(cname, []):
        all_concept_stock_codes.add(scode)
print(f"概念板块成分股总数(去重): {len(all_concept_stock_codes)}")

# 批量查询这些成分股在回测期间的K线聚合数据
# 按(概念板块, 日期)聚合涨跌比
print(f"\n查询概念板块成分股K线聚合...")

concept_daily_stats = {}  # (concept_name, date) -> {up_ratio, avg_chg, total}

for cname in relevant_concepts:
    codes = concept_stocks.get(cname, [])
    if not codes or len(codes) < 5:  # 成分股太少的概念跳过
        continue
    
    # 分批查询（避免SQL太长）
    batch_size = 200
    codes_batch = codes[:batch_size]  # 取前200只
    placeholders = ','.join(['%s'] * len(codes_batch))
    
    try:
        cursor.execute(f"""
            SELECT date,
                   COUNT(*) as total_stocks,
                   SUM(CASE WHEN change_percent > 0.3 THEN 1 ELSE 0 END) as up_count,
                   SUM(CASE WHEN change_percent < -0.3 THEN 1 ELSE 0 END) as down_count,
                   AVG(change_percent) as avg_chg
            FROM stock_kline
            WHERE stock_code IN ({placeholders})
            AND date >= %s AND date <= %s
            AND trading_volume > 0
            GROUP BY date
            ORDER BY date
        """, codes_batch + [date_min, date_max])
        
        for row in cursor.fetchall():
            dt = row['date']
            total_s = row['total_stocks']
            up_ratio = row['up_count'] / total_s if total_s > 0 else 0.5
            concept_daily_stats[(cname, dt)] = {
                'up_ratio': up_ratio,
                'avg_chg': float(row['avg_chg'] or 0),
                'total': total_s,
                'up_count': row['up_count'],
                'down_count': row['down_count'],
            }
    except Exception as e:
        logger.warning(f"查询概念{cname}失败: {e}")

print(f"概念板块日聚合数据: {len(concept_daily_stats)}条")

# 同样查询概念板块成分股的资金流聚合
print(f"查询概念板块成分股资金流聚合...")

concept_daily_fund = {}  # (concept_name, date) -> {inflow_ratio, avg_net_pct}

for cname in relevant_concepts:
    codes = concept_stocks.get(cname, [])
    if not codes or len(codes) < 5:
        continue
    
    codes_batch = codes[:200]
    placeholders = ','.join(['%s'] * len(codes_batch))
    
    try:
        cursor.execute(f"""
            SELECT date,
                   COUNT(*) as total_stocks,
                   SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) as inflow_count,
                   AVG(big_net_pct) as avg_big_net_pct
            FROM stock_fund_flow
            WHERE stock_code IN ({placeholders})
            AND date >= %s AND date <= %s
            GROUP BY date
            ORDER BY date
        """, codes_batch + [date_min, date_max])
        
        for row in cursor.fetchall():
            dt = row['date']
            total_s = row['total_stocks']
            inflow_ratio = row['inflow_count'] / total_s if total_s > 0 else 0.5
            concept_daily_fund[(cname, dt)] = {
                'inflow_ratio': inflow_ratio,
                'avg_big_net_pct': float(row['avg_big_net_pct'] or 0),
                'total': total_s,
            }
    except Exception as e:
        logger.warning(f"查询概念{cname}资金流失败: {e}")

print(f"概念板块资金流日聚合数据: {len(concept_daily_fund)}条")

cursor.close()
conn.close()


# ═══════════════════════════════════════════════════════════
# 分析1: 概念板块涨跌比 vs 次日涨跌（方向一致率）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析1: 概念板块涨跌比 vs 次日涨跌")
print(f"{'=' * 80}")

# 对每个回测样本，计算其所属概念板块的当日涨跌信号
concept_signal_results = []  # 每个样本的概念信号

for d in details:
    code = d['代码']
    score_date = d['评分日']
    actual_chg = parse_chg(d['实际涨跌'])
    actual_up = actual_chg >= 0  # 宽松模式
    pred_dir = d['预测方向']
    loose_correct = d['宽松正确'] == '✓'
    sector = d['板块']
    
    concepts = stock_concepts.get(code, [])
    if not concepts:
        concept_signal_results.append({
            'code': code, 'date': score_date, 'sector': sector,
            'actual_up': actual_up, 'actual_chg': actual_chg,
            'pred_dir': pred_dir, 'loose_correct': loose_correct,
            'concept_signals': [], 'concept_fund_signals': [],
            'concept_vote': None, 'concept_fund_vote': None,
        })
        continue
    
    concept_signals = []  # 每个概念的涨跌信号
    concept_fund_signals = []  # 每个概念的资金流信号
    
    for cname in concepts:
        stats = concept_daily_stats.get((cname, score_date))
        if stats and stats['total'] >= 5:
            # 概念板块当日涨跌比信号
            if stats['up_ratio'] > 0.6:
                sig = 1  # 概念看涨
            elif stats['up_ratio'] < 0.4:
                sig = -1  # 概念看跌
            else:
                sig = 0  # 中性
            concept_signals.append({
                'concept': cname, 'signal': sig,
                'up_ratio': stats['up_ratio'], 'avg_chg': stats['avg_chg'],
            })
        
        fund = concept_daily_fund.get((cname, score_date))
        if fund and fund['total'] >= 5:
            if fund['inflow_ratio'] > 0.6:
                fsig = 1
            elif fund['inflow_ratio'] < 0.4:
                fsig = -1
            else:
                fsig = 0
            concept_fund_signals.append({
                'concept': cname, 'signal': fsig,
                'inflow_ratio': fund['inflow_ratio'],
                'avg_big_net_pct': fund['avg_big_net_pct'],
            })
    
    # 多概念投票
    if concept_signals:
        vote_sum = sum(s['signal'] for s in concept_signals)
        vote = '看涨' if vote_sum > 0 else ('看跌' if vote_sum < 0 else '中性')
    else:
        vote = None
    
    if concept_fund_signals:
        fvote_sum = sum(s['signal'] for s in concept_fund_signals)
        fvote = '流入' if fvote_sum > 0 else ('流出' if fvote_sum < 0 else '中性')
    else:
        fvote = None
    
    concept_signal_results.append({
        'code': code, 'date': score_date, 'sector': sector,
        'actual_up': actual_up, 'actual_chg': actual_chg,
        'pred_dir': pred_dir, 'loose_correct': loose_correct,
        'concept_signals': concept_signals,
        'concept_fund_signals': concept_fund_signals,
        'concept_vote': vote, 'concept_fund_vote': fvote,
    })

# 统计概念涨跌投票 vs 次日涨跌
print(f"\n── 概念板块涨跌投票 vs 次日涨跌 ──")
vote_stats = defaultdict(lambda: {'total': 0, 'next_up': 0})
for r in concept_signal_results:
    if r['concept_vote']:
        vote_stats[r['concept_vote']]['total'] += 1
        if r['actual_up']:
            vote_stats[r['concept_vote']]['next_up'] += 1

for vote in ['看涨', '看跌', '中性']:
    s = vote_stats[vote]
    if s['total'] > 0:
        rate = s['next_up'] / s['total'] * 100
        print(f"  概念{vote}: {s['total']}样本, 次日涨(>=0%): {s['next_up']}/{s['total']} ({rate:.1f}%)")

# 按板块分组
print(f"\n── 按板块分组的概念涨跌投票效果 ──")
sector_vote_stats = defaultdict(lambda: defaultdict(lambda: {'total': 0, 'next_up': 0}))
for r in concept_signal_results:
    if r['concept_vote']:
        sector_vote_stats[r['sector']][r['concept_vote']]['total'] += 1
        if r['actual_up']:
            sector_vote_stats[r['sector']][r['concept_vote']]['next_up'] += 1

for sector in ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']:
    print(f"\n  {sector}:")
    for vote in ['看涨', '看跌', '中性']:
        s = sector_vote_stats[sector][vote]
        if s['total'] > 0:
            rate = s['next_up'] / s['total'] * 100
            print(f"    概念{vote}: {s['next_up']}/{s['total']} ({rate:.1f}%) 次日涨")


# ═══════════════════════════════════════════════════════════
# 分析2: 概念板块资金流投票 vs 次日涨跌
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析2: 概念板块资金流投票 vs 次日涨跌")
print(f"{'=' * 80}")

fvote_stats = defaultdict(lambda: {'total': 0, 'next_up': 0})
for r in concept_signal_results:
    if r['concept_fund_vote']:
        fvote_stats[r['concept_fund_vote']]['total'] += 1
        if r['actual_up']:
            fvote_stats[r['concept_fund_vote']]['next_up'] += 1

for vote in ['流入', '流出', '中性']:
    s = fvote_stats[vote]
    if s['total'] > 0:
        rate = s['next_up'] / s['total'] * 100
        print(f"  概念资金{vote}: {s['total']}样本, 次日涨(>=0%): {s['next_up']}/{s['total']} ({rate:.1f}%)")

# 按板块
print(f"\n── 按板块分组 ──")
sector_fvote = defaultdict(lambda: defaultdict(lambda: {'total': 0, 'next_up': 0}))
for r in concept_signal_results:
    if r['concept_fund_vote']:
        sector_fvote[r['sector']][r['concept_fund_vote']]['total'] += 1
        if r['actual_up']:
            sector_fvote[r['sector']][r['concept_fund_vote']]['next_up'] += 1

for sector in ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']:
    print(f"\n  {sector}:")
    for vote in ['流入', '流出', '中性']:
        s = sector_fvote[sector][vote]
        if s['total'] > 0:
            rate = s['next_up'] / s['total'] * 100
            print(f"    概念资金{vote}: {s['next_up']}/{s['total']} ({rate:.1f}%) 次日涨")

# ═══════════════════════════════════════════════════════════
# 分析3: 概念信号能否修正模型预测错误
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析3: 概念信号能否修正模型预测错误")
print(f"{'=' * 80}")

# 找出模型预测错误的样本
wrong_samples = [r for r in concept_signal_results if not r['loose_correct']]
right_samples = [r for r in concept_signal_results if r['loose_correct']]
print(f"模型预测错误: {len(wrong_samples)}样本, 正确: {len(right_samples)}样本")

# 在错误样本中，概念信号是否给出了正确方向？
print(f"\n── 模型预测错误时，概念涨跌投票的方向 ──")
wrong_concept_fix = {'可修正': 0, '也错': 0, '无信号': 0}
wrong_by_sector = defaultdict(lambda: {'可修正': 0, '也错': 0, '无信号': 0, 'total': 0})

for r in wrong_samples:
    sector = r['sector']
    wrong_by_sector[sector]['total'] += 1
    
    if r['concept_vote'] is None:
        wrong_concept_fix['无信号'] += 1
        wrong_by_sector[sector]['无信号'] += 1
        continue
    
    # 模型预测上涨但实际下跌 → 概念看跌能修正
    # 模型预测下跌但实际上涨 → 概念看涨能修正
    pred_up = r['pred_dir'] == '上涨'
    actual_up = r['actual_up']
    concept_up = r['concept_vote'] == '看涨'
    concept_down = r['concept_vote'] == '看跌'
    
    if pred_up and not actual_up:
        # 模型错误预测上涨，实际下跌
        if concept_down:
            wrong_concept_fix['可修正'] += 1
            wrong_by_sector[sector]['可修正'] += 1
        else:
            wrong_concept_fix['也错'] += 1
            wrong_by_sector[sector]['也错'] += 1
    elif not pred_up and actual_up:
        # 模型错误预测下跌，实际上涨
        if concept_up:
            wrong_concept_fix['可修正'] += 1
            wrong_by_sector[sector]['可修正'] += 1
        else:
            wrong_concept_fix['也错'] += 1
            wrong_by_sector[sector]['也错'] += 1
    else:
        wrong_concept_fix['也错'] += 1
        wrong_by_sector[sector]['也错'] += 1

total_wrong_with_signal = wrong_concept_fix['可修正'] + wrong_concept_fix['也错']
if total_wrong_with_signal > 0:
    fix_rate = wrong_concept_fix['可修正'] / total_wrong_with_signal * 100
    print(f"  有概念信号的错误样本: {total_wrong_with_signal}")
    print(f"  概念信号可修正: {wrong_concept_fix['可修正']} ({fix_rate:.1f}%)")
    print(f"  概念信号也错: {wrong_concept_fix['也错']} ({100-fix_rate:.1f}%)")
    print(f"  无概念信号: {wrong_concept_fix['无信号']}")

print(f"\n── 按板块的概念修正能力 ──")
for sector in ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']:
    s = wrong_by_sector[sector]
    with_signal = s['可修正'] + s['也错']
    if with_signal > 0:
        fix_rate = s['可修正'] / with_signal * 100
        print(f"  {sector}: 错误{s['total']}样本, 可修正{s['可修正']}/{with_signal} ({fix_rate:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 分析4: 概念信号与行业信号的互补性
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析4: 概念信号与行业信号的互补性")
print(f"{'=' * 80}")

# 当行业同行信号和概念信号一致/矛盾时的准确率
combo_stats = defaultdict(lambda: {'total': 0, 'correct': 0})

for i, r in enumerate(concept_signal_results):
    d = details[i]
    # 从逐日详情中获取同行信号
    peer_sig = d.get('同行信号', 0)
    if isinstance(peer_sig, str):
        try:
            peer_sig = float(peer_sig)
        except ValueError:
            peer_sig = 0
    
    if r['concept_vote'] is None:
        continue
    
    # 行业同行方向
    if peer_sig > 0.5:
        peer_dir = '看涨'
    elif peer_sig < -0.5:
        peer_dir = '看跌'
    else:
        peer_dir = '中性'
    
    concept_dir = r['concept_vote']
    
    # 一致性
    if peer_dir == concept_dir:
        combo = '一致'
    elif peer_dir == '中性' or concept_dir == '中性':
        combo = '部分'
    else:
        combo = '矛盾'
    
    combo_stats[combo]['total'] += 1
    if r['loose_correct']:
        combo_stats[combo]['correct'] += 1
    
    # 按板块细分
    combo_key = f"{r['sector']}_{combo}"
    combo_stats[combo_key]['total'] += 1
    if r['loose_correct']:
        combo_stats[combo_key]['correct'] += 1

print(f"\n── 行业同行 vs 概念信号一致性 → 模型准确率 ──")
for combo in ['一致', '矛盾', '部分']:
    s = combo_stats[combo]
    if s['total'] > 0:
        rate = s['correct'] / s['total'] * 100
        print(f"  {combo}: {s['correct']}/{s['total']} ({rate:.1f}%)")

print(f"\n── 按板块细分 ──")
for sector in ['科技', '有色金属', '汽车', '新能源', '医药', '化工', '制造']:
    print(f"\n  {sector}:")
    for combo in ['一致', '矛盾', '部分']:
        s = combo_stats[f"{sector}_{combo}"]
        if s['total'] > 0:
            rate = s['correct'] / s['total'] * 100
            print(f"    {combo}: {s['correct']}/{s['total']} ({rate:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析5: 概念板块极端信号（强一致看涨/看跌）
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析5: 概念板块极端信号效果")
print(f"{'=' * 80}")

# 当多数概念(>=70%)看涨或看跌时
extreme_stats = defaultdict(lambda: {'total': 0, 'next_up': 0, 'model_correct': 0})

for r in concept_signal_results:
    sigs = r['concept_signals']
    if len(sigs) < 3:  # 至少3个概念有信号
        continue
    
    up_count = sum(1 for s in sigs if s['signal'] > 0)
    down_count = sum(1 for s in sigs if s['signal'] < 0)
    total_sigs = len(sigs)
    
    up_pct = up_count / total_sigs
    down_pct = down_count / total_sigs
    
    if up_pct >= 0.7:
        key = '强看涨(≥70%概念涨)'
    elif down_pct >= 0.7:
        key = '强看跌(≥70%概念跌)'
    elif up_pct >= 0.5:
        key = '偏看涨(50-70%概念涨)'
    elif down_pct >= 0.5:
        key = '偏看跌(50-70%概念跌)'
    else:
        key = '分歧'
    
    extreme_stats[key]['total'] += 1
    if r['actual_up']:
        extreme_stats[key]['next_up'] += 1
    if r['loose_correct']:
        extreme_stats[key]['model_correct'] += 1

print(f"\n── 概念信号强度 vs 次日涨跌 ──")
for key in ['强看涨(≥70%概念涨)', '偏看涨(50-70%概念涨)', '分歧', '偏看跌(50-70%概念跌)', '强看跌(≥70%概念跌)']:
    s = extreme_stats[key]
    if s['total'] > 0:
        up_rate = s['next_up'] / s['total'] * 100
        model_rate = s['model_correct'] / s['total'] * 100
        print(f"  {key}: {s['total']}样本")
        print(f"    次日涨(>=0%): {s['next_up']}/{s['total']} ({up_rate:.1f}%)")
        print(f"    当前模型准确率: {s['model_correct']}/{s['total']} ({model_rate:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 分析6: 概念资金流极端信号
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析6: 概念资金流极端信号（反转效应验证）")
print(f"{'=' * 80}")

# v14发现行业板块重资金流入(>70%)是强反转信号
# 验证概念板块是否也有类似效应
fund_extreme = defaultdict(lambda: {'total': 0, 'next_up': 0})

for r in concept_signal_results:
    fsigs = r['concept_fund_signals']
    if len(fsigs) < 3:
        continue
    
    inflow_count = sum(1 for s in fsigs if s['signal'] > 0)
    outflow_count = sum(1 for s in fsigs if s['signal'] < 0)
    total_f = len(fsigs)
    
    inflow_pct = inflow_count / total_f
    outflow_pct = outflow_count / total_f
    
    if inflow_pct >= 0.7:
        key = '重流入(≥70%概念流入)'
    elif outflow_pct >= 0.7:
        key = '重流出(≥70%概念流出)'
    elif inflow_pct >= 0.5:
        key = '偏流入'
    elif outflow_pct >= 0.5:
        key = '偏流出'
    else:
        key = '分歧'
    
    fund_extreme[key]['total'] += 1
    if r['actual_up']:
        fund_extreme[key]['next_up'] += 1

for key in ['重流入(≥70%概念流入)', '偏流入', '分歧', '偏流出', '重流出(≥70%概念流出)']:
    s = fund_extreme[key]
    if s['total'] > 0:
        rate = s['next_up'] / s['total'] * 100
        print(f"  {key}: 次日涨 {s['next_up']}/{s['total']} ({rate:.1f}%)")


# ═══════════════════════════════════════════════════════════
# 分析7: 模拟概念信号加入后的准确率提升
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析7: 模拟概念信号加入后的准确率提升")
print(f"{'=' * 80}")

# 策略: 当概念信号与模型预测矛盾时，翻转预测
# 计算不同翻转策略的效果

strategies = {
    '策略A: 强概念看涨+模型预测下跌→翻转为上涨': {
        'condition': lambda r, sigs: (
            r['pred_dir'] == '下跌' and
            len(sigs) >= 3 and
            sum(1 for s in sigs if s['signal'] > 0) / len(sigs) >= 0.7
        ),
        'new_dir': '上涨',
    },
    '策略B: 强概念看跌+模型预测上涨→翻转为下跌': {
        'condition': lambda r, sigs: (
            r['pred_dir'] == '上涨' and
            len(sigs) >= 3 and
            sum(1 for s in sigs if s['signal'] < 0) / len(sigs) >= 0.7
        ),
        'new_dir': '下跌',
    },
    '策略C: 概念资金重流入+模型预测下跌→翻转(反转)': {
        'condition': lambda r, sigs: False,  # placeholder
        'new_dir': '上涨',
    },
    '策略D: 概念资金重流出+模型预测上涨→翻转(反转)': {
        'condition': lambda r, sigs: False,  # placeholder
        'new_dir': '下跌',
    },
}

# 策略A+B组合
flipped_a = 0
flipped_b = 0
new_correct = loose_ok  # 从基线开始

flip_details_a = []
flip_details_b = []

for r in concept_signal_results:
    sigs = r['concept_signals']
    
    # 策略A
    if (r['pred_dir'] == '下跌' and len(sigs) >= 3 and
        sum(1 for s in sigs if s['signal'] > 0) / len(sigs) >= 0.7):
        # 翻转为上涨
        new_dir = '上涨'
        new_loose = (r['actual_chg'] >= 0)
        old_loose = r['loose_correct']
        if new_loose != old_loose:
            if new_loose:
                new_correct += 1
                flip_details_a.append(f"  ✅ {r['code']} {r['date']} [{r['sector']}] 下跌→上涨 实际{r['actual_chg']:+.2f}%")
            else:
                new_correct -= 1
                flip_details_a.append(f"  ❌ {r['code']} {r['date']} [{r['sector']}] 下跌→上涨 实际{r['actual_chg']:+.2f}%")
        flipped_a += 1
    
    # 策略B
    if (r['pred_dir'] == '上涨' and len(sigs) >= 3 and
        sum(1 for s in sigs if s['signal'] < 0) / len(sigs) >= 0.7):
        new_dir = '下跌'
        new_loose = (r['actual_chg'] <= 0)
        old_loose = r['loose_correct']
        if new_loose != old_loose:
            if new_loose:
                new_correct += 1
                flip_details_b.append(f"  ✅ {r['code']} {r['date']} [{r['sector']}] 上涨→下跌 实际{r['actual_chg']:+.2f}%")
            else:
                new_correct -= 1
                flip_details_b.append(f"  ❌ {r['code']} {r['date']} [{r['sector']}] 上涨→下跌 实际{r['actual_chg']:+.2f}%")
        flipped_b += 1

print(f"\n策略A (强概念看涨→翻转为上涨): 翻转{flipped_a}样本")
for line in flip_details_a[:20]:
    print(line)
print(f"\n策略B (强概念看跌→翻转为下跌): 翻转{flipped_b}样本")
for line in flip_details_b[:20]:
    print(line)

print(f"\n策略A+B组合效果:")
print(f"  基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"  翻转后: {new_correct}/{total} ({new_correct/total*100:.1f}%)")
print(f"  变化: {new_correct - loose_ok:+d}样本 ({(new_correct-loose_ok)/total*100:+.1f}pp)")

# 策略E: 概念资金流反转策略
print(f"\n── 策略E: 概念资金流反转策略 ──")
new_correct_e = loose_ok
flipped_e = 0
flip_details_e = []

for r in concept_signal_results:
    fsigs = r['concept_fund_signals']
    if len(fsigs) < 3:
        continue
    
    inflow_pct = sum(1 for s in fsigs if s['signal'] > 0) / len(fsigs)
    outflow_pct = sum(1 for s in fsigs if s['signal'] < 0) / len(fsigs)
    
    # 重流入(>70%) + 模型预测上涨 → 反转为下跌（v14发现的反转效应）
    if inflow_pct >= 0.7 and r['pred_dir'] == '上涨':
        new_loose = (r['actual_chg'] <= 0)
        old_loose = r['loose_correct']
        if new_loose != old_loose:
            if new_loose:
                new_correct_e += 1
                flip_details_e.append(f"  ✅ {r['code']} {r['date']} [{r['sector']}] 上涨→下跌(资金反转) 实际{r['actual_chg']:+.2f}%")
            else:
                new_correct_e -= 1
                flip_details_e.append(f"  ❌ {r['code']} {r['date']} [{r['sector']}] 上涨→下跌(资金反转) 实际{r['actual_chg']:+.2f}%")
        flipped_e += 1
    
    # 重流出(>70%) + 模型预测下跌 → 反转为上涨
    elif outflow_pct >= 0.7 and r['pred_dir'] == '下跌':
        new_loose = (r['actual_chg'] >= 0)
        old_loose = r['loose_correct']
        if new_loose != old_loose:
            if new_loose:
                new_correct_e += 1
                flip_details_e.append(f"  ✅ {r['code']} {r['date']} [{r['sector']}] 下跌→上涨(资金反转) 实际{r['actual_chg']:+.2f}%")
            else:
                new_correct_e -= 1
                flip_details_e.append(f"  ❌ {r['code']} {r['date']} [{r['sector']}] 下跌→上涨(资金反转) 实际{r['actual_chg']:+.2f}%")
        flipped_e += 1

print(f"翻转{flipped_e}样本")
for line in flip_details_e[:20]:
    print(line)
print(f"\n策略E效果:")
print(f"  基线: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"  翻转后: {new_correct_e}/{total} ({new_correct_e/total*100:.1f}%)")
print(f"  变化: {new_correct_e - loose_ok:+d}样本 ({(new_correct_e-loose_ok)/total*100:+.1f}pp)")

# ═══════════════════════════════════════════════════════════
# 分析8: 单个概念板块的预测能力排名
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"分析8: 单个概念板块的预测能力排名")
print(f"{'=' * 80}")

# 对每个概念板块，计算其涨跌信号的方向一致率
concept_accuracy = {}  # concept_name -> {total, correct}

for r in concept_signal_results:
    for sig in r['concept_signals']:
        cname = sig['concept']
        if cname not in concept_accuracy:
            concept_accuracy[cname] = {'total': 0, 'correct': 0, 'reverse_correct': 0}
        concept_accuracy[cname]['total'] += 1
        
        # 正向: 概念看涨→次日涨, 概念看跌→次日跌
        if (sig['signal'] > 0 and r['actual_up']) or (sig['signal'] < 0 and not r['actual_up']):
            concept_accuracy[cname]['correct'] += 1
        # 反向: 概念看涨→次日跌, 概念看跌→次日涨
        if (sig['signal'] > 0 and not r['actual_up']) or (sig['signal'] < 0 and r['actual_up']):
            concept_accuracy[cname]['reverse_correct'] += 1

# 按正向准确率排序
sorted_concepts = sorted(
    [(k, v) for k, v in concept_accuracy.items() if v['total'] >= 20],
    key=lambda x: x[1]['correct'] / x[1]['total'],
    reverse=True
)

print(f"\n── 正向预测能力TOP20（概念涨→次日涨）──")
for cname, stats in sorted_concepts[:20]:
    rate = stats['correct'] / stats['total'] * 100
    rev_rate = stats['reverse_correct'] / stats['total'] * 100
    print(f"  {cname}: 正向{stats['correct']}/{stats['total']} ({rate:.1f}%), 反向({rev_rate:.1f}%)")

print(f"\n── 反向预测能力TOP20（概念涨→次日跌，反转信号）──")
sorted_reverse = sorted(
    [(k, v) for k, v in concept_accuracy.items() if v['total'] >= 20],
    key=lambda x: x[1]['reverse_correct'] / x[1]['total'],
    reverse=True
)
for cname, stats in sorted_reverse[:20]:
    rate = stats['correct'] / stats['total'] * 100
    rev_rate = stats['reverse_correct'] / stats['total'] * 100
    print(f"  {cname}: 反向{stats['reverse_correct']}/{stats['total']} ({rev_rate:.1f}%), 正向({rate:.1f}%)")

# ═══════════════════════════════════════════════════════════
# 总结
# ═══════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"总结")
print(f"{'=' * 80}")
print(f"基线准确率: {loose_ok}/{total} ({loose_ok/total*100:.1f}%)")
print(f"目标: 65% = {int(total*0.65)}/{total}")
print(f"差距: {int(total*0.65) - loose_ok}样本")
print(f"\n概念板块数据覆盖: {covered}/{len(bt_codes)}只回测股票有概念数据")
print(f"涉及概念板块: {len(relevant_concepts)}个")
print(f"概念板块日聚合数据: {len(concept_daily_stats)}条")
print(f"\n策略A+B(概念涨跌翻转): {new_correct - loose_ok:+d}样本")
print(f"策略E(概念资金流反转): {new_correct_e - loose_ok:+d}样本")
