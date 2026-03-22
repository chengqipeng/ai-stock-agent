#!/usr/bin/env python3
"""
V12 稳健性方向分析
==================
基于全量回测数据（v12_high_confidence_analysis.json），
从数据角度评估四个改进方向的可行性和稳健性。

方向1: 仓位管理（Kelly Criterion）
方向2: 组合分散化（板块集中度）
方向3: 信号质量分级（money_flow主导）
方向4: 样本外验证（时间外推稳定性）

不跑新回测，纯粹基于已有数据做分析。
"""
import json
import math
import sys
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).resolve().parent.parent / "data_results"


def load_data():
    """加载全量回测的high confidence分析数据"""
    path = DATA_DIR / "v12_high_confidence_analysis.json"
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def load_backtest_data():
    """加载全量回测结果（含所有置信度）"""
    path = DATA_DIR / "v12_backtest_result.json"
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def analyze_direction1_kelly(records, weekly_detail):
    """
    方向1: 仓位管理 — Kelly Criterion 分析
    
    Kelly公式: f* = (bp - q) / b
    其中 b=盈亏比, p=胜率, q=1-p
    
    评估不同子集的Kelly值，判断哪些子集值得加仓、哪些应减仓。
    """
    print("\n" + "=" * 70)
    print("方向1: 仓位管理（Kelly Criterion）")
    print("=" * 70)
    
    # 定义子集
    subsets = {
        '全部high': lambda r: True,
        'UP方向': lambda r: r['pred_direction'] == 'UP',
        'DOWN方向': lambda r: r['pred_direction'] == 'DOWN',
        'RSI<25': lambda r: r.get('rsi', 50) < 25,
        'RSI 25-35': lambda r: r.get('rsi', 50) >= 25 and r.get('rsi', 50) < 35,
        'RSI 35-50': lambda r: r.get('rsi', 50) >= 35 and r.get('rsi', 50) < 50,
        'money_flow强': lambda r: r.get('signal_scores', {}).get('money_flow', 0) > 0.3,
        'money_flow弱/无': lambda r: r.get('signal_scores', {}).get('money_flow', 0) <= 0.3,
        'composite>0.5': lambda r: abs(r.get('composite_score', 0)) > 0.5,
        'composite 0.3-0.5': lambda r: 0.3 <= abs(r.get('composite_score', 0)) <= 0.5,
        'market_aligned': lambda r: r.get('market_aligned', True),
        'market_independent': lambda r: not r.get('market_aligned', True),
    }
    
    results = {}
    print(f"\n{'子集':<22s} {'胜率':>6s} {'盈亏比':>6s} {'Kelly%':>7s} {'样本':>6s} {'期望收益':>8s} {'建议':>8s}")
    print("-" * 70)
    
    for name, filter_fn in subsets.items():
        subset = [r for r in records if filter_fn(r)]
        if len(subset) < 30:
            continue
        
        wins = [r['actual_return'] for r in subset if r['is_correct']]
        losses = [r['actual_return'] for r in subset if not r['is_correct']]
        
        if not wins or not losses:
            continue
        
        p = len(wins) / len(subset)
        q = 1 - p
        avg_win = sum(abs(w) for w in wins) / len(wins)
        avg_loss = sum(abs(l) for l in losses) / len(losses)
        b = avg_win / avg_loss if avg_loss > 0 else 0
        
        # Kelly fraction
        kelly = (b * p - q) / b if b > 0 else 0
        
        # 期望收益 = p * avg_win - q * avg_loss
        expected = p * avg_win - q * avg_loss
        
        # 建议
        if kelly > 0.15:
            advice = '✅加仓'
        elif kelly > 0.05:
            advice = '➡️标准'
        elif kelly > 0:
            advice = '⚠️轻仓'
        else:
            advice = '❌不做'
        
        results[name] = {
            'win_rate': p, 'payoff_ratio': b, 'kelly': kelly,
            'n': len(subset), 'expected': expected
        }
        
        print(f"  {name:<20s} {p:>5.1%} {b:>6.2f} {kelly:>6.1%} {len(subset):>6d} {expected:>+7.2f}% {advice}")
    
    # 周间Kelly稳定性
    print(f"\n  周间Kelly稳定性分析:")
    week_kellys = []
    for wd in weekly_detail:
        wk = wd['week']
        week_recs = [r for r in records if r['week'] == wk]
        if len(week_recs) < 5:
            continue
        wins = [r['actual_return'] for r in week_recs if r['is_correct']]
        losses = [r['actual_return'] for r in week_recs if not r['is_correct']]
        if not wins or not losses:
            continue
        p = len(wins) / len(week_recs)
        avg_win = sum(abs(w) for w in wins) / len(wins)
        avg_loss = sum(abs(l) for l in losses) / len(losses)
        b = avg_win / avg_loss if avg_loss > 0 else 0
        kelly = (b * p - (1-p)) / b if b > 0 else 0
        week_kellys.append(kelly)
    
    if week_kellys:
        mean_k = sum(week_kellys) / len(week_kellys)
        std_k = (sum((k - mean_k)**2 for k in week_kellys) / (len(week_kellys)-1)) ** 0.5
        positive_weeks = sum(1 for k in week_kellys if k > 0)
        print(f"  平均Kelly: {mean_k:.1%}, 标准差: {std_k:.1%}")
        print(f"  Kelly>0的周数: {positive_weeks}/{len(week_kellys)} ({positive_weeks/len(week_kellys):.0%})")
        print(f"  Kelly范围: [{min(week_kellys):.1%}, {max(week_kellys):.1%}]")
    
    return results


def analyze_direction2_diversification(records, weekly_detail):
    """
    方向2: 组合分散化 — 板块集中度分析
    
    检查同一周内预测是否集中在少数板块/市值区间，
    以及分散化后是否能降低周间方差。
    """
    print("\n" + "=" * 70)
    print("方向2: 组合分散化（板块集中度）")
    print("=" * 70)
    
    # 按周分组
    by_week = defaultdict(list)
    for r in records:
        by_week[r['week']].append(r)
    
    # 分析每周的集中度
    print(f"\n  周度集中度分析（stock_code前3位=交易所+板块）:")
    print(f"  {'周':<12s} {'预测数':>6s} {'准确率':>6s} {'板块数':>6s} {'最大板块占比':>10s} {'HHI':>6s}")
    print("  " + "-" * 55)
    
    week_stats = []
    for wk in sorted(by_week.keys()):
        recs = by_week[wk]
        if len(recs) < 10:
            continue
        
        # 用stock_code前3位作为板块代理
        # 600=沪主板, 601=沪大盘, 000=深主板, 002=中小板, 300=创业板, 688=科创板
        sector_map = defaultdict(int)
        for r in recs:
            code = r['stock_code']
            if code.startswith('688'):
                sector = '科创板'
            elif code.startswith('300'):
                sector = '创业板'
            elif code.startswith('002'):
                sector = '中小板'
            elif code.startswith('000'):
                sector = '深主板'
            elif code.startswith('601') or code.startswith('603'):
                sector = '沪大盘'
            else:
                sector = '沪主板'
            sector_map[sector] += 1
        
        n = len(recs)
        max_pct = max(sector_map.values()) / n
        hhi = sum((v/n)**2 for v in sector_map.values())
        acc = sum(1 for r in recs if r['is_correct']) / n
        
        week_stats.append({
            'week': wk, 'n': n, 'acc': acc,
            'n_sectors': len(sector_map), 'max_pct': max_pct, 'hhi': hhi,
            'sectors': dict(sector_map)
        })
        
        if n >= 50:  # 只显示大量预测的周
            print(f"  {wk:<12s} {n:>6d} {acc:>5.1%} {len(sector_map):>6d} {max_pct:>9.1%} {hhi:>6.3f}")
    
    # 集中度 vs 准确率相关性
    if len(week_stats) >= 10:
        # 高集中度周 vs 低集中度周
        sorted_by_hhi = sorted(week_stats, key=lambda x: x['hhi'])
        low_hhi = sorted_by_hhi[:len(sorted_by_hhi)//3]
        high_hhi = sorted_by_hhi[-len(sorted_by_hhi)//3:]
        
        low_acc = sum(s['acc'] * s['n'] for s in low_hhi) / sum(s['n'] for s in low_hhi)
        high_acc = sum(s['acc'] * s['n'] for s in high_hhi) / sum(s['n'] for s in high_hhi)
        
        print(f"\n  集中度与准确率关系:")
        print(f"  低集中度周(HHI<{sorted_by_hhi[len(sorted_by_hhi)//3]['hhi']:.3f}): "
              f"准确率 {low_acc:.1%} ({len(low_hhi)}周)")
        print(f"  高集中度周(HHI>{sorted_by_hhi[-len(sorted_by_hhi)//3]['hhi']:.3f}): "
              f"准确率 {high_acc:.1%} ({len(high_hhi)}周)")
    
    # 预测数量 vs 准确率
    if week_stats:
        sorted_by_n = sorted(week_stats, key=lambda x: x['n'])
        small_weeks = [s for s in sorted_by_n if s['n'] < 100]
        large_weeks = [s for s in sorted_by_n if s['n'] >= 100]
        
        if small_weeks and large_weeks:
            small_acc = sum(s['acc'] * s['n'] for s in small_weeks) / sum(s['n'] for s in small_weeks)
            large_acc = sum(s['acc'] * s['n'] for s in large_weeks) / sum(s['n'] for s in large_weeks)
            
            print(f"\n  预测数量与准确率关系:")
            print(f"  少量预测周(<100): 准确率 {small_acc:.1%} ({len(small_weeks)}周, "
                  f"共{sum(s['n'] for s in small_weeks)}条)")
            print(f"  大量预测周(≥100): 准确率 {large_acc:.1%} ({len(large_weeks)}周, "
                  f"共{sum(s['n'] for s in large_weeks)}条)")
            
            # 大量预测周通常是系统性事件
            print(f"\n  ⚠️ 大量预测周（系统性事件）详情:")
            for s in sorted(large_weeks, key=lambda x: x['n'], reverse=True)[:5]:
                print(f"    {s['week']}: {s['n']}条, 准确率{s['acc']:.1%}, "
                      f"板块分布: {s['sectors']}")
    
    return week_stats


def analyze_direction3_signal_quality(records):
    """
    方向3: 信号质量分级 — money_flow主导的精细分层
    
    分析不同信号组合的准确率，找出最稳定的信号组合。
    """
    print("\n" + "=" * 70)
    print("方向3: 信号质量分级（信号组合分析）")
    print("=" * 70)
    
    # 每条记录的信号组合
    combo_stats = defaultdict(lambda: {'total': 0, 'correct': 0, 'returns': []})
    
    for r in records:
        sigs = r.get('signal_scores', {})
        # 哪些信号存在且有效（score > 0.1 或 < -0.1）
        active_sigs = sorted([s for s, v in sigs.items() if abs(v) > 0.1])
        combo_key = '+'.join(active_sigs) if active_sigs else 'none'
        
        combo_stats[combo_key]['total'] += 1
        combo_stats[combo_key]['returns'].append(r['actual_return'])
        if r['is_correct']:
            combo_stats[combo_key]['correct'] += 1
    
    print(f"\n  信号组合准确率（样本≥30）:")
    print(f"  {'信号组合':<50s} {'准确率':>6s} {'样本':>6s} {'平均收益':>8s}")
    print("  " + "-" * 75)
    
    sorted_combos = sorted(combo_stats.items(), 
                           key=lambda x: x[1]['correct']/x[1]['total'] if x[1]['total'] > 0 else 0,
                           reverse=True)
    
    good_combos = []
    for combo, stats in sorted_combos:
        if stats['total'] < 30:
            continue
        acc = stats['correct'] / stats['total']
        avg_ret = sum(stats['returns']) / len(stats['returns'])
        marker = '✅' if acc >= 0.65 else ('➡️' if acc >= 0.55 else '❌')
        print(f"  {marker} {combo:<48s} {acc:>5.1%} {stats['total']:>6d} {avg_ret:>+7.2f}%")
        
        if acc >= 0.60:
            good_combos.append((combo, acc, stats['total']))
    
    # money_flow信号的独立贡献
    print(f"\n  money_flow信号的独立贡献:")
    has_mf = [r for r in records if r.get('signal_scores', {}).get('money_flow', 0) != 0]
    no_mf = [r for r in records if r.get('signal_scores', {}).get('money_flow', 0) == 0]
    
    if has_mf:
        mf_acc = sum(1 for r in has_mf if r['is_correct']) / len(has_mf)
        print(f"  有money_flow信号: {mf_acc:.1%} ({len(has_mf)}条)")
    if no_mf:
        no_mf_acc = sum(1 for r in no_mf if r['is_correct']) / len(no_mf)
        print(f"  无money_flow信号: {no_mf_acc:.1%} ({len(no_mf)}条)")
    
    # money_flow强度分段
    print(f"\n  money_flow强度分段:")
    mf_buckets = defaultdict(lambda: {'total': 0, 'correct': 0})
    for r in records:
        mf = abs(r.get('signal_scores', {}).get('money_flow', 0))
        if mf == 0:
            bucket = '无信号'
        elif mf < 0.2:
            bucket = '弱(0-0.2)'
        elif mf < 0.4:
            bucket = '中(0.2-0.4)'
        elif mf < 0.6:
            bucket = '强(0.4-0.6)'
        else:
            bucket = '极强(>0.6)'
        mf_buckets[bucket]['total'] += 1
        if r['is_correct']:
            mf_buckets[bucket]['correct'] += 1
    
    for bucket in ['无信号', '弱(0-0.2)', '中(0.2-0.4)', '强(0.4-0.6)', '极强(>0.6)']:
        d = mf_buckets.get(bucket, {'total': 0, 'correct': 0})
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"  {bucket:<15s}: {acc:.1%} ({d['total']}条)")
    
    # reversal信号强度分段
    print(f"\n  reversal信号强度分段:")
    rev_buckets = defaultdict(lambda: {'total': 0, 'correct': 0})
    for r in records:
        rev = abs(r.get('signal_scores', {}).get('reversal', 0))
        if rev < 0.3:
            bucket = '弱(<0.3)'
        elif rev < 0.5:
            bucket = '中(0.3-0.5)'
        elif rev < 0.7:
            bucket = '强(0.5-0.7)'
        else:
            bucket = '极强(>0.7)'
        rev_buckets[bucket]['total'] += 1
        if r['is_correct']:
            rev_buckets[bucket]['correct'] += 1
    
    for bucket in ['弱(<0.3)', '中(0.3-0.5)', '强(0.5-0.7)', '极强(>0.7)']:
        d = rev_buckets.get(bucket, {'total': 0, 'correct': 0})
        if d['total'] > 0:
            acc = d['correct'] / d['total']
            print(f"  {bucket:<15s}: {acc:.1%} ({d['total']}条)")
    
    return good_combos


def analyze_direction4_oos(records, weekly_detail, backtest_data):
    """
    方向4: 样本外验证 — 时间外推稳定性
    
    将数据分为前70%和后30%，检查规则在时间外推上是否稳定。
    这是最关键的过拟合检测手段。
    """
    print("\n" + "=" * 70)
    print("方向4: 样本外验证（时间外推稳定性）")
    print("=" * 70)
    
    # 按周排序
    all_weeks = sorted(set(r['week'] for r in records))
    n_weeks = len(all_weeks)
    split_idx = int(n_weeks * 0.7)
    train_weeks = set(all_weeks[:split_idx])
    test_weeks = set(all_weeks[split_idx:])
    
    train_recs = [r for r in records if r['week'] in train_weeks]
    test_recs = [r for r in records if r['week'] in test_weeks]
    
    print(f"\n  数据划分:")
    print(f"  训练集(前70%): {len(train_weeks)}周, {len(train_recs)}条预测")
    print(f"  测试集(后30%): {len(test_weeks)}周, {len(test_recs)}条预测")
    print(f"  训练期: {min(train_weeks)} ~ {max(train_weeks)}")
    print(f"  测试期: {min(test_weeks)} ~ {max(test_weeks)}")
    
    # 各维度的训练/测试对比
    dimensions = {
        '总体': (lambda r: True,),
        'UP方向': (lambda r: r['pred_direction'] == 'UP',),
        'DOWN方向': (lambda r: r['pred_direction'] == 'DOWN',),
        'RSI<25': (lambda r: r.get('rsi', 50) < 25,),
        'RSI 25-35': (lambda r: r.get('rsi', 50) >= 25 and r.get('rsi', 50) < 35,),
        'RSI 35-50': (lambda r: r.get('rsi', 50) >= 35 and r.get('rsi', 50) < 50,),
        'market_aligned': (lambda r: r.get('market_aligned', True),),
        'market_independent': (lambda r: not r.get('market_aligned', True),),
        'money_flow有': (lambda r: r.get('signal_scores', {}).get('money_flow', 0) != 0,),
        'money_flow无': (lambda r: r.get('signal_scores', {}).get('money_flow', 0) == 0,),
        'extreme_score≥8': (lambda r: r.get('extreme_score', 0) >= 8,),
        'extreme_score 6-7': (lambda r: 6 <= r.get('extreme_score', 0) <= 7,),
    }
    
    print(f"\n  {'维度':<22s} {'训练准确率':>8s} {'测试准确率':>8s} {'差异':>6s} {'稳定性':>6s} {'测试样本':>8s}")
    print("  " + "-" * 65)
    
    stability_scores = {}
    for name, (filter_fn,) in dimensions.items():
        train_sub = [r for r in train_recs if filter_fn(r)]
        test_sub = [r for r in test_recs if filter_fn(r)]
        
        if len(train_sub) < 20 or len(test_sub) < 20:
            continue
        
        train_acc = sum(1 for r in train_sub if r['is_correct']) / len(train_sub)
        test_acc = sum(1 for r in test_sub if r['is_correct']) / len(test_sub)
        diff = test_acc - train_acc
        
        # 稳定性评分：差异越小越稳定
        if abs(diff) < 0.03:
            stability = '✅稳定'
            score = 3
        elif abs(diff) < 0.08:
            stability = '➡️一般'
            score = 2
        else:
            stability = '❌不稳'
            score = 1
        
        stability_scores[name] = {
            'train_acc': train_acc, 'test_acc': test_acc,
            'diff': diff, 'score': score,
            'train_n': len(train_sub), 'test_n': len(test_sub)
        }
        
        print(f"  {name:<20s} {train_acc:>7.1%} {test_acc:>7.1%} {diff:>+5.1%} {stability} {len(test_sub):>8d}")
    
    # 滚动窗口稳定性（每10周一个窗口）
    print(f"\n  滚动窗口准确率（10周窗口）:")
    window_size = 10
    window_results = []
    for i in range(0, len(all_weeks) - window_size + 1, 5):
        window_weeks = set(all_weeks[i:i+window_size])
        window_recs = [r for r in records if r['week'] in window_weeks]
        if len(window_recs) < 20:
            continue
        acc = sum(1 for r in window_recs if r['is_correct']) / len(window_recs)
        window_results.append({
            'start': all_weeks[i], 'end': all_weeks[min(i+window_size-1, len(all_weeks)-1)],
            'acc': acc, 'n': len(window_recs)
        })
        print(f"  {all_weeks[i]}~{all_weeks[min(i+window_size-1, len(all_weeks)-1)]}: "
              f"{acc:.1%} ({len(window_recs)}条)")
    
    # 滚动窗口方差
    if window_results:
        accs = [w['acc'] for w in window_results]
        mean_acc = sum(accs) / len(accs)
        std_acc = (sum((a - mean_acc)**2 for a in accs) / (len(accs)-1)) ** 0.5
        print(f"\n  滚动窗口统计: 均值{mean_acc:.1%}, 标准差{std_acc:.1%}, "
              f"范围[{min(accs):.1%}, {max(accs):.1%}]")
    
    # 全量回测的周度数据也做前后对比
    if backtest_data and 'weekly_accuracy' in backtest_data:
        wa = backtest_data['weekly_accuracy']
        valid_wa = [w for w in wa if w['n_pred'] >= 10]
        if len(valid_wa) >= 10:
            split = int(len(valid_wa) * 0.7)
            first_half = valid_wa[:split]
            second_half = valid_wa[split:]
            
            first_acc = sum(w['accuracy'] * w['n_pred'] for w in first_half) / sum(w['n_pred'] for w in first_half)
            second_acc = sum(w['accuracy'] * w['n_pred'] for w in second_half) / sum(w['n_pred'] for w in second_half)
            
            print(f"\n  全量回测（所有置信度）前后对比:")
            print(f"  前70%周: {first_acc:.1%} ({sum(w['n_pred'] for w in first_half)}条)")
            print(f"  后30%周: {second_acc:.1%} ({sum(w['n_pred'] for w in second_half)}条)")
            print(f"  差异: {second_acc - first_acc:+.1%}")
    
    return stability_scores


def final_verdict(kelly_results, week_stats, good_combos, stability_scores):
    """
    综合评估四个方向，给出最终建议
    """
    print("\n" + "=" * 70)
    print("综合评估：四个方向的稳健性排名")
    print("=" * 70)
    
    scores = {}
    
    # 方向1评分：Kelly
    # 看Kelly值是否在周间稳定为正
    d1_score = 0
    all_kelly = kelly_results.get('全部high', {})
    if all_kelly.get('kelly', 0) > 0:
        d1_score += 2  # 整体Kelly为正
    if all_kelly.get('kelly', 0) > 0.1:
        d1_score += 1  # Kelly较大
    # 子集间差异是否显著（能否通过仓位管理获益）
    kellys = [v['kelly'] for v in kelly_results.values() if v.get('kelly') is not None]
    if kellys:
        kelly_range = max(kellys) - min(kellys)
        if kelly_range > 0.2:
            d1_score += 2  # 子集间差异大，仓位管理有空间
        elif kelly_range > 0.1:
            d1_score += 1
    scores['方向1:仓位管理'] = d1_score
    
    # 方向2评分：分散化
    d2_score = 0
    if week_stats:
        # 大量预测周是否准确率更低（说明系统性事件是问题）
        large = [s for s in week_stats if s['n'] >= 100]
        small = [s for s in week_stats if 10 <= s['n'] < 100]
        if large and small:
            large_acc = sum(s['acc'] * s['n'] for s in large) / sum(s['n'] for s in large)
            small_acc = sum(s['acc'] * s['n'] for s in small) / sum(s['n'] for s in small)
            if large_acc < small_acc - 0.05:
                d2_score += 2  # 大量预测周确实更差
            if large_acc < small_acc - 0.1:
                d2_score += 1
        # 集中度是否影响准确率
        sorted_by_hhi = sorted(week_stats, key=lambda x: x['hhi'])
        if len(sorted_by_hhi) >= 6:
            low_hhi = sorted_by_hhi[:len(sorted_by_hhi)//3]
            high_hhi = sorted_by_hhi[-len(sorted_by_hhi)//3:]
            if low_hhi and high_hhi:
                low_acc = sum(s['acc'] * s['n'] for s in low_hhi) / max(sum(s['n'] for s in low_hhi), 1)
                high_acc = sum(s['acc'] * s['n'] for s in high_hhi) / max(sum(s['n'] for s in high_hhi), 1)
                if abs(low_acc - high_acc) > 0.05:
                    d2_score += 2
    scores['方向2:组合分散化'] = d2_score
    
    # 方向3评分：信号质量
    d3_score = 0
    if good_combos:
        # 有多少个>60%准确率的信号组合
        d3_score += min(len(good_combos), 3)
        # 最好的组合准确率
        best_acc = max(acc for _, acc, _ in good_combos)
        if best_acc > 0.70:
            d3_score += 2
        elif best_acc > 0.65:
            d3_score += 1
    scores['方向3:信号质量分级'] = d3_score
    
    # 方向4评分：样本外验证
    d4_score = 0
    if stability_scores:
        # 稳定维度的数量
        stable_count = sum(1 for v in stability_scores.values() if v['score'] == 3)
        unstable_count = sum(1 for v in stability_scores.values() if v['score'] == 1)
        d4_score += stable_count
        d4_score -= unstable_count
        # 总体是否稳定
        overall = stability_scores.get('总体', {})
        if overall and abs(overall.get('diff', 1)) < 0.05:
            d4_score += 2
    scores['方向4:样本外验证'] = d4_score
    
    # 排名
    print(f"\n  {'方向':<25s} {'得分':>6s} {'评价'}")
    print("  " + "-" * 50)
    
    for name, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
        if score >= 5:
            rating = '⭐⭐⭐ 强烈推荐'
        elif score >= 3:
            rating = '⭐⭐ 推荐'
        elif score >= 1:
            rating = '⭐ 可选'
        else:
            rating = '❌ 不推荐'
        print(f"  {name:<25s} {score:>4d}   {rating}")
    
    # 最终建议
    best = max(scores.items(), key=lambda x: x[1])
    print(f"\n  📌 最终建议: {best[0]}")
    
    # 详细理由
    print(f"\n  详细分析:")
    print(f"  ─────────")
    
    print(f"\n  方向1(仓位管理):")
    print(f"    优势: 不改变预测逻辑，纯数学框架（Kelly），零过拟合风险")
    print(f"    劣势: 依赖准确率和盈亏比的稳定性，周间波动大时Kelly不稳定")
    print(f"    实施难度: 低")
    
    print(f"\n  方向2(组合分散化):")
    print(f"    优势: 降低系统性事件的冲击，减少周间方差")
    print(f"    劣势: 可能降低覆盖率，大量预测周本身可能是最佳机会（如暴跌反弹）")
    print(f"    实施难度: 中")
    
    print(f"\n  方向3(信号质量分级):")
    print(f"    优势: 利用已有信号的差异化表现，精细化置信度")
    print(f"    劣势: 信号组合的准确率差异可能是样本噪声，需要样本外验证")
    print(f"    实施难度: 中")
    print(f"    ⚠️ 过拟合风险: 中等 — 信号组合的准确率差异需要在测试集上确认")
    
    print(f"\n  方向4(样本外验证):")
    print(f"    优势: 这不是改进方向，而是验证框架 — 任何改进都应该通过这个检验")
    print(f"    劣势: 不直接提升准确率")
    print(f"    实施难度: 低")
    print(f"    📌 这是基础设施，无论选哪个方向都应该先做")
    
    return scores


def main():
    print("加载数据...")
    data = load_data()
    backtest = load_backtest_data()
    
    records = data.get('records', [])
    weekly_detail = data.get('weekly_detail', [])
    
    print(f"共 {len(records)} 条 high confidence 记录, {len(weekly_detail)} 周")
    
    # 四个方向分析
    kelly_results = analyze_direction1_kelly(records, weekly_detail)
    week_stats = analyze_direction2_diversification(records, weekly_detail)
    good_combos = analyze_direction3_signal_quality(records)
    stability_scores = analyze_direction4_oos(records, weekly_detail, backtest)
    
    # 综合评估
    final_scores = final_verdict(kelly_results, week_stats, good_combos, stability_scores)
    
    # 保存结果
    output = {
        'kelly_analysis': {k: {kk: round(vv, 4) if isinstance(vv, float) else vv 
                                for kk, vv in v.items()} 
                           for k, v in kelly_results.items()},
        'signal_combos': [{'combo': c, 'accuracy': round(a, 4), 'n': n} 
                          for c, a, n in good_combos],
        'stability': {k: {kk: round(vv, 4) if isinstance(vv, float) else vv 
                          for kk, vv in v.items()} 
                      for k, v in stability_scores.items()},
        'direction_scores': final_scores,
    }
    
    output_path = DATA_DIR / "v12_robustness_analysis.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n结果已保存: {output_path}")


if __name__ == '__main__':
    main()
