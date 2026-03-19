#!/usr/bin/env python3
"""
深度分析 DeepSeek 预测数据，找出"绝对可信"的高准确率信号组合。
目标：准确率 ≥ 75%，宁可少预测也要高可信度。

分析维度：
1. 个股涨幅区间 × 大盘涨跌 交叉
2. 个股涨幅 × 60日位置 交叉
3. 个股涨幅 × 连涨天数 交叉
4. 大盘跌幅 × 个股涨幅 交叉
5. 多条件组合筛选
"""
import json
import sys
from collections import defaultdict
from pathlib import Path


def load_data():
    """加载最新的回测结果 JSON"""
    # 用 V11 的数据（最新完整数据）
    path = Path(__file__).parent.parent / 'data_results' / 'four_way_200stocks_result_v11.json'
    if not path.exists():
        path = Path(__file__).parent.parent / 'data_results' / 'four_way_200stocks_result.json'
    with open(path) as f:
        return json.load(f)


def extract_predictions(data):
    """提取所有 DeepSeek 预测及其特征"""
    preds = []
    for week_data in data['weekly_results']:
        w = week_data['week']
        for s in week_data['stocks']:
            ds = s.get('deepseek') or {}
            actual = s.get('actual')
            if actual is None:
                continue

            direction = ds.get('direction', 'UNCERTAIN')
            confidence = ds.get('confidence', 0.5)
            justification = ds.get('justification', '')

            # V4/V5 规则结果
            v4 = s.get('v4', {})
            v5 = s.get('v5', {})

            preds.append({
                'week': w,
                'code': s['code'],
                'name': s.get('name', ''),
                'direction': direction,
                'confidence': confidence,
                'justification': justification,
                'actual': actual,
                'actual_up': actual > 0,
                'is_pred': direction != 'UNCERTAIN',
                'correct': (direction == 'UP' and actual > 0) or
                           (direction == 'DOWN' and actual < 0)
                           if direction != 'UNCERTAIN' else None,
                # V4/V5
                'v4_dir': v4.get('direction', 'UNCERTAIN'),
                'v5_dir': v5.get('direction', 'UNCERTAIN'),
                'v4_correct': (v4.get('direction') == 'UP' and actual > 0) or
                              (v4.get('direction') == 'DOWN' and actual < 0)
                              if v4.get('direction', 'UNCERTAIN') != 'UNCERTAIN' else None,
            })
    return preds


def analyze_justification_patterns(preds):
    """分析 justification 中的关键词模式"""
    active = [p for p in preds if p['is_pred']]

    print("\n" + "=" * 70)
    print("  1. Justification 关键词准确率分析")
    print("=" * 70)

    # 提取所有关键词模式
    patterns = defaultdict(lambda: {'correct': 0, 'total': 0})
    for p in active:
        j = p['justification']
        # 涨幅相关
        if '涨幅超过7%' in j or '涨幅超7%' in j or '涨幅>7%' in j or '涨>7%' in j:
            k = '涨>7%'
        elif '涨幅超过5%' in j or '涨幅超5%' in j or '涨幅>5%' in j or '涨>5%' in j:
            k = '涨>5%(不含>7%)'
        elif '涨幅超过3%' in j or '涨幅超3%' in j or '涨幅>3%' in j or '涨>3%' in j:
            k = '涨>3%(不含>5%)'
        else:
            k = '其他涨幅'

        patterns[k]['total'] += 1
        if p['correct']:
            patterns[k]['correct'] += 1

        # 高位相关
        if '高位' in j:
            patterns['含"高位"']['total'] += 1
            if p['correct']:
                patterns['含"高位"']['correct'] += 1

        # 连涨相关
        if '连涨' in j:
            patterns['含"连涨"']['total'] += 1
            if p['correct']:
                patterns['含"连涨"']['correct'] += 1

        # 逆势/过热
        if '逆势' in j or '过热' in j:
            patterns['含"逆势/过热"']['total'] += 1
            if p['correct']:
                patterns['含"逆势/过热"']['correct'] += 1

        # 回调
        if '回调' in j:
            patterns['含"回调"']['total'] += 1
            if p['correct']:
                patterns['含"回调"']['correct'] += 1

        # D规则
        for d in ['D1', 'D2', 'D3', 'D4']:
            if d in j:
                patterns[f'提到{d}']['total'] += 1
                if p['correct']:
                    patterns[f'提到{d}']['correct'] += 1

    for k, v in sorted(patterns.items(), key=lambda x: -x[1]['total']):
        acc = v['correct'] / v['total'] * 100 if v['total'] > 0 else 0
        marker = ' ★' if acc >= 75 and v['total'] >= 10 else ''
        print(f"  {k:<20} {v['correct']:>3}/{v['total']:<3} = {acc:>5.1f}%{marker}")


def analyze_cross_dimensions(preds):
    """交叉维度分析 — 找出高准确率的条件组合"""
    active = [p for p in preds if p['is_pred']]

    print("\n" + "=" * 70)
    print("  2. 交叉维度分析（寻找 ≥75% 准确率的组合）")
    print("=" * 70)

    # 按周分组，看哪些周的特征
    print("\n  ── 按周 ──")
    for w in sorted(set(p['week'] for p in active)):
        subset = [p for p in active if p['week'] == w]
        correct = sum(1 for p in subset if p['correct'])
        acc = correct / len(subset) * 100
        marker = ' ★' if acc >= 75 else ' ✗' if acc < 50 else ''
        print(f"    {w}: {correct}/{len(subset)} = {acc:.1f}%{marker}")

    # V4/V5 规则一致性
    print("\n  ── DeepSeek + 规则引擎一致性 ──")
    combos = {
        'DS+V4一致(非UNC)': lambda p: p['v4_dir'] != 'UNCERTAIN' and p['v4_dir'] == p['direction'],
        'DS+V5一致(非UNC)': lambda p: p['v5_dir'] != 'UNCERTAIN' and p['v5_dir'] == p['direction'],
        'DS+V4+V5三方一致': lambda p: p['v4_dir'] != 'UNCERTAIN' and p['v5_dir'] != 'UNCERTAIN'
                                       and p['v4_dir'] == p['direction'] and p['v5_dir'] == p['direction'],
        'DS预测但V4=UNC': lambda p: p['v4_dir'] == 'UNCERTAIN',
        'DS预测但V5=UNC': lambda p: p['v5_dir'] == 'UNCERTAIN',
    }
    for name, cond in combos.items():
        subset = [p for p in active if cond(p)]
        if subset:
            correct = sum(1 for p in subset if p['correct'])
            acc = correct / len(subset) * 100
            marker = ' ★' if acc >= 75 and len(subset) >= 10 else ''
            print(f"    {name}: {correct}/{len(subset)} = {acc:.1f}%{marker}")

    # 排除最差的周后
    print("\n  ── 排除特定周后的准确率 ──")
    bad_weeks = ['W3→W4', 'W10→W11']
    for exclude in [['W3→W4'], ['W10→W11'], bad_weeks]:
        subset = [p for p in active if p['week'] not in exclude]
        correct = sum(1 for p in subset if p['correct'])
        acc = correct / len(subset) * 100
        print(f"    排除{exclude}: {correct}/{len(subset)} = {acc:.1f}%")


def analyze_actual_distribution(preds):
    """分析所有样本的实际涨跌分布，理解基准率"""
    print("\n" + "=" * 70)
    print("  3. 实际涨跌分布（基准率分析）")
    print("=" * 70)

    all_preds = preds  # 包括 UNCERTAIN
    for w in sorted(set(p['week'] for p in all_preds)):
        subset = [p for p in all_preds if p['week'] == w]
        up = sum(1 for p in subset if p['actual'] > 0)
        down = sum(1 for p in subset if p['actual'] < 0)
        flat = sum(1 for p in subset if p['actual'] == 0)
        total = len(subset)
        down_rate = down / total * 100 if total > 0 else 0
        print(f"    {w}: 涨{up} 跌{down} 平{flat} (跌率{down_rate:.1f}%)")


def analyze_high_confidence_combos(preds):
    """寻找多条件组合的高准确率信号"""
    active = [p for p in preds if p['is_pred']]

    print("\n" + "=" * 70)
    print("  4. 高可信度信号组合搜索")
    print("=" * 70)

    # 条件函数
    conditions = {
        '涨>7%': lambda p: '涨幅超过7%' in p['justification'] or '涨幅超7%' in p['justification']
                           or '涨幅>7%' in p['justification'] or '涨>7%' in p['justification'],
        '涨>5%': lambda p: '涨幅超过5%' in p['justification'] or '涨幅超5%' in p['justification']
                           or '涨幅>5%' in p['justification'] or '涨>5%' in p['justification']
                           or '涨幅超过7%' in p['justification'] or '涨幅超7%' in p['justification']
                           or '涨幅>7%' in p['justification'] or '涨>7%' in p['justification'],
        '含高位': lambda p: '高位' in p['justification'],
        '含连涨': lambda p: '连涨' in p['justification'],
        '含回调': lambda p: '回调' in p['justification'],
        '不含高位': lambda p: '高位' not in p['justification'],
        '不含W3W4': lambda p: p['week'] != 'W3→W4',
        '不含W10W11': lambda p: p['week'] != 'W10→W11',
        '排除差周': lambda p: p['week'] not in ('W3→W4', 'W10→W11'),
        'V4也DOWN': lambda p: p['v4_dir'] == 'DOWN',
        'V5也DOWN': lambda p: p['v5_dir'] == 'DOWN',
        'V4=UNC': lambda p: p['v4_dir'] == 'UNCERTAIN',
        'DOWN预测': lambda p: p['direction'] == 'DOWN',
    }

    # 单条件
    print("\n  ── 单条件筛选 ──")
    for name, cond in conditions.items():
        subset = [p for p in active if cond(p)]
        if subset:
            correct = sum(1 for p in subset if p['correct'])
            acc = correct / len(subset) * 100
            marker = ' ★' if acc >= 75 and len(subset) >= 10 else ''
            print(f"    {name}: {correct}/{len(subset)} = {acc:.1f}%{marker}")

    # 双条件组合
    print("\n  ── 双条件组合（≥75% 且 ≥10样本）──")
    cond_names = list(conditions.keys())
    found = []
    for i in range(len(cond_names)):
        for j in range(i + 1, len(cond_names)):
            n1, n2 = cond_names[i], cond_names[j]
            c1, c2 = conditions[n1], conditions[n2]
            subset = [p for p in active if c1(p) and c2(p)]
            if len(subset) >= 10:
                correct = sum(1 for p in subset if p['correct'])
                acc = correct / len(subset) * 100
                if acc >= 75:
                    found.append((acc, correct, len(subset), f"{n1} + {n2}"))

    found.sort(key=lambda x: (-x[0], -x[2]))
    for acc, correct, total, name in found:
        print(f"    {name}: {correct}/{total} = {acc:.1f}% ★")

    # 三条件组合
    print("\n  ── 三条件组合（≥75% 且 ≥10样本）──")
    found3 = []
    for i in range(len(cond_names)):
        for j in range(i + 1, len(cond_names)):
            for k in range(j + 1, len(cond_names)):
                n1, n2, n3 = cond_names[i], cond_names[j], cond_names[k]
                c1, c2, c3 = conditions[n1], conditions[n2], conditions[n3]
                subset = [p for p in active if c1(p) and c2(p) and c3(p)]
                if len(subset) >= 10:
                    correct = sum(1 for p in subset if p['correct'])
                    acc = correct / len(subset) * 100
                    if acc >= 75:
                        found3.append((acc, correct, len(subset), f"{n1} + {n2} + {n3}"))

    found3.sort(key=lambda x: (-x[0], -x[2]))
    for acc, correct, total, name in found3[:20]:
        print(f"    {name}: {correct}/{total} = {acc:.1f}% ★")


def main():
    data = load_data()
    preds = extract_predictions(data)

    total = len(preds)
    active = [p for p in preds if p['is_pred']]
    correct = sum(1 for p in active if p['correct'])
    print(f"总样本: {total}, 预测: {len(active)}, 正确: {correct}/{len(active)} = {correct/len(active)*100:.1f}%")

    analyze_justification_patterns(preds)
    analyze_cross_dimensions(preds)
    analyze_actual_distribution(preds)
    analyze_high_confidence_combos(preds)


if __name__ == '__main__':
    main()
