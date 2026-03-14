#!/usr/bin/env python3
"""
深度分析 v7b 回测结果，找出系统性失败模式和可改进方向。

分析维度：
1. 融合信号强度 vs 准确率 — 信号越强是否越准？
2. 连续预测失败模式 — 是否存在系统性偏差？
3. 实际涨跌幅分布 vs 预测方向 — 大涨大跌时预测如何？
4. 多因子一致性 vs 准确率 — 多因子同向时是否更准？
5. 波动率环境 vs 准确率 — 高波动/低波动时表现差异
6. 时间段分析 — 不同月份/周几的准确率差异
7. 评分区间细分 — 找出最佳预测区间
8. 信号置信度过滤 — 只在高置信度时预测能否提升准确率
"""
import json
import sys
import os
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_results():
    with open('data_results/backtest_prediction_enhanced_result.json', 'r', encoding='utf-8') as f:
        return json.load(f)


def analyze():
    data = load_results()
    details = data.get('逐日详情', [])
    print(f"总样本数: {len(details)}")
    print("=" * 80)

    # ═══════════════════════════════════════════════════════
    # 1. 融合信号强度 vs 准确率
    # ═══════════════════════════════════════════════════════
    print("\n[1] 融合信号强度 vs 准确率")
    print("-" * 60)
    signal_buckets = [
        ("极强看涨(>2.0)", lambda s: s > 2.0),
        ("强看涨(1.0~2.0)", lambda s: 1.0 < s <= 2.0),
        ("弱看涨(0.3~1.0)", lambda s: 0.3 < s <= 1.0),
        ("中性(-0.3~0.3)", lambda s: -0.3 <= s <= 0.3),
        ("弱看跌(-1.0~-0.3)", lambda s: -1.0 <= s < -0.3),
        ("强看跌(-2.0~-1.0)", lambda s: -2.0 <= s < -1.0),
        ("极强看跌(<-2.0)", lambda s: s < -2.0),
    ]
    for name, cond in signal_buckets:
        group = [d for d in details if cond(d.get('融合信号', 0))]
        if not group:
            continue
        n = len(group)
        loose_ok = sum(1 for d in group if d['宽松正确'] == '✓')
        strict_ok = sum(1 for d in group if d['严格正确'] == '✓')
        avg_chg = sum(float(d['实际涨跌'].replace('%', '').replace('+', '')) for d in group) / n
        print(f"  {name:20s}: {n:4d}样本  宽松{loose_ok/n*100:5.1f}%  严格{strict_ok/n*100:5.1f}%  均涨跌{avg_chg:+.2f}%")

    # ═══════════════════════════════════════════════════════
    # 2. 多因子一致性分析 — 有多少因子同向时准确率最高
    # ═══════════════════════════════════════════════════════
    print("\n[2] 多因子一致性 vs 准确率（因子同向数量）")
    print("-" * 60)

    # 需要从完整结果中获取因子数据，逐日详情中没有因子明细
    # 用融合信号绝对值作为代理
    abs_signal_buckets = [
        ("|信号|>3.0", lambda s: abs(s) > 3.0),
        ("|信号|2.0~3.0", lambda s: 2.0 < abs(s) <= 3.0),
        ("|信号|1.0~2.0", lambda s: 1.0 < abs(s) <= 2.0),
        ("|信号|0.5~1.0", lambda s: 0.5 < abs(s) <= 1.0),
        ("|信号|<0.5", lambda s: abs(s) <= 0.5),
    ]
    for name, cond in abs_signal_buckets:
        group = [d for d in details if cond(d.get('融合信号', 0))]
        if not group:
            continue
        n = len(group)
        loose_ok = sum(1 for d in group if d['宽松正确'] == '✓')
        print(f"  {name:20s}: {n:4d}样本  宽松{loose_ok/n*100:5.1f}%")

    # ═══════════════════════════════════════════════════════
    # 3. 实际涨跌幅分布 — 大涨大跌时预测表现
    # ═══════════════════════════════════════════════════════
    print("\n[3] 实际涨跌幅分布 vs 预测准确率")
    print("-" * 60)
    chg_buckets = [
        ("大涨>3%", lambda c: c > 3),
        ("中涨1~3%", lambda c: 1 < c <= 3),
        ("小涨0~1%", lambda c: 0 < c <= 1),
        ("小跌-1~0%", lambda c: -1 <= c < 0),
        ("中跌-3~-1%", lambda c: -3 <= c < -1),
        ("大跌<-3%", lambda c: c < -3),
    ]
    for name, cond in chg_buckets:
        group = [d for d in details if cond(float(d['实际涨跌'].replace('%', '').replace('+', '')))]
        if not group:
            continue
        n = len(group)
        pred_up = sum(1 for d in group if d['预测方向'] == '上涨')
        pred_down = sum(1 for d in group if d['预测方向'] == '下跌')
        loose_ok = sum(1 for d in group if d['宽松正确'] == '✓')
        print(f"  {name:12s}: {n:4d}样本  预测涨{pred_up:3d} 预测跌{pred_down:3d}  宽松{loose_ok/n*100:5.1f}%")

    # ═══════════════════════════════════════════════════════
    # 4. 按星期几分析
    # ═══════════════════════════════════════════════════════
    print("\n[4] 按星期几分析（预测日）")
    print("-" * 60)
    weekday_stats = defaultdict(lambda: {'n': 0, 'ok': 0, 'loose_ok': 0})
    weekday_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
    for d in details:
        try:
            dt = datetime.strptime(d['预测日'], '%Y-%m-%d')
            wd = dt.weekday()
            weekday_stats[wd]['n'] += 1
            if d['宽松正确'] == '✓':
                weekday_stats[wd]['loose_ok'] += 1
            if d['严格正确'] == '✓':
                weekday_stats[wd]['ok'] += 1
        except:
            pass
    for wd in range(5):
        s = weekday_stats[wd]
        if s['n'] > 0:
            print(f"  {weekday_names[wd]}: {s['n']:4d}样本  宽松{s['loose_ok']/s['n']*100:5.1f}%  严格{s['ok']/s['n']*100:5.1f}%")

    # ═══════════════════════════════════════════════════════
    # 5. 按月份分析
    # ═══════════════════════════════════════════════════════
    print("\n[5] 按月份分析")
    print("-" * 60)
    month_stats = defaultdict(lambda: {'n': 0, 'ok': 0, 'loose_ok': 0})
    for d in details:
        try:
            m = d['预测日'][:7]
            month_stats[m]['n'] += 1
            if d['宽松正确'] == '✓':
                month_stats[m]['loose_ok'] += 1
            if d['严格正确'] == '✓':
                month_stats[m]['ok'] += 1
        except:
            pass
    for m in sorted(month_stats):
        s = month_stats[m]
        if s['n'] > 0:
            print(f"  {m}: {s['n']:4d}样本  宽松{s['loose_ok']/s['n']*100:5.1f}%  严格{s['ok']/s['n']*100:5.1f}%")

    # ═══════════════════════════════════════════════════════
    # 6. 信号置信度过滤 — 只在高置信度时预测
    # ═══════════════════════════════════════════════════════
    print("\n[6] 信号置信度过滤（只在|融合信号|>阈值时预测）")
    print("-" * 60)
    for threshold in [0.0, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0]:
        group = [d for d in details if abs(d.get('融合信号', 0)) > threshold]
        if not group:
            continue
        n = len(group)
        loose_ok = sum(1 for d in group if d['宽松正确'] == '✓')
        strict_ok = sum(1 for d in group if d['严格正确'] == '✓')
        coverage = n / len(details) * 100
        print(f"  |信号|>{threshold:.1f}: {n:4d}样本({coverage:4.1f}%)  宽松{loose_ok/n*100:5.1f}%  严格{strict_ok/n*100:5.1f}%")

    # ═══════════════════════════════════════════════════════
    # 7. 技术信号 vs 融合信号方向一致性
    # ═══════════════════════════════════════════════════════
    print("\n[7] 技术信号 vs 融合信号方向一致性")
    print("-" * 60)
    aligned = [d for d in details if d.get('技术信号', 0) * d.get('融合信号', 0) > 0]
    misaligned = [d for d in details if d.get('技术信号', 0) * d.get('融合信号', 0) < 0]
    neutral = [d for d in details if d.get('技术信号', 0) * d.get('融合信号', 0) == 0]
    for name, group in [("一致", aligned), ("矛盾", misaligned), ("中性", neutral)]:
        if not group:
            continue
        n = len(group)
        loose_ok = sum(1 for d in group if d['宽松正确'] == '✓')
        print(f"  {name}: {n:4d}样本  宽松{loose_ok/n*100:5.1f}%")

    # ═══════════════════════════════════════════════════════
    # 8. 评分+方向组合分析
    # ═══════════════════════════════════════════════════════
    print("\n[8] 评分+预测方向组合分析")
    print("-" * 60)
    combos = [
        ("高分(≥55)+预测涨", lambda d: d['评分'] >= 55 and d['预测方向'] == '上涨'),
        ("高分(≥55)+预测跌", lambda d: d['评分'] >= 55 and d['预测方向'] == '下跌'),
        ("中分(45-54)+预测涨", lambda d: 45 <= d['评分'] < 55 and d['预测方向'] == '上涨'),
        ("中分(45-54)+预测跌", lambda d: 45 <= d['评分'] < 55 and d['预测方向'] == '下跌'),
        ("低分(<45)+预测涨", lambda d: d['评分'] < 45 and d['预测方向'] == '上涨'),
        ("低分(<45)+预测跌", lambda d: d['评分'] < 45 and d['预测方向'] == '下跌'),
    ]
    for name, cond in combos:
        group = [d for d in details if cond(d)]
        if not group:
            continue
        n = len(group)
        loose_ok = sum(1 for d in group if d['宽松正确'] == '✓')
        strict_ok = sum(1 for d in group if d['严格正确'] == '✓')
        print(f"  {name:25s}: {n:4d}样本  宽松{loose_ok/n*100:5.1f}%  严格{strict_ok/n*100:5.1f}%")

    # ═══════════════════════════════════════════════════════
    # 9. 按板块+信号强度交叉分析
    # ═══════════════════════════════════════════════════════
    print("\n[9] 按板块+信号强度交叉分析（|融合信号|>0.5时）")
    print("-" * 60)
    sectors = set(d['板块'] for d in details)
    for sec in sorted(sectors):
        sec_data = [d for d in details if d['板块'] == sec]
        strong = [d for d in sec_data if abs(d.get('融合信号', 0)) > 0.5]
        weak = [d for d in sec_data if abs(d.get('融合信号', 0)) <= 0.5]
        if strong:
            n = len(strong)
            ok = sum(1 for d in strong if d['宽松正确'] == '✓')
            print(f"  {sec:6s} 强信号: {n:3d}样本  宽松{ok/n*100:5.1f}%", end="")
        if weak:
            n = len(weak)
            ok = sum(1 for d in weak if d['宽松正确'] == '✓')
            print(f"  |  弱信号: {n:3d}样本  宽松{ok/n*100:5.1f}%")
        else:
            print()

    # ═══════════════════════════════════════════════════════
    # 10. 连续错误模式分析
    # ═══════════════════════════════════════════════════════
    print("\n[10] 连续错误模式分析（按股票）")
    print("-" * 60)
    stock_details = defaultdict(list)
    for d in details:
        stock_details[d['名称']].append(d)

    max_streak_errors = []
    for name, dlist in stock_details.items():
        dlist.sort(key=lambda x: x['评分日'])
        max_err = 0
        cur_err = 0
        for d in dlist:
            if d['宽松正确'] == '✗':
                cur_err += 1
                max_err = max(max_err, cur_err)
            else:
                cur_err = 0
        max_streak_errors.append((name, max_err, len(dlist)))

    max_streak_errors.sort(key=lambda x: x[1], reverse=True)
    for name, max_err, total in max_streak_errors[:10]:
        print(f"  {name:10s}: 最长连续错误{max_err:2d}天 (共{total}天)")

    # ═══════════════════════════════════════════════════════
    # 11. 前一天预测结果 vs 今天准确率
    # ═══════════════════════════════════════════════════════
    print("\n[11] 前一天预测结果 vs 今天准确率")
    print("-" * 60)
    prev_ok_today = {'n': 0, 'ok': 0}
    prev_fail_today = {'n': 0, 'ok': 0}
    for name, dlist in stock_details.items():
        dlist.sort(key=lambda x: x['评分日'])
        for i in range(1, len(dlist)):
            if dlist[i-1]['宽松正确'] == '✓':
                prev_ok_today['n'] += 1
                if dlist[i]['宽松正确'] == '✓':
                    prev_ok_today['ok'] += 1
            else:
                prev_fail_today['n'] += 1
                if dlist[i]['宽松正确'] == '✓':
                    prev_fail_today['ok'] += 1
    if prev_ok_today['n'] > 0:
        print(f"  前天正确→今天: {prev_ok_today['n']}样本  宽松{prev_ok_today['ok']/prev_ok_today['n']*100:.1f}%")
    if prev_fail_today['n'] > 0:
        print(f"  前天错误→今天: {prev_fail_today['n']}样本  宽松{prev_fail_today['ok']/prev_fail_today['n']*100:.1f}%")

    # ═══════════════════════════════════════════════════════
    # 12. 评分变化方向 vs 准确率
    # ═══════════════════════════════════════════════════════
    print("\n[12] 评分变化方向 vs 准确率")
    print("-" * 60)
    score_up = {'n': 0, 'ok': 0}
    score_down = {'n': 0, 'ok': 0}
    score_flat = {'n': 0, 'ok': 0}
    for name, dlist in stock_details.items():
        dlist.sort(key=lambda x: x['评分日'])
        for i in range(1, len(dlist)):
            diff = dlist[i]['评分'] - dlist[i-1]['评分']
            if diff > 2:
                bucket = score_up
            elif diff < -2:
                bucket = score_down
            else:
                bucket = score_flat
            bucket['n'] += 1
            if dlist[i]['宽松正确'] == '✓':
                bucket['ok'] += 1
    for name, bucket in [("评分上升(>2)", score_up), ("评分下降(<-2)", score_down), ("评分平稳", score_flat)]:
        if bucket['n'] > 0:
            print(f"  {name:15s}: {bucket['n']:4d}样本  宽松{bucket['ok']/bucket['n']*100:5.1f}%")

    # ═══════════════════════════════════════════════════════
    # 13. 关键发现：哪些条件组合能达到60%+
    # ═══════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("[13] 寻找60%+准确率的条件组合")
    print("=" * 80)

    conditions = [
        ("有色金属+|信号|>0.5", lambda d: d['板块'] == '有色金属' and abs(d.get('融合信号', 0)) > 0.5),
        ("有色金属+|信号|>1.0", lambda d: d['板块'] == '有色金属' and abs(d.get('融合信号', 0)) > 1.0),
        ("医药+同行一致", lambda d: d['板块'] == '医药' and d.get('同行信号', 0) * (1 if d['预测方向'] == '上涨' else -1) > 0.5),
        ("科技+|信号|>1.0", lambda d: d['板块'] == '科技' and abs(d.get('融合信号', 0)) > 1.0),
        ("评分<40+预测跌", lambda d: d['评分'] < 40 and d['预测方向'] == '下跌'),
        ("评分>60+预测涨", lambda d: d['评分'] > 60 and d['预测方向'] == '上涨'),
        ("|融合信号|>1.5", lambda d: abs(d.get('融合信号', 0)) > 1.5),
        ("|融合信号|>2.0", lambda d: abs(d.get('融合信号', 0)) > 2.0),
        ("技术+融合同向+|信号|>0.5", lambda d: d.get('技术信号', 0) * d.get('融合信号', 0) > 0 and abs(d.get('融合信号', 0)) > 0.5),
        ("技术+融合同向+|信号|>1.0", lambda d: d.get('技术信号', 0) * d.get('融合信号', 0) > 0 and abs(d.get('融合信号', 0)) > 1.0),
        ("低分(<45)+预测跌+|信号|>0.5", lambda d: d['评分'] < 45 and d['预测方向'] == '下跌' and abs(d.get('融合信号', 0)) > 0.5),
        ("高分(>55)+预测涨+|信号|>0.5", lambda d: d['评分'] > 55 and d['预测方向'] == '上涨' and abs(d.get('融合信号', 0)) > 0.5),
        ("评分<40", lambda d: d['评分'] < 40),
        ("评分>60", lambda d: d['评分'] > 60),
        ("评分<35", lambda d: d['评分'] < 35),
        ("评分>65", lambda d: d['评分'] > 65),
        ("评分<45+融合<-0.5", lambda d: d['评分'] < 45 and d.get('融合信号', 0) < -0.5),
        ("评分>55+融合>0.5", lambda d: d['评分'] > 55 and d.get('融合信号', 0) > 0.5),
        ("评分<45+融合<-1.0", lambda d: d['评分'] < 45 and d.get('融合信号', 0) < -1.0),
        ("评分>55+融合>1.0", lambda d: d['评分'] > 55 and d.get('融合信号', 0) > 1.0),
        # 评分与方向矛盾（可能是反转信号）
        ("评分<45+预测涨", lambda d: d['评分'] < 45 and d['预测方向'] == '上涨'),
        ("评分>55+预测跌", lambda d: d['评分'] > 55 and d['预测方向'] == '下跌'),
        # 同行信号强度
        ("|同行信号|>2.0", lambda d: abs(d.get('同行信号', 0)) > 2.0),
        ("|同行信号|>2.0+|融合|>0.5", lambda d: abs(d.get('同行信号', 0)) > 2.0 and abs(d.get('融合信号', 0)) > 0.5),
    ]

    found_60 = []
    for name, cond in conditions:
        group = [d for d in details if cond(d)]
        if len(group) < 10:
            continue
        n = len(group)
        loose_ok = sum(1 for d in group if d['宽松正确'] == '✓')
        rate = loose_ok / n * 100
        marker = "✅" if rate >= 60 else ("⚠" if rate >= 55 else "")
        print(f"  {marker} {name:40s}: {n:4d}样本  宽松{rate:5.1f}%")
        if rate >= 55:
            found_60.append((name, n, rate))

    print("\n" + "=" * 80)
    print("高准确率条件汇总（≥55%）:")
    for name, n, rate in sorted(found_60, key=lambda x: -x[2]):
        print(f"  {rate:5.1f}%  {n:4d}样本  {name}")


if __name__ == '__main__':
    analyze()
