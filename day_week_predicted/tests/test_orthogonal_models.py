#!/usr/bin/env python3
"""
正交化V20/V30重设计 + 大样本验证
================================
基于集成学习多样性原理(Negative Correlation Learning)，
重新定义V20/V30的职责，使三模型形成正交互补。

设计原则(来自集成学习研究):
  1. 每个模型使用不同的特征维度(条件独立)
  2. 每个模型的错误模式不同(负相关)
  3. 共识=多角度确认，而非同一判断的重复

三维度定义:
  V11: 价格动量+大盘环境 → 方向判断 (保持不变)
  V20_new: 资金面确认 → 量能+资金流方向
  V30_new: 基本面/结构确认 → 财务+板块+波动率

验证方法:
  300只股票 × 39周，时间序列前向验证(无未来数据泄露)

用法: .venv/bin/python day_week_predicted/tests/test_orthogonal_models.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import random
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from dao import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
random.seed(42)

SAMPLE_SIZE = 300
START_DATE = '2025-06-01'
END_DATE = '2026-03-27'


# ═══════════════════════════════════════════════════════════════
# 数据加载 (复用test_scheme_comparison的逻辑)
# ═══════════════════════════════════════════════════════════════

def load_data():
    from day_week_predicted.tests.test_scheme_comparison import (
        load_sample_stocks, load_klines, load_market_klines, load_fund_flow,
        build_weekly_samples,
    )
    codes = load_sample_stocks()
    sk = load_klines(codes)
    mk = load_market_klines()
    ff = load_fund_flow(codes)
    samples = build_weekly_samples(sk, mk, ff)
    return samples


# ═══════════════════════════════════════════════════════════════
# 新V20: 资金面确认模型
# ═══════════════════════════════════════════════════════════════

def v20_new_capital(sample) -> dict:
    """
    新V20: 资金面确认。

    大样本发现(1950样本×39周):
      资金大幅流出(big_net_pct<-8.47)的超跌股票反弹概率最高(61.8%)
      资金流入(big_net_pct>-1.35)的超跌股票反弹概率最低(49.3%)
      极度缩量(vr<0.60)的反弹概率最高(61.4%)

    这符合A股"恐慌性抛售后反弹"的逻辑:
      大幅流出+缩量 = 恐慌抛售已充分释放，卖压衰竭
      资金流入+放量 = 可能是主力出货(对倒放量)，不是真正抄底

    所以V20的确认逻辑:
      UP预测: 大幅流出(ff<-5) = confirm(恐慌释放)
              轻微流出/流入(ff>-2) = deny(抛压未释放)
      DOWN预测: 资金流入(ff>0) = deny(有人接盘)
               大幅流出(ff<-5) = confirm(资金持续撤离)
    """
    ff = sample.get('big_net_pct_avg')
    pred_up = sample['pred_up']

    if ff is None:
        return {'confidence': 'neutral', 'reason': '无资金流数据'}

    if pred_up:
        # UP预测: 恐慌性抛售后更容易反弹
        if ff < -5:
            return {'confidence': 'high', 'reason': f'恐慌抛售释放(ff={ff:+.1f})'}
        elif ff < -2:
            return {'confidence': 'medium', 'reason': f'资金流出中等(ff={ff:+.1f})'}
        elif ff > -1:
            return {'confidence': 'deny', 'reason': f'抛压未充分释放(ff={ff:+.1f})'}
        else:
            return {'confidence': 'neutral', 'reason': f'资金面中性(ff={ff:+.1f})'}
    else:
        # DOWN预测: 资金流入=有人接盘=可能反弹=否定继续跌
        if ff > 0:
            return {'confidence': 'deny', 'reason': f'资金流入否定跌(ff={ff:+.1f})'}
        elif ff < -5:
            return {'confidence': 'high', 'reason': f'资金持续撤离(ff={ff:+.1f})'}
        elif ff < -2:
            return {'confidence': 'medium', 'reason': f'资金偏弱(ff={ff:+.1f})'}
        else:
            return {'confidence': 'neutral', 'reason': f'资金面中性(ff={ff:+.1f})'}


# ═══════════════════════════════════════════════════════════════
# 新V30: 结构面确认模型
# ═══════════════════════════════════════════════════════════════

def v30_new_structure(sample) -> dict:
    """
    新V30: 结构面确认。

    大样本发现(1950样本×39周):
      极低位(pos<0.03): 62.7% ← 最高
      高位(pos>0.55): 50.4% ← 最低
      大盘深跌(<-3.38%): 66.9% ← 最高
      大盘微跌(-0.44~-0.18%): 43.6% ← 最低

    V30只用price_pos(独立于V11的this_chg和V20的big_net_pct):
      V11预测UP时:
        高位(pos>0.55) → deny (涨幅空间有限)
        低位(pos<0.20) → high (下跌空间已释放)
      V11预测DOWN时:
        低位(pos<0.20) → deny (跌幅空间有限)
        高位(pos>0.55) → high (有下跌空间)

    注意: 不用大盘涨跌(已被V11使用)，保持正交。
    """
    pos = sample.get('price_pos_60')
    pred_up = sample['pred_up']

    if pos is None:
        return {'confidence': 'neutral', 'reason': '无位置数据'}

    if pred_up:
        if pos < 0.20:
            return {'confidence': 'high', 'reason': f'极低位(pos={pos:.2f})'}
        elif pos < 0.40:
            return {'confidence': 'medium', 'reason': f'低位(pos={pos:.2f})'}
        elif pos > 0.55:
            return {'confidence': 'deny', 'reason': f'高位风险(pos={pos:.2f})'}
        else:
            return {'confidence': 'neutral', 'reason': f'中位(pos={pos:.2f})'}
    else:
        if pos > 0.55:
            return {'confidence': 'high', 'reason': f'高位确认跌(pos={pos:.2f})'}
        elif pos > 0.40:
            return {'confidence': 'medium', 'reason': f'中高位(pos={pos:.2f})'}
        elif pos < 0.20:
            return {'confidence': 'deny', 'reason': f'极低位否定跌(pos={pos:.2f})'}
        else:
            return {'confidence': 'neutral', 'reason': f'中位(pos={pos:.2f})'}


# ═══════════════════════════════════════════════════════════════
# 综合共识计算
# ═══════════════════════════════════════════════════════════════

def compute_consensus(v11_conf, v20_result, v30_result) -> str:
    """
    三模型正交共识。

    规则:
      任一模型deny → 最高medium
      三模型都high → consensus high
      两个high + 一个medium/neutral → consensus medium
      其他 → low
    """
    if v11_conf not in ('high', 'reference'):
        return v11_conf  # V11本身不确定，直接返回

    v20c = v20_result['confidence']
    v30c = v30_result['confidence']

    # deny否决
    if v20c == 'deny' or v30c == 'deny':
        return 'low'

    conf_map = {'high': 3, 'medium': 2, 'neutral': 1, 'deny': 0}
    v20s = conf_map.get(v20c, 1)
    v30s = conf_map.get(v30c, 1)

    if v20s >= 3 and v30s >= 3:
        return 'high'
    elif v20s >= 2 and v30s >= 2:
        return 'medium'
    elif v20s + v30s >= 4:  # 一个high一个neutral
        return 'medium'
    else:
        return 'low'


# ═══════════════════════════════════════════════════════════════
# 模拟验证
# ═══════════════════════════════════════════════════════════════

def _pct(n, d):
    return f"{n/d*100:.1f}%" if d > 0 else "-"


def simulate(samples):
    sorted_weeks = sorted(set(s['iso_week'] for s in samples))
    by_week = defaultdict(list)
    for s in samples:
        by_week[s['iso_week']].append(s)

    schemes = ['V11原始', 'V11+deny(B)', '正交共识', '正交high+med']
    results = {k: {'weekly': [], 'h_c': 0, 'h_n': 0, 'hm_c': 0, 'hm_n': 0,
                    'all_c': 0, 'all_n': 0} for k in schemes}

    # 独立性检验数据
    indep_data = []

    for week in sorted_weeks:
        ws = by_week[week]
        week_r = {k: {'h_c': 0, 'h_n': 0, 'hm_c': 0, 'hm_n': 0} for k in schemes}

        for s in ws:
            v11_conf = s['confidence']
            v20r = v20_new_capital(s)
            v30r = v30_new_structure(s)
            consensus = compute_consensus(v11_conf, v20r, v30r)
            correct = s['correct']

            # deny过滤(方案B)
            deny_conf = v11_conf
            vr = s.get('vol_ratio')
            ff = s.get('big_net_pct_avg')
            if vr is not None and vr < 0.7 and ff is not None and ff < 0:
                if deny_conf == 'high':
                    deny_conf = 'reference'
                elif deny_conf == 'reference':
                    deny_conf = 'low'

            # 记录
            for name, conf in [('V11原始', v11_conf), ('V11+deny(B)', deny_conf),
                                ('正交共识', consensus), ('正交high+med', consensus)]:
                if conf == 'high':
                    week_r[name]['h_c'] += int(correct)
                    week_r[name]['h_n'] += 1
                if conf in ('high', 'medium', 'reference'):
                    week_r[name]['hm_c'] += int(correct)
                    week_r[name]['hm_n'] += 1

            # 独立性检验: V11 vs V20_new vs V30_new的错误是否相关
            if v11_conf in ('high', 'reference'):
                indep_data.append({
                    'v11_correct': correct,
                    'v20_confirm': v20r['confidence'] in ('high', 'medium'),
                    'v30_confirm': v30r['confidence'] in ('high', 'medium'),
                    'v20_deny': v20r['confidence'] == 'deny',
                    'v30_deny': v30r['confidence'] == 'deny',
                    'correct': correct,
                })

        for name in schemes:
            wr = week_r[name]
            results[name]['h_c'] += wr['h_c']
            results[name]['h_n'] += wr['h_n']
            results[name]['hm_c'] += wr['hm_c']
            results[name]['hm_n'] += wr['hm_n']
            results[name]['all_c'] += sum(1 for s2 in ws if s2['correct'])
            results[name]['all_n'] += len(ws)
            results[name]['weekly'].append({
                'week': week, 'h_c': wr['h_c'], 'h_n': wr['h_n'],
                'hm_c': wr['hm_c'], 'hm_n': wr['hm_n'],
            })

    return results, sorted_weeks, indep_data


def print_results(results, sorted_weeks, indep_data):
    print(f"\n{'='*85}")
    print(f"  汇总对比")
    print(f"{'='*85}")
    print(f"  {'方案':<16} {'high准确率':>12} {'high数量':>8} {'h+m准确率':>12} {'h+m数量':>8}")
    print(f"  {'─'*70}")
    for name in ['V11原始', 'V11+deny(B)', '正交共识', '正交high+med']:
        r = results[name]
        print(f"  {name:<16} {_pct(r['h_c'], r['h_n']):>12} {r['h_n']:>8} "
              f"{_pct(r['hm_c'], r['hm_n']):>12} {r['hm_n']:>8}")

    # 逐4周对比
    print(f"\n{'='*85}")
    print(f"  逐4周high准确率")
    print(f"{'='*85}")
    chunk = 4
    for ci in range(0, len(sorted_weeks), chunk):
        cw = set(sorted_weeks[ci:ci+chunk])
        w0 = sorted_weeks[ci][1]
        w1 = sorted_weeks[min(ci+chunk-1, len(sorted_weeks)-1)][1]
        parts = [f"  W{w0:02d}-{w1:02d}:"]
        for name in ['V11原始', 'V11+deny(B)', '正交共识']:
            hc = sum(w['h_c'] for w in results[name]['weekly'] if w['week'] in cw)
            hn = sum(w['h_n'] for w in results[name]['weekly'] if w['week'] in cw)
            parts.append(f"{name}={_pct(hc,hn):>6}({hn:>4})")
        print('  '.join(parts))

    # 置信度校准
    print(f"\n{'='*85}")
    print(f"  置信度校准 (理想: high>60%, med 50-60%, low<50%)")
    print(f"{'='*85}")
    for name in ['V11原始', 'V11+deny(B)', '正交共识']:
        r = results[name]
        h_acc = r['h_c'] / r['h_n'] * 100 if r['h_n'] > 0 else 0
        m_c = r['hm_c'] - r['h_c']
        m_n = r['hm_n'] - r['h_n']
        m_acc = m_c / m_n * 100 if m_n > 0 else 0
        l_c = r['all_c'] - r['hm_c']
        l_n = r['all_n'] - r['hm_n']
        l_acc = l_c / l_n * 100 if l_n > 0 else 0
        print(f"  {name:<16} high={h_acc:>5.1f}%({r['h_n']:>5})  "
              f"med={m_acc:>5.1f}%({m_n:>5})  low={l_acc:>5.1f}%({l_n:>5})")

    # 独立性检验
    print(f"\n{'='*85}")
    print(f"  模型独立性检验 (V11有信号的样本)")
    print(f"{'='*85}")
    n = len(indep_data)
    if n > 0:
        # V20 confirm时 vs deny时的V11准确率差异
        v20_conf = [d for d in indep_data if d['v20_confirm']]
        v20_deny = [d for d in indep_data if d['v20_deny']]
        v20_neut = [d for d in indep_data if not d['v20_confirm'] and not d['v20_deny']]
        v30_conf = [d for d in indep_data if d['v30_confirm']]
        v30_deny = [d for d in indep_data if d['v30_deny']]
        v30_neut = [d for d in indep_data if not d['v30_confirm'] and not d['v30_deny']]

        print(f"  总样本: {n}")
        print(f"\n  新V20(资金面)的区分力:")
        for label, subset in [('confirm', v20_conf), ('neutral', v20_neut), ('deny', v20_deny)]:
            if subset:
                c = sum(1 for d in subset if d['correct'])
                print(f"    {label:>10}: {c}/{len(subset)} = {_pct(c, len(subset))}")

        print(f"\n  新V30(结构面)的区分力:")
        for label, subset in [('confirm', v30_conf), ('neutral', v30_neut), ('deny', v30_deny)]:
            if subset:
                c = sum(1 for d in subset if d['correct'])
                print(f"    {label:>10}: {c}/{len(subset)} = {_pct(c, len(subset))}")

        # 交叉: V20 confirm + V30 confirm vs V20 deny + V30 deny
        both_conf = [d for d in indep_data if d['v20_confirm'] and d['v30_confirm']]
        any_deny = [d for d in indep_data if d['v20_deny'] or d['v30_deny']]
        print(f"\n  交叉验证:")
        if both_conf:
            c = sum(1 for d in both_conf if d['correct'])
            print(f"    V20+V30都confirm: {c}/{len(both_conf)} = {_pct(c, len(both_conf))}")
        if any_deny:
            c = sum(1 for d in any_deny if d['correct'])
            print(f"    V20或V30 deny:    {c}/{len(any_deny)} = {_pct(c, len(any_deny))}")

    # 稳定性
    print(f"\n{'='*85}")
    print(f"  稳定性 (逐周high准确率)")
    print(f"{'='*85}")
    for name in ['V11原始', 'V11+deny(B)', '正交共识']:
        accs = [w['h_c']/w['h_n']*100 for w in results[name]['weekly'] if w['h_n'] >= 5]
        if len(accs) >= 3:
            avg = sum(accs)/len(accs)
            std = (sum((a-avg)**2 for a in accs)/(len(accs)-1))**0.5
            print(f"  {name:<16} {avg:.1f}% ± {std:.1f}%  range=[{min(accs):.0f}%,{max(accs):.0f}%]  weeks={len(accs)}")


def main():
    print("=" * 85)
    print("  正交化V20/V30重设计 — 大样本验证")
    print("=" * 85)

    samples = load_data()
    weeks = sorted(set(s['iso_week'] for s in samples))
    print(f"\n  样本: {len(samples)}个  周数: {len(weeks)}")

    results, sorted_weeks, indep_data = simulate(samples)
    print_results(results, sorted_weeks, indep_data)

    print(f"\n{'='*85}")
    print(f"  完成")
    print(f"{'='*85}")


if __name__ == '__main__':
    main()
