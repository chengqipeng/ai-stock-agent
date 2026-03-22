#!/usr/bin/env python3
"""
V12 仓位管理提升量化模拟
========================
用全量回测数据（11568条high confidence记录），
模拟不同仓位策略的收益表现，精确量化提升幅度。

策略对比：
  A) 基准：等权配置（每条预测等额投入）
  B) Kelly仓位：根据子集的Kelly值动态调整仓位
  C) 简单过滤：只做UP方向 + RSI 25-35
  D) 综合策略：Kelly仓位 + 过滤规则

假设：
  - 每周初始资金100万，按预测分配
  - 单只股票最大仓位10%（风控）
  - 持仓1周后平仓
  - 不考虑交易成本（双边约0.3%，后面单独扣除）
"""
import json
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).resolve().parent.parent / "data_results"


def load_records():
    with open(DATA_DIR / "v12_high_confidence_analysis.json", 'r') as f:
        data = json.load(f)
    return data['records'], data['weekly_detail']


def simulate_equal_weight(records_by_week, min_positions=5):
    """策略A：等权配置，所有high confidence预测等额投入"""
    weekly_returns = []
    for wk, recs in sorted(records_by_week.items()):
        if not recs or len(recs) < min_positions:
            continue  # 跳过预测太少的周（不具统计意义）
        n = len(recs)
        weight = min(1.0 / n, 0.10)  # 单只最大10%
        total_weight = weight * n
        # 组合收益 = sum(weight_i * return_i) / total_weight
        # actual_return 已经是百分比（如 +3.2 表示涨3.2%）
        port_ret = 0
        for r in recs:
            ret = r['actual_return']
            # UP预测：做多，收益=actual_return
            # DOWN预测：做空，收益=-actual_return
            if r['pred_direction'] == 'UP':
                port_ret += weight * ret
            else:
                port_ret += weight * (-ret)
        
        if total_weight > 0:
            port_ret = port_ret / total_weight  # 加权平均收益率(%)
        
        weekly_returns.append({
            'week': wk, 'return': port_ret, 'n_positions': n,
            'accuracy': sum(1 for r in recs if r['is_correct']) / n
        })
    return weekly_returns


def simulate_kelly_sizing(records_by_week, min_positions=5):
    """策略B：Kelly仓位，根据子集特征动态调整权重"""
    weekly_returns = []
    
    for wk, recs in sorted(records_by_week.items()):
        if not recs or len(recs) < min_positions:
            continue
        
        positions = []
        for r in recs:
            # 基础权重
            base_weight = 1.0
            
            # DOWN方向：Kelly为负，降到0.3（不完全排除，因为有56%准确率）
            if r['pred_direction'] == 'DOWN':
                base_weight *= 0.3
            
            # RSI区间调整
            rsi = r.get('rsi', 50)
            if 25 <= rsi < 35:
                base_weight *= 1.5  # 最稳定区间，加仓
            elif rsi < 25:
                base_weight *= 0.8  # 看似极端但不稳定
            elif 35 <= rsi < 50:
                base_weight *= 1.0  # 标准
            else:
                base_weight *= 0.7  # RSI>50，反转逻辑弱
            
            # extreme_score调整
            es = r.get('extreme_score', 6)
            if 6 <= es <= 7:
                base_weight *= 1.2  # 时间稳定性最好
            elif es >= 8:
                base_weight *= 0.9  # 准确率不稳定
            
            # price_pos调整
            pp = r.get('price_pos', 0.5)
            if pp < 0.2:
                base_weight *= 1.1  # 盈亏比最好
            
            positions.append({
                'record': r,
                'weight': base_weight
            })
        
        if not positions:
            continue
        
        # 归一化权重，单只最大10%
        total_raw = sum(p['weight'] for p in positions)
        for p in positions:
            p['norm_weight'] = min(p['weight'] / total_raw, 0.10)
        
        # 重新归一化
        total_norm = sum(p['norm_weight'] for p in positions)
        
        port_ret = 0
        for p in positions:
            r = p['record']
            w = p['norm_weight'] / total_norm if total_norm > 0 else 0
            ret = r['actual_return']
            if r['pred_direction'] == 'UP':
                port_ret += w * ret
            else:
                port_ret += w * (-ret)
        
        n = len(positions)
        acc = sum(1 for p in positions if p['record']['is_correct']) / n
        
        weekly_returns.append({
            'week': wk, 'return': port_ret, 'n_positions': n,
            'accuracy': acc
        })
    return weekly_returns


def simulate_filtered_only(records_by_week, min_positions=5):
    """策略C：简单过滤 — 只做UP方向"""
    weekly_returns = []
    
    for wk, recs in sorted(records_by_week.items()):
        # 只保留UP方向
        filtered = [r for r in recs if r['pred_direction'] == 'UP']
        if not filtered or len(filtered) < min_positions:
            continue
        
        n = len(filtered)
        weight = min(1.0 / n, 0.10)
        total_weight = weight * n
        
        port_ret = 0
        for r in filtered:
            port_ret += weight * r['actual_return']
        
        if total_weight > 0:
            port_ret = port_ret / total_weight  # 加权平均收益率(%)
        
        acc = sum(1 for r in filtered if r['is_correct']) / n
        weekly_returns.append({
            'week': wk, 'return': port_ret, 'n_positions': n,
            'accuracy': acc
        })
    return weekly_returns


def simulate_combined(records_by_week, min_positions=5):
    """策略D：综合策略 — 过滤 + Kelly仓位"""
    weekly_returns = []
    
    for wk, recs in sorted(records_by_week.items()):
        # 过滤：只做UP方向
        filtered = [r for r in recs if r['pred_direction'] == 'UP']
        if not filtered or len(filtered) < min_positions:
            continue
        
        positions = []
        for r in filtered:
            base_weight = 1.0
            
            rsi = r.get('rsi', 50)
            if 25 <= rsi < 35:
                base_weight *= 1.5
            elif rsi < 25:
                base_weight *= 0.8
            elif 35 <= rsi < 50:
                base_weight *= 1.0
            else:
                base_weight *= 0.7
            
            es = r.get('extreme_score', 6)
            if 6 <= es <= 7:
                base_weight *= 1.2
            elif es >= 8:
                base_weight *= 0.9
            
            pp = r.get('price_pos', 0.5)
            if pp < 0.2:
                base_weight *= 1.1
            
            positions.append({'record': r, 'weight': base_weight})
        
        total_raw = sum(p['weight'] for p in positions)
        for p in positions:
            p['norm_weight'] = min(p['weight'] / total_raw, 0.10)
        total_norm = sum(p['norm_weight'] for p in positions)
        
        port_ret = 0
        for p in positions:
            w = p['norm_weight'] / total_norm if total_norm > 0 else 0
            port_ret += w * p['record']['actual_return']
        
        n = len(positions)
        acc = sum(1 for p in positions if p['record']['is_correct']) / n
        
        weekly_returns.append({
            'week': wk, 'return': port_ret, 'n_positions': n,
            'accuracy': acc
        })
    return weekly_returns


def compute_metrics(weekly_returns, name):
    """计算策略指标"""
    if not weekly_returns:
        return None
    
    rets = [w['return'] for w in weekly_returns]
    n_weeks = len(rets)
    
    # 累计收益
    cumulative = 1.0
    peak = 1.0
    max_dd = 0
    equity_curve = [1.0]
    
    for r in rets:
        cumulative *= (1 + r / 100)
        equity_curve.append(cumulative)
        if cumulative > peak:
            peak = cumulative
        dd = (peak - cumulative) / peak
        if dd > max_dd:
            max_dd = dd
    
    total_return = (cumulative - 1) * 100
    
    # 年化收益（假设52周/年）
    if cumulative > 0 and n_weeks > 0:
        annual_return = ((cumulative ** (52 / n_weeks)) - 1) * 100
    else:
        annual_return = -100.0  # 全亏
    
    # 周均收益
    avg_weekly = sum(rets) / n_weeks
    
    # 标准差
    std_weekly = (sum((r - avg_weekly) ** 2 for r in rets) / (n_weeks - 1)) ** 0.5 if n_weeks > 1 else 0
    
    # Sharpe（周度，无风险利率按年化3%≈周0.058%）
    rf_weekly = 0.058
    sharpe = (avg_weekly - rf_weekly) / std_weekly if std_weekly > 0 else 0
    sharpe_annual = sharpe * (52 ** 0.5)  # 年化
    
    # 胜率（周度）
    win_weeks = sum(1 for r in rets if r > 0)
    win_rate = win_weeks / n_weeks
    
    # 盈亏比（周度）
    gains = [r for r in rets if r > 0]
    losses = [r for r in rets if r < 0]
    avg_gain = sum(gains) / len(gains) if gains else 0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0
    payoff = avg_gain / avg_loss if avg_loss > 0 else float('inf')
    
    # 平均持仓数
    avg_positions = sum(w['n_positions'] for w in weekly_returns) / n_weeks
    
    # 扣除交易成本后的收益（双边0.3%）
    cost_per_trade = 0.3  # %
    total_cost = sum(w['n_positions'] * cost_per_trade for w in weekly_returns)
    net_return = total_return - total_cost / 100 * 100  # 近似
    
    return {
        'name': name,
        'n_weeks': n_weeks,
        'total_return': total_return,
        'annual_return': annual_return,
        'avg_weekly': avg_weekly,
        'std_weekly': std_weekly,
        'sharpe_annual': sharpe_annual,
        'max_drawdown': max_dd * 100,
        'win_rate': win_rate,
        'payoff_ratio': payoff,
        'avg_positions': avg_positions,
        'total_cost_pct': total_cost / 100,
        'net_return_approx': total_return - total_cost / 100,
    }


def main():
    records, weekly_detail = load_records()
    
    # 按周分组
    by_week = defaultdict(list)
    for r in records:
        by_week[r['week']].append(r)
    
    print("=" * 80)
    print("V12 仓位管理策略模拟 — 量化提升幅度")
    print("=" * 80)
    print(f"数据: {len(records)}条 high confidence 预测, {len(by_week)}周")
    
    # 运行四个策略
    strategies = {
        'A) 等权基准': simulate_equal_weight(by_week),
        'B) Kelly仓位': simulate_kelly_sizing(by_week),
        'C) 只做UP(等权)': simulate_filtered_only(by_week),
        'D) UP+Kelly综合': simulate_combined(by_week),
    }
    
    # 计算指标
    metrics = {}
    for name, wr in strategies.items():
        m = compute_metrics(wr, name)
        if m:
            metrics[name] = m
    
    # 对比表
    print(f"\n{'指标':<20s}", end='')
    for name in metrics:
        print(f" {name:>16s}", end='')
    print()
    print("-" * (20 + 17 * len(metrics)))
    
    rows = [
        ('活跃周数', 'n_weeks', '{:d}'),
        ('累计收益%', 'total_return', '{:+.1f}%'),
        ('年化收益%', 'annual_return', '{:+.1f}%'),
        ('周均收益%', 'avg_weekly', '{:+.2f}%'),
        ('周标准差%', 'std_weekly', '{:.2f}%'),
        ('年化Sharpe', 'sharpe_annual', '{:.2f}'),
        ('最大回撤%', 'max_drawdown', '{:.1f}%'),
        ('周胜率', 'win_rate', '{:.1%}'),
        ('周盈亏比', 'payoff_ratio', '{:.2f}'),
        ('平均持仓数', 'avg_positions', '{:.0f}'),
    ]
    
    for label, key, fmt in rows:
        print(f"  {label:<18s}", end='')
        for name, m in metrics.items():
            val = m[key]
            print(f" {fmt.format(val):>16s}", end='')
        print()
    
    # 提升幅度
    base = metrics.get('A) 等权基准')
    if base:
        print(f"\n相对基准(A)的提升:")
        print("-" * 60)
        for name, m in metrics.items():
            if name == 'A) 等权基准':
                continue
            ret_diff = m['total_return'] - base['total_return']
            sharpe_diff = m['sharpe_annual'] - base['sharpe_annual']
            dd_diff = m['max_drawdown'] - base['max_drawdown']
            wr_diff = m['win_rate'] - base['win_rate']
            print(f"  {name}:")
            print(f"    累计收益: {ret_diff:+.1f}% (基准{base['total_return']:+.1f}%)")
            print(f"    年化Sharpe: {sharpe_diff:+.2f} (基准{base['sharpe_annual']:.2f})")
            print(f"    最大回撤: {dd_diff:+.1f}% (基准{base['max_drawdown']:.1f}%)")
            print(f"    周胜率: {wr_diff:+.1%} (基准{base['win_rate']:.1%})")
    
    # 周度明细对比（只显示差异最大的周）
    print(f"\n周度收益对比（差异最大的10周）:")
    print(f"  {'周':<12s} {'等权A':>8s} {'Kelly B':>8s} {'UP C':>8s} {'综合D':>8s} {'D-A差':>8s}")
    print("  " + "-" * 50)
    
    # 合并周度数据
    week_compare = {}
    for name, wr in strategies.items():
        for w in wr:
            if w['week'] not in week_compare:
                week_compare[w['week']] = {}
            week_compare[w['week']][name] = w['return']
    
    # 按D-A差异排序
    diffs = []
    for wk, vals in week_compare.items():
        a = vals.get('A) 等权基准', 0)
        d = vals.get('D) UP+Kelly综合', 0)
        diffs.append((wk, d - a, vals))
    
    diffs.sort(key=lambda x: abs(x[1]), reverse=True)
    for wk, diff, vals in diffs[:10]:
        a = vals.get('A) 等权基准', 0)
        b = vals.get('B) Kelly仓位', 0)
        c = vals.get('C) 只做UP(等权)', 0)
        d = vals.get('D) UP+Kelly综合', 0)
        print(f"  {wk:<12s} {a:>+7.2f}% {b:>+7.2f}% {c:>+7.2f}% {d:>+7.2f}% {diff:>+7.2f}%")
    
    # 前后半段对比（检查策略稳定性）
    print(f"\n策略稳定性（前/后半段）:")
    all_weeks = sorted(by_week.keys())
    mid = len(all_weeks) // 2
    first_half = set(all_weeks[:mid])
    
    for name, wr in strategies.items():
        first = [w for w in wr if w['week'] in first_half]
        second = [w for w in wr if w['week'] not in first_half]
        if first and second:
            f_avg = sum(w['return'] for w in first) / len(first)
            s_avg = sum(w['return'] for w in second) / len(second)
            print(f"  {name}: 前半{f_avg:+.2f}%/周, 后半{s_avg:+.2f}%/周, 差{s_avg-f_avg:+.2f}%")
    
    # 保存结果
    output = {
        'strategies': {name: m for name, m in metrics.items()},
        'weekly_detail': {
            name: [{'week': w['week'], 'return': round(w['return'], 4),
                    'n': w['n_positions'], 'acc': round(w['accuracy'], 4)}
                   for w in wr]
            for name, wr in strategies.items()
        }
    }
    
    out_path = DATA_DIR / "v12_position_sizing_simulation.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n结果已保存: {out_path}")


if __name__ == '__main__':
    main()
